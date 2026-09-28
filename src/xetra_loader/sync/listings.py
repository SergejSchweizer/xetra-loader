"""Transactional PostgreSQL publication for validated listing Gold."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, cast

from psycopg import Connection, Cursor

from xetra_loader.gold.listings import ListingGoldResult
from xetra_loader.sync.core import SyncCounters, SyncOutcome, run_sync


def sync_listings(
    connection: Connection[Any],
    gold: ListingGoldResult,
    *,
    run_id: str | None = None,
    published_at_utc: datetime | None = None,
    fetched_at_by_key: Mapping[tuple[str, str, str], datetime] | None = None,
) -> SyncOutcome:
    """Insert/update listing rows while preserving semantic no-op behavior."""

    published_at = published_at_utc or datetime.now(UTC)
    _require_utc(published_at)

    def mutate(cursor: Cursor[Any]) -> SyncCounters:
        inserted = 0
        updated = 0
        for row in gold.rows:
            fetched_at = _fetched_at(row.key, fetched_at_by_key, published_at)
            cursor.execute(
                "SELECT name, instrument_type, currency, country, is_active "
                "FROM xetra_loader.listings "
                "WHERE isin = %s AND exchange = %s AND code = %s",
                row.key,
            )
            existing = cast(
                tuple[str | None, str | None, str | None, str | None, bool] | None,
                cursor.fetchone(),
            )
            semantic = (
                row.name,
                row.instrument_type,
                row.currency,
                row.country,
                row.is_active,
            )
            if existing is None:
                cursor.execute(
                    "INSERT INTO xetra_loader.listings "
                    "(isin, exchange, code, name, instrument_type, currency, country, is_active, "
                    "fetched_at_utc, published_at_utc) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (*row.key, *semantic, fetched_at, published_at),
                )
                inserted += 1
            elif existing != semantic:
                cursor.execute(
                    "UPDATE xetra_loader.listings SET name = %s, instrument_type = %s, "
                    "currency = %s, country = %s, is_active = %s, fetched_at_utc = %s, "
                    "published_at_utc = %s "
                    "WHERE isin = %s AND exchange = %s AND code = %s",
                    (*semantic, fetched_at, published_at, *row.key),
                )
                updated += 1
        return SyncCounters(inserted=inserted, updated=updated)

    return run_sync(
        connection,
        dataset="listings",
        semantic_rows=gold.semantic_rows(),
        mutate=mutate,
        run_id=run_id,
    )


def prune_stale_listings(connection: Connection[Any], gold: ListingGoldResult) -> int:
    """Delete listings absent from Gold after dependent datasets were reconciled."""

    expected_keys = {row.key for row in gold.rows}
    with connection.transaction(), connection.cursor() as cursor:
        existing_keys = {
            tuple(str(value) for value in key)
            for key in cursor.execute(
                "SELECT isin, exchange, code FROM xetra_loader.listings"
            ).fetchall()
        }
        deleted = 0
        for stale_key in sorted(existing_keys - expected_keys):
            cursor.execute(
                "DELETE FROM xetra_loader.listings WHERE isin = %s AND exchange = %s AND code = %s",
                stale_key,
            )
            deleted += cursor.rowcount
    return deleted


def _require_utc(value: datetime) -> None:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset != UTC.utcoffset(value):
        raise ValueError("published_at_utc must be timezone-aware UTC")


def _fetched_at(
    key: tuple[str, str, str],
    values: Mapping[tuple[str, str, str], datetime] | None,
    published_at: datetime,
) -> datetime:
    value = published_at if values is None else values.get(key, published_at)
    _require_utc(value)
    if value > published_at:
        raise ValueError("fetched_at_utc must not be after published_at_utc")
    return value
