"""Transactional PostgreSQL publication for validated split Gold."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, cast

from psycopg import Connection, Cursor

from xetra_loader.gold.splits import SplitGoldResult
from xetra_loader.gold.validation import GoldValidationSummary
from xetra_loader.sync.core import (
    AuthoritativeSnapshotRequired,
    JSONValue,
    SyncCounters,
    SyncOutcome,
    run_sync,
)


def sync_splits(
    connection: Connection[Any],
    gold: SplitGoldResult,
    *,
    run_id: str | None = None,
    published_at_utc: datetime | None = None,
    fetched_at_by_key: Mapping[tuple[str, str, str, str], datetime] | None = None,
    authoritative_snapshot: GoldValidationSummary | None = None,
) -> SyncOutcome:
    """Apply active split events and tombstone retractions in one transaction."""

    published_at = published_at_utc or datetime.now(UTC)
    _require_utc(published_at)
    _validate_snapshot(authoritative_snapshot, gold.row_count, gold.semantic_fingerprint)
    semantic_rows: list[dict[str, JSONValue]] = list(gold.semantic_rows())
    semantic_rows.extend(
        {
            "isin": key[0],
            "exchange": key[1],
            "code": key[2],
            "event_key": key[3],
            "retracted": True,
        }
        for key in gold.retracted_keys
    )

    def mutate(cursor: Cursor[Any]) -> SyncCounters:
        inserted = 0
        updated = 0
        retracted = 0
        expected_keys = {event.key for event in gold.rows}
        for key in gold.retracted_keys:
            cursor.execute(
                "DELETE FROM xetra_loader.splits "
                "WHERE isin = %s AND exchange = %s AND code = %s AND event_key = %s",
                key,
            )
            if cursor.rowcount > 0:
                retracted += cursor.rowcount

        for event in gold.rows:
            fetched_at = _fetched_at(event.key, fetched_at_by_key, published_at)
            cursor.execute(
                "SELECT event_date, split_ratio, split_factor FROM xetra_loader.splits "
                "WHERE isin = %s AND exchange = %s AND code = %s AND event_key = %s",
                event.key,
            )
            existing = cast(
                tuple[date, str, Decimal | None] | None,
                cursor.fetchone(),
            )
            semantic = (event.event_date, event.split_ratio, event.split_factor)
            if existing is None:
                cursor.execute(
                    "INSERT INTO xetra_loader.splits "
                    "(isin, exchange, code, event_key, event_date, split_ratio, split_factor, "
                    "fetched_at_utc, published_at_utc) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (*event.key, *semantic, fetched_at, published_at),
                )
                inserted += 1
            elif existing != semantic:
                cursor.execute(
                    "UPDATE xetra_loader.splits SET event_date = %s, split_ratio = %s, "
                    "split_factor = %s, fetched_at_utc = %s, published_at_utc = %s "
                    "WHERE isin = %s AND exchange = %s AND code = %s AND event_key = %s",
                    (*semantic, fetched_at, published_at, *event.key),
                )
                updated += 1
        existing_keys = cursor.execute(
            "SELECT isin, exchange, code, event_key FROM xetra_loader.splits"
        ).fetchall()
        for key in existing_keys:
            normalized = cast(tuple[str, str, str, str], key)
            if normalized not in expected_keys:
                cursor.execute(
                    "DELETE FROM xetra_loader.splits "
                    "WHERE isin = %s AND exchange = %s AND code = %s AND event_key = %s",
                    normalized,
                )
                retracted += cursor.rowcount
        return SyncCounters(inserted=inserted, updated=updated, retracted=retracted)

    return run_sync(
        connection,
        dataset="splits",
        semantic_rows=semantic_rows,
        mutate=mutate,
        run_id=run_id,
        authoritative_snapshot=authoritative_snapshot,
    )


def _validate_snapshot(
    snapshot: GoldValidationSummary | None,
    row_count: int,
    semantic_fingerprint: str,
) -> None:
    if snapshot is not None and not snapshot.matches(
        "splits", row_count=row_count, semantic_fingerprint=semantic_fingerprint
    ):
        raise AuthoritativeSnapshotRequired("Gold snapshot proof does not match splits")


def _require_utc(value: datetime) -> None:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset != UTC.utcoffset(value):
        raise ValueError("published_at_utc must be timezone-aware UTC")


def _fetched_at(
    key: tuple[str, str, str, str],
    values: Mapping[tuple[str, str, str, str], datetime] | None,
    published_at: datetime,
) -> datetime:
    value = published_at if values is None else values.get(key, published_at)
    _require_utc(value)
    if value > published_at:
        raise ValueError("fetched_at_utc must not be after published_at_utc")
    return value
