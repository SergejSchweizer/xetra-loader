"""Transactional PostgreSQL publication for validated dividend Gold."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, cast

from psycopg import Connection, Cursor

from xetra_loader.gold.dividends import DividendGoldResult
from xetra_loader.gold.validation import GoldValidationSummary
from xetra_loader.sync.core import JSONValue, SyncCounters, SyncOutcome, run_sync

DividendSemantic = tuple[
    date,
    date | None,
    date | None,
    date | None,
    Decimal,
    str | None,
    str | None,
]


def sync_dividends(
    connection: Connection[Any],
    gold: DividendGoldResult,
    *,
    run_id: str | None = None,
    published_at_utc: datetime | None = None,
    fetched_at_by_key: Mapping[tuple[str, str, str, str], datetime] | None = None,
    authoritative_snapshot: GoldValidationSummary | None = None,
) -> SyncOutcome:
    """Apply active dividend events and tombstone retractions in one transaction."""

    published_at = published_at_utc or datetime.now(UTC)
    _require_utc(published_at)
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
                "DELETE FROM xetra_loader.dividends "
                "WHERE isin = %s AND exchange = %s AND code = %s AND event_key = %s",
                key,
            )
            if cursor.rowcount > 0:
                retracted += cursor.rowcount

        for event in gold.rows:
            fetched_at = _fetched_at(event.key, fetched_at_by_key, published_at)
            cursor.execute(
                "SELECT event_date, declaration_date, record_date, payment_date, value, "
                "currency, period FROM xetra_loader.dividends "
                "WHERE isin = %s AND exchange = %s AND code = %s AND event_key = %s",
                event.key,
            )
            existing = cast(DividendSemantic | None, cursor.fetchone())
            semantic: DividendSemantic = (
                event.event_date,
                event.declaration_date,
                event.record_date,
                event.payment_date,
                event.value,
                event.currency,
                event.period,
            )
            if existing is None:
                cursor.execute(
                    "INSERT INTO xetra_loader.dividends "
                    "(isin, exchange, code, event_key, event_date, declaration_date, record_date, "
                    "payment_date, value, currency, period, fetched_at_utc, published_at_utc) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (*event.key, *semantic, fetched_at, published_at),
                )
                inserted += 1
            elif existing != semantic:
                cursor.execute(
                    "UPDATE xetra_loader.dividends SET event_date = %s, declaration_date = %s, "
                    "record_date = %s, payment_date = %s, value = %s, currency = %s, period = %s, "
                    "fetched_at_utc = %s, published_at_utc = %s "
                    "WHERE isin = %s AND exchange = %s AND code = %s AND event_key = %s",
                    (*semantic, fetched_at, published_at, *event.key),
                )
                updated += 1
        existing_keys = cursor.execute(
            "SELECT isin, exchange, code, event_key FROM xetra_loader.dividends"
        ).fetchall()
        for key in existing_keys:
            normalized = cast(tuple[str, str, str, str], key)
            if normalized not in expected_keys:
                cursor.execute(
                    "DELETE FROM xetra_loader.dividends "
                    "WHERE isin = %s AND exchange = %s AND code = %s AND event_key = %s",
                    normalized,
                )
                retracted += cursor.rowcount
        return SyncCounters(inserted=inserted, updated=updated, retracted=retracted)

    return run_sync(
        connection,
        dataset="dividends",
        semantic_rows=semantic_rows,
        mutate=mutate,
        run_id=run_id,
        authoritative_snapshot=authoritative_snapshot,
    )


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
