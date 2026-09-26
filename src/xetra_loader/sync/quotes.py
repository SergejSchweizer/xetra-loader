"""Transactional PostgreSQL publication for validated EOD quote Gold."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Any, cast

from psycopg import Connection, Cursor

from xetra_loader.gold.quotes import QuoteGoldResult
from xetra_loader.sync.core import SyncCounters, SyncOutcome, run_sync


def sync_quotes(
    connection: Connection[Any],
    gold: QuoteGoldResult,
    *,
    run_id: str | None = None,
    published_at_utc: datetime | None = None,
    fetched_at_by_key: Mapping[tuple[str, str, str, date], datetime] | None = None,
) -> SyncOutcome:
    """Insert new quote dates, update corrections, and skip semantic replays."""

    published_at = published_at_utc or datetime.now(UTC)
    _require_utc(published_at)

    def mutate(cursor: Cursor[Any]) -> SyncCounters:
        _copy_desired_quotes(cursor, gold, fetched_at_by_key, published_at)
        inserted = _count_inserted_quotes(cursor)
        updated = _count_updated_quotes(cursor)
        deleted = _count_deleted_quotes(cursor)
        cursor.execute(_UPSERT_DESIRED_QUOTES)
        cursor.execute(_DELETE_RETRACTED_QUOTES)
        return SyncCounters(inserted=inserted, updated=updated, deleted=deleted)

    return run_sync(
        connection,
        dataset="eod_quotes",
        semantic_rows=gold.semantic_rows(),
        mutate=mutate,
        run_id=run_id,
    )


def _require_utc(value: datetime) -> None:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset != UTC.utcoffset(value):
        raise ValueError("published_at_utc must be timezone-aware UTC")


def _fetched_at(
    key: tuple[str, str, str, date],
    values: Mapping[tuple[str, str, str, date], datetime] | None,
    published_at: datetime,
) -> datetime:
    value = published_at if values is None else values.get(key, published_at)
    _require_utc(value)
    if value > published_at:
        raise ValueError("fetched_at_utc must not be after published_at_utc")
    return value


def _copy_desired_quotes(
    cursor: Cursor[Any],
    gold: QuoteGoldResult,
    fetched_at_by_key: Mapping[tuple[str, str, str, date], datetime] | None,
    published_at: datetime,
) -> None:
    """Stage the complete desired quote state once for set-based reconciliation."""

    cursor.execute("DROP TABLE IF EXISTS xdl_desired_eod_quotes")
    cursor.execute(_CREATE_DESIRED_QUOTES)
    with cursor.copy(_COPY_DESIRED_QUOTES) as copy:
        for row in gold.rows:
            copy.write_row(
                (
                    *row.key,
                    row.timestamp_eod,
                    row.open,
                    row.high,
                    row.low,
                    row.close,
                    row.adjusted_close,
                    row.volume,
                    _fetched_at(row.key, fetched_at_by_key, published_at),
                    published_at,
                )
            )


def _count_inserted_quotes(cursor: Cursor[Any]) -> int:
    row = cursor.execute(
        """
        SELECT count(*)
        FROM xdl_desired_eod_quotes AS desired
        LEFT JOIN xetra_loader.eod_quotes AS current
          USING (isin, exchange, code, trade_date)
        WHERE current.isin IS NULL
        """
    ).fetchone()
    return int(cast(tuple[int], row)[0])


def _count_updated_quotes(cursor: Cursor[Any]) -> int:
    row = cursor.execute(
        """
        SELECT count(*)
        FROM xdl_desired_eod_quotes AS desired
        JOIN xetra_loader.eod_quotes AS current
          USING (isin, exchange, code, trade_date)
        WHERE (current.open, current.high, current.low, current.close,
               current.adjusted_close, current.volume)
              IS DISTINCT FROM
              (desired.open, desired.high, desired.low, desired.close,
               desired.adjusted_close, desired.volume)
        """
    ).fetchone()
    return int(cast(tuple[int], row)[0])


def _count_deleted_quotes(cursor: Cursor[Any]) -> int:
    row = cursor.execute(
        """
        SELECT count(*)
        FROM xetra_loader.eod_quotes AS current
        WHERE NOT EXISTS (
            SELECT 1
            FROM xdl_desired_eod_quotes AS desired
            WHERE desired.isin = current.isin
              AND desired.exchange = current.exchange
              AND desired.code = current.code
              AND desired.trade_date = current.trade_date
        )
        """
    ).fetchone()
    return int(cast(tuple[int], row)[0])


_CREATE_DESIRED_QUOTES = """
CREATE TEMP TABLE xdl_desired_eod_quotes (
    isin text NOT NULL,
    exchange text NOT NULL,
    code text NOT NULL,
    trade_date date NOT NULL,
    timestamp_eod timestamptz NOT NULL,
    open numeric,
    high numeric,
    low numeric,
    close numeric NOT NULL,
    adjusted_close numeric,
    volume bigint,
    fetched_at_utc timestamptz NOT NULL,
    published_at_utc timestamptz NOT NULL,
    PRIMARY KEY (isin, exchange, code, trade_date)
) ON COMMIT DROP
"""

_COPY_DESIRED_QUOTES = """
COPY xdl_desired_eod_quotes (
    isin, exchange, code, trade_date, timestamp_eod, open, high, low, close,
    adjusted_close, volume, fetched_at_utc, published_at_utc
) FROM STDIN
"""

_UPSERT_DESIRED_QUOTES = """
INSERT INTO xetra_loader.eod_quotes AS current (
    isin, exchange, code, trade_date, timestamp_eod, open, high, low, close,
    adjusted_close, volume, fetched_at_utc, published_at_utc
)
SELECT
    isin, exchange, code, trade_date, timestamp_eod, open, high, low, close,
    adjusted_close, volume, fetched_at_utc, published_at_utc
FROM xdl_desired_eod_quotes
ON CONFLICT (isin, exchange, code, trade_date) DO UPDATE SET
    timestamp_eod = EXCLUDED.timestamp_eod,
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    adjusted_close = EXCLUDED.adjusted_close,
    volume = EXCLUDED.volume,
    fetched_at_utc = EXCLUDED.fetched_at_utc,
    published_at_utc = EXCLUDED.published_at_utc
WHERE (current.open, current.high, current.low, current.close,
       current.adjusted_close, current.volume)
      IS DISTINCT FROM
      (EXCLUDED.open, EXCLUDED.high, EXCLUDED.low, EXCLUDED.close,
       EXCLUDED.adjusted_close, EXCLUDED.volume)
"""

_DELETE_RETRACTED_QUOTES = """
DELETE FROM xetra_loader.eod_quotes AS current
WHERE NOT EXISTS (
    SELECT 1
    FROM xdl_desired_eod_quotes AS desired
    WHERE desired.isin = current.isin
      AND desired.exchange = current.exchange
      AND desired.code = current.code
      AND desired.trade_date = current.trade_date
)
"""
