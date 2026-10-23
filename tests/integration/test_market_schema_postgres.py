import os
import subprocess
from pathlib import Path

import psycopg
import pytest

from xetra_loader.ops.bootstrap import PostgresEodhdBootstrapRuntime

DSN = os.getenv("XDL_TEST_POSTGRES_DSN")
pytestmark = pytest.mark.integration


def _psql(*arguments: str, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    if DSN is None:
        pytest.skip("XDL_TEST_POSTGRES_DSN is not configured")
    return subprocess.run(
        ["psql", DSN, "-X", "-v", "ON_ERROR_STOP=1", *arguments],
        check=True,
        text=True,
        input=input_text,
        capture_output=True,
    )


def test_market_schema_recreates_and_introspects_exact_types() -> None:
    _psql("-c", "DROP SCHEMA IF EXISTS xetra_loader CASCADE")
    schema_sql = Path("sql/schema/001_xetra_loader.sql").read_text(encoding="utf-8")
    _psql(input_text=schema_sql)
    result = _psql(
        "-At",
        "-q",
        "-c",
        "SELECT table_name || ':' || column_name || ':' || data_type || ':' || "
        "COALESCE(datetime_precision::text, '') FROM information_schema.columns "
        "WHERE table_schema='xetra_loader' ORDER BY table_name, ordinal_position",
    )
    rows = result.stdout.splitlines()
    assert any(row.startswith("eod_quotes:trade_date:date:") for row in rows)
    for table in ("listings", "eod_quotes", "dividends", "splits"):
        assert any(
            row.startswith(f"{table}:available_at_utc:timestamp with time zone:6")
            for row in rows
        )
    timestamp_rows = [row for row in rows if "timestamp with time zone" in row]
    assert timestamp_rows
    assert all(row.endswith(":6") for row in timestamp_rows)

    values = _psql(
        "-At",
        "-c",
        "SELECT (xetra_loader.next_business_day_utc('2026-10-23') AT TIME ZONE 'UTC')::date, "
        "(xetra_loader.next_business_day_utc('2026-10-24') AT TIME ZONE 'UTC')::date, "
        "(xetra_loader.next_business_day_utc('2026-10-26') AT TIME ZONE 'UTC')::date",
    )
    assert values.stdout.strip() == "2026-10-26|2026-10-26|2026-10-27"


def test_generated_availability_columns_follow_source_dates() -> None:
    _psql("-c", "DROP SCHEMA IF EXISTS xetra_loader CASCADE")
    schema_sql = Path("sql/schema/001_xetra_loader.sql").read_text(encoding="utf-8")
    _psql(input_text=schema_sql)
    result = _psql(
        "-At",
        "-q",
        "-c",
        "INSERT INTO xetra_loader.listings "
        "(isin, exchange, code, fetched_at_utc, published_at_utc) VALUES "
        "('DE0000000001','XETRA','AAA','2026-10-23 12:00+00','2026-10-23 12:00+00'); "
        "INSERT INTO xetra_loader.eod_quotes "
        "(isin, exchange, code, trade_date, timestamp_eod, close, fetched_at_utc, "
        "published_at_utc) "
        "VALUES ('DE0000000001','XETRA','AAA','2026-10-23','2026-10-23 00:00+00',1,"
        "'2026-10-23 12:00+00','2026-10-23 12:00+00'); "
        "INSERT INTO xetra_loader.dividends "
        "(isin, exchange, code, event_key, event_date, value, fetched_at_utc, published_at_utc) "
        "VALUES ('DE0000000001','XETRA','AAA',repeat('a',64),'2026-10-23',1,"
        "'2026-10-23 12:00+00','2026-10-23 12:00+00'); "
        "INSERT INTO xetra_loader.splits "
        "(isin, exchange, code, event_key, event_date, split_ratio, fetched_at_utc, "
        "published_at_utc) "
        "VALUES ('DE0000000001','XETRA','AAA',repeat('b',64),'2026-10-23','2:1',"
        "'2026-10-23 12:00+00','2026-10-23 12:00+00'); "
        "SELECT (available_at_utc AT TIME ZONE 'UTC')::date FROM xetra_loader.listings "
        "UNION ALL SELECT (available_at_utc AT TIME ZONE 'UTC')::date FROM xetra_loader.eod_quotes "
        "UNION ALL SELECT (available_at_utc AT TIME ZONE 'UTC')::date FROM xetra_loader.dividends "
        "UNION ALL SELECT (available_at_utc AT TIME ZONE 'UTC')::date "
        "FROM xetra_loader.splits ORDER BY 1",
    )
    assert result.stdout.splitlines() == ["2026-10-26"] * 4


def test_weekly_schema_preflight_rejects_deployed_not_null_close() -> None:
    _psql("-c", "DROP SCHEMA IF EXISTS xetra_loader CASCADE")
    schema_sql = Path("sql/schema/001_xetra_loader.sql").read_text(encoding="utf-8")
    _psql(input_text=schema_sql)
    _psql("-c", "ALTER TABLE xetra_loader.eod_quotes ALTER COLUMN close SET NOT NULL")
    assert DSN is not None
    connection = psycopg.connect(DSN)
    runtime = object.__new__(PostgresEodhdBootstrapRuntime)
    runtime._connection = connection
    runtime._serving_schema = "xetra_loader"
    try:
        with pytest.raises(RuntimeError, match="must be nullable.*xdl-migrate"):
            runtime.validate_weekly_schema_contract()
    finally:
        connection.close()
