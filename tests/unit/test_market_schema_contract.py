from pathlib import Path

import pytest

from xetra_loader.ops.bootstrap import PostgresEodhdBootstrapRuntime

SCHEMA = Path("sql/schema/001_xetra_loader.sql")


def test_schema_freezes_serving_tables_and_keys() -> None:
    sql = SCHEMA.read_text(encoding="utf-8")
    assert "CREATE SCHEMA IF NOT EXISTS xetra_loader" in sql
    assert "xetra_loader.listings" in sql
    assert "xetra_loader.eod_quotes" in sql
    assert "xetra_loader.dividends" in sql
    assert "xetra_loader.splits" in sql
    assert "PRIMARY KEY (isin, exchange, code)" in sql
    assert "PRIMARY KEY (isin, exchange, code, trade_date)" in sql
    assert sql.count("PRIMARY KEY (isin, exchange, code, event_key)") == 2
    assert "is_active BOOLEAN NOT NULL DEFAULT TRUE" in sql


def test_schema_uses_exact_timestamp_and_date_contract() -> None:
    sql = SCHEMA.read_text(encoding="utf-8")
    assert "timestamp_eod TIMESTAMPTZ(6) NOT NULL" in sql
    assert "trade_date DATE NOT NULL" in sql
    assert "timestamp_eod = (trade_date::timestamp AT TIME ZONE 'UTC')" in sql
    assert "TIMESTAMP WITHOUT TIME ZONE" not in sql.upper()


def test_all_xetra_tables_have_generated_availability_timestamps() -> None:
    sql = SCHEMA.read_text(encoding="utf-8")
    assert sql.count("available_at_utc TIMESTAMPTZ(6) GENERATED ALWAYS AS (") == 8
    assert "next_business_day_utc((published_at_utc AT TIME ZONE 'UTC')::date)" in sql
    assert "next_business_day_utc(trade_date)" in sql
    assert sql.count("next_business_day_utc(event_date)") == 4


def test_feature_refresh_does_not_create_a_separate_writer_role() -> None:
    roles_sql = Path("sql/schema/002_roles.sql").read_text(encoding="utf-8")
    assert "CREATE ROLE xetra_writer NOLOGIN" in roles_sql
    assert "xetra-data-loader" not in roles_sql


@pytest.mark.parametrize("nullable", ["YES", "NO", None])
def test_weekly_schema_preflight_rejects_missing_or_non_nullable_close(
    nullable: str | None,
) -> None:
    class _Result:
        def fetchone(self) -> tuple[str] | None:
            return None if nullable is None else (nullable,)

    class _Connection:
        committed = False

        def execute(self, *_args: object) -> _Result:
            return _Result()

        def commit(self) -> None:
            self.committed = True

    runtime = object.__new__(PostgresEodhdBootstrapRuntime)
    connection = _Connection()
    runtime._connection = connection
    runtime._serving_schema = "xetra_loader"

    if nullable == "YES":
        runtime.validate_weekly_schema_contract()
        assert connection.committed
    else:
        with pytest.raises(RuntimeError, match="close"):
            runtime.validate_weekly_schema_contract()
        assert not connection.committed
