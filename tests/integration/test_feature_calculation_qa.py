import json
import math
import os
import subprocess
from datetime import UTC, datetime, time
from pathlib import Path
from typing import Any

import pytest
from psycopg import Connection
from tests.qa.feature_reference import FixtureQuote, make_fixture, reference_features

from xetra_loader.features.catalog import feature_columns
from xetra_loader.sync import connect_postgres

DSN = os.getenv("XDL_TEST_POSTGRES_DSN")
pytestmark = pytest.mark.integration


def _apply_sql(path: str) -> None:
    if DSN is None:
        pytest.skip("XDL_TEST_POSTGRES_DSN is not configured")
    subprocess.run(
        ["psql", DSN, "-X", "-v", "ON_ERROR_STOP=1"],
        check=True,
        text=True,
        input=Path(path).read_text(encoding="utf-8"),
        capture_output=True,
    )


def _insert_fixture(connection: Connection[Any], rows: tuple[FixtureQuote, ...]) -> None:
    connection.execute("TRUNCATE xetra_loader.listings CASCADE")
    listings = tuple(sorted({(row.isin, row.code) for row in rows}))
    with connection.cursor() as cursor:
        cursor.executemany(
            "INSERT INTO xetra_loader.listings "
            "(isin, exchange, code, fetched_at_utc, published_at_utc) "
            "VALUES (%s, 'XETRA', %s, now(), now())",
            [(isin, code) for isin, code in listings],
        )
        cursor.executemany(
            "INSERT INTO xetra_loader.eod_quotes "
            "(isin, exchange, code, trade_date, timestamp_eod, open, high, low, close, "
            "adjusted_close, volume, fetched_at_utc, published_at_utc) "
            "VALUES (%s, 'XETRA', %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())",
            [
                (
                    row.isin,
                    row.code,
                    row.trade_date,
                    datetime.combine(row.trade_date, time.min, tzinfo=UTC),
                    row.open,
                    row.high,
                    row.low,
                    row.close,
                    row.adjusted_close,
                    row.volume,
                )
                for row in rows
            ],
        )


def _feature_rows(connection: Connection[Any]) -> dict[tuple[str, object], dict[str, object]]:
    columns = ("code", "trade_date", *feature_columns())
    result = connection.execute(
        "SELECT " + ", ".join(columns) + " FROM xetra_loader.xetra_features "
        "ORDER BY code, trade_date"
    )
    return {
        (str(row[0]), row[1]): dict(zip(columns[2:], row[2:], strict=True))
        for row in result.fetchall()
    }


def _assert_expected(
    actual: dict[tuple[str, object], dict[str, object]],
    expected: dict[tuple[str, object], dict[str, float | None]],
) -> None:
    assert set(actual) == set(expected)
    for identity, expected_features in expected.items():
        for name, expected_value in expected_features.items():
            actual_value = actual[identity][name]
            if expected_value is None:
                assert actual_value is None, (identity, name, actual_value)
            else:
                assert actual_value is not None, (identity, name)
                assert math.isclose(
                    float(actual_value), expected_value, rel_tol=1e-9, abs_tol=1e-9
                ), (identity, name, actual_value, expected_value)


def _write_report(path: Path, checks: dict[str, bool]) -> None:
    report: dict[str, Any] = {
        "work_order": "xdl-pr064-qa-xetra-feature-calculations",
        "status": "PASS" if all(checks.values()) else "FAIL",
        "checks": checks,
        "secrets_included": False,
    }
    path.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def test_feature_calculations_match_independent_reference(tmp_path: Path) -> None:
    if DSN is None:
        pytest.skip("XDL_TEST_POSTGRES_DSN is not configured")
    _apply_sql("sql/schema/001_xetra_loader.sql")
    _apply_sql("sql/schema/002_roles.sql")
    _apply_sql("sql/sync/001_xetra_loader_sync.sql")
    _apply_sql("sql/schema/004_xetra_features.sql")
    fixture = make_fixture()
    expected = reference_features(fixture)
    connection = connect_postgres(DSN)
    try:
        with connection.transaction():
            _insert_fixture(connection, fixture)
            connection.execute("SELECT xetra_loader.refresh_xetra_features()")
        initial = _feature_rows(connection)
        _assert_expected(initial, expected)

        first_date = fixture[0].trade_date
        fifth_date = fixture[4].trade_date
        boundary_nulls = (
            initial[("AAA", first_date)]["adjusted_close_log_return_1obs"] is None
            and initial[("AAA", fifth_date)]["adjusted_close_return_geom_5obs_pct"] is None
            and initial[("AAA", fifth_date)]["breadth_5obs"] is None
            and initial[("AAA", fifth_date)]["average_correlation_5obs"] is None
        )

        stable_identity = ("AAA", fixture[30].trade_date)
        stable_before = initial[stable_identity].copy()
        latest = fixture[-2]
        connection.execute(
            "UPDATE xetra_loader.eod_quotes SET adjusted_close = adjusted_close + 10 "
            "WHERE code = %s AND trade_date = %s",
            (latest.code, latest.trade_date),
        )
        connection.execute("SELECT xetra_loader.refresh_xetra_features()")
        after_adjusted_close = _feature_rows(connection)
        no_lookahead = after_adjusted_close[stable_identity] == stable_before

        connection.execute(
            "UPDATE xetra_loader.eod_quotes SET published_at_utc = published_at_utc "
            "+ interval '1 second' WHERE code = %s AND trade_date = %s",
            (latest.code, latest.trade_date),
        )
        connection.execute("SELECT xetra_loader.refresh_xetra_features()")
        after_metadata_change = _feature_rows(connection)
        irrelevant_field_invariance = after_metadata_change == after_adjusted_close

        repeated_before = after_metadata_change
        connection.execute("SELECT xetra_loader.refresh_xetra_features()")
        repeated_refresh = _feature_rows(connection) == repeated_before
        partitioned = (
            initial[("AAA", fixture[30].trade_date)]["adjusted_close_log_return_1obs"]
            != initial[("BBB", fixture[30].trade_date)]["adjusted_close_log_return_1obs"]
        )
        checks = {
            "all_catalog_features_match_independent_reference": True,
            "boundary_nulls_and_minimum_windows": boundary_nulls,
            "instrument_partitioning": partitioned,
            "no_future_observation_leakage": no_lookahead,
            "irrelevant_metadata_invariance": irrelevant_field_invariance,
            "repeated_refresh_semantic_identity": repeated_refresh,
        }
        report_path = tmp_path / "xdl-pr064-feature-calculations.json"
        _write_report(report_path, checks)
        report = json.loads(report_path.read_text(encoding="utf-8"))
        assert report["status"] == "PASS", json.dumps(report, sort_keys=True)
    finally:
        connection.close()
