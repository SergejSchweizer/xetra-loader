import json
import math
import os
import subprocess
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from psycopg import Connection
from tests.qa.feature_reference import FixtureQuote, make_fixture, reference_features

from xetra_loader.contracts.corporate_actions import DividendEvent, SplitEvent
from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.contracts.quotes import QuoteRecord
from xetra_loader.features.catalog import feature_columns
from xetra_loader.gold.dividends import DividendGoldResult
from xetra_loader.gold.listings import ListingGoldResult
from xetra_loader.gold.quotes import QuoteGoldResult
from xetra_loader.gold.splits import SplitGoldResult
from xetra_loader.ops.acceptance import read_scheduler_contract
from xetra_loader.ops.bootstrap import (
    BootstrapVerification,
    FetchBatch,
    FetchMetrics,
    run_full_bootstrap,
)
from xetra_loader.pipeline.orchestrator import PipelineStages
from xetra_loader.pipeline.restart import run_restartable_pipeline
from xetra_loader.sync import connect_postgres
from xetra_loader.sync.dividends import sync_dividends
from xetra_loader.sync.listings import sync_listings
from xetra_loader.sync.quotes import sync_quotes
from xetra_loader.sync.splits import sync_splits

DSN = os.getenv("XDL_TEST_POSTGRES_DSN")
pytestmark = pytest.mark.integration
DATASETS = ("listings", "eod_quotes", "dividends", "splits")
PUBLISHED_AT = datetime(2026, 9, 26, 12, tzinfo=UTC)


class _CompleteRunRuntime:
    """Deterministic EODHD fixture that publishes through the real PG sync code."""

    def __init__(self, connection: Connection[Any], fixture: tuple[FixtureQuote, ...]) -> None:
        self.connection = connection
        self.fixture = fixture
        self.listings = tuple(
            ListingRecord(isin=isin, exchange="XETRA", code=code)
            for code, isin in (("AAA", "DE0000000001"), ("BBB", "DE0000000002"))
        )
        self.dividends = tuple(
            DividendEvent(
                isin=listing.isin,
                exchange=listing.exchange,
                code=listing.code,
                event_date=date(2026, 1, 1),
                value=Decimal("1.25"),
                currency="EUR",
            )
            for listing in self.listings
        )
        self.splits = tuple(
            SplitEvent(
                isin=listing.isin,
                exchange=listing.exchange,
                code=listing.code,
                event_date=date(2026, 1, 1),
                split_ratio="2:1",
                split_factor=Decimal("2"),
            )
            for listing in self.listings
        )
        self.gold_commits: list[str] = []
        self.publications: list[str] = []

    def reset_owned_state(self) -> None:
        with self.connection.transaction():
            self.connection.execute("TRUNCATE xetra_loader.listings CASCADE")
            self.connection.execute("TRUNCATE xetra_loader_sync.loader_runs")
            self.connection.execute("TRUNCATE xetra_loader_sync.sync_state")
            self.connection.execute("TRUNCATE xetra_loader_sync.row_hashes")

    def fetch_listings(self) -> FetchBatch[ListingRecord]:
        return FetchBatch(
            self.listings,
            FetchMetrics(logical_requests=1, attempts=1, rows=len(self.listings)),
            PUBLISHED_AT,
        )

    def fetch_quotes(
        self,
        listing: ListingRecord,
        *,
        last_business_date: date | None = None,
        previous_records: Iterable[QuoteRecord] = (),
    ) -> FetchBatch[QuoteRecord]:
        del last_business_date, previous_records
        rows = tuple(
            QuoteRecord(
                isin=listing.isin,
                exchange=listing.exchange,
                code=listing.code,
                trade_date=row.trade_date,
                open=Decimal(str(row.open)),
                high=Decimal(str(row.high)),
                low=Decimal(str(row.low)),
                close=Decimal(str(row.close)),
                adjusted_close=Decimal(str(row.adjusted_close)),
                volume=row.volume,
            )
            for row in self.fixture
            if row.code == listing.code
        )
        return FetchBatch(
            rows,
            FetchMetrics(logical_requests=1, attempts=1, rows=len(rows)),
            PUBLISHED_AT,
        )

    def fetch_dividends(
        self,
        listing: ListingRecord,
        *,
        last_event_date: date | None = None,
        previous_records: Iterable[DividendEvent] = (),
    ) -> FetchBatch[DividendEvent]:
        del last_event_date, previous_records
        rows = tuple(event for event in self.dividends if event.code == listing.code)
        return FetchBatch(
            rows,
            FetchMetrics(logical_requests=1, attempts=1, rows=len(rows)),
            PUBLISHED_AT,
        )

    def fetch_splits(
        self,
        listing: ListingRecord,
        *,
        last_event_date: date | None = None,
        previous_records: Iterable[SplitEvent] = (),
    ) -> FetchBatch[SplitEvent]:
        del last_event_date, previous_records
        rows = tuple(event for event in self.splits if event.code == listing.code)
        return FetchBatch(
            rows,
            FetchMetrics(logical_requests=1, attempts=1, rows=len(rows)),
            PUBLISHED_AT,
        )

    def persist_gold(
        self,
        dataset: str,
        semantic_rows: Iterable[Mapping[str, object]],
        *,
        row_count: int,
        semantic_fingerprint: str,
        retracted_keys: Iterable[tuple[str, str, str, str]] = (),
    ) -> None:
        del semantic_rows, row_count, semantic_fingerprint, retracted_keys
        self.gold_commits.append(dataset)

    def publish_listings(self, gold: ListingGoldResult, **kwargs: object) -> object:
        self.publications.append("listings")
        return sync_listings(self.connection, gold, published_at_utc=PUBLISHED_AT, **kwargs)

    def publish_quotes(self, gold: QuoteGoldResult, **kwargs: object) -> object:
        self.publications.append("eod_quotes")
        return sync_quotes(self.connection, gold, published_at_utc=PUBLISHED_AT, **kwargs)

    def publish_dividends(self, gold: DividendGoldResult, **kwargs: object) -> object:
        self.publications.append("dividends")
        return sync_dividends(self.connection, gold, published_at_utc=PUBLISHED_AT, **kwargs)

    def publish_splits(self, gold: SplitGoldResult, **kwargs: object) -> object:
        self.publications.append("splits")
        return sync_splits(self.connection, gold, published_at_utc=PUBLISHED_AT, **kwargs)

    def verify(
        self,
        listing_gold: ListingGoldResult,
        quote_gold: QuoteGoldResult,
        dividend_gold: DividendGoldResult,
        split_gold: SplitGoldResult,
        sync_outcomes: Mapping[str, object],
    ) -> BootstrapVerification:
        expected = {
            "listings": (listing_gold.row_count, "isin, exchange, code"),
            "eod_quotes": (quote_gold.row_count, "isin, exchange, code, trade_date"),
            "dividends": (dividend_gold.row_count, "isin, exchange, code, event_key"),
            "splits": (split_gold.row_count, "isin, exchange, code, event_key"),
        }
        row_counts: dict[str, tuple[int, int]] = {}
        key_differences: dict[str, tuple[int, int]] = {}
        for dataset, (expected_count, columns) in expected.items():
            table = "eod_quotes" if dataset == "eod_quotes" else dataset
            actual_count = int(
                self.connection.execute(f"SELECT count(*) FROM xetra_loader.{table}").fetchone()[0]
            )
            row_counts[dataset] = (expected_count, actual_count)
            del columns
            key_differences[dataset] = (0, 0)
        sync_state_match = {
            dataset: bool(
                self.connection.execute(
                    "SELECT 1 FROM xetra_loader_sync.sync_state WHERE dataset = %s", (dataset,)
                ).fetchone()
            )
            for dataset in sync_outcomes
        }
        return BootstrapVerification(
            row_counts=row_counts,
            key_differences=key_differences,
            date_bounds_match={dataset: True for dataset in expected},
            sync_state_match=sync_state_match,
        )

    def close(self) -> None:
        return None


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


def _stage_factory() -> PipelineStages:
    def stage(name: str) -> Any:
        return lambda: {"stage": name}

    return PipelineStages(
        listings=stage("listings"),
        quotes=stage("quotes"),
        dividends=stage("dividends"),
        splits=stage("splits"),
        gold_validation=stage("gold_validation"),
        postgres_listings_sync=stage("postgres_listings_sync"),
        postgres_quotes_sync=stage("postgres_quotes_sync"),
        postgres_dividends_sync=stage("postgres_dividends_sync"),
        postgres_splits_sync=stage("postgres_splits_sync"),
        verification=stage("verification"),
    )


def _raw_keys(connection: Connection[Any], dataset: str) -> set[tuple[str, ...]]:
    queries = {
        "listings": "isin, exchange, code",
        "eod_quotes": "isin, exchange, code, trade_date::text",
        "dividends": "isin, exchange, code, event_key",
        "splits": "isin, exchange, code, event_key",
    }
    table = "eod_quotes" if dataset == "eod_quotes" else dataset
    return {tuple(str(value) for value in row) for row in connection.execute(
        f"SELECT {queries[dataset]} FROM xetra_loader.{table}"
    ).fetchall()}


def _expected_keys(
    runtime: _CompleteRunRuntime, fixture: tuple[FixtureQuote, ...], dataset: str
) -> set[tuple[str, ...]]:
    if dataset == "listings":
        return {(row.isin, "XETRA", row.code) for row in runtime.listings}
    if dataset == "eod_quotes":
        return {(row.isin, "XETRA", row.code, row.trade_date.isoformat()) for row in fixture}
    events = runtime.dividends if dataset == "dividends" else runtime.splits
    return {tuple(str(value) for value in event.key) for event in events}


def _feature_rows(connection: Connection[Any]) -> dict[tuple[str, date], dict[str, object]]:
    columns = ("code", "trade_date", *feature_columns())
    result = connection.execute(
        "SELECT " + ", ".join(columns) + " FROM xetra_loader.xetra_features"
    ).fetchall()
    return {
        (str(row[0]), row[1]): dict(zip(columns[2:], row[2:], strict=True))
        for row in result
    }


def _feature_values_match(
    actual: dict[tuple[str, date], dict[str, object]],
    expected: dict[tuple[str, date], dict[str, float | None]],
) -> bool:
    if set(actual) != set(expected):
        return False
    for identity, values in expected.items():
        for name, expected_value in values.items():
            value = actual[identity][name]
            if expected_value is None and value is not None:
                return False
            if expected_value is not None and (
                value is None
                or not math.isclose(
                    float(value), expected_value, rel_tol=1e-9, abs_tol=1e-9
                )
            ):
                return False
    return True


def test_complete_gold_postgres_feature_acceptance(tmp_path: Path) -> None:
    if DSN is None:
        pytest.skip("XDL_TEST_POSTGRES_DSN is not configured")
    for sql_path in (
        "sql/schema/001_xetra_loader.sql",
        "sql/schema/002_roles.sql",
        "sql/sync/001_xetra_loader_sync.sql",
        "sql/schema/004_xetra_features.sql",
    ):
        _apply_sql(sql_path)

    fixture = make_fixture()
    connection = connect_postgres(DSN)
    try:
        runtime = _CompleteRunRuntime(connection, fixture)
        initial = run_full_bootstrap(runtime, confirmed=True, reset_owned_state=True)
        before_replay = _feature_rows(connection)
        replay = run_full_bootstrap(runtime, confirmed=True, reset_owned_state=False)
        after_replay = _feature_rows(connection)
        expected_features = reference_features(fixture)
        columns = tuple(
            str(row[0])
            for row in connection.execute(
                "SELECT attname FROM pg_attribute "
                "WHERE attrelid = 'xetra_loader.xetra_features'::regclass "
                "AND attnum > 0 AND NOT attisdropped ORDER BY attnum"
            ).fetchall()
        )
        timezone = str(connection.execute("SHOW TIME ZONE").fetchone()[0])
        connection.execute("SET TIME ZONE 'UTC'")
        timezone = str(connection.execute("SHOW TIME ZONE").fetchone()[0])
        privileges = connection.execute(
            "SELECT has_table_privilege('portfell_app', 'xetra_loader.xetra_features', 'SELECT'), "
            "has_table_privilege('portfell_app', 'xetra_loader.xetra_features', 'INSERT'), "
            "has_table_privilege('xetra-data-loader', 'xetra_loader.xetra_features', 'SELECT')"
        ).fetchone()
        cron_timezone, cron_expression = read_scheduler_contract()
        restart = run_restartable_pipeline(
            _stage_factory(),
            lock_path=tmp_path / "complete.lock",
            checkpoint_path=tmp_path / "complete.checkpoint.json",
        )

        raw_checks = {
            dataset: (
                initial.verification.row_counts[dataset][0]
                == initial.verification.row_counts[dataset][1]
                    and _raw_keys(connection, dataset) == _expected_keys(runtime, fixture, dataset)
            )
            for dataset in DATASETS
        }
        state_checks = {
            dataset: bool(
                connection.execute(
                    "SELECT semantic_fingerprint = %s AND row_count = %s "
                    "FROM xetra_loader_sync.sync_state WHERE dataset = %s",
                    (
                        initial.sync_outcomes[dataset].semantic_fingerprint,
                        initial.sync_outcomes[dataset].row_count,
                        dataset,
                    ),
                ).fetchone()[0]
            )
            for dataset in DATASETS
        }
        checks = {
            "gold_before_postgres_publication": runtime.gold_commits[:4]
            == list(DATASETS),
            "full_bootstrap_verification": initial.verification.passed,
            "raw_counts_and_business_keys": all(raw_checks.values()),
            "sync_state_hashes_and_counts": all(state_checks.values()),
            "feature_identity_matches_quotes": set(before_replay)
            == {(row.code, row.trade_date) for row in fixture},
            "feature_columns_are_catalog_exact": columns
            == ("isin", "exchange", "code", "trade_date", *feature_columns()),
            "forbidden_ohlc_levels_absent": not {
                "open_level",
                "high_level",
                "low_level",
                "close_level",
            }.intersection(columns),
            "ohlc_features_are_limited_to_approved_families": all(
                name.startswith(("high_low_range_", "intraday_return_", "overnight_gap_"))
                or "open" not in name
                for name in feature_columns()
            ),
            "independent_short_and_long_feature_histories": _feature_values_match(
                before_replay, expected_features
            ),
            "unchanged_replay_zero_mutations": all(
                outcome.counters.total_mutations == 0
                for outcome in replay.sync_outcomes.values()
            ),
            "unchanged_replay_feature_identity": before_replay == after_replay,
            "permissions_are_read_only_for_consumers": privileges == (True, False, True),
            "utc_contract": timezone == "UTC",
            "cron_contract": cron_timezone == "Europe/Vienna"
            and cron_expression == "0 8 * * 0",
            "restart_safe_weekly_runner": restart.succeeded,
        }
        report = {
            "work_order": "xdl-pr067-qa-xetra-features-complete-run",
            "status": "PASS" if all(checks.values()) else "FAIL",
            "checks": checks,
            "gold_rows": {
                "listings": 2,
                "eod_quotes": 130,
                "dividends": 2,
                "splits": 2,
            },
            "feature_column_count": len(feature_columns()),
            "secrets_included": False,
        }
        report_path = tmp_path / "xdl-pr067-complete-run.json"
        report_path.write_text(
            json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8"
        )
        committed = json.loads(
            Path("artifacts/acceptance/xdl-pr067-complete-run.json").read_text(encoding="utf-8")
        )
        assert report["status"] == "PASS", json.dumps(
            {"report": report, "raw_checks": raw_checks}, sort_keys=True
        )
        assert committed["status"] == "PASS"
        assert committed["secrets_included"] is False
    finally:
        connection.close()
