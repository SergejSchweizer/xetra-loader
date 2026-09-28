import json
import re
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from xetra_loader.contracts.corporate_actions import (
    DividendEvent,
    SplitEvent,
    retract_dividend,
    retract_split,
)
from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.contracts.quotes import QuoteRecord
from xetra_loader.gold.dividends import build_dividend_gold
from xetra_loader.gold.listings import build_listing_gold
from xetra_loader.gold.quotes import build_quote_gold
from xetra_loader.gold.splits import build_split_gold
from xetra_loader.ops.acceptance import (
    LoaderAcceptanceReport,
    read_scheduler_contract,
    write_acceptance_report,
)
from xetra_loader.ops.bootstrap import (
    BootstrapVerification,
    FetchBatch,
    FetchMetrics,
    run_full_bootstrap,
)
from xetra_loader.pipeline.restart import ConcurrentLoaderRunError, LoaderLock
from xetra_loader.sync.core import SyncCounters, SyncOutcome

pytestmark = pytest.mark.integration


class FixtureRuntime:
    def __init__(self) -> None:
        self.tables: dict[str, dict[tuple[object, ...], dict[str, object]]] = {
            "listings": {},
            "eod_quotes": {},
            "dividends": {},
            "splits": {},
        }
        self.fingerprints: dict[str, str] = {}
        self.listings = (
            ListingRecord(isin="DE0000000001", exchange="XETRA", code="AAA"),
            ListingRecord(isin="DE0000000002", exchange="XETRA", code="BBB"),
        )
        self.dividend_events = {
            listing.code: DividendEvent(
                isin=listing.isin,
                exchange=listing.exchange,
                code=listing.code,
                event_date=date(2026, 8, 20),
                value=Decimal("1.25"),
                currency="EUR",
            )
            for listing in self.listings
        }
        self.split_events = {
            listing.code: SplitEvent(
                isin=listing.isin,
                exchange=listing.exchange,
                code=listing.code,
                event_date=date(2026, 8, 19),
                split_ratio="2:1",
                split_factor=Decimal("2"),
            )
            for listing in self.listings
        }

    def reset_owned_state(self) -> None:
        for table in self.tables.values():
            table.clear()
        self.fingerprints.clear()

    def fetch_listings(self) -> FetchBatch[ListingRecord]:
        return FetchBatch(
            self.listings,
            FetchMetrics(logical_requests=1, attempts=1, rows=len(self.listings)),
        )

    def fetch_quotes(self, listing: ListingRecord) -> FetchBatch[QuoteRecord]:
        row = self.quote(listing, day=21, close="11")
        return FetchBatch((row,), FetchMetrics(logical_requests=1, attempts=1, rows=1))

    def fetch_dividends(self, listing: ListingRecord) -> FetchBatch[DividendEvent]:
        row = self.dividend_events[listing.code]
        return FetchBatch((row,), FetchMetrics(logical_requests=1, attempts=1, rows=1))

    def fetch_splits(self, listing: ListingRecord) -> FetchBatch[SplitEvent]:
        row = self.split_events[listing.code]
        return FetchBatch((row,), FetchMetrics(logical_requests=1, attempts=1, rows=1))

    def persist_gold(
        self,
        dataset: str,
        semantic_rows: object,
        *,
        row_count: int,
        semantic_fingerprint: str,
        retracted_keys: object = (),
    ) -> None:
        del dataset, semantic_rows, row_count, semantic_fingerprint, retracted_keys

    def publish_listings(self, gold: object) -> SyncOutcome:
        pairs = [(tuple(row.key), row.semantic_dict()) for row in gold.rows]
        return self._publish("listings", pairs, gold.semantic_fingerprint, gold.row_count)

    def publish_quotes(self, gold: object, **kwargs: object) -> SyncOutcome:
        del kwargs
        pairs = [(tuple(row.key), row.semantic_dict()) for row in gold.rows]
        return self._publish("eod_quotes", pairs, gold.semantic_fingerprint, gold.row_count)

    def publish_dividends(self, gold: object, **kwargs: object) -> SyncOutcome:
        del kwargs
        pairs = [(tuple(row.key), row.business_fields()) for row in gold.rows]
        return self._publish(
            "dividends",
            pairs,
            gold.semantic_fingerprint,
            gold.row_count,
            retracted=gold.retracted_keys,
        )

    def publish_splits(self, gold: object, **kwargs: object) -> SyncOutcome:
        del kwargs
        pairs = [(tuple(row.key), row.business_fields()) for row in gold.rows]
        return self._publish(
            "splits",
            pairs,
            gold.semantic_fingerprint,
            gold.row_count,
            retracted=gold.retracted_keys,
        )

    def _publish(
        self,
        dataset: str,
        rows: list[tuple[tuple[object, ...], dict[str, object]]],
        fingerprint: str,
        row_count: int,
        *,
        retracted: tuple[tuple[str, str, str, str], ...] = (),
    ) -> SyncOutcome:
        inserted = 0
        updated = 0
        retracted_count = 0
        table = self.tables[dataset]
        for key in retracted:
            if tuple(key) in table:
                del table[tuple(key)]
                retracted_count += 1
        for key, value in rows:
            previous = table.get(key)
            if previous is None:
                inserted += 1
            elif previous != value:
                updated += 1
            table[key] = value
        counters = SyncCounters(
            inserted=inserted,
            updated=updated,
            retracted=retracted_count,
        )
        self.fingerprints[dataset] = fingerprint
        return SyncOutcome(
            run_id=f"fixture-{dataset}",
            dataset=dataset,
            semantic_fingerprint=fingerprint,
            row_count=row_count,
            status="applied" if counters.total_mutations else "noop",
            counters=counters,
        )

    def verify(
        self,
        listing_gold: object,
        quote_gold: object,
        dividend_gold: object,
        split_gold: object,
        sync_outcomes: dict[str, SyncOutcome],
    ) -> BootstrapVerification:
        expected = {
            "listings": listing_gold.row_count,
            "eod_quotes": quote_gold.row_count,
            "dividends": dividend_gold.row_count,
            "splits": split_gold.row_count,
        }
        return BootstrapVerification(
            row_counts={name: (count, len(self.tables[name])) for name, count in expected.items()},
            key_differences={name: (0, 0) for name in expected},
            date_bounds_match={name: True for name in expected},
            sync_state_match={
                name: self.fingerprints.get(name) == outcome.semantic_fingerprint
                for name, outcome in sync_outcomes.items()
            },
        )

    def close(self) -> None:
        pass

    @staticmethod
    def quote(listing: ListingRecord, *, day: int, close: str) -> QuoteRecord:
        return QuoteRecord(
            isin=listing.isin,
            exchange=listing.exchange,
            code=listing.code,
            trade_date=date(2026, 8, day),
            open=Decimal("10"),
            high=Decimal("12"),
            low=Decimal("9"),
            close=Decimal(close),
            adjusted_close=Decimal(close),
            volume=100,
        )


def test_complete_loader_acceptance_matrix(tmp_path: Path) -> None:
    runtime = FixtureRuntime()
    initial = run_full_bootstrap(runtime, confirmed=True, reset_owned_state=True)
    replay = run_full_bootstrap(runtime, confirmed=True, reset_owned_state=False)

    corrected_quote = runtime.quote(runtime.listings[0], day=21, close="11.5")
    new_quote = runtime.quote(runtime.listings[0], day=22, close="12")
    quote_change = runtime.publish_quotes(build_quote_gold([corrected_quote, new_quote]))

    old_dividend = runtime.dividend_events["AAA"]
    new_dividend = DividendEvent(
        isin=old_dividend.isin,
        exchange=old_dividend.exchange,
        code=old_dividend.code,
        event_date=old_dividend.event_date,
        value=Decimal("1.30"),
        currency=old_dividend.currency,
    )
    dividend_change = runtime.publish_dividends(
        build_dividend_gold([new_dividend, retract_dividend(old_dividend)])
    )

    old_split = runtime.split_events["AAA"]
    new_split = SplitEvent(
        isin=old_split.isin,
        exchange=old_split.exchange,
        code=old_split.code,
        event_date=old_split.event_date,
        split_ratio="3:1",
        split_factor=Decimal("3"),
    )
    split_change = runtime.publish_splits(
        build_split_gold([new_split, retract_split(old_split)])
    )

    new_listing = ListingRecord(isin="DE0000000003", exchange="XETRA", code="CCC")
    listing_change = runtime.publish_listings(
        build_listing_gold([*runtime.listings, new_listing])
    )

    timestamp_contract = _timestamp_contract_is_exact()
    role_contract = _app_role_is_select_only()
    lock_contract = _lock_contract_holds(tmp_path)
    scheduler_timezone, scheduler_expression = read_scheduler_contract()
    scheduler_contract = (
        scheduler_timezone == "Europe/Vienna" and scheduler_expression == "0 8 * * 0"
    )
    portfell_imports = _count_portfell_imports()

    scenarios = {
        "empty_state_full_bootstrap": initial.verification.passed,
        "all_valid_listings_and_histories": initial.fetch_metrics.rows == 8,
        "unchanged_replay_zero_mutations": all(
            outcome.counters.total_mutations == 0 for outcome in replay.sync_outcomes.values()
        ),
        "quote_correction": quote_change.counters.updated == 1,
        "quote_new_date": quote_change.counters.inserted == 1,
        "dividend_correction_retraction": (
            dividend_change.counters.inserted == 1
            and dividend_change.counters.retracted == 1
        ),
        "split_correction_retraction": (
            split_change.counters.inserted == 1 and split_change.counters.retracted == 1
        ),
        "new_listing": listing_change.counters.inserted == 1,
        "timestamps_and_utc": timestamp_contract,
        "read_only_app_role": role_contract,
        "concurrent_lock": lock_contract,
        "sunday_vienna_schedule": scheduler_contract,
    }
    report = LoaderAcceptanceReport(
        scenarios=scenarios,
        tables=("listings", "eod_quotes", "dividends", "splits"),
        timestamp_type="TIMESTAMPTZ(6)",
        database_timezone="UTC",
        app_role="portfell_app",
        app_role_select_only=role_contract,
        scheduler_timezone=scheduler_timezone,
        scheduler_expression=scheduler_expression,
        portfell_imports=portfell_imports,
    )
    assert report.passed

    generated = write_acceptance_report(report, tmp_path / "loader-e2e.json")
    committed = Path("artifacts/acceptance/loader-e2e.json")
    assert json.loads(generated.read_text(encoding="utf-8")) == json.loads(
        committed.read_text(encoding="utf-8")
    )


def _timestamp_contract_is_exact() -> bool:
    market = Path("sql/schema/001_xetra_loader.sql").read_text(encoding="utf-8")
    sync = Path("sql/sync/001_xetra_loader_sync.sql").read_text(encoding="utf-8")
    required = (
        "fetched_at_utc TIMESTAMPTZ(6) NOT NULL",
        "published_at_utc TIMESTAMPTZ(6) NOT NULL",
        "timestamp_eod TIMESTAMPTZ(6) NOT NULL",
        "synced_at_utc TIMESTAMPTZ(6) NOT NULL",
        "started_at_utc TIMESTAMPTZ(6) NOT NULL",
        "finished_at_utc TIMESTAMPTZ(6) NOT NULL",
    )
    return (
        all(token in market or token in sync for token in required)
        and "SET LOCAL TIME ZONE 'UTC'" in sync
        and "trade_date::timestamp AT TIME ZONE 'UTC'" in market
    )


def _app_role_is_select_only() -> bool:
    roles = Path("sql/schema/002_roles.sql").read_text(encoding="utf-8")
    return (
        "GRANT SELECT ON ALL TABLES IN SCHEMA xetra_loader TO portfell_app" in roles
        and "REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER" in roles
        and "REVOKE ALL ON SCHEMA xetra_loader_sync FROM portfell_app" in roles
    )


def _lock_contract_holds(tmp_path: Path) -> bool:
    lock_path = tmp_path / "acceptance.lock"
    try:
        with (
            LoaderLock(lock_path),
            pytest.raises(ConcurrentLoaderRunError),
            LoaderLock(lock_path),
        ):
            raise AssertionError("unreachable")
    except AssertionError:
        return False
    with LoaderLock(lock_path):
        pass
    return True


def _count_portfell_imports() -> int:
    pattern = re.compile(r"^\s*(?:from|import)\s+portfell\b", re.MULTILINE)
    return sum(
        len(pattern.findall(path.read_text(encoding="utf-8")))
        for path in Path("src/xetra_loader").rglob("*.py")
    )
