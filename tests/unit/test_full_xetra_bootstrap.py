from datetime import date
from decimal import Decimal

import pytest

from xetra_loader.contracts.corporate_actions import DividendEvent, SplitEvent
from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.contracts.quotes import QuoteRecord
from xetra_loader.ops.bootstrap import (
    BootstrapVerification,
    DestructiveConfirmationRequired,
    FetchBatch,
    FetchMetrics,
    run_full_bootstrap,
)
from xetra_loader.sync.core import SyncCounters, SyncOutcome


class FixtureRuntime:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.fingerprints: dict[str, str] = {}
        self.persisted: dict[str, int] = {}
        self.listings = (
            ListingRecord(isin="DE0000000001", exchange="XETRA", code="AAA"),
            ListingRecord(isin="DE0000000002", exchange="XETRA", code="BBB"),
        )

    def reset_owned_state(self) -> None:
        self.calls.append("reset")
        self.fingerprints.clear()
        self.persisted.clear()

    def fetch_listings(self) -> FetchBatch[ListingRecord]:
        self.calls.append("fetch-listings")
        return FetchBatch(
            self.listings,
            FetchMetrics(logical_requests=1, attempts=1, rows=2),
        )

    def fetch_quotes(self, listing: ListingRecord) -> FetchBatch[QuoteRecord]:
        self.calls.append(f"fetch-quotes:{listing.code}")
        row = QuoteRecord(
            isin=listing.isin,
            exchange=listing.exchange,
            code=listing.code,
            trade_date=date(2026, 8, 21),
            open=Decimal("10"),
            high=Decimal("12"),
            low=Decimal("9"),
            close=Decimal("11"),
            adjusted_close=Decimal("11"),
            volume=100,
        )
        metrics = (
            FetchMetrics(logical_requests=1, attempts=2, retries=1, rows=1)
            if listing.code == "AAA"
            else FetchMetrics(logical_requests=1, attempts=1, rows=1)
        )
        return FetchBatch((row,), metrics)

    def fetch_dividends(self, listing: ListingRecord) -> FetchBatch[DividendEvent]:
        self.calls.append(f"fetch-dividends:{listing.code}")
        row = DividendEvent(
            isin=listing.isin,
            exchange=listing.exchange,
            code=listing.code,
            event_date=date(2026, 8, 20),
            value=Decimal("1.25"),
            currency="EUR",
        )
        return FetchBatch(
            (row,),
            FetchMetrics(logical_requests=1, attempts=1, rows=1),
        )

    def fetch_splits(self, listing: ListingRecord) -> FetchBatch[SplitEvent]:
        self.calls.append(f"fetch-splits:{listing.code}")
        row = SplitEvent(
            isin=listing.isin,
            exchange=listing.exchange,
            code=listing.code,
            event_date=date(2026, 8, 19),
            split_ratio="2:1",
            split_factor=Decimal("2"),
        )
        return FetchBatch(
            (row,),
            FetchMetrics(logical_requests=1, attempts=1, rows=1),
        )

    def persist_gold(
        self,
        dataset: str,
        semantic_rows: object,
        *,
        row_count: int,
        semantic_fingerprint: str,
        retracted_keys: object = (),
    ) -> None:
        del semantic_rows, semantic_fingerprint, retracted_keys
        self.calls.append(f"persist:{dataset}")
        self.persisted[dataset] = row_count

    def _publish(self, dataset: str, fingerprint: str, row_count: int) -> SyncOutcome:
        self.calls.append(f"publish:{dataset}")
        previous = self.fingerprints.get(dataset)
        changed = previous != fingerprint
        self.fingerprints[dataset] = fingerprint
        return SyncOutcome(
            run_id=f"fixture-{dataset}",
            dataset=dataset,
            semantic_fingerprint=fingerprint,
            row_count=row_count,
            status="applied" if changed else "noop",
            counters=SyncCounters(inserted=row_count if changed else 0),
        )

    def publish_listings(self, gold: object) -> SyncOutcome:
        return self._publish("listings", gold.semantic_fingerprint, gold.row_count)

    def publish_quotes(self, gold: object, **kwargs: object) -> SyncOutcome:
        del kwargs
        return self._publish("eod_quotes", gold.semantic_fingerprint, gold.row_count)

    def publish_dividends(self, gold: object, **kwargs: object) -> SyncOutcome:
        del kwargs
        return self._publish("dividends", gold.semantic_fingerprint, gold.row_count)

    def publish_splits(self, gold: object, **kwargs: object) -> SyncOutcome:
        del kwargs
        return self._publish("splits", gold.semantic_fingerprint, gold.row_count)

    def verify(
        self,
        listing_gold: object,
        quote_gold: object,
        dividend_gold: object,
        split_gold: object,
        sync_outcomes: object,
    ) -> BootstrapVerification:
        del sync_outcomes
        self.calls.append("verify")
        expected = {
            "listings": listing_gold.row_count,
            "eod_quotes": quote_gold.row_count,
            "dividends": dividend_gold.row_count,
            "splits": split_gold.row_count,
        }
        return BootstrapVerification(
            row_counts={name: (count, self.persisted[name]) for name, count in expected.items()},
            key_differences={name: (0, 0) for name in expected},
            date_bounds_match={name: True for name in expected},
            sync_state_match={name: True for name in expected},
        )

    def close(self) -> None:
        self.calls.append("close")


def test_absent_confirmation_performs_zero_runtime_calls() -> None:
    runtime = FixtureRuntime()

    with pytest.raises(DestructiveConfirmationRequired):
        run_full_bootstrap(runtime, confirmed=False)

    assert runtime.calls == []


def test_clean_full_universe_bootstrap_then_unchanged_replay_is_noop() -> None:
    runtime = FixtureRuntime()

    first = run_full_bootstrap(runtime, confirmed=True, reset_owned_state=True)

    assert first.verification.passed
    assert first.listing_gold.row_count == 2
    assert first.quote_gold.row_count == 2
    assert first.dividend_gold.row_count == 2
    assert first.split_gold.row_count == 2
    assert first.fetch_metrics.logical_requests == 7
    assert first.fetch_metrics.attempts == 8
    assert first.fetch_metrics.retries == 1
    assert first.fetch_metrics.rows == 8
    assert runtime.calls[0] == "reset"
    assert runtime.calls.count("verify") == 1
    assert all(outcome.counters.total_mutations > 0 for outcome in first.sync_outcomes.values())

    runtime.calls.clear()
    replay = run_full_bootstrap(runtime, confirmed=True, reset_owned_state=False)

    assert replay.verification.passed
    assert "reset" not in runtime.calls
    assert all(outcome.status == "noop" for outcome in replay.sync_outcomes.values())
    assert all(outcome.counters.total_mutations == 0 for outcome in replay.sync_outcomes.values())
