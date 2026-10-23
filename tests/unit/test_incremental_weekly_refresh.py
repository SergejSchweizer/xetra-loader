"""Weekly refresh uses quote deltas except after corporate-action changes."""

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from threading import Barrier, Lock
from typing import cast

import pytest

from xetra_loader.contracts.corporate_actions import (
    DividendEvent,
    SplitEvent,
    retract_dividend,
)
from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.contracts.quotes import QuoteRecord, overlap_start
from xetra_loader.gold.listings import build_listing_gold
from xetra_loader.medallion.core import MedallionLayout
from xetra_loader.ops.bootstrap import (
    FetchBatch,
    FetchMetrics,
    PostgresEodhdBootstrapRuntime,
)
from xetra_loader.pipeline.runtime import (
    _corporate_fingerprint,
    _dividend_silver_row,
    _fetch_times_from_checkpoint,
    _parallel_database_core_budgets,
    _parallel_fetch,
    _QuoteFetchTimeIndex,
    _replace_action_window,
    _split_silver_row,
    _WeeklyState,
    build_weekly_stages,
)


def _quote(day: int, close: str) -> QuoteRecord:
    return QuoteRecord(
        isin="DE0000000001",
        exchange="XETRA",
        code="AAA",
        trade_date=date(2026, 8, day),
        open=Decimal(close),
        high=Decimal(close),
        low=Decimal(close),
        close=Decimal(close),
        adjusted_close=Decimal(close),
        volume=100,
    )


def _dividend(day: int, value: str = "1.25") -> DividendEvent:
    return DividendEvent(
        isin="DE0000000001",
        exchange="XETRA",
        code="AAA",
        event_date=date(2026, 8, day),
        value=Decimal(value),
        currency="EUR",
    )


def _split(day: int) -> SplitEvent:
    return SplitEvent(
        isin="DE0000000001",
        exchange="XETRA",
        code="AAA",
        event_date=date(2026, 8, day),
        split_ratio="2:1",
        split_factor=Decimal("2"),
    )


def test_quote_fetch_checkpoint_compresses_per_row_times_into_listing_windows() -> None:
    fetched_at = datetime(2026, 10, 23, 17, tzinfo=UTC)
    index = _QuoteFetchTimeIndex()
    first = _quote(3, "10")
    last = _quote(7, "11")
    index.record_batch((first, last), fetched_at)

    serialized = index.checkpoint_rows()
    restored = _fetch_times_from_checkpoint(
        "eod_quotes",
        {"fetched_at_by_window": serialized},
    )
    fallback = datetime(2026, 10, 24, tzinfo=UTC)

    assert len(serialized) == 1
    assert isinstance(restored, _QuoteFetchTimeIndex)
    assert restored.get(first.key, fallback) == fetched_at
    assert restored.get(last.key, fallback) == fetched_at
    assert restored.get((*first.key[:3], date(2026, 8, 8)), fallback) == fallback


def test_legacy_quote_checkpoint_rows_restore_as_compact_windows() -> None:
    fetched_at = "2026-10-23T17:00:00+00:00"
    details = {
        "fetched_at_by_key": [
            {
                "key": ["DE0000000001", "XETRA", "AAA", "2026-08-03"],
                "fetched_at_utc": fetched_at,
            },
            {
                "key": ["DE0000000001", "XETRA", "AAA", "2026-08-07"],
                "fetched_at_utc": fetched_at,
            },
        ]
    }

    restored = _fetch_times_from_checkpoint("eod_quotes", details)

    assert isinstance(restored, _QuoteFetchTimeIndex)
    assert len(restored.checkpoint_rows()) == 1
    assert restored.get(_quote(3, "10").key, datetime.min.replace(tzinfo=UTC)).isoformat() == (
        fetched_at
    )


def test_provider_fetch_streams_with_core_bounded_concurrency() -> None:
    cores = 4
    barrier = Barrier(cores)
    guard = Lock()
    active = 0
    peak_active = 0

    class Worker:
        def close(self) -> None:
            return None

    class Runtime:
        def fetch_worker(self) -> Worker:
            return Worker()

    def fetch(worker: PostgresEodhdBootstrapRuntime, job: int) -> FetchBatch[int]:
        nonlocal active, peak_active
        del worker
        with guard:
            active += 1
            peak_active = max(peak_active, active)
        try:
            barrier.wait(timeout=5)
            return FetchBatch((job,), FetchMetrics())
        finally:
            with guard:
                active -= 1

    completed = list(
        _parallel_fetch(
            cast(PostgresEodhdBootstrapRuntime, Runtime()),
            list(range(8)),
            fetch,
            cores,
        )
    )

    assert {job for job, _ in completed} == set(range(8))
    assert all(batch.rows == (job,) for job, batch in completed)
    assert peak_active == cores


class _Runtime:
    def __init__(self, root: Path) -> None:
        self._layout = MedallionLayout(root)
        self.calls: list[tuple[date | None, tuple[QuoteRecord, ...]]] = []
        self.action_calls: list[tuple[str, date | None, tuple[object, ...]]] = []
        self.persisted_silver: list[dict[str, object]] = []
        self.full_snapshot = (
            _quote(14, "10"),
            _quote(18, "11"),
            _quote(20, "12"),
            _quote(22, "13"),
        )
        self.dividend_snapshot = (_dividend(10),)
        self.split_snapshot = (_split(10),)
        self.persisted_silver_by_dataset: dict[str, list[dict[str, object]]] = {}
        self.persistence_events: list[tuple[str, str]] = []

    def fetch_listings(self) -> FetchBatch[ListingRecord]:
        rows = (ListingRecord("DE0000000001", "XETRA", "AAA"),)
        return FetchBatch(
            rows,
            FetchMetrics(logical_requests=2, attempts=2, rows=1),
            datetime(2026, 8, 23, 10, 0, tzinfo=UTC),
        )

    def fetch_quotes(
        self,
        listing: ListingRecord,
        *,
        last_business_date: date | None = None,
        previous_records: tuple[QuoteRecord, ...] = (),
    ) -> FetchBatch[QuoteRecord]:
        self.calls.append((last_business_date, previous_records))
        rows = tuple(
            row
            for row in self.full_snapshot
            if last_business_date is None or row.trade_date >= overlap_start(last_business_date)
        )
        return FetchBatch(
            rows,
            FetchMetrics(logical_requests=1, attempts=1, rows=len(rows)),
            datetime(2026, 8, 23, 10, 0, tzinfo=UTC),
        )

    def fetch_dividends(
        self,
        listing: ListingRecord,
        *,
        last_event_date: date | None = None,
        previous_records: tuple[DividendEvent, ...] = (),
    ) -> FetchBatch[DividendEvent]:
        del listing
        self.action_calls.append(("dividends", last_event_date, previous_records))
        return FetchBatch(
            self.dividend_snapshot,
            FetchMetrics(logical_requests=1, attempts=1, rows=1),
            datetime(2026, 8, 23, 10, 0, tzinfo=UTC),
        )

    def fetch_splits(
        self,
        listing: ListingRecord,
        *,
        last_event_date: date | None = None,
        previous_records: tuple[SplitEvent, ...] = (),
    ) -> FetchBatch[SplitEvent]:
        del listing
        self.action_calls.append(("splits", last_event_date, previous_records))
        return FetchBatch(
            self.split_snapshot,
            FetchMetrics(logical_requests=1, attempts=1, rows=1),
            datetime(2026, 8, 23, 10, 0, tzinfo=UTC),
        )

    def persist_gold(self, *args: object, **kwargs: object) -> None:
        del kwargs
        self.persistence_events.append(("gold", str(args[0])))

    def persist_silver(
        self,
        dataset: str,
        rows: list[dict[str, object]],
        *,
        durable_sync: bool = True,
    ) -> None:
        del durable_sync
        self.persistence_events.append(("silver", dataset))
        self.persisted_silver_by_dataset[dataset] = rows
        if dataset == "eod_quotes":
            self.persisted_silver = rows


def _seed_silver(root: Path, records: tuple[QuoteRecord, ...]) -> None:
    writer = object.__new__(PostgresEodhdBootstrapRuntime)
    writer._layout = MedallionLayout(root)
    writer.persist_silver(
        "eod_quotes",
        [record.semantic_dict() for record in records],
    )


def _seed_silver_rows(root: Path, dataset: str, rows: list[dict[str, object]]) -> None:
    writer = object.__new__(PostgresEodhdBootstrapRuntime)
    writer._layout = MedallionLayout(root)
    writer.persist_silver(dataset, rows)  # type: ignore[arg-type]


def test_corporate_fingerprint_is_independent_of_partition_read_order() -> None:
    first = {"isin": "A", "exchange": "XETRA", "code": "AAA", "event_key": "1"}
    second = {"isin": "B", "exchange": "XETRA", "code": "BBB", "event_key": "2"}

    assert _corporate_fingerprint([first, second], ()) == _corporate_fingerprint(
        [second, first], ()
    )


def test_weekly_quote_refresh_uses_delta_without_split(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path)
    old_history = (_quote(14, "10"), _quote(18, "11"), _quote(20, "10"), _quote(22, "12"))
    _seed_silver(tmp_path, old_history)
    state = _WeeklyState(runtime)  # type: ignore[arg-type]
    state.listings = build_listing_gold([ListingRecord("DE0000000001", "XETRA", "AAA")])

    details = state.ingest_quotes()

    assert runtime.calls[0][0] == date(2026, 8, 22)
    assert [record.trade_date for record in runtime.calls[0][1]] == [
        date(2026, 8, 14),
        date(2026, 8, 18),
        date(2026, 8, 20),
        date(2026, 8, 22),
    ]
    assert [row["trade_date"] for row in runtime.persisted_silver] == [
        "2026-08-14",
        "2026-08-18",
        "2026-08-20",
        "2026-08-22",
    ]
    assert runtime.persisted_silver[2]["close"] == "12"
    assert runtime.persistence_events == [
        ("silver", "eod_quotes"),
        ("gold", "eod_quotes"),
    ]
    assert details["requests"] == 1
    assert details["source_fetch_mode"] == "delta"
    assert details["publication_mode"] == "semantic_delta"


def test_new_listing_receives_initial_full_quote_history(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path)
    listing = ListingRecord("DE0000000002", "XETRA", "NEW")
    state = _WeeklyState(runtime)  # type: ignore[arg-type]
    state.listings = build_listing_gold([listing])

    details = state.ingest_quotes()

    assert runtime.calls == [(None, ())]
    assert details["source_fetch_mode"] == "full_history"
    assert [row["trade_date"] for row in runtime.persisted_silver] == [
        "2026-08-14",
        "2026-08-18",
        "2026-08-20",
        "2026-08-22",
    ]


def test_quote_history_is_full_after_split_change(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path)
    old_history = (_quote(14, "10"), _quote(22, "12"))
    _seed_silver(tmp_path, old_history)
    listing = ListingRecord("DE0000000001", "XETRA", "AAA")
    state = _WeeklyState(runtime)  # type: ignore[arg-type]
    state.listings = build_listing_gold([listing])
    state.action_changed_listings.add(listing.key)
    details = state.ingest_quotes()

    assert runtime.calls == [(None, old_history)]
    assert [row["trade_date"] for row in runtime.persisted_silver] == [
        "2026-08-14",
        "2026-08-18",
        "2026-08-20",
        "2026-08-22",
    ]
    assert details["source_fetch_mode"] == "full_history"


@pytest.mark.parametrize("changed_dataset", ["dividends", "splits"])
def test_corporate_action_change_triggers_full_quote_history(
    tmp_path: Path, changed_dataset: str
) -> None:
    runtime = _Runtime(tmp_path)
    listing = ListingRecord("DE0000000001", "XETRA", "AAA")
    old_history = (_quote(14, "10"), _quote(22, "12"))
    _seed_silver(tmp_path, old_history)
    state = _WeeklyState(runtime)  # type: ignore[arg-type]
    state.listings = build_listing_gold([listing])

    if changed_dataset == "dividends":
        prior = _dividend(10, "1.25")
        runtime.dividend_snapshot = (_dividend(10, "1.50"),)
        _seed_silver_rows(tmp_path, "dividends", [_dividend_silver_row(prior)])
        state.ingest_dividends()
    else:
        prior = _split(10)
        runtime.split_snapshot = (
            SplitEvent(
                isin=prior.isin,
                exchange=prior.exchange,
                code=prior.code,
                event_date=prior.event_date,
                split_ratio="3:1",
                split_factor=Decimal("3"),
            ),
        )
        _seed_silver_rows(tmp_path, "splits", [_split_silver_row(prior)])
        state.ingest_splits()

    assert listing.key in state.action_changed_listings
    details = state.ingest_quotes()

    assert runtime.calls == [(None, old_history)]
    assert details["source_fetch_mode"] == "full_history"


def test_action_refresh_overwrites_retained_duplicate_provider_history() -> None:
    historical = _dividend(10)
    latest = _dividend(20, "1.30")

    merged = _replace_action_window(
        (historical, latest),
        (historical, latest),
        last_date=latest.event_date,
    )

    assert merged == (historical, latest)


def test_full_action_snapshot_keeps_prior_retraction_tombstones() -> None:
    retracted = retract_dividend(_dividend(10))

    assert _replace_action_window((retracted,), (), last_date=None) == (retracted,)


def test_weekly_corporate_actions_fetch_complete_history(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path)
    state = _WeeklyState(runtime)  # type: ignore[arg-type]
    state.listings = build_listing_gold([ListingRecord("DE0000000001", "XETRA", "AAA")])

    dividends = state.ingest_dividends()
    splits = state.ingest_splits()

    assert runtime.action_calls == [("dividends", None, ()), ("splits", None, ())]
    assert dividends["source_fetch_mode"] == splits["source_fetch_mode"] == "full_history"
    assert dividends["publication_mode"] == splits["publication_mode"] == "semantic_delta"
    assert runtime.persistence_events == [
        ("silver", "dividends"),
        ("gold", "dividends"),
        ("silver", "splits"),
        ("gold", "splits"),
    ]


def test_weekly_listings_publish_silver_before_gold(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path)
    state = _WeeklyState(runtime)  # type: ignore[arg-type]

    state.ingest_listings()

    assert runtime.persistence_events == [("silver", "listings"), ("gold", "listings")]


def test_weekly_stages_quarantine_invalid_provider_ohlc(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    captured: dict[str, bool | int] = {}
    caplog.set_level("INFO", logger="xetra_loader.pipeline.runtime")

    class Runtime:
        def validate_weekly_schema_contract(self) -> None:
            captured["schema_preflight"] = True

    def from_environment(
        *,
        quarantine_invalid_ohlc: bool = False,
        cores: int | None = None,
    ) -> object:
        captured["quarantine_invalid_ohlc"] = quarantine_invalid_ohlc
        captured["cores"] = cores or 0
        return Runtime()

    monkeypatch.setattr(
        "xetra_loader.pipeline.runtime.PostgresEodhdBootstrapRuntime.from_environment",
        from_environment,
    )

    build_weekly_stages(cores=4)

    assert captured == {
        "quarantine_invalid_ohlc": True,
        "cores": 4,
        "schema_preflight": True,
    }
    assert "weekly_configuration cores=4 provider_cores=4" in caplog.text


@pytest.mark.parametrize(
    ("cores", "expected"),
    [(1, (1, 1)), (2, (1, 1)), (3, (2, 1)), (6, (3, 3)), (7, (4, 3))],
)
def test_parallel_database_queries_share_the_configured_cpu_budget(
    cores: int,
    expected: tuple[int, int],
) -> None:
    budgets = _parallel_database_core_budgets(cores)

    assert budgets == expected
    if cores == 1:
        assert budgets == (1, 1)  # one executor thread runs these serially
    else:
        assert sum(budgets) == cores
