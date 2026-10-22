"""Weekly refresh checks full provider history and publishes only semantic deltas."""

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from xetra_loader.contracts.corporate_actions import (
    DividendEvent,
    SplitEvent,
    retract_dividend,
)
from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.contracts.quotes import QuoteRecord
from xetra_loader.gold.listings import build_listing_gold
from xetra_loader.medallion.core import MedallionLayout
from xetra_loader.ops.bootstrap import (
    FetchBatch,
    FetchMetrics,
    PostgresEodhdBootstrapRuntime,
)
from xetra_loader.pipeline.runtime import (
    _parallel_database_core_budgets,
    _replace_action_window,
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

    def fetch_quotes(
        self,
        listing: ListingRecord,
        *,
        last_business_date: date | None = None,
        previous_records: tuple[QuoteRecord, ...] = (),
    ) -> FetchBatch[QuoteRecord]:
        del listing
        self.calls.append((last_business_date, previous_records))
        return FetchBatch(
            self.full_snapshot,
            FetchMetrics(logical_requests=1, attempts=1, rows=len(self.full_snapshot)),
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
            (_dividend(10),),
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
            (_split(10),),
            FetchMetrics(logical_requests=1, attempts=1, rows=1),
            datetime(2026, 8, 23, 10, 0, tzinfo=UTC),
        )

    def persist_gold(self, *args: object, **kwargs: object) -> None:
        del args, kwargs

    def persist_silver(self, dataset: str, rows: list[dict[str, object]]) -> None:
        if dataset == "eod_quotes":
            self.persisted_silver = rows


def _seed_silver(root: Path, records: tuple[QuoteRecord, ...]) -> None:
    writer = object.__new__(PostgresEodhdBootstrapRuntime)
    writer._layout = MedallionLayout(root)
    writer.persist_silver(
        "eod_quotes",
        [record.semantic_dict() for record in records],
    )


def test_weekly_quote_refresh_replaces_gold_with_full_provider_history(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path)
    old_history = (_quote(14, "10"), _quote(18, "11"), _quote(20, "10"), _quote(22, "12"))
    _seed_silver(tmp_path, old_history)
    state = _WeeklyState(runtime)  # type: ignore[arg-type]
    state.listings = build_listing_gold([ListingRecord("DE0000000001", "XETRA", "AAA")])

    details = state.ingest_quotes()

    assert runtime.calls[0][0] is None
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
    assert details["requests"] == 1
    assert details["source_fetch_mode"] == "full_history"
    assert details["publication_mode"] == "semantic_delta"


def test_quote_history_is_full_even_without_a_corporate_action_change(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path)
    old_history = (_quote(14, "10"), _quote(22, "12"))
    _seed_silver(tmp_path, old_history)
    listing = ListingRecord("DE0000000001", "XETRA", "AAA")
    state = _WeeklyState(runtime)  # type: ignore[arg-type]
    state.listings = build_listing_gold([listing])
    details = state.ingest_quotes()

    assert runtime.calls == [(None, old_history)]
    assert [row["trade_date"] for row in runtime.persisted_silver] == [
        "2026-08-14",
        "2026-08-18",
        "2026-08-20",
        "2026-08-22",
    ]
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


def test_weekly_stages_quarantine_invalid_provider_ohlc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, bool | int] = {}

    def from_environment(
        *,
        quarantine_invalid_ohlc: bool = False,
        cores: int | None = None,
    ) -> object:
        captured["quarantine_invalid_ohlc"] = quarantine_invalid_ohlc
        captured["cores"] = cores or 0
        return object()

    monkeypatch.setattr(
        "xetra_loader.pipeline.runtime.PostgresEodhdBootstrapRuntime.from_environment",
        from_environment,
    )

    build_weekly_stages(cores=4)

    assert captured == {"quarantine_invalid_ohlc": True, "cores": 4}


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
