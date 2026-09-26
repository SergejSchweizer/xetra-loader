"""Full confirmed XETRA bootstrap from EODHD through Gold and PostgreSQL."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Protocol, cast
from urllib.parse import quote
from urllib.request import Request, urlopen

from psycopg import Connection

from xetra_loader.config import resolve_eodhd_token, resolve_medallion_root
from xetra_loader.contracts.corporate_actions import DividendEvent, SplitEvent
from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.contracts.quotes import QuoteRecord
from xetra_loader.eodhd.transport import BinaryResponse, EodhdTransport, JSONValue
from xetra_loader.gold.dividends import DividendGoldResult, build_dividend_gold
from xetra_loader.gold.listings import ListingGoldResult, build_listing_gold
from xetra_loader.gold.quotes import QuoteGoldResult, build_quote_gold
from xetra_loader.gold.splits import SplitGoldResult, build_split_gold
from xetra_loader.ingestion.dividends import ingest_dividends
from xetra_loader.ingestion.listings import ingest_xetra_listings
from xetra_loader.ingestion.quotes import ingest_quotes
from xetra_loader.ingestion.splits import ingest_splits
from xetra_loader.medallion.core import (
    Layer,
    Manifest,
    MedallionLayout,
    atomic_write,
    atomic_write_text,
    canonical_json,
)
from xetra_loader.ops.reset import build_reset_plan, execute_reset
from xetra_loader.sync import connect_postgres
from xetra_loader.sync.core import SyncOutcome
from xetra_loader.sync.dividends import sync_dividends
from xetra_loader.sync.listings import sync_listings
from xetra_loader.sync.quotes import sync_quotes
from xetra_loader.sync.splits import sync_splits


@dataclass(frozen=True, slots=True)
class FetchMetrics:
    """Measured provider work; no request, retry, row, or timing value is guessed."""

    logical_requests: int = 0
    attempts: int = 0
    retries: int = 0
    failures: int = 0
    elapsed_seconds: float = 0.0
    rows: int = 0

    def __add__(self, other: FetchMetrics) -> FetchMetrics:
        return FetchMetrics(
            logical_requests=self.logical_requests + other.logical_requests,
            attempts=self.attempts + other.attempts,
            retries=self.retries + other.retries,
            failures=self.failures + other.failures,
            elapsed_seconds=self.elapsed_seconds + other.elapsed_seconds,
            rows=self.rows + other.rows,
        )

    def as_dict(self) -> dict[str, JSONValue]:
        return {
            "logical_requests": self.logical_requests,
            "attempts": self.attempts,
            "retries": self.retries,
            "failures": self.failures,
            "elapsed_seconds": self.elapsed_seconds,
            "rows": self.rows,
        }


@dataclass(frozen=True, slots=True)
class FetchBatch[T]:
    rows: tuple[T, ...]
    metrics: FetchMetrics
    fetched_at_utc: datetime | None = None


@dataclass(frozen=True, slots=True)
class BootstrapVerification:
    """Independent serving-state checks required before bootstrap is accepted."""

    row_counts: Mapping[str, tuple[int, int]]
    key_differences: Mapping[str, tuple[int, int]]
    date_bounds_match: Mapping[str, bool]
    sync_state_match: Mapping[str, bool]

    @property
    def passed(self) -> bool:
        return (
            all(expected == actual for expected, actual in self.row_counts.values())
            and all(missing == extra == 0 for missing, extra in self.key_differences.values())
            and all(self.date_bounds_match.values())
            and all(self.sync_state_match.values())
        )

    def as_dict(self) -> dict[str, JSONValue]:
        return {
            "passed": self.passed,
            "row_counts": {
                name: [expected, actual] for name, (expected, actual) in self.row_counts.items()
            },
            "key_differences": {
                name: [missing, extra] for name, (missing, extra) in self.key_differences.items()
            },
            "date_bounds_match": dict(self.date_bounds_match),
            "sync_state_match": dict(self.sync_state_match),
        }


@dataclass(frozen=True, slots=True)
class BootstrapResult:
    """Measured and verified result of one full-universe bootstrap or replay."""

    fetch_metrics: FetchMetrics
    listing_gold: ListingGoldResult
    quote_gold: QuoteGoldResult
    dividend_gold: DividendGoldResult
    split_gold: SplitGoldResult
    sync_outcomes: Mapping[str, SyncOutcome]
    verification: BootstrapVerification

    def as_dict(self) -> dict[str, JSONValue]:
        return {
            "status": "PASS" if self.verification.passed else "FAIL",
            "fetch_metrics": self.fetch_metrics.as_dict(),
            "gold_rows": {
                "listings": self.listing_gold.row_count,
                "eod_quotes": self.quote_gold.row_count,
                "dividends": self.dividend_gold.row_count,
                "splits": self.split_gold.row_count,
            },
            "sync_mutations": {
                name: outcome.counters.total_mutations
                for name, outcome in self.sync_outcomes.items()
            },
            "verification": self.verification.as_dict(),
        }


class DestructiveConfirmationRequired(RuntimeError):
    """Raised before any runtime call when bootstrap confirmation is absent."""


class BootstrapVerificationError(RuntimeError):
    """Raised when independent PostgreSQL verification does not match Gold."""


class BootstrapRuntime(Protocol):
    """Provider, persistence, publication, and verification boundary for bootstrap."""

    def reset_owned_state(self) -> None: ...
    def fetch_listings(self) -> FetchBatch[ListingRecord]: ...
    def fetch_quotes(
        self,
        listing: ListingRecord,
        *,
        last_business_date: date | None = None,
        previous_records: Iterable[QuoteRecord] = (),
    ) -> FetchBatch[QuoteRecord]: ...
    def fetch_dividends(
        self,
        listing: ListingRecord,
        *,
        last_event_date: date | None = None,
        previous_records: Iterable[DividendEvent] = (),
    ) -> FetchBatch[DividendEvent]: ...
    def fetch_splits(
        self,
        listing: ListingRecord,
        *,
        last_event_date: date | None = None,
        previous_records: Iterable[SplitEvent] = (),
    ) -> FetchBatch[SplitEvent]: ...

    def persist_gold(
        self,
        dataset: str,
        semantic_rows: Sequence[Mapping[str, JSONValue]],
        *,
        row_count: int,
        semantic_fingerprint: str,
        retracted_keys: Sequence[tuple[str, str, str, str]] = (),
    ) -> None: ...

    def publish_listings(
        self,
        gold: ListingGoldResult,
        *,
        fetched_at_by_key: Mapping[tuple[str, str, str], datetime] | None = None,
    ) -> SyncOutcome: ...
    def publish_quotes(
        self,
        gold: QuoteGoldResult,
        *,
        fetched_at_by_key: Mapping[tuple[str, str, str, date], datetime] | None = None,
    ) -> SyncOutcome: ...
    def publish_dividends(
        self,
        gold: DividendGoldResult,
        *,
        fetched_at_by_key: Mapping[tuple[str, str, str, str], datetime] | None = None,
    ) -> SyncOutcome: ...
    def publish_splits(
        self,
        gold: SplitGoldResult,
        *,
        fetched_at_by_key: Mapping[tuple[str, str, str, str], datetime] | None = None,
    ) -> SyncOutcome: ...

    def verify(
        self,
        listing_gold: ListingGoldResult,
        quote_gold: QuoteGoldResult,
        dividend_gold: DividendGoldResult,
        split_gold: SplitGoldResult,
        sync_outcomes: Mapping[str, SyncOutcome],
    ) -> BootstrapVerification: ...

    def close(self) -> None: ...


def run_full_bootstrap(
    runtime: BootstrapRuntime,
    *,
    confirmed: bool,
    reset_owned_state: bool = True,
    parallel_runtime_factory: Callable[[], BootstrapRuntime] | None = None,
    max_workers: int = 1,
) -> BootstrapResult:
    """Load every valid XETRA identity and every available history, then verify publication."""

    if not confirmed:
        raise DestructiveConfirmationRequired("--confirm-destructive-reset is required")
    if max_workers < 1:
        raise ValueError("max_workers must be positive")
    if max_workers > 1 and parallel_runtime_factory is None:
        raise ValueError("parallel bootstrap requires a runtime factory")
    if reset_owned_state:
        runtime.reset_owned_state()

    metrics = FetchMetrics()
    listing_batch = runtime.fetch_listings()
    metrics += listing_batch.metrics
    listing_gold = build_listing_gold(listing_batch.rows)
    listing_fetch_times = _batch_fetch_times(listing_batch)

    quotes: list[QuoteRecord] = []
    dividends: list[DividendEvent] = []
    splits: list[SplitEvent] = []
    quote_fetch_times: dict[tuple[str, str, str, date], datetime] = {}
    dividend_fetch_times: dict[tuple[str, str, str, str], datetime] = {}
    split_fetch_times: dict[tuple[str, str, str, str], datetime] = {}
    for _listing, quote_batch, dividend_batch, split_batch in _fetch_listing_batches(
        runtime,
        listing_gold.rows,
        parallel_runtime_factory=parallel_runtime_factory,
        max_workers=max_workers,
    ):
        quotes.extend(quote_batch.rows)
        dividends.extend(dividend_batch.rows)
        splits.extend(split_batch.rows)
        quote_fetch_times.update(_batch_fetch_times(quote_batch))
        dividend_fetch_times.update(_batch_fetch_times(dividend_batch))
        split_fetch_times.update(_batch_fetch_times(split_batch))
        metrics += quote_batch.metrics + dividend_batch.metrics + split_batch.metrics

    quote_gold = build_quote_gold(quotes)
    dividend_gold = build_dividend_gold(dividends)
    split_gold = build_split_gold(splits)
    _persist_all_gold(runtime, listing_gold, quote_gold, dividend_gold, split_gold)

    sync_outcomes = {
        "listings": _publish_listings(runtime, listing_gold, listing_fetch_times),
        "eod_quotes": _publish_quotes(runtime, quote_gold, quote_fetch_times),
        "dividends": _publish_dividends(runtime, dividend_gold, dividend_fetch_times),
        "splits": _publish_splits(runtime, split_gold, split_fetch_times),
    }
    verification = runtime.verify(
        listing_gold,
        quote_gold,
        dividend_gold,
        split_gold,
        sync_outcomes,
    )
    if not verification.passed:
        raise BootstrapVerificationError("PostgreSQL serving state does not match validated Gold")

    return BootstrapResult(
        fetch_metrics=metrics,
        listing_gold=listing_gold,
        quote_gold=quote_gold,
        dividend_gold=dividend_gold,
        split_gold=split_gold,
        sync_outcomes=sync_outcomes,
        verification=verification,
    )


def _fetch_listing_batches(
    runtime: BootstrapRuntime,
    listings: Sequence[ListingRecord],
    *,
    parallel_runtime_factory: Callable[[], BootstrapRuntime] | None,
    max_workers: int,
) -> Iterable[
    tuple[
        ListingRecord,
        FetchBatch[QuoteRecord],
        FetchBatch[DividendEvent],
        FetchBatch[SplitEvent],
    ]
]:
    """Fetch independent listing histories in stable order, with isolated worker state."""

    if max_workers == 1:
        return tuple(_fetch_one_listing(runtime, listing) for listing in listings)
    if parallel_runtime_factory is None:
        raise ValueError("parallel bootstrap requires a runtime factory")

    def fetch_with_worker(
        listing: ListingRecord,
    ) -> tuple[
        ListingRecord,
        FetchBatch[QuoteRecord],
        FetchBatch[DividendEvent],
        FetchBatch[SplitEvent],
    ]:
        worker = parallel_runtime_factory()
        try:
            return _fetch_one_listing(worker, listing)
        finally:
            worker.close()

    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="xdl-fetch") as executor:
        return tuple(executor.map(fetch_with_worker, listings))


def _fetch_one_listing(
    runtime: BootstrapRuntime,
    listing: ListingRecord,
) -> tuple[
    ListingRecord,
    FetchBatch[QuoteRecord],
    FetchBatch[DividendEvent],
    FetchBatch[SplitEvent],
]:
    return (
        listing,
        runtime.fetch_quotes(listing),
        runtime.fetch_dividends(listing),
        runtime.fetch_splits(listing),
    )


def _batch_fetch_times(batch: FetchBatch[Any]) -> dict[Any, datetime]:
    """Associate every fetched semantic row with its measured provider response time."""

    if batch.fetched_at_utc is None:
        return {}
    return {row.key: batch.fetched_at_utc for row in batch.rows}


def _publish_listings(
    runtime: BootstrapRuntime,
    gold: ListingGoldResult,
    fetched_at_by_key: Mapping[tuple[str, str, str], datetime],
) -> SyncOutcome:
    return (
        runtime.publish_listings(gold, fetched_at_by_key=fetched_at_by_key)
        if fetched_at_by_key
        else runtime.publish_listings(gold)
    )


def _publish_quotes(
    runtime: BootstrapRuntime,
    gold: QuoteGoldResult,
    fetched_at_by_key: Mapping[tuple[str, str, str, date], datetime],
) -> SyncOutcome:
    return (
        runtime.publish_quotes(gold, fetched_at_by_key=fetched_at_by_key)
        if fetched_at_by_key
        else runtime.publish_quotes(gold)
    )


def _publish_dividends(
    runtime: BootstrapRuntime,
    gold: DividendGoldResult,
    fetched_at_by_key: Mapping[tuple[str, str, str, str], datetime],
) -> SyncOutcome:
    return (
        runtime.publish_dividends(gold, fetched_at_by_key=fetched_at_by_key)
        if fetched_at_by_key
        else runtime.publish_dividends(gold)
    )


def _publish_splits(
    runtime: BootstrapRuntime,
    gold: SplitGoldResult,
    fetched_at_by_key: Mapping[tuple[str, str, str, str], datetime],
) -> SyncOutcome:
    return (
        runtime.publish_splits(gold, fetched_at_by_key=fetched_at_by_key)
        if fetched_at_by_key
        else runtime.publish_splits(gold)
    )


def _persist_all_gold(
    runtime: BootstrapRuntime,
    listings: ListingGoldResult,
    quotes: QuoteGoldResult,
    dividends: DividendGoldResult,
    splits: SplitGoldResult,
) -> None:
    for dataset, rows, count, fingerprint in (
        (
            "listings",
            listings.semantic_rows(),
            listings.row_count,
            listings.semantic_fingerprint,
        ),
        (
            "eod_quotes",
            quotes.semantic_rows(),
            quotes.row_count,
            quotes.semantic_fingerprint,
        ),
        (
            "dividends",
            dividends.semantic_rows(),
            dividends.row_count,
            dividends.semantic_fingerprint,
        ),
        (
            "splits",
            splits.semantic_rows(),
            splits.row_count,
            splits.semantic_fingerprint,
        ),
    ):
        runtime.persist_gold(
            dataset,
            rows,
            row_count=count,
            semantic_fingerprint=fingerprint,
        )


class _AttemptCounter:
    def __init__(self) -> None:
        self.attempts = 0

    def open(self, request: Request, timeout: float) -> BinaryResponse:
        self.attempts += 1
        return cast(BinaryResponse, urlopen(request, timeout=timeout))


class _MeasuredTransport:
    def __init__(self, token: str | None = None) -> None:
        self._opener = _AttemptCounter()
        self._transport = EodhdTransport(
            token=resolve_eodhd_token(token),
            opener=self._opener.open,
        )
        self.logical_requests = 0

    @property
    def attempts(self) -> int:
        return self._opener.attempts

    def get_json(
        self,
        path: str,
        params: Mapping[str, str | int | float] | None = None,
    ) -> JSONValue:
        self.logical_requests += 1
        return self._transport.get_json(path, params)


class PostgresEodhdBootstrapRuntime:
    """Concrete EODHD + medallion + PostgreSQL implementation for production bootstrap."""

    def __init__(
        self,
        *,
        connection: Connection[Any],
        transport: _MeasuredTransport,
        medallion_root: Path,
        repository_root: Path,
        quarantine_invalid_ohlc: bool = False,
    ) -> None:
        self._connection = connection
        self._transport = transport
        self._layout = MedallionLayout(medallion_root.resolve())
        self._repository_root = repository_root.resolve()
        self._quarantine_invalid_ohlc = quarantine_invalid_ohlc

    @classmethod
    def from_environment(
        cls,
        *,
        quarantine_invalid_ohlc: bool = False,
    ) -> PostgresEodhdBootstrapRuntime:
        root = resolve_medallion_root()
        return cls(
            connection=connect_postgres(require_non_superuser=True),
            transport=_MeasuredTransport(),
            medallion_root=Path(root),
            repository_root=Path.cwd(),
            quarantine_invalid_ohlc=quarantine_invalid_ohlc,
        )

    @classmethod
    def from_admin_environment(
        cls,
        *,
        quarantine_invalid_ohlc: bool = False,
    ) -> PostgresEodhdBootstrapRuntime:
        root = resolve_medallion_root()
        return cls(
            connection=connect_postgres(admin=True),
            transport=_MeasuredTransport(),
            medallion_root=Path(root),
            repository_root=Path.cwd(),
            quarantine_invalid_ohlc=quarantine_invalid_ohlc,
        )

    def reset_owned_state(self) -> None:
        execute_reset(
            build_reset_plan(self._layout.root),
            confirmed=True,
            dry_run=False,
            connection=self._connection,
        )
        self._initialize_database()

    def fetch_listings(self) -> FetchBatch[ListingRecord]:
        before = self._measurement_start()
        result = ingest_xetra_listings(self._transport)
        metrics = self._measurement_finish(before, len(result.silver_records))
        raw = cast(JSONValue, json.loads(result.bronze_payload))
        self._write_layer(Layer.BRONZE, "listings", raw)
        silver = [record.semantic_dict() for record in result.silver_records]
        self._write_layer(Layer.SILVER, "listings", cast(JSONValue, silver))
        return FetchBatch(result.silver_records, metrics, datetime.now(UTC))

    def fetch_quotes(
        self,
        listing: ListingRecord,
        *,
        last_business_date: date | None = None,
        previous_records: Iterable[QuoteRecord] = (),
    ) -> FetchBatch[QuoteRecord]:
        before = self._measurement_start()
        result = ingest_quotes(
            self._transport,
            listing,
            last_business_date=last_business_date,
            previous_records=previous_records,
            quarantine_invalid_ohlc=self._quarantine_invalid_ohlc,
        )
        metrics = self._measurement_finish(before, len(result.silver_records))
        self._record_partition_bronze("eod_quotes", listing, result.bronze_payload)
        self._write_partition(
            Layer.SILVER,
            "eod_quotes",
            listing,
            [record.semantic_dict() for record in result.silver_records],
        )
        return FetchBatch(result.silver_records, metrics, datetime.now(UTC))

    def fetch_dividends(
        self,
        listing: ListingRecord,
        *,
        last_event_date: date | None = None,
        previous_records: Iterable[DividendEvent] = (),
    ) -> FetchBatch[DividendEvent]:
        before = self._measurement_start()
        result = ingest_dividends(
            self._transport,
            listing,
            last_event_date=last_event_date,
            previous_records=previous_records,
        )
        metrics = self._measurement_finish(before, len(result.silver_records))
        self._record_partition_bronze("dividends", listing, result.bronze_payload)
        self._write_partition(
            Layer.SILVER,
            "dividends",
            listing,
            [_dividend_silver(event) for event in result.silver_records],
        )
        return FetchBatch(result.silver_records, metrics, datetime.now(UTC))

    def fetch_splits(
        self,
        listing: ListingRecord,
        *,
        last_event_date: date | None = None,
        previous_records: Iterable[SplitEvent] = (),
    ) -> FetchBatch[SplitEvent]:
        before = self._measurement_start()
        result = ingest_splits(
            self._transport,
            listing,
            last_event_date=last_event_date,
            previous_records=previous_records,
        )
        metrics = self._measurement_finish(before, len(result.silver_records))
        self._record_partition_bronze("splits", listing, result.bronze_payload)
        self._write_partition(
            Layer.SILVER,
            "splits",
            listing,
            [_split_silver(event) for event in result.silver_records],
        )
        return FetchBatch(result.silver_records, metrics, datetime.now(UTC))

    def persist_gold(
        self,
        dataset: str,
        semantic_rows: Sequence[Mapping[str, JSONValue]],
        *,
        row_count: int,
        semantic_fingerprint: str,
        retracted_keys: Sequence[tuple[str, str, str, str]] = (),
    ) -> None:
        if dataset in {"eod_quotes", "dividends", "splits"}:
            self._finalize_partitions(dataset)
        rows = [dict(row) for row in semantic_rows]
        self._write_layer(Layer.GOLD, dataset, cast(JSONValue, rows))
        retractions = [list(key) for key in sorted(retracted_keys)]
        retraction_fingerprint = hashlib.sha256(
            canonical_json(cast(JSONValue, retractions)).encode("utf-8")
        ).hexdigest()
        retractions_path = self._layout.retractions_path(Layer.GOLD, dataset)
        if dataset in {"dividends", "splits"}:
            retractions_path.write_text(
                canonical_json(cast(JSONValue, retractions)) + "\n",
                encoding="utf-8",
            )
        manifest = Manifest(
            dataset=dataset,
            layer=Layer.GOLD,
            semantic_metadata={
                "row_count": row_count,
                "builder_semantic_fingerprint": semantic_fingerprint,
                "retractions_fingerprint": retraction_fingerprint,
            },
            run_metadata={},
        )
        atomic_write_text(
            self._layout.manifest_path(Layer.GOLD, dataset),
            manifest.to_json() + "\n",
        )

    def persist_silver(
        self,
        dataset: str,
        semantic_rows: Sequence[Mapping[str, JSONValue]],
    ) -> None:
        """Replace the complete normalized Silver state after an incremental merge."""

        self._write_layer(
            Layer.SILVER,
            dataset,
            cast(JSONValue, [dict(row) for row in semantic_rows]),
        )

    def publish_listings(
        self,
        gold: ListingGoldResult,
        *,
        fetched_at_by_key: Mapping[tuple[str, str, str], datetime] | None = None,
    ) -> SyncOutcome:
        return sync_listings(self._connection, gold, fetched_at_by_key=fetched_at_by_key)

    def publish_quotes(
        self,
        gold: QuoteGoldResult,
        *,
        fetched_at_by_key: Mapping[tuple[str, str, str, date], datetime] | None = None,
    ) -> SyncOutcome:
        return sync_quotes(self._connection, gold, fetched_at_by_key=fetched_at_by_key)

    def publish_dividends(
        self,
        gold: DividendGoldResult,
        *,
        fetched_at_by_key: Mapping[tuple[str, str, str, str], datetime] | None = None,
    ) -> SyncOutcome:
        return sync_dividends(self._connection, gold, fetched_at_by_key=fetched_at_by_key)

    def publish_splits(
        self,
        gold: SplitGoldResult,
        *,
        fetched_at_by_key: Mapping[tuple[str, str, str, str], datetime] | None = None,
    ) -> SyncOutcome:
        return sync_splits(self._connection, gold, fetched_at_by_key=fetched_at_by_key)

    def verify(
        self,
        listing_gold: ListingGoldResult,
        quote_gold: QuoteGoldResult,
        dividend_gold: DividendGoldResult,
        split_gold: SplitGoldResult,
        sync_outcomes: Mapping[str, SyncOutcome],
    ) -> BootstrapVerification:
        expected_keys: dict[str, set[tuple[str, ...]]] = {
            "listings": {tuple(row.key) for row in listing_gold.rows},
            "eod_quotes": {tuple(str(value) for value in row.key) for row in quote_gold.rows},
            "dividends": {tuple(row.key) for row in dividend_gold.rows},
            "splits": {tuple(row.key) for row in split_gold.rows},
        }
        actual_keys = {
            "listings": self._keys("listings", "isin, exchange, code"),
            "eod_quotes": self._keys("eod_quotes", "isin, exchange, code, trade_date::text"),
            "dividends": self._keys("dividends", "isin, exchange, code, event_key"),
            "splits": self._keys("splits", "isin, exchange, code, event_key"),
        }
        row_counts = {
            "listings": (listing_gold.row_count, self._count("listings")),
            "eod_quotes": (quote_gold.row_count, self._count("eod_quotes")),
            "dividends": (dividend_gold.row_count, self._count("dividends")),
            "splits": (split_gold.row_count, self._count("splits")),
        }
        key_differences = {
            name: (
                len(expected_keys[name] - actual_keys[name]),
                len(actual_keys[name] - expected_keys[name]),
            )
            for name in expected_keys
        }
        date_bounds_match = {
            "listings": True,
            "eod_quotes": self._date_bounds("eod_quotes", "trade_date")
            == _bounds(record.trade_date for record in quote_gold.rows),
            "dividends": self._date_bounds("dividends", "event_date")
            == _bounds(record.event_date for record in dividend_gold.rows),
            "splits": self._date_bounds("splits", "event_date")
            == _bounds(record.event_date for record in split_gold.rows),
        }
        sync_state_match = {
            name: self._sync_state_matches(name, outcome) for name, outcome in sync_outcomes.items()
        }
        return BootstrapVerification(
            row_counts=row_counts,
            key_differences=key_differences,
            date_bounds_match=date_bounds_match,
            sync_state_match=sync_state_match,
        )

    def close(self) -> None:
        self._connection.close()

    def _initialize_database(self) -> None:
        for relative in (
            "sql/schema/001_xetra_loader.sql",
            "sql/schema/002_roles.sql",
            "sql/schema/003_listing_lifecycle.sql",
            "sql/sync/001_xetra_loader_sync.sql",
            "sql/schema/004_xetra_features.sql",
        ):
            sql = (self._repository_root / relative).read_text(encoding="utf-8")
            self._connection.execute(sql)

    def _measurement_start(self) -> tuple[int, int, float]:
        return self._transport.logical_requests, self._transport.attempts, time.perf_counter()

    def _measurement_finish(
        self,
        before: tuple[int, int, float],
        row_count: int,
    ) -> FetchMetrics:
        calls_before, attempts_before, started = before
        calls = self._transport.logical_requests - calls_before
        attempts = self._transport.attempts - attempts_before
        return FetchMetrics(
            logical_requests=calls,
            attempts=attempts,
            retries=max(0, attempts - calls),
            failures=0,
            elapsed_seconds=time.perf_counter() - started,
            rows=row_count,
        )

    def _record_partition_bronze(
        self,
        dataset: str,
        listing: ListingRecord,
        bronze_payload: str,
    ) -> None:
        payload = cast(JSONValue, json.loads(bronze_payload))
        self._write_partition(
            Layer.BRONZE,
            dataset,
            listing,
            [
                {
                    "isin": listing.isin,
                    "exchange": listing.exchange,
                    "code": listing.code,
                    "payload": payload,
                }
            ],
        )

    def _write_partition(
        self,
        layer: Layer,
        dataset: str,
        listing: ListingRecord,
        rows: list[dict[str, JSONValue]],
    ) -> None:
        path = self._layout.partition_path(layer, dataset, _partition_name(listing))
        content = canonical_json(cast(JSONValue, sorted(rows, key=canonical_json))) + "\n"
        atomic_write_text(path, content)

    def _finalize_partitions(self, dataset: str) -> None:
        for layer in (Layer.BRONZE, Layer.SILVER):
            directory = self._layout.dataset_path(layer, dataset) / "partitions"
            partitions = sorted(directory.glob("*.json")) if directory.exists() else []
            target = self._layout.dataset_path(layer, dataset) / "data.json"
            _stream_partitions_to_json_array(partitions, target)

    def _write_layer(self, layer: Layer, dataset: str, payload: JSONValue) -> None:
        directory = self._layout.dataset_path(layer, dataset)
        atomic_write_text(directory / "data.json", canonical_json(payload) + "\n")

    def _count(self, table: str) -> int:
        row = self._connection.execute(f'SELECT count(*) FROM xetra_loader."{table}"').fetchone()
        if row is None:
            raise RuntimeError(f"missing count result for {table}")
        return int(row[0])

    def _keys(self, table: str, columns: str) -> set[tuple[str, ...]]:
        rows = self._connection.execute(f'SELECT {columns} FROM xetra_loader."{table}"').fetchall()
        return {tuple(str(value) for value in row) for row in rows}

    def _date_bounds(self, table: str, column: str) -> tuple[date | None, date | None]:
        row = self._connection.execute(
            f'SELECT min("{column}"), max("{column}") FROM xetra_loader."{table}"'
        ).fetchone()
        if row is None:
            raise RuntimeError(f"missing date-bound result for {table}")
        return cast(tuple[date | None, date | None], row)

    def _sync_state_matches(self, dataset: str, outcome: SyncOutcome) -> bool:
        row = self._connection.execute(
            "SELECT semantic_fingerprint, row_count "
            "FROM xetra_loader_sync.sync_state WHERE dataset = %s",
            (dataset,),
        ).fetchone()
        if row is None:
            return False
        return str(row[0]) == outcome.semantic_fingerprint and int(row[1]) == outcome.row_count


def _dividend_silver(event: DividendEvent) -> dict[str, JSONValue]:
    row = event.business_fields()
    row["event_key"] = event.event_key
    row["status"] = event.status.value
    return row


def _split_silver(event: SplitEvent) -> dict[str, JSONValue]:
    row = event.business_fields()
    row["event_key"] = event.event_key
    row["status"] = event.status.value
    return row


def _partition_name(listing: ListingRecord) -> str:
    """Return a deterministic, path-safe identity partition ordered by listing key."""

    return "!".join(
        quote(value, safe="") for value in (listing.code, listing.exchange, listing.isin)
    )


def _stream_partitions_to_json_array(partitions: Sequence[Path], target: Path) -> None:
    """Flatten already-bounded listing partitions without retaining the full universe."""

    def write(handle: Any) -> None:
        handle.write("[")
        first = True
        for partition in partitions:
            payload = cast(JSONValue, json.loads(partition.read_text(encoding="utf-8")))
            if not isinstance(payload, list):
                raise ValueError(f"invalid medallion partition: {partition}")
            for row in payload:
                if not first:
                    handle.write(",")
                handle.write(canonical_json(row))
                first = False
        handle.write("]\n")

    atomic_write(target, write)


def _bounds(values: Iterable[date]) -> tuple[date | None, date | None]:
    materialized = tuple(values)
    if not materialized:
        return None, None
    return min(materialized), max(materialized)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="xdl-bootstrap")
    parser.add_argument("--confirm-destructive-reset", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Execute the production bootstrap only after literal destructive confirmation."""

    args = _parser().parse_args(argv)
    if not args.confirm_destructive_reset:
        print(canonical_json({"status": "BLOCKED", "reason": "confirmation-required"}))
        return 2

    runtime = PostgresEodhdBootstrapRuntime.from_admin_environment()
    try:
        result = run_full_bootstrap(runtime, confirmed=True, reset_owned_state=True)
    finally:
        runtime.close()
    print(canonical_json(result.as_dict()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
