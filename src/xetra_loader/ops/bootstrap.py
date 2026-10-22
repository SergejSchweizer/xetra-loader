"""Full confirmed XETRA bootstrap from EODHD through Gold and PostgreSQL."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime
from itertools import chain, groupby
from pathlib import Path
from typing import Any, Protocol, cast
from urllib.request import Request, urlopen

from psycopg import Connection

from xetra_loader.config import resolve_cores, resolve_eodhd_token, resolve_medallion_root
from xetra_loader.contracts.corporate_actions import DividendEvent, SplitEvent
from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.contracts.quotes import QuoteRecord
from xetra_loader.eodhd.transport import (
    BinaryResponse,
    EodhdTransport,
    JSONValue,
    TransportConfig,
)
from xetra_loader.gold.dividends import DividendGoldResult, build_dividend_gold
from xetra_loader.gold.listings import ListingGoldResult, build_listing_gold
from xetra_loader.gold.quotes import QuoteGoldResult, build_quote_gold
from xetra_loader.gold.splits import SplitGoldResult, build_split_gold
from xetra_loader.gold.validation import GoldValidationSummary, validate_complete_gold
from xetra_loader.ingestion.dividends import ingest_dividends
from xetra_loader.ingestion.listings import ingest_xetra_listings
from xetra_loader.ingestion.quotes import ingest_quotes
from xetra_loader.ingestion.splits import ingest_splits
from xetra_loader.logging_config import configure_logging
from xetra_loader.medallion.core import (
    BronzePartition,
    GoldArtifact,
    Layer,
    Manifest,
    MedallionLayout,
    SilverPartition,
    atomic_write_text,
    canonical_json,
    write_parquet_rows,
    write_raw_listing_partitions,
)
from xetra_loader.ops.migrate import SCHEMA_FILES
from xetra_loader.ops.reset import build_reset_plan, execute_reset
from xetra_loader.sync import connect_postgres
from xetra_loader.sync.core import SyncOutcome
from xetra_loader.sync.dividends import sync_dividends
from xetra_loader.sync.listings import prune_stale_listings, sync_listings
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


@dataclass(frozen=True, slots=True)
class SnapshotWrite:
    """Semantic and execution options for one lake snapshot publication."""

    semantic_metadata: Mapping[str, JSONValue]
    retractions: Sequence[Mapping[str, JSONValue]] | None = None


class DestructiveConfirmationRequired(RuntimeError):
    """Raised before any runtime call when bootstrap confirmation is absent."""


class BootstrapVerificationError(RuntimeError):
    """Raised when independent PostgreSQL verification does not match Gold."""


class BootstrapRuntime(Protocol):
    """Provider, persistence, publication, and verification boundary for bootstrap."""

    def reset_owned_state(self) -> None: ...
    def database_worker(self, *, cores: int | None = None) -> BootstrapRuntime: ...
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
        authoritative_snapshot: GoldValidationSummary,
    ) -> SyncOutcome: ...
    def publish_dividends(
        self,
        gold: DividendGoldResult,
        *,
        fetched_at_by_key: Mapping[tuple[str, str, str, str], datetime] | None = None,
        authoritative_snapshot: GoldValidationSummary,
    ) -> SyncOutcome: ...
    def publish_splits(
        self,
        gold: SplitGoldResult,
        *,
        fetched_at_by_key: Mapping[tuple[str, str, str, str], datetime] | None = None,
        authoritative_snapshot: GoldValidationSummary,
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
    authoritative_snapshot = validate_complete_gold(
        listing_gold, quote_gold, dividend_gold, split_gold
    )
    _persist_all_gold(runtime, listing_gold, quote_gold, dividend_gold, split_gold)

    sync_outcomes = {
        "listings": _publish_listings(runtime, listing_gold, listing_fetch_times),
        "eod_quotes": _publish_quotes(
            runtime, quote_gold, quote_fetch_times, authoritative_snapshot
        ),
        "dividends": _publish_dividends(
            runtime, dividend_gold, dividend_fetch_times, authoritative_snapshot
        ),
        "splits": _publish_splits(runtime, split_gold, split_fetch_times, authoritative_snapshot),
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
    authoritative_snapshot: GoldValidationSummary,
) -> SyncOutcome:
    return (
        runtime.publish_quotes(
            gold,
            fetched_at_by_key=fetched_at_by_key,
            authoritative_snapshot=authoritative_snapshot,
        )
        if fetched_at_by_key
        else runtime.publish_quotes(gold, authoritative_snapshot=authoritative_snapshot)
    )


def _publish_dividends(
    runtime: BootstrapRuntime,
    gold: DividendGoldResult,
    fetched_at_by_key: Mapping[tuple[str, str, str, str], datetime],
    authoritative_snapshot: GoldValidationSummary,
) -> SyncOutcome:
    return (
        runtime.publish_dividends(
            gold,
            fetched_at_by_key=fetched_at_by_key,
            authoritative_snapshot=authoritative_snapshot,
        )
        if fetched_at_by_key
        else runtime.publish_dividends(gold, authoritative_snapshot=authoritative_snapshot)
    )


def _publish_splits(
    runtime: BootstrapRuntime,
    gold: SplitGoldResult,
    fetched_at_by_key: Mapping[tuple[str, str, str, str], datetime],
    authoritative_snapshot: GoldValidationSummary,
) -> SyncOutcome:
    return (
        runtime.publish_splits(
            gold,
            fetched_at_by_key=fetched_at_by_key,
            authoritative_snapshot=authoritative_snapshot,
        )
        if fetched_at_by_key
        else runtime.publish_splits(gold, authoritative_snapshot=authoritative_snapshot)
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
        self._token = resolve_eodhd_token(token)
        self._opener = _AttemptCounter()
        self._transport = EodhdTransport(
            TransportConfig(token=self._token, opener=self._opener.open)
        )
        self.logical_requests = 0

    def clone(self) -> _MeasuredTransport:
        """Create an independent HTTP counter/connection seam for one fetch worker."""

        return _MeasuredTransport(self._token)

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

    def __init__(  # noqa: PLR0913 - dependency/configuration injection boundary
        self,
        *,
        connection: Connection[Any],
        transport: _MeasuredTransport,
        medallion_root: Path,
        repository_root: Path,
        quarantine_invalid_ohlc: bool = False,
        cores: int | None = None,
    ) -> None:
        self._connection = connection
        self._transport = transport
        self._layout = MedallionLayout(medallion_root.resolve())
        self._repository_root = repository_root.resolve()
        self._quarantine_invalid_ohlc = quarantine_invalid_ohlc
        self._cores = resolve_cores(cores)
        self._owns_connection = True

    @classmethod
    def from_environment(
        cls,
        *,
        quarantine_invalid_ohlc: bool = False,
        cores: int | None = None,
    ) -> PostgresEodhdBootstrapRuntime:
        worker_count = resolve_cores(cores)
        root = resolve_medallion_root()
        return cls(
            connection=connect_postgres(require_non_superuser=True, cores=worker_count),
            transport=_MeasuredTransport(),
            medallion_root=Path(root),
            repository_root=Path.cwd(),
            quarantine_invalid_ohlc=quarantine_invalid_ohlc,
            cores=worker_count,
        )

    @classmethod
    def from_admin_environment(
        cls,
        *,
        quarantine_invalid_ohlc: bool = False,
        cores: int | None = None,
    ) -> PostgresEodhdBootstrapRuntime:
        worker_count = resolve_cores(cores)
        root = resolve_medallion_root()
        return cls(
            connection=connect_postgres(admin=True, cores=worker_count),
            transport=_MeasuredTransport(),
            medallion_root=Path(root),
            repository_root=Path.cwd(),
            quarantine_invalid_ohlc=quarantine_invalid_ohlc,
            cores=worker_count,
        )

    def reset_owned_state(self) -> None:
        execute_reset(
            build_reset_plan(self._layout.root),
            confirmed=True,
            dry_run=False,
            connection=self._connection,
        )
        self._initialize_database()

    def fetch_worker(self) -> PostgresEodhdBootstrapRuntime:
        """Return a fetch-only worker with independent HTTP state.

        Fetch methods write only listing-specific Bronze/Silver partitions. The
        shared PostgreSQL connection is intentionally used only by the parent
        runtime during serialized publication.
        """

        worker = type(self)(
            connection=self._connection,
            transport=self._transport.clone(),
            medallion_root=self._layout.root,
            repository_root=self._repository_root,
            quarantine_invalid_ohlc=self._quarantine_invalid_ohlc,
            cores=self._cores,
        )
        worker._owns_connection = False
        return worker

    def database_worker(
        self,
        *,
        cores: int | None = None,
    ) -> PostgresEodhdBootstrapRuntime:
        """Return a publication worker with an independent writer connection."""

        worker_count = resolve_cores(cores) if cores is not None else self._cores
        return type(self)(
            connection=connect_postgres(require_non_superuser=True, cores=worker_count),
            transport=self._transport.clone(),
            medallion_root=self._layout.root,
            repository_root=self._repository_root,
            quarantine_invalid_ohlc=self._quarantine_invalid_ohlc,
            cores=worker_count,
        )

    def fetch_listings(self) -> FetchBatch[ListingRecord]:
        before = self._measurement_start()
        result = ingest_xetra_listings(self._transport)
        metrics = self._measurement_finish(before, len(result.silver_records))
        raw = cast(JSONValue, json.loads(result.bronze_payload))
        self._write_layer(Layer.BRONZE, "listings", raw)
        silver = [record.semantic_dict() for record in result.silver_records]
        self._persist_partitioned_snapshot(
            Layer.SILVER,
            "listings",
            silver,
            snapshot=SnapshotWrite({"row_count": len(silver)}),
        )
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
        rows = [dict(row) for row in semantic_rows]
        retractions = [list(key) for key in sorted(retracted_keys)]
        retraction_fingerprint = hashlib.sha256(
            canonical_json(cast(JSONValue, retractions)).encode("utf-8")
        ).hexdigest()
        self._persist_partitioned_snapshot(
            Layer.GOLD,
            dataset,
            rows,
            snapshot=SnapshotWrite(
                semantic_metadata={
                    "row_count": row_count,
                    "builder_semantic_fingerprint": semantic_fingerprint,
                    "retractions_fingerprint": retraction_fingerprint,
                },
                retractions=(
                    [
                        {
                            "isin": key[0],
                            "exchange": key[1],
                            "code": key[2],
                            "event_key": key[3],
                        }
                        for key in retractions
                    ]
                    if dataset in {"dividends", "splits"}
                    else None
                ),
            ),
        )

    def persist_silver(
        self,
        dataset: str,
        semantic_rows: Sequence[Mapping[str, JSONValue]],
    ) -> None:
        """Atomically publish the complete normalized Silver Parquet snapshot."""

        self._persist_partitioned_snapshot(
            Layer.SILVER,
            dataset,
            [dict(row) for row in semantic_rows],
            snapshot=SnapshotWrite({"row_count": len(semantic_rows)}),
        )

    def publish_listings(
        self,
        gold: ListingGoldResult,
        *,
        fetched_at_by_key: Mapping[tuple[str, str, str], datetime] | None = None,
    ) -> SyncOutcome:
        return sync_listings(self._connection, gold, fetched_at_by_key=fetched_at_by_key)

    def prune_stale_listings(self, gold: ListingGoldResult) -> int:
        """Prune listing identities only after dependent datasets are published."""

        return prune_stale_listings(self._connection, gold)

    def publish_quotes(
        self,
        gold: QuoteGoldResult,
        *,
        fetched_at_by_key: Mapping[tuple[str, str, str, date], datetime] | None = None,
        authoritative_snapshot: GoldValidationSummary,
    ) -> SyncOutcome:
        return sync_quotes(
            self._connection,
            gold,
            fetched_at_by_key=fetched_at_by_key,
            authoritative_snapshot=authoritative_snapshot,
        )

    def publish_dividends(
        self,
        gold: DividendGoldResult,
        *,
        fetched_at_by_key: Mapping[tuple[str, str, str, str], datetime] | None = None,
        authoritative_snapshot: GoldValidationSummary,
    ) -> SyncOutcome:
        return sync_dividends(
            self._connection,
            gold,
            fetched_at_by_key=fetched_at_by_key,
            authoritative_snapshot=authoritative_snapshot,
        )

    def publish_splits(
        self,
        gold: SplitGoldResult,
        *,
        fetched_at_by_key: Mapping[tuple[str, str, str, str], datetime] | None = None,
        authoritative_snapshot: GoldValidationSummary,
    ) -> SyncOutcome:
        return sync_splits(
            self._connection,
            gold,
            fetched_at_by_key=fetched_at_by_key,
            authoritative_snapshot=authoritative_snapshot,
        )

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
        if self._owns_connection:
            self._connection.close()

    def _initialize_database(self) -> None:
        for relative in SCHEMA_FILES:
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
        if not isinstance(payload, list) or not all(isinstance(row, dict) for row in payload):
            raise ValueError(f"raw {dataset} response must contain JSON objects")
        timeframe = "1d" if dataset == "eod_quotes" else "event"
        fallback_date = datetime.now(UTC).date()
        grouped: dict[Path, list[dict[str, JSONValue]]] = {}
        for raw_row in cast(list[dict[str, JSONValue]], payload):
            raw_date = raw_row.get("date")
            partition_date = (
                date.fromisoformat(raw_date) if isinstance(raw_date, str) else fallback_date
            )
            path = self._layout.bronze_path(
                BronzePartition(
                    dataset_type=dataset,
                    exchange=listing.exchange,
                    instrument_type=listing.instrument_type or "unknown",
                    symbol=listing.code,
                    timeframe=timeframe,
                    partition_date=partition_date,
                )
            )
            grouped.setdefault(path, []).append(
                {
                    "isin": listing.isin,
                    "exchange": listing.exchange,
                    "code": listing.code,
                    "payload_json": canonical_json(raw_row),
                }
            )
        for path, rows in grouped.items():
            write_parquet_rows(
                path,
                sorted(rows, key=lambda row: canonical_json(cast(JSONValue, dict(row)))),
            )

    def _persist_partitioned_snapshot(  # noqa: PLR0913 - explicit persistence policy inputs
        self,
        layer: Layer,
        dataset: str,
        rows: Iterable[Mapping[str, JSONValue]],
        *,
        snapshot: SnapshotWrite,
        durable_sync: bool = True,
        presorted: bool = False,
    ) -> None:
        """Write canonical Parquet artifacts and atomically publish their manifest."""

        artifact_paths = self._write_canonical_snapshot(
            layer,
            dataset,
            rows,
            durable_sync=durable_sync,
            presorted=presorted,
        )
        semantic_metadata = dict(snapshot.semantic_metadata)
        if layer is Layer.GOLD:
            dataset_id = _gold_dataset_id(dataset)
            recorded_dataset_id = semantic_metadata.setdefault("dataset_id", dataset_id)
            if recorded_dataset_id != dataset_id:
                raise ValueError(f"Gold {dataset} manifest has an unstable dataset_id")
            semantic_metadata.setdefault("feature_set_version", "v1.0.0")
        retractions_path: str | None = None
        if snapshot.retractions:
            path = self._layout.retractions_path(layer, dataset)
            write_parquet_rows(
                path,
                snapshot.retractions,
                sync_file=durable_sync,
                sync_directory=durable_sync,
            )
            retractions_path = path.relative_to(self._layout.root).as_posix()
        manifest = Manifest(
            dataset=dataset,
            layer=layer,
            semantic_metadata=semantic_metadata,
            run_metadata={
                "artifact_paths": cast(JSONValue, artifact_paths),
                "retractions_path": retractions_path,
            },
        )
        atomic_write_text(
            self._layout.manifest_path(layer, dataset),
            manifest.to_json() + "\n",
        )

    def _write_canonical_snapshot(
        self,
        layer: Layer,
        dataset: str,
        rows: Iterable[Mapping[str, JSONValue]],
        *,
        durable_sync: bool = True,
        presorted: bool = False,
    ) -> list[str]:
        """Publish canonical partitions, buffering at most one partition at a time."""

        artifact_paths: list[str] = []
        ordered = (
            iter(rows)
            if presorted
            else iter(sorted(rows, key=lambda row: _snapshot_sort_key(dataset, row)))
        )
        if layer is Layer.SILVER:
            groups = groupby(ordered, key=lambda row: _silver_partition_key(dataset, row))
            for (exchange, symbol, year), group in groups:
                path = self._layout.silver_path(
                    SilverPartition(
                        dataset_type=dataset,
                        exchange=exchange,
                        symbol=symbol,
                        timeframe=_timeframe(dataset),
                        year=year,
                    )
                )
                write_parquet_rows(
                    path,
                    (dict(row) for row in group),
                    sync_file=durable_sync,
                    sync_directory=durable_sync,
                )
                artifact_paths.append(path.relative_to(self._layout.root).as_posix())
        elif layer is Layer.GOLD:
            feature_set_version = "v1.0.0"
            first_row = next(ordered, None)
            if first_row is None:
                return []
            schema = sorted(first_row)
            feature_hash = hashlib.sha256(
                canonical_json(
                    {
                        "dataset": dataset,
                        "feature_set_version": feature_set_version,
                        "columns": cast(JSONValue, schema),
                    }
                ).encode("utf-8")
            ).hexdigest()
            dataset_id = _gold_dataset_id(dataset)
            gold_groups = groupby(
                chain((first_row,), ordered),
                key=lambda row: (str(row.get("exchange", "")), str(row.get("code", ""))),
            )
            for (exchange, symbol), group in gold_groups:
                if not exchange or not symbol:
                    raise ValueError(f"Gold {dataset} row has an incomplete symbol identity")
                symbol_rows = [dict(row) for row in group]
                if any(sorted(row) != schema for row in symbol_rows):
                    raise ValueError(f"Gold {dataset} rows do not share a stable schema")
                source_hash = hashlib.sha256(
                    canonical_json(cast(JSONValue, symbol_rows)).encode("utf-8")
                ).hexdigest()
                path = self._layout.gold_path(
                    GoldArtifact(
                        dataset_id=dataset_id,
                        exchange=exchange,
                        symbol=symbol,
                        feature_hash=feature_hash,
                        source_hash=source_hash,
                    )
                )
                write_parquet_rows(
                    path,
                    symbol_rows,
                    sync_file=durable_sync,
                    sync_directory=durable_sync,
                )
                artifact_paths.append(path.relative_to(self._layout.root).as_posix())
        else:
            raise ValueError("canonical snapshots support only Silver and Gold")
        return sorted(artifact_paths)

    def _write_layer(self, layer: Layer, dataset: str, payload: JSONValue) -> None:
        if layer is not Layer.BRONZE or dataset != "listings" or not isinstance(payload, list):
            raise ValueError("raw listing Parquet writer received an unsupported dataset")
        if not all(isinstance(row, dict) for row in payload):
            raise ValueError("raw listing payload must contain JSON objects")
        write_raw_listing_partitions(
            self._layout,
            dataset,
            cast(list[Mapping[str, JSONValue]], payload),
            snapshot_date=datetime.now(UTC).date(),
        )

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


def _timeframe(dataset: str) -> str:
    if dataset == "eod_quotes":
        return "1d"
    if dataset == "listings":
        return "static"
    return "event"


def _gold_dataset_id(dataset: str) -> str:
    return f"xetra-{dataset}-gold-v1"


def _partition_date(dataset: str, row: Mapping[str, JSONValue]) -> date:
    field = "trade_date" if dataset == "eod_quotes" else "event_date"
    if dataset == "listings":
        return datetime.now(UTC).date()
    raw = row.get(field)
    if not isinstance(raw, str):
        raise ValueError(f"{dataset} row has no {field} for lake partitioning")
    try:
        return date.fromisoformat(raw[:10])
    except ValueError as exc:
        raise ValueError(f"{dataset} row has invalid {field} for lake partitioning") from exc


def _silver_partition_key(
    dataset: str,
    row: Mapping[str, JSONValue],
) -> tuple[str, str, int]:
    exchange = row.get("exchange")
    symbol = row.get("code")
    if not isinstance(exchange, str) or not exchange or not isinstance(symbol, str) or not symbol:
        raise ValueError(f"Silver {dataset} row has an incomplete symbol identity")
    partition_date = _partition_date(dataset, row)
    return exchange, symbol, partition_date.year


def _snapshot_sort_key(
    dataset: str,
    row: Mapping[str, JSONValue],
) -> tuple[str, str, int, int, str, str, str]:
    partition_date = _partition_date(dataset, row)
    return (
        str(row.get("exchange", "")),
        str(row.get("code", "")),
        partition_date.year,
        partition_date.month,
        partition_date.isoformat(),
        str(row.get("isin", "")),
        canonical_json(cast(JSONValue, dict(row))),
    )


def _bounds(values: Iterable[date]) -> tuple[date | None, date | None]:
    materialized = tuple(values)
    if not materialized:
        return None, None
    return min(materialized), max(materialized)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="xdl-bootstrap")
    parser.add_argument("--confirm-destructive-reset", action="store_true")
    parser.add_argument(
        "--cores",
        type=int,
        default=None,
        help="CPU budget for Polars, PostgreSQL, and independent downloads (default: config or 6)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Execute the production bootstrap only after literal destructive confirmation."""

    configure_logging("xdl-bootstrap")
    args = _parser().parse_args(argv)
    if not args.confirm_destructive_reset:
        print(canonical_json({"status": "BLOCKED", "reason": "confirmation-required"}))
        return 2

    cores = resolve_cores(args.cores)
    os.environ["POLARS_MAX_THREADS"] = str(cores)
    os.environ["XDL_CORES"] = str(cores)
    runtime = PostgresEodhdBootstrapRuntime.from_admin_environment(cores=cores)
    try:
        result = run_full_bootstrap(
            runtime,
            confirmed=True,
            reset_owned_state=True,
            parallel_runtime_factory=runtime.fetch_worker,
            max_workers=cores,
        )
    finally:
        runtime.close()
    print(canonical_json(result.as_dict()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
