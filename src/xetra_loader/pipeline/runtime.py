"""Production stage factory used by the non-interactive Sunday runner."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

from xetra_loader.config import resolve_weekly_workers
from xetra_loader.contracts.corporate_actions import ActionStatus, DividendEvent, SplitEvent
from xetra_loader.contracts.listings import ListingRecord, deserialize_listings
from xetra_loader.contracts.quotes import QuoteRecord, overlap_start
from xetra_loader.gold.dividends import DividendGoldResult, build_dividend_gold
from xetra_loader.gold.listings import ListingGoldResult, build_listing_gold
from xetra_loader.gold.quotes import QuoteGoldResult, build_quote_gold
from xetra_loader.gold.splits import SplitGoldResult, build_split_gold
from xetra_loader.gold.validation import GoldValidationSummary, validate_complete_gold
from xetra_loader.ingestion.corporate_actions import action_overlap_start
from xetra_loader.medallion.core import JSONValue, Layer, Manifest, MedallionLayout, canonical_json
from xetra_loader.ops.bootstrap import FetchBatch, PostgresEodhdBootstrapRuntime
from xetra_loader.pipeline.orchestrator import PipelineStages
from xetra_loader.sync.core import SyncCounters, SyncOutcome

_LOGGER = logging.getLogger(__name__)
_PROVIDER_STABLE_WORKERS = 8


@dataclass(slots=True)
class _WeeklyState:
    runtime: PostgresEodhdBootstrapRuntime
    max_workers: int = 1
    listings: ListingGoldResult | None = None
    quotes: QuoteGoldResult | None = None
    dividends: DividendGoldResult | None = None
    splits: SplitGoldResult | None = None
    outcomes: dict[str, SyncOutcome] = field(default_factory=dict)
    action_changed_listings: set[tuple[str, str, str]] = field(default_factory=set)
    fetched_at_by_dataset: dict[str, dict[object, datetime]] = field(default_factory=dict)
    authoritative_snapshot: GoldValidationSummary | None = None
    _publication_executor: ThreadPoolExecutor | None = None
    _publication_futures: dict[str, Future[SyncOutcome]] = field(default_factory=dict)

    def ingest_listings(self) -> dict[str, JSONValue]:
        batch = self.runtime.fetch_listings()
        self._record_fetch_times("listings", batch.rows, batch.fetched_at_utc)
        self.listings = build_listing_gold(batch.rows)
        self.runtime.persist_gold(
            "listings",
            self.listings.semantic_rows(),
            row_count=self.listings.row_count,
            semantic_fingerprint=self.listings.semantic_fingerprint,
        )
        return self._with_fetch_provenance(
            "listings",
            _ingest_details(
                self.listings.row_count,
                batch.metrics.logical_requests,
                self.listings.semantic_fingerprint,
            ),
        )

    def ingest_quotes(self) -> dict[str, JSONValue]:
        listings = self._require_listings()
        previous = _quotes_from_rows(_load_silver_rows(self.runtime._layout.root, "eod_quotes"))
        previous_by_listing: dict[tuple[str, str, str], list[QuoteRecord]] = {}
        for record in previous:
            previous_by_listing.setdefault(record.key[:3], []).append(record)
        records: list[QuoteRecord] = []
        jobs: list[tuple[ListingRecord, tuple[QuoteRecord, ...], date | None, bool]] = []
        requests = 0
        for listing in listings.rows:
            listing_previous = tuple(previous_by_listing.get(listing.key, ()))
            last_date = _latest_date(listing_previous)
            if not listing.is_active:
                records.extend(listing_previous)
                continue
            action_changed = listing.key in self.action_changed_listings
            jobs.append((listing, listing_previous, last_date, action_changed))

        _LOGGER.info(
            "provider_stage_start dataset=eod_quotes jobs=%d workers=%d",
            len(jobs),
            self.max_workers,
        )
        batches = _parallel_fetch(
            self.runtime,
            jobs,
            lambda worker, job: worker.fetch_quotes(
                job[0],
                last_business_date=None if job[3] else job[2],
                previous_records=() if job[3] else job[1],
            ),
            self.max_workers,
        )
        for job, batch in zip(jobs, batches, strict=True):
            listing, listing_previous, last_date, action_changed = job
            records.extend(
                _replace_quote_window(
                    listing_previous,
                    batch.rows,
                    None if action_changed else last_date,
                )
            )
            self._record_fetch_times("eod_quotes", batch.rows, batch.fetched_at_utc)
            requests += batch.metrics.logical_requests
            _LOGGER.debug(
                "provider_fetch_complete dataset=eod_quotes code=%s exchange=%s "
                "rows=%d requests=%d action_changed=%s",
                listing.code,
                listing.exchange,
                len(batch.rows),
                batch.metrics.logical_requests,
                action_changed,
            )
        self.quotes = build_quote_gold(records)
        self.runtime.persist_gold(
            "eod_quotes",
            self.quotes.semantic_rows(),
            row_count=self.quotes.row_count,
            semantic_fingerprint=self.quotes.semantic_fingerprint,
        )
        self.runtime.persist_silver(
            "eod_quotes",
            [record.semantic_dict() for record in self.quotes.rows],
        )
        return self._with_fetch_provenance(
            "eod_quotes",
            _ingest_details(self.quotes.row_count, requests, self.quotes.semantic_fingerprint),
        )

    def ingest_dividends(self) -> dict[str, JSONValue]:
        listings = self._require_listings()
        previous = _dividends_from_silver_rows(
            _load_silver_rows(self.runtime._layout.root, "dividends")
        )
        previous_by_listing: dict[tuple[str, str, str], list[DividendEvent]] = {}
        for event in previous:
            previous_by_listing.setdefault(event.key[:3], []).append(event)
        records: list[DividendEvent] = []
        jobs: list[tuple[ListingRecord, tuple[DividendEvent, ...], date | None]] = []
        requests = 0
        for listing in listings.rows:
            listing_previous = tuple(previous_by_listing.get(listing.key, ()))
            last_date = _latest_date(listing_previous)
            if not listing.is_active:
                records.extend(listing_previous)
                continue
            jobs.append((listing, listing_previous, last_date))

        _LOGGER.info(
            "provider_stage_start dataset=dividends jobs=%d workers=%d",
            len(jobs),
            self.max_workers,
        )
        batches = _parallel_fetch(
            self.runtime,
            jobs,
            lambda worker, job: worker.fetch_dividends(
                job[0], last_event_date=job[2], previous_records=job[1]
            ),
            self.max_workers,
        )
        for job, batch in zip(jobs, batches, strict=True):
            listing, listing_previous, last_date = job
            merged = _replace_action_window(listing_previous, batch.rows, last_date)
            if _action_set_changed(listing_previous, merged):
                self.action_changed_listings.add(listing.key)
            records.extend(merged)
            self._record_fetch_times("dividends", batch.rows, batch.fetched_at_utc)
            requests += batch.metrics.logical_requests
            _LOGGER.debug(
                "provider_fetch_complete dataset=dividends code=%s exchange=%s rows=%d requests=%d",
                listing.code,
                listing.exchange,
                len(batch.rows),
                batch.metrics.logical_requests,
            )
        self.dividends = build_dividend_gold(records)
        self.runtime.persist_gold(
            "dividends",
            self.dividends.semantic_rows(),
            row_count=self.dividends.row_count,
            semantic_fingerprint=self.dividends.semantic_fingerprint,
            retracted_keys=self.dividends.retracted_keys,
        )
        self.runtime.persist_silver(
            "dividends",
            [_dividend_silver_row(record) for record in records],
        )
        details = self._with_fetch_provenance(
            "dividends",
            _ingest_details(
                self.dividends.row_count,
                requests,
                self.dividends.semantic_fingerprint,
            ),
        )
        details["changed_listing_keys"] = _changed_listing_keys(self.action_changed_listings)
        return details

    def ingest_splits(self) -> dict[str, JSONValue]:
        listings = self._require_listings()
        previous = _splits_from_silver_rows(_load_silver_rows(self.runtime._layout.root, "splits"))
        previous_by_listing: dict[tuple[str, str, str], list[SplitEvent]] = {}
        for event in previous:
            previous_by_listing.setdefault(event.key[:3], []).append(event)
        records: list[SplitEvent] = []
        jobs: list[tuple[ListingRecord, tuple[SplitEvent, ...], date | None]] = []
        requests = 0
        for listing in listings.rows:
            listing_previous = tuple(previous_by_listing.get(listing.key, ()))
            last_date = _latest_date(listing_previous)
            if not listing.is_active:
                records.extend(listing_previous)
                continue
            jobs.append((listing, listing_previous, last_date))

        _LOGGER.info(
            "provider_stage_start dataset=splits jobs=%d workers=%d",
            len(jobs),
            self.max_workers,
        )
        batches = _parallel_fetch(
            self.runtime,
            jobs,
            lambda worker, job: worker.fetch_splits(
                job[0], last_event_date=job[2], previous_records=job[1]
            ),
            self.max_workers,
        )
        for job, batch in zip(jobs, batches, strict=True):
            listing, listing_previous, last_date = job
            merged = _replace_action_window(listing_previous, batch.rows, last_date)
            if _action_set_changed(listing_previous, merged):
                self.action_changed_listings.add(listing.key)
            records.extend(merged)
            self._record_fetch_times("splits", batch.rows, batch.fetched_at_utc)
            requests += batch.metrics.logical_requests
            _LOGGER.debug(
                "provider_fetch_complete dataset=splits code=%s exchange=%s rows=%d requests=%d",
                listing.code,
                listing.exchange,
                len(batch.rows),
                batch.metrics.logical_requests,
            )
        self.splits = build_split_gold(records)
        self.runtime.persist_gold(
            "splits",
            self.splits.semantic_rows(),
            row_count=self.splits.row_count,
            semantic_fingerprint=self.splits.semantic_fingerprint,
            retracted_keys=self.splits.retracted_keys,
        )
        self.runtime.persist_silver(
            "splits",
            [_split_silver_row(record) for record in records],
        )
        details = self._with_fetch_provenance(
            "splits",
            _ingest_details(self.splits.row_count, requests, self.splits.semantic_fingerprint),
        )
        details["changed_listing_keys"] = _changed_listing_keys(self.action_changed_listings)
        return details

    def validate_gold(self) -> dict[str, JSONValue]:
        listings, quotes, dividends, splits = self._require_gold()
        self.authoritative_snapshot = validate_complete_gold(listings, quotes, dividends, splits)
        return self.authoritative_snapshot.as_dict()

    def _require_authoritative_snapshot(self) -> GoldValidationSummary:
        if self.authoritative_snapshot is None:
            raise RuntimeError("complete Gold validation must precede PostgreSQL sync")
        return self.authoritative_snapshot

    def sync_listings(self) -> dict[str, JSONValue]:
        outcome = self.runtime.publish_listings(
            self._require_listings(),
            fetched_at_by_key=cast(
                dict[tuple[str, str, str], datetime],
                self.fetched_at_by_dataset["listings"],
            ),
        )
        self.outcomes["listings"] = outcome
        return _sync_details(outcome)

    def sync_quotes(self) -> dict[str, JSONValue]:
        outcome = self.runtime.publish_quotes(
            self._require_quotes(),
            fetched_at_by_key=cast(
                dict[tuple[str, str, str, date], datetime],
                self.fetched_at_by_dataset["eod_quotes"],
            ),
            authoritative_snapshot=self._require_authoritative_snapshot(),
        )
        self.outcomes["eod_quotes"] = outcome
        return _sync_details(outcome)

    def sync_dividends(self) -> dict[str, JSONValue]:
        self._start_parallel_corporate_publication()
        outcome = self._publication_futures["dividends"].result()
        self.outcomes["dividends"] = outcome
        return _sync_details(outcome)

    def sync_splits(self) -> dict[str, JSONValue]:
        self._start_parallel_corporate_publication()
        outcome = self._publication_futures["splits"].result()
        self.outcomes["splits"] = outcome
        details = _sync_details(outcome)
        details["stale_listings_deleted"] = self.runtime.prune_stale_listings(
            self._require_listings()
        )
        executor = self._publication_executor
        self._publication_executor = None
        if executor is not None:
            executor.shutdown(wait=True)
        return details

    def verify(self) -> dict[str, JSONValue]:
        listings, quotes, dividends, splits = self._require_gold()
        try:
            verification = self.runtime.verify(
                listings,
                quotes,
                dividends,
                splits,
                self.outcomes,
            )
            if not verification.passed:
                raise RuntimeError("weekly PostgreSQL verification failed")
            return verification.as_dict()
        finally:
            executor = self._publication_executor
            self._publication_executor = None
            if executor is not None:
                executor.shutdown(wait=True)
            self.runtime.close()

    def _start_parallel_corporate_publication(self) -> None:
        if self._publication_futures:
            return
        database_worker = getattr(self.runtime, "database_worker", None)
        if not callable(database_worker):
            self._publication_futures = {
                "dividends": _completed_future(
                    self.runtime.publish_dividends(
                        self._require_dividends(),
                        fetched_at_by_key=cast(
                            dict[tuple[str, str, str, str], datetime],
                            self.fetched_at_by_dataset["dividends"],
                        ),
                        authoritative_snapshot=self._require_authoritative_snapshot(),
                    )
                ),
                "splits": _completed_future(
                    self.runtime.publish_splits(
                        self._require_splits(),
                        fetched_at_by_key=cast(
                            dict[tuple[str, str, str, str], datetime],
                            self.fetched_at_by_dataset["splits"],
                        ),
                        authoritative_snapshot=self._require_authoritative_snapshot(),
                    )
                ),
            }
            return

        self._publication_executor = ThreadPoolExecutor(
            max_workers=2,
            thread_name_prefix="xdl-postgres",
        )
        self._publication_futures = {
            "dividends": self._publication_executor.submit(
                _publish_dividends_worker,
                database_worker,
                self._require_dividends(),
                cast(
                    dict[tuple[str, str, str, str], datetime],
                    self.fetched_at_by_dataset["dividends"],
                ),
                self._require_authoritative_snapshot(),
            ),
            "splits": self._publication_executor.submit(
                _publish_splits_worker,
                database_worker,
                self._require_splits(),
                cast(
                    dict[tuple[str, str, str, str], datetime],
                    self.fetched_at_by_dataset["splits"],
                ),
                self._require_authoritative_snapshot(),
            ),
        }

    def rehydrate(self, checkpoint: Mapping[str, Mapping[str, JSONValue]]) -> None:
        """Restore completed Gold and sync state, refusing mismatched artifacts."""

        root = self.runtime._layout.root
        if "listings" in checkpoint:
            rows, fingerprint, _ = _load_gold_artifact(root, "listings")
            self.listings = build_listing_gold(
                deserialize_listings(canonical_json(cast(JSONValue, rows)))
            )
            _require_checkpoint_fingerprint(
                "listings", checkpoint, fingerprint, self.listings.semantic_fingerprint
            )
        if "quotes" in checkpoint:
            rows, fingerprint, _ = _load_gold_artifact(root, "eod_quotes")
            self.quotes = build_quote_gold(_quotes_from_rows(rows))
            _require_checkpoint_fingerprint(
                "quotes", checkpoint, fingerprint, self.quotes.semantic_fingerprint
            )
        if "dividends" in checkpoint:
            rows, fingerprint, retracted = _load_gold_artifact(root, "dividends")
            dividend_rows = _dividends_from_rows(rows)
            computed = _corporate_fingerprint(rows, retracted)
            _require_checkpoint_fingerprint("dividends", checkpoint, fingerprint, computed)
            self.dividends = DividendGoldResult(
                dividend_rows,
                retracted,
                len(dividend_rows),
                computed,
            )
        if "splits" in checkpoint:
            rows, fingerprint, retracted = _load_gold_artifact(root, "splits")
            split_rows = _splits_from_rows(rows)
            computed = _corporate_fingerprint(rows, retracted)
            _require_checkpoint_fingerprint("splits", checkpoint, fingerprint, computed)
            self.splits = SplitGoldResult(split_rows, retracted, len(split_rows), computed)
        for stage, dataset in (
            ("postgres_listings_sync", "listings"),
            ("postgres_quotes_sync", "eod_quotes"),
            ("postgres_dividends_sync", "dividends"),
            ("postgres_splits_sync", "splits"),
        ):
            if stage in checkpoint:
                self.outcomes[dataset] = _outcome_from_checkpoint(dataset, checkpoint[stage])
        for stage in ("dividends", "splits"):
            if stage in checkpoint:
                self.action_changed_listings.update(
                    _changed_listing_keys_from_checkpoint(checkpoint[stage])
                )
        for stage, dataset in (
            ("listings", "listings"),
            ("quotes", "eod_quotes"),
            ("dividends", "dividends"),
            ("splits", "splits"),
        ):
            if stage in checkpoint:
                self.fetched_at_by_dataset[dataset] = _fetch_times_from_checkpoint(
                    dataset,
                    checkpoint[stage],
                )
        if "gold_validation" in checkpoint:
            listings, quotes, dividends, splits = self._require_gold()
            self.authoritative_snapshot = validate_complete_gold(
                listings,
                quotes,
                dividends,
                splits,
            )
            expected = checkpoint["gold_validation"]
            if self.authoritative_snapshot.as_dict() != dict(expected):
                raise ValueError("checkpoint Gold validation does not match Gold artifacts")

    def _require_listings(self) -> ListingGoldResult:
        if self.listings is None:
            raise RuntimeError("listing Gold must exist before this stage")
        return self.listings

    def _record_fetch_times(
        self,
        dataset: str,
        rows: tuple[ListingRecord | QuoteRecord | DividendEvent | SplitEvent, ...],
        fetched_at_utc: datetime | None,
    ) -> None:
        if fetched_at_utc is None:
            raise ValueError(f"provider fetch time is required for {dataset}")
        self.fetched_at_by_dataset.setdefault(dataset, {}).update(
            {cast(object, row.key): fetched_at_utc for row in rows}
        )

    def _with_fetch_provenance(
        self,
        dataset: str,
        details: dict[str, JSONValue],
    ) -> dict[str, JSONValue]:
        entries: list[JSONValue] = []
        for key, value in sorted(
            self.fetched_at_by_dataset[dataset].items(),
            key=lambda item: str(item[0]),
        ):
            if not isinstance(key, tuple):
                raise ValueError(f"invalid fetch provenance key for {dataset}")
            entries.append(
                {
                    "key": [str(part) for part in key],
                    "fetched_at_utc": value.isoformat(),
                }
            )
        details["fetched_at_by_key"] = entries
        return details

    def _require_quotes(self) -> QuoteGoldResult:
        if self.quotes is None:
            raise RuntimeError("quote Gold must exist before this stage")
        return self.quotes

    def _require_dividends(self) -> DividendGoldResult:
        if self.dividends is None:
            raise RuntimeError("dividend Gold must exist before this stage")
        return self.dividends

    def _require_splits(self) -> SplitGoldResult:
        if self.splits is None:
            raise RuntimeError("split Gold must exist before this stage")
        return self.splits

    def _require_gold(
        self,
    ) -> tuple[ListingGoldResult, QuoteGoldResult, DividendGoldResult, SplitGoldResult]:
        return (
            self._require_listings(),
            self._require_quotes(),
            self._require_dividends(),
            self._require_splits(),
        )


def build_weekly_stages() -> PipelineStages:
    """Build the concrete factory referenced by the deployed Vienna cron entry."""

    state = _WeeklyState(
        PostgresEodhdBootstrapRuntime.from_environment(quarantine_invalid_ohlc=True),
        max_workers=resolve_weekly_workers(),
    )
    _LOGGER.info("weekly_configuration workers=%d", state.max_workers)
    return PipelineStages(
        listings=state.ingest_listings,
        quotes=state.ingest_quotes,
        dividends=state.ingest_dividends,
        splits=state.ingest_splits,
        gold_validation=state.validate_gold,
        postgres_listings_sync=state.sync_listings,
        postgres_quotes_sync=state.sync_quotes,
        postgres_dividends_sync=state.sync_dividends,
        postgres_splits_sync=state.sync_splits,
        verification=state.verify,
        rehydrate=state.rehydrate,
    )


def _sync_details(outcome: SyncOutcome) -> dict[str, JSONValue]:
    return {
        "run_id": outcome.run_id,
        "fingerprint": outcome.semantic_fingerprint,
        "row_count": outcome.row_count,
        "status": outcome.status,
        "inserted": outcome.counters.inserted,
        "updated": outcome.counters.updated,
        "deleted": outcome.counters.deleted,
        "retracted": outcome.counters.retracted,
    }


def _completed_future(value: SyncOutcome) -> Future[SyncOutcome]:
    future: Future[SyncOutcome] = Future()
    future.set_result(value)
    return future


def _publish_dividends_worker(
    database_worker: Callable[[], PostgresEodhdBootstrapRuntime],
    gold: DividendGoldResult,
    fetched_at_by_key: dict[tuple[str, str, str, str], datetime],
    authoritative_snapshot: GoldValidationSummary,
) -> SyncOutcome:
    worker = database_worker()
    try:
        return worker.publish_dividends(
            gold,
            fetched_at_by_key=fetched_at_by_key,
            authoritative_snapshot=authoritative_snapshot,
        )
    finally:
        worker.close()


def _publish_splits_worker(
    database_worker: Callable[[], PostgresEodhdBootstrapRuntime],
    gold: SplitGoldResult,
    fetched_at_by_key: dict[tuple[str, str, str, str], datetime],
    authoritative_snapshot: GoldValidationSummary,
) -> SyncOutcome:
    worker = database_worker()
    try:
        return worker.publish_splits(
            gold,
            fetched_at_by_key=fetched_at_by_key,
            authoritative_snapshot=authoritative_snapshot,
        )
    finally:
        worker.close()


def _ingest_details(rows: int, requests: int, fingerprint: str) -> dict[str, JSONValue]:
    return {"rows": rows, "requests": requests, "fingerprint": fingerprint}


def _parallel_fetch[JobT, ResultT](
    runtime: PostgresEodhdBootstrapRuntime,
    jobs: Sequence[JobT],
    fetch: Callable[[PostgresEodhdBootstrapRuntime, JobT], FetchBatch[ResultT]],
    max_workers: int,
) -> tuple[FetchBatch[ResultT], ...]:
    """Fetch independent provider jobs concurrently while preserving input order."""

    def run(job: JobT) -> FetchBatch[ResultT]:
        fetch_worker = getattr(runtime, "fetch_worker", None)
        for attempt in range(1, 4):
            try:
                worker = runtime if not callable(fetch_worker) else fetch_worker()
                return fetch(worker, job)
            except RuntimeError as exc:
                if attempt == 3:
                    raise
                _LOGGER.warning(
                    "provider_fetch_retry attempt=%d error=%s",
                    attempt,
                    exc,
                )
                time.sleep(float(2**attempt))
        raise AssertionError("provider fetch retry loop exhausted")

    def execute(worker_count: int) -> tuple[FetchBatch[ResultT], ...]:
        if worker_count == 1 or len(jobs) <= 1:
            return tuple(run(job) for job in jobs)
        with ThreadPoolExecutor(
            max_workers=worker_count, thread_name_prefix="xdl-fetch"
        ) as executor:
            return tuple(executor.map(run, jobs))

    try:
        return execute(max_workers)
    except RuntimeError:
        if max_workers <= _PROVIDER_STABLE_WORKERS:
            raise
        _LOGGER.warning(
            "provider_parallel_fallback requested_workers=%d fallback_workers=%d",
            max_workers,
            _PROVIDER_STABLE_WORKERS,
        )
        return execute(_PROVIDER_STABLE_WORKERS)


def _changed_listing_keys(keys: set[tuple[str, str, str]]) -> list[JSONValue]:
    return [list(key) for key in sorted(keys)]


def _changed_listing_keys_from_checkpoint(
    details: Mapping[str, JSONValue],
) -> tuple[tuple[str, str, str], ...]:
    raw_keys = details.get("changed_listing_keys", [])
    if not isinstance(raw_keys, list):
        raise ValueError("checkpoint action changes must be a list")
    keys: list[tuple[str, str, str]] = []
    for key in raw_keys:
        if (
            not isinstance(key, list)
            or len(key) != 3
            or not all(isinstance(part, str) for part in key)
        ):
            raise ValueError("checkpoint action change key is invalid")
        keys.append(cast(tuple[str, str, str], tuple(key)))
    if keys != sorted(set(keys)):
        raise ValueError("checkpoint action changes must be unique and sorted")
    return tuple(keys)


def _fetch_times_from_checkpoint(
    dataset: str,
    details: Mapping[str, JSONValue],
) -> dict[object, datetime]:
    raw = details.get("fetched_at_by_key")
    if not isinstance(raw, list):
        raise ValueError(f"checkpoint fetch provenance is missing for {dataset}")
    result: dict[object, datetime] = {}
    expected_size = 4 if dataset in {"eod_quotes", "dividends", "splits"} else 3
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError(f"checkpoint fetch provenance is invalid for {dataset}")
        raw_key = item.get("key")
        raw_time = item.get("fetched_at_utc")
        if (
            not isinstance(raw_key, list)
            or len(raw_key) != expected_size
            or not all(isinstance(part, str) for part in raw_key)
            or not isinstance(raw_time, str)
        ):
            raise ValueError(f"checkpoint fetch provenance is invalid for {dataset}")
        fetched_at = datetime.fromisoformat(raw_time)
        if fetched_at.tzinfo is None or fetched_at.utcoffset() != UTC.utcoffset(fetched_at):
            raise ValueError(f"checkpoint fetch provenance must be UTC for {dataset}")
        key: object
        if dataset == "eod_quotes":
            key = (
                cast(str, raw_key[0]),
                cast(str, raw_key[1]),
                cast(str, raw_key[2]),
                date.fromisoformat(cast(str, raw_key[3])),
            )
        else:
            key = tuple(raw_key)
        if key in result:
            raise ValueError(f"checkpoint fetch provenance has duplicate key for {dataset}")
        result[key] = fetched_at
    return result


def _load_gold_artifact(
    root: Path,
    dataset: str,
) -> tuple[list[dict[str, JSONValue]], str, tuple[tuple[str, str, str, str], ...]]:
    layout = MedallionLayout(root)
    payload = cast(
        JSONValue, json.loads((layout.dataset_path(Layer.GOLD, dataset) / "data.json").read_text())
    )
    if not isinstance(payload, list) or not all(isinstance(row, dict) for row in payload):
        raise ValueError(f"invalid Gold data artifact for {dataset}")
    rows = cast(list[dict[str, JSONValue]], payload)
    manifest_payload = cast(
        JSONValue, json.loads(layout.manifest_path(Layer.GOLD, dataset).read_text())
    )
    if not isinstance(manifest_payload, dict):
        raise ValueError(f"invalid Gold manifest for {dataset}")
    semantic_metadata = manifest_payload.get("semantic_metadata")
    if not isinstance(semantic_metadata, dict):
        raise ValueError(f"Gold manifest has no semantic metadata for {dataset}")
    fingerprint = semantic_metadata.get("builder_semantic_fingerprint")
    if not isinstance(fingerprint, str) or len(fingerprint) != 64:
        raise ValueError(f"Gold manifest has no builder fingerprint for {dataset}")
    manifest_fingerprint = manifest_payload.get("semantic_fingerprint")
    run_metadata = manifest_payload.get("run_metadata")
    if not isinstance(run_metadata, dict):
        raise ValueError(f"Gold manifest has invalid run metadata for {dataset}")
    expected_manifest = Manifest(dataset, Layer.GOLD, semantic_metadata, run_metadata)
    if manifest_fingerprint != expected_manifest.semantic_fingerprint():
        raise ValueError(f"Gold manifest semantic fingerprint mismatch for {dataset}")
    retracted: tuple[tuple[str, str, str, str], ...] = ()
    if dataset in {"dividends", "splits"}:
        retracted_payload = cast(
            JSONValue, json.loads(layout.retractions_path(Layer.GOLD, dataset).read_text())
        )
        if not isinstance(retracted_payload, list):
            raise ValueError(f"invalid Gold retractions for {dataset}")
        parsed: list[tuple[str, str, str, str]] = []
        for key in retracted_payload:
            if (
                not isinstance(key, list)
                or len(key) != 4
                or not all(isinstance(part, str) for part in key)
            ):
                raise ValueError(f"invalid Gold retraction key for {dataset}")
            parsed.append(cast(tuple[str, str, str, str], tuple(key)))
        if parsed != sorted(set(parsed)):
            raise ValueError(f"Gold retractions must be unique and sorted for {dataset}")
        retracted = tuple(parsed)
        sidecar_fingerprint = semantic_metadata.get("retractions_fingerprint")
        actual_sidecar_fingerprint = hashlib.sha256(
            canonical_json([list(key) for key in retracted]).encode("utf-8")
        ).hexdigest()
        if sidecar_fingerprint != actual_sidecar_fingerprint:
            raise ValueError(f"Gold retractions fingerprint mismatch for {dataset}")
    return rows, fingerprint, retracted


def _require_checkpoint_fingerprint(
    stage: str,
    checkpoint: Mapping[str, Mapping[str, JSONValue]],
    artifact_fingerprint: str,
    rebuilt_fingerprint: str,
) -> None:
    expected = checkpoint[stage].get("fingerprint")
    if (
        not isinstance(expected, str)
        or expected != artifact_fingerprint
        or expected != rebuilt_fingerprint
    ):
        raise ValueError(f"checkpoint fingerprint mismatch for {stage}")


def _corporate_fingerprint(
    rows: list[dict[str, JSONValue]],
    retracted: tuple[tuple[str, str, str, str], ...],
) -> str:
    payload: JSONValue = {
        "rows": cast(JSONValue, rows),
        "retracted_keys": [list(key) for key in retracted],
    }
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _required_text(row: Mapping[str, JSONValue], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Gold field {field} must be non-empty text")
    return value


def _optional_text(row: Mapping[str, JSONValue], field: str) -> str | None:
    value = row.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"Gold field {field} must be text or null")
    return value


def _optional_date(row: Mapping[str, JSONValue], field: str) -> date | None:
    value = _optional_text(row, field)
    return None if value is None else date.fromisoformat(value)


def _optional_decimal(row: Mapping[str, JSONValue], field: str) -> Decimal | None:
    value = _optional_text(row, field)
    return None if value is None else Decimal(value)


def _quotes_from_rows(rows: list[dict[str, JSONValue]]) -> tuple[QuoteRecord, ...]:
    return tuple(
        QuoteRecord(
            isin=_required_text(row, "isin"),
            exchange=_required_text(row, "exchange"),
            code=_required_text(row, "code"),
            trade_date=date.fromisoformat(_required_text(row, "trade_date")),
            open=_optional_decimal(row, "open"),
            high=_optional_decimal(row, "high"),
            low=_optional_decimal(row, "low"),
            close=Decimal(_required_text(row, "close")),
            adjusted_close=_optional_decimal(row, "adjusted_close"),
            volume=cast(int | None, row.get("volume")),
        )
        for row in rows
    )


def _load_silver_rows(root: Path, dataset: str) -> list[dict[str, JSONValue]]:
    path = MedallionLayout(root).dataset_path(Layer.SILVER, dataset) / "data.json"
    if not path.exists():
        return []
    payload = cast(JSONValue, json.loads(path.read_text(encoding="utf-8")))
    if not isinstance(payload, list) or not all(isinstance(row, dict) for row in payload):
        raise ValueError(f"invalid Silver data artifact for {dataset}")
    return cast(list[dict[str, JSONValue]], payload)


def _latest_date(
    records: tuple[QuoteRecord, ...] | tuple[DividendEvent, ...] | tuple[SplitEvent, ...],
) -> date | None:
    return max(
        (
            record.trade_date if isinstance(record, QuoteRecord) else record.event_date
            for record in records
        ),
        default=None,
    )


def _replace_quote_window(
    previous: tuple[QuoteRecord, ...],
    refreshed: tuple[QuoteRecord, ...],
    last_date: date | None,
) -> tuple[QuoteRecord, ...]:
    if last_date is None:
        return refreshed
    start = overlap_start(last_date)
    return tuple(record for record in previous if record.trade_date < start) + refreshed


def _replace_action_window[T: DividendEvent | SplitEvent](
    previous: tuple[T, ...],
    refreshed: tuple[T, ...],
    last_date: date | None,
) -> tuple[T, ...]:
    if last_date is None:
        return refreshed
    start = action_overlap_start(last_date)
    if start is None:
        return refreshed
    retained = (record for record in previous if record.event_date < start)
    by_key = {record.key: record for record in retained}
    by_key.update({record.key: record for record in refreshed})
    return tuple(
        sorted(by_key.values(), key=lambda record: (record.event_date, record.key, record.status))
    )


def _action_set_changed(
    previous: tuple[DividendEvent, ...] | tuple[SplitEvent, ...],
    merged: tuple[DividendEvent, ...] | tuple[SplitEvent, ...],
) -> bool:
    """Detect a provider-action change that can retroactively restate adjusted close."""

    return {(record.key, record.status.value) for record in previous} != {
        (record.key, record.status.value) for record in merged
    }


def _action_status(row: Mapping[str, JSONValue]) -> ActionStatus:
    value = row.get("status", ActionStatus.ACTIVE.value)
    try:
        return ActionStatus(cast(str, value))
    except ValueError as exc:
        raise ValueError("Gold action status is invalid") from exc


def _dividends_from_silver_rows(rows: list[dict[str, JSONValue]]) -> tuple[DividendEvent, ...]:
    events = tuple(
        DividendEvent(
            isin=_required_text(row, "isin"),
            exchange=_required_text(row, "exchange"),
            code=_required_text(row, "code"),
            event_date=date.fromisoformat(_required_text(row, "event_date")),
            value=Decimal(_required_text(row, "value")),
            currency=_optional_text(row, "currency"),
            period=_optional_text(row, "period"),
            declaration_date=_optional_date(row, "declaration_date"),
            record_date=_optional_date(row, "record_date"),
            payment_date=_optional_date(row, "payment_date"),
            status=_action_status(row),
        )
        for row in rows
    )
    if any(
        _required_text(row, "event_key") != event.event_key
        for row, event in zip(rows, events, strict=True)
    ):
        raise ValueError("Silver dividend event key mismatch")
    return events


def _splits_from_silver_rows(rows: list[dict[str, JSONValue]]) -> tuple[SplitEvent, ...]:
    events = tuple(
        SplitEvent(
            isin=_required_text(row, "isin"),
            exchange=_required_text(row, "exchange"),
            code=_required_text(row, "code"),
            event_date=date.fromisoformat(_required_text(row, "event_date")),
            split_ratio=_required_text(row, "split_ratio"),
            split_factor=_optional_decimal(row, "split_factor"),
            status=_action_status(row),
        )
        for row in rows
    )
    if any(
        _required_text(row, "event_key") != event.event_key
        for row, event in zip(rows, events, strict=True)
    ):
        raise ValueError("Silver split event key mismatch")
    return events


def _dividend_silver_row(event: DividendEvent) -> dict[str, JSONValue]:
    row = event.business_fields()
    row["event_key"] = event.event_key
    row["status"] = event.status.value
    return row


def _split_silver_row(event: SplitEvent) -> dict[str, JSONValue]:
    row = event.business_fields()
    row["event_key"] = event.event_key
    row["status"] = event.status.value
    return row


def _dividends_from_rows(rows: list[dict[str, JSONValue]]) -> tuple[DividendEvent, ...]:
    events = tuple(
        DividendEvent(
            isin=_required_text(row, "isin"),
            exchange=_required_text(row, "exchange"),
            code=_required_text(row, "code"),
            event_date=date.fromisoformat(_required_text(row, "event_date")),
            value=Decimal(_required_text(row, "value")),
            currency=_optional_text(row, "currency"),
            period=_optional_text(row, "period"),
            declaration_date=_optional_date(row, "declaration_date"),
            record_date=_optional_date(row, "record_date"),
            payment_date=_optional_date(row, "payment_date"),
        )
        for row in rows
    )
    if any(
        _required_text(row, "event_key") != event.event_key
        for row, event in zip(rows, events, strict=True)
    ):
        raise ValueError("Gold dividend event key mismatch")
    return events


def _splits_from_rows(rows: list[dict[str, JSONValue]]) -> tuple[SplitEvent, ...]:
    events = tuple(
        SplitEvent(
            isin=_required_text(row, "isin"),
            exchange=_required_text(row, "exchange"),
            code=_required_text(row, "code"),
            event_date=date.fromisoformat(_required_text(row, "event_date")),
            split_ratio=_required_text(row, "split_ratio"),
            split_factor=_optional_decimal(row, "split_factor"),
        )
        for row in rows
    )
    if any(
        _required_text(row, "event_key") != event.event_key
        for row, event in zip(rows, events, strict=True)
    ):
        raise ValueError("Gold split event key mismatch")
    return events


def _outcome_from_checkpoint(dataset: str, details: Mapping[str, JSONValue]) -> SyncOutcome:
    run_id = _required_text(details, "run_id")
    fingerprint = _required_text(details, "fingerprint")
    status = _required_text(details, "status")
    row_count = details.get("row_count")
    counters = tuple(details.get(name) for name in ("inserted", "updated", "deleted", "retracted"))
    if not isinstance(row_count, int) or not all(isinstance(value, int) for value in counters):
        raise ValueError(f"invalid sync checkpoint for {dataset}")
    return SyncOutcome(
        run_id,
        dataset,
        fingerprint,
        row_count,
        status,
        SyncCounters(
            inserted=cast(int, counters[0]),
            updated=cast(int, counters[1]),
            deleted=cast(int, counters[2]),
            retracted=cast(int, counters[3]),
        ),
    )
