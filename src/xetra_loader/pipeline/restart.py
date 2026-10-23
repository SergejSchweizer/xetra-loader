"""Single-run locking and restart checkpoints for the weekly loader."""

from __future__ import annotations

import ctypes
import errno
import fcntl
import json
import logging
import os
import sys
import time
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager, ExitStack
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import TextIO

import polars as pl

from xetra_loader.medallion.core import atomic_write_parquet, migrate_json_manifests
from xetra_loader.pipeline.orchestrator import (
    PipelineStageError,
    PipelineStages,
    PipelineSummary,
    run_weekly_pipeline,
)
from xetra_loader.sync.core import JSONValue

_LOGGER = logging.getLogger(__name__)

type RestartStage = Callable[[], Mapping[str, JSONValue] | None]
type WrappedStage = Callable[[], dict[str, JSONValue]]

_STAGE_NAMES = (
    "listings",
    "dividends",
    "splits",
    "quotes",
    "gold_validation",
    "postgres_listings_sync",
    "postgres_quotes_sync",
    "postgres_dividends_sync",
    "postgres_splits_sync",
    "verification",
)


class ConcurrentLoaderRunError(RuntimeError):
    """Raised when another loader process already owns the run lock."""


@dataclass(frozen=True, slots=True)
class RestartCheckpoint:
    """Non-semantic, fail-closed evidence for a resumable weekly process."""

    completed: tuple[str, ...]
    details: Mapping[str, Mapping[str, JSONValue]]


@dataclass(slots=True)
class LoaderLock(AbstractContextManager["LoaderLock"]):
    """Advisory process lock automatically released on exit or process death."""

    path: Path
    _handle: TextIO | None = None

    def __enter__(self) -> LoaderLock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            handle.close()
            raise ConcurrentLoaderRunError(f"loader lock is already held: {self.path}") from exc
        handle.seek(0)
        handle.truncate()
        handle.write("locked\n")
        handle.flush()
        self._handle = handle
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        handle = self._handle
        if handle is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()
            self._handle = None


def run_restartable_pipeline(
    stages: PipelineStages,
    *,
    lock_path: Path,
    checkpoint_path: Path,
    authoritative_lock_held: bool = False,
) -> PipelineSummary:
    """Run once with rewrite and weekly locks, then clear checkpoint on success.

    The authoritative lock is always acquired before the weekly lock. A caller
    performing a larger rewrite may pass ``authoritative_lock_held=True`` and
    retain that outer lock while this function acquires the weekly lock.
    """

    with ExitStack() as locks:
        if not authoritative_lock_held:
            locks.enter_context(LoaderLock(lock_path.parent / "authoritative-rewrite.lock"))
        locks.enter_context(LoaderLock(lock_path))
        migrated_manifests = migrate_json_manifests(checkpoint_path.parent)
        if migrated_manifests:
            _LOGGER.info(
                "lake_control_metadata_migrated manifests=%d format=parquet",
                migrated_manifests,
            )
        migrated_archives = _migrate_aborted_checkpoint_archives(checkpoint_path.parent)
        if migrated_archives:
            _LOGGER.info(
                "lake_control_metadata_migrated archived_checkpoints=%d format=parquet",
                migrated_archives,
            )
        checkpoint = _read_checkpoint(checkpoint_path)
        if checkpoint.completed:
            if stages.rehydrate is None:
                raise ValueError("checkpoint requires a runtime rehydration hook")
            stages.rehydrate(checkpoint.details)
        wrapped = _wrap_stages(stages, checkpoint, checkpoint_path)
        try:
            summary = run_weekly_pipeline(wrapped)
        except PipelineStageError as exc:
            if exc.stage == "verification" and checkpoint_path.exists():
                persisted = _read_checkpoint(checkpoint_path)
                publication_start = _STAGE_NAMES.index("postgres_listings_sync")
                prefix = tuple(
                    name for name in _STAGE_NAMES[:publication_start] if name in persisted.completed
                )
                _write_checkpoint(
                    checkpoint_path,
                    RestartCheckpoint(
                        prefix,
                        {name: persisted.details[name] for name in prefix},
                    ),
                )
                _LOGGER.warning(
                    "verification_failed checkpoint_rewound completed=%s",
                    ",".join(prefix),
                )
            raise
        checkpoint_path.unlink(missing_ok=True)
        return summary


def _wrap_stages(
    stages: PipelineStages,
    checkpoint: RestartCheckpoint,
    checkpoint_path: Path,
) -> PipelineStages:
    completed = set(checkpoint.completed)
    details_by_stage = {name: dict(details) for name, details in checkpoint.details.items()}

    def wrap(name: str, stage: RestartStage) -> WrappedStage:
        def run() -> dict[str, JSONValue]:
            if name in completed:
                _LOGGER.debug("stage_checkpoint_skip name=%s", name)
                return {"restart": "checkpoint-skip"}
            started = time.perf_counter()
            _LOGGER.debug("stage_start name=%s", name)
            try:
                details = stage()
            except Exception as exc:
                _LOGGER.error(
                    "stage_failed name=%s error_type=%s",
                    name,
                    type(exc).__name__,
                )
                raise
            if name in {"listings", "dividends", "splits", "quotes"}:
                # Bronze partitions are replayable until the stage checkpoint.
                # Flush them once per stage rather than fsyncing every small file.
                _sync_lake_filesystem(checkpoint_path.parent)
            completed.add(name)
            stage_details = {} if details is None else dict(details)
            details_by_stage[name] = stage_details
            ordered_completed = tuple(name for name in _STAGE_NAMES if name in completed)
            _write_checkpoint(
                checkpoint_path,
                RestartCheckpoint(ordered_completed, details_by_stage),
            )
            _LOGGER.debug(
                "stage_success name=%s elapsed_seconds=%.3f",
                name,
                time.perf_counter() - started,
            )
            return stage_details

        return run

    return PipelineStages(
        listings=wrap("listings", stages.listings),
        quotes=wrap("quotes", stages.quotes),
        dividends=wrap("dividends", stages.dividends),
        splits=wrap("splits", stages.splits),
        gold_validation=wrap("gold_validation", stages.gold_validation),
        postgres_listings_sync=wrap("postgres_listings_sync", stages.postgres_listings_sync),
        postgres_quotes_sync=wrap("postgres_quotes_sync", stages.postgres_quotes_sync),
        postgres_dividends_sync=wrap("postgres_dividends_sync", stages.postgres_dividends_sync),
        postgres_splits_sync=wrap("postgres_splits_sync", stages.postgres_splits_sync),
        verification=wrap("verification", stages.verification),
        rehydrate=stages.rehydrate,
    )


def _read_checkpoint(path: Path) -> RestartCheckpoint:
    legacy_path = path.with_suffix(".json")
    if path.exists():
        return _checkpoint_from_payload(_read_checkpoint_parquet(path))
    if not legacy_path.exists():
        return RestartCheckpoint((), {})

    checkpoint = _checkpoint_from_payload(json.loads(legacy_path.read_text(encoding="utf-8")))
    _write_checkpoint(path, checkpoint)
    if _checkpoint_from_payload(_read_checkpoint_parquet(path)) != checkpoint:
        raise ValueError("loader checkpoint Parquet verification failed")
    legacy_path.unlink()
    _LOGGER.info("checkpoint_migrated format=parquet")
    return checkpoint


def _read_checkpoint_parquet(path: Path) -> dict[str, object]:
    rows = pl.read_parquet(path, low_memory=True).to_dicts()
    if len(rows) != 1:
        raise ValueError("invalid loader checkpoint Parquet row count")
    return rows[0]


def _checkpoint_from_payload(payload: object) -> RestartCheckpoint:
    if not isinstance(payload, dict) or payload.get("version") != 1:
        raise ValueError("invalid loader checkpoint")
    completed = _validated_completed_stages(payload.get("completed"))
    raw_details = payload.get("details")
    if not isinstance(raw_details, dict) or set(raw_details) != completed:
        raise ValueError("loader checkpoint details must match completed stages")
    details: dict[str, Mapping[str, JSONValue]] = {}
    for name, value in raw_details.items():
        if not isinstance(value, dict):
            raise ValueError("invalid loader checkpoint stage detail")
        details[name] = value
    return RestartCheckpoint(
        tuple(name for name in _STAGE_NAMES if name in completed), details
    )


def _validated_completed_stages(raw_completed: object) -> set[str]:
    if not isinstance(raw_completed, list) or not all(
        isinstance(item, str) for item in raw_completed
    ):
        raise ValueError("invalid loader checkpoint completed stages")
    completed = set(raw_completed)
    if not completed.issubset(_STAGE_NAMES):
        raise ValueError("loader checkpoint contains unknown stage")
    expected_prefix = set(_STAGE_NAMES[: len(completed)])
    if completed != expected_prefix:
        raise ValueError("loader checkpoint stages must form an ordered prefix")
    return completed


def _migrate_aborted_checkpoint_archives(root: Path) -> int:
    """Convert preserved historical checkpoint JSON files to Parquet rows."""

    migrated = 0
    for source in sorted(root.glob("weekly.checkpoint.aborted*.json")):
        payload = json.loads(source.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"invalid archived checkpoint metadata: {source}")
        target = source.with_suffix(".parquet")
        if not target.exists():
            atomic_write_parquet(
                target,
                pl.DataFrame([payload], infer_schema_length=None, strict=False),
            )
        rows = pl.read_parquet(target, low_memory=True).to_dicts()
        if len(rows) != 1 or rows[0] != payload:
            raise ValueError(f"archived checkpoint Parquet verification failed: {target}")
        source.unlink()
        migrated += 1
    return migrated


def _write_checkpoint(path: Path, checkpoint: RestartCheckpoint) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "completed": list(checkpoint.completed),
        "details": {name: dict(checkpoint.details[name]) for name in checkpoint.completed},
    }
    atomic_write_parquet(
        path,
        pl.DataFrame([payload], infer_schema_length=None, strict=False),
    )


def _sync_lake_filesystem(path: Path) -> None:
    """Durably flush replayable lake writes once before advancing a checkpoint."""

    if sys.platform.startswith("linux"):
        library = ctypes.CDLL(None, use_errno=True)
        syncfs = getattr(library, "syncfs", None)
        if syncfs is None:
            raise OSError(errno.ENOSYS, "syncfs is unavailable")
        syncfs.argtypes = [ctypes.c_int]
        syncfs.restype = ctypes.c_int
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            if syncfs(descriptor) != 0:
                error_number = ctypes.get_errno()
                raise OSError(error_number, os.strerror(error_number), str(path))
        finally:
            os.close(descriptor)
    else:
        os.sync()
