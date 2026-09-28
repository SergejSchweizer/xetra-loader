"""Single-run locking and restart checkpoints for the weekly loader."""

from __future__ import annotations

import fcntl
import json
import logging
import time
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import TextIO

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
        return None


def run_restartable_pipeline(
    stages: PipelineStages,
    *,
    lock_path: Path,
    checkpoint_path: Path,
) -> PipelineSummary:
    """Run once, resume after the last completed stage, and clear checkpoints on success."""

    with LoaderLock(lock_path):
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
            except Exception:
                _LOGGER.exception("stage_failed name=%s", name)
                raise
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
    if not path.exists():
        return RestartCheckpoint((), {})
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("version") != 1:
        raise ValueError("invalid loader checkpoint")
    raw_completed = payload.get("completed")
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
    raw_details = payload.get("details")
    if not isinstance(raw_details, dict):
        raise ValueError("invalid loader checkpoint stage details")
    if set(raw_details) != completed:
        raise ValueError("loader checkpoint details must match completed stages")
    details: dict[str, Mapping[str, JSONValue]] = {}
    for name, value in raw_details.items():
        if not isinstance(value, dict):
            raise ValueError("invalid loader checkpoint stage detail")
        details[name] = value
    return RestartCheckpoint(tuple(name for name in _STAGE_NAMES if name in completed), details)


def _write_checkpoint(path: Path, checkpoint: RestartCheckpoint) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "completed": list(checkpoint.completed),
        "details": {name: dict(checkpoint.details[name]) for name in checkpoint.completed},
    }
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)
