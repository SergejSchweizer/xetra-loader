import json
from collections import Counter
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import pytest

from xetra_loader.pipeline.orchestrator import PipelineStageError, PipelineStages
from xetra_loader.pipeline.restart import (
    ConcurrentLoaderRunError,
    LoaderLock,
    run_restartable_pipeline,
)
from xetra_loader.sync.core import JSONValue

STAGE_NAMES = (
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


def _stages(
    calls: Counter[str],
    fail_once: set[str],
    rehydrated: list[dict[str, dict[str, JSONValue]]] | None = None,
) -> PipelineStages:
    def stage(name: str) -> Callable[[], dict[str, JSONValue]]:
        def run() -> dict[str, JSONValue]:
            calls[name] += 1
            if name in fail_once:
                fail_once.remove(name)
                raise RuntimeError(f"fail-{name}")
            return {"stage": name}

        return run

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
        rehydrate=lambda details: (
            rehydrated.append(dict(details)) if rehydrated is not None else None
        ),
    )


def test_second_concurrent_run_is_denied_and_lock_releases(tmp_path: Path) -> None:
    lock_path = tmp_path / "loader.lock"

    with (
        LoaderLock(lock_path),
        pytest.raises(ConcurrentLoaderRunError),
        LoaderLock(lock_path),
    ):
        raise AssertionError("unreachable")

    with LoaderLock(lock_path):
        pass


def test_failed_run_resumes_without_repeating_completed_stages(tmp_path: Path) -> None:
    calls: Counter[str] = Counter()
    stages = _stages(calls, {"dividends"})
    lock_path = tmp_path / "loader.lock"
    checkpoint_path = tmp_path / "checkpoint.json"

    with pytest.raises(PipelineStageError):
        run_restartable_pipeline(
            stages,
            lock_path=lock_path,
            checkpoint_path=checkpoint_path,
        )

    assert checkpoint_path.exists()
    assert calls["listings"] == 1
    assert calls["dividends"] == 1
    assert calls["quotes"] == 0
    assert calls["splits"] == 0

    rehydrated: list[dict[str, dict[str, JSONValue]]] = []
    summary = run_restartable_pipeline(
        _stages(calls, set(), rehydrated),
        lock_path=lock_path,
        checkpoint_path=checkpoint_path,
    )

    assert summary.succeeded
    assert not checkpoint_path.exists()
    assert calls["listings"] == 1
    assert calls["quotes"] == 1
    assert calls["dividends"] == 2
    assert all(calls[name] == 1 for name in STAGE_NAMES[2:])
    assert rehydrated == [
        {
            "listings": {"stage": "listings"},
        }
    ]


def test_checkpoint_without_fresh_runtime_rehydration_fails_closed(tmp_path: Path) -> None:
    calls: Counter[str] = Counter()
    checkpoint_path = tmp_path / "checkpoint.json"
    with pytest.raises(PipelineStageError):
        run_restartable_pipeline(
            _stages(calls, {"dividends"}),
            lock_path=tmp_path / "loader.lock",
            checkpoint_path=checkpoint_path,
        )

    with pytest.raises(ValueError, match="rehydration hook"):
        run_restartable_pipeline(
            replace(_stages(calls, set()), rehydrate=None),
            lock_path=tmp_path / "loader.lock",
            checkpoint_path=checkpoint_path,
        )
    assert calls["splits"] == 0


def test_verification_failure_rewinds_publication_stages(tmp_path: Path) -> None:
    calls: Counter[str] = Counter()
    checkpoint_path = tmp_path / "checkpoint.json"
    with pytest.raises(PipelineStageError):
        run_restartable_pipeline(
            _stages(calls, {"verification"}),
            lock_path=tmp_path / "loader.lock",
            checkpoint_path=checkpoint_path,
        )

    assert checkpoint_path.exists()
    assert calls["verification"] == 1
    assert json.loads(checkpoint_path.read_text(encoding="utf-8"))["completed"] == [
        "listings",
        "dividends",
        "splits",
        "quotes",
        "gold_validation",
    ]
