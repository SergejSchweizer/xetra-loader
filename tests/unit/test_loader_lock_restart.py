import json
from collections import Counter
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import polars as pl
import pytest

from xetra_loader.pipeline.orchestrator import PipelineStageError, PipelineStages
from xetra_loader.pipeline.restart import (
    ConcurrentLoaderRunError,
    LoaderLock,
    _migrate_aborted_checkpoint_archives,
    _read_checkpoint,
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


def test_legacy_json_checkpoint_migrates_to_parquet_and_is_removed(
    tmp_path: Path,
) -> None:
    checkpoint_path = tmp_path / "weekly.checkpoint.parquet"
    legacy_path = checkpoint_path.with_suffix(".json")
    legacy_path.write_text(
        json.dumps(
            {
                "version": 1,
                "completed": ["listings"],
                "details": {"listings": {"rows": 1, "fingerprint": "a" * 64}},
            }
        ),
        encoding="utf-8",
    )

    checkpoint = _read_checkpoint(checkpoint_path)

    assert checkpoint.completed == ("listings",)
    assert checkpoint.details == {"listings": {"rows": 1, "fingerprint": "a" * 64}}
    assert checkpoint_path.is_file()
    assert not legacy_path.exists()


def test_aborted_json_checkpoint_archive_migrates_to_parquet(tmp_path: Path) -> None:
    source = tmp_path / "weekly.checkpoint.aborted-run.json"
    source.write_text('{"version":1,"completed":["listings"]}', encoding="utf-8")

    assert _migrate_aborted_checkpoint_archives(tmp_path) == 1

    target = source.with_suffix(".parquet")
    assert not source.exists()
    assert pl.read_parquet(target).to_dicts() == [
        {"version": 1, "completed": ["listings"]}
    ]


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


def test_authoritative_rewrite_lock_blocks_weekly_pipeline(tmp_path: Path) -> None:
    authoritative_lock = tmp_path / "authoritative-rewrite.lock"
    with LoaderLock(authoritative_lock):
        with pytest.raises(ConcurrentLoaderRunError):
            run_restartable_pipeline(
                _stages(Counter(), set()),
                lock_path=tmp_path / "weekly.lock",
                checkpoint_path=tmp_path / "weekly.checkpoint.parquet",
            )


def test_nested_full_refresh_replay_reuses_authoritative_lock(tmp_path: Path) -> None:
    authoritative_lock = tmp_path / "authoritative-rewrite.lock"
    with LoaderLock(authoritative_lock):
        summary = run_restartable_pipeline(
            _stages(Counter(), set()),
            lock_path=tmp_path / "weekly.lock",
            checkpoint_path=tmp_path / "weekly.checkpoint.parquet",
            authoritative_lock_held=True,
        )

    assert summary.succeeded


def test_failed_run_resumes_without_repeating_completed_stages(tmp_path: Path) -> None:
    calls: Counter[str] = Counter()
    stages = _stages(calls, {"dividends"})
    lock_path = tmp_path / "loader.lock"
    checkpoint_path = tmp_path / "checkpoint.parquet"

    with pytest.raises(PipelineStageError):
        run_restartable_pipeline(
            stages,
            lock_path=lock_path,
            checkpoint_path=checkpoint_path,
        )

    assert checkpoint_path.exists()
    assert not list(tmp_path.rglob("*.json"))
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


def test_failed_stage_log_redacts_exception_text(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "api_token=credential-must-not-leak"
    stages = _stages(Counter(), set())

    def fail_with_secret() -> dict[str, JSONValue]:
        raise RuntimeError(secret)

    caplog.set_level("ERROR", logger="xetra_loader.pipeline.restart")
    with pytest.raises(PipelineStageError):
        run_restartable_pipeline(
            replace(stages, dividends=fail_with_secret),
            lock_path=tmp_path / "weekly.lock",
            checkpoint_path=tmp_path / "weekly.checkpoint.parquet",
        )

    assert secret not in caplog.text
    assert "stage_failed name=dividends error_type=RuntimeError" in caplog.text


def test_checkpoint_without_fresh_runtime_rehydration_fails_closed(tmp_path: Path) -> None:
    calls: Counter[str] = Counter()
    checkpoint_path = tmp_path / "checkpoint.parquet"
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
    checkpoint_path = tmp_path / "checkpoint.parquet"
    with pytest.raises(PipelineStageError):
        run_restartable_pipeline(
            _stages(calls, {"verification"}),
            lock_path=tmp_path / "loader.lock",
            checkpoint_path=checkpoint_path,
        )

    assert checkpoint_path.exists()
    assert calls["verification"] == 1
    assert pl.read_parquet(checkpoint_path).to_dicts()[0]["completed"] == [
        "listings",
        "dividends",
        "splits",
        "quotes",
        "gold_validation",
    ]
