import os
import sys
from pathlib import Path
from types import ModuleType

from xetra_loader.pipeline.orchestrator import PipelineStages
from xetra_loader.pipeline.runner import main


def _stages(calls: list[str]) -> PipelineStages:
    def stage(name: str):
        def run() -> dict[str, str]:
            calls.append(name)
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
    )


def test_weekly_runner_uses_medallion_root_and_core_budget(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    budgets: list[int] = []

    def stages(cores: int):
        budgets.append(cores)
        return _stages(calls)

    assert main(
        ["--cores", "4"], stages_factory=stages, medallion_root=tmp_path
    ) == 0
    assert budgets == [4]
    assert calls == [
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
    ]
    assert (tmp_path / "weekly.lock").exists()
    assert not (tmp_path / "weekly.checkpoint.json").exists()


def test_weekly_runner_writes_debug_log(tmp_path: Path, monkeypatch) -> None:
    log_dir = tmp_path / "logs"
    monkeypatch.setenv("XDL_LOG_DIR", str(log_dir))

    assert main(
        [], stages_factory=lambda cores: _stages([]), medallion_root=tmp_path / "lake"
    ) == 0

    log = (log_dir / "xdl-weekly.log").read_text(encoding="utf-8")
    assert "weekly_start" in log
    assert "stage_success name=verification" in log


def test_polars_thread_limit_is_set_before_runtime_import(
    tmp_path: Path,
    monkeypatch,
) -> None:
    observed: list[tuple[int, str | None]] = []
    fake_runtime = ModuleType("xetra_loader.pipeline.runtime")

    def build_stages(cores: int) -> PipelineStages:
        observed.append((cores, os.getenv("POLARS_MAX_THREADS")))
        return _stages([])

    fake_runtime.build_weekly_stages = build_stages
    monkeypatch.setitem(sys.modules, "xetra_loader.pipeline.runtime", fake_runtime)
    monkeypatch.delenv("POLARS_MAX_THREADS", raising=False)

    assert main(["--cores", "3"], medallion_root=tmp_path) == 0
    assert observed == [(3, "3")]
