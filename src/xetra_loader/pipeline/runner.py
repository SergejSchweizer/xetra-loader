"""Non-interactive guarded production entry point for the weekly pipeline."""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections.abc import Callable, Sequence
from pathlib import Path

from xetra_loader.config import resolve_cores, resolve_medallion_root
from xetra_loader.logging_config import configure_logging
from xetra_loader.pipeline.orchestrator import PipelineStageError, PipelineStages

_LOGGER = logging.getLogger("xetra_loader")


type StagesFactory = Callable[[int], PipelineStages]


def main(
    argv: Sequence[str] | None = None,
    *,
    stages_factory: StagesFactory | None = None,
    medallion_root: Path | None = None,
) -> int:
    """Run one locked pipeline using the same bounded CPU budget for each engine."""

    parser = argparse.ArgumentParser(prog="xdl-weekly")
    parser.add_argument(
        "--cores",
        type=int,
        default=None,
        help="CPU/concurrency budget (default: weekly.cores or 6)",
    )
    arguments = parser.parse_args(argv)
    cores = resolve_cores(arguments.cores)
    # Polars reads this once when its native thread pool is initialized. Set it
    # before importing restart/runtime modules, including in injected runs.
    os.environ["POLARS_MAX_THREADS"] = str(cores)
    os.environ["XDL_CORES"] = str(cores)
    import polars as pl

    polars_threads = pl.thread_pool_size()
    if polars_threads != cores:
        _LOGGER.warning(
            "polars_thread_pool_already_initialized requested=%d actual=%d",
            cores,
            polars_threads,
        )
    if stages_factory is None:
        from xetra_loader.pipeline.runtime import build_weekly_stages

        stages_factory = build_weekly_stages
    from xetra_loader.pipeline.restart import (
        ConcurrentLoaderRunError,
        run_restartable_pipeline,
    )

    log_path = configure_logging("xdl-weekly")
    root = (medallion_root or Path(resolve_medallion_root())).resolve()
    _LOGGER.info(
        "weekly_start root=%s log=%s cores=%d polars_threads=%d",
        root,
        log_path,
        cores,
        polars_threads,
    )
    try:
        summary = run_restartable_pipeline(
            stages_factory(cores),
            lock_path=root / "weekly.lock",
            checkpoint_path=root / "weekly.checkpoint.parquet",
        )
    except ConcurrentLoaderRunError:
        _LOGGER.error("weekly_blocked reason=lock_conflict")
        print(json.dumps({"status": "blocked", "reason": "lock_conflict"}, sort_keys=True))
        return 2
    except PipelineStageError as exc:
        cause_type = type(exc.__cause__).__name__ if exc.__cause__ is not None else "unknown"
        _LOGGER.error(
            "weekly_failed stage=%s error_type=%s",
            exc.stage,
            cause_type,
        )
        print(json.dumps(exc.summary.as_dict(), sort_keys=True, separators=(",", ":")))
        return 1
    _LOGGER.info("weekly_success stages=%d", len(summary.reports))
    print(json.dumps(summary.as_dict(), sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
