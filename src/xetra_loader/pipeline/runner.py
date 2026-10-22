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
from xetra_loader.pipeline.restart import ConcurrentLoaderRunError, run_restartable_pipeline

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
    if stages_factory is None:
        # Set this before importing Polars; its native pool is initialized once.
        os.environ["POLARS_MAX_THREADS"] = str(cores)
        os.environ["XDL_CORES"] = str(cores)
        from xetra_loader.pipeline.runtime import build_weekly_stages

        stages_factory = build_weekly_stages
    log_path = configure_logging("xdl-weekly")
    root = (medallion_root or Path(resolve_medallion_root())).resolve()
    _LOGGER.info("weekly_start root=%s log=%s cores=%d", root, log_path, cores)
    try:
        summary = run_restartable_pipeline(
            stages_factory(cores),
            lock_path=root / "weekly.lock",
            checkpoint_path=root / "weekly.checkpoint.json",
        )
    except ConcurrentLoaderRunError as exc:
        _LOGGER.error("weekly_blocked reason=%s", exc)
        print(json.dumps({"status": "blocked", "reason": str(exc)}, sort_keys=True))
        return 2
    except PipelineStageError as exc:
        _LOGGER.error("weekly_failed stage=%s error=%s", exc.stage, exc)
        print(json.dumps(exc.summary.as_dict(), sort_keys=True, separators=(",", ":")))
        return 1
    _LOGGER.info("weekly_success stages=%d", len(summary.reports))
    print(json.dumps(summary.as_dict(), sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
