"""Non-interactive guarded production entry point for the weekly pipeline."""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable, Sequence
from logging.handlers import RotatingFileHandler
from pathlib import Path

from xetra_loader.config import resolve_medallion_root
from xetra_loader.pipeline.orchestrator import PipelineStageError, PipelineStages
from xetra_loader.pipeline.restart import ConcurrentLoaderRunError, run_restartable_pipeline
from xetra_loader.pipeline.runtime import build_weekly_stages

_LOGGER = logging.getLogger("xetra_loader")


def _configure_logging() -> Path:
    """Configure secret-free DEBUG logging in the ignored `.logs` directory."""

    log_dir = Path(os.getenv("XDL_LOG_DIR", ".logs")).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "xdl-weekly.log"
    for handler in _LOGGER.handlers:
        if (
            isinstance(handler, logging.FileHandler)
            and Path(handler.baseFilename).resolve() == log_path
        ):
            return log_path
    handler = RotatingFileHandler(
        log_path,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
        delay=True,
    )
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    _LOGGER.setLevel(logging.DEBUG)
    _LOGGER.propagate = False
    _LOGGER.addHandler(handler)
    return log_path


type StagesFactory = Callable[[], PipelineStages]


def main(
    argv: Sequence[str] | None = None,
    *,
    stages_factory: StagesFactory = build_weekly_stages,
    medallion_root: Path | None = None,
) -> int:
    """Run exactly one locked weekly pipeline using paths beneath the Medallion root."""

    if argv:
        raise ValueError("xdl-weekly does not accept command-line arguments")
    log_path = _configure_logging()
    root = (medallion_root or Path(resolve_medallion_root())).resolve()
    _LOGGER.info("weekly_start root=%s log=%s", root, log_path)
    try:
        summary = run_restartable_pipeline(
            stages_factory(),
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
