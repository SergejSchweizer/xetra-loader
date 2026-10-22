"""Shared secret-conscious file logging for XETRA loader entry points."""

from __future__ import annotations

import logging
import os
import re
import time
from datetime import timedelta
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

_PACKAGE_LOGGER = logging.getLogger("xetra_loader")
_LOG_RETENTION = timedelta(days=90)
_COMPONENT_NAME = re.compile(r"[a-z0-9][a-z0-9-]*")


def _prune_expired_logs(log_dir: Path, log_name: str, now: float | None = None) -> None:
    """Delete only this component's active/rotated logs older than 90 days."""

    cutoff = (time.time() if now is None else now) - _LOG_RETENTION.total_seconds()
    prefix = f"{log_name}."
    for candidate in log_dir.iterdir():
        if candidate.name != log_name and not candidate.name.startswith(prefix):
            continue
        if candidate.is_symlink() or not candidate.is_file():
            continue
        try:
            if candidate.stat().st_mtime < cutoff:
                candidate.unlink()
        except FileNotFoundError:
            continue


class _ThreeMonthTimedRotatingFileHandler(TimedRotatingFileHandler):
    """Rotate at UTC midnight and prune by age rather than backup-file count."""

    xdl_log_component: str

    def doRollover(self) -> None:
        super().doRollover()
        _prune_expired_logs(
            Path(self.baseFilename).parent,
            Path(self.baseFilename).name,
        )


def configure_logging(component: str) -> Path:
    """Capture DEBUG records from every package module in a private log file."""

    if _COMPONENT_NAME.fullmatch(component) is None:
        raise ValueError(
            "logging component must contain only lowercase letters, digits, and hyphens"
        )

    log_dir = Path(os.getenv("XDL_LOG_DIR", ".logs")).resolve()
    log_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    log_dir.chmod(0o700)
    log_path = log_dir / f"{component}.log"

    for handler in tuple(_PACKAGE_LOGGER.handlers):
        if getattr(handler, "xdl_log_component", None) == component:
            if (
                isinstance(handler, logging.FileHandler)
                and Path(handler.baseFilename).resolve() == log_path
            ):
                return log_path
            _PACKAGE_LOGGER.removeHandler(handler)
            handler.close()

    _prune_expired_logs(log_dir, log_path.name)
    handler = _ThreeMonthTimedRotatingFileHandler(
        log_path,
        when="midnight",
        interval=1,
        backupCount=0,
        encoding="utf-8",
        delay=False,
        utc=True,
    )
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    )
    handler.xdl_log_component = component
    os.chmod(log_path, 0o600)
    _PACKAGE_LOGGER.setLevel(logging.DEBUG)
    _PACKAGE_LOGGER.propagate = False
    _PACKAGE_LOGGER.addHandler(handler)
    return log_path


def shutdown_logging(component: str) -> None:
    """Flush and detach a component's handler during orderly service shutdown."""

    for handler in tuple(_PACKAGE_LOGGER.handlers):
        if getattr(handler, "xdl_log_component", None) == component:
            _PACKAGE_LOGGER.removeHandler(handler)
            handler.close()
