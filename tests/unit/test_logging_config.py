from __future__ import annotations

import logging
import os
import time
from datetime import timedelta
from pathlib import Path

import pytest

from xetra_loader.logging_config import configure_logging


def test_debug_records_from_package_modules_go_to_private_daily_log(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    log_dir = tmp_path / ".logs"
    monkeypatch.setenv("XDL_LOG_DIR", str(log_dir))
    package_logger = logging.getLogger("xetra_loader")
    original_handlers = package_logger.handlers
    original_level = package_logger.level
    original_propagate = package_logger.propagate
    package_logger.handlers = []

    try:
        log_path = configure_logging("xdl-weekly")
        configure_logging("xdl-weekly")
        logging.getLogger("xetra_loader.pipeline.test_module").debug(
            "module_debug_record"
        )
        for handler in package_logger.handlers:
            handler.flush()

        assert log_path == log_dir / "xdl-weekly.log"
        assert log_path.read_text(encoding="utf-8").count("module_debug_record") == 1
        assert len(package_logger.handlers) == 1
        assert package_logger.handlers[0].level == logging.DEBUG
        assert package_logger.handlers[0].when == "MIDNIGHT"
        assert log_dir.stat().st_mode & 0o777 == 0o700
        assert log_path.stat().st_mode & 0o777 == 0o600
    finally:
        for handler in package_logger.handlers:
            handler.close()
        package_logger.handlers = original_handlers
        package_logger.setLevel(original_level)
        package_logger.propagate = original_propagate


def test_logging_startup_prunes_only_this_component_logs_older_than_90_days(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    log_dir = tmp_path / ".logs"
    log_dir.mkdir()
    monkeypatch.setenv("XDL_LOG_DIR", str(log_dir))
    now = time.time()
    expired = log_dir / "xdl-weekly.log.2026-01-01"
    fresh = log_dir / "xdl-weekly.log.2026-10-01"
    other_component = log_dir / "xdl-api.log.2026-01-01"
    for path in (expired, fresh, other_component):
        path.write_text("old log", encoding="utf-8")
    os.utime(expired, (now - timedelta(days=91).total_seconds(),) * 2)
    os.utime(fresh, (now - timedelta(days=10).total_seconds(),) * 2)
    os.utime(other_component, (now - timedelta(days=91).total_seconds(),) * 2)

    package_logger = logging.getLogger("xetra_loader")
    original_handlers = package_logger.handlers
    original_level = package_logger.level
    original_propagate = package_logger.propagate
    package_logger.handlers = []
    try:
        configure_logging("xdl-weekly")
        assert not expired.exists()
        assert fresh.exists()
        assert other_component.exists()
    finally:
        for handler in package_logger.handlers:
            handler.close()
        package_logger.handlers = original_handlers
        package_logger.setLevel(original_level)
        package_logger.propagate = original_propagate


def test_logging_component_rejects_path_traversal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("XDL_LOG_DIR", str(tmp_path))

    with pytest.raises(ValueError, match="logging component"):
        configure_logging("../outside")
