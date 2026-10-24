"""Fail-closed CI enforcement for the real PostgreSQL integration suite."""

from __future__ import annotations

import os

import pytest

from xetra_loader.testing import validate_local_test_postgres_dsn

_SKIPPED: list[str] = []


def _required() -> bool:
    return os.getenv("XDL_REQUIRE_POSTGRES_INTEGRATION") == "1"


def pytest_sessionstart(session: pytest.Session) -> None:
    """Reset state and fail closed on a non-local/non-test PostgreSQL DSN."""

    del session
    _SKIPPED.clear()
    dsn = os.getenv("XDL_TEST_POSTGRES_DSN")
    if dsn:
        try:
            validate_local_test_postgres_dsn(dsn)
        except ValueError:
            raise pytest.UsageError(
                "refusing PostgreSQL integration tests unless the DSN explicitly names "
                "a local test database"
            ) from None


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    """Record skips so CI cannot pass while omitting PostgreSQL coverage."""

    if report.skipped:
        _SKIPPED.append(report.nodeid)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Turn missing DSN or skipped integration coverage into a failing CI run."""

    del exitstatus
    if not _required():
        return

    failures: list[str] = []
    if not os.getenv("XDL_TEST_POSTGRES_DSN"):
        failures.append("XDL_TEST_POSTGRES_DSN is required in CI")
    if _SKIPPED:
        failures.append(f"integration tests skipped: {', '.join(sorted(_SKIPPED))}")
    if failures:
        session.config.issue_config_time_warning(
            pytest.PytestConfigWarning("; ".join(failures)),
            stacklevel=1,
        )
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
