from __future__ import annotations

import os
import sys
from pathlib import Path
from types import ModuleType

import pytest

import xetra_loader.ops.verify_postgres_sync as production_verifier
from xetra_loader.ops import bootstrap_cli, verify_cli


def test_bootstrap_cli_sets_core_environment_before_importing_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}
    fake_bootstrap = ModuleType("xetra_loader.ops.bootstrap")

    def bootstrap_main(argv: list[str] | None = None) -> int:
        observed["argv"] = argv
        observed["polars_threads"] = os.environ.get("POLARS_MAX_THREADS")
        observed["xdl_cores"] = os.environ.get("XDL_CORES")
        return 0

    fake_bootstrap.main = bootstrap_main  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "xetra_loader.ops.bootstrap", fake_bootstrap)
    monkeypatch.setenv("POLARS_MAX_THREADS", "6")
    monkeypatch.setenv("XDL_CORES", "6")
    arguments = ["--confirm-destructive-reset", "--cores", "3"]

    assert bootstrap_cli.main(arguments) == 0
    assert observed == {
        "argv": arguments,
        "polars_threads": "3",
        "xdl_cores": "3",
    }


def test_production_verifier_passes_explicit_core_budget(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    observed: dict[str, object] = {}

    class Report:
        def as_dict(self) -> dict[str, str]:
            return {"status": "PASS"}

    def verify(**kwargs: object) -> Report:
        observed.update(kwargs)
        return Report()

    monkeypatch.setattr(
        production_verifier,
        "resolve_medallion_root",
        lambda: str(tmp_path),
    )
    monkeypatch.setattr(
        production_verifier,
        "execute_production_full_sync_and_verify",
        verify,
    )

    assert production_verifier.main(
        [
            "--execute-full-sync",
            "--confirm-destructive-reset",
            "--backup-root",
            str(tmp_path / "backup"),
            "--cores",
            "2",
        ]
    ) == 0
    assert observed["cores"] == 2
    assert '"status":"PASS"' in capsys.readouterr().out


def test_verifier_cli_sets_core_environment_before_importing_polars_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}
    fake_verifier = ModuleType("xetra_loader.ops.verify_postgres_sync")

    def verifier_main(argv: list[str] | None = None) -> int:
        observed["argv"] = argv
        observed["polars_threads"] = os.environ.get("POLARS_MAX_THREADS")
        observed["xdl_cores"] = os.environ.get("XDL_CORES")
        return 0

    fake_verifier.main = verifier_main  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "xetra_loader.ops.verify_postgres_sync", fake_verifier)
    arguments = ["--execute-full-sync", "--backup-root", "/tmp/backup", "--cores", "2"]

    assert verify_cli.main(arguments) == 0
    assert observed == {
        "argv": arguments,
        "polars_threads": "2",
        "xdl_cores": "2",
    }
