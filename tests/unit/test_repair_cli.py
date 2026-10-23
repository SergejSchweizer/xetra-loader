import os
import sys
from types import ModuleType

import pytest

from xetra_loader.ops.repair_cli import main


def test_repair_cli_sets_polars_budget_before_importing_repair_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[tuple[str | None, str | None, list[str] | None]] = []
    repair_module = ModuleType("xetra_loader.ops.repair_quote_quarantines")

    def repair_main(argv: list[str] | None = None) -> int:
        observed.append(
            (os.getenv("POLARS_MAX_THREADS"), os.getenv("XDL_CORES"), argv)
        )
        return 0

    monkeypatch.setattr(repair_module, "main", repair_main, raising=False)
    monkeypatch.setitem(
        sys.modules,
        "xetra_loader.ops.repair_quote_quarantines",
        repair_module,
    )
    monkeypatch.delenv("POLARS_MAX_THREADS", raising=False)
    monkeypatch.delenv("XDL_CORES", raising=False)

    arguments = [
        "--wrapper-log",
        "run.log",
        "--source-report",
        "report.json",
        "--output",
        "out.json",
        "--cores",
        "4",
    ]
    assert main(arguments) == 0
    assert observed == [
        ("4", "4", arguments)
    ]
