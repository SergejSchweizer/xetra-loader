from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from xetra_loader.ops.acceptance import LoaderAcceptanceReport, read_scheduler_contract

CRON_PATH = Path("deploy/cron/xetra-loader.cron")


def test_cron_contract_is_exact_sunday_0900_vienna() -> None:
    lines = CRON_PATH.read_text(encoding="utf-8").splitlines()

    assert lines[0] == "CRON_TZ=Europe/Vienna"
    assert lines[1].startswith("0 9 * * 0 ")
    assert "bash scripts/run_full_refresh.sh" in lines[1]
    assert ">> artifacts/cron/full-refresh.log 2>&1" in lines[1]
    assert "xdl-enqueue-weekly" not in lines[1]
    assert "xdl-bootstrap" not in lines[1]


def test_vienna_0900_remains_local_0900_across_dst() -> None:
    vienna = ZoneInfo("Europe/Vienna")
    winter = datetime(2026, 1, 11, 9, 0, tzinfo=vienna)
    summer = datetime(2026, 7, 12, 9, 0, tzinfo=vienna)

    assert winter.weekday() == 6
    assert summer.weekday() == 6
    assert winter.hour == summer.hour == 9
    assert winter.utcoffset().total_seconds() == 3600
    assert summer.utcoffset().total_seconds() == 7200


def test_acceptance_reads_the_deployed_expression(tmp_path: Path) -> None:
    cron = tmp_path / "xetra-loader.cron"
    cron.write_text(
        "CRON_TZ=Europe/Vienna\n0 7 * * 0 /usr/bin/true\n",
        encoding="utf-8",
    )

    timezone, expression = read_scheduler_contract(cron)
    report = LoaderAcceptanceReport(
        scenarios={"sunday_vienna_schedule": True},
        tables=("listings", "eod_quotes", "dividends", "splits"),
        timestamp_type="TIMESTAMPTZ(6)",
        database_timezone="UTC",
        app_role="portfell_app",
        app_role_select_only=True,
        scheduler_timezone=timezone,
        scheduler_expression=expression,
        portfell_imports=0,
    )

    assert expression == "0 7 * * 0"
    assert not report.passed
    invalid = tmp_path / "missing-command.cron"
    invalid.write_text("CRON_TZ=Europe/Vienna\n0 8 * * 0\n", encoding="utf-8")
    with pytest.raises(ValueError, match="command"):
        read_scheduler_contract(invalid)
