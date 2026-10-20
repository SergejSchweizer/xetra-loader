from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from xetra_loader.ops.acceptance import LoaderAcceptanceReport, read_scheduler_contract

CRON_PATH = Path("deploy/cron/xetra-loader.cron")


def test_cron_contract_is_exact_sunday_1100_vienna() -> None:
    lines = CRON_PATH.read_text(encoding="utf-8").splitlines()

    assert lines[0] == "CRON_TZ=Europe/Vienna"
    assert lines[1].startswith("0 11 * * 0 ")
    assert "xdl-enqueue-weekly" in lines[1]
    assert "xdl-bootstrap" not in lines[1]


def test_vienna_1100_remains_local_1100_across_dst() -> None:
    vienna = ZoneInfo("Europe/Vienna")
    winter = datetime(2026, 1, 11, 11, 0, tzinfo=vienna)
    summer = datetime(2026, 7, 12, 11, 0, tzinfo=vienna)

    assert winter.weekday() == 6
    assert summer.weekday() == 6
    assert winter.hour == summer.hour == 11
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
    invalid.write_text("CRON_TZ=Europe/Vienna\n0 11 * * 0\n", encoding="utf-8")
    with pytest.raises(ValueError, match="command"):
        read_scheduler_contract(invalid)
