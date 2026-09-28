import re
from pathlib import Path

from xetra_loader.features.catalog import feature_columns
from xetra_loader.ops.acceptance import read_scheduler_contract


def test_corrective_complete_run_contract_is_closed() -> None:
    sql = Path("sql/schema/004_xetra_features.sql").read_text(encoding="utf-8")
    cron_timezone, cron_expression = read_scheduler_contract()
    columns = ("isin", "exchange", "code", "trade_date", *feature_columns())

    assert len(feature_columns()) == 48
    assert len(columns) == 52
    assert "average_correlation" not in sql
    for forbidden in ("open_level", "high_level", "low_level", "close_level"):
        assert re.search(rf"(?<![a-z_]){forbidden}(?![a-z_])", sql) is None
    assert cron_timezone == "Europe/Vienna"
    assert cron_expression == "0 8 * * 0"


def test_corrective_run_keeps_configuration_and_logs_out_of_git() -> None:
    gitignore = Path(".gitignore").read_text(encoding="utf-8")
    assert "config.yaml" in gitignore
    assert "artifacts/cron/*.log" in gitignore
