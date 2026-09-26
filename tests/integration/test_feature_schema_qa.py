import json
import os
import subprocess
from pathlib import Path
from typing import Any

import pytest

from xetra_loader.features.catalog import feature_columns

DSN = os.getenv("XDL_TEST_POSTGRES_DSN")
pytestmark = pytest.mark.integration

_IDENTITY_COLUMNS = ("isin", "exchange", "code", "trade_date")
_EXPECTED_COLUMNS = _IDENTITY_COLUMNS + feature_columns()


def _apply_sql(path: str) -> None:
    if DSN is None:
        pytest.skip("XDL_TEST_POSTGRES_DSN is not configured")
    subprocess.run(
        ["psql", DSN, "-X", "-v", "ON_ERROR_STOP=1"],
        check=True,
        text=True,
        input=Path(path).read_text(encoding="utf-8"),
        capture_output=True,
    )


def _psql(*arguments: str) -> list[str]:
    if DSN is None:
        pytest.skip("XDL_TEST_POSTGRES_DSN is not configured")
    result = subprocess.run(
        ["psql", DSN, "-X", "-At", "-v", "ON_ERROR_STOP=1", *arguments],
        check=True,
        text=True,
        capture_output=True,
    )
    return result.stdout.splitlines()


def _write_report(path: Path, checks: dict[str, bool]) -> None:
    report: dict[str, Any] = {
        "work_order": "xdl-pr063-qa-xetra-feature-contract-and-schema",
        "status": "PASS" if all(checks.values()) else "FAIL",
        "checks": checks,
        "secrets_included": False,
    }
    path.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def test_feature_schema_catalog_grants_and_types(tmp_path: Path) -> None:
    if DSN is None:
        pytest.skip("XDL_TEST_POSTGRES_DSN is not configured")
    _apply_sql("sql/schema/001_xetra_loader.sql")
    _apply_sql("sql/schema/002_roles.sql")
    _apply_sql("sql/sync/001_xetra_loader_sync.sql")
    _apply_sql("sql/schema/004_xetra_features.sql")

    relation = _psql(
        "-c",
        "SELECT c.relkind FROM pg_class AS c "
        "JOIN pg_namespace AS n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'xetra_loader' AND c.relname = 'xetra_features'",
    )
    columns = _psql(
        "-c",
        "SELECT a.attname || ':' || format_type(a.atttypid, a.atttypmod) "
        "FROM pg_attribute AS a "
        "WHERE a.attrelid = 'xetra_loader.xetra_features'::regclass "
        "AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
    )
    comment = _psql(
        "-c",
        "SELECT obj_description('xetra_loader.xetra_features'::regclass, 'pg_class')",
    )
    privileges = _psql(
        "-c",
        "SELECT has_table_privilege('portfell_app', 'xetra_loader.xetra_features', 'SELECT'), "
        "has_table_privilege('portfell_app', 'xetra_loader.xetra_features', 'INSERT'), "
        "has_table_privilege('portfell_app', 'xetra_loader.xetra_features', 'UPDATE'), "
        "has_table_privilege('portfell_app', 'xetra_loader.xetra_features', 'DELETE'), "
        "has_schema_privilege('xetra_data_loader_writer', 'xetra_loader', 'CREATE')",
    )

    actual_names = tuple(column.split(":", 1)[0] for column in columns)
    checks = {
        "materialized_view": relation == ["m"],
        "exact_column_order": actual_names == _EXPECTED_COLUMNS,
        "identity_types": columns[:4]
        == ["isin:text", "exchange:text", "code:text", "trade_date:date"],
        "base_level_types": columns[4:6] == [
            "adjusted_close_level:double precision",
            "volume_level:double precision",
        ],
        "catalog_comment": bool(comment) and "catalog version=1" in comment[0],
        "portfell_select_only": privileges == ["t|f|f|f|f"],
        "forbidden_level_columns_absent": not any(
            name in actual_names
            for name in ("open_level", "high_level", "low_level", "close_level")
        ),
    }
    report_path = tmp_path / "xdl-pr063-feature-schema.json"
    _write_report(report_path, checks)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "PASS"
    assert report["secrets_included"] is False
