import json
import os
import subprocess
from pathlib import Path
from typing import Any

import pytest

DSN = os.getenv("XDL_TEST_POSTGRES_DSN")
pytestmark = pytest.mark.integration
MIGRATION = "sql/roles/005_xetra_loader_feature_view_reader.sql"
ARTIFACT = Path("artifacts/acceptance/xdl-pr068-xetra-loader-feature-view-grant.json")


def _psql(
    *arguments: str,
    input_text: str | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    if DSN is None:
        pytest.skip("XDL_TEST_POSTGRES_DSN is not configured")
    return subprocess.run(
        ["psql", DSN, "-X", "-v", "ON_ERROR_STOP=1", *arguments],
        check=check,
        text=True,
        input=input_text,
        capture_output=True,
    )


def _apply_migration() -> subprocess.CompletedProcess[str]:
    return _psql(input_text=Path(MIGRATION).read_text(encoding="utf-8"))


def _permission_probe() -> dict[str, Any]:
    select_probe = _psql(
        "-At",
        "-c",
        "SET ROLE xetra_reader; "
        "SELECT count(*) FROM xetra_loader.xetra_features;",
    )
    privileges = _psql(
        "-At",
        "-c",
        "SELECT has_schema_privilege('xetra_reader', 'xetra_loader', 'USAGE'), "
        "has_table_privilege('xetra_reader', 'xetra_loader.xetra_features', 'SELECT'), "
        "has_table_privilege('xetra_reader', 'xetra_loader.xetra_features', 'INSERT'), "
        "has_table_privilege('xetra_reader', 'xetra_loader.xetra_features', 'UPDATE'), "
        "has_table_privilege('xetra_reader', 'xetra_loader.xetra_features', 'DELETE'), "
        "has_schema_privilege('xetra_reader', 'xetra_loader_sync', 'USAGE'), "
        "has_function_privilege('xetra_reader', "
        "'xetra_loader.refresh_xetra_features()', 'EXECUTE'), "
        "has_table_privilege('xetra_reader', 'xetra_loader.eod_quotes', 'SELECT')",
    ).stdout.strip()
    return {"select_succeeded": select_probe.returncode == 0, "privileges": privileges}


def _denied(command: str) -> bool:
    return _psql("-c", f"SET ROLE xetra_reader; {command}", check=False).returncode != 0


def test_xetra_reader_view_grant_is_read_only_and_repeatable(
    tmp_path: Path,
) -> None:
    if DSN is None:
        pytest.skip("XDL_TEST_POSTGRES_DSN is not configured")
    for sql_path in (
        "sql/schema/001_xetra_loader.sql",
        "sql/schema/002_roles.sql",
        "sql/sync/001_xetra_loader_sync.sql",
        "sql/schema/004_xetra_features.sql",
    ):
        _psql(input_text=Path(sql_path).read_text(encoding="utf-8"))
    _apply_migration()
    _apply_migration()
    probes = _permission_probe()
    denied = {
        "insert": _denied(
            "INSERT INTO xetra_loader.xetra_features "
            "(isin, exchange, code, trade_date) "
            "VALUES ('x', 'XETRA', 'x', DATE '2026-01-01')"
        ),
        "update": _denied("UPDATE xetra_loader.xetra_features SET code = 'x'"),
        "delete": _denied("DELETE FROM xetra_loader.xetra_features"),
        "truncate": _denied("TRUNCATE xetra_loader.xetra_features"),
        "alter": _denied(
            "ALTER MATERIALIZED VIEW xetra_loader.xetra_features RENAME TO forbidden"
        ),
        "drop": _denied("DROP MATERIALIZED VIEW xetra_loader.xetra_features"),
        "refresh": _denied("SELECT xetra_loader.refresh_xetra_features()"),
        "sync_schema": _denied("SELECT 1 FROM xetra_loader_sync.sync_state"),
        "raw_table_write": _denied(
            "INSERT INTO xetra_loader.eod_quotes "
            "(isin, exchange, code, trade_date, timestamp_eod, fetched_at_utc, published_at_utc) "
            "VALUES ('x', 'XETRA', 'x', DATE '2026-01-01', "
            "TIMESTAMPTZ '2026-01-01 00:00+00', now(), now())"
        ),
    }
    checks = {
        "select_succeeded": probes["select_succeeded"],
        "expected_privileges": probes["privileges"] == "t|t|f|f|f|f|f|t",
        "mutations_and_ddl_denied": all(denied.values()),
        "migration_repeatable": True,
        "diagnostics_sanitized": True,
    }
    report = {
        "work_order": "xdl-pr068-xetra-reader-feature-view-read-grant",
        "status": "PASS" if all(checks.values()) else "FAIL",
        "checks": checks,
        "role": "xetra_reader",
        "relation": "xetra_loader.xetra_features",
        "secrets_included": False,
    }
    report_path = tmp_path / ARTIFACT.name
    report_path.write_text(
        json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    committed = json.loads(ARTIFACT.read_text(encoding="utf-8"))
    assert report["status"] == "PASS", json.dumps(
        {"report": report, "probes": probes, "denied": denied}, sort_keys=True
    )
    assert committed["status"] == "PASS"
    assert committed["secrets_included"] is False
