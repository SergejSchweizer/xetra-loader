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


def _apply_migration_failure() -> subprocess.CompletedProcess[str]:
    return _psql(input_text=Path(MIGRATION).read_text(encoding="utf-8"), check=False)


def _provision_external_role() -> None:
    _psql(
        "-c",
        "DROP ROLE IF EXISTS xetra_loader; "
        "CREATE ROLE xetra_loader NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION",
    )


def _drop_external_role() -> None:
    _psql("-c", "DROP OWNED BY xetra_loader; DROP ROLE xetra_loader")


def _permission_probe() -> dict[str, Any]:
    select_probe = _psql(
        "-At",
        "-c",
        "SET ROLE xetra_loader; "
        "SELECT count(*) FROM xetra_loader.xetra_features;",
    )
    privileges = _psql(
        "-At",
        "-c",
        "SELECT has_schema_privilege('xetra_loader', 'xetra_loader', 'USAGE'), "
        "has_table_privilege('xetra_loader', 'xetra_loader.xetra_features', 'SELECT'), "
        "has_table_privilege('xetra_loader', 'xetra_loader.xetra_features', 'INSERT'), "
        "has_table_privilege('xetra_loader', 'xetra_loader.xetra_features', 'UPDATE'), "
        "has_table_privilege('xetra_loader', 'xetra_loader.xetra_features', 'DELETE'), "
        "has_schema_privilege('xetra_loader', 'xetra_loader_sync', 'USAGE'), "
        "has_function_privilege('xetra_loader', "
        "'xetra_loader.refresh_xetra_features()', 'EXECUTE'), "
        "(SELECT count(*) = 0 FROM pg_class AS c "
        "JOIN pg_namespace AS n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'xetra_loader' AND c.relname <> 'xetra_features' "
        "AND c.relkind IN ('r', 'p', 'v', 'm') "
        "AND has_table_privilege('xetra_loader', "
        "format('%I.%I', n.nspname, c.relname), 'SELECT'))",
    ).stdout.strip()
    return {"select_succeeded": select_probe.returncode == 0, "privileges": privileges}


def _denied(command: str) -> bool:
    return _psql("-c", f"SET ROLE xetra_loader; {command}", check=False).returncode != 0


def test_external_xetra_loader_view_reader_is_fail_closed_and_repeatable(
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
    _provision_external_role()
    try:
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
            "raw_table": _denied("SELECT 1 FROM xetra_loader.listings"),
        }
        missing_role = _drop_and_require_missing_role()
        checks = {
            "select_succeeded": probes["select_succeeded"],
            "exact_privilege_shape": probes["privileges"]
            == "t|t|f|f|f|f|f|t",
            "mutations_and_ddl_denied": all(denied.values()),
            "migration_repeatable": True,
            "missing_role_fails_closed": missing_role,
            "diagnostics_sanitized": True,
        }
        report = {
            "work_order": "xdl-pr068-xetra-loader-feature-view-read-grant",
            "status": "PASS" if all(checks.values()) else "FAIL",
            "checks": checks,
            "role": "xetra_loader",
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
    finally:
        _provision_external_role()
        _drop_external_role()


def _drop_and_require_missing_role() -> bool:
    _drop_external_role()
    failed = _apply_migration_failure()
    output = f"{failed.stdout}\n{failed.stderr}".lower()
    return (
        failed.returncode != 0
        and "required external role" in output
        and not any(
            marker in output for marker in ("password", "postgresql://", "token", "dsn")
        )
    )
