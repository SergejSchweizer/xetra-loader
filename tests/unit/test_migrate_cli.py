"""CLI contracts for bounded PostgreSQL schema provisioning."""

from pathlib import Path
from typing import Any, cast

import pytest
from psycopg import Connection

from xetra_loader.ops import migrate


class _RecordingConnection:
    def __init__(
        self,
        *,
        reader_role_exists: bool = False,
        server_version: int = 160000,
    ) -> None:
        self.statements: list[tuple[str, tuple[object, ...]]] = []
        self.reader_role_exists = reader_role_exists
        self.server_version = server_version
        self.commit_count = 0

    def execute(self, statement: str, params: tuple[object, ...] = ()) -> Any:
        self.statements.append((statement, params))
        if "FROM pg_roles" in statement:
            result = (self.reader_role_exists,)
        elif statement == "SHOW server_version_num":
            result = (self.server_version,)
        else:
            result = None
        return _RecordingResult(result)

    def commit(self) -> None:
        self.commit_count += 1


class _RecordingResult:
    def __init__(self, result: tuple[bool] | None) -> None:
        self.result = result

    def fetchone(self) -> tuple[bool] | None:
        return self.result


def test_xdl_migrate_passes_explicit_core_budget_to_schema_provisioning(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    observed: dict[str, int | None] = {}

    def apply_schema(*, cores: int | None = None) -> tuple[str, ...]:
        observed["cores"] = cores
        return ("sql/schema/004_xetra_features.sql",)

    monkeypatch.setattr(migrate, "apply_postgres_schema", apply_schema)

    assert migrate.main(["--cores", "4"]) == 0
    assert observed["cores"] == 4
    assert '"status":"applied"' in capsys.readouterr().out


def test_schema_provisioner_sets_database_core_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("XDL_FEATURE_WORK_MEM", "256MB")
    monkeypatch.setenv("XDL_FEATURE_TEMP_FILE_LIMIT", "16GB")
    connection = _RecordingConnection()
    for relative in migrate.SCHEMA_FILES:
        sql_path = tmp_path / relative
        sql_path.parent.mkdir(parents=True, exist_ok=True)
        sql_path.write_text("SELECT 1;", encoding="utf-8")

    applied = migrate.apply_postgres_schema(
        repository_root=tmp_path,
        connection=cast(Connection[Any], connection),
        cores=4,
    )

    assert applied == migrate.SCHEMA_FILES
    assert connection.statements[:2] == [
        ("SELECT set_config('xetra_loader.compute_cores', %s, false)", ("4",)),
        ("SELECT set_config('xetra_loader.feature_work_mem', %s, false)", ("256MB",)),
    ]
    assert connection.statements[2] == (
        "SELECT set_config('xetra_loader.feature_temp_file_limit', %s, false)",
        ("16GB",),
    )
    assert connection.statements[-1] == (
        "SHOW server_version_num",
        (),
    )
    assert connection.statements[-2] == (
        "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %s)",
        ("xetra_loader",),
    )


def test_schema_provisioner_configures_pg14_writer_login_temp_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("XDL_FEATURE_WORK_MEM", "256MB")
    monkeypatch.setenv("XDL_FEATURE_TEMP_FILE_LIMIT", "24GB")
    connection = _RecordingConnection(server_version=140000)
    for relative in migrate.SCHEMA_FILES:
        sql_path = tmp_path / relative
        sql_path.parent.mkdir(parents=True, exist_ok=True)
        sql_path.write_text("SELECT 1;", encoding="utf-8")

    migrate.apply_postgres_schema(
        repository_root=tmp_path,
        connection=cast(Connection[Any], connection),
        cores=4,
    )

    statement, params = connection.statements[-1]
    statement_text = str(statement)
    assert "ALTER ROLE" in statement_text
    assert "xetra_data_loader_writer" in statement_text
    assert "temp_file_limit" in statement_text
    assert "24GB" in statement_text
    assert params == ()


def test_schema_provisioner_reapplies_reader_contract_after_view_rebuild(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("XDL_FEATURE_WORK_MEM", "256MB")
    monkeypatch.setenv("XDL_FEATURE_TEMP_FILE_LIMIT", "16GB")
    connection = _RecordingConnection(reader_role_exists=True)
    for relative in migrate.SCHEMA_FILES:
        sql_path = tmp_path / relative
        sql_path.parent.mkdir(parents=True, exist_ok=True)
        sql_path.write_text("SELECT 1;", encoding="utf-8")
    reader_file = tmp_path / migrate.FEATURE_VIEW_READER_FILE
    reader_file.parent.mkdir(parents=True, exist_ok=True)
    reader_file.write_text(
        "GRANT SELECT ON xetra_loader.xetra_features TO xetra_loader;",
        encoding="utf-8",
    )

    applied = migrate.apply_postgres_schema(
        repository_root=tmp_path,
        connection=cast(Connection[Any], connection),
        cores=4,
    )

    assert applied[-1] == migrate.FEATURE_VIEW_READER_FILE
    assert connection.statements[-1][0] == reader_file.read_text(encoding="utf-8")
    assert connection.commit_count == 1
