from pathlib import Path
from typing import Any

from xetra_loader.ops.bootstrap import (
    PostgresEodhdBootstrapRuntime,
    _rewrite_staging_schema_names,
    drop_previous_serving_schemas,
)


class _Context:
    def __init__(self, value: object | None = None) -> None:
        self.value = value

    def __enter__(self) -> Any:
        return self.value if self.value is not None else self

    def __exit__(self, *_args: object) -> None:
        return None


class _Cursor:
    def __init__(self, statements: list[str]) -> None:
        self.statements = statements
        self.last_statement = ""

    def execute(self, statement: str, *_args: Any) -> _Cursor:
        self.last_statement = statement
        self.statements.append(statement)
        return self

    def fetchone(self) -> tuple[bool | int]:
        if "pg_namespace" in self.last_statement:
            if "nspname = 'xetra_loader')" in self.last_statement:
                return (True,)
            if "nspname = 'xetra_loader_sync')" in self.last_statement:
                return (True,)
            return (False,)
        return (False,) if "xetra_loader_previous" in self.last_statement else (0,)

    def fetchall(self) -> list[tuple[object, ...]]:
        return []


class _Connection:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self._cursor = _Cursor(self.statements)

    def transaction(self) -> _Context:
        return _Context()

    def cursor(self) -> _Context:
        return _Context(self._cursor)

    def execute(self, statement: str) -> _Cursor:
        return self._cursor.execute(statement)


def test_staging_sql_retargets_schemas_without_renaming_loader_role() -> None:
    source = (
        "CREATE SCHEMA IF NOT EXISTS xetra_loader; "
        "CREATE TABLE xetra_loader.eod_quotes (id int); "
        "CREATE TABLE xetra_loader_sync.sync_state (id int); "
        "GRANT USAGE ON SCHEMA xetra_loader TO \"xetra-data-loader\";"
    )

    staged = _rewrite_staging_schema_names(source)

    assert "CREATE SCHEMA IF NOT EXISTS xetra_loader_build" in staged
    assert "xetra_loader_build.eod_quotes" in staged
    assert "xetra_loader_sync_build.sync_state" in staged
    assert 'TO "xetra-data-loader"' in staged
    assert "xetra_loader.eod_quotes" not in staged


def test_staging_reset_builds_separate_schemas_and_keeps_active_names() -> None:
    runtime = object.__new__(PostgresEodhdBootstrapRuntime)
    connection = _Connection()
    runtime._connection = connection
    runtime._staging = True
    runtime._repository_root = Path.cwd()

    runtime.reset_owned_state()

    statements = "\n".join(connection.statements)
    assert 'DROP SCHEMA IF EXISTS "xetra_loader_build" CASCADE' in statements
    assert 'DROP SCHEMA IF EXISTS "xetra_loader_sync_build" CASCADE' in statements
    assert "CREATE TABLE IF NOT EXISTS xetra_loader_build.eod_quotes" in statements
    assert "CREATE TABLE IF NOT EXISTS xetra_loader_sync_build.sync_state" in statements
    assert 'DROP SCHEMA IF EXISTS "xetra_loader" CASCADE' not in statements
    assert 'DROP SCHEMA IF EXISTS "xetra_loader_sync" CASCADE' not in statements


def test_promotion_builds_and_verifies_before_old_schema_cleanup() -> None:
    runtime = object.__new__(PostgresEodhdBootstrapRuntime)
    connection = _Connection()
    runtime._connection = connection
    runtime._staging = True
    runtime._repository_root = Path.cwd()

    runtime.promote_staging_state()
    promotion_sql = "\n".join(connection.statements)

    assert 'ALTER SCHEMA "xetra_loader" RENAME TO "xetra_loader_previous"' in promotion_sql
    assert 'ALTER SCHEMA "xetra_loader_build" RENAME TO "xetra_loader"' in promotion_sql
    assert 'DROP SCHEMA "xetra_loader_previous"' not in promotion_sql

    drop_previous_serving_schemas(connection)  # called only after PASS verification
    assert connection.statements[-2:] == [
        'DROP SCHEMA IF EXISTS "xetra_loader_sync_previous" CASCADE',
        'DROP SCHEMA IF EXISTS "xetra_loader_previous" CASCADE',
    ]
