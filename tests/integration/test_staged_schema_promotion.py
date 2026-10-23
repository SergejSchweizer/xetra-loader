import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest

from xetra_loader.ops.bootstrap import (
    PostgresEodhdBootstrapRuntime,
    drop_previous_serving_schemas,
)

DSN = os.getenv("XDL_TEST_POSTGRES_DSN")
pytestmark = pytest.mark.integration
_OWNED_SCHEMAS = (
    "xetra_loader_sync_previous",
    "xetra_loader_previous",
    "xetra_loader_sync_build",
    "xetra_loader_build",
    "xetra_loader_sync",
    "xetra_loader",
)


@pytest.fixture
def promotion_connection() -> Iterator[psycopg.Connection[Any]]:
    if DSN is None:
        pytest.skip("XDL_TEST_POSTGRES_DSN is not configured")
    connection = psycopg.connect(DSN)
    created_reader_role = False
    try:
        _drop_owned_schemas(connection)
        reader_exists = connection.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'xetra_loader')"
        ).fetchone()
        if reader_exists is None:
            raise RuntimeError("could not inspect test reader role")
        if not reader_exists[0]:
            connection.execute('CREATE ROLE "xetra_loader" NOLOGIN')
            created_reader_role = True
        connection.commit()
        yield connection
    finally:
        connection.rollback()
        _drop_owned_schemas(connection)
        if created_reader_role:
            connection.execute('DROP ROLE IF EXISTS "xetra_loader"')
        connection.commit()
        connection.close()


def test_real_postgres_promotion_swaps_build_and_retains_old_schema_until_cleanup(
    promotion_connection: psycopg.Connection[Any],
) -> None:
    connection = promotion_connection
    _create_active_and_build_schemas(connection, include_functions=True)

    _staging_runtime(connection).promote_staging_state()

    names = connection.execute(
        "SELECT nspname FROM pg_namespace WHERE nspname IN "
        "('xetra_loader', 'xetra_loader_sync', 'xetra_loader_previous', "
        "'xetra_loader_sync_previous', 'xetra_loader_build', 'xetra_loader_sync_build')"
    ).fetchall()
    assert {row[0] for row in names} == {
        "xetra_loader",
        "xetra_loader_sync",
        "xetra_loader_previous",
        "xetra_loader_sync_previous",
    }
    assert connection.execute(
        "SELECT value FROM xetra_loader.active_marker"
    ).fetchone() == ("new",)
    assert connection.execute(
        "SELECT value FROM xetra_loader_previous.active_marker"
    ).fetchone() == ("old",)
    assert connection.execute(
        "SELECT count(*) FROM xetra_loader.loader_jobs WHERE status = 'succeeded'"
    ).fetchone() == (2,)
    assert connection.execute(
        "SELECT count(*) FROM xetra_loader.xetra_features"
    ).fetchone() == (0,)
    connection.execute("CREATE SCHEMA promotion_cleanup_probe")
    connection.execute(
        "CREATE VIEW promotion_cleanup_probe.retained_rows AS "
        "SELECT value FROM xetra_loader_previous.active_marker"
    )
    connection.commit()
    with pytest.raises(RuntimeError, match="external objects depend on it"):
        drop_previous_serving_schemas(connection)
    assert connection.execute(
        "SELECT value FROM promotion_cleanup_probe.retained_rows"
    ).fetchone() == ("old",)
    connection.execute("DROP SCHEMA promotion_cleanup_probe CASCADE")
    connection.commit()
    drop_previous_serving_schemas(connection)
    assert connection.execute(
        "SELECT count(*) FROM pg_namespace WHERE nspname IN "
        "('xetra_loader_previous', 'xetra_loader_sync_previous')"
    ).fetchone() == (0,)


def test_real_postgres_failed_promotion_rolls_schema_renames_back(
    promotion_connection: psycopg.Connection[Any],
) -> None:
    connection = promotion_connection
    _create_active_and_build_schemas(connection, include_functions=False)

    with pytest.raises(psycopg.errors.UndefinedFunction):
        _staging_runtime(connection).promote_staging_state()

    assert connection.execute(
        "SELECT value FROM xetra_loader.active_marker"
    ).fetchone() == ("old",)
    assert connection.execute(
        "SELECT value FROM xetra_loader_build.active_marker"
    ).fetchone() == ("new",)
    assert connection.execute(
        "SELECT count(*) FROM pg_namespace WHERE nspname = 'xetra_loader_previous'"
    ).fetchone() == (0,)


def _create_active_and_build_schemas(
    connection: psycopg.Connection[Any],
    *,
    include_functions: bool,
) -> None:
    for schema in (
        "xetra_loader",
        "xetra_loader_sync",
        "xetra_loader_build",
        "xetra_loader_sync_build",
    ):
        connection.execute(f'CREATE SCHEMA "{schema}"')
    connection.execute(
        "CREATE TABLE xetra_loader.active_marker (value text NOT NULL)"
    )
    connection.execute("INSERT INTO xetra_loader.active_marker VALUES ('old')")
    connection.execute(
        "CREATE TABLE xetra_loader_build.active_marker (value text NOT NULL)"
    )
    connection.execute("INSERT INTO xetra_loader_build.active_marker VALUES ('new')")
    for schema in ("xetra_loader", "xetra_loader_build"):
        connection.execute(
            f'CREATE TABLE "{schema}".loader_jobs '
            "(job_id integer, status text NOT NULL)"
        )
        connection.execute(
            f'CREATE TABLE "{schema}".xetra_features (id integer NOT NULL)'
        )
    connection.execute(
        "INSERT INTO xetra_loader.loader_jobs VALUES (1, 'succeeded')"
    )
    connection.execute(
        "INSERT INTO xetra_loader_build.loader_jobs VALUES (2, 'succeeded')"
    )
    if include_functions:
        connection.execute(
            "CREATE FUNCTION xetra_loader_build.refresh_xetra_features() "
            "RETURNS void LANGUAGE plpgsql AS $$ BEGIN RETURN; END $$"
        )
        connection.execute(
            "CREATE FUNCTION xetra_loader_build.resolve_feature_work_mem() "
            "RETURNS text LANGUAGE sql AS $$ SELECT '256MB'::text $$"
        )
        connection.execute(
            "CREATE FUNCTION xetra_loader_build.resolve_feature_temp_file_limit() "
            "RETURNS text LANGUAGE sql AS $$ SELECT '32GB'::text $$"
        )
    connection.commit()


def _staging_runtime(
    connection: psycopg.Connection[Any],
) -> PostgresEodhdBootstrapRuntime:
    runtime = object.__new__(PostgresEodhdBootstrapRuntime)
    runtime._connection = connection
    runtime._staging = True
    runtime._repository_root = Path.cwd()
    return runtime


def _drop_owned_schemas(connection: psycopg.Connection[Any]) -> None:
    for schema in _OWNED_SCHEMAS:
        connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    connection.commit()
