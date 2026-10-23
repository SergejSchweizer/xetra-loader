"""Explicit PostgreSQL schema provisioning for the XETRA serving plane."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from psycopg import Connection, sql

from xetra_loader.config import (
    resolve_cores,
    resolve_feature_temp_file_limit,
    resolve_feature_work_mem,
)
from xetra_loader.logging_config import configure_logging
from xetra_loader.sync import connect_postgres

SCHEMA_FILES: tuple[str, ...] = (
    "sql/schema/001_xetra_loader.sql",
    "sql/schema/002_roles.sql",
    "sql/schema/003_listing_lifecycle.sql",
    "sql/sync/001_xetra_loader_sync.sql",
    "sql/schema/004_xetra_features.sql",
    "sql/schema/006_durable_jobs.sql",
)
FEATURE_VIEW_READER_FILE = "sql/roles/005_xetra_loader_feature_view_reader.sql"


def apply_postgres_schema(
    *,
    repository_root: Path | None = None,
    connection: Connection[Any] | None = None,
    cores: int | None = None,
) -> tuple[str, ...]:
    """Apply the loader-owned schema and materialized feature view explicitly.

    The command is intentionally separate from the weekly writer path. It uses
    an administrative connection and is safe to re-run because every owned
    SQL contract is idempotent or replaces the materialized view definition.
    """

    root = (repository_root or Path.cwd()).resolve()
    own_connection = connection is None
    db: Connection[Any] = connection or connect_postgres(admin=True, cores=cores)
    try:
        db.execute(
            "SELECT set_config('xetra_loader.compute_cores', %s, false)",
            (str(resolve_cores(cores)),),
        )
        db.execute(
            "SELECT set_config('xetra_loader.feature_work_mem', %s, false)",
            (resolve_feature_work_mem(),),
        )
        db.execute(
            "SELECT set_config('xetra_loader.feature_temp_file_limit', %s, false)",
            (resolve_feature_temp_file_limit(),),
        )
        if own_connection:
            db.commit()
        applied_files = list(SCHEMA_FILES)
        for relative in SCHEMA_FILES:
            sql_text = (root / relative).read_text(encoding="utf-8")
            db.execute(sql_text)
        role_exists = db.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %s)",
            ("xetra_loader",),
        ).fetchone()
        version_row = db.execute("SHOW server_version_num").fetchone()
        if version_row is None:
            raise RuntimeError("PostgreSQL server version could not be determined")
        server_version = int(version_row[0])
        db.commit()
        if server_version < 150000:
            # PostgreSQL 14 cannot delegate SET on a superuser-context GUC.
            # Apply the validated cap at login for the dedicated writer role.
            db.execute(
                sql.SQL("ALTER ROLE {} SET temp_file_limit TO {}").format(
                    sql.Identifier("xetra_data_loader_writer"),
                    sql.Literal(resolve_feature_temp_file_limit()),
                )
            )
            db.commit()
        if role_exists is not None and role_exists[0]:
            # 004 drops and recreates the materialized view, which removes
            # object grants. Reapply the external reader contract afterward.
            db.execute((root / FEATURE_VIEW_READER_FILE).read_text(encoding="utf-8"))
            applied_files.append(FEATURE_VIEW_READER_FILE)
        if own_connection:
            db.commit()
    finally:
        if own_connection:
            db.close()
    return tuple(applied_files)


def main(argv: Sequence[str] | None = None) -> int:
    """Provision the real target using the protected admin configuration."""

    configure_logging("xdl-migrate")
    parser = argparse.ArgumentParser(prog="xdl-migrate")
    parser.add_argument(
        "--cores",
        type=int,
        default=None,
        help="CPU budget for PostgreSQL parallel plans (default: config or 6)",
    )
    arguments = parser.parse_args(argv)
    cores = resolve_cores(arguments.cores)
    files = apply_postgres_schema(cores=cores)
    print(json.dumps({"status": "applied", "files": files}, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
