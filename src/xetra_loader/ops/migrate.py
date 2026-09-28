"""Explicit PostgreSQL schema provisioning for the XETRA serving plane."""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from psycopg import Connection

from xetra_loader.config import resolve_feature_work_mem
from xetra_loader.sync import connect_postgres

SCHEMA_FILES: tuple[str, ...] = (
    "sql/schema/001_xetra_loader.sql",
    "sql/schema/002_roles.sql",
    "sql/schema/003_listing_lifecycle.sql",
    "sql/sync/001_xetra_loader_sync.sql",
    "sql/schema/004_xetra_features.sql",
)


def apply_postgres_schema(
    *,
    repository_root: Path | None = None,
    connection: Connection[Any] | None = None,
) -> tuple[str, ...]:
    """Apply the loader-owned schema and materialized feature view explicitly.

    The command is intentionally separate from the weekly writer path. It uses
    an administrative connection and is safe to re-run because every owned
    SQL contract is idempotent or replaces the materialized view definition.
    """

    root = (repository_root or Path.cwd()).resolve()
    own_connection = connection is None
    db: Connection[Any] = connection or connect_postgres(admin=True)
    try:
        db.execute(
            "SELECT set_config('xetra_loader.feature_work_mem', %s, false)",
            (resolve_feature_work_mem(),),
        )
        for relative in SCHEMA_FILES:
            sql = (root / relative).read_text(encoding="utf-8")
            db.execute(sql)
        if own_connection:
            db.commit()
    finally:
        if own_connection:
            db.close()
    return SCHEMA_FILES


def main(argv: Sequence[str] | None = None) -> int:
    """Provision the real target using the protected admin configuration."""

    if argv:
        raise ValueError("xdl-migrate does not accept command-line arguments")
    files = apply_postgres_schema()
    print(json.dumps({"status": "applied", "files": files}, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
