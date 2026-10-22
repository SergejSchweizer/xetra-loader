"""Guarded destructive reset of loader-owned PostgreSQL and medallion state."""

from __future__ import annotations

import argparse
import shutil
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from psycopg import Connection

from xetra_loader.logging_config import configure_logging
from xetra_loader.sync import connect_postgres

CONFIRM_FLAG = "--confirm-destructive-reset"
_OWNED_SCHEMAS = ("xetra_loader_sync", "xetra_loader")
_OWNED_LAYERS = ("bronze", "silver", "gold")


@dataclass(frozen=True, slots=True)
class ResetPlan:
    """Exact reset scope; unrelated schemas and filesystem paths are absent by construction."""

    schemas: tuple[str, ...]
    medallion_paths: tuple[Path, ...]


def build_reset_plan(medallion_root: Path) -> ResetPlan:
    """Enumerate only loader-owned schemas and medallion layer roots."""

    root = medallion_root.resolve()
    return ResetPlan(
        schemas=_OWNED_SCHEMAS,
        medallion_paths=tuple(root / layer for layer in _OWNED_LAYERS),
    )


def execute_reset(
    plan: ResetPlan,
    *,
    confirmed: bool,
    dry_run: bool,
    connection: Connection[Any] | None = None,
    remove_tree: Callable[[Path], None] = shutil.rmtree,
) -> ResetPlan:
    """Apply a reset only after literal confirmation; dry run never mutates state."""

    if not confirmed or dry_run:
        return plan
    if connection is None:
        raise ValueError("a PostgreSQL connection is required for destructive reset")

    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute("SET LOCAL TIME ZONE 'UTC'")
        for schema in plan.schemas:
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')

    for path in plan.medallion_paths:
        if path.exists():
            remove_tree(path)
    return plan


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reset xetra-loader owned state")
    parser.add_argument("--medallion-root", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(CONFIRM_FLAG, action="store_true", dest="confirmed")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point; absent confirmation always yields a non-mutating plan."""

    configure_logging("xdl-reset")
    args = _parser().parse_args(argv)
    plan = build_reset_plan(args.medallion_root)
    print("schemas=" + ",".join(plan.schemas))
    print("paths=" + ",".join(str(path) for path in plan.medallion_paths))
    if not args.confirmed or args.dry_run:
        return 0
    connection = connect_postgres(admin=True)
    try:
        execute_reset(plan, confirmed=True, dry_run=False, connection=connection)
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
