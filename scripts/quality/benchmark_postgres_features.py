#!/usr/bin/env python3
"""Read-only PostgreSQL feature-scan benchmark against a dedicated test DB."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from pathlib import Path
from typing import Any

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPOSITORY_ROOT / "src"))


def _worker_counts(plan: Mapping[str, Any]) -> tuple[int, int]:
    planned = int(plan.get("Workers Planned", 0))
    launched = int(plan.get("Workers Launched", 0))
    for child in plan.get("Plans", []):
        child_planned, child_launched = _worker_counts(child)
        planned += child_planned
        launched += child_launched
    return planned, launched


def _decode_plan(value: object) -> Mapping[str, Any]:
    decoded = json.loads(value) if isinstance(value, str) else value
    if not isinstance(decoded, list) or not decoded or not isinstance(decoded[0], dict):
        raise ValueError("PostgreSQL returned an unexpected EXPLAIN JSON document")
    plan = decoded[0].get("Plan")
    if not isinstance(plan, dict):
        raise ValueError("PostgreSQL EXPLAIN result has no root Plan")
    return plan


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="benchmark-postgres-features")
    parser.add_argument("--cores", type=int, default=None)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--lookback-days", type=int, default=252)
    args = parser.parse_args(argv)

    from xetra_loader.config import resolve_cores

    cores = resolve_cores(args.cores)
    if args.rounds < 1 or args.lookback_days < 1:
        parser.error("--rounds and --lookback-days must be positive")
    dsn = os.environ.get("XDL_TEST_POSTGRES_DSN")
    if not dsn:
        parser.error("XDL_TEST_POSTGRES_DSN must point to a dedicated test database")

    import psycopg

    connection = psycopg.connect(dsn)
    try:
        connection.execute("SET TRANSACTION READ ONLY")
        database_row = connection.execute("SELECT current_database()").fetchone()
        if database_row is None:
            raise RuntimeError("PostgreSQL did not report the connected database")
        database_name = str(database_row[0])
        if database_name == "xetra_loader":
            parser.error("refusing to benchmark against the production xetra_loader database")
        connection.execute(
            "SELECT set_config('max_parallel_workers_per_gather', %s, true)",
            (str(max(cores - 1, 0)),),
        )
        relation = connection.execute(
            "SELECT relation.relkind "
            "FROM pg_class AS relation "
            "JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace "
            "WHERE namespace.nspname = 'xetra_loader' "
            "AND relation.relname = 'xetra_features'"
        ).fetchone()
        if relation is None or relation[0] != "m":
            parser.error("xetra_loader.xetra_features must be a materialized view")
        exists_row = connection.execute(
            "SELECT EXISTS (SELECT 1 FROM xetra_loader.xetra_features LIMIT 1)"
        ).fetchone()
        if exists_row is None or not bool(exists_row[0]):
            parser.error("test feature view is empty; load the dedicated fixture first")

        cutoff = date.today() - timedelta(days=args.lookback_days)
        sql = """
            EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
            SELECT count(*)
            FROM xetra_loader.xetra_features
            WHERE trade_date >= %s
        """
        samples: list[float] = []
        planned_workers: list[int] = []
        launched_workers: list[int] = []
        for _ in range(args.rounds):
            started = time.perf_counter()
            explain_row = connection.execute(sql, (cutoff,)).fetchone()
            if explain_row is None:
                raise RuntimeError("PostgreSQL did not return an EXPLAIN plan")
            explain = explain_row[0]
            samples.append(time.perf_counter() - started)
            plan = _decode_plan(explain)
            planned, launched = _worker_counts(plan)
            planned_workers.append(planned)
            launched_workers.append(launched)

        print(json.dumps({
            "database": database_name,
            "read_only": True,
            "cores": cores,
            "max_parallel_workers_per_gather": max(cores - 1, 0),
            "lookback_days": args.lookback_days,
            "rounds": args.rounds,
            "median_seconds": statistics.median(samples),
            "workers_planned": planned_workers,
            "workers_launched": launched_workers,
            "max_workers_launched": max(launched_workers, default=0),
        }, sort_keys=True, separators=(",", ":")))
        connection.rollback()
        return 0
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
