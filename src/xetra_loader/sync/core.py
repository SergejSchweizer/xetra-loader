"""Transactional PostgreSQL publication state shared by every serving dataset."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast
from uuid import uuid4

import psycopg
from psycopg import Connection, Cursor

from xetra_loader.config import (
    resolve_feature_work_mem,
    resolve_postgres_admin_dsn,
    resolve_postgres_writer_dsn,
)

type JSONValue = str | int | float | bool | None | list[JSONValue] | dict[str, JSONValue]
type SemanticRow = Mapping[str, JSONValue]
type Mutator = Callable[[Cursor[Any]], "SyncCounters"]

_DATASET_TABLES: dict[str, str] = {
    "listings": "listings",
    "eod_quotes": "eod_quotes",
    "dividends": "dividends",
    "splits": "splits",
}


@dataclass(frozen=True, slots=True)
class SyncCounters:
    """Generic serving-table mutation counters."""

    inserted: int = 0
    updated: int = 0
    deleted: int = 0
    retracted: int = 0

    def __post_init__(self) -> None:
        if min(self.inserted, self.updated, self.deleted, self.retracted) < 0:
            raise ValueError("sync counters must be non-negative")

    @property
    def total_mutations(self) -> int:
        return self.inserted + self.updated + self.deleted + self.retracted


@dataclass(frozen=True, slots=True)
class SyncOutcome:
    """One committed synchronization result."""

    run_id: str
    dataset: str
    semantic_fingerprint: str
    row_count: int
    status: str
    counters: SyncCounters

    @property
    def changed(self) -> bool:
        return self.status == "applied"


def connect_postgres(
    dsn: str | None = None,
    *,
    admin: bool = False,
    require_non_superuser: bool = False,
) -> Connection[Any]:
    """Connect as an explicit admin or a normal writer, rejecting unsafe weekly sessions."""

    resolved = resolve_postgres_admin_dsn(dsn) if admin else resolve_postgres_writer_dsn(dsn)
    connection = psycopg.connect(resolved, autocommit=False)
    connection.execute(
        "SELECT set_config('xetra_loader.feature_work_mem', %s, false)",
        (resolve_feature_work_mem(),),
    )
    connection.commit()
    if require_non_superuser:
        row = connection.execute(
            "SELECT rolsuper FROM pg_roles WHERE rolname = current_user"
        ).fetchone()
        if row is None or bool(row[0]):
            connection.close()
            raise PermissionError("weekly PostgreSQL connection must not use a superuser")
        # The probe above starts a transaction on psycopg connections with
        # autocommit disabled.  End it before callers open their own
        # transaction blocks; otherwise ``connection.transaction()`` creates
        # a savepoint and a later close can roll back the publication.
        connection.commit()
    return connection


def semantic_fingerprint(rows: Iterable[SemanticRow]) -> tuple[str, int]:
    """Hash semantic rows deterministically, independent of iteration order."""

    canonical_rows = sorted(_canonical_row(row) for row in rows)
    payload = "[" + ",".join(canonical_rows) + "]"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest(), len(canonical_rows)


def run_sync(
    connection: Connection[Any],
    *,
    dataset: str,
    semantic_rows: Iterable[SemanticRow],
    mutate: Mutator,
    run_id: str | None = None,
    now: Callable[[], datetime] | None = None,
) -> SyncOutcome:
    """Couple serving mutations and sync-state advance in one PostgreSQL transaction."""

    if not dataset.strip():
        raise ValueError("dataset must be non-empty")
    materialized_rows = tuple(semantic_rows)
    fingerprint, row_count = semantic_fingerprint(materialized_rows)
    serving_row_count = sum(
        1 for row in materialized_rows if not bool(row.get("retracted", False))
    )
    from xetra_loader.sync.row_digests import DATASET_KEY_FIELDS, digest_map, row_digests

    source_digests = (
        digest_map(row_digests(dataset, materialized_rows))
        if dataset in DATASET_KEY_FIELDS
        else {}
    )
    resolved_run_id = run_id or str(uuid4())
    clock = now or (lambda: datetime.now(UTC))
    started_at = _require_utc(clock())

    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute("SET LOCAL TIME ZONE 'UTC'")
        cursor.execute("SET LOCAL lock_timeout = '5s'")
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (dataset,),
        )
        _preflight(cursor, dataset)
        cursor.execute(
            "SELECT semantic_fingerprint, row_count "
            "FROM xetra_loader_sync.sync_state WHERE dataset = %s FOR UPDATE",
            (dataset,),
        )
        raw_state = cursor.fetchone()
        state = cast(tuple[str, int] | None, raw_state)
        cursor.execute(
            "SELECT entity_key, row_sha256 "
            "FROM xetra_loader_sync.row_hashes WHERE dataset = %s FOR UPDATE",
            (dataset,),
        )
        persisted_digests = {
            str(entity_key): str(row_sha256)
            for entity_key, row_sha256 in cursor.fetchall()
        }
        if state is None and persisted_digests:
            raise RuntimeError(
                f"digest state exists without sync state for dataset {dataset!r}"
            )
        is_noop = (
            state is not None
            and state[0] == fingerprint
            and state[1] == row_count
            and persisted_digests == source_digests
        )

        counters = SyncCounters() if is_noop else mutate(cursor)
        if not is_noop:
            if dataset in DATASET_KEY_FIELDS:
                _replace_row_digests(cursor, dataset, source_digests)
            if dataset == "eod_quotes" and counters.total_mutations:
                cursor.execute("SELECT xetra_loader.refresh_xetra_features()")
            _verify_post_write(cursor, dataset, serving_row_count, source_digests)
            cursor.execute(
                "INSERT INTO xetra_loader_sync.sync_state "
                "(dataset, semantic_fingerprint, row_count, synced_at_utc) "
                "VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (dataset) DO UPDATE SET "
                "semantic_fingerprint = EXCLUDED.semantic_fingerprint, "
                "row_count = EXCLUDED.row_count, "
                "synced_at_utc = EXCLUDED.synced_at_utc",
                (dataset, fingerprint, row_count, started_at),
            )

        finished_at = _require_utc(clock())
        status = "noop" if is_noop else "applied"
        cursor.execute(
            "INSERT INTO xetra_loader_sync.loader_runs "
            "(run_id, dataset, semantic_fingerprint, row_count, inserted_count, "
            "updated_count, deleted_count, retracted_count, started_at_utc, "
            "finished_at_utc, status) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                resolved_run_id,
                dataset,
                fingerprint,
                row_count,
                counters.inserted,
                counters.updated,
                counters.deleted,
                counters.retracted,
                started_at,
                finished_at,
                status,
            ),
        )

    return SyncOutcome(
        run_id=resolved_run_id,
        dataset=dataset,
        semantic_fingerprint=fingerprint,
        row_count=row_count,
        status=status,
        counters=counters,
    )


def _replace_row_digests(
    cursor: Cursor[Any],
    dataset: str,
    digests: Mapping[str, str],
) -> None:
    """Replace one dataset's complete digest snapshot in the open transaction."""

    cursor.execute(
        "DELETE FROM xetra_loader_sync.row_hashes WHERE dataset = %s",
        (dataset,),
    )
    if digests:
        cursor.executemany(
            "INSERT INTO xetra_loader_sync.row_hashes "
            "(dataset, entity_key, row_sha256) VALUES (%s, %s, %s)",
            [(dataset, entity_key, row_sha256) for entity_key, row_sha256 in digests.items()],
        )


def _preflight(cursor: Cursor[Any], dataset: str) -> None:
    """Check every relation and routine needed before allowing a mutation."""

    table = _DATASET_TABLES.get(dataset)
    cursor.execute(
        "SELECT to_regclass('xetra_loader_sync.sync_state') IS NOT NULL, "
        "to_regclass('xetra_loader_sync.loader_runs') IS NOT NULL, "
        "to_regclass('xetra_loader_sync.row_hashes') IS NOT NULL, "
        "to_regclass(%s) IS NOT NULL, "
        "(%s = 'eod_quotes' AND "
        "to_regclass('xetra_loader.xetra_features') IS NOT NULL AND "
        "to_regprocedure('xetra_loader.refresh_xetra_features()') IS NOT NULL) "
        "OR %s <> 'eod_quotes'",
        (f"xetra_loader.{table}" if table else "xetra_loader_sync.sync_state", dataset, dataset),
    )
    raw = cursor.fetchone()
    if raw is None or not all(bool(value) for value in raw):
        raise RuntimeError(f"PostgreSQL sync preflight failed for dataset {dataset!r}")


def _verify_post_write(
    cursor: Cursor[Any],
    dataset: str,
    row_count: int,
    expected_digests: Mapping[str, str],
) -> None:
    """Verify the complete Gold snapshot before the transaction can commit."""

    from xetra_loader.sync.row_digests import DATASET_KEY_FIELDS

    if dataset in DATASET_KEY_FIELDS:
        cursor.execute(
            "SELECT entity_key, row_sha256 "
            "FROM xetra_loader_sync.row_hashes WHERE dataset = %s",
            (dataset,),
        )
        actual_digests = {
            str(entity_key): str(row_sha256)
            for entity_key, row_sha256 in cursor.fetchall()
        }
        if actual_digests != expected_digests:
            raise RuntimeError(f"row digest verification failed for dataset {dataset!r}")

    table = _DATASET_TABLES.get(dataset)
    if table is not None:
        cursor.execute(f"SELECT count(*) FROM xetra_loader.{table}")
        raw_count = cursor.fetchone()
        if raw_count is None or int(raw_count[0]) != row_count:
            raise RuntimeError(f"row count verification failed for dataset {dataset!r}")

    if dataset == "eod_quotes":
        cursor.execute(
            "SELECT count(*), count(DISTINCT (isin, exchange, code, trade_date)) "
            "FROM xetra_loader.xetra_features"
        )
        view_counts = cursor.fetchone()
        if view_counts is None or tuple(map(int, view_counts)) != (row_count, row_count):
            raise RuntimeError("feature view identity verification failed")


def _canonical_row(row: SemanticRow) -> str:
    return json.dumps(dict(row), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _require_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("sync timestamps must be timezone-aware")
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError("sync timestamps must use UTC")
    return value
