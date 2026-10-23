"""Transactional PostgreSQL publication state shared by every serving dataset."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast
from uuid import uuid4

import psycopg
from psycopg import Connection, Cursor

from xetra_loader.config import (
    resolve_cores,
    resolve_feature_temp_file_limit,
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
_SCHEMA_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


def validate_schema_identifier(schema: str) -> str:
    """Validate a schema name before interpolating this internal SQL identifier."""

    if not _SCHEMA_IDENTIFIER.fullmatch(schema):
        raise ValueError(f"invalid PostgreSQL schema identifier: {schema!r}")
    return schema


def qualify_serving_sql(statement: str, schema: str) -> str:
    """Retarget loader SQL from the active schema to a validated staging schema."""

    validate_schema_identifier(schema)
    return statement.replace("xetra_loader.", f"{schema}.")


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


class AuthoritativeSnapshotRequired(RuntimeError):
    """Raised when destructive reconciliation lacks a validated Gold proof."""


def connect_postgres(
    dsn: str | None = None,
    *,
    admin: bool = False,
    require_non_superuser: bool = False,
    cores: int | None = None,
) -> Connection[Any]:
    """Connect as an explicit admin or a normal writer, rejecting unsafe weekly sessions."""

    resolved = resolve_postgres_admin_dsn(dsn) if admin else resolve_postgres_writer_dsn(dsn)
    connection = psycopg.connect(resolved, autocommit=False)
    try:
        connection.execute(
            "SELECT set_config('xetra_loader.feature_work_mem', %s, false)",
            (resolve_feature_work_mem(),),
        )
        configured_temp_limit = resolve_feature_temp_file_limit()
        version_row = connection.execute("SHOW server_version_num").fetchone()
        if version_row is None:
            raise RuntimeError("PostgreSQL server version could not be determined")
        server_version = int(version_row[0])
        if admin or server_version >= 150000:
            connection.execute(
                "SELECT set_config('xetra_loader.feature_temp_file_limit', %s, false)",
                (configured_temp_limit,),
            )
        else:
            # PostgreSQL 14 has no parameter-level SET grants. xdl-migrate
            # installs the configured cap as a login-role session default.
            matches_config = connection.execute(
                "SELECT pg_size_bytes(current_setting('temp_file_limit')) "
                "= pg_size_bytes(%s)",
                (configured_temp_limit,),
            ).fetchone()
            if matches_config is None or not matches_config[0]:
                raise RuntimeError(
                    "PostgreSQL 14 writer temp_file_limit differs from configuration; "
                    "run xdl-migrate as the database administrator"
                )
        configured_cores = resolve_cores(cores)
        connection.execute(
            "SELECT set_config('xetra_loader.compute_cores', %s, false)",
            (str(configured_cores),),
        )
        parallel_workers = str(max(configured_cores - 1, 0))
        for setting in (
            "max_parallel_workers_per_gather",
            "max_parallel_maintenance_workers",
        ):
            connection.execute(
                "SELECT set_config(%s, %s, false)",
                (setting, parallel_workers),
            )
        connection.execute(
            "SELECT set_config('parallel_leader_participation', 'on', false)"
        )
        if require_non_superuser:
            row = connection.execute(
                "SELECT rolsuper FROM pg_roles WHERE rolname = current_user"
            ).fetchone()
            if row is None or bool(row[0]):
                raise PermissionError("weekly PostgreSQL connection must not use a superuser")
        # End configuration/probe queries before callers open their own
        # transaction blocks.
        connection.commit()
        return connection
    except BaseException:
        connection.close()
        raise


def semantic_fingerprint(rows: Iterable[SemanticRow]) -> tuple[str, int]:
    """Hash semantic rows deterministically, independent of iteration order."""

    canonical_rows = sorted(_canonical_row(row) for row in rows)
    payload = "[" + ",".join(canonical_rows) + "]"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest(), len(canonical_rows)


def run_sync(  # noqa: PLR0913 -- schema routing remains explicit at the transaction boundary.
    connection: Connection[Any],
    *,
    dataset: str,
    semantic_rows: Iterable[SemanticRow],
    mutate: Mutator,
    run_id: str | None = None,
    serving_schema: str = "xetra_loader",
    sync_schema: str = "xetra_loader_sync",
) -> SyncOutcome:
    """Couple serving mutations and sync-state advance in one PostgreSQL transaction."""

    if not dataset.strip():
        raise ValueError("dataset must be non-empty")
    validate_schema_identifier(serving_schema)
    validate_schema_identifier(sync_schema)
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
    started_at = _require_utc(datetime.now(UTC))

    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute("SET LOCAL TIME ZONE 'UTC'")
        cursor.execute("SET LOCAL lock_timeout = '5s'")
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (dataset,),
        )
        _preflight(cursor, dataset, serving_schema, sync_schema)
        cursor.execute(
            "SELECT semantic_fingerprint, row_count "
            f"FROM {sync_schema}.sync_state WHERE dataset = %s FOR UPDATE",
            (dataset,),
        )
        raw_state = cursor.fetchone()
        state = cast(tuple[str, int] | None, raw_state)
        cursor.execute(
            "SELECT entity_key, row_sha256 "
            f"FROM {sync_schema}.row_hashes WHERE dataset = %s FOR UPDATE",
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
            and (
                (serving_count := _serving_row_count(cursor, dataset, serving_schema)) is None
                or serving_count == serving_row_count
            )
        )

        counters = SyncCounters() if is_noop else mutate(cursor)
        if not is_noop:
            if dataset in DATASET_KEY_FIELDS:
                _replace_row_digests(cursor, dataset, source_digests, sync_schema)
            if dataset == "eod_quotes" and counters.total_mutations:
                cursor.execute(f"SELECT {serving_schema}.refresh_xetra_features()")
            _verify_post_write(
                cursor, dataset, serving_row_count, source_digests, serving_schema, sync_schema
            )
            cursor.execute(
                f"INSERT INTO {sync_schema}.sync_state "
                "(dataset, semantic_fingerprint, row_count, synced_at_utc) "
                "VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (dataset) DO UPDATE SET "
                "semantic_fingerprint = EXCLUDED.semantic_fingerprint, "
                "row_count = EXCLUDED.row_count, "
                "synced_at_utc = EXCLUDED.synced_at_utc",
                (dataset, fingerprint, row_count, started_at),
            )

        finished_at = _require_utc(datetime.now(UTC))
        status = "noop" if is_noop else "applied"
        cursor.execute(
            f"INSERT INTO {sync_schema}.loader_runs "
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
    sync_schema: str = "xetra_loader_sync",
) -> None:
    """Replace one dataset's complete digest snapshot in the open transaction."""

    cursor.execute("DROP TABLE IF EXISTS xdl_desired_row_hashes")
    cursor.execute(
        "CREATE TEMP TABLE xdl_desired_row_hashes ("
        "entity_key text PRIMARY KEY, row_sha256 text NOT NULL"
        ") ON COMMIT DROP"
    )
    with cursor.copy(
        "COPY xdl_desired_row_hashes (entity_key, row_sha256) FROM STDIN"
    ) as copy:
        for entity_key, row_sha256 in digests.items():
            copy.write_row((entity_key, row_sha256))
    cursor.execute(
        f"DELETE FROM {sync_schema}.row_hashes WHERE dataset = %s",
        (dataset,),
    )
    if digests:
        cursor.execute(
            f"INSERT INTO {sync_schema}.row_hashes "
            "(dataset, entity_key, row_sha256) "
            "SELECT %s, entity_key, row_sha256 FROM xdl_desired_row_hashes",
            (dataset,),
        )


def _preflight(
    cursor: Cursor[Any],
    dataset: str,
    serving_schema: str = "xetra_loader",
    sync_schema: str = "xetra_loader_sync",
) -> None:
    """Check every relation and routine needed before allowing a mutation."""

    table = _DATASET_TABLES.get(dataset)
    cursor.execute(
        "SELECT to_regclass(%s) IS NOT NULL, "
        "to_regclass(%s) IS NOT NULL, "
        "to_regclass(%s) IS NOT NULL, "
        "to_regclass(%s) IS NOT NULL, "
        "(%s = 'eod_quotes' AND "
        "to_regclass(%s) IS NOT NULL AND "
        "to_regprocedure(%s) IS NOT NULL) "
        "OR %s <> 'eod_quotes'",
        (
            f"{sync_schema}.sync_state",
            f"{sync_schema}.loader_runs",
            f"{sync_schema}.row_hashes",
            f"{serving_schema}.{table}" if table else f"{sync_schema}.sync_state",
            dataset,
            f"{serving_schema}.xetra_features",
            f"{serving_schema}.refresh_xetra_features()",
            dataset,
        ),
    )
    raw = cursor.fetchone()
    if raw is None or not all(bool(value) for value in raw):
        raise RuntimeError(f"PostgreSQL sync preflight failed for dataset {dataset!r}")


def _serving_row_count(
    cursor: Cursor[Any], dataset: str, serving_schema: str = "xetra_loader"
) -> int | None:
    """Confirm that a state-matching sync still has the expected table cardinality."""

    table = _DATASET_TABLES.get(dataset)
    if table is None:
        return None
    cursor.execute(f'SELECT count(*) FROM {serving_schema}."{table}"')
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"missing serving row count for dataset {dataset!r}")
    return int(row[0])


def _verify_post_write(  # noqa: PLR0913, PLR0917 -- schema routing is kept explicit.
    cursor: Cursor[Any],
    dataset: str,
    row_count: int,
    expected_digests: Mapping[str, str],
    serving_schema: str = "xetra_loader",
    sync_schema: str = "xetra_loader_sync",
) -> None:
    """Verify the complete Gold snapshot before the transaction can commit."""

    from xetra_loader.sync.row_digests import DATASET_KEY_FIELDS

    if dataset in DATASET_KEY_FIELDS:
        cursor.execute(
            "SELECT entity_key, row_sha256 "
            f"FROM {sync_schema}.row_hashes WHERE dataset = %s",
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
        cursor.execute(f"SELECT count(*) FROM {serving_schema}.{table}")
        raw_count = cursor.fetchone()
        if raw_count is None or (
            int(raw_count[0]) < row_count
            or (dataset != "listings" and int(raw_count[0]) != row_count)
        ):
            raise RuntimeError(f"row count verification failed for dataset {dataset!r}")

    if dataset == "eod_quotes":
        cursor.execute(
            "SELECT count(*), count(DISTINCT (isin, exchange, code, trade_date)) "
            f"FROM {serving_schema}.xetra_features"
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
