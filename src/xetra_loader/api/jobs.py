"""Persistence boundary for durable loader jobs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast
from uuid import UUID, uuid4

import psycopg

from xetra_loader.config import resolve_postgres_writer_dsn
from xetra_loader.sync.core import JSONValue

_JOB_COLUMNS = (
    "job_id::text, job_type, status, attempts, created_at_utc, started_at_utc, "
    "finished_at_utc, result, error_code"
)


@dataclass(frozen=True, slots=True)
class JobRecord:
    """Public, secret-free status projection for one durable queue row."""

    job_id: str
    job_type: str
    status: str
    attempts: int
    created_at_utc: datetime
    started_at_utc: datetime | None
    finished_at_utc: datetime | None
    result: dict[str, JSONValue] | None
    error_code: str | None

    def as_dict(self) -> dict[str, JSONValue]:
        return {
            "job_id": self.job_id,
            "job_type": self.job_type,
            "status": self.status,
            "attempts": self.attempts,
            "created_at_utc": self.created_at_utc.isoformat(),
            "started_at_utc": (
                None if self.started_at_utc is None else self.started_at_utc.isoformat()
            ),
            "finished_at_utc": (
                None if self.finished_at_utc is None else self.finished_at_utc.isoformat()
            ),
            "result": self.result,
            "error_code": self.error_code,
        }


class JobAlreadyActive(RuntimeError):
    """Raised when a weekly synchronization is already queued or running."""

    def __init__(self, job: JobRecord) -> None:
        super().__init__("a weekly synchronization is already active")
        self.job = job


class JobStore:
    """PostgreSQL-backed queue API; notifications commit atomically with inserts."""

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn or resolve_postgres_writer_dsn()

    def enqueue_weekly_sync(self) -> JobRecord:
        job_id = uuid4()
        with psycopg.connect(self.dsn) as connection, connection.transaction():
            row = connection.execute(
                "INSERT INTO xetra_loader.loader_jobs "
                "(job_id, job_type, status, payload) "
                "VALUES (%s, 'weekly_sync', 'queued', '{}'::jsonb) "
                "ON CONFLICT (job_type) WHERE status IN ('queued', 'running') DO NOTHING "
                f"RETURNING {_JOB_COLUMNS}",
                (job_id,),
            ).fetchone()
            if row is None:
                existing = connection.execute(
                    "SELECT "
                    + _JOB_COLUMNS
                    + " FROM xetra_loader.loader_jobs "
                    "WHERE job_type = 'weekly_sync' AND status IN ('queued', 'running') "
                    "ORDER BY created_at_utc LIMIT 1"
                ).fetchone()
                if existing is None:
                    raise RuntimeError("active weekly job conflict has no matching row")
                raise JobAlreadyActive(_job_record(existing))
        if row is None:
            raise RuntimeError("weekly job insert returned no row")
        return _job_record(row)

    def get(self, job_id: UUID) -> JobRecord | None:
        with psycopg.connect(self.dsn) as connection:
            row = connection.execute(
                f"SELECT {_JOB_COLUMNS} FROM xetra_loader.loader_jobs WHERE job_id = %s",
                (job_id,),
            ).fetchone()
        return None if row is None else _job_record(row)

    def notify_worker_shutdown(self) -> None:
        """Wake the LISTEN connection so the API can stop without an idle poll."""

        with psycopg.connect(self.dsn) as connection:
            connection.execute("SELECT pg_notify('xetra_loader_jobs', '__shutdown__')")


def _job_record(row: tuple[Any, ...]) -> JobRecord:
    raw_result = row[7]
    result = cast(dict[str, JSONValue] | None, raw_result)
    return JobRecord(
        job_id=str(row[0]),
        job_type=str(row[1]),
        status=str(row[2]),
        attempts=int(row[3]),
        created_at_utc=cast(datetime, row[4]),
        started_at_utc=cast(datetime | None, row[5]),
        finished_at_utc=cast(datetime | None, row[6]),
        result=result,
        error_code=None if row[8] is None else str(row[8]),
    )
