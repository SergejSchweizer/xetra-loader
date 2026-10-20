"""Durable event-driven worker resident in the API process."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from xetra_loader.api.jobs import JobStore
from xetra_loader.sync.core import JSONValue

_LOGGER = logging.getLogger(__name__)
_WORKER_LOCK_NAME = "xetra-loader-api-durable-job-worker"
_NOTIFY_CHANNEL = "xetra_loader_jobs"

type JobRunner = Callable[[str, int], Mapping[str, JSONValue]]


@dataclass(frozen=True, slots=True)
class _ClaimedJob:
    job_id: str
    job_type: str
    attempt: int


class _WorkerLockHeld(RuntimeError):
    """Another API process already owns the singleton worker lease."""


class DurableJobWorker:
    """Consume committed queue inserts via LISTEN/NOTIFY, never idle SQL polling."""

    def __init__(
        self,
        store: JobStore,
        *,
        cores: int,
        job_runner: JobRunner,
    ) -> None:
        if cores < 1:
            raise ValueError("cores must be positive")
        self._store = store
        self._cores = cores
        self._job_runner = job_runner
        self._stop_requested = threading.Event()
        self._ready = threading.Event()
        self._connected = threading.Event()
        self._startup_error: BaseException | None = None
        self._thread: threading.Thread | None = None

    @property
    def is_alive(self) -> bool:
        return (
            self._thread is not None
            and self._thread.is_alive()
            and self._connected.is_set()
        )

    def start(self) -> None:
        """Start the single database-locked worker and wait for readiness."""

        if self._thread is not None:
            raise RuntimeError("durable job worker was already started")
        self._thread = threading.Thread(
            target=self._run,
            name="xdl-api-job-worker",
            daemon=True,
        )
        self._thread.start()
        if not self._ready.wait(timeout=30):
            raise TimeoutError("durable job worker did not become ready")
        if self._startup_error is not None:
            raise RuntimeError("durable job worker failed to start") from self._startup_error

    def stop(self) -> None:
        """Stop after the active run finishes and wake an idle listener."""

        self._stop_requested.set()
        thread = self._thread
        if thread is None or not thread.is_alive():
            return
        try:
            self._store.notify_worker_shutdown()
        except psycopg.Error:
            _LOGGER.warning("durable_job_worker_shutdown_notify_failed")
        thread.join()

    def _run(self) -> None:
        failures = 0
        while not self._stop_requested.is_set():
            try:
                self._serve_connection()
                break
            except Exception as exc:
                if self._record_startup_failure(exc):
                    return
                if isinstance(exc, _WorkerLockHeld):
                    _LOGGER.error("durable_job_worker_lock_unavailable")
                    return
                failures += 1
                self._log_reconnect(exc, failures)
                if self._stop_requested.wait(timeout=min(30, 2**min(failures, 5))):
                    return

    def _serve_connection(self) -> None:
        connection = psycopg.connect(self._store.dsn, autocommit=True)
        locked = False
        try:
            connection.execute(f"LISTEN {_NOTIFY_CHANNEL}")
            lock_result = connection.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
                (_WORKER_LOCK_NAME,),
            ).fetchone()
            locked = lock_result is not None and bool(lock_result[0])
            if not locked:
                raise _WorkerLockHeld("another API process owns the worker lock")
            self._requeue_interrupted(connection)
            self._connected.set()
            self._ready.set()
            while not self._stop_requested.is_set():
                job = self._claim_next(connection)
                if job is not None:
                    self._execute(connection, job)
                    continue
                # LISTEN was established before the first queue scan. A
                # racing commit stays buffered as a notification.
                next(connection.notifies(timeout=None, stop_after=1), None)
        finally:
            self._connected.clear()
            if locked:
                with suppress(psycopg.Error):
                    connection.execute(
                        "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
                        (_WORKER_LOCK_NAME,),
                    )
            connection.close()

    def _record_startup_failure(self, failure: Exception) -> bool:
        if self._ready.is_set():
            return False
        self._startup_error = failure
        self._ready.set()
        return True

    @staticmethod
    def _log_reconnect(failure: Exception, failures: int) -> None:
        _LOGGER.error(
            "durable_job_worker_reconnecting error_type=%s attempt=%d",
            type(failure).__name__,
            failures,
        )

    @staticmethod
    def _requeue_interrupted(connection: psycopg.Connection[Any]) -> None:
        with connection.transaction():
            connection.execute(
                "UPDATE xetra_loader.loader_jobs "
                "SET status = 'queued', started_at_utc = NULL, error_code = NULL "
                "WHERE status = 'running'"
            )

    @staticmethod
    def _claim_next(connection: psycopg.Connection[Any]) -> _ClaimedJob | None:
        with connection.transaction():
            row = connection.execute(
                "SELECT job_id::text, job_type FROM xetra_loader.loader_jobs "
                "WHERE status = 'queued' ORDER BY created_at_utc, job_id "
                "FOR UPDATE SKIP LOCKED LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            claimed = connection.execute(
                "UPDATE xetra_loader.loader_jobs SET status = 'running', "
                "attempts = attempts + 1, started_at_utc = clock_timestamp(), "
                "error_code = NULL WHERE job_id = %s AND status = 'queued' "
                "RETURNING job_id::text, job_type, attempts",
                (row[0],),
            ).fetchone()
            if claimed is None:
                return None
            return _ClaimedJob(str(claimed[0]), str(claimed[1]), int(claimed[2]))

    def _execute(self, connection: psycopg.Connection[Any], job: _ClaimedJob) -> None:
        _LOGGER.info(
            "durable_job_start id=%s type=%s attempt=%d cores=%d",
            job.job_id,
            job.job_type,
            job.attempt,
            self._cores,
        )
        try:
            if job.job_type != "weekly_sync":
                raise ValueError("unsupported durable job type")
            result = dict(self._job_runner(job.job_id, self._cores))
        except Exception as exc:
            with connection.transaction():
                connection.execute(
                    "UPDATE xetra_loader.loader_jobs SET status = 'failed', "
                    "error_code = 'weekly_sync_failed', finished_at_utc = clock_timestamp() "
                    "WHERE job_id = %s AND status = 'running'",
                    (job.job_id,),
                )
            _LOGGER.error(
                "durable_job_failed id=%s error_type=%s",
                job.job_id,
                type(exc).__name__,
            )
            return
        with connection.transaction():
            connection.execute(
                "UPDATE xetra_loader.loader_jobs SET status = 'succeeded', result = %s, "
                "error_code = NULL, finished_at_utc = clock_timestamp() "
                "WHERE job_id = %s AND status = 'running'",
                (Jsonb(result), job.job_id),
            )
        _LOGGER.info("durable_job_success id=%s", job.job_id)
