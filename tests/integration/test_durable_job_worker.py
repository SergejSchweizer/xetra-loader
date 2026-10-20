from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from threading import Event
from uuid import UUID, uuid4

import psycopg
import pytest

from xetra_loader.api.jobs import JobAlreadyActive, JobRecord, JobStore
from xetra_loader.api.worker import DurableJobWorker

DSN = os.getenv("XDL_TEST_POSTGRES_DSN")
pytestmark = pytest.mark.integration


def _apply_sql(path: str) -> None:
    if DSN is None:
        pytest.skip("XDL_TEST_POSTGRES_DSN is not configured")
    subprocess.run(
        ["psql", DSN, "-X", "-v", "ON_ERROR_STOP=1"],
        check=True,
        text=True,
        input=Path(path).read_text(encoding="utf-8"),
        capture_output=True,
    )


def _wait_for_status(store: JobStore, job_id: UUID, status: str) -> JobRecord:
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        job = store.get(job_id)
        if job is not None and job.status == status:
            return job
        time.sleep(0.02)
    raise AssertionError(f"job did not reach status {status!r}")


def test_queue_wakes_worker_and_recovers_interrupted_run() -> None:
    if DSN is None:
        pytest.skip("XDL_TEST_POSTGRES_DSN is not configured")
    _apply_sql("sql/schema/001_xetra_loader.sql")
    _apply_sql("sql/schema/002_roles.sql")
    _apply_sql("sql/schema/006_durable_jobs.sql")
    with psycopg.connect(DSN) as connection:
        connection.execute("TRUNCATE xetra_loader.loader_jobs")
        interrupted_id = uuid4()
        connection.execute(
            "INSERT INTO xetra_loader.loader_jobs "
            "(job_id, job_type, status, attempts, started_at_utc) "
            "VALUES (%s, 'weekly_sync', 'running', 1, clock_timestamp())",
            (interrupted_id,),
        )

    store = JobStore(DSN)
    interrupted_started = Event()
    release_interrupted = Event()
    completed_ids: list[str] = []

    def run_job(job_id: str, cores: int) -> dict[str, str | int]:
        completed_ids.append(job_id)
        if job_id == str(interrupted_id):
            interrupted_started.set()
            if not release_interrupted.wait(timeout=8):
                raise TimeoutError("test did not release simulated interrupted job")
        return {"job_id": job_id, "cores": cores}

    worker = DurableJobWorker(store, cores=3, job_runner=run_job)
    worker.start()
    try:
        assert interrupted_started.wait(timeout=5)
        with pytest.raises(JobAlreadyActive) as duplicate:
            store.enqueue_weekly_sync()
        assert duplicate.value.job.job_id == str(interrupted_id)

        release_interrupted.set()
        recovered = _wait_for_status(store, interrupted_id, "succeeded")
        assert recovered.attempts == 2
        assert recovered.result == {"job_id": str(interrupted_id), "cores": 3}

        # The worker is idle in LISTEN here. This committed INSERT's NOTIFY
        # must wake it; no timer-driven database query is used by the worker.
        queued = store.enqueue_weekly_sync()
        notified = _wait_for_status(
            store,
            UUID(queued.job_id),
            "succeeded",
        )
        assert notified.result == {"job_id": queued.job_id, "cores": 3}
        assert completed_ids == [str(interrupted_id), queued.job_id]
    finally:
        release_interrupted.set()
        worker.stop()
