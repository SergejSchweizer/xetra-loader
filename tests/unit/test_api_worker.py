from __future__ import annotations

from threading import Event
from typing import Any, cast

import psycopg

from xetra_loader.api.jobs import JobStore
from xetra_loader.api.worker import DurableJobWorker, _ClaimedJob


class _Store:
    dsn = "postgresql://unit-test"

    def __init__(self, notification: Event) -> None:
        self.notification = notification
        self.shutdown_notifications = 0

    def notify_worker_shutdown(self) -> None:
        self.shutdown_notifications += 1
        self.notification.set()


class _Cursor:
    def __init__(self, row: tuple[bool] | None = None) -> None:
        self.row = row

    def fetchone(self) -> tuple[bool] | None:
        return self.row


class _Connection:
    def __init__(self, notification: Event) -> None:
        self.notification = notification
        self.closed = False
        self.statements: list[str] = []
        self.parameters: list[object] = []

    def execute(self, query: str, parameters: object = None) -> _Cursor:
        self.statements.append(query)
        self.parameters.append(parameters)
        if "pg_try_advisory_lock" in query:
            return _Cursor((True,))
        if query.startswith("SELECT job_id::text"):
            return _Cursor(None)
        return _Cursor()

    def transaction(self) -> _Connection:
        return self

    def __enter__(self) -> _Connection:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def notifies(self, *, timeout: None, stop_after: int) -> Any:
        assert timeout is None
        assert stop_after == 1
        self.notification.wait(timeout=5)
        yield object()

    def close(self) -> None:
        self.closed = True


def test_idle_worker_shutdown_wakes_listener_and_closes_connection(
    monkeypatch: Any,
) -> None:
    notification = Event()
    store = _Store(notification)
    connection = _Connection(notification)
    monkeypatch.setattr(psycopg, "connect", lambda *_args, **_kwargs: connection)
    worker = DurableJobWorker(store, cores=2, job_runner=lambda *_args: {})  # type: ignore[arg-type]

    worker.start()
    assert worker.is_alive

    worker.stop()

    assert store.shutdown_notifications == 1
    assert not worker.is_alive
    assert connection.closed
    assert any("pg_advisory_unlock" in query for query in connection.statements)


def test_pipeline_reported_failure_is_persisted_as_failed_job() -> None:
    connection = _Connection(Event())
    result: dict[str, Any] = {
        "status": "failed",
        "stages": [{"name": "postgres_sync", "status": "failed"}],
    }
    worker = DurableJobWorker(
        cast(JobStore, _Store(Event())),
        cores=2,
        job_runner=lambda *_args: result,
    )

    worker._execute(  # noqa: SLF001
        cast(psycopg.Connection[Any], connection),
        _ClaimedJob("job-123", "weekly_sync", 1),
    )

    failure_update = next(
        (query, parameters)
        for query, parameters in zip(
            connection.statements,
            connection.parameters,
            strict=True,
        )
        if "SET status = 'failed'" in query
    )
    query, parameters = failure_update
    assert "result = %s" in query
    assert isinstance(parameters, tuple)
    stored_result, job_id = parameters
    assert stored_result.obj == result
    assert job_id == "job-123"
    assert not any("SET status = 'succeeded'" in item for item in connection.statements)
