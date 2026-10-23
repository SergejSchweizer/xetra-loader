from __future__ import annotations

import json
from datetime import UTC, datetime
from io import BytesIO
from typing import Any
from uuid import UUID
from wsgiref.simple_server import WSGIRequestHandler
from wsgiref.util import setup_testing_defaults

from xetra_loader.api.application import LoaderApiApplication
from xetra_loader.api.jobs import JobAlreadyActive, JobRecord
from xetra_loader.api.runner import MonitoringWSGIServer


class _Store:
    def __init__(self) -> None:
        self.record = JobRecord(
            job_id="2ec0c68b-c61d-4ec3-bb93-47ce4712f9cc",
            job_type="weekly_sync",
            status="queued",
            attempts=0,
            created_at_utc=datetime(2026, 10, 20, tzinfo=UTC),
            started_at_utc=None,
            finished_at_utc=None,
            result=None,
            error_code=None,
        )
        self.enqueued = 0
        self.already_active = False

    def enqueue_weekly_sync(self) -> JobRecord:
        self.enqueued += 1
        if self.already_active:
            raise JobAlreadyActive(self.record)
        return self.record

    def get(self, job_id: UUID) -> JobRecord | None:
        return self.record if str(job_id) == self.record.job_id else None


class _Worker:
    is_alive = True


def _request(  # noqa: PLR0913 -- test helper exposes every request input directly.
    app: LoaderApiApplication,
    *,
    method: str,
    path: str,
    body: bytes = b"",
    token: str | None = None,
    content_type: str = "application/json",
    content_length: str | None = None,
) -> tuple[str, dict[str, Any]]:
    environ: dict[str, Any] = {}
    setup_testing_defaults(environ)
    environ.update(
        {
            "REQUEST_METHOD": method,
            "PATH_INFO": path,
            "CONTENT_TYPE": content_type,
            "CONTENT_LENGTH": str(len(body)) if content_length is None else content_length,
            "wsgi.input": BytesIO(body),
        }
    )
    if token is not None:
        environ["HTTP_AUTHORIZATION"] = f"Bearer {token}"
    statuses: list[str] = []
    response = b"".join(app(environ, lambda status, _headers: statuses.append(status)))
    return statuses[0], json.loads(response)


def test_health_route_is_local_and_reports_worker_state() -> None:
    app = LoaderApiApplication(_Store(), _Worker(), "test-secret")

    status, payload = _request(app, method="GET", path="/healthz")

    assert status.startswith("200")
    assert payload == {"status": "ok", "worker": "running"}


def test_job_routes_require_bearer_token_and_validate_request() -> None:
    store = _Store()
    app = LoaderApiApplication(store, _Worker(), "test-secret")

    unauthorized, _ = _request(
        app, method="POST", path="/v1/jobs", body=b'{"type":"weekly_sync"}'
    )
    invalid_type, _ = _request(
        app,
        method="POST",
        path="/v1/jobs",
        body=b'{"type":"weekly_sync"}',
        token="test-secret",
        content_type="text/plain",
    )
    accepted, job = _request(
        app,
        method="POST",
        path="/v1/jobs",
        body=b'{"type":"weekly_sync"}',
        token="test-secret",
    )
    inspected, status = _request(
        app,
        method="GET",
        path=f"/v1/jobs/{store.record.job_id}",
        token="test-secret",
    )

    assert unauthorized.startswith("401")
    assert invalid_type.startswith("415")
    assert accepted.startswith("202")
    assert job["job_id"] == store.record.job_id
    assert inspected.startswith("200")
    assert status["status"] == "queued"
    assert store.enqueued == 1


def test_active_job_conflict_is_reported_without_creating_another_job() -> None:
    store = _Store()
    store.already_active = True
    app = LoaderApiApplication(store, _Worker(), "test-secret")

    status, payload = _request(
        app,
        method="POST",
        path="/v1/jobs",
        body=b'{"type":"weekly_sync"}',
        token="test-secret",
    )

    assert status.startswith("409")
    assert payload == {
        "error": "weekly_sync_already_active",
        "job_id": store.record.job_id,
    }
    assert store.enqueued == 1


def test_api_rejects_invalid_length_and_unknown_routes() -> None:
    app = LoaderApiApplication(_Store(), _Worker(), "test-secret")

    too_large, too_large_payload = _request(
        app,
        method="POST",
        path="/v1/jobs",
        body=b"{}",
        token="test-secret",
        content_length="5000",
    )
    unknown, unknown_payload = _request(app, method="GET", path="/not-a-route")

    assert too_large.startswith("413")
    assert too_large_payload == {"error": "request_too_large"}
    assert unknown.startswith("404")
    assert unknown_payload == {"error": "not_found"}


def test_api_server_blocks_idle_requests_when_worker_is_unhealthy() -> None:
    server = MonitoringWSGIServer(("127.0.0.1", 0), WSGIRequestHandler)
    server.worker = _Worker()
    server.worker.is_alive = False
    try:
        try:
            server.service_actions()
        except RuntimeError as exc:
            assert str(exc) == "durable job worker is unavailable"
        else:
            raise AssertionError("unhealthy worker must stop the API server")
    finally:
        server.server_close()
