"""REST boundary for enqueueing and inspecting weekly synchronization jobs."""

from __future__ import annotations

import hmac
import json
import re
from typing import Protocol, cast
from uuid import UUID

import psycopg

from xetra_loader.api.jobs import JobAlreadyActive, JobRecord
from xetra_loader.api.routes.http import ApiRequest, ApiResponse
from xetra_loader.sync.core import JSONValue

_JOB_PATH = re.compile(r"^/v1/jobs/([0-9a-fA-F-]{36})$")


class JobRepository(Protocol):
    def enqueue_weekly_sync(self) -> JobRecord: ...

    def get(self, job_id: UUID) -> JobRecord | None: ...


def handle_jobs(
    request: ApiRequest,
    *,
    store: JobRepository,
    token: str,
) -> ApiResponse | None:
    if request.method == "POST" and request.path == "/v1/jobs":
        return _handle_enqueue(request, store, token)
    if request.method == "GET":
        return _handle_job_status(request, store, token)
    return None


def _handle_enqueue(request: ApiRequest, store: JobRepository, token: str) -> ApiResponse:
    invalid_request = _validate_enqueue_request(request, token)
    if invalid_request is not None:
        return invalid_request
    try:
        job = store.enqueue_weekly_sync()
    except JobAlreadyActive as exc:
        return ApiResponse(
            409,
            {"error": "weekly_sync_already_active", "job_id": exc.job.job_id},
        )
    except psycopg.Error:
        return _error(503, "database_unavailable")
    return ApiResponse(202, job.as_dict())


def _validate_enqueue_request(request: ApiRequest, token: str) -> ApiResponse | None:
    if not _authorized(request.authorization, token):
        return _error(401, "unauthorized")
    if request.content_type.split(";", maxsplit=1)[0].strip().lower() != "application/json":
        return _error(415, "application_json_required")
    try:
        payload = json.loads(request.body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _error(400, "invalid_json")
    if not isinstance(payload, dict) or payload != {"type": "weekly_sync"}:
        return _error(400, "unsupported_job_request")
    return None


def _handle_job_status(
    request: ApiRequest,
    store: JobRepository,
    token: str,
) -> ApiResponse | None:
    match = _JOB_PATH.fullmatch(request.path)
    if match is None:
        return None
    if not _authorized(request.authorization, token):
        return _error(401, "unauthorized")
    try:
        job_id = UUID(match.group(1))
    except ValueError:
        return _error(404, "job_not_found")
    try:
        found_job = store.get(job_id)
    except psycopg.Error:
        return _error(503, "database_unavailable")
    if found_job is None:
        return _error(404, "job_not_found")
    return ApiResponse(200, found_job.as_dict())


def _authorized(header: str, token: str) -> bool:
    prefix = "Bearer "
    return header.startswith(prefix) and hmac.compare_digest(header[len(prefix) :], token)


def _error(status: int, code: str) -> ApiResponse:
    return ApiResponse(status, cast(dict[str, JSONValue], {"error": code}))
