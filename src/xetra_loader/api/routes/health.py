"""Health REST boundary; intentionally independent of database access."""

from __future__ import annotations

from xetra_loader.api.routes.http import ApiRequest, ApiResponse


def handle_health(
    request: ApiRequest,
    *,
    worker_alive: bool,
    database_ready: bool,
) -> ApiResponse | None:
    if request.method != "GET" or request.path != "/healthz":
        return None
    ready = worker_alive and database_ready
    status = "ok" if ready else "unavailable"
    return ApiResponse(
        status=200 if ready else 503,
        payload={
            "status": status,
            "worker": "running" if worker_alive else "stopped",
            "database": "ready" if database_ready else "unavailable",
        },
    )
