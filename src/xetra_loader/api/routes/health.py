"""Health REST boundary; intentionally independent of database access."""

from __future__ import annotations

from xetra_loader.api.routes.http import ApiRequest, ApiResponse


def handle_health(request: ApiRequest, *, worker_alive: bool) -> ApiResponse | None:
    if request.method != "GET" or request.path != "/healthz":
        return None
    status = "ok" if worker_alive else "unavailable"
    return ApiResponse(
        status=200 if worker_alive else 503,
        payload={"status": status, "worker": "running" if worker_alive else "stopped"},
    )
