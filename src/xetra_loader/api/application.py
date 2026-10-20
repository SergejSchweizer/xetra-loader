"""WSGI application composing independent REST route modules."""

from __future__ import annotations

import json
from collections.abc import Iterable
from http import HTTPStatus
from typing import Protocol, cast
from urllib.parse import urlsplit
from wsgiref.types import StartResponse, WSGIApplication, WSGIEnvironment

from xetra_loader.api.routes.health import handle_health
from xetra_loader.api.routes.http import ApiRequest, ApiResponse
from xetra_loader.api.routes.jobs import JobRepository, handle_jobs

_MAX_REQUEST_BYTES = 4096


class WorkerHealth(Protocol):
    @property
    def is_alive(self) -> bool: ...


class LoaderApiApplication:
    """Compose health and jobs routes without coupling them to ingestion code."""

    def __init__(self, store: JobRepository, worker: WorkerHealth, token: str) -> None:
        if not token:
            raise ValueError("the API bearer token must not be empty")
        self._store = store
        self._worker = worker
        self._token = token

    def __call__(
        self,
        environ: WSGIEnvironment,
        start_response: StartResponse,
    ) -> Iterable[bytes]:
        request = self._request(environ)
        response: ApiResponse | None
        if request.body == b"__oversized__":
            response = ApiResponse(413, {"error": "request_too_large"})
        elif request.body == b"__invalid_length__":
            response = ApiResponse(400, {"error": "invalid_content_length"})
        else:
            response = handle_health(request, worker_alive=self._worker.is_alive)
            if response is None:
                response = handle_jobs(request, store=self._store, token=self._token)
        if response is None:
            response = ApiResponse(404, {"error": "not_found"})
        return self._send(start_response, response)

    @staticmethod
    def _request(environ: WSGIEnvironment) -> ApiRequest:
        method = str(environ.get("REQUEST_METHOD", "GET")).upper()
        path = urlsplit(str(environ.get("PATH_INFO", "/"))).path
        authorization = str(environ.get("HTTP_AUTHORIZATION", ""))
        content_type = str(environ.get("CONTENT_TYPE", ""))
        body = b""
        if method == "POST":
            try:
                length = int(environ.get("CONTENT_LENGTH") or "0")
            except ValueError:
                return ApiRequest(
                    method, path, authorization, content_type, b"__invalid_length__"
                )
            if length < 0 or length > _MAX_REQUEST_BYTES:
                return ApiRequest(method, path, authorization, content_type, b"__oversized__")
            input_stream = cast(Iterable[bytes], environ["wsgi.input"])
            read = getattr(input_stream, "read", None)
            if not callable(read):
                return ApiRequest(
                    method, path, authorization, content_type, b"__missing_body__"
                )
            body = cast(bytes, read(length))
        return ApiRequest(method, path, authorization, content_type, body)

    @staticmethod
    def _send(start_response: StartResponse, response: ApiResponse) -> list[bytes]:
        body = json.dumps(response.payload, sort_keys=True, separators=(",", ":")).encode()
        status = HTTPStatus(response.status)
        start_response(
            f"{response.status} {status.phrase}",
            [
                ("Content-Type", "application/json; charset=utf-8"),
                ("Cache-Control", "no-store"),
                ("Content-Length", str(len(body))),
            ],
        )
        return [body]


def create_application(
    *,
    store: JobRepository,
    worker: WorkerHealth,
    token: str,
) -> WSGIApplication:
    return LoaderApiApplication(store, worker, token)
