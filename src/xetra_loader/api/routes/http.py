"""Small WSGI request/response types shared by independent API routes."""

from __future__ import annotations

from dataclasses import dataclass

from xetra_loader.sync.core import JSONValue


@dataclass(frozen=True, slots=True)
class ApiRequest:
    method: str
    path: str
    authorization: str
    content_type: str
    body: bytes


@dataclass(frozen=True, slots=True)
class ApiResponse:
    status: int
    payload: dict[str, JSONValue]
