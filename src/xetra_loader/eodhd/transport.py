"""Typed, retry-bounded HTTP transport for EODHD."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

type JSONValue = str | int | float | bool | None | list[JSONValue] | dict[str, JSONValue]


class BinaryResponse(Protocol):
    """Minimal response surface needed by the transport."""

    def read(self) -> bytes: ...

    def __enter__(self) -> BinaryResponse: ...

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None: ...


type OpenUrl = Callable[[Request, float], BinaryResponse]
type Sleeper = Callable[[float], None]


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Bounded exponential retry policy."""

    max_attempts: int = 4
    base_delay_seconds: float = 0.25
    max_delay_seconds: float = 2.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        if self.base_delay_seconds < 0 or self.max_delay_seconds < 0:
            raise ValueError("retry delays must be non-negative")

    def delay(self, attempt: int) -> float:
        delay = self.base_delay_seconds * (2 ** max(0, attempt - 1))
        return float(min(delay, self.max_delay_seconds))


@dataclass(frozen=True, slots=True)
class TransportConfig:
    """Provider connection and retry settings, including injectable test seams."""

    token: str | None = None
    base_url: str = "https://eodhd.com/api"
    timeout_seconds: float = 30.0
    retry_policy: RetryPolicy | None = None
    opener: OpenUrl | None = None
    sleeper: Sleeper = field(default=time.sleep)


class EodhdTransport:
    """Small provider seam with secret-safe errors and deterministic retry behavior."""

    def __init__(self, config: TransportConfig | None = None) -> None:
        resolved_config = config or TransportConfig()
        resolved_token = (
            resolved_config.token
            if resolved_config.token is not None
            else os.getenv("EODHD_API_TOKEN")
        )
        if resolved_token is None or not resolved_token.strip():
            raise ValueError("EODHD_API_TOKEN is required")
        if resolved_config.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._token = resolved_token
        self._base_url = resolved_config.base_url.rstrip("/")
        self._timeout_seconds = resolved_config.timeout_seconds
        self._retry_policy = resolved_config.retry_policy or RetryPolicy()
        self._opener = resolved_config.opener or _default_open
        self._sleeper = resolved_config.sleeper

    def get_json(
        self,
        path: str,
        params: Mapping[str, str | int | float] | None = None,
    ) -> JSONValue:
        """GET and decode one JSON response, retrying only transient failures."""

        normalized_path = path.strip("/")
        if not normalized_path:
            raise ValueError("path must be non-empty")
        query: dict[str, str | int | float] = dict(params or {})
        query["api_token"] = self._token
        query.setdefault("fmt", "json")
        url = f"{self._base_url}/{normalized_path}?{urlencode(query)}"
        request = Request(
            url,
            headers={"Accept": "application/json", "User-Agent": "xetra-loader"},
        )
        safe_endpoint = _safe_endpoint(normalized_path)
        failure: RuntimeError | None = None

        for attempt in range(1, self._retry_policy.max_attempts + 1):
            try:
                with self._opener(request, self._timeout_seconds) as response:
                    payload = json.loads(
                        response.read().decode("utf-8"),
                        parse_float=Decimal,
                        parse_constant=Decimal,
                    )
                return cast(JSONValue, payload)
            except HTTPError as exc:
                if not _is_retryable_status(exc.code) or attempt == self._retry_policy.max_attempts:
                    failure = RuntimeError(
                        f"EODHD request failed with HTTP {exc.code} for {safe_endpoint}"
                    )
                    break
                self._sleeper(_retry_delay(exc, self._retry_policy, attempt))
            except URLError:
                if attempt == self._retry_policy.max_attempts:
                    failure = RuntimeError(f"EODHD request failed for {safe_endpoint}")
                    break
                self._sleeper(self._retry_policy.delay(attempt))
            except (UnicodeDecodeError, json.JSONDecodeError):
                failure = RuntimeError(f"EODHD returned invalid JSON for {safe_endpoint}")
                break

        if failure is not None:
            raise failure
        raise AssertionError("retry loop exhausted unexpectedly")


def scrub_url(url: str) -> str:
    """Remove provider secrets from a URL before it is logged or reported."""

    parts = urlsplit(url)
    safe_pairs: list[str] = []
    for pair in parts.query.split("&"):
        if not pair:
            continue
        key = pair.split("=", 1)[0]
        if key.lower() in {"api_token", "token", "apikey", "api_key"}:
            safe_pairs.append(f"{key}=***")
        else:
            safe_pairs.append(pair)
    return urlunsplit(
        (parts.scheme, parts.netloc, parts.path, "&".join(safe_pairs), parts.fragment)
    )


def _safe_endpoint(path: str) -> str:
    """Return only a request path, never any caller-supplied query parameters."""

    parsed = urlsplit(f"https://eodhd.invalid/{path.lstrip('/')}")
    return parsed.path


def _default_open(request: Request, timeout: float) -> BinaryResponse:
    return cast(BinaryResponse, urlopen(request, timeout=timeout))


def _is_retryable_status(status: int) -> bool:
    return status == 429 or status in {500, 502, 503, 504}


def _retry_delay(exc: HTTPError, policy: RetryPolicy, attempt: int) -> float:
    retry_after = exc.headers.get("Retry-After") if exc.headers is not None else None
    if retry_after is not None:
        try:
            return min(float(retry_after), policy.max_delay_seconds)
        except ValueError:
            pass
    return policy.delay(attempt)
