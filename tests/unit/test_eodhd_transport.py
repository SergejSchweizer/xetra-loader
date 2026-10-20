import traceback
from decimal import Decimal
from email.message import Message
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request

import pytest

from xetra_loader.eodhd import EodhdTransport, RetryPolicy, scrub_url
from xetra_loader.eodhd.transport import TransportConfig


class FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def read(self) -> bytes:
        return self.payload

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        return None


def test_missing_token_is_clear(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EODHD_API_TOKEN", raising=False)
    with pytest.raises(ValueError, match="EODHD_API_TOKEN is required"):
        EodhdTransport()


def test_typed_get_uses_fixture_seam() -> None:
    seen: list[str] = []

    def opener(request: Request, timeout: float) -> FakeResponse:
        assert timeout == 5.0
        seen.append(request.full_url)
        return FakeResponse(b'{"ok":true,"rows":2}')

    transport = EodhdTransport(
        TransportConfig(token="top-secret", timeout_seconds=5.0, opener=opener)
    )
    assert transport.get_json("exchange-symbol-list/XETRA", {"limit": 2}) == {
        "ok": True,
        "rows": 2,
    }
    assert "api_token=top-secret" in seen[0]


def test_transport_decodes_provider_decimals_without_binary_float_round_trip() -> None:
    def opener(request: Request, timeout: float) -> FakeResponse:
        del request, timeout
        return FakeResponse(b'[{"value":1.0000000000000000001}]')

    payload = EodhdTransport(TransportConfig(token="secret", opener=opener)).get_json(
        "div/AAA.XETRA"
    )
    assert isinstance(payload, list)
    assert payload == [{"value": Decimal("1.0000000000000000001")}]


def test_network_retries_are_bounded() -> None:
    attempts = 0
    sleeps: list[float] = []

    def opener(request: Request, timeout: float) -> FakeResponse:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise URLError("temporary")
        return FakeResponse(b"[]")

    transport = EodhdTransport(TransportConfig(
        token="secret",
        opener=opener,
        sleeper=sleeps.append,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0.1, max_delay_seconds=1.0),
    ))
    assert transport.get_json("eod/AAA.XETRA") == []
    assert attempts == 3
    assert sleeps == [0.1, 0.2]


@pytest.mark.parametrize(
    ("failure", "expected"),
    (
        ("http-400", "HTTP 400"),
        ("http-429", "HTTP 429"),
        ("http-500", "HTTP 500"),
        ("network", "request failed"),
        ("invalid-json", "invalid JSON"),
    ),
)
def test_provider_failures_never_retain_or_render_token(
    failure: str,
    expected: str,
) -> None:
    token = "do not/leak+this"

    def opener(request: Request, timeout: float) -> FakeResponse:
        del timeout
        if failure.startswith("http-"):
            status = int(failure.removeprefix("http-"))
            raise HTTPError(request.full_url, status, "bad request", Message(), None)
        if failure == "network":
            raise URLError(request.full_url)
        return FakeResponse(b"not-json")

    transport = EodhdTransport(TransportConfig(
        token=token,
        opener=opener,
        retry_policy=RetryPolicy(max_attempts=1),
    ))
    with pytest.raises(RuntimeError) as captured:
        transport.get_json("eod/AAA.XETRA")

    error = captured.value
    rendered = "\n".join(
        [
            str(error),
            repr(error),
            *traceback.format_exception(type(error), error, error.__traceback__),
            repr(error.__cause__),
            repr(error.__context__),
        ]
    )
    assert token not in rendered
    assert urlencode({"api_token": token}).split("=", 1)[1] not in rendered
    assert error.__cause__ is None
    assert error.__context__ is None
    assert expected in str(error)


def test_scrub_url_removes_common_secret_query_parameters() -> None:
    scrubbed = scrub_url("https://eodhd.com/api/eod/AAA?api_token=secret&fmt=json")
    assert "secret" not in scrubbed
    assert "api_token=***" in scrubbed
    assert "fmt=json" in scrubbed
