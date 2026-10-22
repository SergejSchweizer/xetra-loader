"""Small authenticated client used by the weekly cron entry."""

from __future__ import annotations

import argparse
import json
import logging
import urllib.error
import urllib.request
from collections.abc import Sequence

from xetra_loader.config import resolve_api_base_url, resolve_api_token
from xetra_loader.logging_config import configure_logging

_LOGGER = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging("xdl-enqueue-weekly")
    parser = argparse.ArgumentParser(prog="xdl-enqueue-weekly")
    parser.add_argument("--base-url", default=None)
    arguments = parser.parse_args(argv)
    url = resolve_api_base_url(arguments.base_url) + "/v1/jobs"
    request = urllib.request.Request(
        url,
        data=b'{"type":"weekly_sync"}',
        headers={
            "Authorization": f"Bearer {resolve_api_token()}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            payload = json.loads(response.read())
            _LOGGER.info("weekly_enqueue_response status=%d", response.status)
    except urllib.error.HTTPError as exc:
        _LOGGER.warning("weekly_enqueue_http_error status=%d", exc.code)
        try:
            payload = json.loads(exc.read())
        except (UnicodeDecodeError, json.JSONDecodeError):
            payload = {"error": "api_request_failed"}
        if exc.code == 409 and payload.get("error") == "weekly_sync_already_active":
            print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
            return 0
        print(json.dumps({"status": "failed", "reason": "api_request_failed"}))
        return 1
    except (OSError, TimeoutError, UnicodeDecodeError, json.JSONDecodeError):
        _LOGGER.error("weekly_enqueue_failed reason=api_unavailable")
        print(json.dumps({"status": "failed", "reason": "api_unavailable"}))
        return 1
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
