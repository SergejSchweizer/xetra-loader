"""Safety helpers for tests that connect to PostgreSQL."""

from __future__ import annotations

import ipaddress
import re

from psycopg.conninfo import conninfo_to_dict

_TEST_DATABASE = re.compile(r"(?:^|[_-])test(?:$|[_-])", re.IGNORECASE)
_LOCAL_HOSTS = frozenset({"localhost", "localhost.localdomain"})


def validate_local_test_postgres_dsn(dsn: str) -> None:
    """Reject DSNs that could make destructive integration tests touch production."""

    try:
        options = conninfo_to_dict(dsn)
    except Exception as exc:
        raise ValueError("XDL_TEST_POSTGRES_DSN is not valid PostgreSQL connection info") from exc

    database = options.get("dbname")
    if not isinstance(database, str) or _TEST_DATABASE.search(database) is None:
        raise ValueError(
            "XDL_TEST_POSTGRES_DSN database name must explicitly contain a test component"
        )
    for key in ("host", "hostaddr"):
        host = options.get(key)
        if host is None:
            continue
        if not isinstance(host, str):
            raise ValueError("XDL_TEST_POSTGRES_DSN host must be local")
        if host.startswith("/"):
            continue
        if host.lower() in _LOCAL_HOSTS:
            continue
        try:
            address = ipaddress.ip_address(host.strip("[]"))
        except ValueError as exc:
            raise ValueError(
                "XDL_TEST_POSTGRES_DSN must use a local host or Unix socket"
            ) from exc
        if not address.is_loopback:
            raise ValueError("XDL_TEST_POSTGRES_DSN must use a local host or Unix socket")
