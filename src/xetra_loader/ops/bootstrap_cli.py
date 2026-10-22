"""Prepare the configured CPU budget before importing Polars-backed modules."""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence

from xetra_loader.config import ConfigurationError, resolve_cores
from xetra_loader.logging_config import configure_logging


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="xdl-bootstrap")
    parser.add_argument("--confirm-destructive-reset", action="store_true")
    parser.add_argument(
        "--cores",
        type=int,
        default=None,
        help="CPU budget for Polars, PostgreSQL, and independent downloads (default: config or 6)",
    )
    arguments = parser.parse_args(argv)
    try:
        cores = resolve_cores(arguments.cores)
    except ConfigurationError as exc:
        parser.error(str(exc))
    configure_logging("xdl-bootstrap")
    os.environ["POLARS_MAX_THREADS"] = str(cores)
    os.environ["XDL_CORES"] = str(cores)

    from xetra_loader.ops.bootstrap import main as bootstrap_main

    return bootstrap_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
