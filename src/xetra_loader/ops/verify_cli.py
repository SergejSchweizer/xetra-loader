"""Set the CLI CPU budget before importing the Polars-backed verifier."""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence

from xetra_loader.config import ConfigurationError, resolve_cores


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--cores", type=int, default=None)
    arguments, _ = parser.parse_known_args(argv)
    try:
        cores = resolve_cores(arguments.cores)
    except ConfigurationError as exc:
        parser.error(str(exc))
    os.environ["POLARS_MAX_THREADS"] = str(cores)
    os.environ["XDL_CORES"] = str(cores)

    from xetra_loader.ops.verify_postgres_sync import main as verifier_main

    return verifier_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
