"""Set the Polars CPU budget before loading the quote-repair implementation."""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence

from xetra_loader.config import ConfigurationError, resolve_cores


def main(argv: Sequence[str] | None = None) -> int:
    """Configure native Polars threads, then delegate to the guarded repair CLI."""

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--cores", type=int, default=None)
    arguments, _ = parser.parse_known_args(argv)
    try:
        cores = resolve_cores(arguments.cores)
    except ConfigurationError as exc:
        parser.error(str(exc))
    os.environ["POLARS_MAX_THREADS"] = str(cores)
    os.environ["XDL_CORES"] = str(cores)

    from xetra_loader.ops.repair_quote_quarantines import main as repair_main

    return repair_main(None if argv is None else list(argv))


if __name__ == "__main__":
    raise SystemExit(main())
