#!/usr/bin/env python3
"""Benchmark deterministic Gold quote transformation with a bounded core budget."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from collections.abc import Sequence
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPOSITORY_ROOT / "src"))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="benchmark-gold-transform")
    parser.add_argument("--rows", type=int, default=600_000)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--cores", type=int, default=None)
    parser.add_argument("--input-order", choices=("sorted", "reverse"), default="reverse")
    args = parser.parse_args(argv)

    from xetra_loader.config import resolve_cores

    cores = resolve_cores(args.cores)
    os.environ["POLARS_MAX_THREADS"] = str(cores)
    os.environ["XDL_CORES"] = str(cores)
    import polars as pl

    from xetra_loader.contracts.quotes import QuoteRecord
    from xetra_loader.gold._ordering import _PARALLEL_SORT_THRESHOLD
    from xetra_loader.gold.quotes import build_quote_gold

    if args.rows < 1 or args.rounds < 1:
        parser.error("--rows and --rounds must be positive")
    instrument_count = min(200, args.rows)
    rows_per_instrument = (args.rows + instrument_count - 1) // instrument_count
    start_date = date(2010, 1, 1)
    quotes = [
        QuoteRecord(
            isin=f"DE{instrument:010d}",
            exchange="XETRA",
            code=f"S{instrument:05d}",
            trade_date=start_date + timedelta(days=observation),
            open=None,
            high=None,
            low=None,
            close=None,
            adjusted_close=Decimal(instrument + observation + 1),
            volume=instrument + observation,
        )
        for instrument in range(instrument_count)
        for observation in range(rows_per_instrument)
    ][: args.rows]
    if args.input_order == "reverse":
        quotes.reverse()

    results: dict[int, tuple[float, str]] = {}
    for budget in (1, cores):
        samples: list[float] = []
        fingerprint = ""
        for _ in range(args.rounds):
            started = time.perf_counter()
            gold = build_quote_gold(quotes, cores=budget)
            samples.append(time.perf_counter() - started)
            fingerprint = gold.semantic_fingerprint
        results[budget] = (statistics.median(samples), fingerprint)
    if results[1][1] != results[cores][1]:
        raise RuntimeError("serial and parallel Gold fingerprints differ")

    serial_seconds, _ = results[1]
    parallel_seconds, _ = results[cores]
    print(json.dumps({
        "rows": len(quotes),
        "input_order": args.input_order,
        "rounds": args.rounds,
        "cores": cores,
        "polars_max_threads": os.environ["POLARS_MAX_THREADS"],
        "polars_thread_pool_size": pl.thread_pool_size(),
        "fingerprints_equal": True,
        "fingerprint": results[cores][1],
        "median_seconds": {"serial": serial_seconds, "configured_cores": parallel_seconds},
        "parallel_transform_active": args.rows >= 400_000 and cores > 1,
        "parallel_sort_active": (
            args.input_order == "reverse"
            and args.rows >= _PARALLEL_SORT_THRESHOLD
            and cores > 1
            and pl.thread_pool_size() == cores
        ),
        "speedup": serial_seconds / parallel_seconds,
    }, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
