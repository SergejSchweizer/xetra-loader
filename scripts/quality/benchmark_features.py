#!/usr/bin/env python3
"""Compare global and per-instrument Polars plans on causal window features."""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPOSITORY_ROOT))
sys.path.insert(0, str(_REPOSITORY_ROOT / "src"))
from xetra_loader.config import resolve_cores  # noqa: E402


def _configure_threads() -> None:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--cores", type=int)
    args, _ = parser.parse_known_args()
    os.environ["POLARS_MAX_THREADS"] = str(resolve_cores(args.cores))


_configure_threads()
import polars as pl  # noqa: E402

FEATURES = (
    "log_return_1",
    "rolling_cum_return_4",
    "adjusted_close_return_mean_5obs",
    "adjusted_close_volatility_20obs",
    "adjusted_close_sma_ratio_5_20",
    "relative_volume_20obs",
)


def input_frame(rows: int, instruments: int) -> pl.DataFrame:
    if rows < 1 or instruments < 1 or rows % instruments:
        raise ValueError("rows must be a positive multiple of instruments")
    count = rows // instruments
    return pl.DataFrame({
        "instrument": [f"I{i:05d}" for i in range(instruments) for _ in range(count)],
        "observation": [n for _ in range(instruments) for n in range(count)],
        "adjusted_close_level": [
            80 + i * 0.07 + n * 0.003 + ((n * 17 + i * 5) % 23) * 0.004
            for i in range(instruments) for n in range(count)
        ],
        "volume_level": [
            float(500 + i * 7 + (n % 31) * 11 + ((n * 17) % 13))
            for i in range(instruments) for n in range(count)
        ],
    })


def _window(column: str, operation: str, size: int) -> pl.Expr:
    options: dict[str, int] = {"window_size": size, "min_samples": size}
    if operation == "rolling_std":
        options["ddof"] = 1
    return getattr(pl.col(column), operation)(**options).over("instrument")


def _compute(frame: pl.DataFrame) -> pl.DataFrame:
    returns = frame.lazy().sort(["instrument", "observation"]).with_columns(
        (
            pl.col("adjusted_close_level")
            / pl.col("adjusted_close_level").shift(1).over("instrument")
        ).log().alias("log_return_1")
    )
    result = returns.with_columns(
        (_window("log_return_1", "rolling_sum", 4).exp() - 1).alias(
            "rolling_cum_return_4"
        ),
        _window("log_return_1", "rolling_mean", 5).alias(
            "adjusted_close_return_mean_5obs"
        ),
        _window("log_return_1", "rolling_std", 20).alias(
            "adjusted_close_volatility_20obs"
        ),
        (
            _window("adjusted_close_level", "rolling_mean", 5)
            / _window("adjusted_close_level", "rolling_mean", 20)
        ).alias("adjusted_close_sma_ratio_5_20"),
        (
            pl.col("volume_level") / _window("volume_level", "rolling_mean", 20)
        ).alias("relative_volume_20obs"),
    )
    return result.select("instrument", "observation", *FEATURES).collect()


def compute_global_plan(frame: pl.DataFrame) -> pl.DataFrame:
    """Compute all instruments in one sorted lazy plan."""

    return _compute(frame)


def compute_per_instrument_plans(frame: pl.DataFrame) -> pl.DataFrame:
    """Eagerly partition, then execute a separate Polars plan per instrument."""

    outputs = [_compute(group) for group in frame.partition_by("instrument")]
    return pl.concat(outputs, how="vertical").sort(["instrument", "observation"])


def _timed(operation: Callable[[], pl.DataFrame], rounds: int) -> tuple[float, pl.DataFrame]:
    operation()
    samples: list[float] = []
    output: pl.DataFrame | None = None
    for _ in range(rounds):
        started = time.perf_counter()
        output = operation()
        samples.append(time.perf_counter() - started)
    assert output is not None
    return statistics.median(samples), output


def validate_reference() -> None:
    """Compare the benchmarked formulas with the independent full-feature oracle."""

    from tests.qa.feature_reference import make_fixture, reference_features

    fixture = make_fixture(days=65)
    grouped = {
        code: sorted((q for q in fixture if q.code == code), key=lambda q: q.trade_date)
        for code in sorted({q.code for q in fixture})
    }
    frame = pl.DataFrame([
        {
            "instrument": code,
            "observation": index,
            "adjusted_close_level": q.adjusted_close,
            "volume_level": float(q.volume),
        }
        for code, quotes in grouped.items()
        for index, q in enumerate(quotes)
    ])
    oracle = reference_features(fixture)
    for row in compute_global_plan(frame).iter_rows(named=True):
        code, index = str(row["instrument"]), int(row["observation"])
        expected = oracle[(code, grouped[code][index].trade_date)]
        for name in FEATURES:
            actual, reference = row[name], expected[name]
            if actual is None or reference is None:
                if actual is not None or reference is not None:
                    raise AssertionError(f"null mismatch for {name} at {code}:{index}")
            elif not math.isclose(float(actual), float(reference), rel_tol=1e-10, abs_tol=1e-12):
                raise AssertionError(f"reference mismatch for {name} at {code}:{index}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="benchmark-features")
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--instruments", type=int, default=100)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--cores", type=int, default=None)
    args = parser.parse_args(argv)
    cores = resolve_cores(args.cores)
    if args.rounds < 1:
        raise ValueError("rounds must be positive")
    if int(os.environ["POLARS_MAX_THREADS"]) != cores:
        raise ValueError("Polars thread budget must be set before engine import")
    validate_reference()
    frame = input_frame(args.rows, args.instruments)
    global_time, global_result = _timed(lambda: compute_global_plan(frame), args.rounds)
    grouped_time, grouped_result = _timed(
        lambda: compute_per_instrument_plans(frame), args.rounds
    )
    from polars.testing import assert_frame_equal

    assert_frame_equal(
        global_result, grouped_result, check_exact=False, rel_tol=1e-12, abs_tol=1e-12
    )
    print(json.dumps({
        "rows": args.rows,
        "instruments": args.instruments,
        "rounds": args.rounds,
        "polars_threads": cores,
        "oracle_validation": "pass",
        "candidate_equivalence": "pass",
        "median_seconds": {
            "global_window_plan": global_time,
            "per_instrument_plans": grouped_time,
        },
        "global_plan_speedup": grouped_time / global_time,
    }, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
