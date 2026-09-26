"""Deterministic contract for the PostgreSQL XETRA feature materialization.

The catalog deliberately distinguishes exposed level columns from raw SQL
inputs.  ``adjusted_close_level`` and ``volume_level`` are the only exposed
base levels.  Raw OHLC values may only feed the three explicitly approved
intraday feature families.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Final

FEATURE_CATALOG_VERSION: Final[int] = 1
BASE_LEVEL_COLUMNS: Final[tuple[str, ...]] = (
    "adjusted_close_level",
    "volume_level",
)
FORBIDDEN_LEVEL_COLUMNS: Final[tuple[str, ...]] = (
    "open_level",
    "high_level",
    "low_level",
    "close_level",
)


@dataclass(frozen=True, slots=True)
class FeatureSpec:
    """One catalogued feature family and its semantic calculation contract."""

    name: str
    source_columns: tuple[str, ...]
    windows: tuple[int | str, ...]
    formula: str
    min_observations: int | None
    internal_ohlc: bool = False


def _series_specs(
    prefix: str,
    windows: tuple[int, ...],
    formula: str,
    *,
    suffix: str = "obs",
    source: tuple[str, ...] = ("adjusted_close_level",),
    min_observations: int | None = None,
) -> tuple[FeatureSpec, ...]:
    return tuple(
        FeatureSpec(
            name=f"{prefix}_{window}{suffix}",
            source_columns=source,
            windows=(window,),
            formula=formula,
            min_observations=min_observations if min_observations is not None else window,
        )
        for window in windows
    )


FEATURE_SPECS: Final[tuple[FeatureSpec, ...]] = (
    *_series_specs(
        "adjusted_close_log_return",
        (1, 3, 5, 10, 20),
        "ln(adjusted_close_level / lag(adjusted_close_level, n))",
    ),
    *_series_specs(
        "adjusted_close_return_geom",
        (5, 10, 20),
        "exp(sum(adjusted_close_log_return_1obs over n)) - 1",
        suffix="obs_pct",
    ),
    *_series_specs(
        "adjusted_close_return_mean",
        (5, 10, 20),
        "avg(adjusted_close_log_return_1obs over n)",
    ),
    *_series_specs(
        "adjusted_close_volatility",
        (5, 10, 20, 40),
        "stddev_samp(adjusted_close_log_return_1obs over n)",
    ),
    *_series_specs(
        "high_low_range",
        (1, 5),
        "avg((high - low) / close over n)",
        source=("open", "high", "low", "close"),
        min_observations=1,
    ),
    *_series_specs(
        "intraday_return",
        (1, 5),
        "avg((close - open) / open over n)",
        source=("open", "close"),
        min_observations=1,
    ),
    *_series_specs(
        "overnight_gap",
        (1, 5),
        "avg((open - lag(close, 1)) / lag(close, 1) over n)",
        source=("open", "close"),
        min_observations=2,
    ),
    FeatureSpec(
        name="adjusted_close_sma_ratio_5_20",
        source_columns=("adjusted_close_level",),
        windows=("5/20",),
        formula="sma(adjusted_close_level, 5) / sma(adjusted_close_level, 20)",
        min_observations=20,
    ),
    FeatureSpec(
        name="adjusted_close_sma_ratio_10_20",
        source_columns=("adjusted_close_level",),
        windows=("10/20",),
        formula="sma(adjusted_close_level, 10) / sma(adjusted_close_level, 20)",
        min_observations=20,
    ),
    FeatureSpec(
        name="adjusted_close_sma_ratio_10_40",
        source_columns=("adjusted_close_level",),
        windows=("10/40",),
        formula="sma(adjusted_close_level, 10) / sma(adjusted_close_level, 40)",
        min_observations=40,
    ),
    *_series_specs(
        "adjusted_close_rsi",
        (7, 14),
        "Wilder RSI over adjusted_close gains and losses",
    ),
    *_series_specs(
        "adjusted_close_roc",
        (3, 5, 10, 20),
        "adjusted_close_level / lag(adjusted_close_level, n) - 1",
    ),
    *_series_specs(
        "adjusted_close_drawdown",
        (20, 60),
        "adjusted_close_level / rolling_max(adjusted_close_level, n) - 1",
    ),
    *_series_specs(
        "relative_volume",
        (5, 10, 20),
        "volume_level / rolling_mean(volume_level, n)",
        source=("volume_level",),
    ),
    *_series_specs(
        "volume_zscore",
        (5, 10, 20),
        "(volume_level - rolling_mean) / rolling_std",
        source=("volume_level",),
    ),
    FeatureSpec(
        name="breadth_daily",
        source_columns=("adjusted_close_level",),
        windows=("daily",),
        formula="cross-sectional fraction of positive adjusted-close daily returns",
        min_observations=1,
    ),
    *_series_specs(
        "breadth",
        (5, 10, 20),
        "rolling_mean(breadth_daily, n)",
        source=("adjusted_close_level",),
    ),
    FeatureSpec(
        name="dispersion_daily",
        source_columns=("adjusted_close_level",),
        windows=("daily",),
        formula="cross-sectional stddev of adjusted-close daily returns",
        min_observations=2,
    ),
    *_series_specs(
        "dispersion",
        (5, 10, 20),
        "rolling_mean(dispersion_daily, n)",
        source=("adjusted_close_level",),
    ),
    *_series_specs(
        "average_correlation",
        (5, 10, 20),
        "mean pairwise rolling correlation of adjusted-close daily returns",
        source=("adjusted_close_level",),
        min_observations=2,
    ),
)

INTERNAL_OHLC_FAMILIES: Final[frozenset[str]] = frozenset(
    {"high_low_range", "intraday_return", "overnight_gap"}
)


def feature_columns() -> tuple[str, ...]:
    """Return the deterministic output columns after the two base levels."""

    return BASE_LEVEL_COLUMNS + tuple(spec.name for spec in FEATURE_SPECS)


def feature_catalog_payload() -> dict[str, object]:
    """Return a JSON-compatible catalog payload for fingerprinting and tests."""

    return {
        "version": FEATURE_CATALOG_VERSION,
        "base_level_columns": BASE_LEVEL_COLUMNS,
        "forbidden_level_columns": FORBIDDEN_LEVEL_COLUMNS,
        "internal_ohlc_families": tuple(sorted(INTERNAL_OHLC_FAMILIES)),
        "features": tuple(asdict(spec) for spec in FEATURE_SPECS),
        "train_fold_standardization": {
            "scope": "downstream_fold_aware_operation",
            "fit_rows": "train_only",
            "global_fit": False,
        },
    }


def validate_feature_catalog() -> None:
    """Validate invariants that must hold before SQL generation."""

    names = [spec.name for spec in FEATURE_SPECS]
    if len(names) != len(set(names)):
        raise ValueError("feature catalog contains duplicate output names")
    forbidden = set(FORBIDDEN_LEVEL_COLUMNS)
    if forbidden.intersection(feature_columns()):
        raise ValueError("feature catalog exposes a forbidden OHLC level")
    for spec in FEATURE_SPECS:
        uses_ohlc = bool(set(spec.source_columns) & {"open", "high", "low", "close"})
        family = spec.name.split("_", 1)[0]
        if uses_ohlc and not any(spec.name.startswith(name) for name in INTERNAL_OHLC_FAMILIES):
            raise ValueError(f"unapproved OHLC feature family: {spec.name}")
        if spec.min_observations is not None and spec.min_observations < 1:
            raise ValueError(f"invalid minimum observation count: {spec.name}")
        if not family:
            raise ValueError("feature name must not be empty")


validate_feature_catalog()
FEATURE_CATALOG_FINGERPRINT: Final[str] = hashlib.sha256(
    json.dumps(feature_catalog_payload(), sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()
