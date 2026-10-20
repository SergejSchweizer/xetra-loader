"""Deterministic contract for the PostgreSQL XETRA feature materialization.

The catalog deliberately distinguishes exposed level columns from raw SQL
inputs.  ``adjusted_close_level`` and ``volume_level`` are the only exposed
base levels.  Raw OHLC values may only feed the three explicitly approved
intraday feature families.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Final

FEATURE_CATALOG_VERSION: Final[int] = 4
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
    count_domain: ObservationCountDomain
    minimum_count: int | None
    internal_ohlc: bool = False


class ObservationCountDomain(StrEnum):
    """Domain in which a feature's minimum count is measured."""

    VALID_PRICE_LEVELS = "valid_price_levels"
    VALID_SOURCE_ROWS = "valid_source_rows"
    VALID_DERIVED_INPUTS = "valid_derived_inputs"
    VALID_MARKET_DATES = "valid_market_dates"
    VALID_INSTRUMENT_RETURNS = "valid_instrument_returns"


@dataclass(frozen=True, slots=True)
class SeriesSpecOptions:
    """Optional source and observation-count policy for a generated series."""

    suffix: str = "obs"
    source: tuple[str, ...] = ("adjusted_close_level",)
    count_domain: ObservationCountDomain = ObservationCountDomain.VALID_SOURCE_ROWS
    minimum_count: int | Callable[[int], int] | None = None


def _series_specs(
    prefix: str,
    windows: tuple[int, ...],
    formula: str,
    options: SeriesSpecOptions | None = None,
) -> tuple[FeatureSpec, ...]:
    resolved_options = options or SeriesSpecOptions()

    def count_for(window: int) -> int:
        if resolved_options.minimum_count is None:
            return window
        return (
            resolved_options.minimum_count(window)
            if callable(resolved_options.minimum_count)
            else resolved_options.minimum_count
        )

    return tuple(
        FeatureSpec(
            name=f"{prefix}_{window}{resolved_options.suffix}",
            source_columns=resolved_options.source,
            windows=(window,),
            formula=formula,
            count_domain=resolved_options.count_domain,
            minimum_count=count_for(window),
        )
        for window in windows
    )


FEATURE_SPECS: Final[tuple[FeatureSpec, ...]] = (
    *_series_specs(
        "adjusted_close_log_return",
        (1, 3, 5, 10, 20),
        "ln(adjusted_close_level / lag(adjusted_close_level, n))",
        SeriesSpecOptions(
            count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
            minimum_count=lambda window: window + 1,
        ),
    ),
    *_series_specs(
        "adjusted_close_return_geom",
        (5, 10, 20),
        "exp(sum(adjusted_close_log_return_1obs over n)) - 1",
        SeriesSpecOptions(
            suffix="obs_pct",
            count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
            minimum_count=lambda window: window + 1,
        ),
    ),
    *_series_specs(
        "adjusted_close_return_mean",
        (5, 10, 20),
        "avg(adjusted_close_log_return_1obs over n)",
        SeriesSpecOptions(
            count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
            minimum_count=lambda window: window + 1,
        ),
    ),
    *_series_specs(
        "adjusted_close_volatility",
        (5, 10, 20, 40),
        "stddev_samp(adjusted_close_log_return_1obs over n)",
        SeriesSpecOptions(
            count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
            minimum_count=lambda window: window + 1,
        ),
    ),
    *_series_specs(
        "high_low_range",
        (1, 5),
        "avg((high - low) / close over n)",
        SeriesSpecOptions(
            source=("open", "high", "low", "close"),
            count_domain=ObservationCountDomain.VALID_SOURCE_ROWS,
            minimum_count=lambda window: window,
        ),
    ),
    *_series_specs(
        "intraday_return",
        (1, 5),
        "avg((close - open) / open over n)",
        SeriesSpecOptions(
            source=("open", "close"),
            count_domain=ObservationCountDomain.VALID_SOURCE_ROWS,
            minimum_count=lambda window: window,
        ),
    ),
    *_series_specs(
        "overnight_gap",
        (1, 5),
        "avg((open - lag(close, 1)) / lag(close, 1) over n)",
        SeriesSpecOptions(
            source=("open", "close"),
            count_domain=ObservationCountDomain.VALID_SOURCE_ROWS,
            minimum_count=lambda window: window + 1,
        ),
    ),
    FeatureSpec(
        name="adjusted_close_sma_ratio_5_20",
        source_columns=("adjusted_close_level",),
        windows=("5/20",),
        formula="sma(adjusted_close_level, 5) / sma(adjusted_close_level, 20)",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=20,
    ),
    FeatureSpec(
        name="adjusted_close_sma_ratio_10_20",
        source_columns=("adjusted_close_level",),
        windows=("10/20",),
        formula="sma(adjusted_close_level, 10) / sma(adjusted_close_level, 20)",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=20,
    ),
    FeatureSpec(
        name="adjusted_close_sma_ratio_10_40",
        source_columns=("adjusted_close_level",),
        windows=("10/40",),
        formula="sma(adjusted_close_level, 10) / sma(adjusted_close_level, 40)",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=40,
    ),
    *_series_specs(
        "adjusted_close_rsi",
        (7, 14),
        "Wilder RSI over adjusted_close gains and losses",
        SeriesSpecOptions(
            count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
            minimum_count=lambda window: window + 1,
        ),
    ),
    *_series_specs(
        "adjusted_close_roc",
        (3, 5, 10, 20),
        "adjusted_close_level / lag(adjusted_close_level, n) - 1",
        SeriesSpecOptions(
            count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
            minimum_count=lambda window: window + 1,
        ),
    ),
    *_series_specs(
        "adjusted_close_drawdown",
        (20, 60),
        "adjusted_close_level / rolling_max(adjusted_close_level, n) - 1",
        SeriesSpecOptions(
            count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
            minimum_count=lambda window: window,
        ),
    ),
    *_series_specs(
        "relative_volume",
        (5, 10, 20),
        "volume_level / rolling_mean(volume_level, n)",
        SeriesSpecOptions(source=("volume_level",)),
    ),
    *_series_specs(
        "volume_zscore",
        (5, 10, 20),
        "(volume_level - rolling_mean) / rolling_std",
        SeriesSpecOptions(source=("volume_level",)),
    ),
    FeatureSpec(
        name="breadth_daily",
        source_columns=("adjusted_close_level",),
        windows=("daily",),
        formula="cross-sectional fraction of positive adjusted-close daily returns",
        count_domain=ObservationCountDomain.VALID_INSTRUMENT_RETURNS,
        minimum_count=1,
    ),
    *_series_specs(
        "breadth",
        (5, 10, 20),
        "rolling_mean(breadth_daily, n)",
        SeriesSpecOptions(count_domain=ObservationCountDomain.VALID_MARKET_DATES),
    ),
    FeatureSpec(
        name="dispersion_daily",
        source_columns=("adjusted_close_level",),
        windows=("daily",),
        formula="cross-sectional stddev of adjusted-close daily returns",
        count_domain=ObservationCountDomain.VALID_INSTRUMENT_RETURNS,
        minimum_count=2,
    ),
    *_series_specs(
        "dispersion",
        (5, 10, 20),
        "rolling_mean(dispersion_daily, n)",
        SeriesSpecOptions(count_domain=ObservationCountDomain.VALID_MARKET_DATES),
    ),
    # The compact names below preserve the feature contract recorded in GOALS.md.
    # Existing prefixed columns remain available as backwards-compatible aliases.
    *_series_specs(
        "log_return",
        (1, 2, 3, 4, 5, 8, 10, 20),
        "ln(adjusted_close_level / lag(adjusted_close_level, n))",
        SeriesSpecOptions(
            suffix="",
            count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
            minimum_count=lambda window: window + 1,
        ),
    ),
    *_series_specs(
        "rolling_cum_return",
        (4, 8, 16),
        "exp(sum(log_return_1 over n)) - 1",
        SeriesSpecOptions(
            suffix="",
            count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
            minimum_count=lambda window: window + 1,
        ),
    ),
    *_series_specs(
        "rsi",
        (3, 5, 7, 14),
        "Wilder RSI over adjusted_close gains and losses",
        SeriesSpecOptions(
            suffix="",
            count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
            minimum_count=lambda window: window + 1,
        ),
    ),
    FeatureSpec(
        name="rsi_ratio_5_15",
        source_columns=("adjusted_close_level",),
        windows=("5/15",),
        formula="rsi_5 / rsi_15",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=16,
    ),
    FeatureSpec(
        name="rsi5_slope_3",
        source_columns=("adjusted_close_level",),
        windows=(3,),
        formula="(rsi_5 - lag(rsi_5, 3)) / 3",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=9,
    ),
    FeatureSpec(
        name="rsi5_delta_1",
        source_columns=("adjusted_close_level",),
        windows=(1,),
        formula="rsi_5 - lag(rsi_5, 1)",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=7,
    ),
    FeatureSpec(
        name="ema5_distance",
        source_columns=("adjusted_close_level",),
        windows=(5,),
        formula="adjusted_close_level / SMA-seeded EMA5 - 1",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=5,
    ),
    FeatureSpec(
        name="ema9_distance",
        source_columns=("adjusted_close_level",),
        windows=(9,),
        formula="adjusted_close_level / SMA-seeded EMA9 - 1",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=9,
    ),
    FeatureSpec(
        name="ema21_distance",
        source_columns=("adjusted_close_level",),
        windows=(21,),
        formula="adjusted_close_level / SMA-seeded EMA21 - 1",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=21,
    ),
    FeatureSpec(
        name="sma_ratio_5_15",
        source_columns=("adjusted_close_level",),
        windows=("5/15",),
        formula="sma(adjusted_close_level, 5) / sma(adjusted_close_level, 15)",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=15,
    ),
    FeatureSpec(
        name="ema5_ema21_spread",
        source_columns=("adjusted_close_level",),
        windows=("5/21",),
        formula="(ema_5 - ema_21) / adjusted_close_level",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=21,
    ),
    FeatureSpec(
        name="ema9_slope_3",
        source_columns=("adjusted_close_level",),
        windows=(3,),
        formula="(ema_9 - lag(ema_9, 3)) / (3 * adjusted_close_level)",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=12,
    ),
    FeatureSpec(
        name="ema5_ema9_cross",
        source_columns=("adjusted_close_level",),
        windows=("5/9",),
        formula="1 on upward EMA5/EMA9 cross, -1 on downward cross, else 0",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=10,
    ),
    *_series_specs(
        "roc",
        (1, 2, 4, 8),
        "adjusted_close_level / lag(adjusted_close_level, n) - 1",
        SeriesSpecOptions(
            suffix="",
            count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
            minimum_count=lambda window: window + 1,
        ),
    ),
    FeatureSpec(
        name="roc_acceleration_4_8",
        source_columns=("adjusted_close_level",),
        windows=("4/8",),
        formula="roc_4 - roc_8",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=9,
    ),
    FeatureSpec(
        name="stoch_k_5",
        source_columns=("high", "low", "close"),
        windows=(5,),
        formula=(
            "100 * (close - rolling_min(low, 5)) "
            "/ (rolling_max(high, 5) - rolling_min(low, 5))"
        ),
        count_domain=ObservationCountDomain.VALID_SOURCE_ROWS,
        minimum_count=5,
    ),
    FeatureSpec(
        name="stoch_d_5_3",
        source_columns=("high", "low", "close"),
        windows=("5/3",),
        formula="sma(stoch_k_5, 3)",
        count_domain=ObservationCountDomain.VALID_SOURCE_ROWS,
        minimum_count=7,
    ),
    FeatureSpec(
        name="stoch_k_14",
        source_columns=("high", "low", "close"),
        windows=(14,),
        formula=(
            "100 * (close - rolling_min(low, 14)) "
            "/ (rolling_max(high, 14) - rolling_min(low, 14))"
        ),
        count_domain=ObservationCountDomain.VALID_SOURCE_ROWS,
        minimum_count=14,
    ),
    FeatureSpec(
        name="stoch_d_14_3",
        source_columns=("high", "low", "close"),
        windows=("14/3",),
        formula="sma(stoch_k_14, 3)",
        count_domain=ObservationCountDomain.VALID_SOURCE_ROWS,
        minimum_count=16,
    ),
    FeatureSpec(
        name="stoch_kd_spread_5",
        source_columns=("high", "low", "close"),
        windows=(5,),
        formula="stoch_k_5 - stoch_d_5_3",
        count_domain=ObservationCountDomain.VALID_SOURCE_ROWS,
        minimum_count=7,
    ),
    FeatureSpec(
        name="stoch_k_slope_3",
        source_columns=("high", "low", "close"),
        windows=(3,),
        formula="(stoch_k_5 - lag(stoch_k_5, 3)) / 3",
        count_domain=ObservationCountDomain.VALID_SOURCE_ROWS,
        minimum_count=8,
    ),
    FeatureSpec(
        name="macd_line_relative",
        source_columns=("adjusted_close_level",),
        windows=("12/26",),
        formula="(ema_12 - ema_26) / adjusted_close_level",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=26,
    ),
    FeatureSpec(
        name="macd_hist_relative",
        source_columns=("adjusted_close_level",),
        windows=("12/26/9",),
        formula="(ema_12 - ema_26 - ema_9(macd_line)) / adjusted_close_level",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=34,
    ),
    FeatureSpec(
        name="macd_hist_delta_3",
        source_columns=("adjusted_close_level",),
        windows=(3,),
        formula="macd_hist_relative - lag(macd_hist_relative, 3)",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=37,
    ),
    FeatureSpec(
        name="donchian_breakout_up",
        source_columns=("high",),
        windows=(20,),
        formula="high > prior-20-observation rolling maximum high",
        count_domain=ObservationCountDomain.VALID_SOURCE_ROWS,
        minimum_count=21,
    ),
    FeatureSpec(
        name="donchian_breakout_down",
        source_columns=("low",),
        windows=(20,),
        formula="low < prior-20-observation rolling minimum low",
        count_domain=ObservationCountDomain.VALID_SOURCE_ROWS,
        minimum_count=21,
    ),
    *_series_specs(
        "rolling_volatility",
        (8, 32, 96),
        "sample standard deviation of daily adjusted-close log returns",
        SeriesSpecOptions(
            suffix="",
            count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
            minimum_count=lambda window: window + 1,
        ),
    ),
    FeatureSpec(
        name="volatility_zscore_96",
        source_columns=("adjusted_close_level",),
        windows=(96,),
        formula="zscore(rolling_volatility_8 over trailing 96 valid volatility values)",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=104,
    ),
    FeatureSpec(
        name="level_zscore_96",
        source_columns=("adjusted_close_level",),
        windows=(96,),
        formula="(adjusted_close_level - trailing_mean_96) / trailing_sample_std_96",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=96,
    ),
    FeatureSpec(
        name="momentum_autocorr_lag1_window96",
        source_columns=("adjusted_close_level",),
        windows=("lag=1/window=96",),
        formula="Pearson correlation(log_return_1, lag(log_return_1, 1)) over 96 pairs",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=99,
    ),
    FeatureSpec(
        name="momentum_autocorr_lag4_window96",
        source_columns=("adjusted_close_level",),
        windows=("lag=4/window=96",),
        formula="Pearson correlation(log_return_1, lag(log_return_1, 4)) over 96 pairs",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=102,
    ),
    FeatureSpec(
        name="momentum_autocorr_lag8_window192",
        source_columns=("adjusted_close_level",),
        windows=("lag=8/window=192",),
        formula="Pearson correlation(log_return_1, lag(log_return_1, 8)) over 192 pairs",
        count_domain=ObservationCountDomain.VALID_PRICE_LEVELS,
        minimum_count=202,
    ),
    *_series_specs(
        "rolling_drawdown",
        (32, 96),
        "adjusted_close_level / trailing_max(adjusted_close_level, n) - 1",
        SeriesSpecOptions(suffix="", count_domain=ObservationCountDomain.VALID_PRICE_LEVELS),
    ),
    FeatureSpec(
        name="log_tick_volume",
        source_columns=("volume_level",),
        windows=(1,),
        formula="ln(1 + volume_level)",
        count_domain=ObservationCountDomain.VALID_SOURCE_ROWS,
        minimum_count=1,
    ),
)

INTERNAL_OHLC_FAMILIES: Final[frozenset[str]] = frozenset(
    {"high_low_range", "intraday_return", "overnight_gap", "stoch", "donchian"}
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
        if not isinstance(spec.count_domain, ObservationCountDomain):
            raise ValueError(f"invalid observation count domain: {spec.name}")
        if spec.minimum_count is not None and spec.minimum_count < 1:
            raise ValueError(f"invalid minimum count: {spec.name}")
        if not family:
            raise ValueError("feature name must not be empty")


validate_feature_catalog()
FEATURE_CATALOG_FINGERPRINT: Final[str] = hashlib.sha256(
    json.dumps(feature_catalog_payload(), sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()
