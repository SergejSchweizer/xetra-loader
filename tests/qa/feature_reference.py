"""Independent, dependency-free reference implementation for feature QA."""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True, slots=True)
class FixtureQuote:
    isin: str
    code: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    adjusted_close: float
    volume: int


def make_fixture(days: int = 65) -> tuple[FixtureQuote, ...]:
    """Create two independent positive-price instruments with varied OHLCV data."""

    start = date(2026, 1, 2)
    rows: list[FixtureQuote] = []
    for code, isin, base, slope, volume_base in (
        ("AAA", "DE0000000001", 100.0, 0.55, 1000),
        ("BBB", "DE0000000002", 80.0, 0.73, 1400),
    ):
        for index in range(days):
            adjusted = base + slope * index + (index % 5) * 0.17
            open_value = adjusted - 1.2 + (index % 3) * 0.11
            close_value = adjusted + 0.8 - (index % 4) * 0.09
            high_value = max(open_value, close_value) + 1.1
            low_value = min(open_value, close_value) - 0.9
            rows.append(
                FixtureQuote(
                    isin=isin,
                    code=code,
                    trade_date=start + timedelta(days=index),
                    open=open_value,
                    high=high_value,
                    low=low_value,
                    close=close_value,
                    adjusted_close=adjusted,
                    volume=volume_base + index * 13 + (index % 4) * 7,
                )
            )
    return tuple(rows)


def reference_features(
    rows: tuple[FixtureQuote, ...],
) -> dict[tuple[str, date], dict[str, float | None]]:
    """Calculate the complete catalog independently of the PostgreSQL SQL."""

    by_code: dict[str, tuple[FixtureQuote, ...]] = {}
    for row in rows:
        by_code[row.code] = tuple(
            sorted((*by_code.get(row.code, ()), row), key=lambda item: item.trade_date)
        )

    output: dict[tuple[str, date], dict[str, float | None]] = {}
    returns_by_code: dict[str, list[float | None]] = {}
    for code, quotes in by_code.items():
        adjusted = [row.adjusted_close for row in quotes]
        volumes = [float(row.volume) for row in quotes]
        log_returns = [_lagged_log_return(adjusted, index, 1) for index in range(len(quotes))]
        returns_by_code[code] = log_returns
        for index, row in enumerate(quotes):
            features: dict[str, float | None] = {
                "adjusted_close_level": row.adjusted_close,
                "volume_level": float(row.volume),
            }
            for window in (1, 3, 5, 10, 20):
                features[f"adjusted_close_log_return_{window}obs"] = _lagged_log_return(
                    adjusted, index, window
                )
            for window in (5, 10, 20):
                window_returns = _window(log_returns, index, window)
                features[f"adjusted_close_return_geom_{window}obs_pct"] = (
                    None if window_returns is None else math.exp(sum(window_returns)) - 1
                )
                features[f"adjusted_close_return_mean_{window}obs"] = _mean(window_returns)
            for window in (5, 10, 20, 40):
                features[f"adjusted_close_volatility_{window}obs"] = _sample_std(
                    _window(log_returns, index, window)
                )
            range_values = [
                (quote.high - quote.low) / quote.close if quote.close > 0 else None
                for quote in quotes
            ]
            intraday_values = [
                (quote.close - quote.open) / quote.open if quote.open > 0 else None
                for quote in quotes
            ]
            gap_values = [
                None
                if position == 0 or quotes[position - 1].close <= 0
                else (quote.open - quotes[position - 1].close) / quotes[position - 1].close
                for position, quote in enumerate(quotes)
            ]
            for window in (1, 5):
                features[f"high_low_range_{window}obs"] = _mean(
                    _window(range_values, index, window)
                )
                features[f"intraday_return_{window}obs"] = _mean(
                    _window(intraday_values, index, window)
                )
                features[f"overnight_gap_{window}obs"] = _mean(
                    _window(gap_values, index, window)
                )
            for numerator, denominator in ((5, 20), (10, 20), (10, 40)):
                denominator_values = _window(adjusted, index, denominator)
                numerator_values = _window(adjusted, index, numerator)
                denominator_mean = _mean(denominator_values)
                numerator_mean = _mean(numerator_values)
                features[f"adjusted_close_sma_ratio_{numerator}_{denominator}"] = (
                    None
                    if denominator_mean in (None, 0.0) or numerator_mean is None
                    else numerator_mean / denominator_mean
                )
            for window in (7, 14):
                features[f"adjusted_close_rsi_{window}obs"] = _wilder_rsi(
                    adjusted, index, window
                )
            for window in (3, 5, 10, 20):
                lagged = _lagged(adjusted, index, window)
                features[f"adjusted_close_roc_{window}obs"] = (
                    None if lagged in (None, 0.0) else adjusted[index] / lagged - 1
                )
            for window in (20, 60):
                window_prices = _window(adjusted, index, window)
                current = adjusted[index]
                features[f"adjusted_close_drawdown_{window}obs"] = (
                    None
                    if window_prices is None or max(window_prices) == 0
                    else current / max(window_prices) - 1
                )
            for window in (5, 10, 20):
                window_volume = _window(volumes, index, window)
                mean_volume = _mean(window_volume)
                std_volume = _sample_std(window_volume)
                features[f"relative_volume_{window}obs"] = (
                    None
                    if mean_volume in (None, 0.0)
                    else volumes[index] / mean_volume
                )
                features[f"volume_zscore_{window}obs"] = (
                    None
                    if mean_volume is None or std_volume in (None, 0.0)
                    else (volumes[index] - mean_volume) / std_volume
                )
            output[(code, row.trade_date)] = features

    dates = tuple(sorted({row.trade_date for row in rows}))
    daily_breadth: list[float | None] = []
    daily_dispersion: list[float | None] = []
    for index, _ in enumerate(dates):
        daily_returns = [
            returns_by_code[code][index]
            for code in sorted(by_code)
            if returns_by_code[code][index] is not None
        ]
        daily_breadth.append(
            None
            if not daily_returns
            else sum(value > 0 for value in daily_returns) / len(daily_returns)
        )
        daily_dispersion.append(_sample_std(daily_returns))
    for index, trade_date in enumerate(dates):
        breadth_values = {
            window: _mean(_window(daily_breadth, index, window)) for window in (5, 10, 20)
        }
        dispersion_values = {
            window: _mean(_window(daily_dispersion, index, window)) for window in (5, 10, 20)
        }
        for code in by_code:
            features = output[(code, trade_date)]
            features["breadth_daily"] = daily_breadth[index]
            features["dispersion_daily"] = daily_dispersion[index]
            for window in (5, 10, 20):
                features[f"breadth_{window}obs"] = breadth_values[window]
                features[f"dispersion_{window}obs"] = dispersion_values[window]
    return output


def _lagged(values: list[float], index: int, window: int) -> float | None:
    return values[index - window] if index >= window else None


def _lagged_log_return(values: list[float], index: int, window: int) -> float | None:
    lagged = _lagged(values, index, window)
    return (
        None
        if lagged is None or values[index] <= 0 or lagged <= 0
        else math.log(values[index] / lagged)
    )


def _window(values: list[float | None], index: int, size: int) -> list[float] | None:
    if index + 1 < size:
        return None
    selected = values[index - size + 1 : index + 1]
    return (
        None
        if any(value is None for value in selected)
        else [float(value) for value in selected]
    )


def _mean(values: list[float] | None) -> float | None:
    return None if values is None else statistics.fmean(values)


def _sample_std(values: list[float] | None) -> float | None:
    return None if values is None or len(values) < 2 else statistics.stdev(values)


def _wilder_rsi(values: list[float | None], index: int, period: int) -> float | None:
    """Calculate causal Wilder RSI after resetting on invalid transitions."""

    if index < 1 or period < 1:
        return None
    valid_changes: list[tuple[float, float]] = []
    for position in range(1, index + 1):
        current = values[position]
        previous = values[position - 1]
        if (
            current is None
            or previous is None
            or current <= 0
            or previous <= 0
        ):
            valid_changes.clear()
            continue
        valid_changes.append(
            (
                max(current - previous, 0.0),
                max(previous - current, 0.0),
            )
        )
    if len(valid_changes) < period:
        return None

    average_gain = statistics.fmean(gain for gain, _ in valid_changes[:period])
    average_loss = statistics.fmean(loss for _, loss in valid_changes[:period])
    for gain, loss in valid_changes[period:]:
        average_gain = (average_gain * (period - 1) + gain) / period
        average_loss = (average_loss * (period - 1) + loss) / period
    if average_gain == 0 and average_loss == 0:
        return None
    if average_loss == 0:
        return 100.0
    if average_gain == 0:
        return 0.0
    return 100.0 - 100.0 / (1.0 + average_gain / average_loss)
