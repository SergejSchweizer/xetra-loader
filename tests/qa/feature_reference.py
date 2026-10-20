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


def make_sparse_fixture(days: int = 65) -> tuple[FixtureQuote, ...]:
    """Create staggered histories with a deliberate missing quote date."""

    complete = make_fixture(days)
    start = date(2026, 1, 2)
    return tuple(
        row
        for row in complete
        if row.code != "BBB"
        or (
            row.trade_date >= start + timedelta(days=3)
            and row.trade_date != start + timedelta(days=20)
        )
    )


def reference_features(  # noqa: C901, PLR0912, PLR0915 -- one independent oracle pass covers the full contract.
    rows: tuple[FixtureQuote, ...],
) -> dict[tuple[str, date], dict[str, float | bool | None]]:
    """Calculate the complete catalog independently of the PostgreSQL SQL."""

    by_code: dict[str, tuple[FixtureQuote, ...]] = {}
    for row in rows:
        by_code[row.code] = tuple(
            sorted((*by_code.get(row.code, ()), row), key=lambda item: item.trade_date)
        )

    output: dict[tuple[str, date], dict[str, float | None]] = {}
    returns_by_code: dict[tuple[str, date], float | None] = {}
    for code, quotes in by_code.items():
        adjusted = [row.adjusted_close for row in quotes]
        volumes = [float(row.volume) for row in quotes]
        log_returns = [_lagged_log_return(adjusted, index, 1) for index in range(len(quotes))]
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
        rsi_values = {
            period: [_wilder_rsi(adjusted, index, period) for index in range(len(quotes))]
            for period in (3, 5, 7, 14, 15)
        }
        ema_values = {
            period: _ema_series(adjusted, period) for period in (5, 9, 12, 21, 26)
        }
        macd_line = [
            None if fast is None or slow is None else fast - slow
            for fast, slow in zip(ema_values[12], ema_values[26], strict=True)
        ]
        macd_signal = _ema_series(macd_line, 9, allow_nonpositive=True)
        macd_hist_relative = [
            None
            if line is None or signal is None or price <= 0
            else (line - signal) / price
            for line, signal, price in zip(macd_line, macd_signal, adjusted, strict=True)
        ]
        stoch_k = {
            (period, index): _stochastic_k(quotes, index, period)
            for period in (5, 14)
            for index in range(len(quotes))
        }
        stoch_k5 = [stoch_k[(5, index)] for index in range(len(quotes))]
        stoch_k14 = [stoch_k[(14, index)] for index in range(len(quotes))]
        rolling_volatility = {
            period: [
                _sample_std(_window(log_returns, index, period))
                for index in range(len(quotes))
            ]
            for period in (8, 32, 96)
        }
        for index, row in enumerate(quotes):
            returns_by_code[(code, row.trade_date)] = log_returns[index]
            features: dict[str, float | bool | None] = {
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

            for window in (1, 2, 3, 4, 5, 8, 10, 20):
                features[f"log_return_{window}"] = _lagged_log_return(adjusted, index, window)
            for window in (4, 8, 16):
                window_returns = _window(log_returns, index, window)
                features[f"rolling_cum_return_{window}"] = (
                    None if window_returns is None else math.exp(sum(window_returns)) - 1
                )
            for period in (3, 5, 7, 14):
                features[f"rsi_{period}"] = rsi_values[period][index]
            rsi5 = rsi_values[5][index]
            rsi15 = rsi_values[15][index]
            rsi5_lag1 = rsi_values[5][index - 1] if index >= 1 else None
            rsi5_lag3 = rsi_values[5][index - 3] if index >= 3 else None
            features["rsi_ratio_5_15"] = (
                None if rsi5 is None or rsi15 in (None, 0.0) else rsi5 / rsi15
            )
            features["rsi5_slope_3"] = (
                None if rsi5 is None or rsi5_lag3 is None else (rsi5 - rsi5_lag3) / 3
            )
            features["rsi5_delta_1"] = (
                None if rsi5 is None or rsi5_lag1 is None else rsi5 - rsi5_lag1
            )

            for period in (5, 9, 21):
                ema = ema_values[period][index]
                features[f"ema{period}_distance"] = (
                    None if ema is None or adjusted[index] <= 0 else adjusted[index] / ema - 1
                )
            sma5 = _mean(_window(adjusted, index, 5))
            sma15 = _mean(_window(adjusted, index, 15))
            features["sma_ratio_5_15"] = (
                None if sma5 is None or sma15 in (None, 0.0) else sma5 / sma15
            )
            ema5 = ema_values[5][index]
            ema9 = ema_values[9][index]
            ema21 = ema_values[21][index]
            features["ema5_ema21_spread"] = (
                None
                if ema5 is None or ema21 is None or adjusted[index] <= 0
                else (ema5 - ema21) / adjusted[index]
            )
            ema9_lag3 = ema_values[9][index - 3] if index >= 3 else None
            features["ema9_slope_3"] = (
                None
                if ema9 is None or ema9_lag3 is None or adjusted[index] <= 0
                else (ema9 - ema9_lag3) / (3 * adjusted[index])
            )
            ema5_lag1 = ema_values[5][index - 1] if index >= 1 else None
            ema9_lag1 = ema_values[9][index - 1] if index >= 1 else None
            features["ema5_ema9_cross"] = (
                None
                if ema5 is None or ema9 is None or ema5_lag1 is None or ema9_lag1 is None
                else (
                    1
                    if ema5 > ema9 and ema5_lag1 <= ema9_lag1
                    else -1
                    if ema5 < ema9 and ema5_lag1 >= ema9_lag1
                    else 0
                )
            )

            for window in (1, 2, 4, 8):
                lagged = _lagged(adjusted, index, window)
                features[f"roc_{window}"] = (
                    None if lagged in (None, 0.0) else adjusted[index] / lagged - 1
                )
            roc4, roc8 = features["roc_4"], features["roc_8"]
            features["roc_acceleration_4_8"] = (
                None if roc4 is None or roc8 is None else roc4 - roc8
            )

            k5, k14 = stoch_k5[index], stoch_k14[index]
            d5 = _mean(_window(stoch_k5, index, 3))
            d14 = _mean(_window(stoch_k14, index, 3))
            features["stoch_k_5"] = k5
            features["stoch_d_5_3"] = d5
            features["stoch_k_14"] = k14
            features["stoch_d_14_3"] = d14
            features["stoch_kd_spread_5"] = (
                None if k5 is None or d5 is None else k5 - d5
            )
            k5_lag3 = stoch_k5[index - 3] if index >= 3 else None
            features["stoch_k_slope_3"] = (
                None if k5 is None or k5_lag3 is None else (k5 - k5_lag3) / 3
            )

            macd_line_value = macd_line[index]
            macd_hist_value = macd_hist_relative[index]
            features["macd_line_relative"] = (
                None
                if macd_line_value is None or adjusted[index] <= 0
                else macd_line_value / adjusted[index]
            )
            features["macd_hist_relative"] = macd_hist_value
            hist_lag3 = macd_hist_relative[index - 3] if index >= 3 else None
            features["macd_hist_delta_3"] = (
                None
                if macd_hist_value is None or hist_lag3 is None
                else macd_hist_value - hist_lag3
            )

            previous_quotes = quotes[max(0, index - 20) : index]
            features["donchian_breakout_up"] = (
                None
                if len(previous_quotes) != 20
                else row.high > max(quote.high for quote in previous_quotes)
            )
            features["donchian_breakout_down"] = (
                None
                if len(previous_quotes) != 20
                else row.low < min(quote.low for quote in previous_quotes)
            )
            for period in (8, 32, 96):
                features[f"rolling_volatility_{period}"] = rolling_volatility[period][index]
            vol8_window = _window(rolling_volatility[8], index, 96)
            vol8_mean = _mean(vol8_window)
            vol8_std = _sample_std(vol8_window)
            features["volatility_zscore_96"] = (
                None
                if vol8_mean is None or vol8_std in (None, 0.0)
                else (rolling_volatility[8][index] - vol8_mean) / vol8_std
            )
            level_window = _window(adjusted, index, 96)
            level_mean = _mean(level_window)
            level_std = _sample_std(level_window)
            features["level_zscore_96"] = (
                None
                if level_mean is None or level_std in (None, 0.0)
                else (adjusted[index] - level_mean) / level_std
            )
            for lag, window in ((1, 96), (4, 96), (8, 192)):
                features[f"momentum_autocorr_lag{lag}_window{window}"] = (
                    _rolling_autocorrelation(log_returns, index, lag, window)
                )
            for window in (32, 96):
                prices = _window(adjusted, index, window)
                features[f"rolling_drawdown_{window}"] = (
                    None
                    if prices is None or max(prices) == 0
                    else adjusted[index] / max(prices) - 1
                )
            features["log_tick_volume"] = math.log1p(volumes[index])
            output[(code, row.trade_date)] = features

    dates = tuple(sorted({row.trade_date for row in rows}))
    daily_breadth: list[float | None] = []
    daily_dispersion: list[float | None] = []
    for _index, trade_date in enumerate(dates):
        daily_returns = [
            returns_by_code[(code, trade_date)]
            for code in sorted(by_code)
            if (code, trade_date) in returns_by_code
            and returns_by_code[(code, trade_date)] is not None
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
            if (code, trade_date) not in output:
                continue
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


def _ema_series(
    values: list[float | None],
    period: int,
    *,
    allow_nonpositive: bool = False,
) -> list[float | None]:
    """Calculate a causal EMA seeded by the first full SMA window."""

    result: list[float | None] = [None] * len(values)
    seed: list[float] = []
    previous: float | None = None
    alpha = 2.0 / (period + 1)
    valid_run = 0
    for index, value in enumerate(values):
        if value is None or (value <= 0 and not allow_nonpositive):
            seed.clear()
            previous = None
            valid_run = 0
            continue
        valid_run += 1
        if valid_run < period:
            seed.append(value)
            continue
        if valid_run == period:
            seed.append(value)
            previous = statistics.fmean(seed)
        else:
            assert previous is not None
            previous = alpha * value + (1 - alpha) * previous
        result[index] = previous
    return result


def _stochastic_k(
    quotes: tuple[FixtureQuote, ...],
    index: int,
    period: int,
) -> float | None:
    if index + 1 < period:
        return None
    window = quotes[index - period + 1 : index + 1]
    highest = max(quote.high for quote in window)
    lowest = min(quote.low for quote in window)
    if highest <= lowest:
        return None
    return 100 * (quotes[index].close - lowest) / (highest - lowest)


def _rolling_autocorrelation(
    values: list[float | None],
    index: int,
    lag: int,
    window: int,
) -> float | None:
    if index + 1 < window:
        return None
    positions = range(index - window + 1, index + 1)
    pairs = [
        (values[position], values[position - lag])
        for position in positions
        if position >= lag
    ]
    if len(pairs) != window or any(left is None or right is None for left, right in pairs):
        return None
    left_values = [float(left) for left, _right in pairs]
    right_values = [float(right) for _left, right in pairs]
    try:
        return statistics.correlation(left_values, right_values)
    except statistics.StatisticsError:
        return None


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
