from __future__ import annotations

from dataclasses import replace
from math import exp, isclose, log

from tests.qa.feature_reference import make_fixture, reference_features
from xetra_loader.features.catalog import feature_columns


def test_goal_feature_families_and_warmup_windows() -> None:
    rows = make_fixture(days=220)
    features = reference_features(rows)
    target = rows[210]
    selected = features[(target.code, target.trade_date)]
    assert set(selected) == set(feature_columns())
    series = [row for row in rows if row.code == target.code]
    index = next(i for i, row in enumerate(series) if row.trade_date == target.trade_date)

    assert isclose(
        float(selected["log_return_2"]),
        log(series[index].adjusted_close / series[index - 2].adjusted_close),
    )
    expected_return_4 = log(series[index].adjusted_close / series[index - 4].adjusted_close)
    assert isclose(float(selected["rolling_cum_return_4"]), exp(expected_return_4) - 1)
    assert 0 <= float(selected["rsi_3"]) <= 100
    assert selected["ema5_distance"] is not None
    assert selected["sma_ratio_5_15"] is not None
    assert selected["stoch_k_5"] is not None
    assert selected["stoch_d_14_3"] is not None
    assert selected["macd_line_relative"] is not None
    assert selected["macd_hist_relative"] is not None
    assert isinstance(selected["donchian_breakout_up"], bool)
    assert selected["rolling_volatility_96"] is not None
    assert selected["volatility_zscore_96"] is not None
    assert selected["level_zscore_96"] is not None
    assert selected["momentum_autocorr_lag1_window96"] is not None
    assert selected["momentum_autocorr_lag4_window96"] is not None
    assert selected["momentum_autocorr_lag8_window192"] is not None
    assert selected["rolling_drawdown_96"] is not None
    assert selected["log_tick_volume"] is not None

    first = features[("AAA", series[21].trade_date)]
    assert first["donchian_breakout_up"] is True
    assert features[("AAA", series[19].trade_date)]["donchian_breakout_up"] is None


def test_goal_features_are_causal_when_future_quote_changes() -> None:
    rows = make_fixture(days=220)
    before = reference_features(rows)
    last = rows[-1]
    changed_last = replace(
        last,
        adjusted_close=last.adjusted_close * 1.4,
        high=last.high * 1.4,
        close=last.close * 1.4,
    )
    after = reference_features((*rows[:-1], changed_last))
    target_date = rows[210].trade_date

    assert before[("AAA", target_date)] == after[("AAA", target_date)]
    assert before[("BBB", target_date)] == after[("BBB", target_date)]
