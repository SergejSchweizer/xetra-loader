from xetra_loader.features.catalog import (
    BASE_LEVEL_COLUMNS,
    FEATURE_CATALOG_FINGERPRINT,
    FEATURE_CATALOG_VERSION,
    FEATURE_SPECS,
    FORBIDDEN_LEVEL_COLUMNS,
    ObservationCountDomain,
    feature_catalog_payload,
    feature_columns,
    validate_feature_catalog,
)


def test_catalog_has_only_adjusted_close_and_volume_base_levels() -> None:
    assert BASE_LEVEL_COLUMNS == ("adjusted_close_level", "volume_level")
    assert not set(FORBIDDEN_LEVEL_COLUMNS).intersection(feature_columns())


def test_catalog_contains_requested_windows() -> None:
    names = set(feature_columns())
    expected_fragments = (
        "adjusted_close_log_return_1obs",
        "adjusted_close_log_return_20obs",
        "adjusted_close_return_geom_5obs_pct",
        "adjusted_close_return_mean_20obs",
        "adjusted_close_volatility_40obs",
        "high_low_range_5obs",
        "intraday_return_1obs",
        "overnight_gap_5obs",
        "adjusted_close_sma_ratio_10_40",
        "adjusted_close_rsi_14obs",
        "adjusted_close_roc_20obs",
        "adjusted_close_drawdown_60obs",
        "relative_volume_20obs",
        "volume_zscore_5obs",
        "breadth_daily",
        "breadth_20obs",
        "dispersion_daily",
        "dispersion_10obs",
        "log_return_2",
        "log_return_4",
        "log_return_8",
        "rolling_cum_return_16",
        "rsi_3",
        "rsi_ratio_5_15",
        "ema5_distance",
        "ema5_ema21_spread",
        "ema5_ema9_cross",
        "roc_acceleration_4_8",
        "stoch_d_14_3",
        "macd_hist_delta_3",
        "donchian_breakout_up",
        "rolling_volatility_96",
        "volatility_zscore_96",
        "level_zscore_96",
        "momentum_autocorr_lag8_window192",
        "rolling_drawdown_96",
        "log_tick_volume",
    )
    assert set(expected_fragments) <= names


def test_only_approved_feature_families_may_use_internal_ohlc() -> None:
    allowed = {
        "high_low_range",
        "intraday_return",
        "overnight_gap",
        "stoch",
        "donchian",
    }
    for spec in FEATURE_SPECS:
        if set(spec.source_columns) & {"open", "high", "low", "close"}:
            assert any(spec.name.startswith(prefix) for prefix in allowed)


def test_catalog_fingerprint_is_stable_and_train_fold_fit_is_not_global() -> None:
    validate_feature_catalog()
    assert FEATURE_CATALOG_VERSION == 4
    assert len(FEATURE_CATALOG_FINGERPRINT) == 64
    assert feature_catalog_payload()["train_fold_standardization"] == {
        "scope": "downstream_fold_aware_operation",
        "fit_rows": "train_only",
        "global_fit": False,
    }


def test_catalog_declares_unambiguous_window_count_domains() -> None:
    specs = {spec.name: spec for spec in FEATURE_SPECS}
    assert (
        specs["adjusted_close_log_return_5obs"].count_domain
        is ObservationCountDomain.VALID_PRICE_LEVELS
    )
    assert specs["adjusted_close_log_return_5obs"].minimum_count == 6
    assert specs["adjusted_close_return_mean_5obs"].minimum_count == 6
    assert specs["high_low_range_5obs"].minimum_count == 5
    assert specs["intraday_return_5obs"].minimum_count == 5
    assert specs["overnight_gap_5obs"].minimum_count == 6
    assert specs["adjusted_close_rsi_14obs"].minimum_count == 15
    assert specs["momentum_autocorr_lag1_window96"].minimum_count == 99
    assert specs["momentum_autocorr_lag4_window96"].minimum_count == 102
    assert specs["momentum_autocorr_lag8_window192"].minimum_count == 202
    assert specs["volatility_zscore_96"].minimum_count == 104
    assert specs["donchian_breakout_up"].minimum_count == 21
    assert specs["breadth_daily"].count_domain is ObservationCountDomain.VALID_INSTRUMENT_RETURNS
    assert specs["dispersion_daily"].minimum_count == 2
    assert specs["breadth_20obs"].count_domain is ObservationCountDomain.VALID_MARKET_DATES
