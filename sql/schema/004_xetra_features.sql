BEGIN;
SET LOCAL TIME ZONE 'UTC';

DROP MATERIALIZED VIEW IF EXISTS xetra_loader.xetra_features;

CREATE MATERIALIZED VIEW xetra_loader.xetra_features AS
WITH RECURSIVE ordered AS (
    SELECT
        q.isin, q.exchange, q.code, q.trade_date,
        q.adjusted_close::double precision AS adjusted_close_level,
        q.volume::double precision AS volume_level,
        q.open::double precision AS open_value,
        q.high::double precision AS high_value,
        q.low::double precision AS low_value,
        q.close::double precision AS close_value,
        lag(q.adjusted_close::double precision, 1) OVER w AS adjusted_close_lag_1,
        lag(q.adjusted_close::double precision, 3) OVER w AS adjusted_close_lag_3,
        lag(q.adjusted_close::double precision, 5) OVER w AS adjusted_close_lag_5,
        lag(q.adjusted_close::double precision, 10) OVER w AS adjusted_close_lag_10,
        lag(q.adjusted_close::double precision, 20) OVER w AS adjusted_close_lag_20,
        lag(q.close::double precision, 1) OVER w AS close_lag_1,
        row_number() OVER w AS observation_number
    FROM xetra_loader.eod_quotes AS q
    WINDOW w AS (PARTITION BY q.isin, q.exchange, q.code ORDER BY q.trade_date)
), returns AS (
    SELECT o.*,
        CASE WHEN adjusted_close_level > 0 AND adjusted_close_lag_1 > 0
             THEN ln(adjusted_close_level / adjusted_close_lag_1) END AS log_return_1,
        CASE WHEN adjusted_close_level > 0 AND adjusted_close_lag_3 > 0
             THEN ln(adjusted_close_level / adjusted_close_lag_3) END AS log_return_3,
        CASE WHEN adjusted_close_level > 0 AND adjusted_close_lag_5 > 0
             THEN ln(adjusted_close_level / adjusted_close_lag_5) END AS log_return_5,
        CASE WHEN adjusted_close_level > 0 AND adjusted_close_lag_10 > 0
             THEN ln(adjusted_close_level / adjusted_close_lag_10) END AS log_return_10,
        CASE WHEN adjusted_close_level > 0 AND adjusted_close_lag_20 > 0
             THEN ln(adjusted_close_level / adjusted_close_lag_20) END AS log_return_20,
        CASE WHEN close_value > 0 THEN (high_value - low_value) / close_value END AS range_1,
        CASE WHEN open_value > 0 THEN (close_value - open_value) / open_value END AS intraday_1,
        CASE WHEN close_lag_1 > 0 THEN (open_value - close_lag_1) / close_lag_1 END AS gap_1,
        CASE WHEN adjusted_close_level > 0 AND adjusted_close_lag_1 > 0
             THEN greatest(adjusted_close_level - adjusted_close_lag_1, 0.0) END AS gain,
        CASE WHEN adjusted_close_level > 0 AND adjusted_close_lag_1 > 0
             THEN greatest(adjusted_close_lag_1 - adjusted_close_level, 0.0) END AS loss
    FROM ordered AS o
), rsi_seed_7 AS (
    SELECT r.*,
        avg(gain) OVER w AS avg_gain_7,
        avg(loss) OVER w AS avg_loss_7
    FROM returns AS r
    WINDOW w AS (
        PARTITION BY r.isin, r.exchange, r.code ORDER BY r.observation_number
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )
), rsi_7 AS (
    SELECT isin, exchange, code, trade_date, observation_number, avg_gain_7, avg_loss_7
    FROM rsi_seed_7
    WHERE observation_number = 8
    UNION ALL
    SELECT n.isin, n.exchange, n.code, n.trade_date, n.observation_number,
        (p.avg_gain_7 * 6.0 + n.gain) / 7.0,
        (p.avg_loss_7 * 6.0 + n.loss) / 7.0
    FROM rsi_7 AS p
    JOIN returns AS n ON n.isin = p.isin AND n.exchange = p.exchange AND n.code = p.code
        AND n.observation_number = p.observation_number + 1
), rsi_seed_14 AS (
    SELECT r.*,
        avg(gain) OVER w AS avg_gain_14,
        avg(loss) OVER w AS avg_loss_14
    FROM returns AS r
    WINDOW w AS (
        PARTITION BY r.isin, r.exchange, r.code ORDER BY r.observation_number
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
), rsi_14 AS (
    SELECT isin, exchange, code, trade_date, observation_number, avg_gain_14, avg_loss_14
    FROM rsi_seed_14
    WHERE observation_number = 15
    UNION ALL
    SELECT n.isin, n.exchange, n.code, n.trade_date, n.observation_number,
        (p.avg_gain_14 * 13.0 + n.gain) / 14.0,
        (p.avg_loss_14 * 13.0 + n.loss) / 14.0
    FROM rsi_14 AS p
    JOIN returns AS n ON n.isin = p.isin AND n.exchange = p.exchange AND n.code = p.code
        AND n.observation_number = p.observation_number + 1
), rolling AS (
    SELECT r.*,
        CASE WHEN count(log_return_1) OVER w5 = 5 THEN exp(sum(log_return_1) OVER w5) - 1 END AS ret_5,
        CASE WHEN count(log_return_1) OVER w10 = 10 THEN exp(sum(log_return_1) OVER w10) - 1 END AS ret_10,
        CASE WHEN count(log_return_1) OVER w20 = 20 THEN exp(sum(log_return_1) OVER w20) - 1 END AS ret_20,
        CASE WHEN count(log_return_1) OVER w5 = 5 THEN avg(log_return_1) OVER w5 END AS mean_5,
        CASE WHEN count(log_return_1) OVER w10 = 10 THEN avg(log_return_1) OVER w10 END AS mean_10,
        CASE WHEN count(log_return_1) OVER w20 = 20 THEN avg(log_return_1) OVER w20 END AS mean_20,
        CASE WHEN count(log_return_1) OVER w5 = 5 THEN stddev_samp(log_return_1) OVER w5 END AS vol_5,
        CASE WHEN count(log_return_1) OVER w10 = 10 THEN stddev_samp(log_return_1) OVER w10 END AS vol_10,
        CASE WHEN count(log_return_1) OVER w20 = 20 THEN stddev_samp(log_return_1) OVER w20 END AS vol_20,
        CASE WHEN count(log_return_1) OVER w40 = 40 THEN stddev_samp(log_return_1) OVER w40 END AS vol_40,
        range_1 AS range_1obs,
        CASE WHEN count(range_1) OVER w5 = 5 THEN avg(range_1) OVER w5 END AS range_5obs,
        intraday_1 AS intraday_1obs,
        CASE WHEN count(intraday_1) OVER w5 = 5 THEN avg(intraday_1) OVER w5 END AS intraday_5obs,
        gap_1 AS gap_1obs,
        CASE WHEN count(gap_1) OVER w5 = 5 THEN avg(gap_1) OVER w5 END AS gap_5obs,
        CASE WHEN count(adjusted_close_level) OVER w20 = 20
             THEN avg(adjusted_close_level) OVER w5 / nullif(avg(adjusted_close_level) OVER w20, 0) END AS sma_5_20,
        CASE WHEN count(adjusted_close_level) OVER w20 = 20
             THEN avg(adjusted_close_level) OVER w10 / nullif(avg(adjusted_close_level) OVER w20, 0) END AS sma_10_20,
        CASE WHEN count(adjusted_close_level) OVER w40 = 40
             THEN avg(adjusted_close_level) OVER w10 / nullif(avg(adjusted_close_level) OVER w40, 0) END AS sma_10_40,
        CASE WHEN count(adjusted_close_level) OVER w20 = 20
             THEN adjusted_close_level / nullif(max(adjusted_close_level) OVER w20, 0) - 1 END AS drawdown_20,
        CASE WHEN count(adjusted_close_level) OVER w60 = 60
             THEN adjusted_close_level / nullif(max(adjusted_close_level) OVER w60, 0) - 1 END AS drawdown_60,
        CASE WHEN count(volume_level) OVER w5 = 5
             THEN volume_level / nullif(avg(volume_level) OVER w5, 0) END AS rel_volume_5,
        CASE WHEN count(volume_level) OVER w10 = 10
             THEN volume_level / nullif(avg(volume_level) OVER w10, 0) END AS rel_volume_10,
        CASE WHEN count(volume_level) OVER w20 = 20
             THEN volume_level / nullif(avg(volume_level) OVER w20, 0) END AS rel_volume_20,
        CASE WHEN count(volume_level) OVER w5 = 5
             THEN (volume_level - avg(volume_level) OVER w5) / nullif(stddev_samp(volume_level) OVER w5, 0) END AS volume_z_5,
        CASE WHEN count(volume_level) OVER w10 = 10
             THEN (volume_level - avg(volume_level) OVER w10) / nullif(stddev_samp(volume_level) OVER w10, 0) END AS volume_z_10,
        CASE WHEN count(volume_level) OVER w20 = 20
             THEN (volume_level - avg(volume_level) OVER w20) / nullif(stddev_samp(volume_level) OVER w20, 0) END AS volume_z_20
    FROM returns AS r
    WINDOW
        w5 AS (PARTITION BY isin, exchange, code ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
        w10 AS (PARTITION BY isin, exchange, code ORDER BY trade_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
        w20 AS (PARTITION BY isin, exchange, code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
        w40 AS (PARTITION BY isin, exchange, code ORDER BY trade_date ROWS BETWEEN 39 PRECEDING AND CURRENT ROW),
        w60 AS (PARTITION BY isin, exchange, code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
), daily_cross AS (
    SELECT trade_date,
        avg(CASE WHEN log_return_1 > 0 THEN 1.0 WHEN log_return_1 IS NOT NULL THEN 0.0 END) AS breadth_daily,
        stddev_samp(log_return_1) AS dispersion_daily
    FROM rolling
    GROUP BY trade_date
), cross_rolling AS (
    SELECT d.*,
        avg(breadth_daily) OVER w5 AS breadth_5,
        avg(breadth_daily) OVER w10 AS breadth_10,
        avg(breadth_daily) OVER w20 AS breadth_20,
        avg(dispersion_daily) OVER w5 AS dispersion_5,
        avg(dispersion_daily) OVER w10 AS dispersion_10,
        avg(dispersion_daily) OVER w20 AS dispersion_20
    FROM daily_cross AS d
    WINDOW
        w5 AS (ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
        w10 AS (ORDER BY trade_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
        w20 AS (ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
), pair_rows AS (
    SELECT a.trade_date, a.isin AS isin_a, a.exchange AS exchange_a, a.code AS code_a,
        b.isin AS isin_b, b.exchange AS exchange_b, b.code AS code_b,
        a.log_return_1 AS return_a, b.log_return_1 AS return_b
    FROM rolling AS a
    JOIN rolling AS b ON a.trade_date = b.trade_date
        AND (a.isin, a.exchange, a.code) < (b.isin, b.exchange, b.code)
    WHERE a.log_return_1 IS NOT NULL AND b.log_return_1 IS NOT NULL
), pair_corr AS (
    SELECT p.*,
        corr(return_a, return_b) OVER w5 AS corr_5,
        corr(return_a, return_b) OVER w10 AS corr_10,
        corr(return_a, return_b) OVER w20 AS corr_20
    FROM pair_rows AS p
    WINDOW
        w5 AS (PARTITION BY isin_a, exchange_a, code_a, isin_b, exchange_b, code_b ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
        w10 AS (PARTITION BY isin_a, exchange_a, code_a, isin_b, exchange_b, code_b ORDER BY trade_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
        w20 AS (PARTITION BY isin_a, exchange_a, code_a, isin_b, exchange_b, code_b ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
), average_corr AS (
    SELECT trade_date,
        avg(corr_5) FILTER (WHERE corr_5 IS NOT NULL) AS average_correlation_5,
        avg(corr_10) FILTER (WHERE corr_10 IS NOT NULL) AS average_correlation_10,
        avg(corr_20) FILTER (WHERE corr_20 IS NOT NULL) AS average_correlation_20
    FROM pair_corr
    GROUP BY trade_date
)
SELECT
    r.isin, r.exchange, r.code, r.trade_date,
    r.adjusted_close_level, r.volume_level,
    r.log_return_1 AS adjusted_close_log_return_1obs,
    r.log_return_3 AS adjusted_close_log_return_3obs,
    r.log_return_5 AS adjusted_close_log_return_5obs,
    r.log_return_10 AS adjusted_close_log_return_10obs,
    r.log_return_20 AS adjusted_close_log_return_20obs,
    r.ret_5 AS adjusted_close_return_geom_5obs_pct,
    r.ret_10 AS adjusted_close_return_geom_10obs_pct,
    r.ret_20 AS adjusted_close_return_geom_20obs_pct,
    r.mean_5 AS adjusted_close_return_mean_5obs,
    r.mean_10 AS adjusted_close_return_mean_10obs,
    r.mean_20 AS adjusted_close_return_mean_20obs,
    r.vol_5 AS adjusted_close_volatility_5obs,
    r.vol_10 AS adjusted_close_volatility_10obs,
    r.vol_20 AS adjusted_close_volatility_20obs,
    r.vol_40 AS adjusted_close_volatility_40obs,
    r.range_1obs AS high_low_range_1obs,
    r.range_5obs AS high_low_range_5obs,
    r.intraday_1obs AS intraday_return_1obs,
    r.intraday_5obs AS intraday_return_5obs,
    r.gap_1obs AS overnight_gap_1obs,
    r.gap_5obs AS overnight_gap_5obs,
    r.sma_5_20 AS adjusted_close_sma_ratio_5_20,
    r.sma_10_20 AS adjusted_close_sma_ratio_10_20,
    r.sma_10_40 AS adjusted_close_sma_ratio_10_40,
    CASE WHEN r7.avg_loss_7 = 0 THEN 100.0
         WHEN r7.avg_gain_7 IS NULL OR r7.avg_loss_7 IS NULL THEN NULL
         ELSE 100.0 - 100.0 / (1.0 + r7.avg_gain_7 / r7.avg_loss_7) END AS adjusted_close_rsi_7obs,
    CASE WHEN r14.avg_loss_14 = 0 THEN 100.0
         WHEN r14.avg_gain_14 IS NULL OR r14.avg_loss_14 IS NULL THEN NULL
         ELSE 100.0 - 100.0 / (1.0 + r14.avg_gain_14 / r14.avg_loss_14) END AS adjusted_close_rsi_14obs,
    CASE WHEN r.adjusted_close_level > 0 AND r.adjusted_close_lag_3 > 0 THEN r.adjusted_close_level / r.adjusted_close_lag_3 - 1 END AS adjusted_close_roc_3obs,
    CASE WHEN r.adjusted_close_level > 0 AND r.adjusted_close_lag_5 > 0 THEN r.adjusted_close_level / r.adjusted_close_lag_5 - 1 END AS adjusted_close_roc_5obs,
    CASE WHEN r.adjusted_close_level > 0 AND r.adjusted_close_lag_10 > 0 THEN r.adjusted_close_level / r.adjusted_close_lag_10 - 1 END AS adjusted_close_roc_10obs,
    CASE WHEN r.adjusted_close_level > 0 AND r.adjusted_close_lag_20 > 0 THEN r.adjusted_close_level / r.adjusted_close_lag_20 - 1 END AS adjusted_close_roc_20obs,
    r.drawdown_20 AS adjusted_close_drawdown_20obs,
    r.drawdown_60 AS adjusted_close_drawdown_60obs,
    r.rel_volume_5 AS relative_volume_5obs,
    r.rel_volume_10 AS relative_volume_10obs,
    r.rel_volume_20 AS relative_volume_20obs,
    r.volume_z_5 AS volume_zscore_5obs,
    r.volume_z_10 AS volume_zscore_10obs,
    r.volume_z_20 AS volume_zscore_20obs,
    c.breadth_daily, c.breadth_5 AS breadth_5obs, c.breadth_10 AS breadth_10obs,
    c.breadth_20 AS breadth_20obs,
    c.dispersion_daily, c.dispersion_5 AS dispersion_5obs,
    c.dispersion_10 AS dispersion_10obs, c.dispersion_20 AS dispersion_20obs,
    a.average_correlation_5 AS average_correlation_5obs,
    a.average_correlation_10 AS average_correlation_10obs,
    a.average_correlation_20 AS average_correlation_20obs
FROM rolling AS r
LEFT JOIN rsi_7 AS r7 ON r7.isin = r.isin AND r7.exchange = r.exchange AND r7.code = r.code AND r7.trade_date = r.trade_date
LEFT JOIN rsi_14 AS r14 ON r14.isin = r.isin AND r14.exchange = r.exchange AND r14.code = r.code AND r14.trade_date = r.trade_date
JOIN cross_rolling AS c ON c.trade_date = r.trade_date
LEFT JOIN average_corr AS a ON a.trade_date = r.trade_date;

CREATE UNIQUE INDEX xetra_features_identity_idx
    ON xetra_loader.xetra_features (isin, exchange, code, trade_date);

COMMENT ON MATERIALIZED VIEW xetra_loader.xetra_features IS
    'XETRA feature view; catalog version=1; base levels=adjusted_close_level,volume_level';

REVOKE ALL ON xetra_loader.xetra_features FROM PUBLIC;
GRANT SELECT ON xetra_loader.xetra_features TO "xetra-data-loader", portfell_app;

CREATE OR REPLACE FUNCTION xetra_loader.refresh_xetra_features()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, xetra_loader
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW xetra_loader.xetra_features;
END
$$;

ALTER FUNCTION xetra_loader.refresh_xetra_features() OWNER TO "xetra-data-loader";
REVOKE ALL ON FUNCTION xetra_loader.refresh_xetra_features() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION xetra_loader.refresh_xetra_features() TO "xetra-data-loader";

COMMIT;
