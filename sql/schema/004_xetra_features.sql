BEGIN;
SET LOCAL TIME ZONE 'UTC';

CREATE OR REPLACE FUNCTION xetra_loader.resolve_feature_work_mem()
RETURNS text
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, xetra_loader
AS $$
DECLARE
    configured text := current_setting('xetra_loader.feature_work_mem', true);
    size_bytes bigint;
BEGIN
    configured := coalesce(nullif(btrim(configured), ''), '256MB');
    BEGIN
        size_bytes := pg_size_bytes(configured);
    EXCEPTION WHEN others THEN
        RAISE EXCEPTION 'invalid xetra_loader.feature_work_mem setting';
    END;
    IF size_bytes < 16::bigint * 1024 * 1024
       OR size_bytes > 4::bigint * 1024 * 1024 * 1024 THEN
        RAISE EXCEPTION
            'xetra_loader.feature_work_mem must be between 16MB and 4GB';
    END IF;
    RETURN configured;
END
$$;

-- work_mem is a per-operation/per-worker budget. The deployment/runtime sets
-- the custom GUC; the helper validates it and supplies the documented default.
SELECT set_config(
    'work_mem', xetra_loader.resolve_feature_work_mem(), true
);

CREATE OR REPLACE FUNCTION xetra_loader.wilder_rsi(
    gains double precision[],
    losses double precision[],
    period integer
)
RETURNS double precision[]
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    observations integer := coalesce(array_length(gains, 1), 0);
    result double precision[];
    average_gain double precision := 0.0;
    average_loss double precision := 0.0;
    valid_run integer := 0;
    position integer;
    seed_start integer;
    seed_position integer;
BEGIN
    IF period < 1 OR observations = 0 THEN
        RETURN CASE
            WHEN observations = 0 THEN ARRAY[]::double precision[]
            ELSE array_fill(NULL::double precision, ARRAY[observations])
        END;
    END IF;

    result := array_fill(NULL::double precision, ARRAY[observations]);
    FOR position IN 2..observations LOOP
        IF gains[position] IS NULL OR losses[position] IS NULL THEN
            valid_run := 0;
            average_gain := 0.0;
            average_loss := 0.0;
            CONTINUE;
        END IF;

        valid_run := valid_run + 1;
        IF valid_run < period THEN
            CONTINUE;
        ELSIF valid_run = period THEN
            average_gain := 0.0;
            average_loss := 0.0;
            seed_start := position - period + 1;
            FOR seed_position IN seed_start..position LOOP
                average_gain := average_gain + gains[seed_position];
                average_loss := average_loss + losses[seed_position];
            END LOOP;
            average_gain := average_gain / period;
            average_loss := average_loss / period;
        ELSE
            average_gain := (average_gain * (period - 1) + gains[position]) / period;
            average_loss := (average_loss * (period - 1) + losses[position]) / period;
        END IF;
        result[position] := CASE
            WHEN average_gain = 0 AND average_loss = 0 THEN NULL
            WHEN average_loss = 0 THEN 100.0
            WHEN average_gain = 0 THEN 0.0
            ELSE 100.0 - 100.0 / (1.0 + average_gain / average_loss)
        END;
    END LOOP;
    RETURN result;
END
$$;

DROP MATERIALIZED VIEW IF EXISTS xetra_loader.xetra_features;

CREATE MATERIALIZED VIEW xetra_loader.xetra_features AS
WITH ordered AS (
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
), returns AS MATERIALIZED (
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
), rsi_arrays AS (
    SELECT r.isin, r.exchange, r.code,
        xetra_loader.wilder_rsi(
            array_agg(r.gain ORDER BY r.observation_number),
            array_agg(r.loss ORDER BY r.observation_number),
            7
        ) AS rsi_7,
        xetra_loader.wilder_rsi(
            array_agg(r.gain ORDER BY r.observation_number),
            array_agg(r.loss ORDER BY r.observation_number),
            14
        ) AS rsi_14
    FROM returns AS r
    GROUP BY r.isin, r.exchange, r.code
), rsi_values AS (
    SELECT a.isin, a.exchange, a.code, s.observation_number,
        a.rsi_7[s.observation_number] AS rsi_7,
        a.rsi_14[s.observation_number] AS rsi_14
    FROM rsi_arrays AS a
    CROSS JOIN LATERAL generate_subscripts(a.rsi_7, 1) AS s(observation_number)
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
        CASE WHEN count(breadth_daily) OVER w5 = 5 THEN avg(breadth_daily) OVER w5 END AS breadth_5,
        CASE WHEN count(breadth_daily) OVER w10 = 10 THEN avg(breadth_daily) OVER w10 END AS breadth_10,
        CASE WHEN count(breadth_daily) OVER w20 = 20 THEN avg(breadth_daily) OVER w20 END AS breadth_20,
        CASE WHEN count(dispersion_daily) OVER w5 = 5 THEN avg(dispersion_daily) OVER w5 END AS dispersion_5,
        CASE WHEN count(dispersion_daily) OVER w10 = 10 THEN avg(dispersion_daily) OVER w10 END AS dispersion_10,
        CASE WHEN count(dispersion_daily) OVER w20 = 20 THEN avg(dispersion_daily) OVER w20 END AS dispersion_20
    FROM daily_cross AS d
    WINDOW
        w5 AS (ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
        w10 AS (ORDER BY trade_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
        w20 AS (ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
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
    rv.rsi_7 AS adjusted_close_rsi_7obs,
    rv.rsi_14 AS adjusted_close_rsi_14obs,
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
    c.dispersion_10 AS dispersion_10obs, c.dispersion_20 AS dispersion_20obs
FROM rolling AS r
LEFT JOIN rsi_values AS rv ON rv.isin = r.isin AND rv.exchange = r.exchange
    AND rv.code = r.code AND rv.observation_number = r.observation_number
JOIN cross_rolling AS c ON c.trade_date = r.trade_date
;

CREATE UNIQUE INDEX xetra_features_identity_idx
    ON xetra_loader.xetra_features (isin, exchange, code, trade_date);

ALTER MATERIALIZED VIEW xetra_loader.xetra_features OWNER TO "xetra-data-loader";

ALTER FUNCTION xetra_loader.wilder_rsi(double precision[], double precision[], integer)
    OWNER TO "xetra-data-loader";
REVOKE ALL ON FUNCTION xetra_loader.wilder_rsi(double precision[], double precision[], integer) FROM PUBLIC;

ALTER FUNCTION xetra_loader.resolve_feature_work_mem()
    OWNER TO "xetra-data-loader";
REVOKE ALL ON FUNCTION xetra_loader.resolve_feature_work_mem() FROM PUBLIC;

COMMENT ON MATERIALIZED VIEW xetra_loader.xetra_features IS
    'XETRA feature view; catalog version=2; base levels=adjusted_close_level,volume_level';

REVOKE ALL ON xetra_loader.xetra_features FROM PUBLIC;
GRANT SELECT ON xetra_loader.xetra_features TO "xetra-data-loader", portfell_app;

CREATE OR REPLACE FUNCTION xetra_loader.refresh_xetra_features()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, xetra_loader
AS $$
BEGIN
    PERFORM set_config('work_mem', xetra_loader.resolve_feature_work_mem(), true);
    REFRESH MATERIALIZED VIEW xetra_loader.xetra_features;
END
$$;

ALTER FUNCTION xetra_loader.refresh_xetra_features() OWNER TO "xetra-data-loader";
REVOKE ALL ON FUNCTION xetra_loader.refresh_xetra_features() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION xetra_loader.refresh_xetra_features() TO "xetra-data-loader";

COMMIT;
