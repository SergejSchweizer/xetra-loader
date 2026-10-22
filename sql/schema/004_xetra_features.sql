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
SELECT set_config(
    'max_parallel_workers_per_gather',
    greatest(
        coalesce(nullif(current_setting('xetra_loader.compute_cores', true), ''), '6')::integer
            - 1,
        0
    )::text,
    true
);
SELECT set_config('parallel_leader_participation', 'on', true);
-- Prefer parallel scans for the large source relation. WindowAgg itself is
-- still executed by the leader, but the upstream table scan/sort input can
-- use the configured worker budget where PostgreSQL's plan allows it.
SELECT set_config('min_parallel_table_scan_size', '0', true);
SELECT set_config('min_parallel_index_scan_size', '0', true);
SELECT set_config('parallel_setup_cost', '0', true);
SELECT set_config('parallel_tuple_cost', '0', true);

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
    relative_strength_log double precision;
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
        IF average_gain = 0 AND average_loss = 0 THEN
            result[position] := NULL;
        ELSIF average_loss = 0 THEN
            result[position] := 100.0::double precision;
        ELSIF average_gain = 0 THEN
            result[position] := 0.0::double precision;
        ELSE
            relative_strength_log := ln(average_gain) - ln(average_loss);
            IF relative_strength_log >= 700.0 THEN
                result[position] := 100.0::double precision;
            ELSIF relative_strength_log <= -700.0 THEN
                result[position] := 0.0::double precision;
            ELSIF relative_strength_log >= 0.0 THEN
                result[position] := 100.0::double precision
                    / (1.0::double precision + exp(-relative_strength_log));
            ELSE
                result[position] := 100.0::double precision
                    * exp(relative_strength_log)
                    / (1.0::double precision + exp(relative_strength_log));
            END IF;
        END IF;
    END LOOP;
    RETURN result;
END
$$;

CREATE OR REPLACE FUNCTION xetra_loader.exponential_moving_average(
    series_values double precision[],
    period integer,
    allow_nonpositive boolean DEFAULT FALSE
)
RETURNS double precision[]
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    observations integer := coalesce(array_length(series_values, 1), 0);
    result double precision[];
    seed_sum double precision := 0.0;
    previous_ema double precision;
    alpha double precision;
    valid_run integer := 0;
    position integer;
BEGIN
    IF period < 1 THEN
        RAISE EXCEPTION 'EMA period must be positive';
    END IF;
    IF observations = 0 THEN
        RETURN ARRAY[]::double precision[];
    END IF;

    result := array_fill(NULL::double precision, ARRAY[observations]);
    alpha := 2.0 / (period + 1.0);
    FOR position IN 1..observations LOOP
        IF series_values[position] IS NULL
           OR (series_values[position] <= 0 AND NOT allow_nonpositive) THEN
            valid_run := 0;
            seed_sum := 0.0;
            previous_ema := NULL;
            CONTINUE;
        END IF;

        valid_run := valid_run + 1;
        IF valid_run < period THEN
            seed_sum := seed_sum + series_values[position];
            CONTINUE;
        ELSIF valid_run = period THEN
            seed_sum := seed_sum + series_values[position];
            previous_ema := seed_sum / period;
        ELSE
            previous_ema := alpha * series_values[position] + (1.0 - alpha) * previous_ema;
        END IF;
        result[position] := previous_ema;
    END LOOP;
    RETURN result;
END
$$;

CREATE OR REPLACE FUNCTION xetra_loader.subtract_feature_arrays(
    left_values double precision[],
    right_values double precision[]
)
RETURNS double precision[]
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    observations integer := coalesce(array_length(left_values, 1), 0);
    result double precision[];
    position integer;
BEGIN
    IF observations <> coalesce(array_length(right_values, 1), 0) THEN
        RAISE EXCEPTION 'feature arrays must have equal lengths';
    END IF;
    IF observations = 0 THEN
        RETURN ARRAY[]::double precision[];
    END IF;

    result := array_fill(NULL::double precision, ARRAY[observations]);
    FOR position IN 1..observations LOOP
        IF left_values[position] IS NOT NULL AND right_values[position] IS NOT NULL THEN
            result[position] := left_values[position] - right_values[position];
        END IF;
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
        lag(q.adjusted_close::double precision, 2) OVER w AS adjusted_close_lag_2,
        lag(q.adjusted_close::double precision, 3) OVER w AS adjusted_close_lag_3,
        lag(q.adjusted_close::double precision, 4) OVER w AS adjusted_close_lag_4,
        lag(q.adjusted_close::double precision, 5) OVER w AS adjusted_close_lag_5,
        lag(q.adjusted_close::double precision, 8) OVER w AS adjusted_close_lag_8,
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
        CASE WHEN adjusted_close_level > 0 AND adjusted_close_lag_2 > 0
             THEN ln(adjusted_close_level / adjusted_close_lag_2) END AS log_return_2,
        CASE WHEN adjusted_close_level > 0 AND adjusted_close_lag_3 > 0
             THEN ln(adjusted_close_level / adjusted_close_lag_3) END AS log_return_3,
        CASE WHEN adjusted_close_level > 0 AND adjusted_close_lag_4 > 0
             THEN ln(adjusted_close_level / adjusted_close_lag_4) END AS log_return_4,
        CASE WHEN adjusted_close_level > 0 AND adjusted_close_lag_5 > 0
             THEN ln(adjusted_close_level / adjusted_close_lag_5) END AS log_return_5,
        CASE WHEN adjusted_close_level > 0 AND adjusted_close_lag_8 > 0
             THEN ln(adjusted_close_level / adjusted_close_lag_8) END AS log_return_8,
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
), return_lags AS (
    SELECT r.*,
        lag(log_return_1, 1) OVER w AS return_lag_1,
        lag(log_return_1, 4) OVER w AS return_lag_4,
        lag(log_return_1, 8) OVER w AS return_lag_8
    FROM returns AS r
    WINDOW w AS (PARTITION BY isin, exchange, code ORDER BY trade_date)
), rsi_arrays AS (
    SELECT r.isin, r.exchange, r.code,
        xetra_loader.wilder_rsi(
            array_agg(r.gain ORDER BY r.observation_number),
            array_agg(r.loss ORDER BY r.observation_number),
            3
        ) AS rsi_3,
        xetra_loader.wilder_rsi(
            array_agg(r.gain ORDER BY r.observation_number),
            array_agg(r.loss ORDER BY r.observation_number),
            5
        ) AS rsi_5,
        xetra_loader.wilder_rsi(
            array_agg(r.gain ORDER BY r.observation_number),
            array_agg(r.loss ORDER BY r.observation_number),
            7
        ) AS rsi_7,
        xetra_loader.wilder_rsi(
            array_agg(r.gain ORDER BY r.observation_number),
            array_agg(r.loss ORDER BY r.observation_number),
            14
        ) AS rsi_14,
        xetra_loader.wilder_rsi(
            array_agg(r.gain ORDER BY r.observation_number),
            array_agg(r.loss ORDER BY r.observation_number),
            15
        ) AS rsi_15
    FROM returns AS r
    GROUP BY r.isin, r.exchange, r.code
), rsi_values AS (
    SELECT a.isin, a.exchange, a.code, s.observation_number,
        a.rsi_3[s.observation_number] AS rsi_3,
        a.rsi_5[s.observation_number] AS rsi_5,
        a.rsi_7[s.observation_number] AS rsi_7,
        a.rsi_14[s.observation_number] AS rsi_14,
        a.rsi_15[s.observation_number] AS rsi_15
    FROM rsi_arrays AS a
    CROSS JOIN LATERAL generate_subscripts(a.rsi_3, 1) AS s(observation_number)
), rsi_value_lags AS (
    SELECT v.*,
        lag(rsi_5, 1) OVER w AS rsi_5_lag_1,
        lag(rsi_5, 3) OVER w AS rsi_5_lag_3
    FROM rsi_values AS v
    WINDOW w AS (PARTITION BY isin, exchange, code ORDER BY observation_number)
), ema_prices AS (
    SELECT r.isin, r.exchange, r.code,
        array_agg(r.adjusted_close_level ORDER BY r.observation_number) AS price_values
    FROM returns AS r
    GROUP BY r.isin, r.exchange, r.code
), ema_base AS (
    SELECT p.isin, p.exchange, p.code,
        xetra_loader.exponential_moving_average(price_values, 5) AS ema_5,
        xetra_loader.exponential_moving_average(price_values, 9) AS ema_9,
        xetra_loader.exponential_moving_average(price_values, 12) AS ema_12,
        xetra_loader.exponential_moving_average(price_values, 21) AS ema_21,
        xetra_loader.exponential_moving_average(price_values, 26) AS ema_26
    FROM ema_prices AS p
), ema_macd_line AS (
    SELECT e.*,
        xetra_loader.subtract_feature_arrays(ema_12, ema_26) AS macd_line
    FROM ema_base AS e
), ema_arrays AS (
    SELECT e.*,
        xetra_loader.exponential_moving_average(macd_line, 9, TRUE) AS macd_signal
    FROM ema_macd_line AS e
), ema_values AS (
    SELECT a.isin, a.exchange, a.code, s.observation_number,
        a.ema_5[s.observation_number] AS ema_5,
        a.ema_9[s.observation_number] AS ema_9,
        a.ema_12[s.observation_number] AS ema_12,
        a.ema_21[s.observation_number] AS ema_21,
        a.ema_26[s.observation_number] AS ema_26,
        a.macd_line[s.observation_number] AS macd_line,
        a.macd_signal[s.observation_number] AS macd_signal
    FROM ema_arrays AS a
    CROSS JOIN LATERAL generate_subscripts(a.ema_5, 1) AS s(observation_number)
), ema_value_lags AS (
    SELECT e.*,
        r.adjusted_close_level,
        CASE WHEN r.adjusted_close_level > 0 AND e.macd_line IS NOT NULL
                  AND e.macd_signal IS NOT NULL
             THEN (e.macd_line - e.macd_signal) / r.adjusted_close_level END
            AS macd_hist_relative,
        lag(e.ema_5, 1) OVER w AS ema_5_lag_1,
        lag(e.ema_9, 1) OVER w AS ema_9_lag_1,
        lag(e.ema_9, 3) OVER w AS ema_9_lag_3,
        lag(
            CASE WHEN r.adjusted_close_level > 0 AND e.macd_line IS NOT NULL
                      AND e.macd_signal IS NOT NULL
                 THEN (e.macd_line - e.macd_signal) / r.adjusted_close_level END,
            3
        ) OVER w AS macd_hist_relative_lag_3
    FROM ema_values AS e
    JOIN returns AS r
      ON r.isin = e.isin AND r.exchange = e.exchange AND r.code = e.code
     AND r.observation_number = e.observation_number
    WINDOW w AS (PARTITION BY e.isin, e.exchange, e.code ORDER BY e.observation_number)
), stochastic AS (
    SELECT r.*,
        CASE WHEN count(high_value) OVER w5 = 5 AND count(low_value) OVER w5 = 5
                   AND max(high_value) OVER w5 > min(low_value) OVER w5
             THEN 100.0 * (close_value - min(low_value) OVER w5)
                  / nullif(max(high_value) OVER w5 - min(low_value) OVER w5, 0) END
            AS stoch_k_5,
        CASE WHEN count(high_value) OVER w14 = 14 AND count(low_value) OVER w14 = 14
                   AND max(high_value) OVER w14 > min(low_value) OVER w14
             THEN 100.0 * (close_value - min(low_value) OVER w14)
                  / nullif(max(high_value) OVER w14 - min(low_value) OVER w14, 0) END
            AS stoch_k_14,
        CASE WHEN count(high_value) OVER prior20 = 20 AND count(low_value) OVER prior20 = 20
             THEN high_value > max(high_value) OVER prior20 END AS donchian_breakout_up,
        CASE WHEN count(high_value) OVER prior20 = 20 AND count(low_value) OVER prior20 = 20
             THEN low_value < min(low_value) OVER prior20 END AS donchian_breakout_down
    FROM return_lags AS r
    WINDOW
        w5 AS (PARTITION BY isin, exchange, code ORDER BY trade_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
        w14 AS (PARTITION BY isin, exchange, code ORDER BY trade_date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),
        prior20 AS (PARTITION BY isin, exchange, code ORDER BY trade_date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)
), stochastic_smoothed AS (
    SELECT s.*,
        CASE WHEN count(stoch_k_5) OVER w3 = 3 THEN avg(stoch_k_5) OVER w3 END AS stoch_d_5_3,
        CASE WHEN count(stoch_k_14) OVER w3 = 3 THEN avg(stoch_k_14) OVER w3 END AS stoch_d_14_3,
        lag(stoch_k_5, 3) OVER w AS stoch_k_5_lag_3
    FROM stochastic AS s
    WINDOW
        w AS (PARTITION BY isin, exchange, code ORDER BY trade_date),
        w3 AS (PARTITION BY isin, exchange, code ORDER BY trade_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
), rolling AS (
    SELECT r.*,
        CASE WHEN count(log_return_1) OVER w4 = 4 THEN exp(sum(log_return_1) OVER w4) - 1 END AS ret_4,
        CASE WHEN count(log_return_1) OVER w5 = 5 THEN exp(sum(log_return_1) OVER w5) - 1 END AS ret_5,
        CASE WHEN count(log_return_1) OVER w8 = 8 THEN exp(sum(log_return_1) OVER w8) - 1 END AS ret_8,
        CASE WHEN count(log_return_1) OVER w10 = 10 THEN exp(sum(log_return_1) OVER w10) - 1 END AS ret_10,
        CASE WHEN count(log_return_1) OVER w20 = 20 THEN exp(sum(log_return_1) OVER w20) - 1 END AS ret_20,
        CASE WHEN count(log_return_1) OVER w16 = 16 THEN exp(sum(log_return_1) OVER w16) - 1 END AS ret_16,
        CASE WHEN count(log_return_1) OVER w5 = 5 THEN avg(log_return_1) OVER w5 END AS mean_5,
        CASE WHEN count(log_return_1) OVER w10 = 10 THEN avg(log_return_1) OVER w10 END AS mean_10,
        CASE WHEN count(log_return_1) OVER w20 = 20 THEN avg(log_return_1) OVER w20 END AS mean_20,
        CASE WHEN count(log_return_1) OVER w5 = 5 THEN stddev_samp(log_return_1) OVER w5 END AS vol_5,
        CASE WHEN count(log_return_1) OVER w10 = 10 THEN stddev_samp(log_return_1) OVER w10 END AS vol_10,
        CASE WHEN count(log_return_1) OVER w20 = 20 THEN stddev_samp(log_return_1) OVER w20 END AS vol_20,
        CASE WHEN count(log_return_1) OVER w40 = 40 THEN stddev_samp(log_return_1) OVER w40 END AS vol_40,
        CASE WHEN count(log_return_1) OVER w8 = 8 THEN stddev_samp(log_return_1) OVER w8 END AS vol_8,
        CASE WHEN count(log_return_1) OVER w32 = 32 THEN stddev_samp(log_return_1) OVER w32 END AS vol_32,
        CASE WHEN count(log_return_1) OVER w96 = 96 THEN stddev_samp(log_return_1) OVER w96 END AS vol_96,
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
        CASE WHEN count(adjusted_close_level) OVER w15 = 15
             THEN avg(adjusted_close_level) OVER w5 / nullif(avg(adjusted_close_level) OVER w15, 0) END AS sma_5_15,
        CASE WHEN count(adjusted_close_level) OVER w20 = 20
             THEN adjusted_close_level / nullif(max(adjusted_close_level) OVER w20, 0) - 1 END AS drawdown_20,
        CASE WHEN count(adjusted_close_level) OVER w60 = 60
             THEN adjusted_close_level / nullif(max(adjusted_close_level) OVER w60, 0) - 1 END AS drawdown_60,
        CASE WHEN count(adjusted_close_level) OVER w32 = 32
             THEN adjusted_close_level / nullif(max(adjusted_close_level) OVER w32, 0) - 1 END AS drawdown_32,
        CASE WHEN count(adjusted_close_level) OVER w96 = 96
             THEN adjusted_close_level / nullif(max(adjusted_close_level) OVER w96, 0) - 1 END AS drawdown_96,
        CASE WHEN adjusted_close_level > 0 AND adjusted_close_lag_1 > 0
             THEN adjusted_close_level / adjusted_close_lag_1 - 1 END AS roc_1,
        CASE WHEN adjusted_close_level > 0 AND adjusted_close_lag_2 > 0
             THEN adjusted_close_level / adjusted_close_lag_2 - 1 END AS roc_2,
        CASE WHEN adjusted_close_level > 0 AND adjusted_close_lag_4 > 0
             THEN adjusted_close_level / adjusted_close_lag_4 - 1 END AS roc_4,
        CASE WHEN adjusted_close_level > 0 AND adjusted_close_lag_8 > 0
             THEN adjusted_close_level / adjusted_close_lag_8 - 1 END AS roc_8,
        CASE WHEN count(CASE WHEN log_return_1 IS NOT NULL AND return_lag_1 IS NOT NULL
                             THEN 1 END) OVER w96 = 96
             THEN corr(log_return_1, return_lag_1) OVER w96 END AS autocorr_lag1_96,
        CASE WHEN count(CASE WHEN log_return_1 IS NOT NULL AND return_lag_4 IS NOT NULL
                             THEN 1 END) OVER w96 = 96
             THEN corr(log_return_1, return_lag_4) OVER w96 END AS autocorr_lag4_96,
        CASE WHEN count(CASE WHEN log_return_1 IS NOT NULL AND return_lag_8 IS NOT NULL
                             THEN 1 END) OVER w192 = 192
             THEN corr(log_return_1, return_lag_8) OVER w192 END AS autocorr_lag8_192,
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
    FROM stochastic_smoothed AS r
    WINDOW
        w4 AS (PARTITION BY isin, exchange, code ORDER BY trade_date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW),
        w5 AS (PARTITION BY isin, exchange, code ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
        w8 AS (PARTITION BY isin, exchange, code ORDER BY trade_date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW),
        w10 AS (PARTITION BY isin, exchange, code ORDER BY trade_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
        w15 AS (PARTITION BY isin, exchange, code ORDER BY trade_date ROWS BETWEEN 14 PRECEDING AND CURRENT ROW),
        w16 AS (PARTITION BY isin, exchange, code ORDER BY trade_date ROWS BETWEEN 15 PRECEDING AND CURRENT ROW),
        w20 AS (PARTITION BY isin, exchange, code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
        w32 AS (PARTITION BY isin, exchange, code ORDER BY trade_date ROWS BETWEEN 31 PRECEDING AND CURRENT ROW),
        w40 AS (PARTITION BY isin, exchange, code ORDER BY trade_date ROWS BETWEEN 39 PRECEDING AND CURRENT ROW),
        w60 AS (PARTITION BY isin, exchange, code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
        w96 AS (PARTITION BY isin, exchange, code ORDER BY trade_date ROWS BETWEEN 95 PRECEDING AND CURRENT ROW),
        w192 AS (PARTITION BY isin, exchange, code ORDER BY trade_date ROWS BETWEEN 191 PRECEDING AND CURRENT ROW)
), normalized AS (
    SELECT r.*,
        CASE WHEN count(vol_8) OVER w96 = 96
             THEN (vol_8 - avg(vol_8) OVER w96)
                  / nullif(stddev_samp(vol_8) OVER w96, 0) END AS volatility_zscore_96,
        CASE WHEN count(adjusted_close_level) OVER w96 = 96
             THEN (adjusted_close_level - avg(adjusted_close_level) OVER w96)
                  / nullif(stddev_samp(adjusted_close_level) OVER w96, 0) END AS level_zscore_96
    FROM rolling AS r
    WINDOW w96 AS (PARTITION BY isin, exchange, code ORDER BY trade_date
        ROWS BETWEEN 95 PRECEDING AND CURRENT ROW)
), daily_cross AS (
    SELECT trade_date,
        avg(CASE WHEN log_return_1 > 0 THEN 1.0 WHEN log_return_1 IS NOT NULL THEN 0.0 END) AS breadth_daily,
        stddev_samp(log_return_1) AS dispersion_daily
    FROM normalized
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
    (
        r.trade_date + CASE EXTRACT(ISODOW FROM r.trade_date)::integer
            WHEN 5 THEN 3
            WHEN 6 THEN 2
            ELSE 1
        END
    )::timestamp without time zone AT TIME ZONE 'UTC' AS available_at_utc,
    r.adjusted_close_level, r.volume_level,
    r.log_return_1 AS adjusted_close_log_return_1obs,
    r.log_return_3 AS adjusted_close_log_return_3obs,
    r.log_return_5 AS adjusted_close_log_return_5obs,
    r.log_return_10 AS adjusted_close_log_return_10obs,
    r.log_return_20 AS adjusted_close_log_return_20obs,
    r.log_return_1 AS log_return_1,
    r.log_return_2 AS log_return_2,
    r.log_return_3 AS log_return_3,
    r.log_return_4 AS log_return_4,
    r.log_return_5 AS log_return_5,
    r.log_return_8 AS log_return_8,
    r.log_return_10 AS log_return_10,
    r.log_return_20 AS log_return_20,
    r.ret_5 AS adjusted_close_return_geom_5obs_pct,
    r.ret_10 AS adjusted_close_return_geom_10obs_pct,
    r.ret_20 AS adjusted_close_return_geom_20obs_pct,
    r.ret_4 AS rolling_cum_return_4,
    r.ret_8 AS rolling_cum_return_8,
    r.ret_16 AS rolling_cum_return_16,
    r.mean_5 AS adjusted_close_return_mean_5obs,
    r.mean_10 AS adjusted_close_return_mean_10obs,
    r.mean_20 AS adjusted_close_return_mean_20obs,
    r.vol_5 AS adjusted_close_volatility_5obs,
    r.vol_10 AS adjusted_close_volatility_10obs,
    r.vol_20 AS adjusted_close_volatility_20obs,
    r.vol_40 AS adjusted_close_volatility_40obs,
    r.vol_8 AS rolling_volatility_8,
    r.vol_32 AS rolling_volatility_32,
    r.vol_96 AS rolling_volatility_96,
    r.range_1obs AS high_low_range_1obs,
    r.range_5obs AS high_low_range_5obs,
    r.intraday_1obs AS intraday_return_1obs,
    r.intraday_5obs AS intraday_return_5obs,
    r.gap_1obs AS overnight_gap_1obs,
    r.gap_5obs AS overnight_gap_5obs,
    r.sma_5_20 AS adjusted_close_sma_ratio_5_20,
    r.sma_10_20 AS adjusted_close_sma_ratio_10_20,
    r.sma_10_40 AS adjusted_close_sma_ratio_10_40,
    r.sma_5_15 AS sma_ratio_5_15,
    rv.rsi_7 AS adjusted_close_rsi_7obs,
    rv.rsi_14 AS adjusted_close_rsi_14obs,
    rv.rsi_3 AS rsi_3,
    rv.rsi_5 AS rsi_5,
    rv.rsi_7 AS rsi_7,
    rv.rsi_14 AS rsi_14,
    CASE WHEN rv.rsi_5 IS NOT NULL AND rv.rsi_15 IS NOT NULL
         THEN rv.rsi_5 / nullif(rv.rsi_15, 0) END AS rsi_ratio_5_15,
    CASE WHEN rv.rsi_5 IS NOT NULL AND rv.rsi_5_lag_3 IS NOT NULL
         THEN (rv.rsi_5 - rv.rsi_5_lag_3) / 3.0 END AS rsi5_slope_3,
    CASE WHEN rv.rsi_5 IS NOT NULL AND rv.rsi_5_lag_1 IS NOT NULL
         THEN rv.rsi_5 - rv.rsi_5_lag_1 END AS rsi5_delta_1,
    CASE WHEN r.adjusted_close_level > 0 AND ev.ema_5 IS NOT NULL
         THEN r.adjusted_close_level / ev.ema_5 - 1 END AS ema5_distance,
    CASE WHEN r.adjusted_close_level > 0 AND ev.ema_9 IS NOT NULL
         THEN r.adjusted_close_level / ev.ema_9 - 1 END AS ema9_distance,
    CASE WHEN r.adjusted_close_level > 0 AND ev.ema_21 IS NOT NULL
         THEN r.adjusted_close_level / ev.ema_21 - 1 END AS ema21_distance,
    CASE WHEN r.adjusted_close_level > 0 AND ev.ema_5 IS NOT NULL AND ev.ema_21 IS NOT NULL
         THEN (ev.ema_5 - ev.ema_21) / r.adjusted_close_level END AS ema5_ema21_spread,
    CASE WHEN r.adjusted_close_level > 0 AND ev.ema_9 IS NOT NULL
                   AND ev.ema_9_lag_3 IS NOT NULL
         THEN (ev.ema_9 - ev.ema_9_lag_3) / (3.0 * r.adjusted_close_level) END AS ema9_slope_3,
    CASE WHEN ev.ema_5 IS NOT NULL AND ev.ema_9 IS NOT NULL
                   AND ev.ema_5_lag_1 IS NOT NULL AND ev.ema_9_lag_1 IS NOT NULL
         THEN CASE WHEN ev.ema_5 > ev.ema_9 AND ev.ema_5_lag_1 <= ev.ema_9_lag_1 THEN 1
                   WHEN ev.ema_5 < ev.ema_9 AND ev.ema_5_lag_1 >= ev.ema_9_lag_1 THEN -1
                   ELSE 0 END END AS ema5_ema9_cross,
    CASE WHEN r.adjusted_close_level > 0 AND ev.macd_line IS NOT NULL
         THEN ev.macd_line / r.adjusted_close_level END AS macd_line_relative,
    ev.macd_hist_relative AS macd_hist_relative,
    CASE WHEN ev.macd_hist_relative IS NOT NULL
                   AND ev.macd_hist_relative_lag_3 IS NOT NULL
         THEN ev.macd_hist_relative - ev.macd_hist_relative_lag_3 END AS macd_hist_delta_3,
    r.stoch_k_5 AS stoch_k_5,
    r.stoch_d_5_3 AS stoch_d_5_3,
    r.stoch_k_14 AS stoch_k_14,
    r.stoch_d_14_3 AS stoch_d_14_3,
    CASE WHEN r.stoch_k_5 IS NOT NULL AND r.stoch_d_5_3 IS NOT NULL
         THEN r.stoch_k_5 - r.stoch_d_5_3 END AS stoch_kd_spread_5,
    CASE WHEN r.stoch_k_5 IS NOT NULL AND r.stoch_k_5_lag_3 IS NOT NULL
         THEN (r.stoch_k_5 - r.stoch_k_5_lag_3) / 3.0 END AS stoch_k_slope_3,
    r.donchian_breakout_up AS donchian_breakout_up,
    r.donchian_breakout_down AS donchian_breakout_down,
    r.autocorr_lag1_96 AS momentum_autocorr_lag1_window96,
    r.autocorr_lag4_96 AS momentum_autocorr_lag4_window96,
    r.autocorr_lag8_192 AS momentum_autocorr_lag8_window192,
    r.drawdown_32 AS rolling_drawdown_32,
    r.drawdown_96 AS rolling_drawdown_96,
    r.volatility_zscore_96 AS volatility_zscore_96,
    r.level_zscore_96 AS level_zscore_96,
    CASE WHEN r.volume_level >= 0 THEN ln(1.0 + r.volume_level) END AS log_tick_volume,
    r.roc_1 AS roc_1,
    r.roc_2 AS roc_2,
    r.roc_4 AS roc_4,
    r.roc_8 AS roc_8,
    CASE WHEN r.roc_4 IS NOT NULL AND r.roc_8 IS NOT NULL
         THEN r.roc_4 - r.roc_8 END AS roc_acceleration_4_8,
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
FROM normalized AS r
LEFT JOIN rsi_value_lags AS rv ON rv.isin = r.isin AND rv.exchange = r.exchange
    AND rv.code = r.code AND rv.observation_number = r.observation_number
LEFT JOIN ema_value_lags AS ev ON ev.isin = r.isin AND ev.exchange = r.exchange
    AND ev.code = r.code AND ev.observation_number = r.observation_number
JOIN cross_rolling AS c ON c.trade_date = r.trade_date
;

CREATE UNIQUE INDEX xetra_features_identity_idx
    ON xetra_loader.xetra_features (isin, exchange, code, trade_date);

ALTER MATERIALIZED VIEW xetra_loader.xetra_features OWNER TO "xetra-data-loader";

ALTER FUNCTION xetra_loader.wilder_rsi(double precision[], double precision[], integer)
    OWNER TO "xetra-data-loader";
REVOKE ALL ON FUNCTION xetra_loader.wilder_rsi(double precision[], double precision[], integer) FROM PUBLIC;

ALTER FUNCTION xetra_loader.exponential_moving_average(double precision[], integer, boolean)
    OWNER TO "xetra-data-loader";
REVOKE ALL ON FUNCTION xetra_loader.exponential_moving_average(double precision[], integer, boolean) FROM PUBLIC;

ALTER FUNCTION xetra_loader.subtract_feature_arrays(double precision[], double precision[])
    OWNER TO "xetra-data-loader";
REVOKE ALL ON FUNCTION xetra_loader.subtract_feature_arrays(double precision[], double precision[]) FROM PUBLIC;

ALTER FUNCTION xetra_loader.resolve_feature_work_mem()
    OWNER TO "xetra-data-loader";
REVOKE ALL ON FUNCTION xetra_loader.resolve_feature_work_mem() FROM PUBLIC;

COMMENT ON MATERIALIZED VIEW xetra_loader.xetra_features IS
    'XETRA feature view; catalog version=4; base levels=adjusted_close_level,volume_level';

REVOKE ALL ON xetra_loader.xetra_features FROM PUBLIC;
GRANT SELECT ON xetra_loader.xetra_features TO "xetra-data-loader", portfell_app;

CREATE OR REPLACE FUNCTION xetra_loader.refresh_xetra_features()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, xetra_loader
AS $$
DECLARE
    configured_cores integer;
BEGIN
    BEGIN
        configured_cores := coalesce(
            nullif(current_setting('xetra_loader.compute_cores', true), ''), '6'
        )::integer;
    EXCEPTION WHEN others THEN
        RAISE EXCEPTION 'invalid xetra_loader.compute_cores setting';
    END;
    IF configured_cores < 1 OR configured_cores > 64 THEN
        RAISE EXCEPTION 'xetra_loader.compute_cores must be between 1 and 64';
    END IF;
    PERFORM set_config('work_mem', xetra_loader.resolve_feature_work_mem(), true);
    -- PostgreSQL's parallel workers plus the participating leader stay within
    -- the configured process-wide CPU budget.
    PERFORM set_config(
        'max_parallel_workers_per_gather', greatest(configured_cores - 1, 0)::text, true
    );
    PERFORM set_config('parallel_leader_participation', 'on', true);
    PERFORM set_config('min_parallel_table_scan_size', '0', true);
    PERFORM set_config('min_parallel_index_scan_size', '0', true);
    PERFORM set_config('parallel_setup_cost', '0', true);
    PERFORM set_config('parallel_tuple_cost', '0', true);
    REFRESH MATERIALIZED VIEW xetra_loader.xetra_features;
END
$$;

ALTER FUNCTION xetra_loader.refresh_xetra_features() OWNER TO "xetra-data-loader";
REVOKE ALL ON FUNCTION xetra_loader.refresh_xetra_features() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION xetra_loader.refresh_xetra_features() TO "xetra-data-loader";

COMMIT;
