# xetra-loader

Deterministic XETRA market-data loader. The repository owns EODHD access,
Bronze/Silver/Gold datasets, PostgreSQL publication, and the Sunday schedule in
`deploy/cron/xetra-loader.cron`; `BACKLOG.md` is a historical archive.

## Contents

- [Python setup](#python-setup)
- [Local secrets](#local-secrets)
- [Lake storage format](#lake-storage-format)
- [API and durable weekly worker](#api-and-durable-weekly-worker)
- [Feature catalog](#feature-catalog)
- [Feature benchmark](#feature-benchmark)
- [PostgreSQL schema inventory](#postgresql-schema-inventory)
- [Schedule and resource limits](#schedule-and-resource-limits)
- [Validation](#validation)

## Python setup

The repository is pinned to **CPython 3.14.7**.

```bash
python3.14 -m venv .venv
source .venv/bin/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
python --version           # must report Python 3.14.7
python -m pip install --upgrade pip
python -m pip install -e .
python -c "import xetra_loader; print(xetra_loader.__version__)"
```

The repository-local `.venv/` is intentionally ignored and must never be committed.

For a new PostgreSQL serving database, apply the current contracts with an
administrative connection in this order: `sql/schema/001_xetra_loader.sql`,
`sql/schema/002_roles.sql`, `sql/schema/003_listing_lifecycle.sql`,
`sql/sync/001_xetra_loader_sync.sql`, `sql/schema/004_xetra_features.sql`, and
`sql/schema/006_durable_jobs.sql`. Apply
`sql/roles/005_xetra_loader_feature_view_reader.sql` only after the externally
provisioned `xetra_loader` role exists; it grants that role `USAGE` on the
`xetra_loader` schema and `SELECT` only on `xetra_loader.xetra_features`.
The explicit provisioning command is `.venv/bin/xdl-migrate`; it uses only the
admin DSN and creates/populates the materialized feature view and durable job
queue. The weekly API worker never creates or alters PostgreSQL objects.
The publication writer is the non-superuser `xetra_data_loader_writer`, a
member of the `xetra-data-loader` group. The legacy files in `sql/migrations/`
are historical rename migrations, not the current clean-create setup.

## Local secrets

Copy `config.example.yaml` to `config.yaml` and fill in the EODHD token plus separate PostgreSQL writer and admin credentials. Set `postgres.database` to the dedicated repository database `xetra_loader`; it overrides the database component of the configured writer URI. `postgres.writer_dsn` (or `XDL_POSTGRES_WRITER_DSN`) is used by the weekly runner and must authenticate as the non-superuser `xetra_data_loader_writer`; the runner rejects superuser sessions. `postgres.admin_dsn` (or `XDL_POSTGRES_ADMIN_DSN`) is required only for schema provisioning, migration, reset, and the controlled full bootstrap. When no explicit admin URI is set, the loader composes one from the local `host`, `port`, `user`, `password`, and `database` fields. The loader reads this ignored local file; never commit it. `EODHD_API_TOKEN` and `XDL_MEDALLION_ROOT` can still be provided through the environment.

Set `api.token` to a long random bearer token in `config.yaml`; it protects job
submission and status reads. The API defaults to loopback (`127.0.0.1:8765`),
so the scheduled enqueue client and API service run on the same host. The real
token must never be placed in cron arguments, logs, or tracked files.

## Lake storage format

All current row artifacts are written by Polars as Parquet with Zstandard
compression. Canonical paths are:

- Bronze monthly rows: `bronze/dataset_type={dataset_type}/exchange={exchange}/instrument_type={instrument_type}/symbol={symbol}/timeframe={timeframe}/year={YYYY}/month={YYYY-MM}/data.parquet`.
- Silver annual rows: `silver/dataset_type={dataset_type}/exchange={exchange}/symbol={symbol}/timeframe={timeframe}/year={YYYY}/{symbol}-{YYYY}.parquet`.
- Gold symbol snapshots: `gold/dataset_id={dataset_id}/dataset_type=gold_symbol_dataset/exchange={exchange}/symbol={symbol}/{symbol}_GOLD_{feature_hash}_{source_hash}.parquet`.

Small JSON manifests live under each layer's `_metadata/` directory and identify
the current canonical artifacts. Bronze provider objects are retained as
canonical JSON strings inside Parquet rows. Gold corporate-action retractions
are Parquet sidecars; checkpoints, manifests, and restart metadata remain JSON
control files.

The one-time migration reads the previous `versions/<storage_version>/`
snapshots and old row JSON, writes and validates canonical Parquet artifacts,
then moves legacy row files and obsolete layouts into a timestamped recovery
backup outside the lake. Manifests and restart metadata remain JSON control
files. The normal runtime reader refuses to fall back to retired layouts and
requires the migration to have published a canonical manifest first.

To migrate an existing JSON lake, first let any active writer finish, then run:

```bash
PYTHONPATH=src .venv/bin/python scripts/migrate_lake_to_parquet.py
```

The migration validates all Gold fingerprints before moving retired row
artifacts below `/home/dev_xetra/.xdl-lake-recovery-backups/`. This preserves a
recoverable copy while ensuring the lake itself contains only canonical
row-bearing artifacts.

## Current scope

`BACKLOG.md` is a historical archive; it contains no active work orders. The
implemented Python pipeline owns EODHD transport, XETRA listings and corporate
actions, Bronze/Silver/Gold datasets, transactional PostgreSQL publication,
restart-safe orchestration, and acceptance verification.

The core pipeline downloads XETRA listings, EOD quotes, dividend events, and
split events from EODHD; builds their Bronze, Silver, and Gold datasets; and
publishes complete retained histories to PostgreSQL raw tables. Its
restart-safe weekly worker reconciles provider corrections as well as new
observations and refreshes the downstream feature materialization.

The feature contract is `xetra_loader.xetra_features`, a materialized view
with only `adjusted_close_level` and `volume_level` as base levels. OHLC
columns are never exposed as levels. Raw OHLC is used internally only by
high-low range, intraday return, overnight gap, Stochastic, and Donchian
features. Fold-local standardization is a downstream operation documented in
[`docs/train-fold-standardization.md`](docs/train-fold-standardization.md); it
never changes Gold or the global feature view.

The feature catalog is currently version **4**. Its window metadata declares
the counting domain explicitly: price lag/return/ROC/RSI windows count valid
price levels (`n + 1` levels for an `n`-lag or period), high-low and intraday
windows count valid source rows, overnight gaps count `n + 1` valid source
rows, and breadth/dispersion rolling windows count valid market dates. The
`*_return_geom_*obs_pct` values are decimal fractions, matching macro-loader;
they are not multiplied by 100.

Feature-view refresh memory is deployment-configurable through
`XDL_FEATURE_WORK_MEM` or `postgres.feature_work_mem` in the ignored
`config.yaml`. The validated range is **16MB through 4GB**, with a conservative
default of **256MB**. PostgreSQL applies `work_mem` per operation and worker,
so the setting is a resource budget rather than a transaction-wide memory cap;
the migration and weekly refresh use the same setting.

## API and durable weekly worker

`xdl-api` hosts both the REST application and its one durable sync worker in
the same process. The cron entry runs `xdl-enqueue-weekly`, which submits a
`weekly_sync` request; it does not start a second downloader or compute
process.

```text
Sunday cron ──POST /v1/jobs──> xdl-api REST routes
                                   │ committed INSERT + NOTIFY
                                   ▼
                         xetra_loader.loader_jobs
                                   │ LISTEN/NOTIFY
                                   ▼
                      in-process weekly job worker
                                   │
                                   └──> restart-safe EODHD → lake → PostgreSQL pipeline
```

The API exposes `GET /healthz`, authenticated `POST /v1/jobs` with
`{"type":"weekly_sync"}`, and authenticated `GET /v1/jobs/{job_id}`. The
database enforces one active weekly sync at a time. The worker acquires a
PostgreSQL advisory lock, requeues any interrupted `running` job on startup,
and blocks on PostgreSQL `LISTEN/NOTIFY` while idle; it does not issue periodic
queue queries. Install `deploy/systemd/xetra-loader-api.service` to keep the
API and worker resident. The service file is a deployment artifact; it must be
installed and enabled separately by the host administrator.

The direct `.venv/bin/xdl-weekly --cores 6` command remains available for a
controlled one-off local run; the deployed Sunday cron uses the API queue.

## Feature catalog

Catalog version **4** defines **98 derived features** from two exposed base
levels. The materialized view therefore has 104 columns: four row-identity
columns, the two levels, and the 98 derived values. The executable catalog is
`src/xetra_loader/features/catalog.py`; the transformations below are checked
against an independent calculation reference and PostgreSQL integration
fixtures.

Every rolling calculation is causal: the current row and earlier observations
only are used. Per-instrument windows count valid observations, not calendar
days, and emit `NULL` until the catalog's minimum valid count is reached.
Breadth and dispersion are cross-sectional by trade date; their rolling
windows count market dates. Standard deviations use the sample estimator
where noted. Return values are decimal fractions (so `0.01` means 1%); the
`_pct` suffix does not multiply a result by 100.

`adjusted_close_level` is the adjusted closing price and `volume_level` is
provider volume. The table lists every derived output column and its
transformation; names separated by commas share the formula shown in that row.
Here, `t` is the current instrument observation, `n` is the indicated window,
`r₁` is the one-observation adjusted-close log return, `SMAₙ` is the trailing
simple moving average, and `EMAₙ` is the SMA-seeded exponential moving
average.

| Output columns | Transformation |
| --- | --- |
| `adjusted_close_log_return_1obs`, `adjusted_close_log_return_3obs`, `adjusted_close_log_return_5obs`, `adjusted_close_log_return_10obs`, `adjusted_close_log_return_20obs`; `log_return_1`, `log_return_2`, `log_return_3`, `log_return_4`, `log_return_5`, `log_return_8`, `log_return_10`, `log_return_20` | `ln(adjusted_close_level[t] / adjusted_close_level[t-n])`, for each name's lag `n`. |
| `adjusted_close_return_geom_5obs_pct`, `adjusted_close_return_geom_10obs_pct`, `adjusted_close_return_geom_20obs_pct`; `rolling_cum_return_4`, `rolling_cum_return_8`, `rolling_cum_return_16` | `exp(sum(r₁ over the trailing n returns)) - 1`; `n` is encoded in each name. |
| `adjusted_close_return_mean_5obs`, `adjusted_close_return_mean_10obs`, `adjusted_close_return_mean_20obs` | Arithmetic mean of the trailing `n` one-observation log returns. |
| `adjusted_close_volatility_5obs`, `adjusted_close_volatility_10obs`, `adjusted_close_volatility_20obs`, `adjusted_close_volatility_40obs`; `rolling_volatility_8`, `rolling_volatility_32`, `rolling_volatility_96` | Sample standard deviation of the trailing `n` one-observation log returns. |
| `high_low_range_1obs`, `high_low_range_5obs` | Mean of `(high - low) / close` over the trailing `n` valid source rows. |
| `intraday_return_1obs`, `intraday_return_5obs` | Mean of `(close - open) / open` over the trailing `n` valid source rows. |
| `overnight_gap_1obs`, `overnight_gap_5obs` | Mean of `(open[t] - close[t-1]) / close[t-1]` over the trailing `n` valid gaps. |
| `adjusted_close_sma_ratio_5_20`, `adjusted_close_sma_ratio_10_20`, `adjusted_close_sma_ratio_10_40`, `sma_ratio_5_15` | Ratio of the two named trailing adjusted-close SMAs, e.g. `SMA₅ / SMA₂₀`. |
| `adjusted_close_rsi_7obs`, `adjusted_close_rsi_14obs`; `rsi_3`, `rsi_5`, `rsi_7`, `rsi_14` | Wilder RSI of adjusted-close gains and losses for the named period; a fully flat gain/loss window is `NULL`. |
| `rsi_ratio_5_15` | `rsi_5[t] / rsi_15[t]`; `rsi_15` is an internal intermediate, not an exposed column. |
| `rsi5_slope_3` | `(rsi_5[t] - rsi_5[t-3]) / 3`. |
| `rsi5_delta_1` | `rsi_5[t] - rsi_5[t-1]`. |
| `ema5_distance`, `ema9_distance`, `ema21_distance` | `adjusted_close_level[t] / EMAₙ[t] - 1`, with `n` from the name. |
| `ema5_ema21_spread` | `(EMA₅[t] - EMA₂₁[t]) / adjusted_close_level[t]`. |
| `ema9_slope_3` | `(EMA₉[t] - EMA₉[t-3]) / (3 × adjusted_close_level[t])`. |
| `ema5_ema9_cross` | `1` for an upward EMA₅/EMA₉ cross, `-1` for a downward cross, otherwise `0`; `NULL` until both EMAs and prior values exist. |
| `adjusted_close_roc_3obs`, `adjusted_close_roc_5obs`, `adjusted_close_roc_10obs`, `adjusted_close_roc_20obs`; `roc_1`, `roc_2`, `roc_4`, `roc_8` | `adjusted_close_level[t] / adjusted_close_level[t-n] - 1`, for the named lag `n`. |
| `roc_acceleration_4_8` | `roc_4[t] - roc_8[t]`. |
| `stoch_k_5`, `stoch_k_14` | `100 × (close[t] - rolling_min(low, n)) / (rolling_max(high, n) - rolling_min(low, n))`, for `n` from the name. |
| `stoch_d_5_3`, `stoch_d_14_3` | Three-observation SMA of the corresponding `%K` series. |
| `stoch_kd_spread_5` | `stoch_k_5[t] - stoch_d_5_3[t]`. |
| `stoch_k_slope_3` | `(stoch_k_5[t] - stoch_k_5[t-3]) / 3`. |
| `macd_line_relative` | `(EMA₁₂[t] - EMA₂₆[t]) / adjusted_close_level[t]`. |
| `macd_hist_relative` | `(MACD line[t] - 9-observation EMA signal[t]) / adjusted_close_level[t]`. |
| `macd_hist_delta_3` | `macd_hist_relative[t] - macd_hist_relative[t-3]`. |
| `donchian_breakout_up` | Boolean: current high exceeds the maximum high of the **prior** 20 observations. |
| `donchian_breakout_down` | Boolean: current low is below the minimum low of the **prior** 20 observations. |
| `volatility_zscore_96` | Z-score of `rolling_volatility_8` against its trailing 96 valid volatility values. |
| `level_zscore_96` | `(adjusted_close_level[t] - trailing_mean₉₆) / trailing_sample_std₉₆`. |
| `momentum_autocorr_lag1_window96` | Pearson correlation of `r₁` with its one-observation lag over 96 valid return pairs. |
| `momentum_autocorr_lag4_window96` | Pearson correlation of `r₁` with its four-observation lag over 96 valid return pairs. |
| `momentum_autocorr_lag8_window192` | Pearson correlation of `r₁` with its eight-observation lag over 192 valid return pairs. |
| `adjusted_close_drawdown_20obs`, `adjusted_close_drawdown_60obs`; `rolling_drawdown_32`, `rolling_drawdown_96` | `adjusted_close_level[t] / trailing_max(adjusted_close_level, n) - 1`, including the current observation. |
| `relative_volume_5obs`, `relative_volume_10obs`, `relative_volume_20obs` | `volume_level[t] / trailing_mean(volume_level, n)`. |
| `volume_zscore_5obs`, `volume_zscore_10obs`, `volume_zscore_20obs` | `(volume_level[t] - trailing_meanₙ) / trailing_sample_stdₙ`. |
| `log_tick_volume` | `ln(1 + volume_level[t])` when volume is nonnegative; otherwise `NULL`. |
| `breadth_daily` | Per trade date, fraction of instruments with a positive one-observation adjusted-close log return. |
| `breadth_5obs`, `breadth_10obs`, `breadth_20obs` | Mean of `breadth_daily` over the trailing 5, 10, or 20 valid market dates. |
| `dispersion_daily` | Per trade date, sample standard deviation across instruments of one-observation adjusted-close log returns. |
| `dispersion_5obs`, `dispersion_10obs`, `dispersion_20obs` | Mean of `dispersion_daily` over the trailing 5, 10, or 20 valid market dates. |

All OHLC-derived columns above are derived features, not exposed OHLC levels.
No pairwise or average-correlation columns are calculated. The complete
104-column view inventory is recorded once in the PostgreSQL schema inventory
below.

## Feature benchmark

Run `scripts/quality/benchmark_features.py` to compare one global Polars window
plan with per-instrument Polars plans on the same synthetic input. The script
checks both implementations against each other and against the independent
feature reference before reporting median timings. The reproducible baseline
uses 100,000 rows, 100 instruments, five timed rounds, and a six-core budget;
the measured result and environment are recorded in
[`docs/feature-benchmark.md`](docs/feature-benchmark.md). In that run, the
global plan was 14.26× faster than per-instrument plans. This synthetic
microbenchmark does not measure total weekly-sync or PostgreSQL-refresh time.

## PostgreSQL schema inventory

The raw quote table `xetra_loader.eod_quotes` has **13 columns**: four identity
columns (`isin`, `exchange`, `code`, `trade_date`), one EOD timestamp, six raw
market fields (`open`, `high`, `low`, `close`, `adjusted_close`, `volume`), and
two publication-audit timestamps. It contains raw OHLCV data, not derived
features.

The materialized view `xetra_loader.xetra_features` has **104 columns**:

- **4 identity columns**: `isin`, `exchange`, `code`, `trade_date`;
- **2 exposed level columns**: `adjusted_close_level`, `volume_level`;
- **98 derived feature columns** from the versioned catalog, or **100 feature
  columns** when the two base levels are included and identity columns are
  excluded.

The view deliberately contains no `open_level`, `high_level`, `low_level`, or
`close_level`, and no `average_correlation_5obs`,
`average_correlation_10obs`, or `average_correlation_20obs`. Internal OHLC
values are used only for high-low range, intraday return, and overnight gap.

## Schedule and resource limits

The deployed schedule contract is Sunday `11:00` in `Europe/Vienna`. The cron
entry submits a durable job; one in-process worker runs the restart-safe
pipeline. `weekly.cores` defaults to **6** and can be overridden with
`xdl-weekly --cores N`, `xdl-api --cores N`, or `XDL_CORES`. The API sets
`POLARS_MAX_THREADS` before importing Polars, and the same budget bounds
concurrent provider fetches. Provider fetch pools are closed before the next
Polars-heavy stage; PostgreSQL publication stays serialized except for the
independent dividend/split writers. Feature-view refresh is one atomic
PostgreSQL operation. API and weekly logs are rotated below ignored `.logs/`.

PostgreSQL `work_mem` for feature refresh is separately configurable through
`XDL_FEATURE_WORK_MEM` or `postgres.feature_work_mem`; the default is `256MB`
within the validated `16MB`–`4GB` range. It is a per-operation/per-worker
budget, not a transaction-wide cap.

## Validation

Run local quality gates with the repository's pinned interpreter:

```bash
.venv/bin/ruff check --no-cache src tests scripts
.venv/bin/mypy --cache-dir=/tmp/xdl-mypy-cache
.venv/bin/python -m pytest -p no:cacheprovider --import-mode=importlib
.venv/bin/python scripts/quality/benchmark_features.py \
  --rows 100000 --instruments 100 --rounds 5 --cores 6
```

PostgreSQL integration tests require `XDL_TEST_POSTGRES_DSN`; they should use a
disposable test database, never the production DSN. No GitHub Actions workflow
is required to run these checks.
