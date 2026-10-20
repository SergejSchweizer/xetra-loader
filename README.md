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

All current row artifacts are written by Polars as Parquet with Zstandard compression. Canonical paths are:

- Bronze daily rows: `bronze/dataset_type={type}/exchange={exchange}/instrument_type={type}/symbol={symbol}/timeframe={timeframe}/year={YYYY}/month={YYYY-MM}/date={YYYY-MM-DD}/data.parquet`.
- Silver monthly rows: `silver/dataset_type={type}/exchange={exchange}/symbol={symbol}/timeframe={timeframe}/year={YYYY}/month={YYYY-MM}/{symbol}-{YYYY-MM}.parquet`.
- Gold symbol snapshots: `gold/dataset_id={id}/dataset_type=gold_symbol_dataset/feature_set_version=v{MAJOR}.{MINOR}.{PATCH}/exchange={exchange}/symbol={symbol}/{symbol}_GOLD_{feature_hash}_{source_hash}.parquet`.

Small JSON manifests live under each layer's `_metadata/` directory and identify
the current canonical artifacts. Bronze provider objects are retained as
canonical JSON strings inside Parquet rows. Gold corporate-action retractions
are Parquet sidecars; checkpoints, manifests, and restart metadata remain JSON
control files.

The one-time migration reads the previous `versions/<storage_version>/`
snapshots and old row JSON, writes and validates canonical Parquet artifacts,
then archives legacy `data.json` and `retractions.json` row artifacts into a
timestamped recoverable backup. Manifests and restart metadata remain JSON
control files. Legacy versioned Parquet files are left in place as recovery
copies. The normal runtime reader refuses to fall back to that retired layout
and requires the migration to have published a canonical manifest first.

To migrate an existing JSON lake, first let any active writer finish, then run:

```bash
PYTHONPATH=src .venv/bin/python scripts/migrate_lake_to_parquet.py
```

The migration validates the Gold fingerprints before archiving legacy JSON
rows below `/home/dev_xetra/.xdl-lake-json-backups/`. It does not remove legacy
versioned Parquet generations; cleanup of those recovery copies is a separate
storage-retention decision.

## Current scope

`BACKLOG.md` is a historical archive; it contains no active work orders. The
implemented Python pipeline owns EODHD transport, XETRA listings and corporate
actions, Bronze/Silver/Gold datasets, transactional PostgreSQL publication,
restart-safe orchestration, and acceptance verification.

The feature contract is `xetra_loader.xetra_features`, a materialized view
with only `adjusted_close_level` and `volume_level` as base levels. OHLC
columns are internal inputs only for the approved high-low range, intraday
return, and overnight-gap families. Fold-local standardization is a
downstream operation documented in
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

The current catalog version is **4** and defines **98 derived features**.
The catalog names and formulas are tested against an independent calculation
reference and PostgreSQL integration fixtures. The complete view column count
is recorded once in the PostgreSQL inventory below.

## Feature benchmark

Run `scripts/quality/benchmark_features.py` to compare one global Polars window
plan with per-instrument Polars plans on the same synthetic input. The script
checks both implementations against each other and against the independent
feature reference before reporting median timings. The reproducible baseline
uses 100,000 rows, 100 instruments, five timed rounds, and a six-core budget;
its command is listed with the other local quality gates below.

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
