# xetra-loader — Project Goals and Rebuild Specification

This document describes the complete intended system in implementation-ready
terms for maintainers and coding agents. It complements `lake/GOALS.md`; it
does not replace that product-goals file. Treat source code, SQL, tests, and
focused runbooks as evidence of the current implementation. When this document
and executable contracts disagree, identify the discrepancy and update the
appropriate source and tests rather than silently choosing one.

## Contents

- [Mission and invariants](#mission-and-invariants)
- [Architecture](#architecture)
- [Data contracts and ingestion](#data-contracts-and-ingestion)
- [Medallion lake](#medallion-lake)
- [PostgreSQL serving plane](#postgresql-serving-plane)
- [Feature materialization](#feature-materialization)
- [Synchronization and recovery](#synchronization-and-recovery)
- [API and durable jobs](#api-and-durable-jobs)
- [Scheduling and deployment](#scheduling-and-deployment)
- [Configuration, security, and logging](#configuration-security-and-logging)
- [Performance requirements](#performance-requirements)
- [Implementation map](#implementation-map)
- [Agent implementation workflow](#agent-implementation-workflow)
- [Acceptance criteria](#acceptance-criteria)

## Mission and invariants

Build a deterministic XETRA market-data pipeline that retrieves listings,
end-of-day (EOD) quotes, dividends, and splits from EODHD; validates and
normalizes the source; writes canonical Bronze/Silver/Gold (BSG) Parquet data;
publishes the complete authoritative state transactionally into PostgreSQL;
and exposes a persistent, causal feature materialization for downstream
portfolio analytics.

The project has one provider integration, one canonical Python pipeline, one
Parquet lake, and one PostgreSQL serving database. Do not introduce a second
download/compute daemon, browser stack, or competing source of truth.

Non-negotiable invariants:

- Identity is `(isin, exchange, code)`; quote identity adds `trade_date`, and
  corporate-action identity adds a deterministic content-addressed `event_key`.
- Provider corrections to any historical observation must replace the old
  semantic value in the lake and PostgreSQL. The system must not assume that
  provider history is immutable.
- The weekly path checks complete provider history for active listings, then
  publishes only semantic changes. A zero-change run is an idempotent no-op.
- Delisted listings and their retained histories remain available. Absence
  from the active listing response is not permission to erase historical data.
- All row-bearing lake files are Parquet written/read with Polars. JSON is for
  manifests, checkpoints, API payloads, and other small control metadata only.
- PostgreSQL owns durable serving state and the API job queue. PostgreSQL
  publication and its synchronization state advance in the same transaction.
- Feature values are causal: no observation may use future data. The feature
  materialization exposes only `adjusted_close_level` and `volume_level` as
  base levels; raw OHLC levels and pairwise/average-correlation features are
  forbidden.
- Never expose credentials in code, documentation, test output, logs, or Git.
- Preserve a working production schema while building its replacement. A full
  refresh must stage, validate, atomically promote, and only then retire old
  objects.

## Architecture

```text
EODHD REST
   │ measured requests, retries, UTC timestamps
   ▼
provider contracts → Bronze → Silver → Gold → cross-dataset validation
                                                   │
                                                   ▼
                              transactional PostgreSQL raw tables
                                                   │
                                                   ▼
                       xetra_loader.xetra_features materialized view

Sunday full-refresh wrapper ──> staged complete rebuild ──> acceptance ──> cleanup
Optional REST submission ──> PostgreSQL durable job ──LISTEN/NOTIFY──> API worker
```

The weekly stage order is fixed and fail-closed:

1. fetch/normalize listings;
2. reconcile dividend history;
3. reconcile split history;
4. reconcile quote history;
5. validate the complete Gold snapshot across datasets;
6. publish listings, quotes, dividends, and splits;
7. verify PostgreSQL counts, keys, dates, fingerprints, and synchronization
   state.

A failure stops all downstream stages. Restart checkpoints record completed
stages and permit safe rehydration; use a process lock so weekly and full
rewrites cannot mutate the same lake concurrently.

## Data contracts and ingestion

### Listings

- Retrieve EODHD active and delisted XETRA listing feeds.
- Normalize identity text, uppercase ISIN and exchange, preserve provider
  metadata (`name`, instrument type, currency, country), and record `is_active`.
- Missing/empty ISIN rows are excluded; missing required exchange/code and
  conflicting duplicate identities fail closed. Exact duplicate semantic rows
  may be deduplicated deterministically.
- Active feed wins if an identity appears in both feeds. Previously known
  identities absent from both current feeds are retained as inactive.

### EOD quotes

- Preserve source OHLCV fields, adjusted close, and EOD business date. Prices
  use decimal-safe values; reject non-finite or negative numeric values and
  invalid OHLC ordering. Volume is a non-negative integer or null.
- `timestamp_eod` is a UTC midnight date anchor, not a claimed exchange-close
  timestamp. Source fetch and publication timestamps are separate UTC audit
  fields and are excluded from semantic identity/fingerprints.
- For weekly reconciliation, fetch complete history for active instruments so
  arbitrary historical provider revisions can be detected. Compare semantic
  records with the prior lake snapshot and publish only inserts, updates, and
  retractions that are actually needed. Keep inactive instruments' prior
  history unless provider evidence authoritatively changes it.
- Incremental provider endpoints may be used only when paired with an explicit
  correction/reconciliation policy that still detects revisions outside the
  overlap. A short overlap alone cannot guarantee complete historical
  correction capture.

### Dividends and splits

- Normalize events into separate typed contracts. Use decimal-safe values and
  retain provider dates/metadata.
- Derive `event_key` as SHA-256 over canonical JSON containing event kind and
  stable business fields. Fetch/run metadata and lifecycle status are excluded
  from the key.
- Represent provider removals as retractions rather than silently forgetting
  them. Persist retraction keys in deterministic Gold sidecars so full-state
  validation and PostgreSQL reconciliation agree.
- An action correction can require quote-history re-fetch/reconciliation for
  the affected listing when adjusted source history may have changed.

### Determinism and validation

- Canonical serialization uses sorted keys, stable ordering, compact JSON, and
  canonical decimal text; do not fingerprint binary floating-point renderings.
- Gold artifacts contain stable semantic rows, row count, dataset identity,
  feature/source hashes, and semantic fingerprint in their manifests.
- Validate uniqueness, schema, domain constraints, and cross-dataset identity
  relationships before destructive reconciliation. Incomplete or internally
  inconsistent Gold input must never authorize pruning PostgreSQL rows.

## Medallion lake

Root is configured by `XDL_MEDALLION_ROOT` or `medallion.root` (normally
`lake/`). Keep exactly one canonical current artifact per partition; avoid
duplicate/orphan row-bearing content.

- Bronze monthly partition:
  `bronze/dataset_type={dataset_type}/exchange={exchange}/instrument_type={instrument_type}/symbol={symbol}/timeframe={timeframe}/year={YYYY}/month={YYYY-MM}/data.parquet`
- Silver annual partition:
  `silver/dataset_type={dataset_type}/exchange={exchange}/symbol={symbol}/timeframe={timeframe}/year={YYYY}/{symbol}-{YYYY}.parquet`
- Gold symbol snapshot:
  `gold/dataset_id={dataset_id}/dataset_type=gold_symbol_dataset/exchange={exchange}/symbol={symbol}/{symbol}_GOLD_{feature_hash}_{source_hash}.parquet`

Dataset-specific partition metadata and field names are defined in the
contracts and medallion writer. `dataset_id` is the stable identity stored on
the parent Gold partition; it is not an additional directory below a Parquet
file. Manifests identify canonical artifacts. Retractions and restart state
must also be deterministic and recoverable. The legacy JSON/versioned lake is
read only by the explicit migration tool; the runtime reader must not silently
fall back to retired storage layouts.

For a lake migration, hold the authoritative rewrite and weekly locks; stage
one source partition at a time outside the live lake; validate Parquet schemas,
manifest ownership, row counts, and Gold fingerprints; atomically publish
canonical manifests; then move retired artifacts to a timestamped recovery
backup. Never bulk-materialize the complete lake into Python row objects.

## PostgreSQL serving plane

Target database is the dedicated `xetra_loader` database. The loader owns the
`xetra_loader` serving schema and `xetra_loader_sync` operational schema.

### Raw serving tables

- `xetra_loader.listings`: primary key `(isin, exchange, code)`; provider
  metadata, lifecycle `is_active`, fetch/publication audit timestamps, and
  generated `available_at_utc`.
- `xetra_loader.eod_quotes`: primary key `(isin, exchange, code, trade_date)`;
  foreign key to listings; raw `open`, `high`, `low`, `close`,
  `adjusted_close`, `volume`; UTC `timestamp_eod`, fetch/publication audit
  timestamps, and generated `available_at_utc`.
- `xetra_loader.dividends`: primary key `(isin, exchange, code, event_key)`;
  event value/currency/period and declaration/record/payment dates, lifecycle
  status in the publication contract, audit timestamps, and generated
  `available_at_utc`.
- `xetra_loader.splits`: primary key `(isin, exchange, code, event_key)`;
  split ratio/factor, audit timestamps, and generated `available_at_utc`.
- `xetra_loader.xetra_features`: persistent materialized feature view with
  exactly one row per `(isin, exchange, code, trade_date)` quote observation.

`available_at_utc` is generated from the data date: listings use the UTC date
of `published_at_utc`; quotes use `trade_date`; actions use `event_date`. It is
00:00 UTC on the next Monday–Friday. Friday advances to Monday. Exchange
holidays are intentionally not modeled. Consumers must not supply this value
on writes.

`xetra_loader_sync` contains:

- `sync_state`: one current semantic fingerprint and serving row count per
  dataset;
- `loader_runs`: immutable per-run status and mutation counters;
- `row_hashes`: per-entity SHA-256 digests used to detect row-level changes.

The durable API queue is `xetra_loader.loader_jobs`; a partial unique index
permits no more than one queued/running `weekly_sync` job. Queue updates and
worker notifications are managed by PostgreSQL functions/triggers.

### Transactional publication

For each dataset, calculate canonical fingerprint and row digests before
mutation. In one transaction: lock/read current state; reject incomplete
authoritative snapshots; insert/update/delete/retract rows; refresh the feature
view after quote mutations; verify the resulting table; update row hashes,
sync state, and run audit record. Rollback all of those changes together on any
error. Identical data must yield `noop` with zero mutations.

### Safe full replacement

The guarded full refresh must not empty active schemas before EODHD download or
view calculation finishes:

1. Back up loader-owned serving/sync schemas and Gold manifests under a private
   timestamped backup root; do not include credentials in the backup log.
2. Build complete replacement objects in `xetra_loader_build` and
   `xetra_loader_sync_build`; route all writer, verifier, and feature-refresh
   SQL to these schemas. Keep canonical schemas readable throughout build.
3. Run Gold completeness checks, publish every dataset, fully build/populate
   the materialized feature view and indexes, and verify keys/counts/fingerprints.
4. In one PostgreSQL transaction, rename old schemas to `*_previous` and build
   schemas to their canonical names. If promotion fails, transaction rollback
   keeps the old names intact. Initial installation with no active schemas is
   supported.
5. Independently verify the promoted target. On `PASS`, remove previous schemas
   without `CASCADE`; a dependent external object must block cleanup, not be
   silently deleted. On failure, preserve previous schemas for recovery.
6. Only after a verified report is written may backup retention prune older
   matching full-sync backups/checkpoints. Preserve the newest four of each;
   leave unrelated/malformed paths untouched.

Never restore a production backup, apply a production migration, launch a
full-sync, or modify the host scheduler without explicit task authorization.
Tests and verification must use a disposable PostgreSQL target, never the
production DSN.

## Feature materialization

The executable feature contract is versioned in
`src/xetra_loader/features/catalog.py`; SQL implementation is in
`sql/schema/004_xetra_features.sql`; independent semantic/reference checks are
under `tests/qa/` and `tests/integration/`. Keep these implementations in exact
agreement. Current contract: 98 derived columns plus two levels, five row
identity/date columns including `available_at_utc`, for **105 columns total**.

Only exposed levels are `adjusted_close_level` and `volume_level`. Never expose
`open_level`, `high_level`, `low_level`, or `close_level`. Raw OHLC may feed only
the approved derived families: high-low range, intraday return, overnight gap,
Stochastic, and Donchian. Do not add pairwise or average correlation columns;
`average_correlation_5obs`, `average_correlation_10obs`, and
`average_correlation_20obs` are expressly absent.

Required feature families and windows:

- adjusted-close log returns: 1, 3, 5, 10, 20 observations;
- rolling geometric/cumulative returns and arithmetic return means: 5, 10, 20;
- sample rolling volatility: 5, 10, 20, 40;
- OHLC high-low range, intraday return, and overnight gap: 1, 5;
- adjusted-close SMA ratios: 5/20, 10/20, 10/40;
- Wilder RSI: 7, 14;
- rate of change: 3, 5, 10, 20;
- drawdown: 20, 60;
- relative volume and volume z-score: 5, 10, 20;
- daily cross-sectional breadth/dispersion plus 5/10/20 market-date rolling
  means;
- compact feature aliases and indicator families documented by the v4 catalog
  and README: EMA/SMA, RSI ratios/slopes, stochastic, MACD, Donchian,
  volatility/level z-scores, return autocorrelation, drawdown, ROC acceleration,
  and `log_tick_volume`.

Use the exact column names and formulas in the v4 catalog and README's feature
table; do not infer names from this summary. All windows are trailing and
causal. Respect each feature's declared counting domain (valid prices, source
rows, derived inputs, returns, or market dates) and minimum count. Emit NULL
until sufficient valid input exists. Use sample standard deviation where
specified. RSI is Wilder-smoothed; a completely flat gain/loss window is
NULL. Returns are decimal fractions, not percent-scaled values. Standardization
for model training is train-fold-local and must never be fitted globally in the
materialized view.

The feature view refresh is one atomic PostgreSQL operation. A schema rebuild
must finish feature calculation and required identity indexes before promotion;
failed build leaves the previously served view/schema available. Keep refresh
functions safe with controlled `search_path`, bounded `work_mem`, temp-file
limits, and role grants.

## Synchronization and recovery

- `xdl-weekly` runs the restart-safe staged orchestrator. Persist a checkpoint
  after each successfully completed stage. Rehydration must read canonical
  manifests and continue only at the first incomplete stage.
- Use a cross-process lock under the lake root to prevent a second weekly or
  authoritative rewrite. Lock conflict is a clear blocked result, not silent
  concurrent execution.
- Weekly histories are provider-full-history checks with semantic-delta lake
  and PostgreSQL publication. The Sunday full-refresh wrapper is a complete
  staged rebuild, not a delta-only download.
- When a PostgreSQL transaction fails, its market rows, feature refresh,
  digests, state, and run log must roll back together. Restart replays safely.
- Gold manifests and recovery backups must be verifiable using SHA-256 before
  restore. Never choose or perform a restore implicitly.

## API and durable jobs

`xdl-api` hosts a small WSGI application and one in-process durable worker.
Routes:

- `GET /healthz`: report API/worker/database readiness without exposing DSNs;
- authenticated `POST /v1/jobs` with exactly `{"type":"weekly_sync"}`:
  enqueue one job, return 202; reject malformed/unsupported requests and return
  409 if one is already active;
- authenticated `GET /v1/jobs/{job_id}`: return status/result or a sanitized
  not-found/database-unavailable response.

Use constant-time bearer-token comparison, JSON content type, strict body-size
limits, no-store response headers, and sanitized errors. Store job history in
PostgreSQL. Worker acquires a PostgreSQL advisory lock; on startup, requeue
interrupted `running` work; claim with `FOR UPDATE SKIP LOCKED`; process
exactly one active job; mark terminal success/failure. Establish `LISTEN`
before scanning the queue, then block on `NOTIFY` while idle—never poll.
Shutdown notification wakes the listener and waits for an active run to finish.

The direct Sunday full-refresh schedule is currently the active canonical
schedule. Do not also enqueue a normal REST weekly job at the same time. API job
submission remains an optional explicit mechanism unless deployment policy
changes.

## Scheduling and deployment

- Cron artifact: `deploy/cron/xetra-loader.cron`.
- Sunday full refresh: 09:00 `Europe/Vienna`, invokes
  `scripts/run_full_refresh.sh`, which delegates to
  `xdl-verify-production-sync --execute-full-sync` with required destructive
  confirmation and reporting/backup paths.
- API service artifact: `deploy/systemd/xetra-loader-api.service`; installation
  and enablement are separate host-operator actions.
- Keep Gold lake copying or other host-specific schedules separate from the
  canonical loader schedule and document them as deployment state, not hidden
  application behavior. Do not alter host crontabs unless explicitly asked.

## Configuration, security, and logging

Read `config.yaml` (ignored) with environment overrides. `config.example.yaml`
documents field names without secrets. Supported secret/config categories:

- EODHD API token (`EODHD_API_TOKEN` or `eodhd.api_token`);
- least-privilege PostgreSQL writer DSN (`XDL_POSTGRES_WRITER_DSN` or
  `postgres.writer_dsn`);
- admin DSN for provisioning, schema migration, backup/reset, and controlled
  full sync only (`XDL_POSTGRES_ADMIN_DSN` or `postgres.admin_dsn`);
- database, host, port, local credentials, medallion root, API bearer token,
  core budget, feature work memory, and temp-file cap.

Weekly publication must reject a PostgreSQL superuser session. The writer role
`xetra_data_loader_writer` is a member of `xetra-data-loader`. The external
reader role `xetra_loader` receives `USAGE` and `SELECT` on
`xetra_loader.xetra_features` only; it must not modify serving tables or read
the sync schema. `portfell_app` receives read-only market data access according
to role SQL. Provision roles through the migration scripts; never place role
passwords in tracked SQL.

Every application entry point configures DEBUG file logs below `.logs/` (or
`XDL_LOG_DIR`): restrictive directory/file permissions, UTC daily rotation,
and age-based removal after 90 days. Cron wrappers write one private log per
run under `.logs/` and prune only their own run logs after 90 days. Log
structured stage/dataset/row/count,
request/retry/timing, mutation, and failure-type information. Never log API
tokens, passwords, DSNs, authorization headers, request URLs containing
secrets, or raw credentials from exceptions.

## Performance requirements

- Python is pinned to CPython **3.14.7**. Runtime dependencies are Polars,
  psycopg 3, and PyYAML; development gates are in `pyproject.toml`.
- A single bounded `--cores`/`XDL_CORES` CPU budget defaults to six (valid
  range 1–64). Set Polars' thread count before importing Polars. Parallelize
  independent provider fetches and eligible PostgreSQL plans without nested
  oversubscription.
- If parallel dividend/split publication is enabled, partition the database
  worker budget so combined query workers do not exceed the configured budget.
- Configure feature `work_mem` within 16MB–4GB (default 256MB), and per-process
  `temp_file_limit` within 1GB–64GB (default 32GB). Treat `work_mem` as a
  per-operation/per-worker bound, not a query-wide RAM cap.
- Prefer vectorized Polars and set-based PostgreSQL SQL. Benchmark representative
  workloads and compare outputs to an independent reference before accepting
  optimizations. Report measured scope and hardware; do not extrapolate
  microbenchmarks into guaranteed end-to-end runtimes.

## Implementation map

- `src/xetra_loader/config.py`, `logging_config.py`: validated configuration,
  connection settings, bounded resource budgets, safe logs.
- `contracts/`: provider-independent typed data contracts, identity,
  deterministic serialization and validation.
- `eodhd/transport.py`, `ingestion/`: HTTP behavior, retry/measurement, source
  normalization and correction windows.
- `gold/`: deterministic semantic datasets, fingerprints, completeness and
  cross-dataset validation.
- `medallion/core.py`: manifests, canonical Parquet IO, path layout,
  retractions, checkpoint/lake persistence.
- `pipeline/`: ordered stages, lock/checkpoint/restart, weekly stage factories.
- `sync/`: PostgreSQL connection policy, row-level digesting, transactional
  reconciliation for each serving dataset.
- `features/catalog.py`, `features/standardization.py`: feature schema,
  windows/formula metadata, fold-local model transforms.
- `api/`: REST routes, PostgreSQL job repository, durable listener worker,
  process runner.
- `ops/`: schema migration, staging/full bootstrap, backup/reset controls,
  target verification and acceptance report.
- `sql/schema/`, `sql/sync/`, `sql/roles/`: versioned PostgreSQL DDL and grants.
- `deploy/`: cron and systemd deployment artifacts; `scripts/`: guarded
  operational wrappers, lake migration, lint/type/test/benchmark helpers.
- `tests/unit/`: isolated contracts and failure/restart tests;
  `tests/integration/`: real PostgreSQL and complete-run checks; `tests/qa/`:
  independent feature semantics.

## Agent implementation workflow

1. Read this file, `lake/GOALS.md`, `README.md`, the relevant code, and focused
   runbooks before changing behavior. Inspect `git status` and preserve all
   pre-existing user work.
2. Treat `BACKLOG.md` as historical unless it explicitly contains an active
   work item. Keep unrelated changes out of scope.
3. Trace the contract across Python, SQL, lake manifests, API/deployment, docs,
   and tests. Make the smallest complete change; update all affected contracts
   and tests together.
4. Never inspect or print `config.yaml` values. Never use production credentials
   for tests. Do not run destructive sync/reset/restore or change host schedules
   without explicit user authorization.
5. Run the gates defined by `pyproject.toml` and focused tests after the
   implementation is complete. PostgreSQL tests require a disposable
   `XDL_TEST_POSTGRES_DSN`; report skips rather than claiming those tests ran.
6. Update focused documentation and acceptance evidence. Do not create,
   invoke, or depend on GitHub Actions. Do not commit, push, rebase, merge, or
   delete branches unless the user explicitly requests that exact operation.

## Acceptance criteria

A rebuild or material change is complete only when applicable items pass:

- [ ] Contract checks cover malformed, duplicate, corrected, missing, and
  delisted provider data; deterministic ordering/fingerprints are stable.
- [ ] Unit tests cover numeric precision, lifecycle/retraction, exact windows,
  null/minimum-count behavior, no-lookahead, restart and failure paths.
- [ ] Lake tests prove canonical Parquet layout, one manifest owner per current
  partition, no duplicate/orphan row data, and recoverable migration behavior.
- [ ] PostgreSQL integration tests prove DDL, FKs, constraints, generated
  `available_at_utc` (including Friday/Saturday-to-Monday), privileges,
  idempotent delta, historical correction, rollback, and exact view schema.
- [ ] The view has exactly 105 columns: four identity fields, availability,
  two levels, 98 catalogued derived columns; no forbidden OHLC levels or
  average-correlation columns.
- [ ] Feature output matches independent calculations, is causal, has expected
  NULL behavior, and refreshes after raw quote mutations in the same
  transaction.
- [ ] Full-refresh failure before promotion leaves canonical schemas intact;
  failed promotion rolls back; successful promotion exposes complete new
  schemas; failed external acceptance retains previous schemas; cleanup does
  not cascade into dependent objects.
- [ ] API tests cover authentication, validation, duplicate active job,
  notifications, interrupted-job recovery, singleton lock, idle blocking, and
  graceful shutdown.
- [ ] Local Ruff, strict Mypy, and pytest gates pass. PostgreSQL integration
  results identify any DSN-based skips. Benchmarks compare equivalent outputs.
- [ ] Documentation matches observed code and deployment; logs contain no
  credentials; no production mutation or Git publication occurred without
  explicit authorization.
