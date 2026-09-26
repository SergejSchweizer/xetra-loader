Last reviewed: 2026-09-26

# XETRA Data Loader — Atomic Parallel Backlog

## 0. New feature-serving program — XETRA levels and derived features

This section is the newest requested work package. It is additive to the
existing loader backlog and must not change the meaning of the existing Bronze,
Silver, Gold, or raw PostgreSQL datasets.

Target architecture:

```text
EODHD
  -> Bronze -> Silver -> Gold
  -> PostgreSQL raw tables
       xetra_loader.eod_quotes
       xetra_loader.listings
       xetra_loader.dividends
       xetra_loader.splits
  -> xetra_loader.xetra_features (MATERIALIZED VIEW)
```

Frozen feature rules:

- the PostgreSQL object is exactly `xetra_loader.xetra_features`;
- the view has one row per `(isin, exchange, code, trade_date)`;
- the only base level columns exposed by the view are `adjusted_close_level`
  and `volume_level`;
- `adjusted_close_level` is the canonical Gold/Raw `adjusted_close` value
  aliased with the macro-loader naming convention; it is not recalculated from
  `close`;
- `volume_level` is the canonical Gold/Raw `volume` value;
- the view contains no `open_level`, `high_level`, `low_level`, `close_level`,
  or any other OHLC level;
- `open`, `high`, `low`, and unadjusted `close` may be used only as internal
  SQL inputs for the explicitly approved high-low range, intraday-return, and
  overnight-gap features; none of them is exposed as a level column;
- every derived feature must be declared in a versioned catalog with its exact
  source fields, windows, null behavior, and leakage rules;
- price-return, rolling-return, SMA, RSI, ROC, drawdown, and price-volatility
  features use `adjusted_close_level`; volume features use `volume_level`;
- cross-sectional breadth, dispersion, and average-correlation features use
  only daily instrument returns derived from `adjusted_close_level`;
- the naming convention follows macro-loader: `<series>_level`,
  `<series>_log_level`, `<series>_delta_<n>obs`,
  `<series>_return_geom_<n>obs_pct`, `<series>_zscore_<n>obs`, and
  `<series>_momentum_autocorr_<lag>_<window>obs`;
- insufficient history, non-positive log inputs, zero denominators, and
  invalid numeric windows produce `NULL`, never fabricated values;
- all windows are partitioned by instrument and ordered by `trade_date`; no
  future observation may influence a feature;
- the view is refreshed only after a successful raw quote publication that
  changed any feature input: `open`, `high`, `low`, `close`,
  `adjusted_close`, or `volume`;
- an unchanged Gold replay performs no raw mutations, no sync-state mutation,
  and no materialized-view refresh.
- train-fold-local z-score standardization is not globally fitted in this
  materialized view; it is applied by a fold-aware query using training rows
  only, as specified by PR069, to prevent leakage into validation/test rows.
- the PostgreSQL user/role `xetra_loader` must have `USAGE` on the
  `xetra_loader` schema and `SELECT` on `xetra_loader.xetra_features`;
- `xetra_loader` receives no INSERT, UPDATE, DELETE, TRUNCATE, DDL, refresh,
  or `xetra_loader_sync` privilege; its password and LOGIN lifecycle are
  provisioned outside the repository and never committed.

The exact design mirrors the macro-loader's canonical Gold -> PostgreSQL raw
replica -> feature materialized view pattern. In macro-loader the feature
object is technically a `MATERIALIZED VIEW`, not an ordinary view; its SQL
definition and explicit refresh are the reference behavior.

### 0.1 New feature work-order dependency graph

```text
current main after existing PR058
              |
            PR059  feature catalog and semantic contract
              |
        +-----+-----+
        |           |
      PR060       PR061
  view/DDL      row-digest state
        |           |
        +-----+-----+
              |
            PR062  raw sync + refresh integration
              |
        +-----+-----+----------------+
        |           |                |
      PR063       PR064            PR065
  schema QA   calculation QA   delta/replay QA
        |           |                |
        +-----------+----------------+
                    |
                  PR066  cron/restart QA
                    |
                  PR067  complete-run QA gate
                    |
                  PR068  xetra_loader view-read grant
                    |
                  PR069  train-fold-local standardization
```

PR059 is the single semantic authority. PR060 and PR061 may proceed in
parallel after PR059. PR063–PR065 are independent QA PRs for the intermediate
implementation state. PR066 verifies the operational path. PR067 verifies the
complete run. PR068 is the privilege-specific gate for the `xetra_loader`
PostgreSQL user. PR069 is the final leakage-safety gate for model
standardization.

### PR059 — xdl-pr059-xetra-feature-contract

Branch `feat/xdl-pr059-xetra-feature-contract`; commit scope
`feat(xdl-pr059-xetra-feature-contract): ...`; depends on the current `main`
including the existing corrected contract work through PR058.

Owned paths: feature catalog/module, feature contract tests, and the relevant
feature documentation only. No PostgreSQL DDL and no cron changes.

Tasks:

- define the exact identity `(isin, exchange, code, trade_date)`;
- define the only exposed base levels: `adjusted_close_level` and
  `volume_level`;
- define the complete derived-feature catalog and exact formulas:
  - adjusted-close log returns for 1, 3, 5, 10, and 20 observations;
  - rolling cumulative returns for 5, 10, and 20 observations;
  - rolling return means for 5, 10, and 20 observations;
  - rolling volatility/std for 5, 10, 20, and 40 observations;
  - high-low range, intraday return, and overnight gap for 1 and 5 observations;
  - adjusted-close SMA ratios 5/20, 10/20, and 10/40;
  - adjusted-close RSI for 7 and 14 observations;
  - adjusted-close ROC for 3, 5, 10, and 20 observations;
  - adjusted-close drawdown for 20 and 60 observations;
  - relative volume and volume Z-score for 5, 10, and 20 observations;
  - daily and 5/10/20 rolling breadth, dispersion, and average correlation;
- define the formula contract explicitly:
  - `log_return_n = ln(adjusted_close_level / lag(adjusted_close_level, n))`;
  - rolling cumulative return is `exp(sum(log_return_1 over window)) - 1`;
  - rolling return mean is the arithmetic mean of `log_return_1`;
  - rolling volatility is the catalog-selected sample standard deviation of
    `log_return_1`;
  - high-low range is the rolling mean of `(high - low) / close`;
  - intraday return is the rolling mean of `(close - open) / open`;
  - overnight gap is the rolling mean of
    `(open - lag(close, 1)) / lag(close, 1)`;
  - SMA ratios divide the adjusted-close SMA numerator by the denominator;
  - RSI uses Wilder smoothing over adjusted-close gains and losses;
  - ROC is `adjusted_close_level / lag(adjusted_close_level, n) - 1`;
  - drawdown is `adjusted_close_level / rolling_max(adjusted_close_level, n) - 1`;
  - relative volume is `volume_level / rolling_mean(volume_level, n)` and
    volume Z-score is `(volume_level - rolling_mean) / rolling_std`;
  - breadth is the cross-sectional fraction of positive daily adjusted-close
    returns, dispersion is their cross-sectional standard deviation, and
    average correlation is the mean pairwise rolling correlation;
- define whether each rolling window includes the current observation and use
  that convention consistently in SQL and independent QA calculations;
- define null/zero/insufficient-history behavior;
- define the version and deterministic fingerprint of the catalog;
- permit raw `open`, `high`, `low`, and `close` only as internal inputs for the
  three explicitly approved intraday features; reject their exposure as level
  columns or use in any other feature family;
- define the exact calculation convention for every window, including
  population/sample standard deviation, Wilder RSI smoothing, rolling
  aggregation, and minimum observation count;
- define that raw `adjusted_close` is renamed to `adjusted_close_level`, never
  recalculated from another price field.

Acceptance:

- contract tests enumerate every allowed base and derived column;
- `adjusted_close_level` and `volume_level` are the only base levels;
- all requested windows and feature families are present exactly once;
- `open`, `high`, `low`, and unadjusted `close` occur only in the three
  approved internal intraday-feature definitions and never as level columns;
- formulas, windows, null rules, and version are deterministic and documented;
- a fixture with missing history returns `NULL` for the affected feature rather
  than a partial or forward-filled value;
- the catalog explicitly marks train-fold-local standardization as a
  fold-aware downstream operation and forbids global fit statistics;
- a semantic catalog change changes the feature fingerprint;
- this PR does not create tables, views, migrations, cron entries, or provider
  calls.

### PR060 — xdl-pr060-xetra-features-materialized-view

Branch `feat/xdl-pr060-xetra-features-materialized-view`; commit scope
`feat(xdl-pr060-xetra-features-materialized-view): ...`; depends on PR059.

Owned paths: feature PostgreSQL migration/DDL, view-refresh function if
needed, view contract integration tests, and feature grants. No ingestion or
provider changes.

Tasks:

- create exactly `xetra_loader.xetra_features` as a `MATERIALIZED VIEW`;
- source the view from canonical `xetra_loader.eod_quotes` only;
- project identity columns, `adjusted_close AS adjusted_close_level`, and
  `volume AS volume_level`;
- implement only the PR059 catalog formulas;
- use raw `open`, `high`, `low`, and `close` only inside the approved
  high-low-range, intraday-return, and overnight-gap expressions;
- never expose `open_level`, `high_level`, `low_level`, or `close_level`;
- keep adjusted-close and volume feature families independent from unadjusted
  OHLC inputs;
- add deterministic view version/fingerprint metadata;
- add the unique identity index required for safe concurrent refresh if that
  refresh mode is selected;
- grant `SELECT` to `portfell_app` and the runtime writer, while keeping DDL
  restricted to the migration/owner role;
- leave the dedicated `xetra_loader` user grant to PR068 so that its external
  login/password lifecycle remains separate from feature DDL.

Acceptance:

- empty-database migration creates the view with the exact expected object
  type, columns, order, and types;
- populated migration produces exactly one feature row per quote identity;
- `adjusted_close_level` equals raw `adjusted_close` and `volume_level` equals
  raw `volume` for every fixture row;
- view definition contains no OHLC level output and uses raw OHLC expressions
  only in the three catalog-approved intraday feature families;
- all derived columns match the PR059 catalog;
- all requested windows 1/3/5/10/20/40/60 and 7/14 are represented exactly
  where specified by the catalog;
- view refresh is idempotent and does not modify raw tables;
- `portfell_app` can select the view but cannot insert, update, delete, alter,
  drop, or refresh it;
- the `xetra_loader` user grant is not implicit and is verified by PR068;
- migration is repeatable and does not recreate or truncate the raw tables.

### PR061 — xdl-pr061-postgres-row-digest-state

Branch `feat/xdl-pr061-postgres-row-digest-state`; commit scope
`feat(xdl-pr061-postgres-row-digest-state): ...`; depends on PR059.

Owned paths: `xetra_loader_sync` row-digest schema, canonical key/digest
helpers, and focused sync-state tests. No feature SQL and no cron changes.

Tasks:

- add a deterministic row-digest state for listings, quotes, dividends, and
  splits;
- use canonical typed business identities, not provider order or fetch time;
- hash semantic source fields only; exclude `fetched_at_utc`,
  `published_at_utc`, and run IDs;
- support quote identity `(isin, exchange, code, trade_date)` and event
  identity `(isin, exchange, code, event_key)`;
- preserve the existing dataset-level `sync_state` as the publication
  checkpoint and add row-level hashes as its reconciliation index;
- fail closed when hashes exist without compatible sync state or contain
  duplicate/invalid identities.

Acceptance:

- semantically identical rows produce identical hashes independent of field or
  provider order;
- changing one semantic field changes exactly one row hash;
- changing only fetch/publication metadata changes no row hash;
- quote, listing, dividend, and split identities are collision-safe and
  round-trip through the persisted schema;
- duplicate identities, malformed hashes, and orphan hash state fail closed;
- first synchronization supports a full bootstrap; subsequent identical state
  produces a zero-change plan;
- no feature view or raw serving row is mutated by this PR alone.

### PR062 — xdl-pr062-xetra-delta-sync-and-feature-refresh

Branch `feat/xdl-pr062-xetra-delta-sync-and-feature-refresh`; commit scope
`feat(xdl-pr062-xetra-delta-sync-and-feature-refresh): ...`; depends on
PR060+PR061.

Owned paths: PostgreSQL sync integration, row-digest delta planner, refresh
orchestration, and sync integration tests. No new feature formulas.

Tasks:

- compare the complete current Gold state against row-digest state;
- apply only inserts, updates, and deletes to the raw serving tables;
- update row hashes and dataset sync state in the same transaction as raw
  mutations;
- acquire a dataset-scoped lock and preflight the schema before mutation;
- refresh `xetra_loader.xetra_features` only when quote rows changed in one of
  its feature inputs: `open`, `high`, `low`, `close`, `adjusted_close`, or
  `volume`;
- verify post-write row counts, key sets, hashes, and feature-view identity
  counts before commit;
- keep the Gold data lake authoritative and PostgreSQL as a serving replica.

Acceptance:

- first run performs a complete bootstrap and creates matching row hashes;
- one new, revised, and removed quote produces exactly one insert, update, and
  delete respectively;
- a listing/dividend/split-only change does not refresh the feature view;
- an `adjusted_close`, `volume`, `open`, `high`, `low`, or `close` change
  refreshes the feature view exactly once;
- unchanged replay produces zero raw mutations, zero hash mutations, zero
  sync-state advancement, and zero view refreshes;
- injected failure rolls back raw rows, row hashes, sync state, and view state;
- stale PostgreSQL rows absent from complete Gold are removed;
- schema/version/hash mismatch fails before any row mutation;
- transaction lock contention fails boundedly and leaves the prior state intact.

### PR063 — xdl-pr063-qa-xetra-feature-contract-and-schema

Branch `test/xdl-pr063-qa-xetra-feature-contract-and-schema`; commit scope
`test(xdl-pr063-qa-xetra-feature-contract-and-schema): ...`; depends on PR060.

Owned paths: feature schema QA tests and sanitized QA report only. No production
implementation changes.

Tasks: independently inspect PostgreSQL catalogs and the materialized-view
definition; validate identity, types, grants, version metadata, and forbidden
OHLC references.

Acceptance:

- object is exactly `xetra_loader.xetra_features` and is a materialized view;
- identity and level columns are exactly the PR059 contract;
- only `adjusted_close_level` and `volume_level` are base levels;
- no view column exposes `open_level`, `high_level`, `low_level`, or
  `close_level`;
- raw OHLC references occur only in the three explicitly approved intraday
  feature families;
- all derived columns are present exactly once and in deterministic order;
- `portfell_app` is SELECT-only;
- runtime writer has no DDL privilege;
- the `xetra_loader` user is verified as a separate view-reader role by PR068;
- report is sanitized and marks `PASS` only when every assertion succeeds.

### PR064 — xdl-pr064-qa-xetra-feature-calculations

Branch `test/xdl-pr064-qa-xetra-feature-calculations`; commit scope
`test(xdl-pr064-qa-xetra-feature-calculations): ...`; depends on PR060.

Owned paths: deterministic calculation fixtures, independent expected-value
calculator, and calculation QA report only.

Tasks: independently verify every PR059 feature family: log returns, rolling
cumulative returns, rolling return means, volatility/std, high-low range,
intraday return, overnight gap, SMA ratios, RSI, ROC, drawdown, relative
volume, volume Z-score, breadth, dispersion, average correlation, null behavior,
partitioning, ordering, and no-look-ahead semantics.

Acceptance:

- expected values are independently calculated rather than copied from the
  production SQL implementation;
- adjusted-close return/mean/volatility/SMA/RSI/ROC/drawdown features use only
  adjusted close values;
- volume features use only volume values;
- high-low, intraday-return, and overnight-gap features use the approved raw
  OHLC inputs and no other OHLC-derived feature does;
- breadth, dispersion, and average correlation are calculated cross-sectionally
  from daily adjusted-close returns with the exact daily and 5/10/20 windows;
- changing an irrelevant source field cannot change a feature, while changing
  an approved source field changes exactly the dependent feature families;
- every requested window (1/3/5/10/20/40/60 and RSI 7/14) is tested;
- insufficient history and invalid log/denominator cases return `NULL`;
- instrument A cannot affect instrument B;
- future rows cannot affect an earlier feature row;
- no global mean/std statistic is used for train-fold standardization;
- repeated refreshes produce byte/semantic-identical results;
- calculation report is sanitized and marked `PASS` only on complete success.

### PR065 — xdl-pr065-qa-xetra-delta-replay-and-rollback

Branch `test/xdl-pr065-qa-xetra-delta-replay-and-rollback`; commit scope
`test(xdl-pr065-qa-xetra-delta-replay-and-rollback): ...`; depends on PR061+PR062.

Owned paths: end-to-end delta/replay/rollback QA fixtures and report only.

Tasks: verify bootstrap, single-row delta, stale-row deletion, unchanged replay,
hash integrity, transaction rollback, and refresh gating across raw tables and
`xetra_features`.

Acceptance:

- complete Gold bootstrap reconciles raw tables and row hashes exactly;
- one adjusted-close change updates only its quote identity and refreshes the
  feature view;
- one volume change behaves the same;
- one open/high/low/close change refreshes the feature view and changes only the
  approved dependent intraday features;
- a change outside the feature inputs does not refresh the feature view;
- deleted Gold rows disappear from PostgreSQL;
- unchanged replay reports zero mutations and zero refreshes;
- a failure between raw mutation and view refresh leaves the previous committed
  state visible;
- rerun after failure converges to Gold without duplicate rows;
- QA report includes inserted/updated/deleted/unchanged/refresh counters.

### PR066 — xdl-pr066-qa-xetra-cron-and-restart

Branch `test/xdl-pr066-qa-xetra-cron-and-restart`; commit scope
`test(xdl-pr066-qa-xetra-cron-and-restart): ...`; depends on PR062+PR063+PR065.

Owned paths: cron/restart QA tests and operational report only.

Tasks: verify the deployed Sunday schedule, guarded weekly runner, lock,
checkpoint rehydration, incremental source requests, feature refresh ordering,
and absence of destructive bootstrap from the scheduled path.

Acceptance:

- cron is exactly `CRON_TZ=Europe/Vienna` plus `0 8 * * 0`;
- cron invokes the restart-safe weekly runner, not destructive bootstrap;
- two concurrent invocations cannot run publication simultaneously;
- process death at every persisted stage boundary resumes successfully;
- an unchanged weekly run uses delta/overlap requests and not a full provider
  download for already known data;
- a quote input change refreshes `xetra_features` after raw publication;
- an unchanged run does not refresh the view;
- failed Gold construction prevents PostgreSQL publication;
- failed PostgreSQL publication does not invalidate committed Gold;
- operational logs contain no provider token, password, full DSN, or raw secret;
- QA report records cron expression, stage order, restart result, request mode,
  and feature-refresh result.

### PR067 — xdl-pr067-qa-xetra-features-complete-run

Branch `chore/xdl-pr067-qa-xetra-features-complete-run`; commit scope
`chore(xdl-pr067-qa-xetra-features-complete-run): ...`; depends on PR063+PR064+PR065+PR066.

Owned paths: final full-run acceptance command, independent verifier, sanitized
acceptance artifact, and completion documentation only.

Tasks: execute a complete isolated XETRA run from Gold publication through raw
PostgreSQL synchronization, materialized-view refresh, verification, and the
scheduled runner path; do not introduce new business semantics.

Acceptance:

- full listing, quote, dividend, and split Gold datasets are committed and
  validated before PostgreSQL publication;
- raw PostgreSQL counts and business keys exactly match Gold;
- row hashes and dataset sync state exactly match Gold;
- `xetra_loader.xetra_features` contains exactly the expected instrument/date
  identities;
- only `adjusted_close_level`, `volume_level`, and catalog-approved derived
  features are exposed;
- no `open_level`, `high_level`, `low_level`, or `close_level` is exposed;
- all requested OHLC-derived features are limited to high-low range,
  intraday return, and overnight gap;
- independent calculation checks pass for representative short and long
  histories;
- all requested transformation families and windows are present and match the
  independent expected-value calculator;
- cron contract and restart-safe weekly runner checks pass;
- immediate unchanged complete replay produces zero raw mutations, zero hash
  mutations, zero feature refreshes, and zero semantic changes;
- permissions pass for `xetra-data-loader` and `portfell_app`;
- all timestamps and sessions satisfy the repository UTC contract;
- the sanitized acceptance artifact is `PASS` and contains no credentials,
  tokens, full DSNs, or raw provider payloads;
- all repository quality checks and `merge-gate` pass on the same head SHA;
- this PR may declare the complete-run core green, but the feature-serving
  program remains incomplete until PR069 passes.

### PR068 — xdl-pr068-xetra-loader-feature-view-read-grant

Branch `chore/xdl-pr068-xetra-loader-feature-view-read-grant`; commit scope
`chore(xdl-pr068-xetra-loader-feature-view-read-grant): ...`; depends on PR060+PR063+PR067.

Owned paths: one forward PostgreSQL privilege migration, privilege integration
tests, and sanitized permission acceptance output only. No password, DSN,
provider, feature formula, raw-table, or cron changes.

Tasks:

- require the externally provisioned PostgreSQL user/role `xetra_loader` to
  exist without creating or changing its password;
- grant `USAGE` on schema `xetra_loader` to the quoted role `xetra_loader`;
- grant `SELECT` on exactly `xetra_loader.xetra_features`;
- explicitly revoke mutation, refresh, DDL, and sync-schema privileges from
  `xetra_loader`;
- keep `portfell_app`, `xetra-data-loader`, and the runtime writer grants
  unchanged;
- make the migration idempotent and fail closed with a sanitized diagnostic if
  the externally managed user does not exist.

Acceptance:

- a real connection as `xetra_loader` can execute `SELECT` against
  `xetra_loader.xetra_features`;
- the same user cannot `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `ALTER`,
  `DROP`, or execute a materialized-view refresh;
- the same user cannot read, write, or use the `xetra_loader_sync` schema;
- the user cannot access passwords, full DSNs, provider tokens, or migration
  secrets through errors or reports;
- `portfell_app` remains SELECT-only and the loader writer retains its required
  publication rights;
- migration is repeatable and grants no privilege on any other raw table unless
  that privilege already existed in the prior role contract;
- a missing externally provisioned `xetra_loader` role fails before any grant
  or data mutation and produces a sanitized actionable error;
- privilege QA is executed in real PostgreSQL CI and is included in this PR's
  permission acceptance artifact.

### PR069 — xdl-pr069-train-fold-local-standardization

Branch `feat/xdl-pr069-train-fold-local-standardization`; commit scope
`feat(xdl-pr069-train-fold-local-standardization): ...`; depends on PR064+PR067+PR068.

Owned paths: fold-aware feature-standardization query/API, training-fold
contract, leakage tests, and sanitized QA output only. No changes to raw Gold
semantics, provider downloads, cron, or PostgreSQL user passwords.

Tasks:

- consume the approved derived columns from `xetra_loader.xetra_features`;
- require an explicit train-fold definition with train, validation, and test
  boundaries or row membership;
- fit each standardization mean and standard deviation on training rows only;
- apply the fitted training statistics to validation and test rows without
  refitting;
- return `NULL` for a zero training standard deviation rather than fabricate a
  standardized value;
- keep fold-local statistics out of the global `xetra_features` materialized
  view and out of Gold fingerprints;
- expose the fold-aware operation through a deterministic SQL query/function
  or equivalent documented training adapter, so consumers cannot accidentally
  perform global standardization.

Acceptance:

- a training fold produces statistics using training rows only;
- changing a validation or test value cannot change fitted training mean/std;
- changing a training value changes the applied validation/test standardization
  deterministically;
- no future observation or validation/test row participates in fitting;
- all catalog-approved derived feature families can be standardized through
  the fold-aware path;
- zero-variance training features return `NULL` and are reported;
- repeated execution with the same fold and source fingerprint is identical;
- the global `xetra_loader.xetra_features` view contains no fit statistics or
  fold-dependent values;
- a leakage probe fails the QA gate if any non-training row influences the
  fitted statistics;
- the sanitized report is `PASS` and contains no credentials or raw payloads;
- PR067's complete-run acceptance is not considered final until PR069 passes.

## 1. Status authority

This file is the complete implementation authority for `SergejSchweizer/xetra-loader`.

The former coarse loader plan `PR297`-`PR307` is superseded. It is replaced by repository-local work orders `XDL-PR001` through `XDL-PR033`, designed for multiple weak agents that must work independently with minimal merge conflicts.

Planning gate:

- work-order: `xdl-pr000-backlog-restructure`
- branch: `docs/xdl-pr000-backlog-restructure`
- required commit scope: `docs(xdl-pr000-backlog-restructure): ...`
- Git status: merged in `origin/main`

The implementation gate was satisfied by XDL-PR000 in `origin/main`.

## 2. Frozen architecture

```text
EODHD
  -> xetra-loader
       Bronze -> Silver -> Gold
       -> PostgreSQL 10.10.1.3:54321
            xetra_loader
            xetra_loader_sync
                 |
                 | SELECT only as portfell_app
                 v
              portfell
```

Frozen rules:

- initial universe: every EODHD XETRA listing with normalized non-empty ISIN;
- no ETF/UCITS/fund/type/country/currency prefilter;
- listing identity: `(isin, exchange, code)`;
- quote identity: `(isin, exchange, code, trade_date)`;
- dividend/split identity: `(isin, exchange, code, event_key)`;
- `event_key`: deterministic SHA-256 from normalized provider business fields only;
- all PostgreSQL timestamp columns: exactly `TIMESTAMPTZ(6)`;
- all PostgreSQL sessions: UTC;
- `trade_date` stays `DATE`;
- `timestamp_eod = trade_date 00:00:00+00:00`; it is not a physical exchange-close timestamp;
- incremental refresh overlap: seven calendar days;
- unchanged source replay must cause zero semantic PostgreSQL mutations;
- exact scheduler: `CRON_TZ=Europe/Vienna` and `0 8 * * 0`;
- passwords, provider tokens, and full DSNs are never committed;
- Portfell code, analytics, UI, users/tenants/projects, and authorization do not belong here;
- the project is not complete until a real full XETRA bootstrap has been completely synchronized to PostgreSQL `10.10.1.3:54321` and independently verified by XDL-PR033.

## 3. Git / branch / weak-agent contract

Every work order is self-contained. An agent may read dependencies but edits only its owned paths.

Mandatory rules:

1. Start from the exact merged dependency SHA.
2. Record `git status --short --branch` before edits and in the PR work log.
3. If any dependency is unmerged, stop; do not invent a workaround.
4. Never branch from a sibling work-order branch.
5. Parallel siblings start from the same predecessor merge SHA.
6. The exact work-order name must appear literally in branch name, every commit message, and PR title.
7. Every commit follows Conventional Commits.
8. Example:

```text
Work-order: xdl-pr015-eod-quote-ingestion
Branch:     feat/xdl-pr015-eod-quote-ingestion
Commit:     feat(xdl-pr015-eod-quote-ingestion): add deterministic quote ingestion
PR title:   feat(xdl-pr015-eod-quote-ingestion): add deterministic quote ingestion
```

9. Do not edit sibling-owned files, add compatibility shims, broaden scope, or perform opportunistic refactors.
10. Run focused tests plus all available repository quality gates on the same head SHA.
11. After XDL-PR006, `main` is protected and merge occurs only through required `merge-gate`; review-ready implementation PRs use auto-merge.

Python / quality baseline:

- CPython `3.14.7`;
- repository-local `.venv` built with Python 3.14.7 and never tracked;
- push and merge CI each run `lint`, `type`, `unit`, `integration` as four independent parallel jobs;
- separate fast `policy` job validates Conventional Commits and exact work-order naming;
- final `push-gate` / `merge-gate` aggregate all required checks.

## 4. Optimized dependency graph

```text
XDL-PR000
   |
 PR001
   |
 +---------+
 |         |
PR002    PR003
 |         |
 +----+----+
      |
 PR004 || PR005
      |
    PR006
      |
 +----+----------------------+--------------------+
 |                           |                    |
PR007                      PR009                PR013
 |                           |                    |
PR008               PR010 || PR011 || PR012      |
 |                    |       |       |           |
 +------ PR022        |       |       |           |
 |          |         |       |       |           |
 |        PR030     PR014   PR015   PR016 || PR017
 |                    |       |       |       |
 |                  PR018   PR019   PR020   PR021
 |                    |       |       |       |
 |                    +--+----+-------+-------+
 |                       |    |       |       |
 +--------------------> PR023 PR024  PR025   PR026
                          \     |      |      /
                           +----+------+-----+
                                |
                              PR027
                                |
                         PR028 || PR029
                                |
                       PR031 <- PR030
                                |
                              PR032
                                |
                              PR033
```

Interpretation: PR022 starts as soon as DB roles and medallion core exist; it does not wait for entity ingestion or Gold builders. Each entity then progresses independently through contract -> ingestion -> Gold -> PostgreSQL sync. PR033 is intentionally final and serial: it is the real target-PostgreSQL completion gate, not a fixture-only test.

Safe parallel waves:

- Wave 1: PR002 + PR003.
- Wave 2: PR004 + PR005.
- Wave 3: PR007 + PR009 + PR013.
- Wave 4: PR008 + PR010 + PR011 + PR012.
- Wave 5: PR014 + PR015 + PR016 + PR017; PR022 starts independently once PR008+PR009 are green.
- Wave 6: PR018 + PR019 + PR020 + PR021; PR030 can start after PR022.
- Wave 7: PR023 + PR024 + PR025 + PR026, each as soon as its own Gold builder plus PR022 are merged.
- Wave 8: PR028 + PR029 after PR027.
- Final serial gates: PR031 -> PR032 -> PR033.

## 5. Work-order index

| ID | Work-order | Branch | Depends on | Atomic result | Git status |
| --- | --- | --- | --- | --- | --- |
| PR001 | `xdl-pr001-python-repository-baseline` | `chore/xdl-pr001-python-repository-baseline` | PR000 | Python/.venv/minimal package skeleton | merged in `origin/main` |
| PR002 | `xdl-pr002-quality-command-contract` | `chore/xdl-pr002-quality-command-contract` | PR001 | canonical local quality commands | merged in `origin/main` |
| PR003 | `xdl-pr003-git-policy-validator` | `test/xdl-pr003-git-policy-validator` | PR001 | machine-enforced git/PR naming policy | merged in `origin/main` |
| PR004 | `xdl-pr004-push-quality-workflow` | `ci/xdl-pr004-push-quality-workflow` | PR002+PR003 | parallel push gate | merged in `origin/main` |
| PR005 | `xdl-pr005-merge-quality-workflow` | `ci/xdl-pr005-merge-quality-workflow` | PR002+PR003 | parallel merge gate | merged in `origin/main` |
| PR006 | `xdl-pr006-main-protection-automerge` | `chore/xdl-pr006-main-protection-automerge` | PR004+PR005 | protected main + required gate + auto-merge | merged in `origin/main` |
| PR007 | `xdl-pr007-postgres-market-schema` | `feat/xdl-pr007-postgres-market-schema` | PR006 | market DDL/DTO/timestamp contract | merged in `origin/main` |
| PR008 | `xdl-pr008-postgres-role-grants` | `feat/xdl-pr008-postgres-role-grants` | PR007 | writer/read-only grants | merged in `origin/main` |
| PR009 | `xdl-pr009-medallion-core-contract` | `feat/xdl-pr009-medallion-core-contract` | PR006 | medallion layout/manifest primitives | merged in `origin/main` |
| PR010 | `xdl-pr010-listing-dataset-contract` | `feat/xdl-pr010-listing-dataset-contract` | PR009 | listing dataset contract | merged in `origin/main` |
| PR011 | `xdl-pr011-quote-dataset-contract` | `feat/xdl-pr011-quote-dataset-contract` | PR009 | quote dataset contract | merged in `origin/main` |
| PR012 | `xdl-pr012-corporate-action-contract` | `feat/xdl-pr012-corporate-action-contract` | PR009 | dividend/split event contract | merged in `origin/main` |
| PR013 | `xdl-pr013-eodhd-transport` | `feat/xdl-pr013-eodhd-transport` | PR006 | provider HTTP/retry/rate-limit seam | merged in `origin/main` |
| PR014 | `xdl-pr014-xetra-listing-ingestion` | `feat/xdl-pr014-xetra-listing-ingestion` | PR010+PR013 | all XETRA non-empty-ISIN listings | merged in `origin/main` |
| PR015 | `xdl-pr015-eod-quote-ingestion` | `feat/xdl-pr015-eod-quote-ingestion` | PR011+PR013 | full/overlap quote ingestion | merged in `origin/main` |
| PR016 | `xdl-pr016-dividend-ingestion` | `feat/xdl-pr016-dividend-ingestion` | PR012+PR013 | full/overlap dividend ingestion | merged in `origin/main` |
| PR017 | `xdl-pr017-split-ingestion` | `feat/xdl-pr017-split-ingestion` | PR012+PR013 | full/overlap split ingestion | merged in `origin/main` |
| PR018 | `xdl-pr018-gold-listing-build` | `feat/xdl-pr018-gold-listing-build` | PR007+PR014 | validated listing Gold | merged in `origin/main` |
| PR019 | `xdl-pr019-gold-quote-build` | `feat/xdl-pr019-gold-quote-build` | PR007+PR015 | validated quote Gold | merged in `origin/main` |
| PR020 | `xdl-pr020-gold-dividend-build` | `feat/xdl-pr020-gold-dividend-build` | PR007+PR016 | validated dividend Gold | merged in `origin/main` |
| PR021 | `xdl-pr021-gold-split-build` | `feat/xdl-pr021-gold-split-build` | PR007+PR017 | validated split Gold | merged in `origin/main` |
| PR022 | `xdl-pr022-postgres-sync-core` | `feat/xdl-pr022-postgres-sync-core` | PR008+PR009 | transactional sync/state/fingerprint core | merged in `origin/main` |
| PR023 | `xdl-pr023-postgres-listing-sync` | `feat/xdl-pr023-postgres-listing-sync` | PR018+PR022 | idempotent listing publication | merged in `origin/main` |
| PR024 | `xdl-pr024-postgres-quote-sync` | `feat/xdl-pr024-postgres-quote-sync` | PR019+PR022 | idempotent quote publication | merged in `origin/main` |
| PR025 | `xdl-pr025-postgres-dividend-sync` | `feat/xdl-pr025-postgres-dividend-sync` | PR020+PR022 | idempotent dividend publication | merged in `origin/main` |
| PR026 | `xdl-pr026-postgres-split-sync` | `feat/xdl-pr026-postgres-split-sync` | PR021+PR022 | idempotent split publication | merged in `origin/main` |
| PR027 | `xdl-pr027-weekly-pipeline-orchestrator` | `feat/xdl-pr027-weekly-pipeline-orchestrator` | PR023-PR026 | ordered weekly command including verification | merged in `origin/main` |
| PR028 | `xdl-pr028-loader-lock-restart` | `feat/xdl-pr028-loader-lock-restart` | PR027 | non-overlap/restart-safe wrapper | merged in `origin/main` |
| PR029 | `xdl-pr029-sunday-1100-schedule` | `feat/xdl-pr029-sunday-1100-schedule` | PR027 | exact Sunday scheduler | merged in `origin/main` |
| PR030 | `xdl-pr030-destructive-reset-guard` | `feat/xdl-pr030-destructive-reset-guard` | PR009+PR022 | scoped confirmed reset primitive | merged in `origin/main` |
| PR031 | `xdl-pr031-full-xetra-bootstrap` | `feat/xdl-pr031-full-xetra-bootstrap` | PR027+PR030 | full-history bootstrap command | merged in `origin/main` |
| PR032 | `xdl-pr032-loader-e2e-gate` | `test/xdl-pr032-loader-e2e-gate` | PR028+PR029+PR031 | production-like fixture acceptance gate | merged in `origin/main` |
| PR033 | `xdl-pr033-production-postgres-full-sync-verification` | `chore/xdl-pr033-production-postgres-full-sync-verification` | PR032 | real full sync to target PostgreSQL + independent verification | implemented locally; real-target acceptance pending |

## 6. Exact atomic PR specifications

### PR001 — xdl-pr001-python-repository-baseline
Branch `chore/xdl-pr001-python-repository-baseline`; commit scope `chore(xdl-pr001-python-repository-baseline): ...`; depends on PR000.
Owned paths: `.python-version`, `.gitignore`, package metadata in `pyproject.toml`, minimal `src/xetra_loader/*`, test-root placeholders, README setup section.
Tasks: pin Python 3.14.7; create installable src layout; document creation/activation of `.venv`; ignore `.venv/`; create unit/integration roots.
Acceptance: `.venv` reports exactly Python 3.14.7; package installs/imports; no `.venv` files tracked; no provider/DB/business implementation.

### PR002 — xdl-pr002-quality-command-contract
Branch `chore/xdl-pr002-quality-command-contract`; commit scope `chore(xdl-pr002-quality-command-contract): ...`; depends on PR001.
Owned paths: quality-tool config and `scripts/quality/*` only.
Tasks: one lint, type, unit, integration command; unit and integration collection isolated; commands non-interactive and fail non-zero.
Acceptance: all four commands run independently; each collects only intended tests; no workflow YAML changed.

### PR003 — xdl-pr003-git-policy-validator
Branch `test/xdl-pr003-git-policy-validator`; commit scope `test(xdl-pr003-git-policy-validator): ...`; depends on PR001.
Owned paths: `scripts/ci/validate_git_policy.py`, policy unit tests/fixtures.
Tasks: validate Conventional Commits; exact `xdl-prNNN-*` in branch, every introduced commit, PR title.
Acceptance: valid examples pass; each malformed/missing case fails; validator is read-only.

### PR004 — xdl-pr004-push-quality-workflow
Branch `ci/xdl-pr004-push-quality-workflow`; commit scope `ci(xdl-pr004-push-quality-workflow): ...`; depends on PR002+PR003.
Owned path: `.github/workflows/push-quality.yml` only.
Tasks: non-main branch trigger; independent `lint`, `type`, `unit`, `integration`, `policy`; final `push-gate`; Python 3.14.7.
Acceptance: four code-quality jobs are parallel; `push-gate` cannot pass if any required job fails/cancels/skips unexpectedly.

### PR005 — xdl-pr005-merge-quality-workflow
Branch `ci/xdl-pr005-merge-quality-workflow`; commit scope `ci(xdl-pr005-merge-quality-workflow): ...`; depends on PR002+PR003.
Owned path: `.github/workflows/merge-quality.yml` only.
Tasks: PR-to-main trigger; independent `lint`, `type`, `unit`, `integration`, `policy`; final check exactly `merge-gate`; no bypass token.
Acceptance: four code-quality jobs are parallel; failing/cancelled required job blocks `merge-gate`.

### PR006 — xdl-pr006-main-protection-automerge
Branch `chore/xdl-pr006-main-protection-automerge`; commit scope `chore(xdl-pr006-main-protection-automerge): ...`; depends on PR004+PR005 and observed green workflow runs.
Owned scope: GitHub repository settings + `docs/repository-governance.md`.
Tasks: enable auto-merge; protect `main`; require PR and `merge-gate`; block direct feature pushes, force pushes, deletion; document auto-merge procedure.
Acceptance: GitHub reports protection active; representative PR cannot merge before required checks and auto-merges only after all requirements pass.

### PR007 — xdl-pr007-postgres-market-schema
Branch `feat/xdl-pr007-postgres-market-schema`; commit scope `feat(xdl-pr007-postgres-market-schema): ...`; depends on PR006.
Owned paths: `sql/schema/001_xetra_loader.sql`, typed market DTOs, schema tests.
Tasks: create `xetra_loader`; tables `listings`, `eod_quotes`, `dividends`, `splits`; frozen keys; exact `TIMESTAMPTZ(6)`; `trade_date DATE`; reject naive datetime DTOs.
Acceptance: DDL recreates on empty PostgreSQL; introspection/types/keys exact; duplicate keys and naive datetimes fail.

### PR008 — xdl-pr008-postgres-role-grants
Branch `feat/xdl-pr008-postgres-role-grants`; commit scope `feat(xdl-pr008-postgres-role-grants): ...`; depends on PR007.
Owned paths: role SQL + role integration test.
Tasks: `xetra-loader` writer; `portfell_app` SELECT-only market schema; deny app DML/DDL and loader-sync access.
Acceptance: required writer DML works; all forbidden app operations fail.

### PR009 — xdl-pr009-medallion-core-contract
Branch `feat/xdl-pr009-medallion-core-contract`; commit scope `feat(xdl-pr009-medallion-core-contract): ...`; depends on PR006.
Owned paths: medallion core/layout + tests.
Tasks: Bronze/Silver/Gold paths; manifests; semantic vs run metadata; deterministic serialization/fingerprint.
Acceptance: semantic fingerprint stable under run-metadata changes; invalid layer/path fails.

### PR010 — xdl-pr010-listing-dataset-contract
Branch `feat/xdl-pr010-listing-dataset-contract`; commit scope `feat(xdl-pr010-listing-dataset-contract): ...`; depends on PR009.
Owned paths: listing contracts/fixtures/tests.
Tasks: Bronze/Silver/Gold listing fields; normalized ISIN; `(isin,exchange,code)` key; deterministic ordering.
Acceptance: only empty/null ISIN excluded; duplicate ISIN with distinct code retained; round-trip deterministic.

### PR011 — xdl-pr011-quote-dataset-contract
Branch `feat/xdl-pr011-quote-dataset-contract`; commit scope `feat(xdl-pr011-quote-dataset-contract): ...`; depends on PR009.
Owned paths: quote contracts/fixtures/tests.
Tasks: quote layers/key; UTC midnight `timestamp_eod`; semantic fields; seven-day overlap boundary.
Acceptance: no physical close inferred; duplicate key fails; run metadata does not change semantics.

### PR012 — xdl-pr012-corporate-action-contract
Branch `feat/xdl-pr012-corporate-action-contract`; commit scope `feat(xdl-pr012-corporate-action-contract): ...`; depends on PR009.
Owned paths: corporate-action contracts/fixtures/tests.
Tasks: separate dividend/split schemas; deterministic `event_key`; correction/retraction representation.
Acceptance: same event same key; changed business field deterministically changes reconciliation; run metadata excluded.

### PR013 — xdl-pr013-eodhd-transport
Branch `feat/xdl-pr013-eodhd-transport`; commit scope `feat(xdl-pr013-eodhd-transport): ...`; depends on PR006.
Owned paths: EODHD transport/retry/rate-limit + tests.
Tasks: token from env; typed GET; timeout; bounded retry/backoff; rate-limit handling; fixture seam; secret scrubbing.
Acceptance: missing token clear; retries bounded; permanent error surfaced; logs leak no token/full secret URL.

### PR014 — xdl-pr014-xetra-listing-ingestion
Branch `feat/xdl-pr014-xetra-listing-ingestion`; commit scope `feat(xdl-pr014-xetra-listing-ingestion): ...`; depends on PR010+PR013.
Owned paths: listing ingestion + fixtures/tests.
Tasks: fetch XETRA exchange symbols; Bronze raw; normalize; retain every non-empty-ISIN identity; no instrument filters.
Acceptance: mixed fixture preserves every valid identity and excludes only missing ISIN; replay deterministic.

### PR015 — xdl-pr015-eod-quote-ingestion
Branch `feat/xdl-pr015-eod-quote-ingestion`; commit scope `feat(xdl-pr015-eod-quote-ingestion): ...`; depends on PR011+PR013.
Owned paths: quote ingestion + fixtures/tests.
Tasks: fetch by exchange/code; full history; incremental from last business date minus seven calendar days; Bronze/Silver; correction detection.
Acceptance: replay no semantic change; corrected row detected once; new date one new key; timestamps aware UTC.

### PR016 — xdl-pr016-dividend-ingestion
Branch `feat/xdl-pr016-dividend-ingestion`; commit scope `feat(xdl-pr016-dividend-ingestion): ...`; depends on PR012+PR013.
Owned paths: dividend ingestion + fixtures/tests.
Tasks: full history; seven-day overlap; normalize; Bronze/Silver; corrections/retractions.
Acceptance: replay stable; correction once; removed overlap event retracted; split files untouched.

### PR017 — xdl-pr017-split-ingestion
Branch `feat/xdl-pr017-split-ingestion`; commit scope `feat(xdl-pr017-split-ingestion): ...`; depends on PR012+PR013.
Owned paths: split ingestion + fixtures/tests.
Tasks: full history; seven-day overlap; normalize; Bronze/Silver; corrections/retractions.
Acceptance: replay stable; correction once; removed overlap event retracted; dividend files untouched.

### PR018 — xdl-pr018-gold-listing-build
Branch `feat/xdl-pr018-gold-listing-build`; commit scope `feat(xdl-pr018-gold-listing-build): ...`; depends on PR007+PR014.
Owned paths: listing Gold builder + tests.
Tasks: build listing Gold; match DDL/DTO; validate required fields/key; emit count/fingerprint/result.
Acceptance: direct load-compatible; duplicate/invalid key fails; semantic fingerprint deterministic.

### PR019 — xdl-pr019-gold-quote-build
Branch `feat/xdl-pr019-gold-quote-build`; commit scope `feat(xdl-pr019-gold-quote-build): ...`; depends on PR007+PR015.
Owned paths: quote Gold builder + tests.
Tasks: build quote Gold; key/timestamp validation; deterministic count/fingerprint.
Acceptance: direct load-compatible; duplicate or naive timestamp fails; replay fingerprint stable.

### PR020 — xdl-pr020-gold-dividend-build
Branch `feat/xdl-pr020-gold-dividend-build`; commit scope `feat(xdl-pr020-gold-dividend-build): ...`; depends on PR007+PR016.
Owned paths: dividend Gold builder + tests.
Tasks: enforce dividend key/event key; correction/retraction reconciliation; deterministic validation metadata.
Acceptance: direct load-compatible; invalid/duplicate event fails; exact expected reconciled state; split files untouched.

### PR021 — xdl-pr021-gold-split-build
Branch `feat/xdl-pr021-gold-split-build`; commit scope `feat(xdl-pr021-gold-split-build): ...`; depends on PR007+PR017.
Owned paths: split Gold builder + tests.
Tasks: enforce split key/event key; correction/retraction reconciliation; deterministic validation metadata.
Acceptance: direct load-compatible; invalid/duplicate event fails; exact expected reconciled state; dividend files untouched.

### PR022 — xdl-pr022-postgres-sync-core
Branch `feat/xdl-pr022-postgres-sync-core`; commit scope `feat(xdl-pr022-postgres-sync-core): ...`; depends on PR008+PR009.
Owned paths: loader-sync schema, generic sync core/state + tests.
Tasks: `xetra_loader_sync` state/run tables with `TIMESTAMPTZ(6)`; semantic fingerprint; transaction coupling data mutation and state advance; generic mutation counters; rollback proof.
Acceptance: injected failure changes neither serving data nor sync state; run metadata excluded from fingerprint; no entity-specific sync code.

### PR023 — xdl-pr023-postgres-listing-sync
Branch `feat/xdl-pr023-postgres-listing-sync`; commit scope `feat(xdl-pr023-postgres-listing-sync): ...`; depends on PR018+PR022.
Owned paths: listing sync + integration test.
Tasks: conflict-safe UPSERT; semantic comparison; transaction/state; insert/update/no-op counts.
Acceptance: first load exact; replay zero mutations; one change exactly one update; rollback clean.

### PR024 — xdl-pr024-postgres-quote-sync
Branch `feat/xdl-pr024-postgres-quote-sync`; commit scope `feat(xdl-pr024-postgres-quote-sync): ...`; depends on PR019+PR022.
Owned paths: quote sync + integration test.
Tasks: UPSERT on quote key; distinguish no-op/correction/new date; transactional state.
Acceptance: initial exact; replay zero; correction one update; new date one insert; rollback clean.

### PR025 — xdl-pr025-postgres-dividend-sync
Branch `feat/xdl-pr025-postgres-dividend-sync`; commit scope `feat(xdl-pr025-postgres-dividend-sync): ...`; depends on PR020+PR022.
Owned paths: dividend sync + integration test.
Tasks: transactional insert/correction/retraction reconciliation and exact counters.
Acceptance: initial exact; replay zero; correction/retraction only intended event; rollback clean.

### PR026 — xdl-pr026-postgres-split-sync
Branch `feat/xdl-pr026-postgres-split-sync`; commit scope `feat(xdl-pr026-postgres-split-sync): ...`; depends on PR021+PR022.
Owned paths: split sync + integration test.
Tasks: transactional insert/correction/retraction reconciliation and exact counters.
Acceptance: initial exact; replay zero; correction/retraction only intended event; rollback clean.

### PR027 — xdl-pr027-weekly-pipeline-orchestrator
Branch `feat/xdl-pr027-weekly-pipeline-orchestrator`; commit scope `feat(xdl-pr027-weekly-pipeline-orchestrator): ...`; depends on PR023+PR024+PR025+PR026.
Owned paths: pipeline/orchestration command + tests.
Tasks: exact order `listings -> quotes -> dividends -> splits -> Gold validation -> four PostgreSQL syncs -> verification`; stop on failure; structured summary; one non-interactive command.
Acceptance: exact order observable; failure blocks downstream stages; success summary covers every stage; no lock/cron code.

### PR028 — xdl-pr028-loader-lock-restart
Branch `feat/xdl-pr028-loader-lock-restart`; commit scope `feat(xdl-pr028-loader-lock-restart): ...`; depends on PR027.
Owned paths: lock/checkpoint/restart wrapper + tests.
Tasks: deny concurrent runs; restart checkpoints outside semantic identity; safe recovery/release.
Acceptance: second concurrent run denied; failed run recovers; restart produces no duplicate semantic DB mutations.

### PR029 — xdl-pr029-sunday-1100-schedule
Branch `feat/xdl-pr029-sunday-1100-schedule`; commit scope `feat(xdl-pr029-sunday-1100-schedule): ...`; depends on PR027.
Owned paths: cron/scheduler deployment config + tests.
Tasks: literal `CRON_TZ=Europe/Vienna`; literal `0 8 * * 0`; invoke the full guarded bootstrap; DST tests.
Acceptance: expression exact and remains Sunday 08:00 Vienna before/after DST; no pipeline business code changed.

### PR030 — xdl-pr030-destructive-reset-guard
Branch `feat/xdl-pr030-destructive-reset-guard`; commit scope `feat(xdl-pr030-destructive-reset-guard): ...`; depends on PR009+PR022.
Owned paths: destructive reset command + tests.
Tasks: enumerate loader-owned DB objects/medallion paths; dry run; require literal `--confirm-destructive-reset`; delete only owned state.
Acceptance: no confirmation = zero deletion; dry-run scope exact; unrelated schema/path survives.

### PR031 — xdl-pr031-full-xetra-bootstrap
Branch `feat/xdl-pr031-full-xetra-bootstrap`; commit scope `feat(xdl-pr031-full-xetra-bootstrap): ...`; depends on PR027+PR030.
Owned paths: bootstrap command + bootstrap tests.
Tasks: forward destructive confirmation; reset owned state; discover full XETRA non-empty-ISIN universe; fetch full available quotes/dividends/splits; build Gold; publish all four entities; verify counts/keys/date bounds/sync state; emit measured requests/retries/time/failures/rows.
Acceptance: absent confirmation zero mutation; clean fixture bootstrap reaches verified serving state; unchanged subsequent fixture run is semantic no-op; metrics measured, never guessed.

### PR032 — xdl-pr032-loader-e2e-gate
Branch `test/xdl-pr032-loader-e2e-gate`; commit scope `test(xdl-pr032-loader-e2e-gate): ...`; depends on PR028+PR029+PR031.
Owned paths: `tests/e2e/*`, acceptance report generator, fixture outputs only.
Tasks: empty-state fixture bootstrap; all valid listings; all histories; replay zero mutations; quote correction; dividend/split correction+retraction; new listing; timestamp/UTC introspection; read-only app role; lock; scheduler; machine-readable contract report.
Acceptance: all scenarios pass on one SHA; lint/type/unit/integration/policy/merge-gate green; no Portfell import; artifact sufficient for cross-repo contract tests.

### PR033 — xdl-pr033-production-postgres-full-sync-verification
Branch `chore/xdl-pr033-production-postgres-full-sync-verification`; commit scope `chore(xdl-pr033-production-postgres-full-sync-verification): ...`; depends on PR032 merged and green.

Atomic outcome: perform the real complete initial XETRA synchronization to PostgreSQL `10.10.1.3:54321` and independently prove that PostgreSQL exactly represents the validated Gold serving state. This is the mandatory loader completion gate.

Owned paths:

- `src/xetra_loader/ops/verify_postgres_sync.py` or equivalent read-only verification command;
- focused verification integration tests;
- `docs/acceptance/production-postgres-full-sync.md`;
- sanitized machine-readable acceptance report, e.g. `artifacts/acceptance/postgres-full-sync.json`;
- no provider, ingestion, Gold-builder, or entity-sync implementation changes.

Tasks:

1. Verify runtime target host/port resolves to exactly `10.10.1.3:54321`; credentials remain secret/env-only.
2. Execute the confirmed full bootstrap/sync using the production loader path: full current XETRA non-empty-ISIN universe plus full available quote, dividend, and split histories.
3. Require a successful committed loader run in `xetra_loader_sync`; partial or failed runs cannot count as completion.
4. After the committed sync, run an independent read-only verification against PostgreSQL rather than trusting only writer counters.
5. For `listings`, `eod_quotes`, `dividends`, and `splits`, compare Gold and PostgreSQL row counts and require exact equality.
6. Compare business keys in both directions (Gold minus PostgreSQL and PostgreSQL minus Gold) and require zero missing/extra keys.
7. Compare deterministic semantic fingerprints/aggregates for all four datasets and require equality; run/fetch metadata is excluded.
8. Assert zero duplicate business keys in PostgreSQL.
9. Assert zero orphan quote/dividend/split rows relative to `(isin,exchange,code)` listings.
10. Compare relevant minimum/maximum business-date/event-date bounds between Gold and PostgreSQL.
11. Introspect every PostgreSQL timestamp column and require exactly `TIMESTAMPTZ(6)`; require DB session timezone `UTC`.
12. Verify `portfell_app` can SELECT all four serving tables and cannot INSERT/UPDATE/DELETE/DDL or access `xetra_loader_sync`.
13. Immediately rerun against unchanged source/Gold state and require exactly zero semantic inserts, updates, retractions, or deletes across all four serving tables.
14. Emit a sanitized acceptance report containing target host/port, run ID, source/Gold/PostgreSQL row counts, key-difference counts, duplicate/orphan counts, date bounds, semantic fingerprints, timestamp/UTC checks, role checks, and no-op replay mutation counters. Never include passwords, tokens, full DSNs, or raw provider payloads.
15. Fail closed: any non-zero mismatch, missing table, unexpected row, duplicate, orphan, fingerprint mismatch, timestamp mismatch, privilege violation, failed run, or non-zero unchanged-replay mutation prevents completion.

Acceptance:

- a real full synchronization to `10.10.1.3:54321` finishes successfully for all four serving datasets;
- the PostgreSQL row count equals validated Gold row count for every serving table;
- symmetric business-key difference is zero for every serving table;
- semantic fingerprints match Gold for every serving table;
- duplicate-key count is zero;
- orphan count is zero;
- business/date bounds match the corresponding validated Gold bounds;
- all timestamp columns are exactly `TIMESTAMPTZ(6)` and the verified session timezone is UTC;
- `portfell_app` passes SELECT-only verification and all prohibited operations fail;
- immediate unchanged replay reports zero semantic mutations for listings, quotes, dividends, and splits;
- sanitized production acceptance report is generated and marked `PASS`;
- branch push gate and PR merge gate are green on the same head SHA;
- PR033 must not be merged and the loader must not be declared complete until the real target-PostgreSQL report is `PASS`.

## 7. Cross-repository handoff to Portfell

Portfell may begin read-contract implementation after XDL-PR007 freezes consumer DDL; permission-level integration waits for XDL-PR008.

Portfell's final cross-repository serving/cutover gate is blocked until **XDL-PR033 is merged and its real target-PostgreSQL full-sync acceptance report is PASS**. XDL-PR032 alone is fixture/production-like evidence and is not sufficient for final Portfell cutover.

Portfell may consume only the PostgreSQL contract, read-only-role contract, and sanitized acceptance artifacts. It must not import `xetra-loader`, call EODHD, read medallion files, or mutate loader schemas.

## 8. Mapping from superseded coarse loader plan

| Old plan | Atomic replacement |
| --- | --- |
| PR297 repository bootstrap/governance | XDL-PR001..PR006 |
| PR298 PostgreSQL serving contract | XDL-PR007..PR008 |
| PR299 medallion contracts | XDL-PR009..PR012 |
| PR300 listing ingestion | XDL-PR013 + PR014 |
| PR301 quote ingestion | XDL-PR011 + PR013 + PR015 |
| PR302 corporate actions | XDL-PR012 + PR013 + PR016 + PR017 |
| PR303 Gold serving build | XDL-PR018..PR021 |
| PR304 PostgreSQL sync | XDL-PR022..PR026 |
| PR305 weekly runner/schedule | XDL-PR027..PR029 |
| PR306 destructive bootstrap | XDL-PR030..PR031 |
| PR307 end-to-end/cutover gate | XDL-PR032 + XDL-PR033 |

Any old PR297-PR307 implementation branch is superseded and must not be merged as current authority.

## 9. Completion gate

`xetra-loader` is complete only when **XDL-PR001 through XDL-PR033** are merged from clean protected `main` and all conditions below hold:

- Python 3.14.7 `.venv` is reproducible and untracked;
- push/merge gates parallelize lint/type/unit/integration and policy validation enforces Conventional Commits plus exact work-order naming;
- `main` is protected with required `merge-gate` and gated auto-merge;
- the complete current XETRA non-empty-ISIN universe is discoverable;
- full available quote/dividend/split bootstrap succeeds;
- Gold validates all keys/types/references;
- PostgreSQL publication is transactional and idempotent;
- corrections/retractions are deterministic;
- unchanged replay is zero-mutation;
- timestamp contract is exactly `TIMESTAMPTZ(6)` + UTC;
- `portfell_app` is SELECT-only;
- Sunday schedule is exactly 08:00 Europe/Vienna;
- destructive reset is explicit and scoped;
- XDL-PR032 production-like E2E artifact is green;
- **a complete real synchronization has been executed against PostgreSQL `10.10.1.3:54321`;**
- **all four PostgreSQL serving tables have been independently reconciled to validated Gold with exact row counts, zero symmetric key differences, matching semantic fingerprints, zero duplicate keys, zero orphans, and matching date bounds;**
- **the immediate unchanged replay against the real target database produces zero semantic mutations;**
- **XDL-PR033's sanitized production PostgreSQL acceptance report is `PASS`.**

No fixture-only success, partial table sync, successful writer counters without independent reconciliation, or unverified production database state may be treated as project completion.

## 10. Corrective audit wave — reviewed 2026-08-24

### 10.1 Authority override and operational stop gate

This section is the newest authority. Where Sections 2, 4, 5, 6, 7, or 9 conflict with this section, **Section 10 supersedes them**. The audit found material drift between the stated contracts and `origin/main`, so XDL-PR033 is no longer a valid final completion gate by itself.

Until XDL-PR038 is merged and deployed, the repository cron entry must be treated as **unsafe for unattended production use** because it currently schedules the destructive full bootstrap instead of the restartable weekly incremental runner. Do not use a scheduled `xdl-bootstrap --confirm-destructive-reset` as the normal weekly path.

The canonical schedule is **Sunday 08:00 Europe/Vienna**, exactly `CRON_TZ=Europe/Vienna` plus `0 8 * * 0`, as explicitly selected for the deployed loader. Any differing time is contract drift and is superseded by XDL-PR037.

The project is now complete only after **XDL-PR034 through XDL-PR053** are finished and XDL-PR053 produces a new real-target PostgreSQL acceptance report marked `PASS`.

### 10.2 Audit findings mapped to corrective work

The corrective wave addresses these observed defects, ambiguities, or incomplete guarantees:

- GitHub currently reports `main` as unprotected and repository auto-merge as disabled although XDL-PR006 and the governance document claim the opposite; direct XDL-PR033 commits exist on `main` without a PR/merge-gate.
- the scheduler contract is contradictory: historical XDL-PR029 says 11:00, earlier backlog text says 12:00, current cron/tests say 08:00, and the committed E2E acceptance artifact still reports 12:00 while `origin/main` actually schedules 08:00;
- the current cron invokes `xdl-bootstrap --confirm-destructive-reset`, so every scheduled run can drop loader-owned PostgreSQL schemas and rebuild all data instead of doing a normal incremental refresh;
- the restart wrapper checkpoints only stage names, while the production pipeline keeps required listing/Gold/sync state only in memory; a new process cannot actually resume after a skipped completed stage;
- the production weekly runtime calls quote/dividend/split ingestion without the existing `last_*` and `previous_records` inputs, so it performs full-history requests rather than the frozen seven-calendar-day overlap refresh;
- dividend and split ingestion reject multiple events on the same date even though event identity is already content-addressed and same-date multiple corporate actions are valid data;
- adjusted-close history can change retroactively after corporate-action corrections, so a seven-day quote overlap alone is not a sufficient reconciliation rule;
- EODHD transport chains raw HTTP exceptions whose URL can contain `api_token`, so secret-safe traceback behavior is not proven;
- provider JSON floating-point values are decoded through binary floats before conversion to `Decimal`, numeric canonicalization is representation-sensitive, and fractional/non-finite provider numerics are not rejected consistently;
- Gold validation does not prove that every quote/dividend/split identity exists in listing Gold; the runtime `gold_validation` stage currently reports counts only;
- corporate-action Gold fingerprints include retracted keys, but the persisted Gold `data.json` contains only active rows, so the disk artifact cannot independently reproduce that fingerprint or rehydrate all tombstones;
- medallion series are accumulated in memory and the whole growing Bronze/Silver dataset is rewritten after each listing, creating avoidable O(N²) write amplification during a full universe load;
- EODHD's exchange-symbol endpoint distinguishes active and delisted listings, but the repository currently requests only the default active set while the backlog alternates between “all XETRA listings” and “current universe” language;
- listing and quote PostgreSQL syncs upsert present rows but do not remove stale serving rows absent from the authoritative merged Gold state;
- CI's PostgreSQL integration tests are conditional on `XDL_TEST_POSTGRES_DSN`, while the GitHub Actions integration jobs provision no PostgreSQL service or DSN, so real database tests can be skipped while `merge-gate` still passes;
- normal runtime configuration points at an administrative PostgreSQL connection even though a least-privilege writer role exists; the writer role also has unnecessary `CREATE` on the sync schema after provisioning;
- serving `fetched_at_utc` is currently populated with the publication timestamp rather than an observed provider-fetch timestamp, making provenance names inaccurate;
- because numeric/event identity, listing-lifecycle, and serving reconciliation semantics will change in this corrective wave, the final PostgreSQL serving state must be rewritten and independently reverified instead of accepting the current database as authoritative.

### 10.3 Corrective dependency graph

```text
XDL-PR034
   |
   +---------------- PR035 ------------------+
   |                                         |
   +---------------- PR036 ------------------+
   |                                         |
   +--> PR037 -> PR038 -> PR039 -> PR040 ----+----> PR042
   |                                         |        ^
   +---------------- PR041 ------------------+--------+
   |
   +---------------- PR043 ------------------+
   |                                         |
   +---------------- PR044 ------------------+
   |                                         |
   +---------------- PR045 ------------------+
   |                                         |
   +------------ PR041+PR044 -> PR046 -> PR047
   |
   +------------ PR043+PR044 -> PR048 -> PR049
   |
   +------ PR040+PR045+PR046+PR049 -> PR050
   |
   +------------ PR035+PR036 -> PR051
   |
   +------------ PR040+PR051 -> PR052
   |
   +------------------------------------------------> PR053
```

Safe parallel start after PR034: PR035, PR036, PR037, PR041, PR043, PR044, and PR045. PR053 is deliberately final and serial.

### 10.4 Corrective work-order index

| ID | Work-order | Branch | Depends on | Atomic result | Status |
| --- | --- | --- | --- | --- | --- |
| PR034 | `xdl-pr034-audit-corrective-backlog` | `docs/xdl-pr034-audit-corrective-backlog` | current `main` | audited corrective authority | merged in `origin/main` |
| PR035 | `xdl-pr035-repository-governance-repair` | `chore/xdl-pr035-repository-governance-repair` | PR034 | protected-main/auto-merge restored and proven | merged in `origin/main` |
| PR036 | `xdl-pr036-real-postgres-ci` | `ci/xdl-pr036-real-postgres-ci` | PR034 | real PostgreSQL integration tests mandatory in CI | merged in `origin/main` |
| PR037 | `xdl-pr037-scheduler-contract-reconciliation` | `fix/xdl-pr037-scheduler-contract-reconciliation` | PR034 | one exact Sunday 08:00 contract everywhere | merged in `origin/main` |
| PR038 | `xdl-pr038-production-weekly-runner-wiring` | `fix/xdl-pr038-production-weekly-runner-wiring` | PR037 | scheduled path is guarded weekly runner, never destructive bootstrap | merged in `origin/main` |
| PR039 | `xdl-pr039-restart-state-rehydration` | `fix/xdl-pr039-restart-state-rehydration` | PR038 | real cross-process restart from persisted state | merged in `origin/main` |
| PR040 | `xdl-pr040-incremental-weekly-refresh` | `fix/xdl-pr040-incremental-weekly-refresh` | PR039 | seven-day merged weekly refresh actually used | merged in `origin/main` |
| PR041 | `xdl-pr041-corporate-action-set-reconciliation` | `fix/xdl-pr041-corporate-action-set-reconciliation` | PR034 | same-date multiple corporate actions supported | merged in `origin/main` |
| PR042 | `xdl-pr042-adjusted-close-retroactive-reconciliation` | `fix/xdl-pr042-adjusted-close-retroactive-reconciliation` | PR040+PR041 | corporate-action changes trigger affected full quote reconciliation | merged in `origin/main` |
| PR043 | `xdl-pr043-provider-secret-safe-errors` | `fix/xdl-pr043-provider-secret-safe-errors` | PR034 | provider tokens cannot leak through tracebacks/errors | merged in `origin/main` |
| PR044 | `xdl-pr044-provider-numeric-integrity` | `fix/xdl-pr044-provider-numeric-integrity` | PR034 | exact Decimal/canonical numeric semantics and validation | merged in `origin/main` |
| PR045 | `xdl-pr045-gold-cross-dataset-validation` | `fix/xdl-pr045-gold-cross-dataset-validation` | PR034 | Gold proves child-to-listing referential integrity | merged in `origin/main` |
| PR046 | `xdl-pr046-corporate-action-gold-tombstones` | `fix/xdl-pr046-corporate-action-gold-tombstones` | PR041+PR044 | persisted Gold fully represents retractions | merged in `origin/main` |
| PR047 | `xdl-pr047-atomic-streamed-medallion-persistence` | `refactor/xdl-pr047-atomic-streamed-medallion-persistence` | PR046 | bounded-memory, atomic medallion writes | merged in `origin/main` |
| PR048 | `xdl-pr048-listing-lifecycle-contract` | `feat/xdl-pr048-listing-lifecycle-contract` | PR043+PR044 | active+delisted XETRA lifecycle is explicit | merged in `origin/main` |
| PR049 | `xdl-pr049-listing-lifecycle-postgres-migration` | `feat/xdl-pr049-listing-lifecycle-postgres-migration` | PR048 | lifecycle field propagated through Gold/PostgreSQL | merged in `origin/main` |
| PR050 | `xdl-pr050-authoritative-postgres-reconciliation` | `fix/xdl-pr050-authoritative-postgres-reconciliation` | PR040+PR045+PR046+PR049 | PostgreSQL exactly reconciles to full merged Gold | merged in `origin/main` |
| PR051 | `xdl-pr051-runtime-role-hardening` | `fix/xdl-pr051-runtime-role-hardening` | PR035+PR036 | weekly runtime uses non-superuser writer permissions | merged in `origin/main` |
| PR052 | `xdl-pr052-fetch-publication-provenance` | `fix/xdl-pr052-fetch-publication-provenance` | PR040+PR051 | fetch and publication timestamps have exact meanings | merged in `origin/main` |
| PR053 | `xdl-pr053-postgres-authoritative-rewrite` | `chore/xdl-pr053-postgres-authoritative-rewrite` | PR035-PR052 | backup, full rewrite, independent real-target PASS | complete; sanitized real-target V2 report is PASS |

### 10.5 Exact corrective PR specifications

#### PR034 — xdl-pr034-audit-corrective-backlog

Branch `docs/xdl-pr034-audit-corrective-backlog`; commit scope `docs(xdl-pr034-audit-corrective-backlog): ...`; depends on the audited `origin/main` head.

Owned path: `BACKLOG.md` only.

Tasks: record only evidenced defects/ambiguities; add PR035-PR053 with exact dependencies/ownership/acceptance; explicitly supersede the old final completion claim; require a controlled PostgreSQL rewrite after semantic/schema fixes.

Acceptance: no production implementation changes; every finding has a corrective owner; no two sibling PRs own the same primary implementation path unless a dependency orders them.

#### PR035 — xdl-pr035-repository-governance-repair

Branch `chore/xdl-pr035-repository-governance-repair`; commit scope `chore(xdl-pr035-repository-governance-repair): ...`; depends on PR034.

Owned scope: GitHub repository settings plus `docs/repository-governance.md` verification evidence only.

Tasks: enable repository auto-merge; protect `main`; require pull requests and the exact `merge-gate`; require checks on the current head; block direct pushes, force pushes, and branch deletion; document the observed pre-fix state and post-fix state.

Acceptance: GitHub API reports `main.protected=true`; required status context includes exactly `merge-gate`; direct push to `main` is rejected; auto-merge is enabled; a representative PR cannot merge while the gate is pending/failing and can merge only after required checks pass.

#### PR036 — xdl-pr036-real-postgres-ci

Branch `ci/xdl-pr036-real-postgres-ci`; commit scope `ci(xdl-pr036-real-postgres-ci): ...`; depends on PR034.

Owned paths: `.github/workflows/push-quality.yml`, `.github/workflows/merge-quality.yml`, CI-only PostgreSQL test bootstrap, focused CI contract tests.

Tasks: provision an isolated PostgreSQL service in both integration jobs; set `XDL_TEST_POSTGRES_DSN` to that service; initialize the schemas/roles needed by tests; make the CI path fail if the real PostgreSQL suite is not configured or is skipped; keep lint/type/unit/policy parallel.

Acceptance: `test_market_schema_postgres.py`, sync-core, entity-sync, role, and production-verifier integration tests all execute against the service rather than report `SKIPPED`; deliberately breaking SQL makes `integration` and therefore `merge-gate` fail; local developer runs may still skip when no explicit test DSN is supplied.

#### PR037 — xdl-pr037-scheduler-contract-reconciliation

Branch `fix/xdl-pr037-scheduler-contract-reconciliation`; commit scope `fix(xdl-pr037-scheduler-contract-reconciliation): ...`; depends on PR034.

Owned paths: `deploy/cron/xetra-loader.cron`, `tests/unit/test_sunday_schedule.py`, scheduler-only E2E assertions, `src/xetra_loader/ops/acceptance.py`, committed fixture acceptance artifact, scheduler statements in README/BACKLOG/docs.

Tasks: enforce the selected schedule `CRON_TZ=Europe/Vienna` and `0 8 * * 0`; remove contradictory hard-coded times; derive/report the observed cron expression in acceptance code instead of passing a separate inconsistent constant.

Acceptance: actual cron, unit test, E2E check, acceptance object, committed acceptance JSON, and documentation all say Sunday 08:00; winter/summer DST checks both preserve local 08:00; changing only the cron expression makes acceptance fail.

#### PR038 — xdl-pr038-production-weekly-runner-wiring

Branch `fix/xdl-pr038-production-weekly-runner-wiring`; commit scope `fix(xdl-pr038-production-weekly-runner-wiring): ...`; depends on PR037.

Owned paths: production runner CLI wiring, `pyproject.toml` script entry, cron command portion, runner tests only.

Tasks: expose one non-interactive guarded weekly entry point around `run_restartable_pipeline`; derive lock/checkpoint paths from `XDL_MEDALLION_ROOT`; make cron invoke that guarded weekly runner and concrete production stage factory; keep `xdl-bootstrap --confirm-destructive-reset` manual-only and absent from every recurring scheduler.

Acceptance: scheduled command contains no destructive-reset flag; concurrent scheduled runs are rejected; normal weekly execution cannot drop PostgreSQL schemas or delete medallion layers; the bootstrap CLI still requires explicit manual confirmation.

#### PR039 — xdl-pr039-restart-state-rehydration

Branch `fix/xdl-pr039-restart-state-rehydration`; commit scope `fix(xdl-pr039-restart-state-rehydration): ...`; depends on PR038.

Owned paths: `src/xetra_loader/pipeline/restart.py`, persisted restart-state codec/loader, focused restart tests.

Tasks: make restart data sufficient for a new process, not just the same in-memory `PipelineStages`; rehydrate listing/quote/dividend/split Gold state from persisted medallion artifacts; persist/recover required sync run IDs/fingerprints/counters as non-semantic checkpoint metadata; validate that completed stage names form a prefix and that every required artifact matches the checkpoint fingerprint before skipping.

Acceptance: simulate process death after each stage boundary, build a fresh runtime/state object, resume successfully without rerunning already committed semantic mutations, and finish verification; missing/corrupt/mismatched persisted state fails closed instead of skipping.

#### PR040 — xdl-pr040-incremental-weekly-refresh

Branch `fix/xdl-pr040-incremental-weekly-refresh`; commit scope `fix(xdl-pr040-incremental-weekly-refresh): ...`; depends on PR039.

Owned paths: weekly production runtime incremental-state loading/merging plus focused tests; no provider transport or schema changes.

Tasks: load the existing full Silver/Gold state; for each listing determine the latest quote/event date; call quote/dividend/split ingestion with the frozen inclusive seven-calendar-day overlap and previous records; replace the requested quote-window slice with the provider response, reconcile corporate actions over the same window, and merge the refreshed window back into the complete historical state before Gold; first-ever listing remains full-history.

Acceptance: an unchanged weekly fixture issues overlap requests, not full-history requests, for existing listings; rows older than the overlap survive unchanged; one corrected row replaces exactly one key; a provider-removed quote inside the authoritative requested window disappears from merged Gold; new dates/events append once; unchanged merged Gold produces zero semantic PostgreSQL mutations.

#### PR041 — xdl-pr041-corporate-action-set-reconciliation

Branch `fix/xdl-pr041-corporate-action-set-reconciliation`; commit scope `fix(xdl-pr041-corporate-action-set-reconciliation): ...`; depends on PR034.

Owned paths: dividend/split reconciliation helpers and their unit fixtures/tests only.

Tasks: remove the one-event-per-date assumption; reconcile the authoritative overlap as sets keyed by deterministic `event_key`; exact current keys remain active, prior active keys absent from the current overlap become retractions, and new current keys become active events; do not require heuristic one-to-one correction matching by date.

Acceptance: two dividends on one date and two splits on one date are accepted; changing one of two same-date events produces exactly one old-key retraction plus one new active key while the second remains unchanged; removing one produces exactly one retraction; provider-order changes are deterministic.

#### PR042 — xdl-pr042-adjusted-close-retroactive-reconciliation

Branch `fix/xdl-pr042-adjusted-close-retroactive-reconciliation`; commit scope `fix(xdl-pr042-adjusted-close-retroactive-reconciliation): ...`; depends on PR040+PR041.

Owned paths: weekly orchestration order/action-delta decision and quote-refresh tests only.

Tasks: process dividend/split overlap before final quote refresh; if either corporate-action set changes for a listing, fetch that listing's full EOD history with no `from` boundary and replace its complete quote history before Gold; otherwise retain the normal seven-day quote overlap path; document that `adjusted_close` is provider-adjusted and can be retroactively restated.

Acceptance: unchanged corporate actions use only overlap quote fetch; adding/correcting/retracting a corporate action forces exactly one full quote-history refresh for the affected listing and no other listing; a fixture where an old adjusted close changes is corrected; immediate unchanged replay returns to overlap mode and zero semantic DB mutations.

#### PR043 — xdl-pr043-provider-secret-safe-errors

Branch `fix/xdl-pr043-provider-secret-safe-errors`; commit scope `fix(xdl-pr043-provider-secret-safe-errors): ...`; depends on PR034.

Owned paths: `src/xetra_loader/eodhd/transport.py` error/redaction behavior and focused tests.

Tasks: ensure all provider failures construct sanitized exceptions; never retain a raw request URL containing `api_token` in user-visible exception text, `repr`, chained cause, or captured traceback; use `scrub_url` or suppress/replace the raw `HTTPError` cause; preserve HTTP status/path diagnostics without secrets.

Acceptance: tests capture `str(exc)`, `repr(exc)`, `traceback.format_exc()`, and exception causes for 4xx/429/5xx/network/invalid JSON paths; the literal token and its URL-encoded form occur zero times; useful endpoint/status information remains.

#### PR044 — xdl-pr044-provider-numeric-integrity

Branch `fix/xdl-pr044-provider-numeric-integrity`; commit scope `fix(xdl-pr044-provider-numeric-integrity): ...`; depends on PR034.

Owned paths: provider JSON numeric decoding/canonical numeric helpers, quote/corporate-action numeric normalization, focused contract/ingestion tests.

Tasks: avoid binary-float round trips for provider decimals; preserve exact decimal semantics from JSON; canonicalize semantically equal decimal values to one deterministic text representation for event keys/fingerprints; reject NaN/Infinity/non-finite numerics; require volume to be an exact non-negative integer rather than truncate a fractional value; require positive split factors and non-negative market prices; validate `low <= open/close <= high` when all involved fields are present.

Acceptance: `1`, `1.0`, and `1.00` canonicalize to the same semantic number/fingerprint; an exact long decimal survives ingestion unchanged; fractional volume fails; NaN/Infinity fail; inconsistent OHLC fails; provider raw Bronze remains preserved for diagnostics; deterministic event keys are stable under harmless numeric representation changes.

#### PR045 — xdl-pr045-gold-cross-dataset-validation

Branch `fix/xdl-pr045-gold-cross-dataset-validation`; commit scope `fix(xdl-pr045-gold-cross-dataset-validation): ...`; depends on PR034.

Owned paths: Gold cross-dataset validator, production `gold_validation` stage, focused tests.

Tasks: validate the four completed Gold results together; require every quote/dividend/split `(isin,exchange,code)` to exist in listing Gold; retain each dataset's duplicate/key/timestamp checks; return an exact validation summary/fingerprints rather than count-only success.

Acceptance: one orphan quote/dividend/split fails before any PostgreSQL mutation; valid children of active or retained inactive listings pass; `gold_validation` output contains row counts and fingerprints for all four datasets; removing a listing referenced by a child fails closed.

#### PR046 — xdl-pr046-corporate-action-gold-tombstones

Branch `fix/xdl-pr046-corporate-action-gold-tombstones`; commit scope `fix(xdl-pr046-corporate-action-gold-tombstones): ...`; depends on PR041+PR044.

Owned paths: corporate-action Gold persistence/reload sidecars, manifest metadata, verifier/restart readers, focused tests.

Tasks: persist dividend/split `retracted_keys` deterministically alongside active Gold rows, e.g. `retractions.json`; include the tombstone content in the manifest/fingerprint contract; make restart and independent verification reload both active rows and retractions; do not silently substitute an empty retraction set.

Acceptance: a Gold result containing a tombstone round-trips disk -> runtime with identical fingerprint; deleting or altering the tombstone sidecar makes manifest verification fail; a restarted sync still applies the intended retraction exactly once.

#### PR047 — xdl-pr047-atomic-streamed-medallion-persistence

Branch `refactor/xdl-pr047-atomic-streamed-medallion-persistence`; commit scope `refactor(xdl-pr047-atomic-streamed-medallion-persistence): ...`; depends on PR046.

Owned paths: `PostgresEodhdBootstrapRuntime` medallion write mechanics, partition layout helper, focused filesystem tests; no serving schema changes.

Tasks: stop holding/re-serializing the entire growing Bronze/Silver universe after every listing; persist deterministic per-listing partitions or an equivalent bounded-memory stream; write data and manifests through temporary files followed by atomic replace; publish the final dataset manifest only after all partitions are complete; keep semantic ordering/fingerprints independent of provider order.

Acceptance: processing N listings performs O(N) partition writes rather than rewriting the accumulated universe N times; peak runtime state does not require all raw provider payloads in one list; injected failure leaves the last committed dataset/manifest readable and no partially published manifest; replay fingerprints remain identical.

#### PR048 — xdl-pr048-listing-lifecycle-contract

Branch `feat/xdl-pr048-listing-lifecycle-contract`; commit scope `feat(xdl-pr048-listing-lifecycle-contract): ...`; depends on PR043+PR044.

Owned paths: listing contract/ingestion, lifecycle merge helper, provider fixtures/tests only; no SQL changes.

Tasks: make “all XETRA listings” exact: fetch the EODHD exchange-symbol list once for the default active set and once with `delisted=1`; retain every normalized non-empty-ISIN identity from the union; add semantic `is_active`; mark default-set rows active and delisted-only rows inactive; retain a previously known identity as inactive if it temporarily disappears from both current responses; no ETF/UCITS/type/country/currency filtering.

Acceptance: active-only, delisted-only, and mixed fixtures merge deterministically; no identity is duplicated; a delisted identity stays in historical listing Gold with `is_active=false`; reactivation flips only `is_active`; an identity disappearing from the provider is retained inactive rather than silently deleted.

#### PR049 — xdl-pr049-listing-lifecycle-postgres-migration

Branch `feat/xdl-pr049-listing-lifecycle-postgres-migration`; commit scope `feat(xdl-pr049-listing-lifecycle-postgres-migration): ...`; depends on PR048.

Owned paths: one forward PostgreSQL migration, canonical schema DDL, listing DTO/Gold/sync field propagation, focused unit/integration tests.

Tasks: add `is_active BOOLEAN NOT NULL` to the listing serving contract; safely backfill existing rows as active before the first lifecycle refresh; include the field in listing Gold fingerprints and semantic sync comparisons; preserve primary/FK identities and `portfell_app` SELECT compatibility.

Acceptance: migration succeeds on an existing populated schema without dropping child data; clean-create DDL and migrated DDL introspect identically; one active->inactive transition is exactly one listing update; quote/dividend/split FKs remain valid; no consumer write privilege is introduced.

#### PR050 — xdl-pr050-authoritative-postgres-reconciliation

Branch `fix/xdl-pr050-authoritative-postgres-reconciliation`; commit scope `fix(xdl-pr050-authoritative-postgres-reconciliation): ...`; depends on PR040+PR045+PR046+PR049.

Owned paths: listing/quote/corporate-action serving reconciliation, sync counters, focused PostgreSQL integration tests.

Tasks: make the complete merged Gold state the only input accepted by entity publication; quotes delete serving keys absent from complete merged quote Gold; corporate-action tombstones remove retracted serving events; listings are retained historically and become inactive rather than hard-deleted; preserve transaction coupling with sync state and exact inserted/updated/deleted/retracted counters.

Acceptance: after each sync PostgreSQL has zero extra keys relative to the complete merged Gold state; a removed quote produces exactly one `deleted`; a delisted/disappeared listing remains with `is_active=false`; no child is orphaned; injected failure rolls back both data and sync state; a complete unchanged replay has exactly zero mutations.

#### PR051 — xdl-pr051-runtime-role-hardening

Branch `fix/xdl-pr051-runtime-role-hardening`; commit scope `fix(xdl-pr051-runtime-role-hardening): ...`; depends on PR035+PR036.

Owned paths: PostgreSQL role/grant SQL, configuration resolver, runtime DB preflight, role integration tests, README secret/config documentation.

Tasks: keep `xetra-loader` as a NOLOGIN group role; run normal weekly publication through a non-superuser login that is a member of that role; separate admin/migration/bootstrap DSN from normal writer DSN; reject a superuser session in the normal weekly path; after schema provisioning revoke unnecessary `CREATE` on `xetra_loader_sync` and grant only required DML/USAGE; keep `portfell_app` a read-only group role.

Acceptance: weekly runtime succeeds as the least-privilege writer and fails closed as an accidental superuser/admin DSN; writer cannot create/drop schemas/tables; writer can perform required serving/sync DML; bootstrap/migrations still require explicit admin configuration; `portfell_app` remains SELECT-only with no sync-schema access.

#### PR052 — xdl-pr052-fetch-publication-provenance

Branch `fix/xdl-pr052-fetch-publication-provenance`; commit scope `fix(xdl-pr052-fetch-publication-provenance): ...`; depends on PR040+PR051.

Owned paths: fetch-batch/run metadata propagation, entity publication timestamp arguments, serving/provenance tests and docs only.

Tasks: define `fetched_at_utc` as the provider-fetch time of the semantic version currently stored and `published_at_utc` as the transaction publication time; carry the measured fetch timestamp from provider batch through Gold publication; do not set both columns from one publication clock; keep both fields outside semantic fingerprints; leave unchanged semantic rows untouched so a no-op poll does not create a serving mutation, while `loader_runs` records the poll.

Acceptance: a newly inserted/changed row has independently controlled fetch and publication timestamps; `fetched_at_utc <= published_at_utc`; unchanged replay does not update serving metadata and reports zero semantic mutations; loader run metadata still records the replay; fingerprints are unchanged by either timestamp.

#### PR053 — xdl-pr053-postgres-authoritative-rewrite

Branch `chore/xdl-pr053-postgres-authoritative-rewrite`; commit scope `chore(xdl-pr053-postgres-authoritative-rewrite): ...`; depends on every PR035-PR052 merged and green.

Atomic outcome: perform the mandatory controlled rewrite of loader-owned medallion/serving state after all corrected contracts are frozen, then independently prove the real target PostgreSQL is exact. This supersedes XDL-PR033 as the final production gate.

Owned paths:

- production rewrite/runbook and one guarded orchestration command;
- updated independent verifier for lifecycle/tombstone/numeric/provenance contracts;
- sanitized `artifacts/acceptance/postgres-full-sync-v2.json` and human-readable acceptance summary;
- no new business semantics beyond PR035-PR052.

Tasks:

1. Acquire the loader lock and require the recurring scheduler to be disabled for the rewrite window.
2. Preflight the exact target `10.10.1.3:54321`; fail if another host/port is resolved.
3. Create a timestamped, non-repository backup of the current `xetra_loader` and `xetra_loader_sync` schemas plus current Gold manifests before any destructive action; record backup checksums/path in the private run log, not committed credentials/data.
4. Record pre-rewrite row counts, key/fingerprint summaries, and listing lifecycle counts for comparison.
5. Apply all forward migrations required by PR049/PR051, then perform one confirmed loader-owned rewrite/rebuild where semantic event keys/fingerprints require it; never touch unrelated PostgreSQL schemas or unrelated filesystem paths.
6. Fetch the complete active+delisted XETRA listing union and full available quote/dividend/split histories under the corrected exact-numeric contracts; build validated Gold including corporate-action tombstones and cross-dataset references.
7. Publish through the least-privilege writer using PR050's authoritative reconciliation; schema/admin work remains on the explicit admin connection only.
8. Independently read Gold and PostgreSQL and require exact row counts, zero symmetric key differences, matching semantic fingerprints, zero duplicate keys, zero orphans, matching date bounds, exact `TIMESTAMPTZ(6)`, UTC sessions, listing lifecycle equality, and tombstone/fingerprint integrity.
9. Verify `portfell_app` SELECT-only behavior and normal writer least privilege with actual permission probes.
10. Run the guarded weekly incremental path immediately against unchanged source state and require zero semantic inserts/updates/deletes/retractions across all four serving datasets; verify it uses overlap requests rather than a second destructive bootstrap.
11. Emit a sanitized V2 report containing no password, token, DSN, or raw provider payload; mark `PASS` only if every assertion succeeds.
12. Re-enable the recurring Sunday 08:00 runner only after the V2 report is `PASS`.

Acceptance:

- backup completed before rewrite and can be restored according to the runbook;
- only loader-owned state is rewritten;
- active+delisted listing lifecycle is represented exactly and historical listing identities are retained inactive rather than silently dropped;
- PostgreSQL serving data is byte/semantic-contract equivalent to corrected Gold under independent verification;
- all corrected corporate-action event keys and numeric fingerprints are represented after the rewrite;
- no duplicate keys or orphans exist;
- every timestamp column remains exactly `TIMESTAMPTZ(6)` and runtime sessions are UTC;
- weekly publication uses a non-superuser writer and `portfell_app` remains SELECT-only;
- unchanged guarded weekly replay produces exactly zero semantic mutations and does not invoke destructive reset;
- actual cron contract is exactly Sunday 08:00 Europe/Vienna and invokes the guarded weekly runner;
- `artifacts/acceptance/postgres-full-sync-v2.json` is committed, sanitized, and `PASS`;
- all quality jobs and `merge-gate` are green on the same PR053 head SHA;
- only after these conditions hold may `xetra-loader` and the Portfell PostgreSQL handoff be declared complete.

### 10.6 Corrected completion gate

The completion claims in Sections 7 and 9 are superseded. XDL-PR033 and the existing fixture acceptance artifact are historical evidence only and are **not sufficient for cutover**.

`xetra-loader` is complete only when PR035-PR052 are merged under repaired governance and PR053's real-target V2 rewrite/verification is `PASS`. Until then, Portfell may read the documented schema for development but must not treat the database as a fully reconciled production contract.

### 10.7 Operational status — reviewed 2026-08-29

- PR034 through PR052 are merged in `origin/main`.
- PR053 is complete: `artifacts/acceptance/postgres-full-sync-v2.json` is a sanitized real-target `PASS` report. The no-quote recovery rebuilt Gold quotes from the restored PostgreSQL serving state, refreshed corporate actions from the provider, and independently verified all serving datasets against Gold.
- Follow-up fixes merged after PR053: PR055 dedicated database configuration, PR056 empty-target backup handling, PR057 action replay key deduplication, and PR058 weekly invalid-OHLC quarantine.
- The restart checkpoint records completed `listings`, `dividends`, and `splits` stages. Resume performs full quote-history fetches only for the `1,336` listing union with corporate-action changes and seven-day overlap fetches for other active listings.
- The recurring cron is enabled as `CRON_TZ=Europe/Vienna`, Sunday 08:00, and invokes the guarded `xdl-weekly` runner.

## 11. Summary of all existing backlog PRs

This summary covers the work orders that already existed before the new
XDL-PR059–XDL-PR069 feature-serving program. Their detailed specifications
remain in the preceding sections. PR054 has no work-order specification in the
existing backlog and is intentionally not inferred here.

| ID | Existing work-order | Summary | Status in existing backlog |
| --- | --- | --- | --- |
| PR001 | `xdl-pr001-python-repository-baseline` | Python 3.14.7 repository and installable source baseline | merged |
| PR002 | `xdl-pr002-quality-command-contract` | Canonical lint, type, unit, and integration commands | merged |
| PR003 | `xdl-pr003-git-policy-validator` | Conventional Commit and work-order naming policy | merged |
| PR004 | `xdl-pr004-push-quality-workflow` | Parallel push quality gate | merged |
| PR005 | `xdl-pr005-merge-quality-workflow` | Parallel pull-request merge gate | merged |
| PR006 | `xdl-pr006-main-protection-automerge` | Protected `main`, required checks, and auto-merge | merged |
| PR007 | `xdl-pr007-postgres-market-schema` | PostgreSQL market tables and timestamp/key contract | merged |
| PR008 | `xdl-pr008-postgres-role-grants` | Loader writer and Portfell read-only permissions | merged |
| PR009 | `xdl-pr009-medallion-core-contract` | Bronze/Silver/Gold paths, manifests, and fingerprints | merged |
| PR010 | `xdl-pr010-listing-dataset-contract` | Listing identity and deterministic dataset contract | merged |
| PR011 | `xdl-pr011-quote-dataset-contract` | Quote identity, UTC date semantics, and overlap contract | merged |
| PR012 | `xdl-pr012-corporate-action-contract` | Dividend/split identity and event contract | merged |
| PR013 | `xdl-pr013-eodhd-transport` | EODHD HTTP, retry, and rate-limit seam | merged |
| PR014 | `xdl-pr014-xetra-listing-ingestion` | XETRA listing ingestion | merged |
| PR015 | `xdl-pr015-eod-quote-ingestion` | EOD quote ingestion | merged |
| PR016 | `xdl-pr016-dividend-ingestion` | Dividend ingestion | merged |
| PR017 | `xdl-pr017-split-ingestion` | Split ingestion | merged |
| PR018 | `xdl-pr018-gold-listing-build` | Validated listing Gold dataset | merged |
| PR019 | `xdl-pr019-gold-quote-build` | Validated quote Gold dataset | merged |
| PR020 | `xdl-pr020-gold-dividend-build` | Validated dividend Gold dataset | merged |
| PR021 | `xdl-pr021-gold-split-build` | Validated split Gold dataset | merged |
| PR022 | `xdl-pr022-postgres-sync-core` | Transactional PostgreSQL sync/state/fingerprint core | merged |
| PR023 | `xdl-pr023-postgres-listing-sync` | Idempotent listing publication | merged |
| PR024 | `xdl-pr024-postgres-quote-sync` | Idempotent quote publication | merged |
| PR025 | `xdl-pr025-postgres-dividend-sync` | Idempotent dividend publication | merged |
| PR026 | `xdl-pr026-postgres-split-sync` | Idempotent split publication | merged |
| PR027 | `xdl-pr027-weekly-pipeline-orchestrator` | Ordered weekly pipeline including verification | merged |
| PR028 | `xdl-pr028-loader-lock-restart` | Non-overlapping and restart-safe execution wrapper | merged |
| PR029 | `xdl-pr029-sunday-1100-schedule` | Historical Sunday scheduler work order; later superseded by the 08:00 contract | merged/superseded |
| PR030 | `xdl-pr030-destructive-reset-guard` | Explicitly confirmed, scoped destructive reset | merged |
| PR031 | `xdl-pr031-full-xetra-bootstrap` | Full-history XETRA bootstrap command | merged |
| PR032 | `xdl-pr032-loader-e2e-gate` | Production-like end-to-end fixture gate | merged |
| PR033 | `xdl-pr033-production-postgres-full-sync-verification` | Historical real-target PostgreSQL full-sync verification gate | historical; superseded by PR053 |
| PR034 | `xdl-pr034-audit-corrective-backlog` | Audit findings and corrective dependency wave | merged |
| PR035 | `xdl-pr035-repository-governance-repair` | Repository protection and auto-merge repair | merged |
| PR036 | `xdl-pr036-real-postgres-ci` | Mandatory real PostgreSQL integration service in CI | merged |
| PR037 | `xdl-pr037-scheduler-contract-reconciliation` | Single Sunday 08:00 Europe/Vienna schedule contract | merged |
| PR038 | `xdl-pr038-production-weekly-runner-wiring` | Guarded weekly runner instead of destructive cron bootstrap | merged |
| PR039 | `xdl-pr039-restart-state-rehydration` | Cross-process checkpoint and restart state | merged |
| PR040 | `xdl-pr040-incremental-weekly-refresh` | Seven-day overlap and complete-state weekly refresh | merged |
| PR041 | `xdl-pr041-corporate-action-set-reconciliation` | Multiple same-date corporate-action reconciliation | merged |
| PR042 | `xdl-pr042-adjusted-close-retroactive-reconciliation` | Full quote refresh after corporate-action corrections | merged |
| PR043 | `xdl-pr043-provider-secret-safe-errors` | Provider error and traceback secret redaction | merged |
| PR044 | `xdl-pr044-provider-numeric-integrity` | Exact numeric parsing and validation | merged |
| PR045 | `xdl-pr045-gold-cross-dataset-validation` | Gold referential integrity across all datasets | merged |
| PR046 | `xdl-pr046-corporate-action-gold-tombstones` | Persisted corporate-action retraction tombstones | merged |
| PR047 | `xdl-pr047-atomic-streamed-medallion-persistence` | Bounded-memory atomic medallion persistence | merged |
| PR048 | `xdl-pr048-listing-lifecycle-contract` | Active/delisted listing lifecycle | merged |
| PR049 | `xdl-pr049-listing-lifecycle-postgres-migration` | Listing lifecycle PostgreSQL migration and propagation | merged |
| PR050 | `xdl-pr050-authoritative-postgres-reconciliation` | Exact PostgreSQL reconciliation to complete Gold | merged |
| PR051 | `xdl-pr051-runtime-role-hardening` | Least-privilege runtime writer and admin separation | merged |
| PR052 | `xdl-pr052-fetch-publication-provenance` | Separate provider fetch and PostgreSQL publication timestamps | merged |
| PR053 | `xdl-pr053-postgres-authoritative-rewrite` | Controlled rewrite and independent real-target V2 verification | complete; real-target report `PASS` |
| PR055 | follow-up: dedicated database configuration | Dedicated PostgreSQL database configuration after PR053 | merged per existing status note |
| PR056 | follow-up: empty-target backup handling | Safe backup behavior for an empty target | merged per existing status note |
| PR057 | follow-up: action replay key deduplication | Deduplication of corporate-action replay keys | merged per existing status note |
| PR058 | follow-up: weekly invalid-OHLC quarantine | Weekly quarantine of invalid OHLC records | merged per existing status note |

The new XDL-PR059–XDL-PR069 work orders are not included in this historical
summary; they are defined at the beginning of this file and form the active
XETRA feature-serving program.
