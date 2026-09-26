# xetra-loader

Deterministic XETRA market-data loader. The repository will own EODHD access, Bronze/Silver/Gold datasets, PostgreSQL publication, and the scheduled loader lifecycle defined in `BACKLOG.md`.

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
`sql/schema/002_roles.sql`, `sql/sync/001_xetra_loader_sync.sql`, and
`sql/schema/004_xetra_features.sql`. Apply
`sql/roles/005_xetra_loader_feature_view_reader.sql` only after the externally
provisioned `xetra_loader` role exists; it grants that role `USAGE` on the
`xetra_loader` schema and `SELECT` only on `xetra_loader.xetra_features`.
The explicit provisioning command is `.venv/bin/xdl-migrate`; it uses only the
admin DSN and creates/populates the materialized feature view. The weekly
`.venv/bin/xdl-weekly` command never creates or alters PostgreSQL objects.
The publication writer is the non-superuser `xetra_data_loader_writer`, a
member of the `xetra-data-loader` group. The legacy files in `sql/migrations/`
are historical rename migrations, not the current clean-create setup.

## Local secrets

Copy `config.example.yaml` to `config.yaml` and fill in the EODHD token plus separate PostgreSQL writer and admin credentials. Set `postgres.database` to the dedicated repository database `xetra_loader`; it overrides the database component of the configured writer URI. `postgres.writer_dsn` (or `XDL_POSTGRES_WRITER_DSN`) is used by the weekly runner and must authenticate as the non-superuser `xetra_data_loader_writer`; the runner rejects superuser sessions. `postgres.admin_dsn` (or `XDL_POSTGRES_ADMIN_DSN`) is required only for schema provisioning, migration, reset, and the controlled full bootstrap. When no explicit admin URI is set, the loader composes one from the local `host`, `port`, `user`, `password`, and `database` fields. The loader reads this ignored local file; never commit it. `EODHD_API_TOKEN` and `XDL_MEDALLION_ROOT` can still be provided through the environment.

## Current scope

The dependency-ordered work orders in `BACKLOG.md` provide the EODHD
transport, XETRA listing and corporate-action ingestion, Bronze/Silver/Gold
datasets, transactional PostgreSQL publication, weekly orchestration, guarded
bootstrap, and acceptance verification.

The feature contract is `xetra_loader.xetra_features`, a materialized view
with only `adjusted_close_level` and `volume_level` as base levels. OHLC
columns are internal inputs only for the approved high-low range, intraday
return, and overnight-gap families. Fold-local standardization is a
downstream operation documented in
[`docs/train-fold-standardization.md`](docs/train-fold-standardization.md); it
never changes Gold or the global feature view.

## PostgreSQL schema inventory

The raw quote table `xetra_loader.eod_quotes` has **13 columns**: four identity
columns (`isin`, `exchange`, `code`, `trade_date`), one EOD timestamp, six raw
market fields (`open`, `high`, `low`, `close`, `adjusted_close`, `volume`), and
two publication-audit timestamps. It contains raw OHLCV data, not derived
features.

The materialized view `xetra_loader.xetra_features` has **52 columns**:

- **4 identity columns**: `isin`, `exchange`, `code`, `trade_date`;
- **2 exposed level columns**: `adjusted_close_level`, `volume_level`;
- **46 derived feature columns** from the versioned catalog, or **48 catalog
  columns** when the two base levels are included and identity columns are
  excluded.

The view deliberately contains no `open_level`, `high_level`, `low_level`, or
`close_level`, and no `average_correlation_5obs`,
`average_correlation_10obs`, or `average_correlation_20obs`. Internal OHLC
values are used only for high-low range, intraday return, and overnight gap.

The deployed scheduler is Sunday `08:00` in `Europe/Vienna` and invokes the
restart-safe `xdl-weekly` runner. The real-target acceptance run remains an
operational deployment step and requires valid access to the configured
PostgreSQL instance.
