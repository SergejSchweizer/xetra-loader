# XETRA Data Loader — Backlog Archive

Last reviewed: 2026-10-23

## Contents

- [Status](#status)
- [Current system summary](#current-system-summary)
- [Consolidated obsolete PR history](#consolidated-obsolete-pr-history)

## Status

The PR plans and acceptance lists formerly tracked here are obsolete and have
been condensed into the historical ledger at the end of this file. This
backlog contains no active work orders. It records the last status already
documented here; it is not a live GitHub PR-status check.

For current implementation details, use `README.md`, the source code, database
migrations, and executable tests rather than these retired plans.

## Current system summary

- EODHD market data flows through Bronze, Silver, and Gold datasets and is
  published to PostgreSQL raw tables.
- The serving contract is `xetra_loader.xetra_features`, a materialized view
  keyed by `(isin, exchange, code, trade_date)`. It includes
  `available_at_utc`, the next weekday at midnight UTC; exchange holidays are
  not modeled. Its only base levels are `adjusted_close_level` and
  `volume_level`; OHLC values are internal inputs only for approved derived
  features. The complete view contains 105 columns: four identities,
  availability, two levels, and 98 derived feature columns.
- The runtime writes row data, manifests, retractions, and restart/checkpoint
  metadata as Polars-written Parquet. Legacy JSON is read only by the explicit
  migration path; verified retired sources are removed from the lake or
  archived outside it.
- The repository cron artifact directly runs the restart-safe Sunday 09:00
  Vienna weekly delta and separately copies Gold at 22:00. The optional durable
  PostgreSQL API job is not part of that cron schedule. A previously recorded
  host-state note reported a guarded full historical refresh at 09:00 and the
  Gold copy at 22:00, with the API service not installed; that host state has
  not been revalidated here and must not be treated as current deployment
  evidence. After a verified PASS report, the guarded full-refresh command
  retains the four newest timestamped backup directories and weekly
  checkpoints; failed runs do not prune. The CPU budget is six cores by default.
- The repository feature-view contract has 105 columns: four identities, one
  availability column, two base levels, and 98 catalog-derived features.
- PostgreSQL access follows least privilege; credentials are provisioned
  outside tracked repository files.

## Consolidated obsolete PR history

| PRs | Condensed scope | Last recorded disposition |
| --- | --- | --- |
| PR001–PR028 | Repository/CI foundations, PostgreSQL schema and roles, medallion/data contracts, EODHD ingestion, Gold builds, transactional sync, orchestration and restart safety | Merged |
| PR029 | Historical Sunday 11:00 scheduler | Merged; repository schedule is now Sunday 09:00 Europe/Vienna (`deploy/cron/xetra-loader.cron`) |
| PR030–PR032 | Destructive-operation guard, full XETRA bootstrap, end-to-end QA | Merged |
| PR033 | Historical production full-sync verification | Superseded by PR053 |
| PR034–PR053 | Governance and CI corrections, scheduler/restart/incremental fixes, data integrity, lifecycle, provenance, PostgreSQL reconciliation and authoritative rewrite | Merged/completed; PR053 recorded a real-target PASS |
| PR054 | No work order was recorded in the prior backlog | Intentionally unassigned; no status inferred |
| PR055–PR058 | Dedicated database configuration and follow-up safety/data-quality fixes | Merged per the prior status ledger |
| PR059–PR078 | Feature contract and materialized view, raw-sync integration, grants, calculation/operational QA, fold-local standardization and follow-up feature fixes | Merged per the prior completion ledger |
| PR079–PR089 | Corrective audit, overlap/window/RSI/oracle/memory/snapshot/identity fixes, complete-run QA and final repository ledger | Merged per the prior completion ledger |
| PR090 | Provision the `xetra_loader` feature-view reader role | Closed unmerged; no merged implementation status inferred |
| PR091 | Close the completion-ledger gap | Merged |
| PR092 | Parallelize and harden weekly delta sync | Closed unmerged; superseded by PR093 |
| PR093 | Final parallel sync hardening and PostgreSQL verification | Merged |
| PR094 | First gated weekly repository-review workflow | Closed unmerged; superseded by PR095 |
| PR095 | Gated weekly repository-review automation | Merged |

PR029 and PR033 are retained only to explain superseded decisions. PR054 is
shown as a numbering gap, not as planned or completed work. No detailed PR
specifications or acceptance lists remain active in this file.
