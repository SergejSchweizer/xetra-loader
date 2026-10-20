# XETRA Data Loader — Backlog Archive

Last reviewed: 2026-10-20

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
- The serving object is `xetra_loader.xetra_features`, a materialized view
  keyed by `(isin, exchange, code, trade_date)`. Its only base levels are
  `adjusted_close_level` and `volume_level`; OHLC values are internal inputs
  only for the approved intraday-derived features.
- Market-data artifacts across the lake use Polars-written Parquet. JSON
  manifests and restart/checkpoint files remain control metadata.
- The Sunday 11:00 Vienna cron enqueues a durable PostgreSQL job consumed by
  the event-driven worker inside the resident API process; the CPU budget is
  six cores by default.
- The feature materialization has 104 columns: four identities, two base
  levels, and 98 catalog-derived features.
- PostgreSQL access follows least privilege; credentials are provisioned
  outside tracked repository files.

## Consolidated obsolete PR history

| PRs | Condensed scope | Last recorded disposition |
| --- | --- | --- |
| PR001–PR028 | Repository/CI foundations, PostgreSQL schema and roles, medallion/data contracts, EODHD ingestion, Gold builds, transactional sync, orchestration and restart safety | Merged |
| PR029 | Historical Sunday 11:00 scheduler | Merged; superseded by the later Sunday 08:00 schedule contract |
| PR030–PR032 | Destructive-operation guard, full XETRA bootstrap, end-to-end QA | Merged |
| PR033 | Historical production full-sync verification | Superseded by PR053 |
| PR034–PR053 | Governance and CI corrections, scheduler/restart/incremental fixes, data integrity, lifecycle, provenance, PostgreSQL reconciliation and authoritative rewrite | Merged/completed; PR053 recorded a real-target PASS |
| PR054 | No work order was recorded in the prior backlog | Intentionally unassigned; no status inferred |
| PR055–PR058 | Dedicated database configuration and follow-up safety/data-quality fixes | Merged per the prior status ledger |
| PR059–PR078 | Feature contract and materialized view, raw-sync integration, grants, calculation/operational QA, fold-local standardization and follow-up feature fixes | Merged per the prior completion ledger |
| PR079–PR089 | Corrective audit, overlap/window/RSI/oracle/memory/snapshot/identity fixes, complete-run QA and final repository ledger | Merged per the prior completion ledger |

PR029 and PR033 are retained only to explain superseded decisions. PR054 is
shown as a numbering gap, not as planned or completed work. No detailed PR
specifications or acceptance lists remain active in this file.
