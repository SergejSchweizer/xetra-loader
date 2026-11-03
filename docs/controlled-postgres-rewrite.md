# Controlled PostgreSQL rewrite

## Contents

- [Purpose and preconditions](#purpose-and-preconditions)
- [Cutover sequence](#cutover-sequence)
- [Return to normal weekly operation](#return-to-normal-weekly-operation)

This runbook covers an explicitly authorized, manually invoked maintenance
rewrite. It is not the repository's weekly schedule and must not be installed
as a cron job. The canonical weekly job is the restart-safe delta runner at
Sunday `09:00 Europe/Vienna`; the separate Gold copy runs Sunday at `22:00`.
The host crontab was verified on 2026-10-24 to match those two entries. Do not
replace the weekly delta with this full-history rewrite or add a parallel REST
enqueue entry.

## Purpose and preconditions

`xdl-verify-production-sync --execute-full-sync --confirm-destructive-reset
--cores 6` is the destructive full-rewrite command (`--cores` defaults to the
configured budget or six). The host wrapper `scripts/run_full_refresh.sh` is
for explicit maintenance only. Before a manual invocation, verify that the
weekly delta runner is not active and that no API worker or queued job can
race the rewrite. Do not alter or disable the recurring crontab entries unless
the operator separately authorizes that change. The command also requires the
configured administrative connection to resolve exactly to
`10.10.1.3:54321`, an acquired lock, and a backup root outside this repository.

Stop `xetra-loader-api.service` and verify that `xetra_loader.loader_jobs` has
no queued or running job before the destructive rewrite. This prevents a new
authenticated API request from racing the cutover.

## Cutover sequence

The command backs up only `xetra_loader` and `xetra_loader_sync` plus the Gold
manifests, then creates `xetra_loader_build` and `xetra_loader_sync_build`.
It fetches the complete active and delisted universe and publishes every table,
job queue, feature materialized view, and index into those build schemas. The
active schemas are not reset or dropped during the download/build phase. Admin
credentials provision the build schemas; serving publication uses the
configured non-superuser writer.

After the build's row/key/fingerprint checks pass, one PostgreSQL transaction
renames the active schemas to `*_previous` and promotes both build schemas to
the canonical names. If this transaction fails, PostgreSQL rolls back the
schema-name changes and the original schemas remain active. An independent
production acceptance run follows the swap. Only after its sanitized V2 report
is successfully written with `PASS` are the previous schemas removed, without
`CASCADE` (external dependent objects therefore block cleanup instead of being
silently deleted). A failed or unverified run leaves the previous schemas in
place for recovery. Each run adds a timestamped recovery directory under the
configured backup root; only a verified `PASS` prunes older backup/checkpoint
artifacts, retaining the four newest matching full-sync directories and
checkpoints. Unrelated files and malformed names are untouched.

## Return to normal weekly operation

After the independent acceptance report is `PASS`, confirm the maintenance
process has exited and released its locks. The regular Sunday `09:00
Europe/Vienna` restart-safe delta and Sunday `22:00` Gold-copy entries remain
the canonical schedule. If an operator separately disabled either entry,
restore the original entries exactly; never substitute the full-refresh
wrapper or install a parallel `xdl-enqueue-weekly` entry.
