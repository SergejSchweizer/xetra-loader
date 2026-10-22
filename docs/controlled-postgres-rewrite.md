# Controlled PostgreSQL rewrite

## Contents

- [Purpose and preconditions](#purpose-and-preconditions)
- [Cutover sequence](#cutover-sequence)
- [Restore the weekly schedule](#restore-the-weekly-schedule)

This is the runbook for the guarded full rewrite used by the repository's
Sunday `09:00 Europe/Vienna` schedule. The current host crontab also runs this
full historical refresh every Sunday at `09:00` and copies Gold to
`/volume1/Temp/Quotes` at `22:00`. The API service is not installed. Do not add
the REST enqueue entry alongside the direct full-refresh schedule.

## Purpose and preconditions

`xdl-verify-production-sync --execute-full-sync --confirm-destructive-reset
--cores 6` is the destructive full-rewrite command (`--cores` defaults to the
configured budget or six). The host wrapper `scripts/run_full_refresh.sh`
invokes it weekly. Before a separate manual invocation, disable the host's
scheduled full-refresh entry and any `xdl-enqueue-weekly` entry, then verify no
API worker or queued job can race the rewrite. The command also requires the
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

## Restore the weekly schedule

Keep the cron entry disabled until that report is `PASS`. Restore only the
Sunday `09:00 Europe/Vienna` direct full-refresh entry and Sunday `22:00`
Gold-copy entry that were disabled before the rewrite. Do not install a
parallel `xdl-enqueue-weekly` entry as part of this procedure.
