# Controlled PostgreSQL rewrite

## Contents

- [Purpose and preconditions](#purpose-and-preconditions)
- [Cutover sequence](#cutover-sequence)
- [Restore the weekly schedule](#restore-the-weekly-schedule)

This is an operational procedure for the explicit one-off full rewrite, not
the normal Sunday full-history fetch with semantic-delta publication.

## Purpose and preconditions

`xdl-verify-production-sync --execute-full-sync --confirm-destructive-reset`
is the one-off final cutover command. It refuses to run unless the recurring
`xdl-enqueue-weekly` cron entry has first been disabled, the configured administrative
connection resolves exactly to `10.10.1.3:54321`, a lock is acquired, and a
backup root outside this repository is supplied.

Stop `xetra-loader-api.service` and verify that `xetra_loader.loader_jobs` has
no queued or running job before the destructive rewrite. This prevents a new
authenticated API request from racing the cutover.

## Cutover sequence

The command backs up only `xetra_loader` and `xetra_loader_sync` plus the Gold
manifests, performs the confirmed loader-owned reset, and fetches the complete
active and delisted universe. Administrative credentials are used only for the
backup and reset; all serving publication is performed through the configured
non-superuser writer. It then runs the guarded weekly path once against the
unchanged source and writes the sanitized V2 acceptance report.

## Restore the weekly schedule

Keep the cron entry disabled until that report is `PASS`. Restart the API
service and restore exactly the Sunday `11:00 Europe/Vienna`
`xdl-enqueue-weekly` entry only after a successful run.
