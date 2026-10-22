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
manifests, performs the confirmed loader-owned reset, and fetches the complete
active and delisted universe. Administrative credentials are used only for the
backup and reset; all serving publication is performed through the configured
non-superuser writer. It then runs the guarded weekly path once against the
unchanged source and writes the sanitized V2 acceptance report. Each run adds
a timestamped recovery directory under the configured backup root. Only after
the verified `PASS` report is successfully written does the command retain the
four newest matching full-sync directories and archived weekly checkpoints,
removing older artifacts of each type. A failed or unverified run never prunes;
unrelated files and malformed names are left untouched. If pruning itself fails
after a PASS, the sync remains successful and emits a warning so the completed
destructive import is not mistakenly retried.

## Restore the weekly schedule

Keep the cron entry disabled until that report is `PASS`. Restore only the
Sunday `09:00 Europe/Vienna` direct full-refresh entry and Sunday `22:00`
Gold-copy entry that were disabled before the rewrite. Do not install a
parallel `xdl-enqueue-weekly` entry as part of this procedure.
