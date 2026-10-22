#!/usr/bin/env bash
set -Eeuo pipefail

umask 077

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

run_stamp="$(date -u +%Y%m%dT%H%M%SZ)-$$"
backup_root="/home/dev_xetra/.xdl-full-backups"
log_path="$repo_root/.logs/full-refresh-cron-${run_stamp}.log"
report_path="$repo_root/.logs/full-refresh-${run_stamp}-report.json"
checkpoint_path="$repo_root/lake/weekly.checkpoint.json"

mkdir -p "$repo_root/.logs" "$backup_root"
chmod 700 "$repo_root/.logs"
chmod 700 "$backup_root"
find "$repo_root/.logs" -maxdepth 1 -type f \
  -name 'full-refresh-cron-*.log' -mtime +90 -delete
exec >>"$log_path" 2>&1

if [[ -f "$checkpoint_path" ]]; then
  mv -- "$checkpoint_path" "$backup_root/weekly-checkpoint-${run_stamp}.json"
fi

started_epoch="$(date +%s)"
started_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf 'full_refresh_start_utc=%s\n' "$started_utc"

if "$repo_root/.venv/bin/xdl-verify-production-sync" \
  --execute-full-sync \
  --confirm-destructive-reset \
  --skip-weekly-replay \
  --backup-root "$backup_root" \
  --output "$report_path"; then
  exit_code=0
else
  exit_code=$?
fi

finished_epoch="$(date +%s)"
finished_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf 'full_refresh_end_utc=%s\n' "$finished_utc"
printf 'elapsed_seconds=%s\n' "$((finished_epoch - started_epoch))"
printf 'exit_code=%s\n' "$exit_code"
exit "$exit_code"
