#!/usr/bin/env bash
set -Eeuo pipefail

umask 077

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

mkdir -p "$repo_root/.logs"
chmod 700 "$repo_root/.logs"
run_stamp="$(date -u +%Y%m%dT%H%M%SZ)-$$"
log_path="$repo_root/.logs/weekly-cron-${run_stamp}.log"
find "$repo_root/.logs" -maxdepth 1 -type f \
  -name 'weekly-cron-*.log' -mtime +90 -delete
exec >>"$log_path" 2>&1

started_epoch="$(date +%s)"
finished_epoch="$started_epoch"
exit_code=0
printf 'weekly_start_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
if "$repo_root/.venv/bin/xdl-weekly"; then
  exit_code=0
else
  exit_code=$?
fi
finished_epoch="$(date +%s)" || finished_epoch="$started_epoch"
printf 'weekly_end_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf 'elapsed_seconds=%s\n' "$((finished_epoch - started_epoch))"
printf 'exit_code=%s\n' "$exit_code"
exit "$exit_code"
