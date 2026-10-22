#!/usr/bin/env bash
set -Eeuo pipefail

umask 077

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

mkdir -p "$repo_root/.logs"
chmod 700 "$repo_root/.logs"
run_stamp="$(date -u +%Y%m%dT%H%M%SZ)-$$"
log_path="$repo_root/.logs/gold-lake-copy-${run_stamp}.log"
find "$repo_root/.logs" -maxdepth 1 -type f \
  -name 'gold-lake-copy-*.log' -mtime +90 -delete
exec >>"$log_path" 2>&1

printf 'gold_lake_copy_start_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
/usr/bin/flock -s -n lake/authoritative-rewrite.lock \
  /usr/bin/rsync -a -- lake/gold/ /volume1/Temp/Quotes/xetra-loader-gold/
printf 'gold_lake_copy_end_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
