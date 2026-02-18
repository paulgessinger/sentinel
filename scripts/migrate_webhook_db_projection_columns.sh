#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  migrate_webhook_db_projection_columns.sh [--no-backup] <path-to-webhooks.sqlite3>

Description:
  One-off schema migration for Sentinel webhook DB.
  Creates the dedicated internal cache table (if missing):
    sentinel_check_run_state
  and creates:
    idx_sentinel_check_run_state_repo_head

Notes:
  - Safe to run multiple times (idempotent).
  - Creates a timestamped backup by default.
EOF
}

backup_enabled=1

if [[ "${1:-}" == "--no-backup" ]]; then
  backup_enabled=0
  shift
fi

if [[ $# -ne 1 ]]; then
  usage
  exit 1
fi

db_path="$1"

if ! command -v sqlite3 >/dev/null 2>&1; then
  echo "error: sqlite3 is required but not installed" >&2
  exit 1
fi

if [[ ! -f "$db_path" ]]; then
  echo "error: database file does not exist: $db_path" >&2
  exit 1
fi

if [[ $backup_enabled -eq 1 ]]; then
  ts="$(date +%Y%m%d%H%M%S)"
  backup_path="${db_path}.bak.${ts}"
  cp -p "$db_path" "$backup_path"
  echo "backup created: $backup_path"
fi

sqlite3 "$db_path" "PRAGMA busy_timeout=10000; CREATE TABLE IF NOT EXISTS sentinel_check_run_state (repo_id INTEGER NOT NULL, repo_full_name TEXT NULL, head_sha TEXT NOT NULL, check_name TEXT NOT NULL, app_id INTEGER NOT NULL, check_run_id INTEGER NULL, status TEXT NOT NULL, conclusion TEXT NULL, started_at TEXT NULL, completed_at TEXT NULL, output_title TEXT NULL, output_summary_hash TEXT NULL, output_text_hash TEXT NULL, last_eval_at TEXT NOT NULL, last_publish_at TEXT NULL, last_publish_result TEXT NOT NULL, last_publish_error TEXT NULL, last_delivery_id TEXT NULL, PRIMARY KEY (repo_id, head_sha, check_name)); CREATE INDEX IF NOT EXISTS idx_sentinel_check_run_state_repo_head ON sentinel_check_run_state (repo_id, head_sha);" >/dev/null

echo "table ensured: sentinel_check_run_state"
echo "index ensured: idx_sentinel_check_run_state_repo_head"

echo "migration complete for: $db_path"
