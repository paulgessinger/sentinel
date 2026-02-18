sentinel!

## Webhook SQLite mirror (PoC)

The webhook service can persist selected webhook deliveries into SQLite and maintain
current-state projection tables.
In the default PoC mode, `/webhook` records events only and does not trigger PR
re-evaluation/dispatch. Projection-driven check-run evaluation can be enabled via
`PROJECTION_EVAL_ENABLED`.
Webhook processing runs asynchronously after signature verification, so the endpoint
acknowledges (`200`) before persistence/projection work completes.
On startup (when `WEBHOOK_DB_ENABLED=true`), the service runs Alembic migrations to
`head` for `WEBHOOK_DB_PATH` automatically.
The state dashboard is available at `/state` with live updates via `/state/stream`
and fragment refresh endpoint `/state/table`. PR detail pages are available at
`/state/pr/<repo_id>/<pr_number>`.

### Environment variables

- `WEBHOOK_DB_ENABLED` (default: `true`)
- `WEBHOOK_DB_PATH` (default: `${DISKCACHE_DIR}/webhooks.sqlite3`)
- `WEBHOOK_DB_RETENTION_SECONDS` (default: `2592000`)
- `WEBHOOK_DB_EVENTS` (default: `check_run,check_suite,workflow_run,status,pull_request`)
- `WEBHOOK_DISPATCH_ENABLED` (default: `false`)
- `WEBHOOK_PROJECTION_PRUNE_ENABLED` (default: `true`)
- `WEBHOOK_PROJECTION_COMPLETED_RETENTION_SECONDS` (default: `604800`)
- `WEBHOOK_PROJECTION_ACTIVE_RETENTION_SECONDS` (default: `2592000`)
- `PROJECTION_EVAL_ENABLED` (default: `false`; enable projection-driven evaluation)
- `PROJECTION_PUBLISH_ENABLED` (default: `false`; compute but do not post when false)
- `PROJECTION_DEBOUNCE_SECONDS` (default: `2`)
- `PROJECTION_PULL_REQUEST_SYNCHRONIZE_DELAY_SECONDS` (default: `5`; additional delay
  after `pull_request` `synchronize`/`opened`/`reopened` before evaluation)
- `PROJECTION_CHECK_RUN_NAME` (default: `merge-sentinel`)
- `PROJECTION_CONFIG_CACHE_SECONDS` (default: `300`)
- `PROJECTION_PR_FILES_CACHE_SECONDS` (default: `86400`)
- `PROJECTION_PATH_RULE_FALLBACK_ENABLED` (default: `true`)
- `CHECK_RUN_NAME_FILTER` (optional regex; matching `check_run` names and `status`
  contexts are skipped before persistence/dispatch)
- `WEBHOOK_FILTER_SELF_APP_ID` (default: `true`; skip events whose source app id
  matches `GITHUB_APP_ID`)
- `WEBHOOK_FILTER_APP_IDS` (optional comma-separated app ids; matching source app ids
  are skipped before persistence/dispatch)

### Tables

- `webhook_events`: webhook delivery log keyed by `delivery_id` with zstd-compressed
  payload bytes in `payload_json`
- `check_runs_current`: latest check run state (including `repo_full_name`) keyed by
  `(repo_id, check_run_id)`
- `check_suites_current`: latest check suite state (including `repo_full_name`) keyed
  by `(repo_id, check_suite_id)`
- `workflow_runs_current`: latest workflow run state (including `repo_full_name`) keyed
  by `(repo_id, workflow_run_id)`
- `commit_status_current`: latest commit status (including `repo_full_name`) keyed by
  `(repo_id, sha, context)`
- `pr_heads_current`: latest PR head state (including `repo_full_name` and `pr_title`) keyed by
  `(repo_id, pr_number)`
- `sentinel_check_run_state`: internal cache of computed/published sentinel check-run
  state (including `output_title`, `output_summary`, `output_text`) keyed by
  `(repo_id, head_sha, check_name)`

### Quick inspection

```sql
SELECT event, COUNT(*) FROM webhook_events GROUP BY event;
SELECT * FROM check_runs_current LIMIT 20;
SELECT * FROM workflow_runs_current LIMIT 20;
SELECT * FROM commit_status_current LIMIT 20;
SELECT * FROM pr_heads_current LIMIT 20;
SELECT * FROM sentinel_check_run_state LIMIT 20;
```

### Manual DB maintenance

```bash
python -m sentinel.cli migrate-webhook-db
python -m sentinel.cli migrate-webhook-db --path /path/to/webhooks.sqlite3
python -m sentinel.cli vacuum-webhook-db
python -m sentinel.cli vacuum-webhook-db --path /path/to/webhooks.sqlite3
python -m sentinel.cli migrate-webhook-db-zstd
python -m sentinel.cli migrate-webhook-db-zstd --path /path/to/webhooks.sqlite3 --batch-size 1000 --no-backup
python -m sentinel.cli prune-webhook-db
python -m sentinel.cli prune-webhook-db --retention-seconds 86400
python -m sentinel.cli prune-webhook-projections
python -m sentinel.cli prune-webhook-projections --completed-retention-seconds 604800 --active-retention-seconds 2592000
```
