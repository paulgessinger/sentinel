import os
import dotenv
import logging
from pathlib import Path

dotenv.load_dotenv()

# GitHub App credentials and webhook verification.
GITHUB_WEBHOOK_SECRET = os.environ["GITHUB_WEBHOOK_SECRET"]
GITHUB_PRIVATE_KEY = os.environ["GITHUB_PRIVATE_KEY"]
GITHUB_APP_ID = int(os.environ["GITHUB_APP_ID"])

# Global log level override for the service.
OVERRIDE_LOGGING = getattr(
    logging, os.environ.get("OVERRIDE_LOGGING", "WARNING").upper(), logging.WARNING
)

# Optional allowlist of private repositories to handle.
REPO_ALLOWLIST = os.environ.get("REPO_ALLOWLIST")
if REPO_ALLOWLIST is not None:
    REPO_ALLOWLIST = REPO_ALLOWLIST.split(",")


# Optional notification integration credentials.
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# Optional local path to override .merge-sentinel.yml during development.
OVERRIDE_CONFIG = os.environ.get("OVERRIDE_CONFIG")

# Startup pause for worker/process boot sequencing.
PROCESS_START_PAUSE = float(os.environ.get("PROCESS_START_PAUSE", 5))

# Regex to skip specific check_run names/status contexts early.
CHECK_RUN_NAME_FILTER = os.environ.get("CHECK_RUN_NAME_FILTER")

# Diskcache root (required) used by queue/cache and default SQLite path.
DISKCACHE_DIR = os.environ["DISKCACHE_DIR"]

# Cooldown window per PR before reprocessing from queue.
PR_TIMEOUT = float(os.environ.get("PR_TIMEOUT", 10))

# Worker polling interval for queue loop.
WORKER_SLEEP = float(os.environ.get("WORKER_SLEEP", 1))

# Installation token cache TTL in seconds.
ACCESS_TOKEN_TTL = float(os.environ.get("ACCESS_TOKEN_TTL", 300))

# TTL for cached repo PR lists used by legacy webhook dispatch path.
PRS_TTL = float(os.environ.get("PRS_TTL", 60))

# Legacy worker dry-run: compute only, skip GitHub writes.
DRY_RUN = os.environ.get("DRY_RUN", "false") == "true"

# Legacy debounce window for duplicate check/status webhook suppression.
CHECK_RUN_DEBOUNCE_WINDOW = float(os.environ.get("CHECK_RUN_DEBOUNCE_WINDOW", 60 * 60))

# Enable/disable webhook SQLite persistence and projections.
WEBHOOK_DB_ENABLED = os.environ.get("WEBHOOK_DB_ENABLED", "true").lower() == "true"
# SQLite file path for webhook event log + projection tables.
WEBHOOK_DB_PATH = os.environ.get(
    "WEBHOOK_DB_PATH", str(Path(DISKCACHE_DIR) / "webhooks.sqlite3")
)
# Event retention in seconds (preferred), with days fallback for compatibility.
_webhook_retention_seconds_raw = os.environ.get("WEBHOOK_DB_RETENTION_SECONDS")
if _webhook_retention_seconds_raw is not None:
    WEBHOOK_DB_RETENTION_SECONDS = int(_webhook_retention_seconds_raw)
else:
    WEBHOOK_DB_RETENTION_SECONDS = int(
        float(os.environ.get("WEBHOOK_DB_RETENTION_DAYS", 30)) * 24 * 60 * 60
    )
# Event types that should be persisted/projected into SQLite.
WEBHOOK_DB_EVENTS = tuple(
    part.strip()
    for part in os.environ.get(
        "WEBHOOK_DB_EVENTS", "check_run,check_suite,workflow_run,status,pull_request"
    ).split(",")
    if part.strip()
)

# Enable legacy gidgethub router dispatch/queue path after persistence.
WEBHOOK_DISPATCH_ENABLED = (
    os.environ.get("WEBHOOK_DISPATCH_ENABLED", "false").lower() == "true"
)

# Enable periodic pruning for projection current-state tables.
WEBHOOK_PROJECTION_PRUNE_ENABLED = (
    os.environ.get("WEBHOOK_PROJECTION_PRUNE_ENABLED", "true").lower() == "true"
)
# Retention window for terminal projection rows (completed/closed/success/failure).
WEBHOOK_PROJECTION_COMPLETED_RETENTION_SECONDS = int(
    os.environ.get("WEBHOOK_PROJECTION_COMPLETED_RETENTION_SECONDS", 7 * 24 * 60 * 60)
)
# Retention window for active projection rows (in_progress/pending/open).
WEBHOOK_PROJECTION_ACTIVE_RETENTION_SECONDS = int(
    os.environ.get("WEBHOOK_PROJECTION_ACTIVE_RETENTION_SECONDS", 30 * 24 * 60 * 60)
)

# Skip webhooks emitted by this app id (prevents self-feedback loops).
WEBHOOK_FILTER_SELF_APP_ID = (
    os.environ.get("WEBHOOK_FILTER_SELF_APP_ID", "true").lower() == "true"
)
# Additional comma-separated app ids to ignore at ingest time.
WEBHOOK_FILTER_APP_IDS = tuple(
    int(part.strip())
    for part in os.environ.get("WEBHOOK_FILTER_APP_IDS", "").split(",")
    if part.strip()
)

# Enable projection-driven check-run evaluation after projection commit.
PROJECTION_EVAL_ENABLED = (
    os.environ.get("PROJECTION_EVAL_ENABLED", "false").lower() == "true"
)
# When false, evaluate and persist local state but do not post check runs.
PROJECTION_PUBLISH_ENABLED = (
    os.environ.get("PROJECTION_PUBLISH_ENABLED", "false").lower() == "true"
)
# Debounce window for per-(repo_id, head_sha) evaluation scheduling.
PROJECTION_DEBOUNCE_SECONDS = float(os.environ.get("PROJECTION_DEBOUNCE_SECONDS", 2))
# Additional wait window after pull_request synchronize/opened/reopened before
# evaluating, to allow late-arriving check runs from other systems.
PROJECTION_PULL_REQUEST_SYNCHRONIZE_DELAY_SECONDS = float(
    os.environ.get("PROJECTION_PULL_REQUEST_SYNCHRONIZE_DELAY_SECONDS", 15)
)
# Name used for Sentinel's own check run.
PROJECTION_CHECK_RUN_NAME = os.environ.get(
    "PROJECTION_CHECK_RUN_NAME", "merge-sentinel"
)
# Enable a manual check-run action button that requests immediate re-evaluation.
PROJECTION_MANUAL_REFRESH_ACTION_ENABLED = (
    os.environ.get("PROJECTION_MANUAL_REFRESH_ACTION_ENABLED", "true").lower() == "true"
)
# GitHub requested_action identifier used by Sentinel re-evaluation button.
PROJECTION_MANUAL_REFRESH_ACTION_IDENTIFIER = os.environ.get(
    "PROJECTION_MANUAL_REFRESH_ACTION_IDENTIFIER", "refresh_from_api"
)
# Label shown for the manual re-evaluation action in GitHub UI.
PROJECTION_MANUAL_REFRESH_ACTION_LABEL = os.environ.get(
    "PROJECTION_MANUAL_REFRESH_ACTION_LABEL", "Re-evaluate now"
)
# Description shown for the manual re-evaluation action in GitHub UI.
PROJECTION_MANUAL_REFRESH_ACTION_DESCRIPTION = os.environ.get(
    "PROJECTION_MANUAL_REFRESH_ACTION_DESCRIPTION",
    "Force refresh checks from GitHub and re-evaluate",
)
# Cache TTL for repository config fetches (.merge-sentinel.yml).
PROJECTION_CONFIG_CACHE_SECONDS = int(
    os.environ.get("PROJECTION_CONFIG_CACHE_SECONDS", 300)
)
# Cache TTL for pull request file-list fallback fetches.
PROJECTION_PR_FILES_CACHE_SECONDS = int(
    os.environ.get("PROJECTION_PR_FILES_CACHE_SECONDS", 86400)
)
# Allow path-filter rule fallback to API (PR files) when projections lack file data.
PROJECTION_PATH_RULE_FALLBACK_ENABLED = (
    os.environ.get("PROJECTION_PATH_RULE_FALLBACK_ENABLED", "true").lower() == "true"
)
