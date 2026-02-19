from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated, Any

import dotenv
from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

dotenv.load_dotenv()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
        case_sensitive=True,
        env_ignore_empty=True,
    )

    # GitHub App credentials and webhook verification.
    GITHUB_WEBHOOK_SECRET: str
    GITHUB_PRIVATE_KEY: str
    GITHUB_APP_ID: int

    # Global log level override for the service.
    OVERRIDE_LOGGING: int = logging.WARNING

    # Optional allowlist of private repositories to handle.
    REPO_ALLOWLIST: Annotated[list[str] | None, NoDecode] = None

    # Optional notification integration credentials.
    TELEGRAM_TOKEN: str | None = None
    TELEGRAM_CHAT_ID: str | None = None

    # Optional local path to override .merge-sentinel.yml during development.
    OVERRIDE_CONFIG: str | None = None

    # Startup pause for worker/process boot sequencing.
    PROCESS_START_PAUSE: float = 5

    # Regex to skip specific check_run names/status contexts early.
    CHECK_RUN_NAME_FILTER: str | None = None

    # Diskcache root (required) used by queue/cache and default SQLite path.
    DISKCACHE_DIR: str

    # Cooldown window per PR before reprocessing from queue.
    PR_TIMEOUT: float = 10

    # Worker polling interval for queue loop.
    WORKER_SLEEP: float = 1

    # Installation token cache TTL in seconds.
    ACCESS_TOKEN_TTL: float = 300

    # TTL for cached repo PR lists used by legacy webhook dispatch path.
    PRS_TTL: float = 60

    # Legacy worker dry-run: compute only, skip GitHub writes.
    DRY_RUN: bool = False

    # Legacy debounce window for duplicate check/status webhook suppression.
    CHECK_RUN_DEBOUNCE_WINDOW: float = 60 * 60

    # Enable/disable webhook SQLite persistence and projections.
    WEBHOOK_DB_ENABLED: bool = True
    # SQLite file path for webhook event log + projection tables.
    WEBHOOK_DB_PATH: str | None = None
    # Event retention in seconds (preferred), with days fallback for compatibility.
    WEBHOOK_DB_RETENTION_SECONDS: int | None = None
    WEBHOOK_DB_RETENTION_DAYS: float = 30
    # Retention window for internal projection activity events shown in PR detail logs.
    WEBHOOK_ACTIVITY_RETENTION_SECONDS: int | None = None
    # Event types that should be persisted/projected into SQLite.
    WEBHOOK_DB_EVENTS: Annotated[tuple[str, ...], NoDecode] = (
        "check_run",
        "check_suite",
        "workflow_run",
        "status",
        "pull_request",
    )

    # Enable legacy gidgethub router dispatch/queue path after persistence.
    WEBHOOK_DISPATCH_ENABLED: bool = False

    # Enable periodic pruning for projection current-state tables.
    WEBHOOK_PROJECTION_PRUNE_ENABLED: bool = True
    # Retention window for terminal projection rows (completed/closed/success/failure).
    WEBHOOK_PROJECTION_COMPLETED_RETENTION_SECONDS: int = 7 * 24 * 60 * 60
    # Retention window for active projection rows (in_progress/pending/open).
    WEBHOOK_PROJECTION_ACTIVE_RETENTION_SECONDS: int = 30 * 24 * 60 * 60

    # Skip webhooks emitted by this app id (prevents self-feedback loops).
    WEBHOOK_FILTER_SELF_APP_ID: bool = True
    # Additional comma-separated app ids to ignore at ingest time.
    WEBHOOK_FILTER_APP_IDS: Annotated[tuple[int, ...], NoDecode] = ()

    # Enable projection-driven check-run evaluation after projection commit.
    PROJECTION_EVAL_ENABLED: bool = False
    # When false, evaluate and persist local state but do not post check runs.
    PROJECTION_PUBLISH_ENABLED: bool = False
    # Debounce window for per-(repo_id, head_sha) evaluation scheduling.
    PROJECTION_DEBOUNCE_SECONDS: float = 2
    # Additional wait window after pull_request synchronize/opened/reopened before
    # evaluating, to allow late-arriving check runs from other systems.
    PROJECTION_PULL_REQUEST_SYNCHRONIZE_DELAY_SECONDS: float = 15
    # Name used for Sentinel's own check run.
    PROJECTION_CHECK_RUN_NAME: str = "merge-sentinel"
    # Enable a manual check-run action button that requests immediate re-evaluation.
    PROJECTION_MANUAL_REFRESH_ACTION_ENABLED: bool = True
    # GitHub requested_action identifier used by Sentinel re-evaluation button.
    PROJECTION_MANUAL_REFRESH_ACTION_IDENTIFIER: str = "refresh_from_api"
    # Label shown for the manual re-evaluation action in GitHub UI.
    PROJECTION_MANUAL_REFRESH_ACTION_LABEL: str = "Re-evaluate now"
    # Description shown for the manual re-evaluation action in GitHub UI.
    PROJECTION_MANUAL_REFRESH_ACTION_DESCRIPTION: str = (
        "Force refresh checks from GitHub and re-evaluate"
    )
    # Cache TTL for repository config fetches (.merge-sentinel.yml).
    PROJECTION_CONFIG_CACHE_SECONDS: int = 300
    # Cache TTL for pull request file-list fallback fetches.
    PROJECTION_PR_FILES_CACHE_SECONDS: int = 86400
    # Allow path-filter rule fallback to API (PR files) when projections lack file data.
    PROJECTION_PATH_RULE_FALLBACK_ENABLED: bool = True
    # Auto-refresh projections from API when evaluation fails only due to missing
    # required-pattern matches and the PR projection appears stale.
    PROJECTION_AUTO_REFRESH_ON_MISSING_ENABLED: bool = True
    # Minimum PR projection age before missing-pattern auto-refresh is attempted.
    PROJECTION_AUTO_REFRESH_ON_MISSING_STALE_SECONDS: int = 1800
    # Cooldown per (repo_id, head_sha) between automatic missing-pattern refreshes.
    PROJECTION_AUTO_REFRESH_ON_MISSING_COOLDOWN_SECONDS: int = 300

    # Optional URL to redirect the index page (/) to.
    INDEX_REDIRECT_URL: str | None = None

    @field_validator("OVERRIDE_LOGGING", mode="before")
    @classmethod
    def _parse_log_level(cls, value: Any) -> int:
        if value is None:
            return logging.WARNING
        if isinstance(value, int):
            return value
        return getattr(logging, str(value).upper(), logging.WARNING)

    @field_validator("REPO_ALLOWLIST", mode="before")
    @classmethod
    def _parse_repo_allowlist(cls, value: Any) -> list[str] | None:
        if value is None:
            return None
        if isinstance(value, str):
            parts = [part.strip() for part in value.split(",") if part.strip()]
            return parts or None
        if isinstance(value, (list, tuple, set)):
            parts = [str(part).strip() for part in value if str(part).strip()]
            return parts or None
        return None

    @field_validator("WEBHOOK_DB_EVENTS", mode="before")
    @classmethod
    def _parse_webhook_events(cls, value: Any) -> tuple[str, ...]:
        if value is None:
            return (
                "check_run",
                "check_suite",
                "workflow_run",
                "status",
                "pull_request",
            )
        if isinstance(value, str):
            return tuple(part.strip() for part in value.split(",") if part.strip())
        if isinstance(value, (list, tuple, set)):
            return tuple(str(part).strip() for part in value if str(part).strip())
        return ()

    @field_validator("WEBHOOK_FILTER_APP_IDS", mode="before")
    @classmethod
    def _parse_webhook_filter_app_ids(cls, value: Any) -> tuple[int, ...]:
        if value is None:
            return ()
        if isinstance(value, str):
            parts = [part.strip() for part in value.split(",") if part.strip()]
            return tuple(int(part) for part in parts)
        if isinstance(value, (list, tuple, set)):
            return tuple(int(part) for part in value)
        return ()

    @model_validator(mode="after")
    def _derive_paths_and_retention(self) -> Settings:
        if not self.WEBHOOK_DB_PATH:
            self.WEBHOOK_DB_PATH = str(Path(self.DISKCACHE_DIR) / "webhooks.sqlite3")
        if self.WEBHOOK_DB_RETENTION_SECONDS is None:
            self.WEBHOOK_DB_RETENTION_SECONDS = int(
                float(self.WEBHOOK_DB_RETENTION_DAYS) * 24 * 60 * 60
            )
        if self.WEBHOOK_ACTIVITY_RETENTION_SECONDS is None:
            self.WEBHOOK_ACTIVITY_RETENTION_SECONDS = self.WEBHOOK_DB_RETENTION_SECONDS
        return self


SETTINGS = Settings()  # ty: ignore[missing-argument]

if SETTINGS.WEBHOOK_DB_PATH is None:
    raise RuntimeError("WEBHOOK_DB_PATH should never be None after settings load")
if SETTINGS.WEBHOOK_DB_RETENTION_SECONDS is None:
    raise RuntimeError(
        "WEBHOOK_DB_RETENTION_SECONDS should never be None after settings load"
    )
if SETTINGS.WEBHOOK_ACTIVITY_RETENTION_SECONDS is None:
    raise RuntimeError(
        "WEBHOOK_ACTIVITY_RETENTION_SECONDS should never be None after settings load"
    )

GITHUB_WEBHOOK_SECRET: str = SETTINGS.GITHUB_WEBHOOK_SECRET
GITHUB_PRIVATE_KEY: str = SETTINGS.GITHUB_PRIVATE_KEY
GITHUB_APP_ID: int = SETTINGS.GITHUB_APP_ID
OVERRIDE_LOGGING: int = SETTINGS.OVERRIDE_LOGGING
REPO_ALLOWLIST: list[str] | None = SETTINGS.REPO_ALLOWLIST
TELEGRAM_TOKEN: str | None = SETTINGS.TELEGRAM_TOKEN
TELEGRAM_CHAT_ID: str | None = SETTINGS.TELEGRAM_CHAT_ID
OVERRIDE_CONFIG: str | None = SETTINGS.OVERRIDE_CONFIG
PROCESS_START_PAUSE: float = SETTINGS.PROCESS_START_PAUSE
CHECK_RUN_NAME_FILTER: str | None = SETTINGS.CHECK_RUN_NAME_FILTER
DISKCACHE_DIR: str = SETTINGS.DISKCACHE_DIR
PR_TIMEOUT: float = SETTINGS.PR_TIMEOUT
WORKER_SLEEP: float = SETTINGS.WORKER_SLEEP
ACCESS_TOKEN_TTL: float = SETTINGS.ACCESS_TOKEN_TTL
PRS_TTL: float = SETTINGS.PRS_TTL
DRY_RUN: bool = SETTINGS.DRY_RUN
CHECK_RUN_DEBOUNCE_WINDOW: float = SETTINGS.CHECK_RUN_DEBOUNCE_WINDOW
WEBHOOK_DB_ENABLED: bool = SETTINGS.WEBHOOK_DB_ENABLED
WEBHOOK_DB_PATH: str = SETTINGS.WEBHOOK_DB_PATH
WEBHOOK_DB_RETENTION_SECONDS: int = SETTINGS.WEBHOOK_DB_RETENTION_SECONDS
WEBHOOK_DB_RETENTION_DAYS: float = SETTINGS.WEBHOOK_DB_RETENTION_DAYS
WEBHOOK_ACTIVITY_RETENTION_SECONDS: int = SETTINGS.WEBHOOK_ACTIVITY_RETENTION_SECONDS
WEBHOOK_DB_EVENTS: tuple[str, ...] = SETTINGS.WEBHOOK_DB_EVENTS
WEBHOOK_DISPATCH_ENABLED: bool = SETTINGS.WEBHOOK_DISPATCH_ENABLED
WEBHOOK_PROJECTION_PRUNE_ENABLED: bool = SETTINGS.WEBHOOK_PROJECTION_PRUNE_ENABLED
WEBHOOK_PROJECTION_COMPLETED_RETENTION_SECONDS: int = (
    SETTINGS.WEBHOOK_PROJECTION_COMPLETED_RETENTION_SECONDS
)
WEBHOOK_PROJECTION_ACTIVE_RETENTION_SECONDS: int = (
    SETTINGS.WEBHOOK_PROJECTION_ACTIVE_RETENTION_SECONDS
)
WEBHOOK_FILTER_SELF_APP_ID: bool = SETTINGS.WEBHOOK_FILTER_SELF_APP_ID
WEBHOOK_FILTER_APP_IDS: tuple[int, ...] = SETTINGS.WEBHOOK_FILTER_APP_IDS
PROJECTION_EVAL_ENABLED: bool = SETTINGS.PROJECTION_EVAL_ENABLED
PROJECTION_PUBLISH_ENABLED: bool = SETTINGS.PROJECTION_PUBLISH_ENABLED
PROJECTION_DEBOUNCE_SECONDS: float = SETTINGS.PROJECTION_DEBOUNCE_SECONDS
PROJECTION_PULL_REQUEST_SYNCHRONIZE_DELAY_SECONDS: float = (
    SETTINGS.PROJECTION_PULL_REQUEST_SYNCHRONIZE_DELAY_SECONDS
)
PROJECTION_CHECK_RUN_NAME: str = SETTINGS.PROJECTION_CHECK_RUN_NAME
PROJECTION_MANUAL_REFRESH_ACTION_ENABLED: bool = (
    SETTINGS.PROJECTION_MANUAL_REFRESH_ACTION_ENABLED
)
PROJECTION_MANUAL_REFRESH_ACTION_IDENTIFIER: str = (
    SETTINGS.PROJECTION_MANUAL_REFRESH_ACTION_IDENTIFIER
)
PROJECTION_MANUAL_REFRESH_ACTION_LABEL: str = (
    SETTINGS.PROJECTION_MANUAL_REFRESH_ACTION_LABEL
)
PROJECTION_MANUAL_REFRESH_ACTION_DESCRIPTION: str = (
    SETTINGS.PROJECTION_MANUAL_REFRESH_ACTION_DESCRIPTION
)
PROJECTION_CONFIG_CACHE_SECONDS: int = SETTINGS.PROJECTION_CONFIG_CACHE_SECONDS
PROJECTION_PR_FILES_CACHE_SECONDS: int = SETTINGS.PROJECTION_PR_FILES_CACHE_SECONDS
PROJECTION_PATH_RULE_FALLBACK_ENABLED: bool = (
    SETTINGS.PROJECTION_PATH_RULE_FALLBACK_ENABLED
)
PROJECTION_AUTO_REFRESH_ON_MISSING_ENABLED: bool = (
    SETTINGS.PROJECTION_AUTO_REFRESH_ON_MISSING_ENABLED
)
PROJECTION_AUTO_REFRESH_ON_MISSING_STALE_SECONDS: int = (
    SETTINGS.PROJECTION_AUTO_REFRESH_ON_MISSING_STALE_SECONDS
)
PROJECTION_AUTO_REFRESH_ON_MISSING_COOLDOWN_SECONDS: int = (
    SETTINGS.PROJECTION_AUTO_REFRESH_ON_MISSING_COOLDOWN_SECONDS
)
INDEX_REDIRECT_URL: str | None = SETTINGS.INDEX_REDIRECT_URL
