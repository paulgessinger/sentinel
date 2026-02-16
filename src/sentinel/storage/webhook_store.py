from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import threading
from typing import Any, Mapping, Optional, Sequence

from sanic.log import logger

from sentinel.metric import (
    webhook_event_pruned_total,
    webhook_persist_total,
    webhook_project_total,
)


SCHEMA = """
CREATE TABLE IF NOT EXISTS webhook_events (
    delivery_id TEXT PRIMARY KEY,
    received_at TEXT NOT NULL,
    event TEXT NOT NULL,
    action TEXT NULL,
    installation_id INTEGER NULL,
    repo_id INTEGER NULL,
    repo_full_name TEXT NULL,
    payload_json TEXT NOT NULL,
    projected_at TEXT NULL,
    projection_error TEXT NULL
);

CREATE TABLE IF NOT EXISTS check_runs_current (
    repo_id INTEGER NOT NULL,
    check_run_id INTEGER NOT NULL,
    head_sha TEXT NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    conclusion TEXT NULL,
    app_id INTEGER NULL,
    app_slug TEXT NULL,
    check_suite_id INTEGER NULL,
    started_at TEXT NULL,
    completed_at TEXT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_delivery_id TEXT NOT NULL,
    PRIMARY KEY (repo_id, check_run_id)
);

CREATE TABLE IF NOT EXISTS commit_status_current (
    repo_id INTEGER NOT NULL,
    sha TEXT NOT NULL,
    context TEXT NOT NULL,
    status_id INTEGER NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NULL,
    updated_at TEXT NULL,
    url TEXT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_delivery_id TEXT NOT NULL,
    PRIMARY KEY (repo_id, sha, context)
);

CREATE TABLE IF NOT EXISTS pr_heads_current (
    repo_id INTEGER NOT NULL,
    pr_id INTEGER NOT NULL,
    pr_number INTEGER NOT NULL,
    state TEXT NOT NULL,
    head_sha TEXT NOT NULL,
    base_ref TEXT NOT NULL,
    action TEXT NULL,
    updated_at TEXT NOT NULL,
    last_delivery_id TEXT NOT NULL,
    PRIMARY KEY (repo_id, pr_number)
);

CREATE INDEX IF NOT EXISTS idx_webhook_events_event_received_at
    ON webhook_events (event, received_at);
CREATE INDEX IF NOT EXISTS idx_check_runs_current_repo_head_name
    ON check_runs_current (repo_id, head_sha, name);
CREATE INDEX IF NOT EXISTS idx_commit_status_current_repo_sha_context
    ON commit_status_current (repo_id, sha, context);
CREATE INDEX IF NOT EXISTS idx_pr_heads_current_repo_head_sha
    ON pr_heads_current (repo_id, head_sha);
"""


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@dataclass(frozen=True)
class PersistResult:
    inserted: bool
    duplicate: bool
    projection: str
    projection_error: Optional[str] = None
    pruned_rows: int = 0


class WebhookStore:
    def __init__(
        self,
        db_path: str,
        retention_days: int = 30,
        enabled: bool = True,
        events: Optional[Sequence[str]] = None,
        prune_every: int = 500,
    ):
        self.db_path = Path(db_path)
        self.retention_days = retention_days
        self.enabled = enabled
        self.events = {
            event.strip()
            for event in (events or ("check_run", "status", "pull_request"))
            if event.strip()
        }
        self.prune_every = max(1, prune_every)
        self._persisted_since_prune = 0
        self._counter_lock = threading.Lock()

    def initialize(self) -> None:
        if not self.enabled:
            return
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(SCHEMA)

    def should_persist(self, event: str) -> bool:
        return self.enabled and event in self.events

    def persist_event(
        self,
        *,
        delivery_id: str,
        event: str,
        payload: Mapping[str, Any],
        payload_json: str,
    ) -> PersistResult:
        if not self.should_persist(event):
            return PersistResult(
                inserted=False,
                duplicate=False,
                projection=event,
            )

        if not delivery_id:
            raise ValueError("Missing delivery_id")

        now = utcnow_iso()
        action = payload.get("action")
        repo = payload.get("repository") or {}
        installation = payload.get("installation") or {}

        try:
            with self._connect() as conn:
                cursor = conn.execute(
                    """
                    INSERT INTO webhook_events (
                        delivery_id,
                        received_at,
                        event,
                        action,
                        installation_id,
                        repo_id,
                        repo_full_name,
                        payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(delivery_id) DO NOTHING
                    """,
                    (
                        delivery_id,
                        now,
                        event,
                        action,
                        installation.get("id"),
                        repo.get("id"),
                        repo.get("full_name"),
                        payload_json,
                    ),
                )

                if cursor.rowcount == 0:
                    webhook_persist_total.labels(event=event, result="duplicate").inc()
                    return PersistResult(
                        inserted=False,
                        duplicate=True,
                        projection=event,
                    )

                webhook_persist_total.labels(event=event, result="inserted").inc()

                projection_error = None
                try:
                    projection = self._project(conn, event, payload, now, delivery_id)
                except Exception as exc:  # noqa: BLE001
                    projection = event
                    projection_error = str(exc)
                    conn.execute(
                        """
                        UPDATE webhook_events
                        SET projection_error = ?
                        WHERE delivery_id = ?
                        """,
                        (projection_error, delivery_id),
                    )
                    webhook_project_total.labels(
                        projection=projection, result="error"
                    ).inc()
                    logger.error(
                        "Webhook projection failed for delivery %s, event=%s",
                        delivery_id,
                        event,
                        exc_info=True,
                    )
                else:
                    conn.execute(
                        """
                        UPDATE webhook_events
                        SET projected_at = ?, projection_error = NULL
                        WHERE delivery_id = ?
                        """,
                        (now, delivery_id),
                    )
                    webhook_project_total.labels(
                        projection=projection, result="upserted"
                    ).inc()
        except Exception:  # noqa: BLE001
            webhook_persist_total.labels(event=event, result="error").inc()
            raise

        pruned_rows = 0
        if self._should_prune():
            pruned_rows = self.prune_old_events()

        return PersistResult(
            inserted=True,
            duplicate=False,
            projection=event,
            projection_error=projection_error,
            pruned_rows=pruned_rows,
        )

    def prune_old_events(self) -> int:
        if not self.enabled:
            return 0

        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=self.retention_days)
        ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM webhook_events WHERE received_at < ?",
                (cutoff,),
            )
            count = cursor.rowcount

        if count > 0:
            webhook_event_pruned_total.inc(count)
        return count

    def _project(
        self,
        conn: sqlite3.Connection,
        event: str,
        payload: Mapping[str, Any],
        now: str,
        delivery_id: str,
    ) -> str:
        if event == "check_run":
            self._project_check_run(conn, payload, now, delivery_id)
            return "check_run"
        if event == "status":
            self._project_status(conn, payload, now, delivery_id)
            return "status"
        if event == "pull_request":
            self._project_pull_request(conn, payload, now, delivery_id)
            return "pull_request"

        webhook_project_total.labels(projection=event, result="skipped").inc()
        return event

    def _project_check_run(
        self,
        conn: sqlite3.Connection,
        payload: Mapping[str, Any],
        now: str,
        delivery_id: str,
    ) -> None:
        repo = payload["repository"]
        check_run = payload["check_run"]
        app = check_run.get("app") or {}
        check_suite = check_run.get("check_suite") or {}

        conn.execute(
            """
            INSERT INTO check_runs_current (
                repo_id,
                check_run_id,
                head_sha,
                name,
                status,
                conclusion,
                app_id,
                app_slug,
                check_suite_id,
                started_at,
                completed_at,
                first_seen_at,
                last_seen_at,
                last_delivery_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(repo_id, check_run_id) DO UPDATE SET
                head_sha = excluded.head_sha,
                name = excluded.name,
                status = excluded.status,
                conclusion = excluded.conclusion,
                app_id = excluded.app_id,
                app_slug = excluded.app_slug,
                check_suite_id = excluded.check_suite_id,
                started_at = excluded.started_at,
                completed_at = excluded.completed_at,
                last_seen_at = excluded.last_seen_at,
                last_delivery_id = excluded.last_delivery_id
            """,
            (
                repo["id"],
                check_run["id"],
                check_run["head_sha"],
                check_run["name"],
                check_run["status"],
                check_run.get("conclusion"),
                app.get("id"),
                app.get("slug"),
                check_suite.get("id"),
                check_run.get("started_at"),
                check_run.get("completed_at"),
                now,
                now,
                delivery_id,
            ),
        )

    def _project_status(
        self,
        conn: sqlite3.Connection,
        payload: Mapping[str, Any],
        now: str,
        delivery_id: str,
    ) -> None:
        repo = payload["repository"]

        conn.execute(
            """
            INSERT INTO commit_status_current (
                repo_id,
                sha,
                context,
                status_id,
                state,
                created_at,
                updated_at,
                url,
                first_seen_at,
                last_seen_at,
                last_delivery_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(repo_id, sha, context) DO UPDATE SET
                status_id = excluded.status_id,
                state = excluded.state,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                url = excluded.url,
                last_seen_at = excluded.last_seen_at,
                last_delivery_id = excluded.last_delivery_id
            """,
            (
                repo["id"],
                payload["sha"],
                payload["context"],
                payload["id"],
                payload["state"],
                payload.get("created_at"),
                payload.get("updated_at"),
                payload.get("url"),
                now,
                now,
                delivery_id,
            ),
        )

    def _project_pull_request(
        self,
        conn: sqlite3.Connection,
        payload: Mapping[str, Any],
        now: str,
        delivery_id: str,
    ) -> None:
        repo = payload["repository"]
        pr = payload["pull_request"]

        conn.execute(
            """
            INSERT INTO pr_heads_current (
                repo_id,
                pr_id,
                pr_number,
                state,
                head_sha,
                base_ref,
                action,
                updated_at,
                last_delivery_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(repo_id, pr_number) DO UPDATE SET
                pr_id = excluded.pr_id,
                state = excluded.state,
                head_sha = excluded.head_sha,
                base_ref = excluded.base_ref,
                action = excluded.action,
                updated_at = excluded.updated_at,
                last_delivery_id = excluded.last_delivery_id
            """,
            (
                repo["id"],
                pr["id"],
                pr["number"],
                pr["state"],
                pr["head"]["sha"],
                pr["base"]["ref"],
                payload.get("action"),
                pr.get("updated_at") or now,
                delivery_id,
            ),
        )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def _should_prune(self) -> bool:
        with self._counter_lock:
            self._persisted_since_prune += 1
            if self._persisted_since_prune < self.prune_every:
                return False
            self._persisted_since_prune = 0
            return True

    @staticmethod
    def payload_to_json(payload: Mapping[str, Any]) -> str:
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)
