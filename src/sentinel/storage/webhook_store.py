from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import threading
from typing import Any, Dict, Mapping, Optional, Sequence

from sanic.log import logger
from sqlalchemy import (
    Column,
    Engine,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    and_,
    create_engine,
    delete,
    event as sa_event,
    or_,
    update,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection

from sentinel.metric import (
    webhook_event_pruned_total,
    webhook_persist_total,
    webhook_project_total,
    webhook_projection_pruned_total,
)


metadata = MetaData()

webhook_events = Table(
    "webhook_events",
    metadata,
    Column("delivery_id", String, primary_key=True),
    Column("received_at", String, nullable=False),
    Column("event", String, nullable=False),
    Column("action", String),
    Column("installation_id", Integer),
    Column("repo_id", Integer),
    Column("repo_full_name", String),
    Column("payload_json", String, nullable=False),
    Column("projected_at", String),
    Column("projection_error", String),
)

check_runs_current = Table(
    "check_runs_current",
    metadata,
    Column("repo_id", Integer, primary_key=True),
    Column("repo_full_name", String),
    Column("check_run_id", Integer, primary_key=True),
    Column("head_sha", String, nullable=False),
    Column("name", String, nullable=False),
    Column("status", String, nullable=False),
    Column("conclusion", String),
    Column("app_id", Integer),
    Column("app_slug", String),
    Column("check_suite_id", Integer),
    Column("started_at", String),
    Column("completed_at", String),
    Column("first_seen_at", String, nullable=False),
    Column("last_seen_at", String, nullable=False),
    Column("last_delivery_id", String, nullable=False),
)

check_suites_current = Table(
    "check_suites_current",
    metadata,
    Column("repo_id", Integer, primary_key=True),
    Column("repo_full_name", String),
    Column("check_suite_id", Integer, primary_key=True),
    Column("head_sha", String, nullable=False),
    Column("head_branch", String),
    Column("status", String),
    Column("conclusion", String),
    Column("app_id", Integer),
    Column("app_slug", String),
    Column("created_at", String),
    Column("updated_at", String),
    Column("first_seen_at", String, nullable=False),
    Column("last_seen_at", String, nullable=False),
    Column("last_delivery_id", String, nullable=False),
)

workflow_runs_current = Table(
    "workflow_runs_current",
    metadata,
    Column("repo_id", Integer, primary_key=True),
    Column("repo_full_name", String),
    Column("workflow_run_id", Integer, primary_key=True),
    Column("name", String, nullable=False),
    Column("event", String),
    Column("status", String),
    Column("conclusion", String),
    Column("head_sha", String, nullable=False),
    Column("run_number", Integer),
    Column("workflow_id", Integer),
    Column("check_suite_id", Integer),
    Column("app_id", Integer),
    Column("app_slug", String),
    Column("created_at", String),
    Column("updated_at", String),
    Column("first_seen_at", String, nullable=False),
    Column("last_seen_at", String, nullable=False),
    Column("last_delivery_id", String, nullable=False),
)

commit_status_current = Table(
    "commit_status_current",
    metadata,
    Column("repo_id", Integer, primary_key=True),
    Column("repo_full_name", String),
    Column("sha", String, primary_key=True),
    Column("context", String, primary_key=True),
    Column("status_id", Integer, nullable=False),
    Column("state", String, nullable=False),
    Column("created_at", String),
    Column("updated_at", String),
    Column("url", String),
    Column("first_seen_at", String, nullable=False),
    Column("last_seen_at", String, nullable=False),
    Column("last_delivery_id", String, nullable=False),
)

pr_heads_current = Table(
    "pr_heads_current",
    metadata,
    Column("repo_id", Integer, primary_key=True),
    Column("repo_full_name", String),
    Column("pr_id", Integer, nullable=False),
    Column("pr_number", Integer, primary_key=True),
    Column("state", String, nullable=False),
    Column("head_sha", String, nullable=False),
    Column("base_ref", String, nullable=False),
    Column("action", String),
    Column("updated_at", String, nullable=False),
    Column("last_delivery_id", String, nullable=False),
)

Index(
    "idx_webhook_events_event_received_at",
    webhook_events.c.event,
    webhook_events.c.received_at,
)
Index(
    "idx_check_runs_current_repo_head_name",
    check_runs_current.c.repo_id,
    check_runs_current.c.head_sha,
    check_runs_current.c.name,
)
Index(
    "idx_check_suites_current_repo_head_sha",
    check_suites_current.c.repo_id,
    check_suites_current.c.head_sha,
)
Index(
    "idx_workflow_runs_current_repo_head_name",
    workflow_runs_current.c.repo_id,
    workflow_runs_current.c.head_sha,
    workflow_runs_current.c.name,
)
Index(
    "idx_commit_status_current_repo_sha_context",
    commit_status_current.c.repo_id,
    commit_status_current.c.sha,
    commit_status_current.c.context,
)
Index(
    "idx_pr_heads_current_repo_head_sha",
    pr_heads_current.c.repo_id,
    pr_heads_current.c.head_sha,
)


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
        retention_seconds: int = 30 * 24 * 60 * 60,
        projection_completed_retention_seconds: Optional[int] = None,
        projection_active_retention_seconds: Optional[int] = None,
        enabled: bool = True,
        events: Optional[Sequence[str]] = None,
        prune_every: int = 500,
    ):
        self.db_path = Path(db_path)
        self.retention_seconds = retention_seconds
        self.projection_completed_retention_seconds = (
            projection_completed_retention_seconds
        )
        self.projection_active_retention_seconds = projection_active_retention_seconds
        self.enabled = enabled
        self.events = {
            event.strip()
            for event in (
                events
                or (
                    "check_run",
                    "check_suite",
                    "workflow_run",
                    "status",
                    "pull_request",
                )
            )
            if event.strip()
        }
        self.prune_every = max(1, prune_every)
        self._persisted_since_prune = 0
        self._counter_lock = threading.Lock()
        self.engine = self._build_engine(self.db_path)

    def initialize(self) -> None:
        if not self.enabled:
            return
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        metadata.create_all(self.engine)

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

        insert_event = sqlite_insert(webhook_events).values(
            delivery_id=delivery_id,
            received_at=now,
            event=event,
            action=action,
            installation_id=installation.get("id"),
            repo_id=repo.get("id"),
            repo_full_name=repo.get("full_name"),
            payload_json=payload_json,
        )
        insert_event = insert_event.on_conflict_do_nothing(
            index_elements=[webhook_events.c.delivery_id]
        )
        projection = event
        projection_error = None

        try:
            with self.engine.begin() as conn:
                insert_result = conn.execute(insert_event)
                if (insert_result.rowcount or 0) == 0:
                    webhook_persist_total.labels(event=event, result="duplicate").inc()
                    return PersistResult(
                        inserted=False,
                        duplicate=True,
                        projection=event,
                    )

                webhook_persist_total.labels(event=event, result="inserted").inc()

                try:
                    projection = self._project(conn, event, payload, now, delivery_id)
                except Exception as exc:  # noqa: BLE001
                    projection = event
                    projection_error = str(exc)
                    conn.execute(
                        update(webhook_events)
                        .where(webhook_events.c.delivery_id == delivery_id)
                        .values(projection_error=projection_error)
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
                        update(webhook_events)
                        .where(webhook_events.c.delivery_id == delivery_id)
                        .values(projected_at=now, projection_error=None)
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
            if (
                self.projection_completed_retention_seconds is not None
                and self.projection_active_retention_seconds is not None
            ):
                self.prune_old_projections(
                    completed_retention_seconds=(
                        self.projection_completed_retention_seconds
                    ),
                    active_retention_seconds=self.projection_active_retention_seconds,
                )

        return PersistResult(
            inserted=True,
            duplicate=False,
            projection=projection,
            projection_error=projection_error,
            pruned_rows=pruned_rows,
        )

    def prune_old_events(self, retention_seconds: Optional[int] = None) -> int:
        if not self.enabled:
            return 0

        retention_seconds = (
            self.retention_seconds
            if retention_seconds is None
            else max(0, retention_seconds)
        )
        cutoff = (
            datetime.now(timezone.utc) - timedelta(seconds=retention_seconds)
        ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        with self.engine.begin() as conn:
            result = conn.execute(
                delete(webhook_events).where(webhook_events.c.received_at < cutoff)
            )
            count = result.rowcount or 0

        if count > 0:
            webhook_event_pruned_total.inc(count)
        return count

    def prune_old_projections(
        self, *, completed_retention_seconds: int, active_retention_seconds: int
    ) -> Dict[str, int]:
        if not self.enabled:
            return {}

        completed_retention_seconds = max(0, completed_retention_seconds)
        active_retention_seconds = max(0, active_retention_seconds)
        completed_cutoff = (
            datetime.now(timezone.utc) - timedelta(seconds=completed_retention_seconds)
        ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        active_cutoff = (
            datetime.now(timezone.utc) - timedelta(seconds=active_retention_seconds)
        ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        statements = (
            (
                "check_runs_current",
                "completed",
                delete(check_runs_current).where(
                    and_(
                        check_runs_current.c.status == "completed",
                        check_runs_current.c.last_seen_at < completed_cutoff,
                    )
                ),
            ),
            (
                "check_runs_current",
                "active",
                delete(check_runs_current).where(
                    and_(
                        or_(
                            check_runs_current.c.status.is_(None),
                            check_runs_current.c.status != "completed",
                        ),
                        check_runs_current.c.last_seen_at < active_cutoff,
                    )
                ),
            ),
            (
                "check_suites_current",
                "completed",
                delete(check_suites_current).where(
                    and_(
                        check_suites_current.c.status == "completed",
                        check_suites_current.c.last_seen_at < completed_cutoff,
                    )
                ),
            ),
            (
                "check_suites_current",
                "active",
                delete(check_suites_current).where(
                    and_(
                        or_(
                            check_suites_current.c.status.is_(None),
                            check_suites_current.c.status != "completed",
                        ),
                        check_suites_current.c.last_seen_at < active_cutoff,
                    )
                ),
            ),
            (
                "workflow_runs_current",
                "completed",
                delete(workflow_runs_current).where(
                    and_(
                        workflow_runs_current.c.status == "completed",
                        workflow_runs_current.c.last_seen_at < completed_cutoff,
                    )
                ),
            ),
            (
                "workflow_runs_current",
                "active",
                delete(workflow_runs_current).where(
                    and_(
                        or_(
                            workflow_runs_current.c.status.is_(None),
                            workflow_runs_current.c.status != "completed",
                        ),
                        workflow_runs_current.c.last_seen_at < active_cutoff,
                    )
                ),
            ),
            (
                "commit_status_current",
                "completed",
                delete(commit_status_current).where(
                    and_(
                        or_(
                            commit_status_current.c.state.is_(None),
                            commit_status_current.c.state != "pending",
                        ),
                        commit_status_current.c.last_seen_at < completed_cutoff,
                    )
                ),
            ),
            (
                "commit_status_current",
                "active",
                delete(commit_status_current).where(
                    and_(
                        commit_status_current.c.state == "pending",
                        commit_status_current.c.last_seen_at < active_cutoff,
                    )
                ),
            ),
            (
                "pr_heads_current",
                "completed",
                delete(pr_heads_current).where(
                    and_(
                        or_(
                            pr_heads_current.c.state.is_(None),
                            pr_heads_current.c.state != "open",
                        ),
                        pr_heads_current.c.updated_at < completed_cutoff,
                    )
                ),
            ),
            (
                "pr_heads_current",
                "active",
                delete(pr_heads_current).where(
                    and_(
                        pr_heads_current.c.state == "open",
                        pr_heads_current.c.updated_at < active_cutoff,
                    )
                ),
            ),
        )

        counts: Dict[str, int] = {}
        with self.engine.begin() as conn:
            for table, kind, statement in statements:
                deleted = conn.execute(statement).rowcount or 0
                if deleted > 0:
                    webhook_projection_pruned_total.labels(
                        table=table, kind=kind
                    ).inc(deleted)
                counts[f"{table}:{kind}"] = deleted
        return counts

    def _project(
        self,
        conn: Connection,
        event: str,
        payload: Mapping[str, Any],
        now: str,
        delivery_id: str,
    ) -> str:
        if event == "check_run":
            self._project_check_run(conn, payload, now, delivery_id)
            return "check_run"
        if event == "check_suite":
            self._project_check_suite(conn, payload, now, delivery_id)
            return "check_suite"
        if event == "workflow_run":
            self._project_workflow_run(conn, payload, now, delivery_id)
            return "workflow_run"
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
        conn: Connection,
        payload: Mapping[str, Any],
        now: str,
        delivery_id: str,
    ) -> None:
        repo = payload["repository"]
        check_run = payload["check_run"]
        app = check_run.get("app") or {}
        check_suite = check_run.get("check_suite") or {}

        values = {
            "repo_id": repo["id"],
            "repo_full_name": repo.get("full_name"),
            "check_run_id": check_run["id"],
            "head_sha": check_run["head_sha"],
            "name": check_run["name"],
            "status": check_run["status"],
            "conclusion": check_run.get("conclusion"),
            "app_id": app.get("id"),
            "app_slug": app.get("slug"),
            "check_suite_id": check_suite.get("id"),
            "started_at": check_run.get("started_at"),
            "completed_at": check_run.get("completed_at"),
            "first_seen_at": now,
            "last_seen_at": now,
            "last_delivery_id": delivery_id,
        }
        self._upsert(
            conn=conn,
            table=check_runs_current,
            values=values,
            conflict_columns=["repo_id", "check_run_id"],
            update_columns=[
                "repo_full_name",
                "head_sha",
                "name",
                "status",
                "conclusion",
                "app_id",
                "app_slug",
                "check_suite_id",
                "started_at",
                "completed_at",
                "last_seen_at",
                "last_delivery_id",
            ],
        )

    def _project_status(
        self,
        conn: Connection,
        payload: Mapping[str, Any],
        now: str,
        delivery_id: str,
    ) -> None:
        repo = payload["repository"]
        values = {
            "repo_id": repo["id"],
            "repo_full_name": repo.get("full_name"),
            "sha": payload["sha"],
            "context": payload["context"],
            "status_id": payload["id"],
            "state": payload["state"],
            "created_at": payload.get("created_at"),
            "updated_at": payload.get("updated_at"),
            "url": payload.get("url"),
            "first_seen_at": now,
            "last_seen_at": now,
            "last_delivery_id": delivery_id,
        }
        self._upsert(
            conn=conn,
            table=commit_status_current,
            values=values,
            conflict_columns=["repo_id", "sha", "context"],
            update_columns=[
                "repo_full_name",
                "status_id",
                "state",
                "created_at",
                "updated_at",
                "url",
                "last_seen_at",
                "last_delivery_id",
            ],
        )

    def _project_check_suite(
        self,
        conn: Connection,
        payload: Mapping[str, Any],
        now: str,
        delivery_id: str,
    ) -> None:
        repo = payload["repository"]
        check_suite = payload["check_suite"]
        app = check_suite.get("app") or {}
        values = {
            "repo_id": repo["id"],
            "repo_full_name": repo.get("full_name"),
            "check_suite_id": check_suite["id"],
            "head_sha": check_suite["head_sha"],
            "head_branch": check_suite.get("head_branch"),
            "status": check_suite.get("status"),
            "conclusion": check_suite.get("conclusion"),
            "app_id": app.get("id"),
            "app_slug": app.get("slug"),
            "created_at": check_suite.get("created_at"),
            "updated_at": check_suite.get("updated_at"),
            "first_seen_at": now,
            "last_seen_at": now,
            "last_delivery_id": delivery_id,
        }
        self._upsert(
            conn=conn,
            table=check_suites_current,
            values=values,
            conflict_columns=["repo_id", "check_suite_id"],
            update_columns=[
                "repo_full_name",
                "head_sha",
                "head_branch",
                "status",
                "conclusion",
                "app_id",
                "app_slug",
                "created_at",
                "updated_at",
                "last_seen_at",
                "last_delivery_id",
            ],
        )

    def _project_pull_request(
        self,
        conn: Connection,
        payload: Mapping[str, Any],
        now: str,
        delivery_id: str,
    ) -> None:
        repo = payload["repository"]
        pr = payload["pull_request"]
        values = {
            "repo_id": repo["id"],
            "repo_full_name": repo.get("full_name"),
            "pr_id": pr["id"],
            "pr_number": pr["number"],
            "state": pr["state"],
            "head_sha": pr["head"]["sha"],
            "base_ref": pr["base"]["ref"],
            "action": payload.get("action"),
            "updated_at": pr.get("updated_at") or now,
            "last_delivery_id": delivery_id,
        }
        self._upsert(
            conn=conn,
            table=pr_heads_current,
            values=values,
            conflict_columns=["repo_id", "pr_number"],
            update_columns=[
                "repo_full_name",
                "pr_id",
                "state",
                "head_sha",
                "base_ref",
                "action",
                "updated_at",
                "last_delivery_id",
            ],
        )

    def _project_workflow_run(
        self,
        conn: Connection,
        payload: Mapping[str, Any],
        now: str,
        delivery_id: str,
    ) -> None:
        repo = payload["repository"]
        workflow_run = payload["workflow_run"]
        app = workflow_run.get("app") or {}
        values = {
            "repo_id": repo["id"],
            "repo_full_name": repo.get("full_name"),
            "workflow_run_id": workflow_run["id"],
            "name": workflow_run["name"],
            "event": workflow_run.get("event"),
            "status": workflow_run.get("status"),
            "conclusion": workflow_run.get("conclusion"),
            "head_sha": workflow_run["head_sha"],
            "run_number": workflow_run.get("run_number"),
            "workflow_id": workflow_run.get("workflow_id"),
            "check_suite_id": workflow_run.get("check_suite_id"),
            "app_id": app.get("id"),
            "app_slug": app.get("slug"),
            "created_at": workflow_run.get("created_at"),
            "updated_at": workflow_run.get("updated_at"),
            "first_seen_at": now,
            "last_seen_at": now,
            "last_delivery_id": delivery_id,
        }
        self._upsert(
            conn=conn,
            table=workflow_runs_current,
            values=values,
            conflict_columns=["repo_id", "workflow_run_id"],
            update_columns=[
                "repo_full_name",
                "name",
                "event",
                "status",
                "conclusion",
                "head_sha",
                "run_number",
                "workflow_id",
                "check_suite_id",
                "app_id",
                "app_slug",
                "created_at",
                "updated_at",
                "last_seen_at",
                "last_delivery_id",
            ],
        )

    def _upsert(
        self,
        *,
        conn: Connection,
        table: Table,
        values: Mapping[str, Any],
        conflict_columns: Sequence[str],
        update_columns: Sequence[str],
    ) -> None:
        stmt = sqlite_insert(table).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=[table.c[col] for col in conflict_columns],
            set_={col: getattr(stmt.excluded, col) for col in update_columns},
        )
        conn.execute(stmt)

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

    @staticmethod
    def _build_engine(db_path: Path) -> Engine:
        engine = create_engine(f"sqlite+pysqlite:///{db_path}", future=True)

        @sa_event.listens_for(engine, "connect")
        def set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA busy_timeout=10000")
            cursor.close()

        return engine
