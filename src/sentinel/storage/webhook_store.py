from __future__ import annotations

from compression import zstd
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import threading
from typing import Any, Dict, Mapping, Sequence

from sanic.log import logger
from sqlalchemy import (
    Boolean,
    bindparam,
    Column,
    Engine,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    and_,
    create_engine,
    delete,
    event as sa_event,
    or_,
    select,
    func,
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
from sentinel.storage.types import (
    CheckRunRow,
    CommitStatusRow,
    PRDashboardRow,
    PullRequestHeadRow,
    RelatedEventRow,
    SentinelCheckRunStateRow,
    WebhookEventRow,
    WorkflowRunRow,
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
    Column("payload_json", LargeBinary, nullable=False),
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
    Column("html_url", String),
    Column("started_at", String),
    Column("completed_at", String),
    Column("first_seen_at", String, nullable=False),
    Column("last_seen_at", String, nullable=False),
    Column("last_delivery_id", String, nullable=False),
)

sentinel_check_run_state = Table(
    "sentinel_check_run_state",
    metadata,
    Column("repo_id", Integer, primary_key=True),
    Column("repo_full_name", String),
    Column("head_sha", String, primary_key=True),
    Column("check_name", String, primary_key=True),
    Column("app_id", Integer, nullable=False),
    Column("check_run_id", Integer),
    Column("status", String, nullable=False),
    Column("conclusion", String),
    Column("started_at", String),
    Column("completed_at", String),
    Column("output_title", String),
    Column("output_summary", String),
    Column("output_text", String),
    Column("output_checks_json", String),
    Column("output_summary_hash", String),
    Column("output_text_hash", String),
    Column("last_eval_at", String, nullable=False),
    Column("last_publish_at", String),
    Column("last_publish_result", String, nullable=False),
    Column("last_publish_error", String),
    Column("last_delivery_id", String),
)

sentinel_activity_events = Table(
    "sentinel_activity_events",
    metadata,
    Column("activity_id", Integer, primary_key=True, autoincrement=True),
    Column("recorded_at", String, nullable=False),
    Column("repo_id", Integer, nullable=False),
    Column("repo_full_name", String),
    Column("pr_number", Integer, nullable=False),
    Column("head_sha", String, nullable=False),
    Column("delivery_id", String),
    Column("activity_type", String, nullable=False),
    Column("result", String),
    Column("detail", String),
    Column("metadata_json", LargeBinary),
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
    Column("pr_title", String),
    Column("pr_draft", Boolean),
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
Index(
    "idx_sentinel_check_run_state_repo_head",
    sentinel_check_run_state.c.repo_id,
    sentinel_check_run_state.c.head_sha,
)
Index(
    "idx_sentinel_activity_events_repo_pr_recorded",
    sentinel_activity_events.c.repo_id,
    sentinel_activity_events.c.pr_number,
    sentinel_activity_events.c.recorded_at,
)
Index(
    "idx_sentinel_activity_events_repo_head_recorded",
    sentinel_activity_events.c.repo_id,
    sentinel_activity_events.c.head_sha,
    sentinel_activity_events.c.recorded_at,
)
Index(
    "idx_sentinel_activity_events_recorded_at",
    sentinel_activity_events.c.recorded_at,
)


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@dataclass(frozen=True)
class PersistResult:
    inserted: bool
    duplicate: bool
    projection: str
    projection_error: str | None = None
    pruned_rows: int = 0


class WebhookStore:
    def __init__(
        self,
        db_path: str,
        retention_seconds: int = 30 * 24 * 60 * 60,
        activity_retention_seconds: int | None = None,
        projection_completed_retention_seconds: int | None = None,
        projection_active_retention_seconds: int | None = None,
        enabled: bool = True,
        events: Sequence[str] | None = None,
        prune_every: int = 500,
    ):
        self.db_path = Path(db_path)
        self.retention_seconds = retention_seconds
        self.activity_retention_seconds = (
            activity_retention_seconds
            if activity_retention_seconds is not None
            else retention_seconds
        )
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
            payload_json=self.encode_payload_json(payload_json),
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
            self.prune_old_activity_events()
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

    def prune_old_events(self, retention_seconds: int | None = None) -> int:
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

    def prune_old_activity_events(self, retention_seconds: int | None = None) -> int:
        if not self.enabled:
            return 0

        retention_seconds = (
            self.activity_retention_seconds
            if retention_seconds is None
            else max(0, retention_seconds)
        )
        cutoff = (
            datetime.now(timezone.utc) - timedelta(seconds=retention_seconds)
        ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        with self.engine.begin() as conn:
            result = conn.execute(
                delete(sentinel_activity_events).where(
                    sentinel_activity_events.c.recorded_at < cutoff
                )
            )
            count = result.rowcount or 0

        if count > 0:
            webhook_projection_pruned_total.labels(
                table="sentinel_activity_events", kind="all"
            ).inc(count)
        return count

    def migrate_event_payloads_to_zstd(self, batch_size: int = 500) -> Dict[str, int]:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")

        select_stmt = select(
            webhook_events.c.delivery_id,
            webhook_events.c.payload_json,
        )
        update_stmt = (
            update(webhook_events)
            .where(webhook_events.c.delivery_id == bindparam("p_delivery_id"))
            .values(payload_json=bindparam("p_payload_json"))
        )

        scanned_rows = 0
        converted_rows = 0
        already_compressed_rows = 0
        skipped_rows = 0
        updates: list[Dict[str, Any]] = []

        with self.engine.begin() as conn:
            rows = conn.execute(select_stmt).all()
            for delivery_id, payload_json in rows:
                scanned_rows += 1
                if payload_json is None:
                    skipped_rows += 1
                    continue
                if self._is_zstd_payload(payload_json):
                    already_compressed_rows += 1
                    continue

                compressed_payload = self.encode_payload_json(
                    self.decode_payload_json(payload_json)
                )
                updates.append(
                    {
                        "p_delivery_id": delivery_id,
                        "p_payload_json": compressed_payload,
                    }
                )
                converted_rows += 1

                if len(updates) >= batch_size:
                    conn.execute(update_stmt, updates)
                    updates.clear()

            if updates:
                conn.execute(update_stmt, updates)

        return {
            "scanned_rows": scanned_rows,
            "converted_rows": converted_rows,
            "already_compressed_rows": already_compressed_rows,
            "skipped_rows": skipped_rows,
        }

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
            (
                "sentinel_check_run_state",
                "completed",
                delete(sentinel_check_run_state).where(
                    and_(
                        sentinel_check_run_state.c.status == "completed",
                        sentinel_check_run_state.c.last_eval_at < completed_cutoff,
                    )
                ),
            ),
            (
                "sentinel_check_run_state",
                "active",
                delete(sentinel_check_run_state).where(
                    and_(
                        or_(
                            sentinel_check_run_state.c.status.is_(None),
                            sentinel_check_run_state.c.status != "completed",
                        ),
                        sentinel_check_run_state.c.last_eval_at < active_cutoff,
                    )
                ),
            ),
        )

        counts: Dict[str, int] = {}
        with self.engine.begin() as conn:
            for table, kind, statement in statements:
                deleted = conn.execute(statement).rowcount or 0
                if deleted > 0:
                    webhook_projection_pruned_total.labels(table=table, kind=kind).inc(
                        deleted
                    )
                counts[f"{table}:{kind}"] = deleted
        return counts

    def get_open_pr_candidates(
        self, repo_id: int, head_sha: str
    ) -> list[PullRequestHeadRow]:
        stmt = (
            select(pr_heads_current)
            .where(
                and_(
                    pr_heads_current.c.repo_id == repo_id,
                    pr_heads_current.c.head_sha == head_sha,
                    pr_heads_current.c.state == "open",
                )
            )
            .order_by(pr_heads_current.c.updated_at.desc())
        )
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
        return [PullRequestHeadRow.from_mapping(dict(row)) for row in rows]

    def get_check_runs_for_head(self, repo_id: int, head_sha: str) -> list[CheckRunRow]:
        stmt = select(check_runs_current).where(
            and_(
                check_runs_current.c.repo_id == repo_id,
                check_runs_current.c.head_sha == head_sha,
            )
        )
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
        return [CheckRunRow.from_mapping(dict(row)) for row in rows]

    def get_workflow_runs_for_head(
        self, repo_id: int, head_sha: str
    ) -> list[WorkflowRunRow]:
        stmt = select(workflow_runs_current).where(
            and_(
                workflow_runs_current.c.repo_id == repo_id,
                workflow_runs_current.c.head_sha == head_sha,
            )
        )
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
        return [WorkflowRunRow.from_mapping(dict(row)) for row in rows]

    def get_commit_statuses_for_sha(
        self, repo_id: int, sha: str
    ) -> list[CommitStatusRow]:
        stmt = select(commit_status_current).where(
            and_(
                commit_status_current.c.repo_id == repo_id,
                commit_status_current.c.sha == sha,
            )
        )
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
        return [CommitStatusRow.from_mapping(dict(row)) for row in rows]

    def upsert_head_snapshot_from_api(
        self,
        *,
        repo_id: int,
        repo_full_name: str | None,
        head_sha: str,
        delivery_id: str | None,
        check_rows: Sequence[CheckRunRow],
        workflow_rows: Sequence[WorkflowRunRow],
        status_rows: Sequence[CommitStatusRow],
    ) -> Dict[str, int]:
        now = utcnow_iso()
        resolved_delivery_id = delivery_id or "api-refresh"
        counts = {"check_runs": 0, "workflow_runs": 0, "commit_statuses": 0}

        with self.engine.begin() as conn:
            for row in check_rows:
                check_run_id = row.check_run_id
                name = row.name
                status = row.status
                if check_run_id is None or not name or not status:
                    continue
                values = row.model_dump(
                    mode="python",
                    include={
                        "check_run_id",
                        "name",
                        "status",
                        "conclusion",
                        "app_id",
                        "app_slug",
                        "check_suite_id",
                        "html_url",
                        "started_at",
                        "completed_at",
                    },
                )
                values.update(
                    {
                        "repo_id": repo_id,
                        "repo_full_name": repo_full_name,
                        "head_sha": head_sha,
                        "first_seen_at": now,
                        "last_seen_at": now,
                        "last_delivery_id": resolved_delivery_id,
                    }
                )
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
                        "html_url",
                        "started_at",
                        "completed_at",
                        "last_seen_at",
                        "last_delivery_id",
                    ],
                )
                counts["check_runs"] += 1

            for row in workflow_rows:
                workflow_run_id = row.workflow_run_id
                name = row.name
                if workflow_run_id is None or not name:
                    continue
                values = row.model_dump(
                    mode="python",
                    include={
                        "workflow_run_id",
                        "name",
                        "event",
                        "status",
                        "conclusion",
                        "run_number",
                        "workflow_id",
                        "check_suite_id",
                        "app_id",
                        "app_slug",
                        "created_at",
                        "updated_at",
                    },
                )
                values.update(
                    {
                        "repo_id": repo_id,
                        "repo_full_name": repo_full_name,
                        "head_sha": head_sha,
                        "first_seen_at": now,
                        "last_seen_at": now,
                        "last_delivery_id": resolved_delivery_id,
                    }
                )
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
                counts["workflow_runs"] += 1

            for row in status_rows:
                context = row.context
                state = row.state
                status_id = row.status_id
                if status_id is None or not context or not state:
                    continue
                values = row.model_dump(
                    mode="python",
                    include={
                        "context",
                        "status_id",
                        "state",
                        "created_at",
                        "updated_at",
                        "url",
                    },
                )
                values.update(
                    {
                        "repo_id": repo_id,
                        "repo_full_name": repo_full_name,
                        "sha": row.sha or head_sha,
                        "first_seen_at": now,
                        "last_seen_at": now,
                        "last_delivery_id": resolved_delivery_id,
                    }
                )
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
                counts["commit_statuses"] += 1

        return counts

    def get_sentinel_check_run(
        self,
        *,
        repo_id: int,
        head_sha: str,
        check_name: str,
        app_id: int,
    ) -> SentinelCheckRunStateRow | None:
        stmt = select(sentinel_check_run_state).where(
            and_(
                sentinel_check_run_state.c.repo_id == repo_id,
                sentinel_check_run_state.c.head_sha == head_sha,
                sentinel_check_run_state.c.check_name == check_name,
                sentinel_check_run_state.c.app_id == app_id,
            )
        )
        with self.engine.begin() as conn:
            row = conn.execute(stmt).mappings().first()
        return None if row is None else SentinelCheckRunStateRow.from_mapping(dict(row))

    def get_pr_dashboard_row(
        self,
        *,
        app_id: int,
        check_name: str,
        repo_id: int,
        pr_number: int,
    ) -> PRDashboardRow | None:
        stmt = (
            self._pr_dashboard_select(app_id=app_id, check_name=check_name)
            .where(
                and_(
                    pr_heads_current.c.repo_id == repo_id,
                    pr_heads_current.c.pr_number == pr_number,
                )
            )
            .limit(1)
        )
        with self.engine.begin() as conn:
            row = conn.execute(stmt).mappings().first()
        return None if row is None else self._normalize_pr_dashboard_row(dict(row))

    def list_pr_related_events(
        self,
        *,
        repo_id: int,
        pr_number: int,
        head_sha: str | None,
        limit: int = 200,
    ) -> list[RelatedEventRow]:
        target_limit = min(1000, max(1, int(limit)))
        scan_limit = min(5000, max(200, target_limit * 20))

        stmt = (
            select(
                webhook_events.c.delivery_id,
                webhook_events.c.received_at,
                webhook_events.c.event,
                webhook_events.c.action,
                webhook_events.c.payload_json,
                webhook_events.c.projection_error,
            )
            .where(
                and_(
                    webhook_events.c.repo_id == repo_id,
                    webhook_events.c.event.in_(
                        (
                            "pull_request",
                            "check_run",
                            "check_suite",
                            "workflow_run",
                            "status",
                        )
                    ),
                )
            )
            .order_by(webhook_events.c.received_at.desc())
            .limit(scan_limit)
        )

        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()

        events: list[RelatedEventRow] = []
        for row in rows:
            try:
                payload = json.loads(self.decode_payload_json(row.get("payload_json")))
            except ValueError:
                payload = {}

            if not self._event_matches_pr(
                event_name=str(row.get("event") or ""),
                payload=payload,
                pr_number=pr_number,
                head_sha=head_sha,
            ):
                continue

            event_name = str(row.get("event") or "")
            events.append(
                RelatedEventRow(
                    delivery_id=str(row.get("delivery_id") or "-"),
                    received_at=row.get("received_at"),
                    event=event_name,
                    action=row.get("action"),
                    projection_error=row.get("projection_error"),
                    detail=self._event_detail(event_name=event_name, payload=payload),
                    payload=payload,
                )
            )
            if len(events) >= target_limit:
                break

        activity_stmt = (
            select(
                sentinel_activity_events.c.activity_id,
                sentinel_activity_events.c.recorded_at,
                sentinel_activity_events.c.activity_type,
                sentinel_activity_events.c.result,
                sentinel_activity_events.c.detail,
                sentinel_activity_events.c.delivery_id,
                sentinel_activity_events.c.metadata_json,
            )
            .where(
                and_(
                    sentinel_activity_events.c.repo_id == repo_id,
                    sentinel_activity_events.c.pr_number == pr_number,
                )
            )
            .order_by(sentinel_activity_events.c.recorded_at.desc())
            .limit(target_limit)
        )
        if head_sha:
            activity_stmt = activity_stmt.where(
                sentinel_activity_events.c.head_sha == head_sha
            )
        with self.engine.begin() as conn:
            activity_rows = conn.execute(activity_stmt).mappings().all()

        for row in activity_rows:
            metadata: Dict[str, Any] = {}
            metadata_blob = row.get("metadata_json")
            if metadata_blob:
                try:
                    metadata = json.loads(self.decode_payload_json(metadata_blob))
                except ValueError:
                    metadata = {}
            detail_parts = []
            result = row.get("result")
            detail = row.get("detail")
            if result:
                detail_parts.append(str(result))
            if detail:
                detail_parts.append(str(detail))
            formatted_detail = ": ".join(detail_parts) if detail_parts else "activity"
            activity_id = row.get("activity_id")
            events.append(
                RelatedEventRow(
                    delivery_id=str(row.get("delivery_id"))
                    if row.get("delivery_id")
                    else (
                        f"activity-{activity_id}" if activity_id is not None else "-"
                    ),
                    received_at=row.get("recorded_at"),
                    event="sentinel",
                    action=row.get("activity_type") or "activity",
                    projection_error=None,
                    detail=formatted_detail,
                    payload=metadata,
                    is_activity=True,
                )
            )

        events.sort(
            key=lambda item: str(item.received_at or ""),
            reverse=True,
        )
        if len(events) > target_limit:
            events = events[:target_limit]

        return events

    def record_projection_activity_event(
        self,
        *,
        repo_id: int,
        repo_full_name: str | None,
        pr_number: int,
        head_sha: str,
        delivery_id: str | None,
        activity_type: str,
        result: str | None = None,
        detail: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        recorded_at: str | None = None,
    ) -> None:
        if not self.enabled:
            return
        now = recorded_at or utcnow_iso()
        metadata_payload = (
            self.encode_payload_json(json.dumps(metadata, sort_keys=True))
            if metadata is not None
            else None
        )
        stmt = sqlite_insert(sentinel_activity_events).values(
            recorded_at=now,
            repo_id=repo_id,
            repo_full_name=repo_full_name,
            pr_number=pr_number,
            head_sha=head_sha,
            delivery_id=delivery_id,
            activity_type=activity_type,
            result=result,
            detail=detail,
            metadata_json=metadata_payload,
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)

    def get_webhook_event(self, delivery_id: str) -> WebhookEventRow | None:
        stmt = (
            select(
                webhook_events.c.delivery_id,
                webhook_events.c.received_at,
                webhook_events.c.event,
                webhook_events.c.action,
                webhook_events.c.installation_id,
                webhook_events.c.repo_id,
                webhook_events.c.repo_full_name,
                webhook_events.c.payload_json,
                webhook_events.c.projected_at,
                webhook_events.c.projection_error,
            )
            .where(webhook_events.c.delivery_id == delivery_id)
            .limit(1)
        )
        with self.engine.begin() as conn:
            row = conn.execute(stmt).mappings().first()
        if row is None:
            return None

        event = dict(row)
        payload: Dict[str, Any]
        try:
            payload = json.loads(self.decode_payload_json(event.pop("payload_json")))
        except ValueError:
            payload = {}

        event_name = str(event.get("event") or "")
        event["payload"] = payload
        event["detail"] = self._event_detail(event_name=event_name, payload=payload)
        return WebhookEventRow.from_mapping(event)

    def count_pr_dashboard_rows(
        self,
        repo_full_name: str | None = None,
        include_closed: bool = True,
    ) -> int:
        stmt = select(func.count()).select_from(pr_heads_current)
        if repo_full_name:
            stmt = stmt.where(pr_heads_current.c.repo_full_name == repo_full_name)
        if not include_closed:
            stmt = stmt.where(pr_heads_current.c.state == "open")
        with self.engine.begin() as conn:
            count = conn.execute(stmt).scalar_one()
        return int(count)

    def list_pr_dashboard_rows(
        self,
        *,
        app_id: int,
        check_name: str,
        page: int,
        page_size: int,
        repo_full_name: str | None = None,
        include_closed: bool = True,
    ) -> list[PRDashboardRow]:
        page = max(1, int(page))
        page_size = min(200, max(1, int(page_size)))
        offset = (page - 1) * page_size

        stmt = (
            self._pr_dashboard_select(app_id=app_id, check_name=check_name)
            .order_by(
                pr_heads_current.c.updated_at.desc(),
                pr_heads_current.c.pr_number.desc(),
            )
            .limit(page_size)
            .offset(offset)
        )
        if repo_full_name:
            stmt = stmt.where(pr_heads_current.c.repo_full_name == repo_full_name)
        if not include_closed:
            stmt = stmt.where(pr_heads_current.c.state == "open")

        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
        return [self._normalize_pr_dashboard_row(dict(row)) for row in rows]

    def _pr_dashboard_select(self, *, app_id: int, check_name: str):
        pr_event = webhook_events.alias("pr_event")
        join_condition = and_(
            sentinel_check_run_state.c.repo_id == pr_heads_current.c.repo_id,
            sentinel_check_run_state.c.head_sha == pr_heads_current.c.head_sha,
            sentinel_check_run_state.c.check_name == check_name,
            sentinel_check_run_state.c.app_id == app_id,
        )
        pr_event_join_condition = and_(
            pr_event.c.delivery_id == pr_heads_current.c.last_delivery_id,
            pr_event.c.event == "pull_request",
        )
        return select(
            pr_heads_current.c.repo_id.label("repo_id"),
            pr_heads_current.c.repo_full_name.label("repo_full_name"),
            pr_heads_current.c.pr_number.label("pr_number"),
            pr_heads_current.c.pr_id.label("pr_id"),
            pr_heads_current.c.pr_title.label("pr_title"),
            pr_heads_current.c.pr_draft.label("pr_is_draft"),
            pr_heads_current.c.state.label("pr_state"),
            pr_heads_current.c.head_sha.label("head_sha"),
            pr_heads_current.c.base_ref.label("base_ref"),
            pr_heads_current.c.action.label("action"),
            pr_heads_current.c.updated_at.label("pr_updated_at"),
            pr_heads_current.c.last_delivery_id.label("pr_last_delivery_id"),
            pr_event.c.payload_json.label("pr_event_payload_json"),
            sentinel_check_run_state.c.status.label("sentinel_status"),
            sentinel_check_run_state.c.conclusion.label("sentinel_conclusion"),
            sentinel_check_run_state.c.check_run_id.label("sentinel_check_run_id"),
            sentinel_check_run_state.c.output_title.label("output_title"),
            sentinel_check_run_state.c.output_summary.label("output_summary"),
            sentinel_check_run_state.c.output_text.label("output_text"),
            sentinel_check_run_state.c.output_checks_json.label("output_checks_json"),
            sentinel_check_run_state.c.output_summary_hash.label("output_summary_hash"),
            sentinel_check_run_state.c.output_text_hash.label("output_text_hash"),
            sentinel_check_run_state.c.last_eval_at.label("last_eval_at"),
            sentinel_check_run_state.c.last_publish_at.label("last_publish_at"),
            sentinel_check_run_state.c.last_publish_result.label("last_publish_result"),
            sentinel_check_run_state.c.last_publish_error.label("last_publish_error"),
            sentinel_check_run_state.c.last_delivery_id.label("last_delivery_id"),
        ).select_from(
            pr_heads_current.outerjoin(pr_event, pr_event_join_condition).outerjoin(
                sentinel_check_run_state, join_condition
            )
        )

    def _normalize_pr_dashboard_row(self, row: Dict[str, Any]) -> PRDashboardRow:
        payload_json = row.pop("pr_event_payload_json", None)
        pr_merged: bool | None = None
        pr_is_draft = row.get("pr_is_draft")

        if payload_json is not None:
            try:
                decoded = self.decode_payload_json(payload_json)
                payload = json.loads(decoded)
                merged_value = (payload.get("pull_request") or {}).get("merged")
                if merged_value is not None:
                    pr_merged = bool(merged_value)
                if pr_is_draft is None:
                    draft_value = (payload.get("pull_request") or {}).get("draft")
                    if draft_value is not None:
                        pr_is_draft = bool(draft_value)
            except Exception:  # noqa: BLE001
                pr_merged = None

        row["pr_merged"] = pr_merged
        row["pr_is_draft"] = bool(pr_is_draft) if pr_is_draft is not None else False
        return PRDashboardRow.from_mapping(row)

    @staticmethod
    def _event_matches_pr(
        *,
        event_name: str,
        payload: Mapping[str, Any],
        pr_number: int,
        head_sha: str | None,
    ) -> bool:
        if event_name == "pull_request":
            pull_request = payload.get("pull_request") or {}
            return pull_request.get("number") == pr_number

        if not head_sha:
            return False

        if event_name == "status":
            return payload.get("sha") == head_sha
        if event_name == "check_run":
            check_run = payload.get("check_run") or {}
            return check_run.get("head_sha") == head_sha
        if event_name == "check_suite":
            check_suite = payload.get("check_suite") or {}
            return check_suite.get("head_sha") == head_sha
        if event_name == "workflow_run":
            workflow_run = payload.get("workflow_run") or {}
            return workflow_run.get("head_sha") == head_sha
        return False

    @staticmethod
    def _event_detail(*, event_name: str, payload: Mapping[str, Any]) -> str:
        if event_name == "pull_request":
            pull_request = payload.get("pull_request") or {}
            head = (pull_request.get("head") or {}).get("sha")
            return (
                f"PR #{pull_request.get('number')} head={head}"
                if head
                else "pull_request"
            )
        if event_name == "check_run":
            check_run = payload.get("check_run") or {}
            name = check_run.get("name") or "check_run"
            status = check_run.get("status")
            conclusion = check_run.get("conclusion")
            if status and conclusion:
                return f"{name}: {status}/{conclusion}"
            if status:
                return f"{name}: {status}"
            return str(name)
        if event_name == "check_suite":
            check_suite = payload.get("check_suite") or {}
            status = check_suite.get("status")
            conclusion = check_suite.get("conclusion")
            if status and conclusion:
                return f"check_suite: {status}/{conclusion}"
            if status:
                return f"check_suite: {status}"
            return "check_suite"
        if event_name == "workflow_run":
            workflow_run = payload.get("workflow_run") or {}
            name = workflow_run.get("name") or "workflow_run"
            status = workflow_run.get("status")
            conclusion = workflow_run.get("conclusion")
            if status and conclusion:
                return f"{name}: {status}/{conclusion}"
            if status:
                return f"{name}: {status}"
            return str(name)
        if event_name == "status":
            context = payload.get("context") or "status"
            state = payload.get("state")
            return f"{context}: {state}" if state else str(context)
        return event_name

    def upsert_sentinel_check_run(
        self,
        *,
        repo_id: int,
        repo_full_name: str | None,
        check_run_id: int | None,
        head_sha: str,
        name: str,
        status: str,
        conclusion: str | None,
        app_id: int,
        started_at: str | None,
        completed_at: str | None,
        output_title: str | None,
        output_summary: str | None,
        output_text: str | None,
        output_checks_json: str | None,
        output_summary_hash: str | None,
        output_text_hash: str | None,
        last_eval_at: str,
        last_publish_at: str | None,
        last_publish_result: str,
        last_publish_error: str | None,
        last_delivery_id: str | None,
    ) -> None:
        values = {
            "repo_id": repo_id,
            "repo_full_name": repo_full_name,
            "head_sha": head_sha,
            "check_name": name,
            "app_id": app_id,
            "check_run_id": check_run_id,
            "status": status,
            "conclusion": conclusion,
            "started_at": started_at,
            "completed_at": completed_at,
            "output_title": output_title,
            "output_summary": output_summary,
            "output_text": output_text,
            "output_checks_json": output_checks_json,
            "output_summary_hash": output_summary_hash,
            "output_text_hash": output_text_hash,
            "last_eval_at": last_eval_at,
            "last_publish_at": last_publish_at,
            "last_publish_result": last_publish_result,
            "last_publish_error": last_publish_error,
            "last_delivery_id": last_delivery_id,
        }
        stmt = sqlite_insert(sentinel_check_run_state).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=[
                sentinel_check_run_state.c.repo_id,
                sentinel_check_run_state.c.head_sha,
                sentinel_check_run_state.c.check_name,
            ],
            set_={
                "repo_full_name": stmt.excluded.repo_full_name,
                "app_id": stmt.excluded.app_id,
                "check_run_id": stmt.excluded.check_run_id,
                "status": stmt.excluded.status,
                "conclusion": stmt.excluded.conclusion,
                "started_at": stmt.excluded.started_at,
                "completed_at": stmt.excluded.completed_at,
                "output_title": stmt.excluded.output_title,
                "output_summary": stmt.excluded.output_summary,
                "output_text": stmt.excluded.output_text,
                "output_checks_json": stmt.excluded.output_checks_json,
                "output_summary_hash": stmt.excluded.output_summary_hash,
                "output_text_hash": stmt.excluded.output_text_hash,
                "last_eval_at": stmt.excluded.last_eval_at,
                "last_publish_at": stmt.excluded.last_publish_at,
                "last_publish_result": stmt.excluded.last_publish_result,
                "last_publish_error": stmt.excluded.last_publish_error,
                "last_delivery_id": stmt.excluded.last_delivery_id,
            },
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)

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
            "html_url": check_run.get("html_url"),
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
                "html_url",
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
            "pr_title": pr.get("title"),
            "pr_draft": pr.get("draft"),
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
                "pr_title",
                "pr_draft",
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
    def encode_payload_json(payload_json: str) -> bytes:
        return zstd.compress(payload_json.encode("utf-8"))

    @staticmethod
    def decode_payload_json(payload_json: Any) -> str:
        if isinstance(payload_json, str):
            return payload_json
        if payload_json is None:
            return ""
        payload_bytes = bytes(payload_json)
        try:
            return zstd.decompress(payload_bytes).decode("utf-8")
        except zstd.ZstdError:
            return payload_bytes.decode("utf-8", errors="replace")

    @staticmethod
    def _is_zstd_payload(payload_json: Any) -> bool:
        if payload_json is None or isinstance(payload_json, str):
            return False
        try:
            zstd.decompress(bytes(payload_json))
            return True
        except zstd.ZstdError:
            return False

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
