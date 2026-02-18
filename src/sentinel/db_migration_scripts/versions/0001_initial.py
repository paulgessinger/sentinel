"""Initial webhook store schema.

Revision ID: 0001_initial
Revises:
Create Date: 2026-02-17 18:15:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "webhook_events",
        sa.Column("delivery_id", sa.String(), nullable=False),
        sa.Column("received_at", sa.String(), nullable=False),
        sa.Column("event", sa.String(), nullable=False),
        sa.Column("action", sa.String(), nullable=True),
        sa.Column("installation_id", sa.Integer(), nullable=True),
        sa.Column("repo_id", sa.Integer(), nullable=True),
        sa.Column("repo_full_name", sa.String(), nullable=True),
        sa.Column("payload_json", sa.LargeBinary(), nullable=False),
        sa.Column("projected_at", sa.String(), nullable=True),
        sa.Column("projection_error", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("delivery_id"),
    )

    op.create_table(
        "check_runs_current",
        sa.Column("repo_id", sa.Integer(), nullable=False),
        sa.Column("repo_full_name", sa.String(), nullable=True),
        sa.Column("check_run_id", sa.Integer(), nullable=False),
        sa.Column("head_sha", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("conclusion", sa.String(), nullable=True),
        sa.Column("app_id", sa.Integer(), nullable=True),
        sa.Column("app_slug", sa.String(), nullable=True),
        sa.Column("check_suite_id", sa.Integer(), nullable=True),
        sa.Column("started_at", sa.String(), nullable=True),
        sa.Column("completed_at", sa.String(), nullable=True),
        sa.Column("first_seen_at", sa.String(), nullable=False),
        sa.Column("last_seen_at", sa.String(), nullable=False),
        sa.Column("last_delivery_id", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("repo_id", "check_run_id"),
    )

    op.create_table(
        "check_suites_current",
        sa.Column("repo_id", sa.Integer(), nullable=False),
        sa.Column("repo_full_name", sa.String(), nullable=True),
        sa.Column("check_suite_id", sa.Integer(), nullable=False),
        sa.Column("head_sha", sa.String(), nullable=False),
        sa.Column("head_branch", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=True),
        sa.Column("conclusion", sa.String(), nullable=True),
        sa.Column("app_id", sa.Integer(), nullable=True),
        sa.Column("app_slug", sa.String(), nullable=True),
        sa.Column("created_at", sa.String(), nullable=True),
        sa.Column("updated_at", sa.String(), nullable=True),
        sa.Column("first_seen_at", sa.String(), nullable=False),
        sa.Column("last_seen_at", sa.String(), nullable=False),
        sa.Column("last_delivery_id", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("repo_id", "check_suite_id"),
    )

    op.create_table(
        "workflow_runs_current",
        sa.Column("repo_id", sa.Integer(), nullable=False),
        sa.Column("repo_full_name", sa.String(), nullable=True),
        sa.Column("workflow_run_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("event", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=True),
        sa.Column("conclusion", sa.String(), nullable=True),
        sa.Column("head_sha", sa.String(), nullable=False),
        sa.Column("run_number", sa.Integer(), nullable=True),
        sa.Column("workflow_id", sa.Integer(), nullable=True),
        sa.Column("check_suite_id", sa.Integer(), nullable=True),
        sa.Column("app_id", sa.Integer(), nullable=True),
        sa.Column("app_slug", sa.String(), nullable=True),
        sa.Column("created_at", sa.String(), nullable=True),
        sa.Column("updated_at", sa.String(), nullable=True),
        sa.Column("first_seen_at", sa.String(), nullable=False),
        sa.Column("last_seen_at", sa.String(), nullable=False),
        sa.Column("last_delivery_id", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("repo_id", "workflow_run_id"),
    )

    op.create_table(
        "commit_status_current",
        sa.Column("repo_id", sa.Integer(), nullable=False),
        sa.Column("repo_full_name", sa.String(), nullable=True),
        sa.Column("sha", sa.String(), nullable=False),
        sa.Column("context", sa.String(), nullable=False),
        sa.Column("status_id", sa.Integer(), nullable=False),
        sa.Column("state", sa.String(), nullable=False),
        sa.Column("created_at", sa.String(), nullable=True),
        sa.Column("updated_at", sa.String(), nullable=True),
        sa.Column("url", sa.String(), nullable=True),
        sa.Column("first_seen_at", sa.String(), nullable=False),
        sa.Column("last_seen_at", sa.String(), nullable=False),
        sa.Column("last_delivery_id", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("repo_id", "sha", "context"),
    )

    op.create_table(
        "pr_heads_current",
        sa.Column("repo_id", sa.Integer(), nullable=False),
        sa.Column("repo_full_name", sa.String(), nullable=True),
        sa.Column("pr_id", sa.Integer(), nullable=False),
        sa.Column("pr_number", sa.Integer(), nullable=False),
        sa.Column("state", sa.String(), nullable=False),
        sa.Column("head_sha", sa.String(), nullable=False),
        sa.Column("base_ref", sa.String(), nullable=False),
        sa.Column("action", sa.String(), nullable=True),
        sa.Column("updated_at", sa.String(), nullable=False),
        sa.Column("last_delivery_id", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("repo_id", "pr_number"),
    )

    op.create_table(
        "sentinel_check_run_state",
        sa.Column("repo_id", sa.Integer(), nullable=False),
        sa.Column("repo_full_name", sa.String(), nullable=True),
        sa.Column("head_sha", sa.String(), nullable=False),
        sa.Column("check_name", sa.String(), nullable=False),
        sa.Column("app_id", sa.Integer(), nullable=False),
        sa.Column("check_run_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("conclusion", sa.String(), nullable=True),
        sa.Column("started_at", sa.String(), nullable=True),
        sa.Column("completed_at", sa.String(), nullable=True),
        sa.Column("output_title", sa.String(), nullable=True),
        sa.Column("output_summary_hash", sa.String(), nullable=True),
        sa.Column("output_text_hash", sa.String(), nullable=True),
        sa.Column("last_eval_at", sa.String(), nullable=False),
        sa.Column("last_publish_at", sa.String(), nullable=True),
        sa.Column("last_publish_result", sa.String(), nullable=False),
        sa.Column("last_publish_error", sa.String(), nullable=True),
        sa.Column("last_delivery_id", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("repo_id", "head_sha", "check_name"),
    )

    op.create_index(
        "idx_webhook_events_event_received_at",
        "webhook_events",
        ["event", "received_at"],
        unique=False,
    )
    op.create_index(
        "idx_check_runs_current_repo_head_name",
        "check_runs_current",
        ["repo_id", "head_sha", "name"],
        unique=False,
    )
    op.create_index(
        "idx_check_suites_current_repo_head_sha",
        "check_suites_current",
        ["repo_id", "head_sha"],
        unique=False,
    )
    op.create_index(
        "idx_workflow_runs_current_repo_head_name",
        "workflow_runs_current",
        ["repo_id", "head_sha", "name"],
        unique=False,
    )
    op.create_index(
        "idx_commit_status_current_repo_sha_context",
        "commit_status_current",
        ["repo_id", "sha", "context"],
        unique=False,
    )
    op.create_index(
        "idx_pr_heads_current_repo_head_sha",
        "pr_heads_current",
        ["repo_id", "head_sha"],
        unique=False,
    )
    op.create_index(
        "idx_sentinel_check_run_state_repo_head",
        "sentinel_check_run_state",
        ["repo_id", "head_sha"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_sentinel_check_run_state_repo_head", table_name="sentinel_check_run_state")
    op.drop_index("idx_pr_heads_current_repo_head_sha", table_name="pr_heads_current")
    op.drop_index("idx_commit_status_current_repo_sha_context", table_name="commit_status_current")
    op.drop_index("idx_workflow_runs_current_repo_head_name", table_name="workflow_runs_current")
    op.drop_index("idx_check_suites_current_repo_head_sha", table_name="check_suites_current")
    op.drop_index("idx_check_runs_current_repo_head_name", table_name="check_runs_current")
    op.drop_index("idx_webhook_events_event_received_at", table_name="webhook_events")

    op.drop_table("sentinel_check_run_state")
    op.drop_table("pr_heads_current")
    op.drop_table("commit_status_current")
    op.drop_table("workflow_runs_current")
    op.drop_table("check_suites_current")
    op.drop_table("check_runs_current")
    op.drop_table("webhook_events")
