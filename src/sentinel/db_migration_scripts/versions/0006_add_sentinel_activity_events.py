"""Add sentinel activity event table for projection/evaluation audit trail.

Revision ID: 0006_add_sentinel_activity_events
Revises: 0005_add_pr_draft_to_pr_heads_current
Create Date: 2026-02-20 00:20:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0006_add_sentinel_activity_events"
down_revision = "0005_add_pr_draft_to_pr_heads_current"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "sentinel_activity_events",
        sa.Column("activity_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("recorded_at", sa.String(), nullable=False),
        sa.Column("repo_id", sa.Integer(), nullable=False),
        sa.Column("repo_full_name", sa.String(), nullable=True),
        sa.Column("pr_number", sa.Integer(), nullable=False),
        sa.Column("head_sha", sa.String(), nullable=False),
        sa.Column("delivery_id", sa.String(), nullable=True),
        sa.Column("activity_type", sa.String(), nullable=False),
        sa.Column("result", sa.String(), nullable=True),
        sa.Column("detail", sa.String(), nullable=True),
        sa.Column("metadata_json", sa.LargeBinary(), nullable=True),
        sa.PrimaryKeyConstraint("activity_id"),
    )
    op.create_index(
        "idx_sentinel_activity_events_repo_pr_recorded",
        "sentinel_activity_events",
        ["repo_id", "pr_number", "recorded_at"],
        unique=False,
    )
    op.create_index(
        "idx_sentinel_activity_events_repo_head_recorded",
        "sentinel_activity_events",
        ["repo_id", "head_sha", "recorded_at"],
        unique=False,
    )
    op.create_index(
        "idx_sentinel_activity_events_recorded_at",
        "sentinel_activity_events",
        ["recorded_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "idx_sentinel_activity_events_recorded_at",
        table_name="sentinel_activity_events",
    )
    op.drop_index(
        "idx_sentinel_activity_events_repo_head_recorded",
        table_name="sentinel_activity_events",
    )
    op.drop_index(
        "idx_sentinel_activity_events_repo_pr_recorded",
        table_name="sentinel_activity_events",
    )
    op.drop_table("sentinel_activity_events")
