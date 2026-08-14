"""Add merge group head projection for GitHub merge queue support.

Revision ID: 0009_add_merge_group_heads_current
Revises: 0008_add_webhook_event_refs_for_pr_view
Create Date: 2026-08-14 10:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0009_add_merge_group_heads_current"
down_revision = "0008_add_webhook_event_refs_for_pr_view"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "merge_group_heads_current",
        sa.Column("repo_id", sa.Integer(), nullable=False),
        sa.Column("head_sha", sa.String(), nullable=False),
        sa.Column("repo_full_name", sa.String(), nullable=True),
        sa.Column("head_ref", sa.String(), nullable=True),
        sa.Column("base_sha", sa.String(), nullable=True),
        sa.Column("base_ref", sa.String(), nullable=False),
        sa.Column("pr_numbers_json", sa.String(), nullable=True),
        sa.Column("state", sa.String(), nullable=False),
        sa.Column("action", sa.String(), nullable=True),
        sa.Column("updated_at", sa.String(), nullable=False),
        sa.Column("last_delivery_id", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("repo_id", "head_sha"),
    )


def downgrade() -> None:
    op.drop_table("merge_group_heads_current")
