"""Add PR draft flag to pr_heads_current projection table.

Revision ID: 0005_add_pr_draft_to_pr_heads_current
Revises: 0004_add_pr_title_to_pr_heads_current
Create Date: 2026-02-19 22:40:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0005_add_pr_draft_to_pr_heads_current"
down_revision = "0004_add_pr_title_to_pr_heads_current"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("pr_heads_current", recreate="always") as batch:
        batch.add_column(sa.Column("pr_draft", sa.Boolean(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("pr_heads_current", recreate="always") as batch:
        batch.drop_column("pr_draft")
