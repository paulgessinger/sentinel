"""Add PR title to pr_heads_current projection table.

Revision ID: 0004_add_pr_title_to_pr_heads_current
Revises: 0003_store_sentinel_output_content
Create Date: 2026-02-17 22:50:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0004_add_pr_title_to_pr_heads_current"
down_revision = "0003_store_sentinel_output_content"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("pr_heads_current", recreate="always") as batch:
        batch.add_column(sa.Column("pr_title", sa.String(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("pr_heads_current", recreate="always") as batch:
        batch.drop_column("pr_title")
