"""Store full sentinel output content in state table.

Revision ID: 0003_store_sentinel_output_content
Revises: 0002_nullable_sentinel_check_run_id
Create Date: 2026-02-17 21:10:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0003_store_sentinel_output_content"
down_revision = "0002_nullable_sentinel_check_run_id"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("sentinel_check_run_state", recreate="always") as batch:
        batch.add_column(sa.Column("output_summary", sa.String(), nullable=True))
        batch.add_column(sa.Column("output_text", sa.String(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("sentinel_check_run_state", recreate="always") as batch:
        batch.drop_column("output_text")
        batch.drop_column("output_summary")
