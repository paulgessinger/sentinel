"""Make sentinel_check_run_state.check_run_id nullable.

Revision ID: 0002_nullable_sentinel_check_run_id
Revises: 0001_initial
Create Date: 2026-02-17 18:16:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0002_nullable_sentinel_check_run_id"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("sentinel_check_run_state", recreate="always") as batch:
        batch.alter_column(
            "check_run_id",
            existing_type=sa.Integer(),
            nullable=True,
        )

    op.execute(
        "UPDATE sentinel_check_run_state SET check_run_id = NULL WHERE check_run_id <= 0"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE sentinel_check_run_state SET check_run_id = 0 WHERE check_run_id IS NULL"
    )
    with op.batch_alter_table("sentinel_check_run_state", recreate="always") as batch:
        batch.alter_column(
            "check_run_id",
            existing_type=sa.Integer(),
            nullable=False,
        )
