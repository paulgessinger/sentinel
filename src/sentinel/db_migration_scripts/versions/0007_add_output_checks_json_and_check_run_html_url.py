"""Add structured output checks and check_run html_url columns.

Revision ID: 0007_add_output_checks_json_and_check_run_html_url
Revises: 0006_add_sentinel_activity_events
Create Date: 2026-02-20 12:10:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0007_add_output_checks_json_and_check_run_html_url"
down_revision = "0006_add_sentinel_activity_events"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("sentinel_check_run_state") as batch:
        batch.add_column(sa.Column("output_checks_json", sa.String(), nullable=True))

    with op.batch_alter_table("check_runs_current") as batch:
        batch.add_column(sa.Column("html_url", sa.String(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("check_runs_current") as batch:
        batch.drop_column("html_url")

    with op.batch_alter_table("sentinel_check_run_state") as batch:
        batch.drop_column("output_checks_json")
