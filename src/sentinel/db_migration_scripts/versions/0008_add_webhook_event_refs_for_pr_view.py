"""Add webhook event reference columns for PR view without payload decode.

Revision ID: 0008_add_webhook_event_refs_for_pr_view
Revises: 0007_add_output_checks_json_and_check_run_html_url
Create Date: 2026-02-20 15:40:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0008_add_webhook_event_refs_for_pr_view"
down_revision = "0007_add_output_checks_json_and_check_run_html_url"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("webhook_events") as batch:
        batch.add_column(sa.Column("head_sha", sa.String(), nullable=True))
        batch.add_column(sa.Column("pr_number", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("event_detail", sa.String(), nullable=True))

    op.create_index(
        "idx_webhook_events_repo_pr_received_at",
        "webhook_events",
        ["repo_id", "pr_number", "received_at"],
        unique=False,
    )
    op.create_index(
        "idx_webhook_events_repo_head_received_at",
        "webhook_events",
        ["repo_id", "head_sha", "received_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "idx_webhook_events_repo_head_received_at",
        table_name="webhook_events",
    )
    op.drop_index(
        "idx_webhook_events_repo_pr_received_at",
        table_name="webhook_events",
    )

    with op.batch_alter_table("webhook_events") as batch:
        batch.drop_column("event_detail")
        batch.drop_column("pr_number")
        batch.drop_column("head_sha")
