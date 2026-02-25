"""add report layout events table

Revision ID: e5b6c7d8a9f0
Revises: 1f2c3d4e5a6b
Create Date: 2026-02-25 15:30:00.000000

"""
from collections.abc import Sequence
from typing import Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e5b6c7d8a9f0"
down_revision: Union[str, Sequence[str], None] = "1f2c3d4e5a6b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if inspector.has_table("report_layout_events"):
        return

    op.create_table(
        "report_layout_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("table_asset_id", sa.Integer(), nullable=False),
        sa.Column("card_id", sa.String(length=255), nullable=False),
        sa.Column("artifact_id", sa.String(length=255), nullable=False),
        sa.Column("card_kind", sa.String(length=32), nullable=False),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("x", sa.Integer(), nullable=False),
        sa.Column("y", sa.Integer(), nullable=False),
        sa.Column("w", sa.Integer(), nullable=False),
        sa.Column("h", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["table_asset_id"], ["table_assets.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_report_layout_events_id"), "report_layout_events", ["id"], unique=False)
    op.create_index(
        op.f("ix_report_layout_events_table_asset_id"),
        "report_layout_events",
        ["table_asset_id"],
        unique=False,
    )
    op.create_index(op.f("ix_report_layout_events_card_id"), "report_layout_events", ["card_id"], unique=False)
    op.create_index(
        op.f("ix_report_layout_events_artifact_id"),
        "report_layout_events",
        ["artifact_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_report_layout_events_created_at"),
        "report_layout_events",
        ["created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_report_layout_events_created_at"), table_name="report_layout_events")
    op.drop_index(op.f("ix_report_layout_events_artifact_id"), table_name="report_layout_events")
    op.drop_index(op.f("ix_report_layout_events_card_id"), table_name="report_layout_events")
    op.drop_index(op.f("ix_report_layout_events_table_asset_id"), table_name="report_layout_events")
    op.drop_index(op.f("ix_report_layout_events_id"), table_name="report_layout_events")
    op.drop_table("report_layout_events")
