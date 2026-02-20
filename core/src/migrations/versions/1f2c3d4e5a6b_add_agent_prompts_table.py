"""add agent prompts table

Revision ID: 1f2c3d4e5a6b
Revises: c2f8d7a5e9b1
Create Date: 2026-02-10 18:05:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "1f2c3d4e5a6b"
down_revision: Union[str, Sequence[str], None] = "c2f8d7a5e9b1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "agent_prompts",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("agent_name", sa.String(length=100), nullable=False),
        sa.Column("prompt", sa.Text(), nullable=False),
        sa.Column("system_prompt", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_agent_prompts_agent_name"), "agent_prompts", ["agent_name"], unique=True)


def downgrade() -> None:
    op.drop_index(op.f("ix_agent_prompts_agent_name"), table_name="agent_prompts")
    op.drop_table("agent_prompts")
