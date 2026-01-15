"""drop post and tier tables

Revision ID: c2f8d7a5e9b1
Revises: 4c9b8d2e8f4a
Create Date: 2026-01-15 10:12:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "c2f8d7a5e9b1"
down_revision: Union[str, None] = "4c9b8d2e8f4a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop legacy post/tier tables if they still exist.
    op.execute("DROP TABLE IF EXISTS post CASCADE")
    op.execute("DROP TABLE IF EXISTS posts CASCADE")
    op.execute("DROP TABLE IF EXISTS tier CASCADE")
    op.execute("DROP TABLE IF EXISTS tiers CASCADE")


def downgrade() -> None:
    raise NotImplementedError("Dropping legacy post/tier tables is irreversible.")
