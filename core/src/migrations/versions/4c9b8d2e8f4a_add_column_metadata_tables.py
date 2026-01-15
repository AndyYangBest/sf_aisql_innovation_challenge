"""add column metadata tables

Revision ID: 4c9b8d2e8f4a
Revises: 9ba36d2688cf
Create Date: 2026-01-12 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4c9b8d2e8f4a'
down_revision: Union[str, None] = '9ba36d2688cf'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not inspector.has_table('table_asset_metadata'):
        op.create_table(
            'table_asset_metadata',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('table_asset_id', sa.Integer(), nullable=False),
            sa.Column('structure_type', sa.String(length=50), nullable=False),
            sa.Column('sampling_strategy', sa.String(length=50), nullable=False),
            sa.Column('metadata', sa.JSON(), nullable=True),
            sa.Column('overrides', sa.JSON(), nullable=True),
            sa.Column('last_updated', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['table_asset_id'], ['table_assets.id']),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('table_asset_id', name='uq_table_asset_metadata_table_asset'),
        )
        op.create_index(op.f('ix_table_asset_metadata_id'), 'table_asset_metadata', ['id'], unique=False)
        op.create_index(
            op.f('ix_table_asset_metadata_table_asset_id'),
            'table_asset_metadata',
            ['table_asset_id'],
            unique=True,
        )

    if not inspector.has_table('column_metadata'):
        op.create_table(
            'column_metadata',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('table_asset_id', sa.Integer(), nullable=False),
            sa.Column('column_name', sa.String(length=255), nullable=False),
            sa.Column('semantic_type', sa.String(length=50), nullable=False),
            sa.Column('confidence', sa.Float(), nullable=False),
            sa.Column('metadata', sa.JSON(), nullable=True),
            sa.Column('provenance', sa.JSON(), nullable=True),
            sa.Column('examples', sa.JSON(), nullable=True),
            sa.Column('overrides', sa.JSON(), nullable=True),
            sa.Column('last_updated', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['table_asset_id'], ['table_assets.id']),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('table_asset_id', 'column_name', name='uq_column_metadata_table_asset_column'),
        )
        op.create_index(op.f('ix_column_metadata_id'), 'column_metadata', ['id'], unique=False)
        op.create_index(op.f('ix_column_metadata_column_name'), 'column_metadata', ['column_name'], unique=False)
        op.create_index(op.f('ix_column_metadata_table_asset_id'), 'column_metadata', ['table_asset_id'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_column_metadata_table_asset_id'), table_name='column_metadata')
    op.drop_index(op.f('ix_column_metadata_column_name'), table_name='column_metadata')
    op.drop_index(op.f('ix_column_metadata_id'), table_name='column_metadata')
    op.drop_table('column_metadata')

    op.drop_index(op.f('ix_table_asset_metadata_table_asset_id'), table_name='table_asset_metadata')
    op.drop_index(op.f('ix_table_asset_metadata_id'), table_name='table_asset_metadata')
    op.drop_table('table_asset_metadata')
