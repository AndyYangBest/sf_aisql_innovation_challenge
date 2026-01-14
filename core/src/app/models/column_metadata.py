"""Column metadata cache for semantic typing and analysis."""

from sqlalchemy import Column, Integer, String, Float, ForeignKey, JSON, TIMESTAMP, UniqueConstraint
from sqlalchemy.sql import func

from ..core.db.database import Base


class ColumnMetadata(Base):
    """Stores semantic metadata for a single column in a table asset."""

    __tablename__ = "column_metadata"
    __table_args__ = (
        UniqueConstraint("table_asset_id", "column_name", name="uq_column_metadata_table_asset_column"),
    )

    id = Column(Integer, primary_key=True, index=True)
    table_asset_id = Column(Integer, ForeignKey("table_assets.id"), index=True, nullable=False)
    column_name = Column(String(255), index=True, nullable=False)
    semantic_type = Column(String(50), nullable=False)
    confidence = Column(Float, nullable=False, default=0.0)
    metadata_payload = Column("metadata", JSON, default=None)
    provenance = Column(JSON, default=None)
    examples = Column(JSON, default=None)
    overrides = Column(JSON, default=None)
    last_updated = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
