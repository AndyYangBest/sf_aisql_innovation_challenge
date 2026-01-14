"""Table-level metadata cache for column semantics-driven workflows."""

from sqlalchemy import Column, Integer, String, ForeignKey, JSON, TIMESTAMP, UniqueConstraint
from sqlalchemy.sql import func

from ..core.db.database import Base


class TableAssetMetadata(Base):
    """Stores table-level structure and sampling metadata."""

    __tablename__ = "table_asset_metadata"
    __table_args__ = (
        UniqueConstraint("table_asset_id", name="uq_table_asset_metadata_table_asset"),
    )

    id = Column(Integer, primary_key=True, index=True)
    table_asset_id = Column(Integer, ForeignKey("table_assets.id"), index=True, nullable=False)
    structure_type = Column(String(50), nullable=False)
    sampling_strategy = Column(String(50), nullable=False)
    metadata_payload = Column("metadata", JSON, default=None)
    overrides = Column(JSON, default=None)
    last_updated = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
