"""Table Asset model for storing user-created SQL tables."""

from sqlalchemy import Boolean, Column, Integer, String, Text, TIMESTAMP, ARRAY
from sqlalchemy.sql import func
from ..core.db.database import Base


class TableAsset(Base):
    """Model for storing table assets created by users."""

    __tablename__ = "table_assets"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    source_sql = Column(Text, nullable=False)
    database = Column(String(255))
    schema = Column(String(255))
    tags = Column(ARRAY(String), default=list)
    owner = Column(String(255), index=True)
    ai_summary = Column(Text)
    use_cases = Column(ARRAY(String), default=list)

    # Metadata fields
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    is_deleted = Column(Boolean, default=False)

    # User reference (optional, for future use)
    user_id = Column(Integer, index=True)
