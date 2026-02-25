"""Report layout event history for drag/resize interactions."""

from sqlalchemy import TIMESTAMP, Column, ForeignKey, Integer, String
from sqlalchemy.sql import func

from ..core.db.database import Base


class ReportLayoutEvent(Base):
    """Stores each report layout adjustment for audit and restore."""

    __tablename__ = "report_layout_events"

    id = Column(Integer, primary_key=True, index=True)
    table_asset_id = Column(
        Integer,
        ForeignKey("table_assets.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    card_id = Column(String(255), index=True, nullable=False)
    artifact_id = Column(String(255), index=True, nullable=False)
    card_kind = Column(String(32), nullable=False)
    event_type = Column(String(32), nullable=False)
    x = Column(Integer, nullable=False)
    y = Column(Integer, nullable=False)
    w = Column(Integer, nullable=False)
    h = Column(Integer, nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
