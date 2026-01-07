"""Rate limit model placeholder."""

from sqlalchemy import Column, Integer, String
from ..core.db.database import Base


class RateLimit(Base):
    """Rate limit model."""

    __tablename__ = "rate_limits"

    id = Column(Integer, primary_key=True, index=True)
    tier_id = Column(Integer)
    path = Column(String)
    limit = Column(Integer)
    period = Column(Integer)
