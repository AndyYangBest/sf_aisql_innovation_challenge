"""Tier model placeholder."""

from sqlalchemy import Column, Integer, String
from ..core.db.database import Base


class Tier(Base):
    """Tier model."""

    __tablename__ = "tiers"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
