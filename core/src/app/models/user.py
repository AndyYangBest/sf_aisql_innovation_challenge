"""User model placeholder."""

from sqlalchemy import Boolean, Column, Integer, String
from ..core.db.database import Base


class User(Base):
    """User model."""

    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    email = Column(String, unique=True, index=True)
    is_superuser = Column(Boolean, default=False)
    tier_id = Column(Integer)
    is_deleted = Column(Boolean, default=False)
