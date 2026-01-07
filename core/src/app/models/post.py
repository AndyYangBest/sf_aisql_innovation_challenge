"""Post model placeholder."""

from sqlalchemy import Column, Integer, String
from ..core.db.database import Base


class Post(Base):
    """Post model."""

    __tablename__ = "posts"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, index=True)
    content = Column(String)
