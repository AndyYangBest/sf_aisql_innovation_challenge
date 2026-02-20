"""Agent prompt templates stored in Postgres."""

from sqlalchemy import Boolean, Column, Integer, String, Text, TIMESTAMP
from sqlalchemy.sql import func

from ..core.db.database import Base


class AgentPrompt(Base):
    """Stores prompts for workflow agents."""

    __tablename__ = "agent_prompts"

    id = Column(Integer, primary_key=True, index=True)
    agent_name = Column(String(100), nullable=False, unique=True, index=True)
    prompt = Column(Text, nullable=False)
    system_prompt = Column(Text, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
