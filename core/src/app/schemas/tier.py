"""Tier schemas."""

from pydantic import BaseModel


class TierRead(BaseModel):
    """Tier read schema."""

    id: int
    name: str
