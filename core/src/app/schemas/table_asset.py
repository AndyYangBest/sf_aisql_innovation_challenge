"""Table Asset schemas for request/response validation."""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class TableAssetBase(BaseModel):
    """Base schema for table asset."""
    name: str = Field(..., min_length=1, max_length=255)
    source_sql: str = Field(..., min_length=1)
    database: Optional[str] = Field(None, max_length=255)
    schema: Optional[str] = Field(None, max_length=255)
    tags: list[str] = Field(default_factory=list)
    owner: Optional[str] = Field(None, max_length=255)
    ai_summary: Optional[str] = None
    use_cases: list[str] = Field(default_factory=list)


class TableAssetCreate(TableAssetBase):
    """Schema for creating a table asset."""
    user_id: Optional[int] = None


class TableAssetUpdate(BaseModel):
    """Schema for updating a table asset."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    source_sql: Optional[str] = Field(None, min_length=1)
    database: Optional[str] = Field(None, max_length=255)
    schema: Optional[str] = Field(None, max_length=255)
    tags: Optional[list[str]] = None
    ai_summary: Optional[str] = None
    use_cases: Optional[list[str]] = None


class TableAssetRead(TableAssetBase):
    """Schema for reading a table asset."""
    id: int
    created_at: datetime
    updated_at: datetime
    is_deleted: bool
    user_id: Optional[int] = None

    class Config:
        from_attributes = True


class TableAssetList(BaseModel):
    """Schema for listing table assets."""
    items: list[TableAssetRead]
    total: int
    page: int = 1
    page_size: int = 50
