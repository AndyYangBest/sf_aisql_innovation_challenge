"""Schemas for column metadata cache."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, ConfigDict


class ColumnMetadataBase(BaseModel):
    """Base schema for column metadata."""
    table_asset_id: int
    column_name: str = Field(..., min_length=1, max_length=255)
    semantic_type: str = Field(..., min_length=1, max_length=50)
    confidence: float = Field(..., ge=0.0, le=1.0)
    metadata: dict[str, Any] | None = Field(default=None, validation_alias="metadata_payload")
    provenance: dict[str, Any] | None = None
    examples: list[Any] | None = None
    overrides: dict[str, Any] | None = None
    last_updated: datetime | None = None


class ColumnMetadataRead(ColumnMetadataBase):
    """Read schema for column metadata."""
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class TableAssetMetadataBase(BaseModel):
    """Base schema for table-level metadata."""
    table_asset_id: int
    structure_type: str = Field(..., min_length=1, max_length=50)
    sampling_strategy: str = Field(..., min_length=1, max_length=50)
    metadata: dict[str, Any] | None = Field(default=None, validation_alias="metadata_payload")
    overrides: dict[str, Any] | None = None
    last_updated: datetime | None = None


class TableAssetMetadataRead(TableAssetMetadataBase):
    """Read schema for table-level metadata."""
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class ColumnMetadataList(BaseModel):
    """Combined table + column metadata response."""
    table: TableAssetMetadataRead | None = None
    columns: list[ColumnMetadataRead] = []


class ColumnMetadataOverrideRequest(BaseModel):
    """Request to override column metadata with user input."""
    column_name: str = Field(..., min_length=1, max_length=255)
    overrides: dict[str, Any] = Field(default_factory=dict)


class TableAssetMetadataOverrideRequest(BaseModel):
    """Request to override table-level metadata with user input."""
    overrides: dict[str, Any] = Field(default_factory=dict)
