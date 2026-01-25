"""Shared context for column workflows."""

from __future__ import annotations

from dataclasses import dataclass

from ..models.column_metadata import ColumnMetadata
from ..models.table_asset_metadata import TableAssetMetadata


@dataclass
class ColumnContext:
    table_asset_id: int
    column_name: str
    base_query: str
    analysis_query: str
    table_ref: str | None
    time_column: str | None
    structure_type: str | None
    column_meta: ColumnMetadata
    table_meta: TableAssetMetadata
