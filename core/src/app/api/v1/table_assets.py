"""API endpoints for Table Assets management."""

import json
import re
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_

from ...core.db.database import get_async_db_session
from ...models.table_asset import TableAsset
from ...models.table_asset_metadata import TableAssetMetadata
from ...models.column_metadata import ColumnMetadata
from ...services.snowflake_service import SnowflakeService
from ...schemas.table_asset import (
    TableAssetCreate,
    TableAssetRead,
    TableAssetUpdate,
    TableAssetList,
    TableAssetPreview,
    TablePreviewColumn,
)

router = APIRouter(prefix="/table-assets", tags=["Table Assets"])


async def get_snowflake_service() -> SnowflakeService:
    return SnowflakeService()


def _looks_like_table_ref(text: str) -> bool:
    if not text:
        return False
    if " " in text or "\n" in text or "\t" in text:
        return False
    return re.match(r"^[A-Za-z0-9_\.]+$", text) is not None


def _build_base_query(asset: TableAsset, table_meta: TableAssetMetadata | None) -> tuple[str, str | None]:
    if table_meta and table_meta.metadata_payload:
        base_query = table_meta.metadata_payload.get("base_query")
        if base_query:
            return str(base_query), table_meta.metadata_payload.get("table_ref")

    source_sql = (asset.source_sql or "").strip().rstrip(";")
    if _looks_like_table_ref(source_sql):
        table_ref = source_sql
        if asset.database and asset.schema and "." not in table_ref:
            table_ref = f"{asset.database}.{asset.schema}.{table_ref}"
        return f"SELECT * FROM {table_ref}", table_ref

    return source_sql, None


def _extract_table_ref(base_query: str) -> str | None:
    match = re.search(r"FROM\\s+([A-Za-z0-9_\\.]+)", base_query, re.IGNORECASE)
    return match.group(1) if match else None


def _split_table_ref(table_ref: str) -> tuple[str | None, str | None, str]:
    parts = table_ref.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return None, None, table_ref


def _role_from_semantic(semantic_type: str | None) -> str | None:
    if not semantic_type:
        return None
    mapping = {
        "numeric": "metric",
        "temporal": "time",
        "categorical": "dimension",
        "text": "dimension",
        "spatial": "dimension",
        "binary": "dimension",
        "image": "dimension",
        "id": "id",
    }
    return mapping.get(semantic_type)


async def _execute_preview_query(service: SnowflakeService, base_query: str, limit: int) -> list[dict[str, Any]]:
    trimmed = base_query.strip().rstrip(";")
    wrapped_sql = f"SELECT * FROM ({trimmed}) LIMIT {limit}"
    try:
        return await service.execute_query(wrapped_sql)
    except Exception as primary_error:
        json_sql = f"""
        SELECT TO_JSON(OBJECT_CONSTRUCT(*)) AS ROW_JSON
        FROM ({trimmed})
        LIMIT {limit}
        """
        try:
            json_rows = await service.execute_query(json_sql)
        except Exception as fallback_error:
            raise HTTPException(status_code=500, detail=str(fallback_error)) from primary_error

        parsed_rows = []
        for row in json_rows:
            raw = row.get("ROW_JSON")
            if raw is None:
                parsed_rows.append({})
                continue
            try:
                parsed_rows.append(json.loads(raw))
            except Exception:
                parsed_rows.append({"ROW_JSON": raw})
        return parsed_rows


@router.post(
    "/",
    response_model=TableAssetRead,
    summary="Create Table Asset",
    description="Save a new table asset to the database",
)
async def create_table_asset(
    asset: TableAssetCreate,
    db: AsyncSession = Depends(get_async_db_session),
) -> TableAssetRead:
    """Create a new table asset."""
    try:
        # Create new table asset
        db_asset = TableAsset()
        db_asset.name = asset.name
        db_asset.source_sql = asset.source_sql
        db_asset.database = asset.database
        db_asset.schema = asset.schema
        db_asset.tags = asset.tags
        db_asset.owner = asset.owner
        db_asset.ai_summary = asset.ai_summary
        db_asset.use_cases = asset.use_cases
        db_asset.user_id = asset.user_id

        db.add(db_asset)
        await db.commit()
        await db.refresh(db_asset)

        return TableAssetRead.model_validate(db_asset)
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/",
    response_model=TableAssetList,
    summary="Get Table Assets",
    description="Retrieve all table assets with optional pagination and filters",
)
async def get_table_assets(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Items per page"),
    search: str | None = Query(None, description="Search in name, tags"),
    owner: str | None = Query(None, description="Filter by owner"),
    db: AsyncSession = Depends(get_async_db_session),
) -> TableAssetList:
    """Get all table assets with pagination."""
    try:
        # Build query
        query = select(TableAsset).where(TableAsset.is_deleted == False)

        # Apply filters
        if search:
            query = query.where(
                TableAsset.name.ilike(f"%{search}%")
            )

        if owner:
            query = query.where(TableAsset.owner == owner)

        # Order by most recent first
        query = query.order_by(TableAsset.created_at.desc())

        # Get total count
        count_query = select(TableAsset).where(TableAsset.is_deleted == False)
        if search:
            count_query = count_query.where(TableAsset.name.ilike(f"%{search}%"))
        if owner:
            count_query = count_query.where(TableAsset.owner == owner)

        result = await db.execute(count_query)
        total = len(result.scalars().all())

        # Apply pagination
        query = query.offset((page - 1) * page_size).limit(page_size)

        # Execute query
        result = await db.execute(query)
        assets = result.scalars().all()

        # Convert to response model
        items = [TableAssetRead.model_validate(asset) for asset in assets]

        return TableAssetList(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/{asset_id}",
    response_model=TableAssetRead,
    summary="Get Table Asset by ID",
    description="Retrieve a specific table asset by its ID",
)
async def get_table_asset(
    asset_id: int,
    db: AsyncSession = Depends(get_async_db_session),
) -> TableAssetRead:
    """Get a table asset by ID."""
    try:
        result = await db.execute(
            select(TableAsset).where(
                and_(
                    TableAsset.id == asset_id,
                    TableAsset.is_deleted == False,
                )
            )
        )
        asset = result.scalar_one_or_none()

        if not asset:
            raise HTTPException(status_code=404, detail="Table asset not found")

        return TableAssetRead.model_validate(asset)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/{asset_id}/preview",
    response_model=TableAssetPreview,
    summary="Preview Table Asset",
    description="Return schema and sample rows for a table asset",
)
async def get_table_asset_preview(
    asset_id: int,
    limit: int = Query(50, ge=1, le=200, description="Number of preview rows"),
    db: AsyncSession = Depends(get_async_db_session),
    sf_service: SnowflakeService = Depends(get_snowflake_service),
) -> TableAssetPreview:
    """Get table asset preview rows and column info."""
    asset_result = await db.execute(
        select(TableAsset).where(
            and_(
                TableAsset.id == asset_id,
                TableAsset.is_deleted == False,
            )
        )
    )
    asset = asset_result.scalar_one_or_none()
    if not asset:
        raise HTTPException(status_code=404, detail="Table asset not found")

    table_meta_result = await db.execute(
        select(TableAssetMetadata).where(TableAssetMetadata.table_asset_id == asset_id)
    )
    table_meta = table_meta_result.scalar_one_or_none()

    base_query, _table_ref = _build_base_query(asset, table_meta)
    if not base_query:
        raise HTTPException(status_code=400, detail="Table asset has no source SQL")

    rows = await _execute_preview_query(sf_service, base_query, limit)

    columns_result = await db.execute(
        select(ColumnMetadata)
        .where(ColumnMetadata.table_asset_id == asset_id)
        .order_by(ColumnMetadata.id)
    )
    column_meta = list(columns_result.scalars().all())

    preview_columns: list[TablePreviewColumn] = []
    if column_meta:
        preview_columns = [
            TablePreviewColumn(
                name=col.column_name,
                type=(col.metadata_payload or {}).get("sql_type"),
                role=_role_from_semantic(col.semantic_type),
            )
            for col in column_meta
        ]
    else:
        table_ref = _extract_table_ref(base_query)
        if table_ref:
            database, schema, table_name = _split_table_ref(table_ref)
            try:
                schema_rows = await sf_service.get_table_columns(
                    table_name=table_name,
                    database=database,
                    schema=schema,
                )
                preview_columns = [
                    TablePreviewColumn(
                        name=row.get("COLUMN_NAME"),
                        type=row.get("DATA_TYPE"),
                    )
                    for row in schema_rows
                    if row.get("COLUMN_NAME")
                ]
            except Exception:
                preview_columns = []

        if not preview_columns and rows:
            preview_columns = [
                TablePreviewColumn(name=column_name)
                for column_name in rows[0].keys()
            ]

    return TableAssetPreview(
        columns=preview_columns,
        rows=rows,
        row_count=len(rows),
    )


@router.put(
    "/{asset_id}",
    response_model=TableAssetRead,
    summary="Update Table Asset",
    description="Update an existing table asset",
)
async def update_table_asset(
    asset_id: int,
    asset_update: TableAssetUpdate,
    db: AsyncSession = Depends(get_async_db_session),
) -> TableAssetRead:
    """Update a table asset."""
    try:
        result = await db.execute(
            select(TableAsset).where(
                and_(
                    TableAsset.id == asset_id,
                    TableAsset.is_deleted == False,
                )
            )
        )
        asset = result.scalar_one_or_none()

        if not asset:
            raise HTTPException(status_code=404, detail="Table asset not found")

        # Update fields
        update_data = asset_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(asset, field, value)

        await db.commit()
        await db.refresh(asset)

        return TableAssetRead.model_validate(asset)
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/{asset_id}",
    summary="Delete Table Asset",
    description="Soft delete a table asset (mark as deleted)",
)
async def delete_table_asset(
    asset_id: int,
    db: AsyncSession = Depends(get_async_db_session),
) -> dict:
    """Delete a table asset (soft delete)."""
    try:
        result = await db.execute(
            select(TableAsset).where(
                and_(
                    TableAsset.id == asset_id,
                    TableAsset.is_deleted == False,
                )
            )
        )
        asset = result.scalar_one_or_none()

        if not asset:
            raise HTTPException(status_code=404, detail="Table asset not found")

        # Soft delete
        asset.is_deleted = True
        await db.commit()

        return {"success": True, "message": "Table asset deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
