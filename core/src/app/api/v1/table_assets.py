"""API endpoints for Table Assets management."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_

from ...core.db.database import get_async_db_session
from ...models.table_asset import TableAsset
from ...schemas.table_asset import (
    TableAssetCreate,
    TableAssetRead,
    TableAssetUpdate,
    TableAssetList,
)

router = APIRouter(prefix="/table-assets", tags=["Table Assets"])


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
