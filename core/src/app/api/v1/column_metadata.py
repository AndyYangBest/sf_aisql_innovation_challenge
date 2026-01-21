"""API endpoints for column metadata caching and initialization."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.ext.asyncio import AsyncSession

from ...core.db.database import get_async_db_session
from ...services.snowflake_service import SnowflakeService
from ...services.modular_ai_sql_service import ModularAISQLService
from ...services.column_metadata_service import ColumnMetadataService
from ...schemas.column_metadata import (
    ColumnMetadataList,
    ColumnMetadataOverrideRequest,
    ColumnMetadataRead,
    TableAssetMetadataRead,
    TableAssetMetadataOverrideRequest,
)

router = APIRouter(prefix="/column-metadata", tags=["Column Metadata"])


async def get_snowflake_service() -> SnowflakeService:
    return SnowflakeService()


async def get_ai_sql_service(
    sf_service: SnowflakeService = Depends(get_snowflake_service),
) -> ModularAISQLService:
    return ModularAISQLService(sf_service)


@router.get("/{table_asset_id}", response_model=ColumnMetadataList)
async def get_column_metadata(
    table_asset_id: int,
    db: AsyncSession = Depends(get_async_db_session),
    sf_service: SnowflakeService = Depends(get_snowflake_service),
    ai_sql_service: ModularAISQLService = Depends(get_ai_sql_service),
) -> ColumnMetadataList:
    """Fetch cached column metadata for a table asset."""
    service = ColumnMetadataService(db, sf_service, ai_sql_service)
    table_meta, columns = await service.get_cached_metadata(table_asset_id)

    return ColumnMetadataList(
        table=TableAssetMetadataRead.model_validate(table_meta) if table_meta else None,
        columns=[ColumnMetadataRead.model_validate(col) for col in columns],
    )


@router.post("/{table_asset_id}/initialize", response_model=ColumnMetadataList)
async def initialize_column_metadata(
    table_asset_id: int,
    force: bool = Query(False, description="Force refresh even if cache exists"),
    db: AsyncSession = Depends(get_async_db_session),
    sf_service: SnowflakeService = Depends(get_snowflake_service),
    ai_sql_service: ModularAISQLService = Depends(get_ai_sql_service),
) -> ColumnMetadataList:
    """Initialize column metadata with sampling and inference."""
    service = ColumnMetadataService(db, sf_service, ai_sql_service)
    try:
        table_meta, columns = await service.initialize_metadata(table_asset_id, force=force)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return ColumnMetadataList(
        table=TableAssetMetadataRead.model_validate(table_meta),
        columns=[ColumnMetadataRead.model_validate(col) for col in columns],
    )


@router.put("/{table_asset_id}/override", response_model=ColumnMetadataList)
async def override_column_metadata(
    table_asset_id: int,
    request: ColumnMetadataOverrideRequest,
    db: AsyncSession = Depends(get_async_db_session),
    sf_service: SnowflakeService = Depends(get_snowflake_service),
    ai_sql_service: ModularAISQLService = Depends(get_ai_sql_service),
) -> ColumnMetadataList:
    """Override column metadata based on user input."""
    service = ColumnMetadataService(db, sf_service, ai_sql_service)
    table_meta, columns = await service.get_cached_metadata(table_asset_id)
    if not columns:
        raise HTTPException(status_code=404, detail="Column metadata not found")

    target = next((col for col in columns if col.column_name == request.column_name), None)
    if not target:
        raise HTTPException(status_code=404, detail="Column not found")

    overrides = dict(target.overrides or {})
    overrides.update(request.overrides)
    target.overrides = overrides
    flag_modified(target, "overrides")

    await db.commit()
    await db.refresh(target)

    return ColumnMetadataList(
        table=TableAssetMetadataRead.model_validate(table_meta) if table_meta else None,
        columns=[ColumnMetadataRead.model_validate(col) for col in columns],
    )


@router.put("/{table_asset_id}/bulk-override", response_model=ColumnMetadataList)
async def bulk_override_column_metadata(
    table_asset_id: int,
    requests: list[ColumnMetadataOverrideRequest],
    db: AsyncSession = Depends(get_async_db_session),
    sf_service: SnowflakeService = Depends(get_snowflake_service),
    ai_sql_service: ModularAISQLService = Depends(get_ai_sql_service),
) -> ColumnMetadataList:
    """Override column metadata in bulk based on user input."""
    service = ColumnMetadataService(db, sf_service, ai_sql_service)
    table_meta, columns = await service.get_cached_metadata(table_asset_id)
    if not columns:
        try:
            table_meta, columns = await service.initialize_metadata(table_asset_id, force=False)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    column_map = {col.column_name: col for col in columns}
    for request in requests:
        target = column_map.get(request.column_name)
        if not target:
            continue
        overrides = dict(target.overrides or {})
        overrides.update(request.overrides)
        target.overrides = overrides
        flag_modified(target, "overrides")

    await db.commit()

    return ColumnMetadataList(
        table=TableAssetMetadataRead.model_validate(table_meta) if table_meta else None,
        columns=[ColumnMetadataRead.model_validate(col) for col in columns],
    )


@router.put("/{table_asset_id}/table-override", response_model=ColumnMetadataList)
async def override_table_metadata(
    table_asset_id: int,
    request: TableAssetMetadataOverrideRequest,
    db: AsyncSession = Depends(get_async_db_session),
    sf_service: SnowflakeService = Depends(get_snowflake_service),
    ai_sql_service: ModularAISQLService = Depends(get_ai_sql_service),
) -> ColumnMetadataList:
    """Override table metadata based on user input."""
    service = ColumnMetadataService(db, sf_service, ai_sql_service)
    table_meta, columns = await service.get_cached_metadata(table_asset_id)
    if not table_meta:
        try:
            table_meta, columns = await service.initialize_metadata(table_asset_id, force=False)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    overrides = dict(table_meta.overrides or {})
    overrides.update(request.overrides)
    table_meta.overrides = overrides
    flag_modified(table_meta, "overrides")

    await db.commit()
    await db.refresh(table_meta)

    return ColumnMetadataList(
        table=TableAssetMetadataRead.model_validate(table_meta),
        columns=[ColumnMetadataRead.model_validate(col) for col in columns],
    )
