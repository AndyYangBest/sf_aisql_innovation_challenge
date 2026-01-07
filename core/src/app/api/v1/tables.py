"""API endpoints for Snowflake table management."""

from fastapi import APIRouter, HTTPException, Query

from ...services.snowflake_service import SnowflakeService

router = APIRouter(prefix="/tables", tags=["Tables"])


async def get_snowflake_service() -> SnowflakeService:
    """Get Snowflake service instance."""
    return SnowflakeService()


@router.get(
    "/",
    summary="Get Snowflake Tables",
    description="Fetch list of tables from Snowflake database",
)
async def get_tables(
    database: str | None = Query(None, description="Filter by database name"),
    schema: str | None = Query(None, description="Filter by schema name"),
):
    """Get list of Snowflake tables."""
    try:
        service = await get_snowflake_service()
        tables = await service.get_tables(database=database, schema=schema)
        return {"success": True, "data": tables, "count": len(tables)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/{table_name}/columns",
    summary="Get Table Columns",
    description="Fetch column metadata for a specific table",
)
async def get_table_columns(
    table_name: str,
    database: str | None = Query(None, description="Database name"),
    schema: str | None = Query(None, description="Schema name"),
):
    """Get columns for a specific table."""
    try:
        service = await get_snowflake_service()
        columns = await service.get_table_columns(
            table_name=table_name, database=database, schema=schema
        )
        return {"success": True, "data": columns, "count": len(columns)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/{table_name}/sample",
    summary="Get Sample Data",
    description="Fetch sample rows from a table",
)
async def get_sample_data(
    table_name: str,
    limit: int = Query(100, ge=1, le=1000, description="Number of rows to return"),
    database: str | None = Query(None, description="Database name"),
    schema: str | None = Query(None, description="Schema name"),
):
    """Get sample data from a table."""
    try:
        service = await get_snowflake_service()
        rows = await service.get_sample_data(
            table_name=table_name, limit=limit, database=database, schema=schema
        )
        return {"success": True, "data": rows, "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
