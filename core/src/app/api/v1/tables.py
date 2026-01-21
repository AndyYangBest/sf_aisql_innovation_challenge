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
    "/databases",
    summary="Get Snowflake Databases",
    description="Fetch list of available Snowflake databases",
)
async def get_databases():
    """Get list of Snowflake databases."""
    try:
        service = await get_snowflake_service()
        databases = await service.get_databases()
        return {"success": True, "data": databases, "count": len(databases)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/schemas",
    summary="Get Snowflake Schemas",
    description="Fetch list of schemas in a database",
)
async def get_schemas(
    database: str = Query(..., description="Database name"),
):
    """Get list of schemas for a database."""
    try:
        service = await get_snowflake_service()
        schemas = await service.get_schemas(database=database)
        return {"success": True, "data": schemas, "count": len(schemas)}
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
