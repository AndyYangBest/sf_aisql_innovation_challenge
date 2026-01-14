"""API endpoints for column-level workflows."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from ...core.db.database import get_async_db_session
from ...services.snowflake_service import SnowflakeService
from ...services.modular_ai_sql_service import ModularAISQLService
from ...orchestration.column_workflows import ColumnWorkflowOrchestrator, ColumnWorkflowTools

router = APIRouter(prefix="/column-workflows", tags=["Column Workflows"])


async def get_snowflake_service() -> SnowflakeService:
    return SnowflakeService()


async def get_ai_sql_service(
    sf_service: SnowflakeService = Depends(get_snowflake_service),
) -> ModularAISQLService:
    return ModularAISQLService(sf_service)


@router.post("/{table_asset_id}/{column_name}/estimate")
async def estimate_column_workflow(
    table_asset_id: int,
    column_name: str,
    db: AsyncSession = Depends(get_async_db_session),
    sf_service: SnowflakeService = Depends(get_snowflake_service),
    ai_sql_service: ModularAISQLService = Depends(get_ai_sql_service),
):
    """Estimate token usage for a column workflow."""
    tools = ColumnWorkflowTools(sf_service, ai_sql_service, db)
    try:
        return await tools.estimate_workflow_tokens(table_asset_id, column_name)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/{table_asset_id}/{column_name}/run")
async def run_column_workflow(
    table_asset_id: int,
    column_name: str,
    db: AsyncSession = Depends(get_async_db_session),
    sf_service: SnowflakeService = Depends(get_snowflake_service),
    ai_sql_service: ModularAISQLService = Depends(get_ai_sql_service),
):
    """Run a column workflow using Strands orchestration."""
    orchestrator = ColumnWorkflowOrchestrator(sf_service, ai_sql_service, db)
    try:
        return await orchestrator.run_column_workflow(table_asset_id, column_name)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
