"""API endpoints for column-level workflows."""

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ...core.db.database import get_async_db_session
from ...services.snowflake_service import SnowflakeService
from ...services.modular_ai_sql_service import ModularAISQLService
from ...orchestration.column_workflows import ColumnWorkflowOrchestrator, ColumnWorkflowTools

router = APIRouter(prefix="/column-workflows", tags=["Column Workflows"])


class ColumnWorkflowSelectedRunRequest(BaseModel):
    tool_calls: list[dict[str, Any]] = Field(default_factory=list)
    focus: str | None = None


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
    focus: str | None = Query(default=None),
    db: AsyncSession = Depends(get_async_db_session),
    sf_service: SnowflakeService = Depends(get_snowflake_service),
    ai_sql_service: ModularAISQLService = Depends(get_ai_sql_service),
):
    """Run a column workflow using Strands orchestration."""
    orchestrator = ColumnWorkflowOrchestrator(sf_service, ai_sql_service, db)
    try:
        return await orchestrator.run_column_workflow(table_asset_id, column_name, focus=focus)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/{table_asset_id}/{column_name}/run-selected")
async def run_selected_tools(
    table_asset_id: int,
    column_name: str,
    payload: ColumnWorkflowSelectedRunRequest,
    db: AsyncSession = Depends(get_async_db_session),
    sf_service: SnowflakeService = Depends(get_snowflake_service),
    ai_sql_service: ModularAISQLService = Depends(get_ai_sql_service),
):
    """Run explicitly selected workflow tools for a column."""
    tools = ColumnWorkflowTools(sf_service, ai_sql_service, db)
    from ...services.eda_workflow_persistence import EDAWorkflowPersistenceService
    import uuid
    from datetime import datetime

    workflow_id = f"column_selected_{table_asset_id}_{column_name}_{uuid.uuid4().hex[:8]}"
    persistence = EDAWorkflowPersistenceService(db)
    execution = None
    try:
        execution = await persistence.create_execution(
            workflow_id=workflow_id,
            workflow_type="COLUMN_WORKFLOW_SELECTED",
            table_asset_id=table_asset_id,
            user_intent=f"{column_name}:{payload.focus or 'selected'}",
            user_id=None,
            tasks_total=len(payload.tool_calls or []),
        )
    except Exception:
        execution = None
    try:
        result = await tools.run_selected_tools(
            table_asset_id,
            column_name,
            payload.tool_calls,
            focus=payload.focus,
        )
        if execution:
            logs = result.get("logs", [])
            tool_calls = result.get("tool_calls", [])
            summary = {
                "tool_calls": len(tool_calls),
                "log_entries": len(logs),
                "completed_at": datetime.utcnow().isoformat(),
            }
            await persistence.update_execution(
                workflow_id=workflow_id,
                status="completed",
                progress=100,
                tasks_completed=len(tool_calls),
                tasks_failed=0,
                summary=summary,
                artifacts={"logs": logs, "tool_calls": tool_calls},
            )
            execution_id = execution.id
            for entry in logs:
                await persistence.log_event(
                    workflow_execution_id=execution_id,
                    log_level="ERROR" if entry.get("type") == "error" else "INFO",
                    log_type=str(entry.get("type", "log")),
                    message=str(entry.get("message", "")),
                    details=entry.get("data"),
                )
            for call in tool_calls:
                await persistence.log_event(
                    workflow_execution_id=execution_id,
                    log_level="ERROR" if call.get("status") == "error" else "INFO",
                    log_type="tool_call",
                    message=f"{call.get('tool_name')} ({call.get('status')})",
                    tool_name=call.get("tool_name"),
                    details={
                        "input": call.get("input"),
                        "output_preview": call.get("output_preview"),
                        "error": call.get("error"),
                        "duration_ms": call.get("duration_ms"),
                        "agent_name": call.get("agent_name"),
                    },
                    duration_seconds=(call.get("duration_ms") or 0) / 1000.0 if call.get("duration_ms") else None,
                )
        return result
    except ValueError as exc:
        if execution:
            await persistence.fail_execution(workflow_id=workflow_id, error_message=str(exc))
        raise HTTPException(status_code=404, detail=str(exc)) from exc
