"""API endpoints for EDA (Exploratory Data Analysis) workflows.

This module provides REST API endpoints for running EDA workflows on table_assets
using the Strands-based agent orchestration system.
"""

from typing import Any, Literal
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, Integer
import base64
import json
import asyncio
import logging
import re
from datetime import datetime
from pathlib import Path

from ...core.db.database import get_async_db_session
from ...models.table_asset import TableAsset
from ...services.snowflake_service import SnowflakeService
from ...services.modular_ai_sql_service import ModularAISQLService
from ...orchestration.eda_workflows import (
    create_eda_orchestrator,
    WorkflowType,
)
from strands_tools import workflow as workflow_tool

router = APIRouter(prefix="/eda", tags=["EDA Workflows"])


# ============================================================================
# Request/Response Models
# ============================================================================


class EDARequest(BaseModel):
    """Request to run EDA on a table asset."""

    table_asset_id: int = Field(..., description="ID of the table asset to analyze")
    user_intent: str | None = Field(
        None,
        description="Optional user goal (e.g., 'find trends', 'check quality')",
    )
    workflow_type: WorkflowType | None = Field(
        None,
        description="Optional explicit workflow type (overrides auto-routing)",
    )


class EDAResponse(BaseModel):
    """Response from EDA workflow execution."""

    success: bool
    workflow: str
    table_asset_id: int
    table_name: str
    artifacts: dict[str, Any]
    summary: dict[str, Any]
    error: str | None = None


class WorkflowListResponse(BaseModel):
    """List of available EDA workflows."""

    workflows: list[dict[str, Any]]


class TableAssetListResponse(BaseModel):
    """List of table assets available for EDA."""

    table_assets: list[dict[str, Any]]
    total: int


class WorkflowCancelResponse(BaseModel):
    """Response for cancelling a workflow."""

    success: bool
    workflow_id: str
    status: str
    error: str | None = None


# ============================================================================
# Dependency Injection
# ============================================================================


async def get_snowflake_service() -> SnowflakeService:
    """Get Snowflake service instance."""
    return SnowflakeService()


async def get_ai_sql_service(
    sf_service: SnowflakeService = Depends(get_snowflake_service),
) -> ModularAISQLService:
    """Get AI SQL service instance."""
    return ModularAISQLService(sf_service)


# ============================================================================
# API Endpoints
# ============================================================================


@router.post("/run", response_model=EDAResponse)
async def run_eda_workflow(
    request: EDARequest,
    db: AsyncSession = Depends(get_async_db_session),
    sf_service: SnowflakeService = Depends(get_snowflake_service),
    ai_sql_service: ModularAISQLService = Depends(get_ai_sql_service),
):
    """Run EDA workflow on a table asset.

    This endpoint:
    1. Retrieves the table asset from PostgreSQL
    2. Routes to appropriate workflow (or uses explicit workflow_type)
    3. Executes the workflow using Strands agents
    4. Persists workflow execution to database
    5. Returns comprehensive EDA results

    Workflows:
    - EDA_OVERVIEW: General comprehensive analysis (default)
    - EDA_TIME_SERIES: Time-series and trend analysis
    - EDA_DATA_QUALITY: Data quality validation and checks

    Example request:
    ```json
    {
        "table_asset_id": 1,
        "user_intent": "find trends and patterns",
        "workflow_type": null  // Auto-route
    }
    ```
    """
    try:
        # Fetch table asset from database
        result = await db.execute(
            select(TableAsset).where(
                TableAsset.id == request.table_asset_id,
                TableAsset.is_deleted == False,
            )
        )
        table_asset = result.scalar_one_or_none()

        if not table_asset:
            raise HTTPException(
                status_code=404,
                detail=f"Table asset with ID {request.table_asset_id} not found",
            )

        # Create orchestrator with database session for persistence
        orchestrator = create_eda_orchestrator(sf_service, ai_sql_service, db=db)

        # Run EDA workflow
        results = await orchestrator.run_eda(
            table_asset=table_asset,
            user_intent=request.user_intent,
            workflow_type=request.workflow_type,
        )

        return EDAResponse(
            success=True,
            workflow=results["workflow"],
            table_asset_id=results["table_asset_id"],
            table_name=results["table_name"],
            artifacts=results["artifacts"],
            summary=results["summary"],
            error=None,
        )

    except HTTPException:
        raise
    except Exception as e:
        return EDAResponse(
            success=False,
            workflow="",
            table_asset_id=request.table_asset_id,
            table_name="",
            artifacts={},
            summary={},
            error=str(e),
        )


@router.get("/run-stream")
async def run_eda_workflow_stream(
    table_asset_id: int,
    request: Request,
    user_intent: str | None = None,
    workflow_type: WorkflowType | None = None,
    workflow_json: str | None = None,
    db: AsyncSession = Depends(get_async_db_session),
    sf_service: SnowflakeService = Depends(get_snowflake_service),
    ai_sql_service: ModularAISQLService = Depends(get_ai_sql_service),
):
    """Run EDA workflow with streaming logs (Server-Sent Events).

    This endpoint streams workflow execution logs in real-time using SSE.
    The frontend can display these logs as they arrive.

    Event types:
    - log: General log messages
    - status: Node status updates
    - progress: Progress updates
    - complete: Workflow completion
    - error: Error messages
    """

    def _sse(event_type: str, payload: dict[str, Any]) -> str:
        return f"event: {event_type}\ndata: {json.dumps(payload, default=str)}\n\n"

    async def event_generator():
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        done_event = asyncio.Event()
        result_container: dict[str, Any] = {}
        loop = asyncio.get_running_loop()
        workflow_id_event = asyncio.Event()
        workflow_id_holder: dict[str, str | None] = {"id": None}
        workflow_graph: dict[str, Any] | None = None

        if workflow_json:
            try:
                decoded = base64.b64decode(workflow_json).decode("utf-8")
                workflow_graph = json.loads(decoded)
            except Exception:
                workflow_graph = None

        def _enqueue(event_type: str, payload: dict[str, Any]) -> None:
            item = {"event": event_type, "payload": payload}
            try:
                loop.call_soon_threadsafe(queue.put_nowait, item)
            except RuntimeError:
                queue.put_nowait(item)

        class _SSELogHandler(logging.Handler):
            def __init__(self) -> None:
                super().__init__(logging.INFO)
                self.setFormatter(logging.Formatter("%(message)s"))

            def emit(self, record: logging.LogRecord) -> None:
                try:
                    message = self.format(record)
                    event_type = "log"
                    data: dict[str, Any] | None = None

                    # Detect strands-specific log patterns
                    if "strands" in record.name.lower():
                        # Mark strands logs with a special indicator
                        if "workflow" in record.name.lower():
                            event_type = "workflow_log"
                        elif "telemetry" in record.name.lower():
                            event_type = "telemetry_log"
                        else:
                            event_type = "strands_log"

                    workflow_match = re.search(r"Creating workflow '([^']+)'", message)
                    if workflow_match and not workflow_id_event.is_set():
                        workflow_id = workflow_match.group(1)
                        workflow_id_holder["id"] = workflow_id
                        workflow_id_event.set()
                        _enqueue(
                            "status",
                            {
                                "type": "status",
                                "timestamp": datetime.utcnow().isoformat(),
                                "message": f"Workflow ID: {workflow_id}",
                                "data": {"workflow_id": workflow_id},
                            },
                        )

                    start_match = re.search(r"Task Started: (\S+)", message)
                    complete_match = re.search(r"Task Completed: (\S+)", message)
                    succeeded_match = re.search(r"Task '([^']+)' completed", message)
                    failed_match = re.search(r"Task Failed: (\S+)", message)
                    failed_named_match = re.search(r"Task '([^']+)' failed", message)

                    task_id = None
                    state = None
                    if start_match:
                        task_id = start_match.group(1)
                        state = "running"
                    elif complete_match:
                        task_id = complete_match.group(1)
                        state = "success"
                    elif succeeded_match:
                        task_id = succeeded_match.group(1)
                        state = "success"
                    elif failed_match:
                        task_id = failed_match.group(1)
                        state = "error"
                    elif failed_named_match:
                        task_id = failed_named_match.group(1)
                        state = "error"

                    if task_id and state and task_id != "workflow":
                        event_type = "status"
                        data = {"task_id": task_id, "state": state}

                    payload = {
                        "type": event_type,
                        "timestamp": datetime.utcnow().isoformat(),
                        "message": message,
                        "data": data,
                    }
                    _enqueue(event_type, payload)
                except Exception:
                    return

        log_handler = _SSELogHandler()
        stream_loggers = [
            logging.getLogger("strands_tools.workflow"),
            logging.getLogger("strands"),
            logging.getLogger("strands.telemetry"),
            logging.getLogger("strands.telemetry.metrics"),
            logging.getLogger("httpx"),
            logging.getLogger("snowflake.connector"),
            logging.getLogger("snowflake.connector.connection"),
            logging.getLogger("src.app.orchestration.eda_hooks"),
            logging.getLogger("src.app.orchestration.eda_workflows"),
        ]
        for logger in stream_loggers:
            logger.addHandler(log_handler)

        async def poll_workflow_status() -> None:
            await workflow_id_event.wait()
            workflow_id = workflow_id_holder["id"]
            if not workflow_id:
                return

            workflow_path = Path.home() / ".strands" / "workflows" / f"{workflow_id}.json"
            last_task_states: dict[str, str] = {}
            last_progress: int | None = None

            while not done_event.is_set():
                if await request.is_disconnected():
                    return

                if workflow_path.exists():
                    try:
                        workflow_data = json.loads(workflow_path.read_text())
                    except Exception:
                        await asyncio.sleep(0.5)
                        continue

                    tasks = workflow_data.get("tasks", [])
                    task_results = workflow_data.get("task_results", {})
                    status_by_id = {
                        task_id: result.get("status", "pending")
                        for task_id, result in task_results.items()
                    }
                    workflow_status = workflow_data.get("status", "created")

                    for task in tasks:
                        task_id = task.get("task_id")
                        if not task_id:
                            continue

                        raw_status = status_by_id.get(task_id, "pending")
                        if raw_status == "completed":
                            state = "success"
                        elif raw_status in ("error", "failed"):
                            state = "error"
                        elif raw_status in ("running", "in_progress"):
                            state = "running"
                        else:
                            dependencies = task.get("dependencies") or []
                            deps_complete = all(
                                status_by_id.get(dep) == "completed" for dep in dependencies
                            )
                            if deps_complete and workflow_status == "running":
                                state = "running"
                            else:
                                state = "idle"

                        if state in ("running", "success", "error"):
                            if last_task_states.get(task_id) != state:
                                last_task_states[task_id] = state
                                _enqueue(
                                    "status",
                                    {
                                        "type": "status",
                                        "timestamp": datetime.utcnow().isoformat(),
                                        "message": f"Task {task_id} {state}",
                                        "data": {"task_id": task_id, "state": state},
                                    },
                                )

                    tasks_total = len([task for task in tasks if task.get("task_id")])
                    tasks_completed = sum(
                        1 for status in status_by_id.values() if status == "completed"
                    )

                    if tasks_total > 0:
                        progress = int((tasks_completed / tasks_total) * 100)
                        if progress != last_progress:
                            last_progress = progress
                            _enqueue(
                                "progress",
                                {
                                    "type": "progress",
                                    "timestamp": datetime.utcnow().isoformat(),
                                    "message": f"Progress: {progress}%",
                                    "data": {
                                        "progress": progress,
                                        "tasks_completed": tasks_completed,
                                        "tasks_total": tasks_total,
                                    },
                                },
                            )

                    if workflow_status in ("completed", "error", "cancelled"):
                        break

                await asyncio.sleep(0.5)

        async def run_workflow() -> None:
            resolved_workflow_type = workflow_type
            try:
                _enqueue(
                    "log",
                    {
                        "type": "log",
                        "timestamp": datetime.utcnow().isoformat(),
                        "message": "Starting EDA workflow...",
                    },
                )

                result = await db.execute(
                    select(TableAsset).where(
                        TableAsset.id == table_asset_id,
                        TableAsset.is_deleted == False,
                    )
                )
                table_asset = result.scalar_one_or_none()

                if not table_asset:
                    result_container["error"] = f"Table asset {table_asset_id} not found"
                    return

                _enqueue(
                    "log",
                    {
                        "type": "log",
                        "timestamp": datetime.utcnow().isoformat(),
                        "message": f"Found table: {table_asset.name}",
                    },
                )

                orchestrator = create_eda_orchestrator(
                    sf_service,
                    ai_sql_service,
                    db=db,
                )

                _enqueue(
                    "status",
                    {
                        "type": "status",
                        "timestamp": datetime.utcnow().isoformat(),
                        "message": "Orchestrator created",
                    },
                )

                if resolved_workflow_type is None:
                    resolved_workflow_type = await orchestrator.router.route_workflow(
                        table_asset,
                        user_intent,
                    )
                    _enqueue(
                        "log",
                        {
                            "type": "log",
                            "timestamp": datetime.utcnow().isoformat(),
                            "message": f"Auto-routed to workflow: {resolved_workflow_type}",
                        },
                    )

                _enqueue(
                    "status",
                    {
                        "type": "status",
                        "timestamp": datetime.utcnow().isoformat(),
                        "message": f"Starting {resolved_workflow_type} workflow",
                    },
                )

                tasks_override: list[dict[str, Any]] | None = None
                if workflow_graph:
                    try:
                        tasks_override = orchestrator.build_tasks_from_graph(
                            workflow_graph,
                            resolved_workflow_type,
                            table_asset,
                            user_intent,
                        )
                        if not tasks_override:
                            _enqueue(
                                "log",
                                {
                                    "type": "log",
                                    "timestamp": datetime.utcnow().isoformat(),
                                    "message": "No runnable nodes found in workflow graph; using default workflow.",
                                },
                            )
                            tasks_override = None
                    except Exception as exc:
                        _enqueue(
                            "log",
                            {
                                "type": "log",
                                "timestamp": datetime.utcnow().isoformat(),
                                "message": f"Failed to build workflow from graph: {exc}",
                            },
                        )
                        tasks_override = None

                results = await orchestrator.run_eda(
                    table_asset=table_asset,
                    user_intent=user_intent,
                    workflow_type=resolved_workflow_type,
                    tasks_override=tasks_override,
                )
                result_container["result"] = results
            except asyncio.CancelledError:
                result_container["error"] = "Workflow cancelled by client"
                raise
            except Exception as exc:
                result_container["error"] = str(exc)
            finally:
                done_event.set()

        workflow_task = asyncio.create_task(run_workflow())
        poll_task = asyncio.create_task(poll_workflow_status())
        def _consume_task_result(task: asyncio.Task) -> None:
            try:
                task.result()
            except asyncio.CancelledError:
                return
            except Exception:
                return
        workflow_task.add_done_callback(_consume_task_result)
        client_disconnected = False

        try:
            while True:
                if await request.is_disconnected():
                    client_disconnected = True
                    result_container["error"] = "Workflow cancelled by client"
                    workflow_id = workflow_id_holder.get("id")
                    if workflow_id:
                        await asyncio.to_thread(
                            workflow_tool,
                            action="cancel",
                            workflow_id=workflow_id,
                        )
                    if not workflow_task.done():
                        workflow_task.cancel()
                    break
                if done_event.is_set() and queue.empty():
                    break
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
                    continue
                yield _sse(item["event"], item["payload"])

            if client_disconnected:
                return

            if not workflow_task.done():
                try:
                    await workflow_task
                except asyncio.CancelledError:
                    pass
            if not poll_task.done():
                poll_task.cancel()
                try:
                    await poll_task
                except asyncio.CancelledError:
                    pass

            if "error" in result_container:
                yield _sse(
                    "workflow-error",
                    {
                        "type": "error",
                        "timestamp": datetime.utcnow().isoformat(),
                        "message": result_container["error"],
                    },
                )
                return

            results = result_container.get("result", {})
            if not isinstance(results, dict):
                results = {}
            payload_data = {
                "success": True,
                **results,
            }
            if results.get("workflow_state") == "cancelled":
                payload_data["success"] = False
                payload_data["error"] = "Workflow cancelled"
            summary = payload_data.get("summary", {})
            tasks_completed = summary.get("tasks_completed", 0)
            tasks_total = summary.get("tasks_total", 0)

            if tasks_total > 0:
                progress = int((tasks_completed / tasks_total) * 100)
                yield _sse(
                    "progress",
                    {
                        "type": "progress",
                        "timestamp": datetime.utcnow().isoformat(),
                        "message": f"Progress: {progress}%",
                        "data": {
                            "progress": progress,
                            "tasks_completed": tasks_completed,
                            "tasks_total": tasks_total,
                        },
                    },
                )

                yield _sse(
                    "complete",
                    {
                        "type": "complete",
                        "timestamp": datetime.utcnow().isoformat(),
                        "message": "Workflow completed successfully",
                        "data": payload_data,
                    },
                )
        finally:
            if not poll_task.done():
                poll_task.cancel()
                try:
                    await poll_task
                except asyncio.CancelledError:
                    pass
            for logger in stream_loggers:
                logger.removeHandler(log_handler)
            await db.close()

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        },
    )


@router.post("/workflow/{workflow_id}/cancel", response_model=WorkflowCancelResponse)
async def cancel_workflow(
    workflow_id: str,
    db: AsyncSession = Depends(get_async_db_session),
):
    """Cancel a running workflow by ID."""
    try:
        result = await asyncio.to_thread(
            workflow_tool,
            action="cancel",
            workflow_id=workflow_id,
        )
        status = result.get("status", "error")
        error_message = None
        if status != "success":
            error_message = result.get("content", [{"text": "Failed to cancel"}])[0].get("text")

        try:
            from ...services.eda_workflow_persistence import EDAWorkflowPersistenceService

            persistence = EDAWorkflowPersistenceService(db)
            await persistence.update_execution(
                workflow_id=workflow_id,
                status="cancelled" if status == "success" else "failed",
            )
        except Exception:
            pass

        return WorkflowCancelResponse(
            success=status == "success",
            workflow_id=workflow_id,
            status=status,
            error=error_message,
        )
    except Exception as exc:
        return WorkflowCancelResponse(
            success=False,
            workflow_id=workflow_id,
            status="error",
            error=str(exc),
        )


@router.get("/workflows", response_model=WorkflowListResponse)
async def list_workflows():
    """List all available EDA workflows with descriptions.

    Returns information about each workflow type including:
    - Name and description
    - When to use it
    - What artifacts it produces
    """
    workflows = [
        {
            "type": "EDA_OVERVIEW",
            "name": "Comprehensive Overview",
            "description": "General-purpose EDA with profiling, insights, charts, and documentation",
            "use_when": "Default workflow for comprehensive table analysis",
            "artifacts": ["profile", "insights", "charts", "documentation"],
            "steps": [
                "Profile table (schema, stats, samples)",
                "Generate AI-powered insights",
                "Create visualization specifications",
                "Generate comprehensive documentation",
            ],
        },
        {
            "type": "EDA_TIME_SERIES",
            "name": "Time Series Analysis",
            "description": "Focused on temporal patterns, trends, and seasonality",
            "use_when": "Table has date/timestamp columns or user wants trend analysis",
            "artifacts": ["profile", "charts", "insights", "documentation"],
            "steps": [
                "Profile table with time-series focus",
                "Generate time-series visualizations",
                "Analyze temporal patterns and trends",
                "Generate time-series documentation",
            ],
        },
        {
            "type": "EDA_DATA_QUALITY",
            "name": "Data Quality Check",
            "description": "Focused on validation, completeness, and quality issues",
            "use_when": "User wants to check data quality or validate data",
            "artifacts": ["profile", "insights", "documentation"],
            "steps": [
                "Profile table for quality metrics",
                "Identify quality issues (nulls, duplicates, outliers)",
                "Generate quality report with recommendations",
            ],
        },
    ]

    return WorkflowListResponse(workflows=workflows)


@router.get("/table-assets", response_model=TableAssetListResponse)
async def list_table_assets(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_async_db_session),
):
    """List all table assets available for EDA.

    Returns table assets from PostgreSQL that can be analyzed.
    """
    try:
        # Query table assets
        result = await db.execute(
            select(TableAsset)
            .where(TableAsset.is_deleted == False)
            .order_by(TableAsset.created_at.desc())
            .offset(skip)
            .limit(limit)
        )
        table_assets = result.scalars().all()

        # Count total
        count_result = await db.execute(
            select(TableAsset).where(TableAsset.is_deleted == False)
        )
        total = len(count_result.scalars().all())

        # Format response
        assets_list = [
            {
                "id": asset.id,
                "name": asset.name,
                "source_sql": asset.source_sql[:100] + "..."
                if len(asset.source_sql) > 100
                else asset.source_sql,
                "database": asset.database,
                "schema": asset.schema,
                "tags": asset.tags or [],
                "owner": asset.owner,
                "ai_summary": asset.ai_summary,
                "use_cases": asset.use_cases or [],
                "created_at": asset.created_at.isoformat() if asset.created_at else None,
            }
            for asset in table_assets
        ]

        return TableAssetListResponse(
            table_assets=assets_list,
            total=total,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/table-assets/{table_asset_id}")
async def get_table_asset(
    table_asset_id: int,
    db: AsyncSession = Depends(get_async_db_session),
):
    """Get details of a specific table asset."""
    try:
        result = await db.execute(
            select(TableAsset).where(
                TableAsset.id == table_asset_id,
                TableAsset.is_deleted == False,
            )
        )
        table_asset = result.scalar_one_or_none()

        if not table_asset:
            raise HTTPException(
                status_code=404,
                detail=f"Table asset with ID {table_asset_id} not found",
            )

        return {
            "id": table_asset.id,
            "name": table_asset.name,
            "source_sql": table_asset.source_sql,
            "database": table_asset.database,
            "schema": table_asset.schema,
            "tags": table_asset.tags or [],
            "owner": table_asset.owner,
            "ai_summary": table_asset.ai_summary,
            "use_cases": table_asset.use_cases or [],
            "created_at": table_asset.created_at.isoformat() if table_asset.created_at else None,
            "updated_at": table_asset.updated_at.isoformat() if table_asset.updated_at else None,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/quick-profile/{table_asset_id}")
async def quick_profile(
    table_asset_id: int,
    db: AsyncSession = Depends(get_async_db_session),
    sf_service: SnowflakeService = Depends(get_snowflake_service),
):
    """Run a quick profile on a table asset (just Layer A: SQL facts).

    This is faster than full EDA and useful for quick checks.
    """
    try:
        # Fetch table asset
        result = await db.execute(
            select(TableAsset).where(
                TableAsset.id == table_asset_id,
                TableAsset.is_deleted == False,
            )
        )
        table_asset = result.scalar_one_or_none()

        if not table_asset:
            raise HTTPException(
                status_code=404,
                detail=f"Table asset with ID {table_asset_id} not found",
            )

        # Create profiler and run quick profile
        from ...orchestration.eda_agents import SnowflakeProfiler

        profiler = SnowflakeProfiler(sf_service)
        profile = await profiler.get_table_profile(
            table_ref=table_asset.source_sql,
            sample_size=50,
        )

        return {
            "success": True,
            "table_asset_id": table_asset_id,
            "table_name": table_asset.name,
            "profile": profile,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Health Check
# ============================================================================


@router.get("/health")
async def eda_health_check():
    """Check if the EDA orchestration system is healthy."""
    return {
        "status": "healthy",
        "workflows_available": ["EDA_OVERVIEW", "EDA_TIME_SERIES", "EDA_DATA_QUALITY"],
        "agents": [
            "TableProfilerAgent",
            "InsightAgent",
            "ChartGeneratorAgent",
            "AnnotationDocAgent",
        ],
        "version": "1.0.0",
    }


# ============================================================================
# Workflow History & Management
# ============================================================================


@router.get("/history/{table_asset_id}")
async def get_workflow_history(
    table_asset_id: int,
    limit: int = 10,
    db: AsyncSession = Depends(get_async_db_session),
):
    """Get workflow execution history for a table asset.

    Returns recent workflow executions with their status, results, and metadata.
    Useful for displaying analysis history and caching results.
    """
    try:
        from ...services.eda_workflow_persistence import EDAWorkflowPersistenceService

        persistence = EDAWorkflowPersistenceService(db)
        executions = await persistence.get_executions_for_table(table_asset_id, limit=limit)

        return {
            "table_asset_id": table_asset_id,
            "total": len(executions),
            "executions": [
                {
                    "id": e.id,
                    "workflow_id": e.workflow_id,
                    "workflow_type": e.workflow_type,
                    "status": e.status,
                    "progress": e.progress,
                    "tasks_total": e.tasks_total,
                    "tasks_completed": e.tasks_completed,
                    "tasks_failed": e.tasks_failed,
                    "data_structure_type": e.data_structure_type,
                    "started_at": e.started_at.isoformat() if e.started_at else None,
                    "completed_at": e.completed_at.isoformat() if e.completed_at else None,
                    "duration_seconds": e.duration_seconds,
                    "user_intent": e.user_intent,
                    "error_message": e.error_message,
                }
                for e in executions
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workflow/{workflow_id}")
async def get_workflow_details(
    workflow_id: str,
    db: AsyncSession = Depends(get_async_db_session),
):
    """Get detailed information about a specific workflow execution.

    Returns complete workflow data including artifacts, summary, and type detection results.
    """
    try:
        from ...services.eda_workflow_persistence import EDAWorkflowPersistenceService

        persistence = EDAWorkflowPersistenceService(db)
        execution = await persistence.get_execution(workflow_id)

        if not execution:
            raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")

        return {
            "id": execution.id,
            "workflow_id": execution.workflow_id,
            "workflow_type": execution.workflow_type,
            "table_asset_id": execution.table_asset_id,
            "status": execution.status,
            "progress": execution.progress,
            "tasks_total": execution.tasks_total,
            "tasks_completed": execution.tasks_completed,
            "tasks_failed": execution.tasks_failed,
            "started_at": execution.started_at.isoformat() if execution.started_at else None,
            "completed_at": execution.completed_at.isoformat() if execution.completed_at else None,
            "duration_seconds": execution.duration_seconds,
            "artifacts": execution.artifacts,
            "summary": execution.summary,
            "data_structure_type": execution.data_structure_type,
            "column_type_inferences": execution.column_type_inferences,
            "user_intent": execution.user_intent,
            "error_message": execution.error_message,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/{table_asset_id}")
async def get_workflow_stats(
    table_asset_id: int,
    db: AsyncSession = Depends(get_async_db_session),
):
    """Get workflow execution statistics for a table asset.

    Returns aggregated statistics like success rate, average duration, etc.
    """
    try:
        from ...services.eda_workflow_persistence import EDAWorkflowPersistenceService
        from sqlalchemy import func
        from ...models.eda_workflow import EDAWorkflowExecution

        # Get all executions for this table
        result = await db.execute(
            select(
                func.count(EDAWorkflowExecution.id).label("total_executions"),
                func.sum(
                    func.cast(EDAWorkflowExecution.status == "completed", Integer)
                ).label("successful_executions"),
                func.avg(EDAWorkflowExecution.duration_seconds).label("avg_duration"),
                func.max(EDAWorkflowExecution.completed_at).label("last_execution"),
            ).where(
                EDAWorkflowExecution.table_asset_id == table_asset_id,
                EDAWorkflowExecution.is_deleted == False,
            )
        )

        stats = result.first()

        total = stats.total_executions or 0
        successful = stats.successful_executions or 0
        success_rate = (successful / total * 100) if total > 0 else 0

        return {
            "table_asset_id": table_asset_id,
            "total_executions": total,
            "successful_executions": successful,
            "failed_executions": total - successful,
            "success_rate": round(success_rate, 2),
            "avg_duration_seconds": round(stats.avg_duration, 2) if stats.avg_duration else None,
            "last_execution": stats.last_execution.isoformat() if stats.last_execution else None,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
