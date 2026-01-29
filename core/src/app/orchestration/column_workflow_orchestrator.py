"""Column workflow orchestrator."""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import PendingRollbackError, SQLAlchemyError
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.ext.asyncio import AsyncSession

from ..core.config import settings
from ..models.column_metadata import ColumnMetadata
from ..services.modular_ai_sql_service import ModularAISQLService
from ..services.snowflake_service import SnowflakeService
from ..services.eda_workflow_persistence import EDAWorkflowPersistenceService
from .column_workflow_tools import ColumnWorkflowTools

class ColumnWorkflowOrchestrator:
    """Runs an autonomous Strands agent for a single column."""

    def __init__(
        self,
        snowflake_service: SnowflakeService,
        ai_sql_service: ModularAISQLService,
        db: AsyncSession,
    ) -> None:
        self.sf = snowflake_service
        self.ai_sql = ai_sql_service
        self.db = db

        openai_key = (
            settings.OPENAI_API_KEY.get_secret_value()
            if settings.OPENAI_API_KEY
            else os.getenv("OPENAI_API_KEY")
        )
        if openai_key:
            os.environ.setdefault("OPENAI_API_KEY", openai_key)

        model_id = (
            settings.STRANDS_MODEL_ID
            or settings.OPENAI_MODEL_ID
            or os.getenv("STRANDS_MODEL_ID")
            or os.getenv("OPENAI_MODEL_ID")
            or "gpt-4o-mini"
        )
        provider = settings.STRANDS_MODEL_PROVIDER or os.getenv("STRANDS_MODEL_PROVIDER") or "openai"
        os.environ["STRANDS_MODEL_PROVIDER"] = provider
        os.environ["STRANDS_MODEL_ID"] = model_id
        os.environ["STRANDS_PROVIDER"] = provider

    async def run_column_workflow(
        self,
        table_asset_id: int,
        column_name: str,
        focus: str | None = None,
    ) -> dict[str, Any]:
        column_meta = await self._get_column_meta(table_asset_id, column_name)
        if not column_meta:
            raise ValueError("Column metadata not found")

        workflow_id = f"column_{table_asset_id}_{column_name}_{uuid.uuid4().hex[:8]}"
        tools = ColumnWorkflowTools(self.sf, self.ai_sql, self.db)
        workflow_state = "completed"
        status: str | dict[str, Any] = "completed"
        workflow_logs: list[dict[str, Any]] = []
        workflow_tool_calls: list[dict[str, Any]] = []
        fallback_used = False
        apply_fallback_used = False
        allow_fallback = bool((column_meta.overrides or {}).get("allow_preset_fallback"))
        persistence = EDAWorkflowPersistenceService(self.db)
        execution = None
        try:
            execution = await persistence.create_execution(
                workflow_id=workflow_id,
                workflow_type="COLUMN_WORKFLOW",
                table_asset_id=table_asset_id,
                user_intent=f"{column_name}:{focus or 'auto'}",
                user_id=None,
                tasks_total=0,
            )
        except Exception as exc:
            workflow_logs.append(
                {
                    "type": "warning",
                    "timestamp": datetime.utcnow().isoformat(),
                    "message": f"Workflow persistence create failed: {exc}",
                }
            )

        try:
            result = await tools.run_column_agent(table_asset_id, column_name, focus=focus)
            workflow_logs = result.get("logs", [])
            workflow_tool_calls = result.get("tool_calls", [])
            if result.get("error"):
                workflow_state = "error"
                status = {"state": "error", "error": result.get("error")}
        except Exception as exc:
            workflow_state = "error"
            status = {"state": "error", "error": str(exc)}
            workflow_logs.append(
                {
                    "type": "error",
                    "timestamp": datetime.utcnow().isoformat(),
                    "message": f"Agent run failed: {exc}",
                }
            )
        finally:
            updated_meta = await self._get_column_meta(table_asset_id, column_name)
            if focus == "repairs" and updated_meta:
                overrides = updated_meta.overrides or {}
                analysis = (updated_meta.metadata_payload or {}).get("analysis", {})
                approved = bool(overrides.get("data_fix_approved"))
                has_plan = bool(analysis.get("repair_plan"))
                has_results = bool(analysis.get("repair_results"))
                if approved and has_plan and not has_results:
                    try:
                        tools = ColumnWorkflowTools(self.sf, self.ai_sql, self.db)
                        await tools.apply_data_repairs(table_asset_id, column_name)
                        apply_fallback_used = True
                        workflow_logs.append(
                            {
                                "type": "warning",
                                "timestamp": datetime.utcnow().isoformat(),
                                "message": "Repair apply fallback executed (apply_data_repairs)",
                                "data": {
                                    "column": column_name,
                                    "table_asset_id": table_asset_id,
                                },
                            }
                        )
                        updated_meta = await self._get_column_meta(table_asset_id, column_name)
                    except Exception as exc:
                        workflow_state = "error"
                        status = {"state": "error", "error": f"repair_apply_failed: {exc}"}
            if allow_fallback and updated_meta and not self._analysis_ready(updated_meta):
                fallback_used = True
                workflow_logs.append(
                    {
                        "type": "warning",
                        "timestamp": datetime.utcnow().isoformat(),
                        "message": "Preset workflow fallback executed (_run_direct)",
                        "data": {
                            "column": column_name,
                            "table_asset_id": table_asset_id,
                            "semantic_type": updated_meta.semantic_type,
                        },
                    }
                )
                await self._run_direct(ColumnWorkflowTools(self.sf, self.ai_sql, self.db), updated_meta)

            try:
                await self._record_workflow_status(
                    column_meta=updated_meta or column_meta,
                    workflow_id=workflow_id,
                    status=status,
                    workflow_state=workflow_state,
                    fallback_used=fallback_used,
                    workflow_data=None,
                    workflow_logs=workflow_logs,
                    workflow_tool_calls=workflow_tool_calls,
                )
            except (PendingRollbackError, SQLAlchemyError) as exc:
                await self.db.rollback()
                workflow_state = "error"
                status = {"state": "error", "error": f"workflow_persist_failed: {exc}"}
            try:
                if execution:
                    summary = {
                        "workflow_state": workflow_state,
                        "fallback_used": fallback_used,
                        "apply_fallback_used": apply_fallback_used,
                        "log_entries": len(workflow_logs),
                        "tool_calls": len(workflow_tool_calls),
                    }
                    if workflow_state == "error":
                        await persistence.fail_execution(
                            workflow_id=workflow_id,
                            error_message=str(status.get("error") if isinstance(status, dict) else status),
                        )
                    else:
                        await persistence.update_execution(
                            workflow_id=workflow_id,
                            status="completed",
                            progress=100,
                            tasks_completed=summary.get("tool_calls", 0),
                            tasks_failed=0,
                            summary=summary,
                            artifacts={
                                "logs": workflow_logs,
                                "tool_calls": workflow_tool_calls,
                            },
                        )

                    execution_id = execution.id
                    for entry in workflow_logs:
                        await persistence.log_event(
                            workflow_execution_id=execution_id,
                            log_level="ERROR" if entry.get("type") == "error" else "INFO",
                            log_type=str(entry.get("type", "log")),
                            message=str(entry.get("message", "")),
                            details=entry.get("data"),
                        )
                    for call in workflow_tool_calls:
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
            except Exception as exc:
                workflow_logs.append(
                    {
                        "type": "warning",
                        "timestamp": datetime.utcnow().isoformat(),
                        "message": f"Workflow persistence update failed: {exc}",
                    }
                )

        return {
            "workflow_id": workflow_id,
            "status": status,
            "workflow_state": workflow_state,
            "column": column_name,
            "semantic_type": column_meta.semantic_type,
            "fallback_used": fallback_used,
            "apply_fallback_used": apply_fallback_used,
            "workflow_logs": workflow_logs,
            "workflow_tool_calls": workflow_tool_calls,
        }

    async def _get_column_meta(self, table_asset_id: int, column_name: str) -> ColumnMetadata | None:
        result = await self.db.execute(
            select(ColumnMetadata).where(
                ColumnMetadata.table_asset_id == table_asset_id,
                ColumnMetadata.column_name == column_name,
            )
        )
        return result.scalar_one_or_none()

    async def _record_workflow_status(
        self,
        column_meta: ColumnMetadata,
        workflow_id: str,
        status: Any,
        workflow_state: str | None,
        fallback_used: bool,
        workflow_data: dict[str, Any] | None = None,
        workflow_logs: list[dict[str, Any]] | None = None,
        workflow_tool_calls: list[dict[str, Any]] | None = None,
    ) -> None:
        metadata = dict(column_meta.metadata_payload or {})
        workflow_meta = dict(metadata.get("workflow", {}))
        task_results = None
        task_list = None
        if isinstance(workflow_data, dict):
            task_results = workflow_data.get("task_results")
            task_list = workflow_data.get("tasks")
        workflow_meta.update(
            {
                "workflow_id": workflow_id,
                "status": status,
                "workflow_state": workflow_state,
                "fallback_used": fallback_used,
                "last_run_at": datetime.now(timezone.utc).isoformat(),
                "task_results": task_results,
                "tasks": task_list,
                "logs": workflow_logs,
                "tool_calls": workflow_tool_calls,
            }
        )
        metadata["workflow"] = workflow_meta
        column_meta.metadata_payload = metadata
        flag_modified(column_meta, "metadata_payload")
        column_meta.last_updated = datetime.now(timezone.utc)
        await self.db.commit()
        await self.db.refresh(column_meta)

    def _analysis_ready(self, column_meta: ColumnMetadata) -> bool:
        metadata = column_meta.metadata_payload or {}
        analysis = metadata.get("analysis", {})
        semantic_type = column_meta.semantic_type

        if analysis.get("agent_summary"):
            return True

        if semantic_type in {"numeric", "temporal"}:
            return bool(
                analysis.get("distribution")
                or analysis.get("correlations")
                or analysis.get("periodicity")
                or analysis.get("visuals")
                or analysis.get("insights")
            )
        if semantic_type == "categorical":
            return bool(
                analysis.get("categories")
                or analysis.get("conflicts")
                or analysis.get("visuals")
                or analysis.get("insights")
            )
        if semantic_type == "text":
            return bool(analysis.get("summary") or analysis.get("row_level_output"))
        if semantic_type == "image":
            return bool(analysis.get("image_descriptions_column"))
        return bool(analysis.get("basic_stats"))

    async def _run_direct(self, tools: ColumnWorkflowTools, column_meta: ColumnMetadata) -> None:
        table_asset_id = column_meta.table_asset_id
        column_name = column_meta.column_name
        semantic_type = column_meta.semantic_type
        try:
            ctx = await tools._load_context(table_asset_id, column_name)
            await tools._ensure_analysis_snapshot(ctx, column_name)
        except Exception:
            # Snapshot is best-effort for preset fallback runs
            pass

        if semantic_type in {"numeric", "temporal"}:
            await tools.analyze_numeric_distribution(table_asset_id, column_name)
            await tools.analyze_numeric_correlations(table_asset_id, column_name)
            await tools.analyze_numeric_periodicity(table_asset_id, column_name)
            await tools.scan_nulls(table_asset_id, column_name)
            await tools.generate_numeric_visuals(table_asset_id, column_name)
            await tools.generate_numeric_insights(table_asset_id, column_name)
            await tools.plan_data_repairs(table_asset_id, column_name)
            return

        if semantic_type == "categorical":
            await tools.analyze_categorical_groups(table_asset_id, column_name)
            await tools.scan_nulls(table_asset_id, column_name)
            await tools.scan_conflicts(table_asset_id, column_name)
            await tools.generate_categorical_visuals(table_asset_id, column_name)
            await tools.generate_categorical_insights(table_asset_id, column_name)
            await tools.plan_data_repairs(table_asset_id, column_name)
            return

        if semantic_type == "text":
            await tools.scan_nulls(table_asset_id, column_name)
            await tools.summarize_text_column(table_asset_id, column_name)
            if (column_meta.overrides or {}).get("row_level_instruction"):
                await tools.row_level_extract_text(table_asset_id, column_name)
            await tools.plan_data_repairs(table_asset_id, column_name)
            return

        if semantic_type == "image":
            await tools.describe_image_column(table_asset_id, column_name)
            return

        await tools.basic_column_stats(table_asset_id, column_name)
