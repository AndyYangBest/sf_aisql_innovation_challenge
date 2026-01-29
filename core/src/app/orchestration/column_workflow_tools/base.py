"""Shared helpers for column workflow tools."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import PendingRollbackError, SQLAlchemyError
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.ext.asyncio import AsyncSession
from strands import Agent
from strands.hooks import HookProvider
from strands.models.openai import OpenAIModel

from ...core.config import settings
from ...models.column_metadata import ColumnMetadata
from ...models.table_asset_metadata import TableAssetMetadata
from ...services.modular_ai_sql_service import ModularAISQLService
from ...services.snowflake_service import SnowflakeService
from ...services.column_metadata_service import ColumnMetadataService
from ..column_workflow_context import ColumnContext
from ..column_workflow_logging import ColumnWorkflowLogBuffer, ColumnWorkflowLogHook

logger = logging.getLogger(__name__)

class ColumnWorkflowToolsBase:
    """Shared helpers for column workflow tools."""

    def __init__(
        self,
        snowflake_service: SnowflakeService,
        ai_sql_service: ModularAISQLService,
        db: AsyncSession,
    ) -> None:
        self.sf = snowflake_service
        self.ai_sql = ai_sql_service
        self.db = db
        self.model_id = settings.SNOWFLAKE_CORTEX_MODEL or "mistral-large2"
        self.image_model_id = settings.SNOWFLAKE_CORTEX_IMAGE_MODEL or "pixtral-large"
        self._log_buffer: ColumnWorkflowLogBuffer | None = None
        self._metadata_lock = asyncio.Lock()

    def _set_log_buffer(self, buffer: ColumnWorkflowLogBuffer | None) -> None:
        self._log_buffer = buffer

    def _get_log_hooks(self) -> list[HookProvider]:
        if self._log_buffer is None:
            return []
        return [ColumnWorkflowLogHook(self._log_buffer)]

    def _select_fallback_tool(self, ctx: ColumnContext) -> Any:
        semantic_type = ctx.column_meta.semantic_type
        if semantic_type in {"numeric", "temporal"}:
            return self.analyze_numeric_distribution
        if semantic_type == "categorical":
            return self.analyze_categorical_groups
        if semantic_type == "text":
            return self.scan_nulls
        if semantic_type == "image":
            return self.describe_image_column
        return self.basic_column_stats

    async def _run_fallback_tool(self, ctx: ColumnContext, reason: str) -> None:
        if self._log_buffer is None:
            return
        fallback_tool = self._select_fallback_tool(ctx)
        tool_name = getattr(fallback_tool, "__name__", "fallback_tool")
        tool_use_id = f"fallback_{uuid.uuid4().hex[:8]}"
        tool_input = {
            "table_asset_id": ctx.table_asset_id,
            "column_name": ctx.column_name,
            "reason": reason,
        }
        self._log_buffer.add_tool_call(tool_use_id, tool_name, "Fallback", tool_input)
        self._log_buffer.add_entry(
            "workflow_log",
            f"Fallback tool invoked: {tool_name}",
            tool_input,
        )
        try:
            await fallback_tool(ctx.table_asset_id, ctx.column_name)
        except Exception as exc:
            self._log_buffer.update_tool_call(tool_use_id, "error", error=str(exc))
            self._log_buffer.add_entry(
                "error",
                f"Fallback tool failed: {exc}",
                {"tool_name": tool_name},
            )
            return
        self._log_buffer.update_tool_call(tool_use_id, "success")
        self._log_buffer.add_entry(
            "workflow_log",
            f"Fallback tool completed: {tool_name}",
            {"tool_name": tool_name},
        )

    async def _run_sub_agent(
        self,
        name: str,
        system_prompt: str,
        tools: list[Any],
        prompt: str,
        table_asset_id: int | None = None,
        column_name: str | None = None,
    ) -> str:
        model = self._build_strands_model()
        agent = Agent(
            name=name,
            system_prompt=system_prompt,
            tools=tools,
            hooks=self._get_log_hooks(),
            model=model,
        )
        invocation_state = {}
        if table_asset_id is not None:
            invocation_state["table_asset_id"] = table_asset_id
        if column_name:
            invocation_state["column_name"] = column_name
        response = await agent.invoke_async(prompt, invocation_state=invocation_state or None)
        return getattr(response, "content", None) or str(response)

    async def run_selected_tools(
        self,
        table_asset_id: int,
        column_name: str,
        tool_calls: list[dict[str, Any]],
        focus: str | None = None,
    ) -> dict[str, Any]:
        """Run a specific set of tools for a column, honoring dependencies."""
        ctx = await self._load_context(table_asset_id, column_name)
        log_buffer = ColumnWorkflowLogBuffer()
        log_buffer.set_default_context(table_asset_id, column_name)
        self._set_log_buffer(log_buffer)

        sync_stop = asyncio.Event()
        sync_task = asyncio.create_task(self._sync_logs_loop(ctx, log_buffer, sync_stop))

        results: list[dict[str, Any]] = []
        errors: list[str] = []
        workflow_state = "completed"

        stage_map = {
            "scan_nulls": 0,
            "scan_conflicts": 0,
            "analyze_numeric_distribution": 0,
            "analyze_numeric_correlations": 0,
            "analyze_numeric_periodicity": 0,
            "analyze_categorical_groups": 0,
            "generate_numeric_visuals": 0,
            "generate_categorical_visuals": 0,
            "generate_chart_candidates": 0,
            "generate_numeric_insights": 0,
            "generate_categorical_insights": 0,
            "generate_column_summary": 0,
            "summarize_text_column": 0,
            "row_level_extract_text": 1,
            "describe_image_column": 0,
            "basic_column_stats": 0,
            "plan_data_repairs": 2,
            "require_user_approval": 3,
            "apply_data_repairs": 4,
            "apply_data_repairs_to_fixing_table": 4,
        }

        def resolve_tool(name: str) -> Any | None:
            tool = getattr(self, name, None)
            if callable(tool):
                return tool
            return None

        async def run_tool(
            tool_name: str,
            tool_input: dict[str, Any],
            tool_use_id: str,
        ) -> None:
            tool = resolve_tool(tool_name)
            if not tool:
                log_buffer.update_tool_call(tool_use_id, "error", error="tool_not_found")
                errors.append(f"tool_not_found:{tool_name}")
                return
            tool_spec = getattr(tool, "tool_spec", None) or getattr(tool, "spec", None)
            schema = tool_spec.get("inputSchema", {}).get("json", {}) if isinstance(tool_spec, dict) else {}
            properties = schema.get("properties") if isinstance(schema, dict) else {}
            allowed = set(properties.keys())
            filtered_input = {
                key: value for key, value in (tool_input or {}).items() if key in allowed
            }
            if "table_asset_id" in allowed:
                filtered_input["table_asset_id"] = table_asset_id
            if "column_name" in allowed:
                filtered_input["column_name"] = column_name
            try:
                result = await tool(**filtered_input)
                results.append(
                    {
                        "tool_name": tool_name,
                        "status": "success",
                        "result": result,
                    }
                )
                preview = str(result)
                if len(preview) > 240:
                    preview = preview[:240].rstrip() + "..."
                log_buffer.update_tool_call(tool_use_id, "success", output_preview=preview)
            except Exception as exc:  # pragma: no cover - defensive
                errors.append(f"{tool_name}:{exc}")
                log_buffer.update_tool_call(
                    tool_use_id,
                    "error",
                    error=str(exc),
                    output_preview=str(exc),
                )

        try:
            log_buffer.add_entry(
                "workflow_log",
                "Running selected workflow nodes",
                {"column": column_name, "tool_calls": len(tool_calls), "focus": focus},
            )
            staged_calls: dict[int, list[dict[str, Any]]] = {}
            for call in tool_calls:
                tool_name = str(call.get("tool_name") or call.get("name") or "")
                if not tool_name:
                    continue
                stage = stage_map.get(tool_name, 1)
                staged_calls.setdefault(stage, []).append(call)

            for stage in sorted(staged_calls.keys()):
                tasks = []
                for call in staged_calls[stage]:
                    tool_name = str(call.get("tool_name") or call.get("name") or "")
                    tool_input = call.get("input") or {}
                    tool_use_id = f"manual_{uuid.uuid4().hex[:8]}"
                    log_buffer.add_tool_call(tool_use_id, tool_name, "SelectionRunner", tool_input)
                    tasks.append(asyncio.create_task(run_tool(tool_name, tool_input, tool_use_id)))
                if tasks:
                    await asyncio.gather(*tasks)
        except Exception as exc:  # pragma: no cover - defensive
            workflow_state = "error"
            errors.append(str(exc))
        finally:
            sync_stop.set()
            if sync_task:
                await sync_task
            await self._sync_workflow_snapshot(ctx, log_buffer, workflow_state=workflow_state)
            self._set_log_buffer(None)

        summary = {
            "column": column_name,
            "tool_calls": [call.get("tool_name") for call in tool_calls if call.get("tool_name")],
            "errors": errors,
        }
        try:
            analysis = {"summary": json.dumps(summary, ensure_ascii=True)}
            await self._update_column_analysis(ctx, analysis)
        except Exception:
            pass

        return {
            "workflow_id": f"selected_{uuid.uuid4().hex[:8]}",
            "status": {"state": workflow_state, "errors": errors},
            "workflow_state": workflow_state,
            "column": column_name,
            "semantic_type": ctx.column_meta.semantic_type,
            "workflow_logs": log_buffer.entries,
            "workflow_tool_calls": log_buffer.tool_calls,
            "results": results,
        }

    async def _sync_workflow_snapshot(
        self,
        ctx: ColumnContext,
        log_buffer: ColumnWorkflowLogBuffer,
        workflow_state: str = "running",
    ) -> None:
        if log_buffer.sync_failed or not log_buffer.has_updates():
            return
        async with self._metadata_lock:
            try:
                await self.db.refresh(ctx.column_meta)
                metadata = dict(ctx.column_meta.metadata_payload or {})
                workflow_meta = dict(metadata.get("workflow", {}))
                workflow_meta.update(
                    {
                        "workflow_state": workflow_state,
                        "status": {"state": workflow_state},
                        "last_run_at": datetime.now(timezone.utc).isoformat(),
                        "logs": log_buffer.entries,
                        "tool_calls": log_buffer.tool_calls,
                    }
                )
                metadata["workflow"] = workflow_meta
                ctx.column_meta.metadata_payload = metadata
                flag_modified(ctx.column_meta, "metadata_payload")
                ctx.column_meta.last_updated = datetime.now(timezone.utc)
                await self.db.commit()
                log_buffer.mark_synced()
            except (PendingRollbackError, SQLAlchemyError) as exc:
                await self.db.rollback()
                log_buffer.sync_failed = True
                log_buffer.sync_error = str(exc)
                log_buffer.add_entry(
                    "error",
                    "Failed to persist workflow logs",
                    {"error": str(exc)},
                )

    async def _sync_logs_loop(
        self,
        ctx: ColumnContext,
        log_buffer: ColumnWorkflowLogBuffer,
        stop_event: asyncio.Event,
        interval: float = 1.2,
    ) -> None:
        try:
            while not stop_event.is_set():
                await asyncio.sleep(interval)
                await self._sync_workflow_snapshot(ctx, log_buffer, workflow_state="running")
                if log_buffer.sync_failed:
                    break
        finally:
            await self._sync_workflow_snapshot(ctx, log_buffer, workflow_state="running")

    async def _load_context(self, table_asset_id: int, column_name: str) -> ColumnContext:
        table_meta_result = await self.db.execute(
            select(TableAssetMetadata).where(TableAssetMetadata.table_asset_id == table_asset_id)
        )
        table_meta = table_meta_result.scalar_one_or_none()
        column_meta: ColumnMetadata | None = None
        if not table_meta:
            service = ColumnMetadataService(self.db, self.sf, self.ai_sql)
            try:
                table_meta, columns = await service.initialize_metadata(table_asset_id, force=False)
                column_meta = next(
                    (col for col in columns if col.column_name == column_name),
                    None,
                )
            except ValueError as exc:
                raise ValueError("Table metadata missing; initialize column metadata first.") from exc

        column_result = await self.db.execute(
            select(ColumnMetadata).where(
                ColumnMetadata.table_asset_id == table_asset_id,
                ColumnMetadata.column_name == column_name,
            )
        )
        column_meta = column_meta or column_result.scalar_one_or_none()
        if not column_meta:
            raise ValueError("Column metadata missing; initialize column metadata first.")

        metadata_payload = table_meta.metadata_payload or {}
        base_query = metadata_payload.get("base_query")
        if not base_query:
            raise ValueError("Base query missing from table metadata.")

        analysis_query = metadata_payload.get("analysis_query") or base_query

        table_ref = None
        meta_payload = column_meta.metadata_payload or {}
        if meta_payload.get("table_ref"):
            table_ref = meta_payload.get("table_ref")
        elif metadata_payload.get("table_ref"):
            table_ref = metadata_payload.get("table_ref")

        if table_ref:
            analysis_query = f"SELECT * FROM {table_ref}"
        else:
            analysis_query = self._strip_limit_clause(str(analysis_query))

        time_column = (table_meta.metadata_payload or {}).get("time_column")
        structure_type = table_meta.structure_type

        return ColumnContext(
            table_asset_id=table_asset_id,
            column_name=column_name,
            base_query=base_query,
            analysis_query=analysis_query,
            table_ref=table_ref,
            time_column=time_column,
            structure_type=structure_type,
            column_meta=column_meta,
            table_meta=table_meta,
        )

    async def _update_column_analysis(self, ctx: ColumnContext, analysis_update: dict[str, Any]) -> None:
        async with self._metadata_lock:
            try:
                await self.db.refresh(ctx.column_meta)
                metadata = dict(ctx.column_meta.metadata_payload or {})
                analysis = dict(metadata.get("analysis", {}))
                analysis.update(analysis_update)
                analysis.setdefault("column", ctx.column_name)
                analysis.setdefault("type", ctx.column_meta.semantic_type)
                analysis.setdefault("confidence", ctx.column_meta.confidence)
                analysis.setdefault("provenance", ctx.column_meta.provenance or {})
                metadata["analysis"] = analysis
                ctx.column_meta.metadata_payload = metadata
                flag_modified(ctx.column_meta, "metadata_payload")
                ctx.column_meta.last_updated = datetime.now(timezone.utc)
                await self.db.commit()
                await self.db.refresh(ctx.column_meta)
            except (PendingRollbackError, SQLAlchemyError) as exc:
                await self.db.rollback()
                raise RuntimeError(f"Failed to persist analysis: {exc}") from exc

    def _normalize_chart_type(self, value: Any) -> str:
        raw = str(value or "").lower()
        if "pie" in raw or "donut" in raw:
            return "pie"
        if "line" in raw:
            return "line"
        if "area" in raw:
            return "area"
        if "bar" in raw or "hist" in raw:
            return "bar"
        return "bar"

    def _extract_visual_overrides(self, ctx: ColumnContext) -> list[dict[str, Any]] | None:
        overrides = ctx.column_meta.overrides or {}
        plan: list[dict[str, Any]] = []
        visual_overrides = overrides.get("visual_overrides")
        if isinstance(visual_overrides, list):
            for item in visual_overrides:
                if not isinstance(item, dict):
                    continue
                plan.append(
                    {
                        "chart_type": item.get("chart_type"),
                        "x_column": item.get("x_column"),
                        "y_column": item.get("y_column"),
                    }
                )
        chart_type = overrides.get("visual_chart_type")
        x_column = overrides.get("visual_x_column")
        y_column = overrides.get("visual_y_column")
        if chart_type or x_column or y_column:
            plan.append(
                {
                    "chart_type": chart_type,
                    "x_column": x_column,
                    "y_column": y_column,
                }
            )
        return plan if plan else None

    async def _get_semantic_type_map(self, table_asset_id: int) -> dict[str, str]:
        result = await self.db.execute(
            select(ColumnMetadata.column_name, ColumnMetadata.semantic_type).where(
                ColumnMetadata.table_asset_id == table_asset_id
            )
        )
        rows = result.all()
        type_map: dict[str, str] = {}
        for row in rows:
            if not row or not row[0]:
                continue
            type_map[str(row[0])] = str(row[1] or "unknown")
        return type_map

    async def _build_custom_visuals(
        self,
        ctx: ColumnContext,
        visual_plan: list[dict[str, Any]] | None,
    ) -> tuple[list[dict[str, Any]] | None, list[dict[str, Any]]]:
        if visual_plan is None:
            return None, []
        if not visual_plan:
            return [], []
        type_map = await self._get_semantic_type_map(ctx.table_asset_id)
        visuals: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        for item in visual_plan:
            chart_type = self._normalize_chart_type(item.get("chart_type"))
            x_column = str(item.get("x_column") or "").strip() or ctx.column_name
            y_column_raw = str(item.get("y_column") or "").strip()
            y_column = y_column_raw or ("count" if chart_type in {"bar", "pie"} else ctx.column_name)
            if not x_column:
                errors.append({"step": "visual_overrides", "error": "Missing x_column"})
                continue
            x_semantic = type_map.get(x_column, "unknown")
            y_semantic = type_map.get(y_column, "unknown") if y_column != "count" else "count"
            if x_semantic == "temporal":
                visual = await self._build_time_visual(
                    ctx, chart_type, x_column, y_column, y_semantic
                )
            else:
                visual = await self._build_category_visual(
                    ctx, chart_type, x_column, y_column, y_semantic
                )
            if visual:
                visuals.append(visual)
        return visuals, errors

    async def _build_time_visual(
        self,
        ctx: ColumnContext,
        chart_type: str,
        x_column: str,
        y_column: str,
        y_semantic: str,
    ) -> dict[str, Any] | None:
        x_ident = self._quote_ident(x_column)
        time_expr = self._resolve_temporal_expr(ctx, x_ident)
        time_bucket_expr = f"DATE_TRUNC('day', {time_expr})"

        use_count = y_column == "count" or y_semantic != "numeric"
        y_key = "count" if use_count else "avg_value"
        y_title = "Count" if use_count else f"Average {y_column}"
        agg_expr = "COUNT(*)" if use_count else f"AVG({self._quote_ident(y_column)})"
        overrides = ctx.column_meta.overrides or {}
        limit_override = overrides.get("visual_time_limit") or overrides.get("visual_point_limit")
        if isinstance(limit_override, str) and limit_override.lower() == "all":
            time_limit = None
        elif limit_override in (-1, 0, ""):
            time_limit = None
        elif limit_override is not None:
            time_limit = max(1, self._coerce_int(limit_override, 500))
        else:
            time_limit = 500

        query = f"""
        WITH base AS (
            {ctx.analysis_query}
        )
        SELECT
            TO_VARCHAR({time_bucket_expr}) AS time_bucket,
            {agg_expr} AS {y_key}
        FROM base
        WHERE {time_expr} IS NOT NULL
        {"" if use_count else f"AND {self._quote_ident(y_column)} IS NOT NULL"}
        GROUP BY {time_bucket_expr}
        ORDER BY time_bucket
        {"" if time_limit is None else f"LIMIT {int(time_limit)}"}
        """
        try:
            rows = await self.sf.execute_query(query)
        except Exception as exc:
            logger.warning("Custom time series query failed for %s: %s", x_column, exc)
            return None
        data = [
            {"time_bucket": row.get("TIME_BUCKET"), y_key: row.get(y_key.upper())}
            for row in rows
        ]
        if not data:
            return None

        source_columns = [x_column]
        if not use_count and y_column:
            source_columns.append(y_column)
        return self._build_chart_spec(
            chart_type=chart_type,
            title=f"{y_title} by {x_column}",
            x_key="time_bucket",
            y_key=y_key,
            data=data,
            narrative=[
                "Daily trend based on counts" if use_count else f"Daily trend of average {y_column}",
                f"Grouped by {x_column}",
            ],
            source_columns=source_columns,
            x_title=x_column,
            y_title=y_title,
        )

    async def _build_category_visual(
        self,
        ctx: ColumnContext,
        chart_type: str,
        x_column: str,
        y_column: str,
        y_semantic: str,
    ) -> dict[str, Any] | None:
        x_ident = self._quote_ident(x_column)
        use_count = y_column == "count" or y_semantic != "numeric"
        overrides = ctx.column_meta.overrides or {}
        limit_override = (
            overrides.get("visual_top_n")
            or overrides.get("categorical_visual_top_n")
            or overrides.get("visual_category_limit")
        )
        if isinstance(limit_override, str) and limit_override.lower() == "all":
            top_limit = None
        elif limit_override in (-1, 0, ""):
            top_limit = None
        elif limit_override is not None:
            top_limit = max(1, self._coerce_int(limit_override, 50))
        else:
            top_limit = None

        if use_count:
            total_query = f"""
            WITH base AS (
                {ctx.analysis_query}
            )
            SELECT COUNT(*) AS total_count
            FROM base
            WHERE {x_ident} IS NOT NULL
            """
            top_query = f"""
            WITH base AS (
                {ctx.analysis_query}
            )
            SELECT {x_ident} AS category, COUNT(*) AS count
            FROM base
            WHERE {x_ident} IS NOT NULL
            GROUP BY category
            ORDER BY count DESC
            {"" if top_limit is None else f"LIMIT {int(top_limit)}"}
            """
            try:
                total_rows = await self.sf.execute_query(total_query)
                top_rows_raw = await self.sf.execute_query(top_query)
            except Exception as exc:
                logger.warning("Custom category count query failed for %s: %s", x_column, exc)
                return None
            total_count = self._coerce_int(total_rows[0]["TOTAL_COUNT"]) if total_rows else 0
            top_rows = [
                {"category": row.get("CATEGORY"), "count": self._coerce_int(row.get("COUNT"))}
                for row in top_rows_raw
            ]
            top_sum = sum(row.get("count", 0) for row in top_rows)
            if top_limit is not None and total_count > top_sum:
                top_rows.append({"category": "Other", "count": total_count - top_sum})
            if not top_rows:
                return None
            return self._build_chart_spec(
                chart_type=chart_type,
                title=f"{x_column} distribution",
                x_key="category",
                y_key="count",
                data=top_rows,
                narrative=[
                    "Top categories shown with long-tail grouped as Other" if top_limit is not None else "All categories shown",
                    f"Total non-null rows: {total_count}",
                ],
                source_columns=[x_column],
                x_title=x_column,
                y_title="Count",
            )

        y_ident = self._quote_ident(y_column)
        if top_limit is None:
            numeric_query = f"""
            WITH base AS (
                {ctx.analysis_query}
            )
            SELECT
                {x_ident} AS category,
                AVG({y_ident}) AS avg_value
            FROM base
            WHERE {x_ident} IS NOT NULL
            AND {y_ident} IS NOT NULL
            GROUP BY {x_ident}
            ORDER BY avg_value DESC
            """
        else:
            numeric_query = f"""
            WITH base AS (
                {ctx.analysis_query}
            ), top AS (
                SELECT {x_ident} AS category
                FROM base
                WHERE {x_ident} IS NOT NULL
                GROUP BY category
                ORDER BY COUNT(*) DESC
                LIMIT {int(top_limit)}
            )
            SELECT
                base.{x_ident} AS category,
                AVG(base.{y_ident}) AS avg_value
            FROM base
            JOIN top ON base.{x_ident} = top.category
            WHERE base.{y_ident} IS NOT NULL
            GROUP BY base.{x_ident}
            ORDER BY avg_value DESC
            """
        try:
            numeric_rows = await self.sf.execute_query(numeric_query)
        except Exception as exc:
            logger.warning("Custom category numeric query failed for %s: %s", x_column, exc)
            return None
        data = [
            {"category": row.get("CATEGORY"), "avg_value": self._coerce_float(row.get("AVG_VALUE"))}
            for row in numeric_rows
        ]
        if not data:
            return None
        return self._build_chart_spec(
            chart_type=chart_type,
            title=f"{y_column} by {x_column}",
            x_key="category",
            y_key="avg_value",
            data=data,
            narrative=[
                f"Average {y_column} across top categories",
                "Use to compare category-level magnitude",
            ],
            source_columns=[x_column, y_column],
            x_title=x_column,
            y_title=f"Average {y_column}",
        )

    async def estimate_workflow_tokens(self, table_asset_id: int, column_name: str) -> dict[str, Any]:
        ctx = await self._load_context(table_asset_id, column_name)
        semantic_type = ctx.column_meta.semantic_type
        estimates: list[dict[str, Any]] = []
        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        null_count = self._coerce_int((analysis.get("nulls") or {}).get("null_count"))
        conflict_groups = self._coerce_int((analysis.get("conflicts") or {}).get("conflict_groups"))
        summary_payload = {
            "column": column_name,
            "semantic_type": semantic_type,
            "stats": analysis.get("stats"),
            "nulls": analysis.get("nulls"),
            "conflicts": analysis.get("conflicts"),
            "visuals": analysis.get("visuals", []),
            "repair_plan": analysis.get("repair_plan"),
        }
        summary_instruction = (
            "Summarize the column in plain language. Return JSON with keys: summary, key_points, risks."
        )

        if semantic_type in {"numeric", "temporal"}:
            estimates.append({
                "task": "numeric_distribution",
                "token_count": 0,
            })
            estimates.append({
                "task": "numeric_correlations",
                "token_count": 0,
            })
            estimates.append({
                "task": "numeric_periodicity",
                "token_count": 0,
            })
            estimates.append({
                "task": "scan_nulls",
                "token_count": 0,
            })
            instruction = "Summarize numeric column insights based on stats and visuals."
            try:
                payload = await self._build_numeric_insights_payload(ctx, column_name)
            except Exception:
                payload = {"column": column_name, "stats": {}, "visuals": []}
            estimates.append({
                "task": "generate_insights",
                "token_count": await self._estimate_ai_agg_tokens(payload, instruction),
            })
            estimates.append({
                "task": "generate_summary",
                "token_count": await self._estimate_ai_agg_tokens(summary_payload, summary_instruction),
            })
            if null_count or conflict_groups:
                estimates.append({"task": "plan_data_repairs", "token_count": 0})
                estimates.append({"task": "approval_gate", "token_count": 0})
                estimates.append({"task": "apply_data_repairs", "token_count": 0})

        if semantic_type == "categorical":
            estimates.append({
                "task": "categorical_groups",
                "token_count": 0,
            })
            estimates.append({
                "task": "scan_nulls",
                "token_count": 0,
            })
            estimates.append({
                "task": "scan_conflicts",
                "token_count": 0,
            })
            instruction = "Summarize category distribution insights based on stats and visuals."
            try:
                payload = await self._build_categorical_insights_payload(ctx, column_name)
            except Exception:
                payload = {"column": column_name, "stats": {}, "visuals": []}
            estimates.append({
                "task": "generate_insights",
                "token_count": await self._estimate_ai_agg_tokens(payload, instruction),
            })
            estimates.append({
                "task": "generate_summary",
                "token_count": await self._estimate_ai_agg_tokens(summary_payload, summary_instruction),
            })
            if null_count or conflict_groups:
                estimates.append({"task": "plan_data_repairs", "token_count": 0})
                estimates.append({"task": "approval_gate", "token_count": 0})
                estimates.append({"task": "apply_data_repairs", "token_count": 0})

        if semantic_type == "text":
            estimates.append({
                "task": "scan_nulls",
                "token_count": 0,
            })
            summary_tokens = await self._estimate_column_tokens(
                ctx.analysis_query, self._quote_ident(column_name)
            )
            estimates.append({
                "task": "summarize_text",
                **summary_tokens,
            })
            if (ctx.column_meta.overrides or {}).get("row_level_instruction"):
                row_tokens = await self._estimate_column_tokens(ctx.analysis_query, self._quote_ident(column_name))
                instruction_tokens = await self._estimate_tokens_for_prompt(
                    str((ctx.column_meta.overrides or {}).get("row_level_instruction"))
                )
                total_tokens = row_tokens.get("token_count", 0) + instruction_tokens * row_tokens.get("row_count", 0)
                estimates.append({
                    "task": "row_level_extract",
                    **row_tokens,
                    "instruction_tokens": instruction_tokens,
                    "token_count": total_tokens,
                })
            estimates.append({
                "task": "generate_summary",
                "token_count": await self._estimate_ai_agg_tokens(summary_payload, summary_instruction),
            })
            if null_count:
                estimates.append({"task": "plan_data_repairs", "token_count": 0})
                estimates.append({"task": "approval_gate", "token_count": 0})
                estimates.append({"task": "apply_data_repairs", "token_count": 0})

        if semantic_type == "image":
            row_info = await self._estimate_column_tokens(ctx.analysis_query, self._quote_ident(column_name))
            instruction = "Describe the image in under 200 characters."
            instruction_tokens = await self._estimate_tokens_for_prompt(instruction)
            estimates.append({
                "task": "describe_images",
                "row_count": row_info.get("row_count", 0),
                "token_count": instruction_tokens * row_info.get("row_count", 0),
                "instruction_tokens": instruction_tokens,
            })

        if semantic_type not in {"numeric", "temporal", "categorical", "text", "image"}:
            estimates.append({"task": "basic_stats", "token_count": 0})

        total_tokens = sum(self._coerce_int(item.get("token_count", 0)) for item in estimates)
        return {
            "column": column_name,
            "semantic_type": semantic_type,
            "estimates": estimates,
            "total_tokens": total_tokens,
        }

    def _build_chart_spec(
        self,
        chart_type: str,
        title: str,
        x_key: str,
        y_key: str,
        data: list[dict[str, Any]],
        narrative: list[str],
        source_columns: list[str],
        x_title: str | None = None,
        y_title: str | None = None,
    ) -> dict[str, Any]:
        values: list[float] = []
        for row in data:
            raw = row.get(y_key)
            if raw is None or isinstance(raw, bool):
                continue
            if isinstance(raw, (int, float)):
                values.append(float(raw))
                continue
            try:
                values.append(float(str(raw)))
            except (TypeError, ValueError):
                continue
        y_scale = "linear"
        if values:
            min_value = min(values)
            max_value = max(values)
            if min_value > 0 and max_value / min_value >= 1000:
                y_scale = "log"
        return {
            "id": f"chart_{uuid.uuid4().hex}",
            "chartType": chart_type,
            "title": title,
            "xKey": x_key,
            "yKey": y_key,
            "xTitle": x_title or x_key,
            "yTitle": y_title or y_key,
            "yScale": y_scale,
            "data": data,
            "narrative": narrative,
            "sourceColumns": source_columns,
        }

    def _coerce_int(self, value: Any, default: int = 0) -> int:
        if value is None:
            return default
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        try:
            return int(float(str(value)))
        except (TypeError, ValueError):
            return default

    def _coerce_float(self, value: Any, default: float | None = None) -> float | None:
        if value is None:
            return default
        if isinstance(value, bool):
            return float(int(value))
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(str(value))
        except (TypeError, ValueError):
            return default

    def _resolve_temporal_expr(self, ctx: ColumnContext, col: str) -> str:
        num_expr = f"TRY_TO_NUMBER(TO_VARCHAR({col}))"
        ts_expr = f"TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR({col}))"
        return (
            "COALESCE("
            f"{ts_expr}, "
            f"TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR({num_expr}), 'YYYYMMDD'), "
            f"TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR({num_expr} / 1000)), "
            f"TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR({num_expr}))"
            ")"
        )

    def _numeric_expr(self, expr: str) -> str:
        return f"TRY_TO_NUMBER(TO_VARCHAR({expr}))"

    def _hash_payload(self, payload: dict[str, Any]) -> str:
        serialized = json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def _resolve_row_id_column(self, ctx: ColumnContext) -> str | None:
        overrides = ctx.column_meta.overrides or {}
        table_overrides = ctx.table_meta.overrides or {}
        for key in ("row_id_column", "primary_key_column", "row_id", "primary_key"):
            value = overrides.get(key) or table_overrides.get(key)
            if value:
                return str(value)
        return None

    def _normalize_identifier(self, value: str) -> str:
        normalized = re.sub(r"[^A-Za-z0-9_]+", "_", str(value))
        normalized = normalized.strip("_").lower()
        return normalized or "col"

    def _split_table_ref(self, table_ref: str) -> list[str]:
        parts: list[str] = []
        buf: list[str] = []
        in_quotes = False
        for ch in str(table_ref):
            if ch == '"':
                in_quotes = not in_quotes
            if ch == "." and not in_quotes:
                parts.append("".join(buf).strip())
                buf = []
                continue
            buf.append(ch)
        if buf:
            parts.append("".join(buf).strip())
        return [part for part in parts if part]

    def _build_fixing_table_ref(self, table_ref: str, column_name: str) -> str:
        parts = self._split_table_ref(table_ref)
        suffix = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        normalized = self._normalize_identifier(column_name)
        table_name = f"fixing_{normalized}_{suffix}"
        quoted_table = self._quote_ident(table_name)
        if len(parts) >= 2:
            prefix = ".".join(parts[:-1])
            return f"{prefix}.{quoted_table}"
        if len(parts) == 1:
            return f"{parts[0]}.{quoted_table}"
        return quoted_table

    def _build_windowed_query(
        self,
        ctx: ColumnContext,
        sample_size: int | None,
        window_days: int | None,
        time_column: str | None,
    ) -> str:
        base = ctx.analysis_query
        filters: list[str] = []
        if window_days and time_column:
            time_expr = self._resolve_temporal_expr(ctx, self._quote_ident(time_column))
            filters.append(
                f"{time_expr} >= DATEADD(day, -{int(window_days)}, CURRENT_TIMESTAMP())"
            )
        query = f"SELECT * FROM ({base})"
        if filters:
            query += " WHERE " + " AND ".join(filters)
        if sample_size and sample_size > 0:
            query += f" LIMIT {int(sample_size)}"
        return query

    async def _compute_snapshot(
        self,
        ctx: ColumnContext,
        column_name: str,
        group_by_columns: list[str],
    ) -> dict[str, Any]:
        def sanitize_error_message(message: str) -> str:
            if not message:
                return ""
            # Remove Snowflake query IDs / UUIDs that change on every run.
            cleaned = re.sub(
                r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                "<id>",
                message,
                flags=re.IGNORECASE,
            )
            cleaned = re.sub(
                r"(?i)query id[:\s]*[0-9a-f\\-]+",
                "query id:<id>",
                cleaned,
            )
            return cleaned

        col = self._quote_ident(column_name)
        base_query = ctx.analysis_query
        time_column = ctx.time_column
        time_expr = self._resolve_temporal_expr(ctx, self._quote_ident(time_column)) if time_column else None
        time_select = ""
        if time_expr:
            time_select = f", MIN({time_expr}) AS min_time, MAX({time_expr}) AS max_time"
        snapshot_query = f"""
        WITH base AS ({base_query})
        SELECT
            COUNT(*) AS total_count,
            COUNT_IF({col} IS NULL) AS null_count
            {time_select}
        FROM base
        """
        row = {}
        try:
            rows = await self.sf.execute_query(snapshot_query)
            row = rows[0] if rows else {}
        except Exception as exc:
            if time_expr:
                fallback_query = f"""
                WITH base AS ({base_query})
                SELECT
                    COUNT(*) AS total_count,
                    COUNT_IF({col} IS NULL) AS null_count
                FROM base
                """
                try:
                    rows = await self.sf.execute_query(fallback_query)
                    row = rows[0] if rows else {}
                    time_expr = None
                except Exception as fallback_exc:
                    error_snapshot = {
                        "error": sanitize_error_message(str(fallback_exc)),
                        "group_by_columns": group_by_columns,
                        "time_column": time_column,
                        "base_query": base_query,
                    }
                    error_snapshot["signature"] = self._hash_payload(error_snapshot)
                    return error_snapshot
            else:
                error_snapshot = {
                    "error": sanitize_error_message(str(exc)),
                    "group_by_columns": group_by_columns,
                    "time_column": time_column,
                    "base_query": base_query,
                }
                error_snapshot["signature"] = self._hash_payload(error_snapshot)
                return error_snapshot

        snapshot = {
            "total_count": self._coerce_int(row.get("TOTAL_COUNT")),
            "null_count": self._coerce_int(row.get("NULL_COUNT")),
        }
        if time_expr:
            snapshot["min_time"] = row.get("MIN_TIME")
            snapshot["max_time"] = row.get("MAX_TIME")

        conflict_rows = None
        if group_by_columns:
            group_exprs = ", ".join(self._quote_ident(name) for name in group_by_columns)
            conflict_query = f"""
            WITH base AS ({base_query}),
            grouped AS (
                SELECT {group_exprs}, COUNT(DISTINCT {col}) AS distinct_values
                FROM base
                WHERE {col} IS NOT NULL
                GROUP BY {group_exprs}
            ),
            conflict_groups AS (
                SELECT {group_exprs}
                FROM grouped
                WHERE distinct_values > 1
            )
            SELECT COUNT(*) AS conflict_rows
            FROM base
            JOIN conflict_groups
            ON {" AND ".join([f"base.{self._quote_ident(name)} = conflict_groups.{self._quote_ident(name)}" for name in group_by_columns])}
            """
            try:
                conflict_rows_result = await self.sf.execute_query(conflict_query)
                if conflict_rows_result:
                    conflict_rows = self._coerce_int(conflict_rows_result[0].get("CONFLICT_ROWS"))
            except Exception:
                conflict_rows = None
        snapshot["conflict_rows"] = conflict_rows
        snapshot["group_by_columns"] = group_by_columns
        snapshot["time_column"] = time_column
        snapshot["signature"] = self._hash_payload({k: v for k, v in snapshot.items() if k != "signature"})
        return snapshot

    async def _compute_null_fill_value(
        self,
        ctx: ColumnContext,
        column_name: str,
        strategy: str | None,
    ) -> tuple[str | None, Any]:
        col = self._quote_ident(column_name)
        numeric_expr = self._numeric_expr(col)
        base_query = ctx.analysis_query
        strategy_key = str(strategy or "").lower()

        async def fetch_single_value(query: str) -> Any:
            rows = await self.sf.execute_query(query)
            if not rows:
                return None
            row = rows[0]
            if not isinstance(row, dict):
                return None
            return next(iter(row.values()), None)

        if strategy_key in {"zero_impute", "zero"}:
            return "0", 0
        if strategy_key in {"empty_string", "empty"}:
            return "''", ""
        if strategy_key in {"mean_impute", "mean"}:
            query = f"""
            WITH base AS ({base_query})
            SELECT AVG({numeric_expr}) AS fill_value
            FROM base
            WHERE {numeric_expr} IS NOT NULL
            """
            value = await fetch_single_value(query)
            return str(value) if value is not None else None, value
        if strategy_key in {"median_impute", "median"}:
            query = f"""
            WITH base AS ({base_query})
            SELECT APPROX_PERCENTILE({numeric_expr}, 0.5) AS fill_value
            FROM base
            WHERE {numeric_expr} IS NOT NULL
            """
            value = await fetch_single_value(query)
            return str(value) if value is not None else None, value
        if strategy_key in {"mode_impute", "mode"}:
            query = f"""
            WITH base AS ({base_query})
            SELECT {col} AS fill_value
            FROM base
            WHERE {col} IS NOT NULL
            GROUP BY {col}
            ORDER BY COUNT(*) DESC
            LIMIT 1
            """
            value = await fetch_single_value(query)
            if value is None:
                return None, None
            return f"'{self._sanitize_literal(value)}'", value
        if strategy_key in {"min_impute", "min"}:
            query = f"""
            WITH base AS ({base_query})
            SELECT MIN({col}) AS fill_value
            FROM base
            WHERE {col} IS NOT NULL
            """
            value = await fetch_single_value(query)
            if value is None:
                return None, None
            if isinstance(value, str):
                return f"'{self._sanitize_literal(value)}'", value
            return str(value), value
        if strategy_key in {"max_impute", "max", "forward_fill"}:
            query = f"""
            WITH base AS ({base_query})
            SELECT MAX({col}) AS fill_value
            FROM base
            WHERE {col} IS NOT NULL
            """
            value = await fetch_single_value(query)
            if value is None:
                return None, None
            if isinstance(value, str):
                return f"'{self._sanitize_literal(value)}'", value
            return str(value), value
        return None, None

    def _build_strands_model(self) -> OpenAIModel:
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
        return OpenAIModel(
            model_id=model_id,
            params={
                "max_tokens": 4096,
                "temperature": 0.2,
            },
        )

    def _periodicity_lags(self, bucket: str) -> list[int]:
        bucket_key = str(bucket or "day").lower()
        if bucket_key == "hour":
            return [1, 6, 12, 24]
        if bucket_key == "week":
            return [1, 4, 12]
        if bucket_key == "month":
            return [1, 3, 6, 12]
        return [1, 7, 30]

    def _looks_like_temporal_name(self, column_name: str) -> bool:
        lowered = str(column_name or "").lower()
        return any(
            token in lowered
            for token in (
                "date",
                "time",
                "timestamp",
                "epoch",
            )
        )

    def _default_time_bucket(self, column_name: str | None) -> str:
        lowered = str(column_name or "").lower()
        if "epoch" in lowered:
            return "hour"
        if "time" in lowered and "date" not in lowered:
            return "hour"
        return "day"

    async def _list_temporal_columns_with_meta(
        self, table_asset_id: int
    ) -> list[dict[str, Any]]:
        result = await self.db.execute(
            select(
                ColumnMetadata.column_name,
                ColumnMetadata.semantic_type,
                ColumnMetadata.metadata_payload,
            ).where(
                ColumnMetadata.table_asset_id == table_asset_id,
            )
        )
        candidates: list[dict[str, Any]] = []
        for row in result.all():
            if not row or not row[0]:
                continue
            column_name, semantic_type, metadata_payload = row
            sql_type = ""
            if isinstance(metadata_payload, dict):
                sql_type = str(metadata_payload.get("sql_type") or "").lower()
            is_native = any(token in sql_type for token in ("date", "time", "timestamp"))
            is_temporal = semantic_type == "temporal" or is_native
            if not is_temporal and self._looks_like_temporal_name(column_name):
                is_temporal = True
            if is_temporal:
                candidates.append(
                    {
                        "column": column_name,
                        "is_native": is_native,
                        "sql_type": sql_type,
                    }
                )
        # 保持顺序去重
        seen: set[str] = set()
        ordered: list[dict[str, Any]] = []
        for item in candidates:
            name = item.get("column")
            if not name or name in seen:
                continue
            seen.add(name)
            ordered.append(item)
        return ordered

    async def _list_temporal_columns(self, table_asset_id: int) -> list[str]:
        items = await self._list_temporal_columns_with_meta(table_asset_id)
        return [item["column"] for item in items if item.get("column")]

    async def _list_categorical_columns(
        self, table_asset_id: int, max_columns: int = 5
    ) -> list[dict[str, Any]]:
        result = await self.db.execute(
            select(
                ColumnMetadata.column_name,
                ColumnMetadata.semantic_type,
                ColumnMetadata.metadata_payload,
            ).where(
                ColumnMetadata.table_asset_id == table_asset_id,
            )
        )
        candidates: list[tuple[str, float, int | None]] = []
        for row in result.all():
            if not row or not row[0]:
                continue
            column_name, semantic_type, metadata_payload = row
            if semantic_type not in {"categorical", "text"}:
                # 如果类型没标注，也允许低基数文本列
                pass
            sql_type = ""
            unique_count = None
            sample_size = None
            if isinstance(metadata_payload, dict):
                sql_type = str(metadata_payload.get("sql_type") or "").lower()
                unique_count = metadata_payload.get("unique_count")
                sample_size = metadata_payload.get("sample_size")
            is_textual = any(token in sql_type for token in ("char", "text", "string", "varchar"))
            if semantic_type not in {"categorical", "text"} and not is_textual:
                continue
            if unique_count is None:
                # 没有唯一值信息时，保守放过
                cardinality = 1.0
            else:
                try:
                    denom = float(sample_size) if sample_size else float(unique_count)
                    cardinality = float(unique_count) / denom if denom else 1.0
                except (TypeError, ValueError):
                    cardinality = 1.0
            # 低基数优先
            if unique_count is not None and unique_count <= 50:
                candidates.append((column_name, cardinality, unique_count))
            elif cardinality <= 0.2:
                candidates.append((column_name, cardinality, unique_count))
        # 按基数排序，取前 max_columns
        candidates.sort(key=lambda item: item[1])
        selected = candidates[: max_columns or 5]
        return [
            {"column": name, "cardinality": card, "unique_count": uniq}
            for name, card, uniq in selected
        ]

    async def _list_all_columns(self, table_asset_id: int) -> list[str]:
        result = await self.db.execute(
            select(ColumnMetadata.column_name).where(
                ColumnMetadata.table_asset_id == table_asset_id
            )
        )
        return [row[0] for row in result.all() if row and row[0]]

    async def _list_numeric_columns(self, table_asset_id: int) -> list[str]:
        result = await self.db.execute(
            select(
                ColumnMetadata.column_name,
                ColumnMetadata.confidence,
                ColumnMetadata.metadata_payload,
            ).where(
                ColumnMetadata.table_asset_id == table_asset_id,
                ColumnMetadata.semantic_type == "numeric",
            )
        )
        rows = result.all()

        def null_rate(row: tuple[Any, Any, Any]) -> float:
            metadata_payload = row[2] or {}
            rate = metadata_payload.get("null_rate")
            if isinstance(rate, (int, float)):
                return float(rate)
            return 1.0

        sorted_rows = sorted(
            rows,
            key=lambda row: (
                null_rate(row),
                -float(row[1] or 0.0),
                str(row[0]).lower(),
            ),
        )
        return [row[0] for row in sorted_rows if row and row[0]]

    def _resolve_image_file_expr(self, ctx: ColumnContext, col: str) -> str | None:
        sql_type = (ctx.column_meta.metadata_payload or {}).get("sql_type")
        if sql_type and "FILE" in str(sql_type).upper():
            return col

        overrides = ctx.column_meta.overrides or {}
        table_overrides = ctx.table_meta.overrides or {}
        stage = overrides.get("image_stage") or table_overrides.get("image_stage")
        if not stage:
            return None

        prefix = overrides.get("image_path_prefix") or ""
        suffix = overrides.get("image_path_suffix") or ""
        stage_value = str(stage)
        if not stage_value.startswith("@"):
            stage_value = f"@{stage_value}"
        stage_literal = self._sanitize_literal(stage_value)

        path_expr = col
        if prefix:
            prefix_literal = self._sanitize_literal(str(prefix))
            path_expr = f"'{prefix_literal}' || {path_expr}"
        if suffix:
            suffix_literal = self._sanitize_literal(str(suffix))
            path_expr = f"{path_expr} || '{suffix_literal}'"

        return f"TO_FILE('{stage_literal}', {path_expr})"

    async def _ensure_feature_column_metadata(
        self,
        table_asset_id: int,
        output_column: str,
        source_column: str,
        feature_type: str,
    ) -> None:
        result = await self.db.execute(
            select(ColumnMetadata).where(
                ColumnMetadata.table_asset_id == table_asset_id,
                ColumnMetadata.column_name == output_column,
            )
        )
        record = result.scalar_one_or_none()
        metadata_payload = dict(record.metadata_payload or {}) if record else {}
        metadata_payload.update(
            {
                "sql_type": "VARCHAR",
                "derived_from": source_column,
                "feature_type": feature_type,
                "generated_by": "feature_engineering",
            }
        )

        if record:
            record.semantic_type = record.semantic_type or "text"
            record.confidence = record.confidence or 0.6
            record.metadata_payload = metadata_payload
            record.last_updated = datetime.now(timezone.utc)
        else:
            record = ColumnMetadata()
            record.table_asset_id = table_asset_id
            record.column_name = output_column
            record.semantic_type = "text"
            record.confidence = 0.6
            record.metadata_payload = metadata_payload
            record.provenance = {
                "generated_by": "feature_engineering",
                "source_column": source_column,
            }
            record.examples = None
            record.overrides = None
            record.last_updated = datetime.now(timezone.utc)
            self.db.add(record)

        await self.db.commit()
        await self.db.refresh(record)

    async def _estimate_column_tokens(self, base_query: str, column_expr: str) -> dict[str, Any]:
        model_id = (self.model_id or "mistral-large2").lower()
        unsupported_models = {
            "claude-4-opus",
            "claude-4-sonnet",
            "claude-3-7-sonnet",
            "claude-3-5-sonnet",
            "openai-gpt-4.1",
            "openai-o4-mini",
        }
        if model_id in unsupported_models:
            model_id = "mistral-large2"
        query = f"""
        WITH base AS (
            {base_query}
        )
        SELECT
            COUNT(*) AS row_count,
            SUM(AI_COUNT_TOKENS('ai_complete', '{model_id}', TO_VARCHAR({column_expr}))) AS token_count
        FROM base
        WHERE {column_expr} IS NOT NULL
        """
        try:
            result = await self.sf.execute_query(query)
        except Exception:
            fallback_query = f"""
            WITH base AS (
                {base_query}
            )
            SELECT
                COUNT(*) AS row_count,
                SUM(AI_COUNT_TOKENS('ai_complete', 'mistral-large2', TO_VARCHAR({column_expr}))) AS token_count
            FROM base
            WHERE {column_expr} IS NOT NULL
            """
            try:
                result = await self.sf.execute_query(fallback_query)
            except Exception:
                count_query = f"""
                WITH base AS (
                    {base_query}
                )
                SELECT
                    COUNT(*) AS row_count,
                    AVG(LENGTH(TO_VARCHAR({column_expr}))) AS avg_len
                FROM base
                WHERE {column_expr} IS NOT NULL
                """
                try:
                    count_result = await self.sf.execute_query(count_query)
                    if not count_result:
                        return {"row_count": 0, "token_count": 0}
                    row_count = self._coerce_int(count_result[0].get("ROW_COUNT"))
                    avg_len = count_result[0].get("AVG_LEN")
                    approx_tokens = self._coerce_int(row_count * (float(avg_len) / 4)) if avg_len else 0
                    return {"row_count": row_count, "token_count": approx_tokens}
                except Exception:
                    return {"row_count": 0, "token_count": 0}

        if not result:
            count_query = f"""
            WITH base AS (
                {base_query}
            )
            SELECT
                COUNT(*) AS row_count,
                AVG(LENGTH(TO_VARCHAR({column_expr}))) AS avg_len
            FROM base
            WHERE {column_expr} IS NOT NULL
            """
            try:
                count_result = await self.sf.execute_query(count_query)
                if not count_result:
                    return {"row_count": 0, "token_count": 0}
                row_count = self._coerce_int(count_result[0].get("ROW_COUNT"))
                avg_len = count_result[0].get("AVG_LEN")
                approx_tokens = self._coerce_int(row_count * (float(avg_len) / 4)) if avg_len else 0
                return {"row_count": row_count, "token_count": approx_tokens}
            except Exception:
                return {"row_count": 0, "token_count": 0}
        return {
            "row_count": self._coerce_int(result[0].get("ROW_COUNT", 0)),
            "token_count": self._coerce_int(result[0].get("TOKEN_COUNT") or 0),
        }

    async def _estimate_tokens_for_prompt(self, prompt: str) -> int:
        safe_prompt = self._sanitize_literal(prompt)
        model_id = self.model_id
        unsupported_models = {
            "claude-4-opus",
            "claude-4-sonnet",
            "claude-3-7-sonnet",
            "claude-3-5-sonnet",
            "openai-gpt-4.1",
            "openai-o4-mini",
        }
        if model_id in unsupported_models:
            model_id = "mistral-large2"
        query = (
            "SELECT AI_COUNT_TOKENS('ai_complete', "
            f"'{model_id}', '{safe_prompt}') AS TOKEN_COUNT"
        )
        try:
            result = await self.sf.execute_query(query)
        except Exception:
            fallback_query = (
                "SELECT AI_COUNT_TOKENS('ai_complete', "
                f"'mistral-large2', '{safe_prompt}') AS TOKEN_COUNT"
            )
            try:
                result = await self.sf.execute_query(fallback_query)
                if not result:
                    return max(1, len(prompt) // 4)
                return int(result[0]["TOKEN_COUNT"])
            except Exception:
                return max(1, len(prompt) // 4)
        if not result:
            return max(1, len(prompt) // 4)
        try:
            return int(result[0]["TOKEN_COUNT"])
        except (TypeError, ValueError, KeyError):
            return max(1, len(prompt) // 4)

    async def _estimate_ai_agg_tokens(self, payload: dict[str, Any], instruction: str) -> int:
        prompt_tokens = await self._estimate_tokens_for_prompt(instruction)
        try:
            payload_tokens = await self._estimate_tokens_for_prompt(json.dumps(payload))
        except Exception:
            payload_tokens = 0
        return prompt_tokens + payload_tokens

    async def _build_numeric_insights_payload(self, ctx: ColumnContext, column_name: str) -> dict[str, Any]:
        col = self._quote_ident(column_name)
        is_temporal = ctx.column_meta.semantic_type == "temporal"
        if is_temporal:
            time_expr = self._resolve_temporal_expr(ctx, col)
            stats_query = f"""
            WITH base AS (
                {ctx.analysis_query}
            )
            SELECT
                TO_VARCHAR(MIN({time_expr})) AS min_value,
                TO_VARCHAR(MAX({time_expr})) AS max_value,
                COUNT_IF({time_expr} IS NOT NULL) AS total_count
            FROM base
            WHERE {time_expr} IS NOT NULL
            """
        else:
            stats_query = f"""
            WITH base AS (
                {ctx.analysis_query}
            )
            SELECT
                MIN({col}) AS min_value,
                MAX({col}) AS max_value,
                AVG({col}) AS avg_value,
                STDDEV({col}) AS stddev_value
            FROM base
            WHERE {col} IS NOT NULL
            """
        stats_rows = await self.sf.execute_query(stats_query)
        raw_stats = stats_rows[0] if stats_rows else {}
        stats = {
            "min_value": raw_stats.get("MIN_VALUE"),
            "max_value": raw_stats.get("MAX_VALUE"),
        }
        if is_temporal:
            stats["total_count"] = self._coerce_int(raw_stats.get("TOTAL_COUNT"))
        else:
            stats["avg_value"] = raw_stats.get("AVG_VALUE")
            stats["stddev_value"] = raw_stats.get("STDDEV_VALUE")

        visuals: list[dict[str, Any]] = []
        if not is_temporal and stats.get("MIN_VALUE") is not None and stats.get("MAX_VALUE") is not None:
            hist_query = f"""
            WITH base AS (
                {ctx.analysis_query}
            ), stats AS (
                SELECT
                    MIN({col}) AS min_value,
                    MAX({col}) AS max_value
                FROM base
                WHERE {col} IS NOT NULL
            )
            SELECT
                WIDTH_BUCKET(base.{col}, stats.min_value, stats.max_value, 12) AS bin,
                COUNT(*) AS count
            FROM base, stats
            WHERE base.{col} IS NOT NULL
            GROUP BY bin
            ORDER BY bin
            """
            histogram_rows = await self.sf.execute_query(hist_query)
            histogram_data = [
                {"bin": row.get("BIN"), "count": row.get("COUNT")}
                for row in histogram_rows
            ]
            visuals.append(
                self._build_chart_spec(
                    chart_type="bar",
                    title=f"Distribution of {column_name}",
                    x_key="bin",
                    y_key="count",
                    data=histogram_data,
                    narrative=[
                        "Distribution based on 12 bins",
                        f"Min: {stats.get('min_value')}, Max: {stats.get('max_value')}",
                    ],
                    source_columns=[column_name],
                    x_title=f"{column_name} (bin)",
                    y_title="Count",
                )
            )

        time_col = None
        if is_temporal:
            time_col = time_expr or col
        elif ctx.time_column:
            time_col = self._resolve_temporal_expr(ctx, self._quote_ident(ctx.time_column))
        if time_col:
            time_bucket_expr = f"DATE_TRUNC('day', {time_col})"
            time_query = f"""
            WITH base AS (
                {ctx.analysis_query}
            )
            SELECT
                TO_VARCHAR({time_bucket_expr}) AS time_bucket,
                {"COUNT(*) AS count" if is_temporal else f"AVG({col}) AS avg_value"}
            FROM base
            WHERE {time_col} IS NOT NULL
            {"AND " + col + " IS NOT NULL" if not is_temporal else ""}
            GROUP BY {time_bucket_expr}
            ORDER BY time_bucket
            LIMIT 90
            """
            time_rows = await self.sf.execute_query(time_query)
            if is_temporal:
                time_data = [
                    {"time_bucket": row.get("TIME_BUCKET"), "count": row.get("COUNT")}
                    for row in time_rows
                ]
            else:
                time_data = [
                    {"time_bucket": row.get("TIME_BUCKET"), "avg_value": row.get("AVG_VALUE")}
                    for row in time_rows
                ]
            time_title = column_name if is_temporal else (ctx.time_column or "time")
            y_title = "Count" if is_temporal else f"Average {column_name}"
            visuals.append(
                self._build_chart_spec(
                    chart_type="line",
                    title=f"{column_name} over time",
                    x_key="time_bucket",
                    y_key="count" if is_temporal else "avg_value",
                    data=time_data,
                    narrative=["Daily trend based on counts" if is_temporal else "Daily trend based on average values"],
                    source_columns=[column_name] if is_temporal else [ctx.time_column, column_name],
                    x_title=time_title,
                    y_title=y_title,
                )
            )

        return {
            "column": column_name,
            "stats": stats,
            "visuals": visuals,
            "structure_type": ctx.structure_type,
        }

    async def _build_categorical_insights_payload(self, ctx: ColumnContext, column_name: str) -> dict[str, Any]:
        col = self._quote_ident(column_name)
        total_query = f"""
        WITH base AS (
            {ctx.analysis_query}
        )
        SELECT COUNT(*) AS total_count
        FROM base
        WHERE {col} IS NOT NULL
        """
        total_rows = await self.sf.execute_query(total_query)
        total_count = self._coerce_int(total_rows[0]["TOTAL_COUNT"]) if total_rows else 0

        top_query = f"""
        WITH base AS (
            {ctx.analysis_query}
        )
        SELECT {col} AS category, COUNT(*) AS count
        FROM base
        WHERE {col} IS NOT NULL
        GROUP BY category
        ORDER BY count DESC
        LIMIT 6
        """
        top_rows_raw = await self.sf.execute_query(top_query)
        top_rows = [
            {"category": row.get("CATEGORY"), "count": self._coerce_int(row.get("COUNT"))}
            for row in top_rows_raw
        ]
        top_sum = sum(row.get("count", 0) for row in top_rows)
        if total_count > top_sum:
            top_rows.append({"category": "Other", "count": total_count - top_sum})

        visuals = [
            self._build_chart_spec(
                chart_type="bar",
                title=f"Top categories for {column_name}",
                x_key="category",
                y_key="count",
                data=top_rows,
                narrative=[
                    "Top categories shown with long-tail grouped as Other",
                    f"Total non-null rows: {total_count}",
                ],
                source_columns=[column_name],
                x_title=column_name,
                y_title="Count",
            )
        ]

        return {
            "column": column_name,
            "stats": {"total_count": total_count},
            "visuals": visuals,
        }

    async def _run_ai_agg(self, payload: dict[str, Any], instruction: str) -> dict[str, Any]:
        payload_json = json.dumps(payload).replace("$$", "$ $")
        query = f"""
        WITH data AS (
            SELECT PARSE_JSON($${payload_json}$$) AS payload
        )
        SELECT AI_AGG(payload, $${instruction}$$) AS RESPONSE
        FROM data
        """
        try:
            result = await self.ai_sql.sf.execute_query(query)
        except Exception as exc:
            logger.warning("AI_AGG failed: %s", exc)
            return {"insights": [], "caveats": ["AI_AGG failed; retry later."]}
        raw_response = result[0]["RESPONSE"] if result else None
        if isinstance(raw_response, dict):
            return raw_response
        if raw_response:
            try:
                return json.loads(raw_response)
            except json.JSONDecodeError:
                return {"insights": [str(raw_response)]}
        return {"insights": []}

    async def _ensure_column(self, table_ref: str, column_name: str) -> None:
        describe_query = f"DESC TABLE {table_ref}"
        columns = await self.sf.execute_query(describe_query)
        existing = {row.get("name", row.get("NAME")) for row in columns}
        if column_name in existing:
            return
        alter_query = f"ALTER TABLE {table_ref} ADD COLUMN {self._quote_ident(column_name)} VARCHAR"
        await self.sf.execute_query(alter_query)

    def _sanitize_literal(self, text: str) -> str:
        cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", " ", str(text))
        cleaned = cleaned.replace("\\", "\\\\").replace("'", "''")
        cleaned = cleaned.replace("\r", "\\n").replace("\n", "\\n").replace("\t", " ")
        return cleaned

    def _quote_ident(self, identifier: str) -> str:
        escaped = identifier.replace('"', '""')
        return f'"{escaped}"'

    def _strip_limit_clause(self, query: str) -> str:
        trimmed = str(query).strip().rstrip(";")
        pattern = re.compile(r"\s+LIMIT\s+\d+(\s+OFFSET\s+\d+)?\s*$", re.IGNORECASE)
        return pattern.sub("", trimmed).strip()
