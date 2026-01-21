"""Column-level analysis workflows using Strands agents."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
import re

from sqlalchemy import select
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.ext.asyncio import AsyncSession
from strands import Agent, tool
from strands.hooks import (
    AfterInvocationEvent,
    AfterToolCallEvent,
    BeforeInvocationEvent,
    BeforeToolCallEvent,
    HookProvider,
    HookRegistry,
    MessageAddedEvent,
)
from strands.models.openai import OpenAIModel

from ..core.config import settings
from ..models.column_metadata import ColumnMetadata
from ..models.table_asset_metadata import TableAssetMetadata
from ..services.modular_ai_sql_service import ModularAISQLService
from ..services.snowflake_service import SnowflakeService
from ..services.column_metadata_service import ColumnMetadataService
from .strands_aisql_agent import create_aisql_agent

logger = logging.getLogger(__name__)


@dataclass
class ColumnContext:
    table_asset_id: int
    column_name: str
    base_query: str
    analysis_query: str
    table_ref: str | None
    time_column: str | None
    structure_type: str | None
    column_meta: ColumnMetadata
    table_meta: TableAssetMetadata


class ColumnWorkflowLogBuffer:
    """Capture Strands agent logs and tool calls for UI inspection."""

    def __init__(self) -> None:
        self.entries: list[dict[str, Any]] = []
        self.tool_calls: list[dict[str, Any]] = []
        self._sequence = 0
        self.default_table_asset_id: int | None = None
        self.default_column_name: str | None = None
        self._last_synced_entries = 0
        self._last_synced_tool_calls = 0

    def set_default_context(self, table_asset_id: int, column_name: str) -> None:
        self.default_table_asset_id = table_asset_id
        self.default_column_name = column_name

    def has_updates(self) -> bool:
        return (
            len(self.entries) > self._last_synced_entries
            or len(self.tool_calls) > self._last_synced_tool_calls
        )

    def mark_synced(self) -> None:
        self._last_synced_entries = len(self.entries)
        self._last_synced_tool_calls = len(self.tool_calls)

    def add_entry(self, event_type: str, message: str, data: dict[str, Any] | None = None) -> None:
        entry = {
            "type": event_type,
            "timestamp": datetime.utcnow().isoformat(),
            "message": message,
            "data": data or {},
        }
        self.entries.append(entry)

    def add_tool_call(
        self,
        tool_use_id: str | None,
        tool_name: str,
        agent_name: str | None,
        tool_input: dict[str, Any] | None,
    ) -> None:
        self._sequence += 1
        started_at = datetime.utcnow().isoformat()
        self.tool_calls.append(
            {
                "tool_use_id": tool_use_id,
                "tool_name": tool_name,
                "agent_name": agent_name,
                "input": tool_input or {},
                "status": "running",
                "timestamp": started_at,
                "started_at": started_at,
                "sequence": self._sequence,
            }
        )

    def update_tool_call(
        self,
        tool_use_id: str | None,
        status: str,
        error: str | None = None,
    ) -> None:
        if tool_use_id:
            for call in reversed(self.tool_calls):
                if call.get("tool_use_id") == tool_use_id:
                    call["status"] = status
                    call["ended_at"] = datetime.utcnow().isoformat()
                    started_at = call.get("started_at")
                    if started_at:
                        try:
                            start_dt = datetime.fromisoformat(str(started_at))
                            end_dt = datetime.fromisoformat(str(call["ended_at"]))
                            call["duration_ms"] = int(
                                (end_dt - start_dt).total_seconds() * 1000
                            )
                        except ValueError:
                            pass
                    if error:
                        call["error"] = error
                    return
            return
        for call in reversed(self.tool_calls):
            if call.get("status") == "running":
                call["status"] = status
                call["ended_at"] = datetime.utcnow().isoformat()
                if error:
                    call["error"] = error
                return


class ColumnWorkflowLogHook(HookProvider):
    """Hook for capturing Strands agent logs and tool invocations."""

    def __init__(self, buffer: ColumnWorkflowLogBuffer, max_preview: int = 240) -> None:
        self.buffer = buffer
        self.max_preview = max_preview

    def register_hooks(self, registry: HookRegistry) -> None:
        registry.add_callback(BeforeInvocationEvent, self.log_agent_start)
        registry.add_callback(AfterInvocationEvent, self.log_agent_end)
        registry.add_callback(BeforeToolCallEvent, self.log_tool_start)
        registry.add_callback(AfterToolCallEvent, self.log_tool_end)
        registry.add_callback(MessageAddedEvent, self.log_message)

    def _truncate(self, value: Any) -> str:
        raw = str(value) if value is not None else ""
        if len(raw) <= self.max_preview:
            return raw
        return raw[: self.max_preview].rstrip() + "..."

    def _format_content(self, content: Any) -> str:
        if content is None:
            return ""
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if isinstance(item, dict):
                    if "text" in item:
                        parts.append(str(item.get("text", "")))
                    elif "json" in item:
                        parts.append(json.dumps(item.get("json"), default=str))
                    else:
                        parts.append(json.dumps(item, default=str))
                else:
                    parts.append(str(item))
            return " ".join(part for part in parts if part)
        if isinstance(content, dict):
            return json.dumps(content, default=str)
        return str(content)

    def _apply_context_overrides(self, event: BeforeToolCallEvent) -> None:
        tool_input = event.tool_use.get("input")
        if not isinstance(tool_input, dict):
            tool_input = {}
        invocation_state = event.invocation_state or {}
        table_asset_id = invocation_state.get("table_asset_id")
        column_name = invocation_state.get("column_name")
        if table_asset_id is None:
            table_asset_id = self.buffer.default_table_asset_id
        if not column_name:
            column_name = self.buffer.default_column_name

        tool_spec = None
        if event.selected_tool:
            tool_spec = getattr(event.selected_tool, "tool_spec", None) or getattr(
                event.selected_tool, "spec", None
            )
        schema = tool_spec.get("inputSchema", {}).get("json", {}) if isinstance(tool_spec, dict) else {}
        properties = schema.get("properties") if isinstance(schema, dict) else {}
        allow_table_id = "table_asset_id" in tool_input or "table_asset_id" in properties
        allow_column = "column_name" in tool_input or "column_name" in properties

        if allow_table_id and table_asset_id is not None:
            tool_input["table_asset_id"] = table_asset_id
        if allow_column and column_name:
            tool_input["column_name"] = column_name
        event.tool_use["input"] = tool_input

    def log_agent_start(self, event: BeforeInvocationEvent) -> None:
        self.buffer.add_entry(
            "strands_log",
            f"Agent started: {event.agent.name}",
            {"agent": event.agent.name},
        )

    def log_agent_end(self, event: AfterInvocationEvent) -> None:
        result = getattr(event, "result", None)
        status = "success" if result is not None else "unknown"
        message = f"Agent completed: {event.agent.name}"
        if result is not None and getattr(result, "stop_reason", None):
            message += f" (stop_reason: {result.stop_reason})"
        self.buffer.add_entry(
            "strands_log",
            message,
            {"agent": event.agent.name, "status": status},
        )

    def log_tool_start(self, event: BeforeToolCallEvent) -> None:
        self._apply_context_overrides(event)
        tool_name = event.tool_use.get("name", "unknown")
        tool_use_id = event.tool_use.get("toolUseId") or event.tool_use.get("tool_use_id")
        tool_input = event.tool_use.get("input", {})
        self.buffer.add_tool_call(tool_use_id, tool_name, event.agent.name, tool_input)
        self.buffer.add_entry(
            "status",
            f"Tool started: {tool_name}",
            {"tool_name": tool_name, "state": "running", "input": tool_input},
        )

    def log_tool_end(self, event: AfterToolCallEvent) -> None:
        tool_name = event.tool_use.get("name", "unknown")
        tool_use_id = event.tool_use.get("toolUseId") or event.tool_use.get("tool_use_id")
        exception = getattr(event, "exception", None)
        status = "error" if exception else "success"
        error = str(exception) if exception else None
        self.buffer.update_tool_call(tool_use_id, status, error=error)
        message = f"Tool completed: {tool_name}"
        if error:
            message += f" (error: {error})"
        self.buffer.add_entry(
            "status",
            message,
            {"tool_name": tool_name, "state": status},
        )
        if event.result is not None:
            result_preview = self._truncate(self._format_content(event.result))
            if result_preview:
                self.buffer.add_entry(
                    "strands_log",
                    f"Tool result: {tool_name} -> {result_preview}",
                    {"tool_name": tool_name, "state": status},
                )

    def log_message(self, event: MessageAddedEvent) -> None:
        message = event.message
        role = message.get("role", "unknown")
        content = message.get("content", "")
        preview = self._truncate(self._format_content(content))
        self.buffer.add_entry(
            "strands_log",
            f"{role}: {preview}",
            {"role": role},
        )


class ColumnWorkflowTools:
    """Tool set for column-level workflows."""

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

    async def _sync_workflow_snapshot(
        self,
        ctx: ColumnContext,
        log_buffer: ColumnWorkflowLogBuffer,
        workflow_state: str = "running",
    ) -> None:
        if not log_buffer.has_updates():
            return
        async with self._metadata_lock:
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
        finally:
            await self._sync_workflow_snapshot(ctx, log_buffer, workflow_state="running")

    @tool
    async def generate_numeric_visuals(self, table_asset_id: int, column_name: str) -> dict[str, Any]:
        """Generate visuals for numeric/temporal columns and persist them."""
        ctx = await self._load_context(table_asset_id, column_name)
        col = self._quote_ident(column_name)

        is_temporal = ctx.column_meta.semantic_type == "temporal"
        used_time_columns: set[str] = set()
        time_expr = None
        analysis_errors: list[dict[str, Any]] = []
        if is_temporal:
            time_expr = self._resolve_temporal_expr(ctx, col)
            used_time_columns.add(column_name)
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
        try:
            stats_rows = await self.sf.execute_query(stats_query)
        except Exception as exc:
            logger.warning("Stats query failed for %s: %s", column_name, exc)
            analysis_errors.append({"step": "stats_query", "error": str(exc)})
            stats_rows = []
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

        visual_plan = self._extract_visual_overrides(ctx)
        custom_visuals, custom_errors = await self._build_custom_visuals(ctx, visual_plan)
        if custom_visuals is not None:
            if custom_errors:
                analysis_errors.extend(custom_errors)
            analysis = {
                "visuals": custom_visuals,
                "stats": stats,
                "queries": {
                    "stats_query": stats_query.strip(),
                },
            }
            if analysis_errors:
                analysis["errors"] = analysis_errors
            await self._update_column_analysis(ctx, analysis)
            return {
                "column": column_name,
                "visuals": custom_visuals,
                "stats": stats,
            }

        visuals: list[dict[str, Any]] = []
        histogram_data: list[dict[str, Any]] = []
        hist_query: str | None = None
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
                WIDTH_BUCKET(base.{col}, stats.min_value, stats.max_value, 20) AS bin,
                COUNT(*) AS count
            FROM base, stats
            WHERE base.{col} IS NOT NULL
            GROUP BY bin
            ORDER BY bin
            """
            try:
                histogram_rows = await self.sf.execute_query(hist_query)
            except Exception as exc:
                logger.warning("Histogram query failed for %s: %s", column_name, exc)
                analysis_errors.append({"step": "hist_query", "error": str(exc)})
                histogram_rows = []
            histogram_data = [
                {"bin": row.get("BIN"), "count": self._coerce_int(row.get("COUNT"))}
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
                        "Distribution based on 20 bins",
                        f"Min: {stats.get('min_value')}, Max: {stats.get('max_value')}",
                    ],
                    source_columns=[column_name],
                    x_title=f"{column_name} (bin)",
                    y_title="Count",
                )
            )

        time_col = None
        fallback_query: str | None = None
        if is_temporal:
            time_col = time_expr or col
        elif ctx.time_column:
            time_col = f"TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR({self._quote_ident(ctx.time_column)}))"
            used_time_columns.add(ctx.time_column)
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
            LIMIT 500
            """
            try:
                time_rows = await self.sf.execute_query(time_query)
            except Exception as exc:
                logger.warning("Time series query failed for %s: %s", column_name, exc)
                analysis_errors.append({"step": "time_query", "error": str(exc)})
                time_rows = []
            if is_temporal:
                time_data = [
                    {"time_bucket": row.get("TIME_BUCKET"), "count": self._coerce_int(row.get("COUNT"))}
                    for row in time_rows
                ]
            else:
                time_data = [
                    {"time_bucket": row.get("TIME_BUCKET"), "avg_value": self._coerce_float(row.get("AVG_VALUE"))}
                    for row in time_rows
                ]
            if time_data:
                time_title = column_name if is_temporal else (ctx.time_column or "time")
                y_title = "Count" if is_temporal else f"Average {column_name}"
                visuals.append(
                    self._build_chart_spec(
                        chart_type="line",
                        title=f"{column_name} over time",
                        x_key="time_bucket",
                        y_key="count" if is_temporal else "avg_value",
                        data=time_data,
                        narrative=[
                            "Daily trend based on counts" if is_temporal else "Daily trend based on average values",
                            "Look for seasonality or breaks in trend",
                        ],
                        source_columns=[column_name] if is_temporal else [ctx.time_column, column_name],
                        x_title=time_title,
                        y_title=y_title,
                    )
                )
            elif is_temporal:
                fallback_query = f"""
                WITH base AS (
                    {ctx.analysis_query}
                )
                SELECT TO_VARCHAR({col}) AS category, COUNT(*) AS count
                FROM base
                WHERE {col} IS NOT NULL
                GROUP BY category
                ORDER BY count DESC
                LIMIT 8
                """
                try:
                    fallback_rows = await self.sf.execute_query(fallback_query)
                except Exception as exc:
                    logger.warning("Temporal fallback query failed for %s: %s", column_name, exc)
                    analysis_errors.append({"step": "temporal_fallback_query", "error": str(exc)})
                    fallback_rows = []
                fallback_data = [
                    {"category": row.get("CATEGORY"), "count": self._coerce_int(row.get("COUNT"))}
                    for row in fallback_rows
                ]
                if fallback_data:
                    visuals.append(
                        self._build_chart_spec(
                            chart_type="bar",
                            title=f"{column_name} value distribution",
                            x_key="category",
                            y_key="count",
                            data=fallback_data,
                            narrative=[
                                "Fallback to raw values when timestamps cannot be parsed",
                                "Review data quality for temporal parsing",
                            ],
                            source_columns=[column_name],
                            x_title=column_name,
                            y_title="Count",
                        )
                    )
                    analysis_errors.append({
                        "step": "temporal_parse_fallback",
                        "detail": "No valid timestamps parsed; used raw value distribution.",
                    })

        if not is_temporal:
            temporal_columns = await self._list_temporal_columns(ctx.table_asset_id)
            for temporal_column in temporal_columns:
                if temporal_column in used_time_columns or temporal_column == column_name:
                    continue
                temporal_expr = f"TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR({self._quote_ident(temporal_column)}))"
                time_bucket_expr = f"DATE_TRUNC('day', {temporal_expr})"
                extra_time_query = f"""
                WITH base AS (
                    {ctx.analysis_query}
                )
                SELECT
                    TO_VARCHAR({time_bucket_expr}) AS time_bucket,
                    AVG({col}) AS avg_value
                FROM base
                WHERE {temporal_expr} IS NOT NULL
                AND {col} IS NOT NULL
                GROUP BY {time_bucket_expr}
                ORDER BY time_bucket
                LIMIT 500
                """
                try:
                    extra_rows = await self.sf.execute_query(extra_time_query)
                except Exception as exc:
                    logger.warning("Extra time series query failed for %s: %s", column_name, exc)
                    analysis_errors.append({"step": "extra_time_query", "error": str(exc)})
                    extra_rows = []
                extra_data = [
                    {"time_bucket": row.get("TIME_BUCKET"), "avg_value": self._coerce_float(row.get("AVG_VALUE"))}
                    for row in extra_rows
                ]
                if extra_data:
                    visuals.append(
                        self._build_chart_spec(
                            chart_type="line",
                            title=f"{column_name} by {temporal_column}",
                            x_key="time_bucket",
                            y_key="avg_value",
                            data=extra_data,
                            narrative=[
                                "Daily trend based on average values",
                                f"Grouped by {temporal_column}",
                            ],
                            source_columns=[temporal_column, column_name],
                            x_title=temporal_column,
                            y_title=f"Average {column_name}",
                        )
                    )

        analysis = {
            "visuals": visuals,
            "stats": stats,
            "queries": {
                "stats_query": stats_query.strip(),
            },
        }
        if analysis_errors:
            analysis["errors"] = analysis_errors
        if histogram_data and hist_query:
            analysis["queries"]["hist_query"] = hist_query.strip()
        if time_col:
            analysis["queries"]["time_query"] = time_query.strip()
        if fallback_query and is_temporal:
            analysis["queries"]["temporal_fallback_query"] = fallback_query.strip()
        await self._update_column_analysis(ctx, analysis)

        return {
            "column": column_name,
            "visuals": visuals,
            "stats": stats,
        }

    @tool
    async def generate_categorical_visuals(self, table_asset_id: int, column_name: str) -> dict[str, Any]:
        """Generate visuals for categorical columns and persist them."""
        ctx = await self._load_context(table_asset_id, column_name)
        col = self._quote_ident(column_name)
        analysis_errors: list[dict[str, Any]] = []

        visual_plan = self._extract_visual_overrides(ctx)
        custom_visuals, custom_errors = await self._build_custom_visuals(ctx, visual_plan)
        if custom_visuals is not None:
            if custom_errors:
                analysis_errors.extend(custom_errors)
            analysis = {
                "visuals": custom_visuals,
                "stats": {},
                "queries": {},
            }
            if analysis_errors:
                analysis["errors"] = analysis_errors
            await self._update_column_analysis(ctx, analysis)
            return {"column": column_name, "visuals": custom_visuals, "total_count": 0}

        total_query = f"""
        WITH base AS (
            {ctx.analysis_query}
        )
        SELECT COUNT(*) AS total_count
        FROM base
        WHERE {col} IS NOT NULL
        """
        try:
            total_rows = await self.sf.execute_query(total_query)
        except Exception as exc:
            logger.warning("Total count query failed for %s: %s", column_name, exc)
            analysis_errors.append({"step": "total_query", "error": str(exc)})
            total_rows = []
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
        LIMIT 8
        """
        try:
            top_rows_raw = await self.sf.execute_query(top_query)
        except Exception as exc:
            logger.warning("Top categories query failed for %s: %s", column_name, exc)
            analysis_errors.append({"step": "top_query", "error": str(exc)})
            top_rows_raw = []
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

        if len(top_rows) <= 6:
            visuals.append(
                self._build_chart_spec(
                    chart_type="pie",
                    title=f"Share of {column_name} categories",
                x_key="category",
                y_key="count",
                data=top_rows,
                    narrative=["Pie chart only when categories are limited"],
                    source_columns=[column_name],
                    x_title=column_name,
                    y_title="Count",
                )
            )

        temporal_columns = await self._list_temporal_columns(ctx.table_asset_id)
        for temporal_column in temporal_columns:
            temporal_expr = f"TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR({self._quote_ident(temporal_column)}))"
            time_bucket_expr = f"DATE_TRUNC('day', {temporal_expr})"
            time_query = f"""
            WITH base AS (
                {ctx.analysis_query}
            )
            SELECT
                TO_VARCHAR({time_bucket_expr}) AS time_bucket,
                COUNT(*) AS count
            FROM base
            WHERE {temporal_expr} IS NOT NULL
            AND {col} IS NOT NULL
            GROUP BY {time_bucket_expr}
            ORDER BY time_bucket
            LIMIT 500
            """
            try:
                time_rows = await self.sf.execute_query(time_query)
            except Exception as exc:
                logger.warning("Categorical time series query failed for %s: %s", column_name, exc)
                analysis_errors.append({"step": "categorical_time_query", "error": str(exc)})
                time_rows = []
            time_data = [
                {"time_bucket": row.get("TIME_BUCKET"), "count": self._coerce_int(row.get("COUNT"))}
                for row in time_rows
            ]
            if time_data:
                    visuals.append(
                        self._build_chart_spec(
                            chart_type="line",
                            title=f"{column_name} count by {temporal_column}",
                            x_key="time_bucket",
                            y_key="count",
                            data=time_data,
                            narrative=[
                                "Daily trend based on counts",
                                f"Grouped by {temporal_column}",
                            ],
                            source_columns=[temporal_column, column_name],
                            x_title=temporal_column,
                            y_title="Count",
                        )
                    )

        numeric_columns = [
            name for name in await self._list_numeric_columns(ctx.table_asset_id)
            if name != column_name
        ][:2]
        for numeric_column in numeric_columns:
            num_col = self._quote_ident(numeric_column)
            numeric_query = f"""
            WITH base AS (
                {ctx.analysis_query}
            ), top AS (
                SELECT {col} AS category
                FROM base
                WHERE {col} IS NOT NULL
                GROUP BY category
                ORDER BY COUNT(*) DESC
                LIMIT 8
            )
            SELECT
                base.{col} AS category,
                AVG(base.{num_col}) AS avg_value
            FROM base
            JOIN top ON base.{col} = top.category
            WHERE base.{num_col} IS NOT NULL
            GROUP BY base.{col}
            ORDER BY avg_value DESC
            """
            try:
                numeric_rows = await self.sf.execute_query(numeric_query)
            except Exception as exc:
                logger.warning("Categorical numeric query failed for %s: %s", column_name, exc)
                analysis_errors.append({"step": "categorical_numeric_query", "error": str(exc)})
                numeric_rows = []
            numeric_data = [
                {"category": row.get("CATEGORY"), "avg_value": self._coerce_float(row.get("AVG_VALUE"))}
                for row in numeric_rows
            ]
            if numeric_data:
                visuals.append(
                    self._build_chart_spec(
                        chart_type="bar",
                        title=f"{numeric_column} by {column_name}",
                        x_key="category",
                        y_key="avg_value",
                        data=numeric_data,
                        narrative=[
                            f"Average {numeric_column} across top categories",
                            "Use to compare category-level magnitude",
                        ],
                        source_columns=[column_name, numeric_column],
                        x_title=column_name,
                        y_title=f"Average {numeric_column}",
                    )
                )

        analysis = {
            "visuals": visuals,
            "stats": {"total_count": total_count},
            "queries": {
                "top_query": top_query.strip(),
                "total_query": total_query.strip(),
            },
        }
        if analysis_errors:
            analysis["errors"] = analysis_errors
        await self._update_column_analysis(ctx, analysis)

        return {"column": column_name, "visuals": visuals, "total_count": total_count}

    @tool
    async def generate_numeric_insights(self, table_asset_id: int, column_name: str) -> dict[str, Any]:
        """Generate AI insights for numeric/temporal columns based on visuals."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        payload = {
            "column": column_name,
            "stats": analysis.get("stats"),
            "visuals": analysis.get("visuals", []),
            "structure_type": ctx.structure_type,
        }

        instruction = (
            "Summarize the numeric column insights based on the provided stats and visuals. "
            "Return JSON with keys: insights (array, max 3), caveats (array)."
        )
        focus_hint = overrides.get("insights_focus") or overrides.get("insight_focus")
        user_notes = overrides.get("insights_user_notes") or overrides.get("insight_user_notes")
        if focus_hint:
            instruction += f" Focus on: {focus_hint}."
        if user_notes:
            instruction += f" User notes: {user_notes}."
        token_estimate = await self._estimate_ai_agg_tokens(payload, instruction)
        insights = await self._run_ai_agg(payload, instruction)

        normalized_insights = insights.get("insights", []) if isinstance(insights, dict) else insights
        normalized_caveats = insights.get("caveats", []) if isinstance(insights, dict) else []
        analysis.update(
            {
                "insights": normalized_insights,
                "caveats": normalized_caveats,
                "insight_token_estimate": token_estimate,
            }
        )
        await self._update_column_analysis(ctx, analysis)
        return {
            "column": column_name,
            "insights": normalized_insights,
            "caveats": normalized_caveats,
            "token_estimate": token_estimate,
        }

    @tool
    async def generate_categorical_insights(self, table_asset_id: int, column_name: str) -> dict[str, Any]:
        """Generate AI insights for categorical columns based on visuals."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        payload = {
            "column": column_name,
            "stats": analysis.get("stats"),
            "visuals": analysis.get("visuals", []),
        }

        instruction = (
            "Summarize category distribution insights based on the provided stats and visuals. "
            "Return JSON with keys: insights (array, max 3), caveats (array)."
        )
        focus_hint = overrides.get("insights_focus") or overrides.get("insight_focus")
        user_notes = overrides.get("insights_user_notes") or overrides.get("insight_user_notes")
        if focus_hint:
            instruction += f" Focus on: {focus_hint}."
        if user_notes:
            instruction += f" User notes: {user_notes}."
        token_estimate = await self._estimate_ai_agg_tokens(payload, instruction)
        insights = await self._run_ai_agg(payload, instruction)

        normalized_insights = insights.get("insights", []) if isinstance(insights, dict) else insights
        normalized_caveats = insights.get("caveats", []) if isinstance(insights, dict) else []
        analysis.update(
            {
                "insights": normalized_insights,
                "caveats": normalized_caveats,
                "insight_token_estimate": token_estimate,
            }
        )
        await self._update_column_analysis(ctx, analysis)
        return {
            "column": column_name,
            "insights": normalized_insights,
            "caveats": normalized_caveats,
            "token_estimate": token_estimate,
        }

    @tool
    async def analyze_numeric_correlations(
        self,
        table_asset_id: int,
        column_name: str,
        sample_size: int = 5000,
        max_columns: int = 12,
        window_days: int | None = None,
        positive_threshold: float = 0.3,
        weak_threshold: float = 0.1,
        compare_sample_size: int | None = 2000,
        compare_window_days: int | None = None,
    ) -> dict[str, Any]:
        """Detect correlations between a numeric column and other numeric columns.

        Categorizes correlated columns into positive, negative, and weak groups.
        Uses windowed sampling when a temporal column is available to limit cost.
        """
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        override_sample_size = overrides.get("numeric_correlations_sample_size")
        if override_sample_size is not None:
            sample_size = self._coerce_int(override_sample_size) or sample_size
        override_max_columns = overrides.get("numeric_correlations_max_columns")
        if override_max_columns is not None:
            max_columns = self._coerce_int(override_max_columns) or max_columns
        override_window_days = overrides.get("numeric_correlations_window_days")
        if override_window_days is not None:
            window_days = self._coerce_int(override_window_days)
        override_positive_threshold = overrides.get("numeric_correlations_positive_threshold")
        if override_positive_threshold is not None:
            positive_threshold = self._coerce_float(override_positive_threshold) or positive_threshold
        override_weak_threshold = overrides.get("numeric_correlations_weak_threshold")
        if override_weak_threshold is not None:
            weak_threshold = self._coerce_float(override_weak_threshold) or weak_threshold
        override_compare_sample = overrides.get("numeric_correlations_compare_sample_size")
        if override_compare_sample is not None:
            compare_sample_size = self._coerce_int(override_compare_sample)
        override_compare_window = overrides.get("numeric_correlations_compare_window_days")
        if override_compare_window is not None:
            compare_window_days = self._coerce_int(override_compare_window)
        numeric_columns = [name for name in await self._list_numeric_columns(table_asset_id) if name != column_name]
        if not numeric_columns:
            return {"column": column_name, "correlations": [], "skipped": True}

        limit = len(numeric_columns)
        if isinstance(max_columns, int) and max_columns > 0:
            limit = max_columns
        candidates = numeric_columns[:limit]
        col_expr = self._numeric_expr(self._quote_ident(column_name))

        async def fetch_correlations(sample_limit: int | None, window_limit: int | None) -> tuple[list[dict[str, Any]], int | None]:
            select_parts = ["COUNT(*) AS sample_count"]
            alias_map: dict[str, str] = {}
            for other in candidates:
                other_expr = self._numeric_expr(self._quote_ident(other))
                alias = f"corr_{self._normalize_identifier(other)}"
                alias_map[alias] = other
                select_parts.append(f"CORR({col_expr}, {other_expr}) AS {alias}")

            base_query = self._build_windowed_query(ctx, sample_limit, window_limit, ctx.time_column)
            query = f"""
            WITH base AS (
                {base_query}
            )
            SELECT
                {", ".join(select_parts)}
            FROM base
            WHERE {col_expr} IS NOT NULL
            """
            rows = await self.sf.execute_query(query)
            row = rows[0] if rows else {}
            corr_list: list[dict[str, Any]] = []
            for alias, other in alias_map.items():
                raw_value = row.get(alias.upper())
                if raw_value is None:
                    raw_value = row.get(alias)
                corr_value = self._coerce_float(raw_value)
                if corr_value is None:
                    continue
                corr_list.append({"column": other, "correlation": corr_value})
            return corr_list, self._coerce_int(row.get("SAMPLE_COUNT"))

        try:
            correlations, sample_count = await fetch_correlations(sample_size, window_days)
        except Exception as exc:
            analysis_update = {
                "correlations": {
                    "sample_size": sample_size,
                    "window_days": window_days,
                    "positive_threshold": positive_threshold,
                    "weak_threshold": weak_threshold,
                    "positive": [],
                    "negative": [],
                    "weak": [],
                    "all": [],
                    "confidence": None,
                    "error": str(exc),
                }
            }
            await self._update_column_analysis(ctx, analysis_update)
            return {
                "column": column_name,
                "sample_count": None,
                "correlations": analysis_update["correlations"],
            }

        correlations.sort(key=lambda item: abs(float(item["correlation"])), reverse=True)
        positive = [item for item in correlations if item["correlation"] >= positive_threshold]
        negative = [item for item in correlations if item["correlation"] <= -positive_threshold]
        weak = [item for item in correlations if abs(item["correlation"]) <= weak_threshold]

        confidence: dict[str, Any] | None = None
        if compare_sample_size and compare_sample_size > 0:
            secondary, secondary_count = await fetch_correlations(compare_sample_size, compare_window_days)
            primary_map = {item["column"]: item["correlation"] for item in correlations}
            secondary_map = {item["column"]: item["correlation"] for item in secondary}
            shared = set(primary_map.keys()) & set(secondary_map.keys())
            if shared:
                def classify(value: float | None) -> str:
                    if value is None:
                        return "unknown"
                    if value >= positive_threshold:
                        return "positive"
                    if value <= -positive_threshold:
                        return "negative"
                    if abs(value) <= weak_threshold:
                        return "weak"
                    return "mixed"

                matches = sum(
                    1 for col in shared if classify(primary_map.get(col)) == classify(secondary_map.get(col))
                )
                confidence = {
                    "agreement_rate": round(matches / len(shared), 4),
                    "primary_sample": sample_count,
                    "secondary_sample": secondary_count,
                    "shared_columns": len(shared),
                }

        analysis_update = {
            "correlations": {
                "sample_size": sample_size,
                "window_days": window_days,
                "positive_threshold": positive_threshold,
                "weak_threshold": weak_threshold,
                "positive": positive,
                "negative": negative,
                "weak": weak,
                "all": correlations,
                "confidence": confidence,
            }
        }
        await self._update_column_analysis(ctx, analysis_update)

        return {
            "column": column_name,
            "sample_count": sample_count,
            "correlations": analysis_update["correlations"],
        }

    @tool
    async def analyze_numeric_distribution(
        self,
        table_asset_id: int,
        column_name: str,
        sample_size: int = 10000,
        window_days: int | None = None,
        time_column: str | None = None,
    ) -> dict[str, Any]:
        """Compute numeric distribution statistics and percentiles."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        override_sample_size = overrides.get("numeric_distribution_sample_size")
        if override_sample_size is not None:
            sample_size = self._coerce_int(override_sample_size) or sample_size
        override_window_days = overrides.get("numeric_distribution_window_days")
        if override_window_days is not None:
            window_days = self._coerce_int(override_window_days)
        override_time_column = overrides.get("numeric_distribution_time_column")
        if override_time_column:
            time_column = str(override_time_column)
        target_time_column = time_column or ctx.time_column
        col_expr = self._numeric_expr(self._quote_ident(column_name))
        base_query = self._build_windowed_query(ctx, sample_size, window_days, target_time_column)

        stats_query = f"""
        WITH base AS (
            {base_query}
        )
        SELECT
            COUNT(*) AS total_count,
            COUNT_IF({col_expr} IS NULL) AS null_count,
            MIN({col_expr}) AS min_value,
            MAX({col_expr}) AS max_value,
            AVG({col_expr}) AS mean_value,
            STDDEV({col_expr}) AS stddev_value,
            APPROX_PERCENTILE({col_expr}, 0.1) AS p10,
            APPROX_PERCENTILE({col_expr}, 0.5) AS p50,
            APPROX_PERCENTILE({col_expr}, 0.9) AS p90
        FROM base
        WHERE {col_expr} IS NOT NULL
        """
        try:
            stats_rows = await self.sf.execute_query(stats_query)
        except Exception as exc:
            analysis_update = {"distribution": {"error": str(exc)}}
            await self._update_column_analysis(ctx, analysis_update)
            return {"column": column_name, "distribution": analysis_update["distribution"]}
        stats_row = stats_rows[0] if stats_rows else {}
        distribution = {
            "min_value": stats_row.get("MIN_VALUE"),
            "max_value": stats_row.get("MAX_VALUE"),
            "mean_value": stats_row.get("MEAN_VALUE"),
            "stddev_value": stats_row.get("STDDEV_VALUE"),
            "p10": stats_row.get("P10"),
            "p50": stats_row.get("P50"),
            "p90": stats_row.get("P90"),
            "total_count": self._coerce_int(stats_row.get("TOTAL_COUNT")),
            "null_count": self._coerce_int(stats_row.get("NULL_COUNT")),
        }

        distribution_shape = "unknown"
        p10 = self._coerce_float(distribution.get("p10"))
        p50 = self._coerce_float(distribution.get("p50"))
        p90 = self._coerce_float(distribution.get("p90"))
        if p10 is not None and p50 is not None and p90 is not None:
            left = p50 - p10
            right = p90 - p50
            if left > right * 1.5:
                distribution_shape = "left_skewed"
            elif right > left * 1.5:
                distribution_shape = "right_skewed"
            else:
                distribution_shape = "roughly_symmetric"
        distribution["shape"] = distribution_shape

        await self._update_column_analysis(ctx, {"distribution": distribution})
        return {"column": column_name, "distribution": distribution}

    @tool
    async def analyze_numeric_periodicity(
        self,
        table_asset_id: int,
        column_name: str,
        bucket: str = "day",
        sample_size: int = 10000,
        window_days: int | None = 180,
        time_column: str | None = None,
        periodicity_threshold: float = 0.4,
        compare_sample_size: int | None = 2000,
        compare_window_days: int | None = None,
    ) -> dict[str, Any]:
        """Analyze numeric periodicity against temporal columns."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        override_bucket = overrides.get("numeric_periodicity_bucket")
        if override_bucket:
            bucket = str(override_bucket)
        override_sample_size = overrides.get("numeric_periodicity_sample_size")
        if override_sample_size is not None:
            sample_size = self._coerce_int(override_sample_size) or sample_size
        override_window_days = overrides.get("numeric_periodicity_window_days")
        if override_window_days is not None:
            window_days = self._coerce_int(override_window_days)
        override_time_column = overrides.get("numeric_periodicity_time_column")
        if override_time_column:
            time_column = str(override_time_column)
        override_threshold = overrides.get("numeric_periodicity_threshold")
        if override_threshold is not None:
            periodicity_threshold = self._coerce_float(override_threshold) or periodicity_threshold
        override_compare_sample = overrides.get("numeric_periodicity_compare_sample_size")
        if override_compare_sample is not None:
            compare_sample_size = self._coerce_int(override_compare_sample)
        override_compare_window = overrides.get("numeric_periodicity_compare_window_days")
        if override_compare_window is not None:
            compare_window_days = self._coerce_int(override_compare_window)
        target_time_column = time_column or ctx.time_column
        bucket_key = str(bucket or "day").lower()
        if bucket_key not in {"hour", "day", "week", "month"}:
            bucket_key = "day"
        if not target_time_column:
            temporal_columns = await self._list_temporal_columns(table_asset_id)
            target_time_column = temporal_columns[0] if temporal_columns else None

        col_expr = self._numeric_expr(self._quote_ident(column_name))
        base_query = self._build_windowed_query(ctx, sample_size, window_days, target_time_column)

        periodicity: dict[str, Any] = {"detected": False, "bucket": bucket_key, "time_column": target_time_column}
        if target_time_column:
            time_expr = self._resolve_temporal_expr(ctx, self._quote_ident(target_time_column))
            lags = self._periodicity_lags(bucket_key)
            lag_exprs = ",\n                ".join(
                f"LAG(value, {lag}) OVER (ORDER BY bucket) AS lag_{lag}" for lag in lags
            )
            corr_exprs = ",\n            ".join(
                f"CORR(value, lag_{lag}) AS corr_lag_{lag}" for lag in lags
            )

            async def compute_periodicity(query_base: str) -> dict[str, Any]:
                periodicity_query = f"""
                WITH base AS (
                    {query_base}
                ),
                series AS (
                    SELECT
                        DATE_TRUNC('{bucket_key}', {time_expr}) AS bucket,
                        AVG({col_expr}) AS value
                    FROM base
                    WHERE {time_expr} IS NOT NULL AND {col_expr} IS NOT NULL
                    GROUP BY 1
                ),
                lagged AS (
                    SELECT
                        bucket,
                        value,
                        {lag_exprs}
                    FROM series
                )
                SELECT
                    {corr_exprs}
                FROM lagged
                """
                try:
                    periodicity_rows = await self.sf.execute_query(periodicity_query)
                except Exception as exc:
                    return {
                        "lag_correlations": [],
                        "dominant_period": None,
                        "strength": None,
                        "detected": False,
                        "error": str(exc),
                    }
                periodicity_row = periodicity_rows[0] if periodicity_rows else {}
                lag_correlations: list[dict[str, Any]] = []
                for lag in lags:
                    key = f"CORR_LAG_{lag}"
                    corr_value = self._coerce_float(periodicity_row.get(key))
                    lag_correlations.append({"lag": lag, "correlation": corr_value})
                best = max(lag_correlations, key=lambda item: abs(item.get("correlation") or 0.0), default=None)
                result: dict[str, Any] = {
                    "lag_correlations": lag_correlations,
                    "dominant_period": best.get("lag") if best else None,
                    "strength": best.get("correlation") if best else None,
                    "detected": False,
                }
                if best and best.get("correlation") is not None:
                    result["detected"] = abs(best.get("correlation") or 0.0) >= periodicity_threshold
                    result["threshold"] = periodicity_threshold
                return result

            periodicity.update(await compute_periodicity(base_query))

            if compare_sample_size and compare_sample_size > 0:
                compare_window = compare_window_days if compare_window_days is not None else window_days
                compare_query = self._build_windowed_query(ctx, compare_sample_size, compare_window, target_time_column)
                secondary = await compute_periodicity(compare_query)
                confidence = {
                    "primary_detected": periodicity.get("detected"),
                    "secondary_detected": secondary.get("detected"),
                    "primary_period": periodicity.get("dominant_period"),
                    "secondary_period": secondary.get("dominant_period"),
                    "primary_strength": periodicity.get("strength"),
                    "secondary_strength": secondary.get("strength"),
                    "agreement": (
                        periodicity.get("detected") == secondary.get("detected")
                        and periodicity.get("dominant_period") == secondary.get("dominant_period")
                    ),
                }
                periodicity["confidence"] = confidence
        else:
            periodicity["reason"] = "no_temporal_column"

        analysis_update = {"periodicity": periodicity}
        await self._update_column_analysis(ctx, analysis_update)

        return {
            "column": column_name,
            "periodicity": periodicity,
        }

    @tool
    async def analyze_categorical_groups(
        self,
        table_asset_id: int,
        column_name: str,
        top_n: int = 10,
        sample_size: int = 20000,
    ) -> dict[str, Any]:
        """Group categorical values into head + tail buckets for faster review."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        override_top_n = overrides.get("categorical_groups_top_n")
        if override_top_n is not None:
            top_n = self._coerce_int(override_top_n) or top_n
        override_sample_size = overrides.get("categorical_groups_sample_size")
        if override_sample_size is not None:
            sample_size = self._coerce_int(override_sample_size) or sample_size
        col = self._quote_ident(column_name)
        base_query = self._build_windowed_query(ctx, sample_size, None, None)

        head_query = f"""
        WITH base AS (
            {base_query}
        )
        SELECT
            {col} AS category,
            COUNT(*) AS count
        FROM base
        WHERE {col} IS NOT NULL
        GROUP BY 1
        ORDER BY count DESC
        LIMIT {int(top_n)}
        """
        head_rows = await self.sf.execute_query(head_query)

        total_query = f"""
        WITH base AS (
            {base_query}
        )
        SELECT
            COUNT(*) AS total_count,
            COUNT(DISTINCT {col}) AS distinct_count
        FROM base
        WHERE {col} IS NOT NULL
        """
        total_rows = await self.sf.execute_query(total_query)
        totals = total_rows[0] if total_rows else {}
        total_count = self._coerce_int(totals.get("TOTAL_COUNT")) or 0
        head_count = sum(self._coerce_int(row.get("COUNT")) or 0 for row in head_rows)
        tail_count = max(0, total_count - head_count)

        groups = {
            "top_categories": [
                {"category": row.get("CATEGORY"), "count": self._coerce_int(row.get("COUNT"))}
                for row in head_rows
            ],
            "tail_count": tail_count,
            "distinct_count": self._coerce_int(totals.get("DISTINCT_COUNT")),
            "coverage": round(head_count / total_count, 4) if total_count else None,
        }

        await self._update_column_analysis(ctx, {"category_groups": groups})
        return {"column": column_name, "category_groups": groups}

    @tool
    async def scan_nulls(
        self,
        table_asset_id: int,
        column_name: str,
        sample_size: int = 20000,
        window_days: int | None = None,
        time_column: str | None = None,
    ) -> dict[str, Any]:
        """Scan null distribution for a column."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        override_sample_size = overrides.get("scan_nulls_sample_size")
        if override_sample_size is not None:
            sample_size = self._coerce_int(override_sample_size) or sample_size
        override_window_days = overrides.get("scan_nulls_window_days")
        if override_window_days is not None:
            window_days = self._coerce_int(override_window_days)
        override_time_column = overrides.get("scan_nulls_time_column")
        if override_time_column:
            time_column = str(override_time_column)
        target_time_column = time_column or ctx.time_column
        col = self._quote_ident(column_name)
        base_query = self._build_windowed_query(ctx, sample_size, window_days, target_time_column)
        query = f"""
        WITH base AS (
            {base_query}
        )
        SELECT
            COUNT(*) AS total_count,
            COUNT_IF({col} IS NULL) AS null_count
        FROM base
        """
        try:
            rows = await self.sf.execute_query(query)
        except Exception as exc:
            analysis_update = {"nulls": {"error": str(exc)}}
            await self._update_column_analysis(ctx, analysis_update)
            return {"column": column_name, "nulls": analysis_update["nulls"]}
        row = rows[0] if rows else {}
        total_count = self._coerce_int(row.get("TOTAL_COUNT"))
        null_count = self._coerce_int(row.get("NULL_COUNT"))
        null_rate = round(null_count / total_count, 6) if total_count else None
        nulls = {
            "total_count": total_count,
            "null_count": null_count,
            "null_rate": null_rate,
        }
        await self._update_column_analysis(ctx, {"nulls": nulls})
        return {"column": column_name, "nulls": nulls}

    @tool
    async def scan_conflicts(
        self,
        table_asset_id: int,
        column_name: str,
        group_by_columns: list[str] | str | None = None,
        sample_size: int = 20000,
        window_days: int | None = None,
        time_column: str | None = None,
    ) -> dict[str, Any]:
        """Detect conflicting values within groups defined by other columns."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        group_by = group_by_columns or overrides.get("conflict_group_columns") or []
        override_sample_size = overrides.get("scan_conflicts_sample_size")
        if override_sample_size is not None:
            sample_size = self._coerce_int(override_sample_size) or sample_size
        override_window_days = overrides.get("scan_conflicts_window_days")
        if override_window_days is not None:
            window_days = self._coerce_int(override_window_days)
        override_time_column = overrides.get("scan_conflicts_time_column")
        if override_time_column:
            time_column = str(override_time_column)
        if isinstance(group_by, str):
            group_by = [item.strip() for item in group_by.split(",") if item.strip()]
        group_by = [str(item) for item in group_by if item]
        if not group_by:
            conflicts = {"skipped": True, "reason": "group_by_columns_missing"}
            await self._update_column_analysis(ctx, {"conflicts": conflicts})
            return {"column": column_name, "conflicts": conflicts}

        target_time_column = time_column or ctx.time_column
        base_query = self._build_windowed_query(ctx, sample_size, window_days, target_time_column)
        col = self._quote_ident(column_name)
        group_exprs = ", ".join(self._quote_ident(name) for name in group_by)
        query = f"""
        WITH base AS (
            {base_query}
        ),
        grouped AS (
            SELECT
                {group_exprs},
                COUNT(DISTINCT {col}) AS distinct_values
            FROM base
            WHERE {col} IS NOT NULL
            GROUP BY {group_exprs}
        )
        SELECT
            COUNT(*) AS group_count,
            SUM(CASE WHEN distinct_values > 1 THEN 1 ELSE 0 END) AS conflict_groups,
            MAX(distinct_values) AS max_distinct
        FROM grouped
        """
        try:
            rows = await self.sf.execute_query(query)
        except Exception as exc:
            conflicts = {"error": str(exc), "group_by_columns": group_by}
            await self._update_column_analysis(ctx, {"conflicts": conflicts})
            return {"column": column_name, "conflicts": conflicts}
        row = rows[0] if rows else {}
        group_count = self._coerce_int(row.get("GROUP_COUNT"))
        conflict_groups = self._coerce_int(row.get("CONFLICT_GROUPS"))
        conflict_rate = round(conflict_groups / group_count, 6) if group_count else None
        conflicts = {
            "group_by_columns": group_by,
            "group_count": group_count,
            "conflict_groups": conflict_groups,
            "conflict_rate": conflict_rate,
            "max_distinct": self._coerce_int(row.get("MAX_DISTINCT")),
        }
        await self._update_column_analysis(ctx, {"conflicts": conflicts})
        return {"column": column_name, "conflicts": conflicts}

    @tool
    async def plan_data_repairs(
        self,
        table_asset_id: int,
        column_name: str,
        null_strategy: str | None = None,
        conflict_strategy: str | None = None,
    ) -> dict[str, Any]:
        """Create a repair plan for null/conflict handling without applying changes."""
        ctx = await self._load_context(table_asset_id, column_name)
        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        nulls = analysis.get("nulls", {})
        conflicts = analysis.get("conflicts", {})
        overrides = ctx.column_meta.overrides or {}
        table_overrides = ctx.table_meta.overrides or {}

        strategy_defaults = {
            "numeric": "median_impute",
            "temporal": "forward_fill",
            "categorical": "mode_impute",
            "text": "empty_string",
        }
        strategy = (
            null_strategy
            or overrides.get("null_strategy")
            or strategy_defaults.get(ctx.column_meta.semantic_type, "manual_review")
        )
        conflict_plan = conflict_strategy or overrides.get("conflict_strategy") or "manual_review"

        group_by = conflicts.get("group_by_columns") or overrides.get("conflict_group_columns") or []
        if isinstance(group_by, str):
            group_by = [item.strip() for item in group_by.split(",") if item.strip()]
        group_by = [str(item) for item in group_by if item]

        snapshot = await self._compute_snapshot(ctx, column_name, group_by)
        conflict_groups = self._coerce_int(conflicts.get("conflict_groups"))
        snapshot["conflict_groups"] = conflict_groups

        total_count = self._coerce_int(snapshot.get("total_count")) or self._coerce_int(
            nulls.get("total_count")
        )
        null_count = self._coerce_int(snapshot.get("null_count")) or self._coerce_int(
            nulls.get("null_count")
        )
        conflict_rows = snapshot.get("conflict_rows")

        row_id_column = self._resolve_row_id_column(ctx)
        audit_table = (
            overrides.get("repair_audit_table")
            or overrides.get("audit_table")
            or table_overrides.get("repair_audit_table")
            or table_overrides.get("audit_table")
        )
        apply_ready = bool(ctx.table_ref) and bool(row_id_column)
        rollback = (
            {"strategy": "audit_table", "audit_table": audit_table}
            if audit_table
            else {"strategy": "time_travel" if ctx.table_ref else "none"}
        )

        plan_steps = []
        sql_previews: dict[str, Any] = {}
        if null_count:
            fill_expr, fill_value = await self._compute_null_fill_value(ctx, column_name, strategy)
            update_sql = None
            count_sql = None
            if ctx.table_ref and fill_expr:
                update_sql = f"UPDATE {ctx.table_ref} SET {self._quote_ident(column_name)} = {fill_expr} WHERE {self._quote_ident(column_name)} IS NULL"
                count_sql = f"SELECT COUNT(*) AS affected_rows FROM {ctx.table_ref} WHERE {self._quote_ident(column_name)} IS NULL"
            plan_steps.append(
                {
                    "type": "null_repair",
                    "strategy": strategy,
                    "estimated_rows": null_count,
                    "fill_expr": fill_expr,
                    "fill_value": fill_value,
                }
            )
            sql_previews["null_repair"] = {
                "update_sql": update_sql,
                "count_sql": count_sql,
                "estimated_rows": null_count,
            }

        if conflict_groups:
            plan_steps.append(
                {
                    "type": "conflict_repair",
                    "strategy": conflict_plan,
                    "estimated_groups": conflict_groups,
                    "estimated_rows": conflict_rows,
                    "group_by_columns": group_by,
                }
            )
            if ctx.table_ref and group_by:
                group_exprs = ", ".join(self._quote_ident(name) for name in group_by)
                strategy_key = str(conflict_plan).lower()
                if strategy_key in {"mean_per_group", "mean"}:
                    agg_expr = f"AVG({self._numeric_expr(self._quote_ident(column_name))})"
                elif strategy_key in {"median_per_group", "median"}:
                    agg_expr = f"APPROX_PERCENTILE({self._numeric_expr(self._quote_ident(column_name))}, 0.5)"
                else:
                    agg_expr = None
                if agg_expr:
                    update_sql = f"""
WITH base AS ({ctx.analysis_query}),
grouped AS (
    SELECT {group_exprs}, {agg_expr} AS target_value
    FROM base
    WHERE {self._quote_ident(column_name)} IS NOT NULL
    GROUP BY {group_exprs}
    HAVING COUNT(DISTINCT {self._quote_ident(column_name)}) > 1
)
UPDATE {ctx.table_ref} AS tgt
SET {self._quote_ident(column_name)} = grouped.target_value
FROM grouped
WHERE {" AND ".join([f"tgt.{self._quote_ident(name)} = grouped.{self._quote_ident(name)}" for name in group_by])}
""".strip()
                else:
                    update_sql = f"""
WITH base AS ({ctx.analysis_query}),
counts AS (
    SELECT {group_exprs}, {self._quote_ident(column_name)} AS value, COUNT(*) AS value_count
    FROM base
    WHERE {self._quote_ident(column_name)} IS NOT NULL
    GROUP BY {group_exprs}, {self._quote_ident(column_name)}
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY {group_exprs} ORDER BY value_count DESC) AS rn
    FROM counts
),
winners AS (
    SELECT {group_exprs}, value AS target_value
    FROM ranked
    WHERE rn = 1
)
UPDATE {ctx.table_ref} AS tgt
SET {self._quote_ident(column_name)} = winners.target_value
FROM winners
WHERE {" AND ".join([f"tgt.{self._quote_ident(name)} = winners.{self._quote_ident(name)}" for name in group_by])}
""".strip()
                count_sql = f"""
WITH base AS ({ctx.analysis_query}),
grouped AS (
    SELECT {group_exprs}, COUNT(DISTINCT {self._quote_ident(column_name)}) AS distinct_values
    FROM base
    WHERE {self._quote_ident(column_name)} IS NOT NULL
    GROUP BY {group_exprs}
),
conflict_groups AS (
    SELECT {group_exprs}
    FROM grouped
    WHERE distinct_values > 1
)
SELECT COUNT(*) AS affected_rows
FROM base
JOIN conflict_groups
ON {" AND ".join([f"base.{self._quote_ident(name)} = conflict_groups.{self._quote_ident(name)}" for name in group_by])}
""".strip()
                sql_previews["conflict_repair"] = {
                    "update_sql": update_sql,
                    "count_sql": count_sql,
                    "estimated_rows": conflict_rows,
                    "group_by_columns": group_by,
                }

        token_estimate = {
            "token_count": 0,
            "estimated_rows": null_count,
            "method": "sql_only",
        }

        summary_parts = []
        if null_count:
            summary_parts.append(f"Null repair: {strategy} for ~{null_count} rows")
        if conflict_groups:
            summary_parts.append(
                f"Conflict repair: {conflict_plan} for {conflict_groups} groups"
            )
        if not summary_parts:
            summary_parts.append("No repair actions required")

        plan_id = uuid.uuid4().hex
        plan_payload = {
            "summary": "; ".join(summary_parts),
            "steps": plan_steps,
            "snapshot": snapshot,
            "row_id_column": row_id_column,
            "apply_ready": apply_ready,
            "rollback": rollback,
            "sql_previews": sql_previews,
            "token_estimate": token_estimate,
            "approval_required": True,
        }
        plan_hash = self._hash_payload(plan_payload)
        plan = {
            "plan_id": plan_id,
            "plan_hash": plan_hash,
            "created_at": datetime.utcnow().isoformat(),
            **plan_payload,
        }
        await self._update_column_analysis(ctx, {"repair_plan": plan})
        return {"column": column_name, "repair_plan": plan}

    @tool
    async def require_user_approval(
        self,
        table_asset_id: int,
        column_name: str,
        approved: bool | None = None,
        approval_key: str = "data_fix_approved",
    ) -> dict[str, Any]:
        """Gate data optimization steps behind explicit user approval."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        is_approved = bool(approved if approved is not None else overrides.get(approval_key))
        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        plan = dict(analysis.get("repair_plan", {}))
        approved_plan_id = overrides.get("data_fix_plan_id")
        approved_plan_hash = overrides.get("data_fix_plan_hash")
        approved_snapshot = overrides.get("data_fix_snapshot_signature")
        approval_match = True
        if approved_plan_id or approved_plan_hash or approved_snapshot:
            approval_match = (
                approved_plan_id == plan.get("plan_id")
                and approved_plan_hash == plan.get("plan_hash")
                and approved_snapshot == (plan.get("snapshot") or {}).get("signature")
            )
        plan["approval_status"] = "approved" if is_approved and approval_match else "pending"
        plan["approved"] = bool(is_approved and approval_match)
        plan["approved_plan_id"] = approved_plan_id
        plan["approved_plan_hash"] = approved_plan_hash
        plan["approved_snapshot_signature"] = approved_snapshot
        plan["approval_match"] = approval_match
        await self._update_column_analysis(ctx, {"repair_plan": plan})
        return {"column": column_name, "approved": is_approved, "approval_key": approval_key}

    @tool
    async def apply_data_repairs(
        self,
        table_asset_id: int,
        column_name: str,
        null_strategy: str | None = None,
        conflict_strategy: str | None = None,
        approval_key: str = "data_fix_approved",
    ) -> dict[str, Any]:
        """Apply approved null/conflict repair strategies to the source table."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        approved = bool(overrides.get(approval_key))
        if not approved:
            return {"column": column_name, "skipped": True, "reason": "approval_required"}
        if not ctx.table_ref:
            return {"column": column_name, "skipped": True, "reason": "table_ref_missing"}

        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        nulls = analysis.get("nulls", {})
        conflicts = analysis.get("conflicts", {})
        plan = analysis.get("repair_plan", {})

        async def record_skip(reason: str) -> dict[str, Any]:
            plan_update = dict(plan) if isinstance(plan, dict) else {}
            plan_update["apply_skipped_reason"] = reason
            await self._update_column_analysis(
                ctx,
                {
                    "repair_results": [
                        {"type": "repair_apply", "status": "skipped", "reason": reason}
                    ],
                    "repair_plan": plan_update,
                },
            )
            return {"column": column_name, "skipped": True, "reason": reason}

        if not plan:
            return await record_skip("plan_missing")

        approved_plan_id = overrides.get("data_fix_plan_id")
        approved_plan_hash = overrides.get("data_fix_plan_hash")
        approved_snapshot = overrides.get("data_fix_snapshot_signature")
        if not (approved_plan_id and approved_plan_hash and approved_snapshot):
            return await record_skip("approval_missing_plan_info")
        if (
            approved_plan_id != plan.get("plan_id")
            or approved_plan_hash != plan.get("plan_hash")
            or approved_snapshot != (plan.get("snapshot") or {}).get("signature")
        ):
            return await record_skip("approval_plan_mismatch")

        group_by = (plan.get("snapshot") or {}).get("group_by_columns") or conflicts.get(
            "group_by_columns"
        ) or overrides.get("conflict_group_columns") or []
        if isinstance(group_by, str):
            group_by = [item.strip() for item in group_by.split(",") if item.strip()]
        group_by = [str(item) for item in group_by if item]
        current_snapshot = await self._compute_snapshot(ctx, column_name, group_by)
        if current_snapshot.get("signature") != (plan.get("snapshot") or {}).get("signature"):
            return await record_skip("snapshot_mismatch")

        row_id_column = plan.get("row_id_column") or self._resolve_row_id_column(ctx)
        if not row_id_column:
            return await record_skip("row_id_column_missing")

        planned_null_strategy = null_strategy or overrides.get("null_strategy")
        planned_conflict_strategy = conflict_strategy or overrides.get("conflict_strategy")
        if not planned_null_strategy:
            for step in plan.get("steps", []):
                if step.get("type") == "null_repair":
                    planned_null_strategy = step.get("strategy")
                    break
        if not planned_conflict_strategy:
            for step in plan.get("steps", []):
                if step.get("type") == "conflict_repair":
                    planned_conflict_strategy = step.get("strategy")
                    break

        col = self._quote_ident(column_name)

        repair_results: list[dict[str, Any]] = []
        sql_previews = plan.get("sql_previews") or {}
        dry_run = bool(overrides.get("data_fix_dry_run") or overrides.get("repair_dry_run"))
        audit_table = (plan.get("rollback") or {}).get("audit_table") or overrides.get(
            "repair_audit_table"
        )
        if audit_table:
            create_audit = f"""
            CREATE TABLE IF NOT EXISTS {audit_table} (
                plan_id STRING,
                column_name STRING,
                repair_type STRING,
                row_id STRING,
                before_value VARIANT,
                after_value VARIANT,
                created_at TIMESTAMP_LTZ
            )
            """
            await self.sf.execute_query(create_audit)

        null_count = self._coerce_int(nulls.get("null_count"))
        if null_count and planned_null_strategy:
            null_step = next(
                (step for step in plan.get("steps", []) if step.get("type") == "null_repair"),
                {},
            )
            fill_expr = null_step.get("fill_expr")
            fill_value = null_step.get("fill_value")
            preview = sql_previews.get("null_repair") or {}
            update_query = preview.get("update_sql")
            if fill_expr is None or not update_query:
                repair_results.append(
                    {
                        "type": "null_repair",
                        "status": "skipped",
                        "strategy": planned_null_strategy,
                        "reason": "fill_value_unavailable",
                    }
                )
            elif dry_run:
                repair_results.append(
                    {
                        "type": "null_repair",
                        "status": "dry_run",
                        "strategy": planned_null_strategy,
                        "fill_value": fill_value,
                        "targeted_rows": null_count,
                        "update_sql": update_query,
                        "count_sql": preview.get("count_sql"),
                    }
                )
            else:
                if audit_table:
                    audit_insert = f"""
                    INSERT INTO {audit_table} (plan_id, column_name, repair_type, row_id, before_value, after_value, created_at)
                    SELECT '{self._sanitize_literal(plan.get("plan_id", ""))}', '{self._sanitize_literal(column_name)}', 'null_repair',
                           TO_VARCHAR({self._quote_ident(row_id_column)}),
                           TO_VARIANT({col}),
                           TO_VARIANT({fill_expr}),
                           CURRENT_TIMESTAMP()
                    FROM {ctx.table_ref}
                    WHERE {col} IS NULL
                    """
                    await self.sf.execute_query(audit_insert)
                await self.sf.execute_query(update_query)
                repair_results.append(
                    {
                        "type": "null_repair",
                        "status": "applied",
                        "strategy": planned_null_strategy,
                        "fill_value": fill_value,
                        "targeted_rows": null_count,
                    }
                )

        conflict_groups = self._coerce_int(conflicts.get("conflict_groups"))
        if conflict_groups and group_by and planned_conflict_strategy:
            group_exprs = ", ".join(self._quote_ident(name) for name in group_by)
            strategy_key = str(planned_conflict_strategy).lower()
            if strategy_key in {"manual_review", "manual"}:
                repair_results.append(
                    {
                        "type": "conflict_repair",
                        "status": "skipped",
                        "strategy": planned_conflict_strategy,
                        "reason": "manual_review",
                    }
                )
                applied_any = any(item.get("status") == "applied" for item in repair_results)
                plan_update = dict(plan) if isinstance(plan, dict) else {}
                if applied_any:
                    plan_update["applied"] = True
                    plan_update["applied_at"] = datetime.utcnow().isoformat()
                analysis_update = {"repair_results": repair_results, "repair_plan": plan_update}
                await self._update_column_analysis(ctx, analysis_update)
                return {
                    "column": column_name,
                    "repairs": repair_results,
                    "approved": approved,
                }
            preview = sql_previews.get("conflict_repair") or {}
            update_query = preview.get("update_sql")
            if not update_query:
                repair_results.append(
                    {
                        "type": "conflict_repair",
                        "status": "skipped",
                        "strategy": planned_conflict_strategy,
                        "reason": "update_sql_missing",
                    }
                )
            elif dry_run:
                repair_results.append(
                    {
                        "type": "conflict_repair",
                        "status": "dry_run",
                        "strategy": planned_conflict_strategy,
                        "group_by_columns": group_by,
                        "targeted_groups": conflict_groups,
                        "update_sql": update_query,
                        "count_sql": preview.get("count_sql"),
                    }
                )
            else:
                if audit_table:
                    if strategy_key in {"mean_per_group", "mean"}:
                        agg_expr = f"AVG({self._numeric_expr(self._quote_ident(column_name))})"
                    elif strategy_key in {"median_per_group", "median"}:
                        agg_expr = f"APPROX_PERCENTILE({self._numeric_expr(self._quote_ident(column_name))}, 0.5)"
                    else:
                        agg_expr = None
                    if agg_expr:
                        audit_insert = f"""
                        WITH base AS ({ctx.analysis_query}),
                        grouped AS (
                            SELECT {group_exprs}, {agg_expr} AS target_value
                            FROM base
                            WHERE {col} IS NOT NULL
                            GROUP BY {group_exprs}
                            HAVING COUNT(DISTINCT {col}) > 1
                        )
                        INSERT INTO {audit_table} (plan_id, column_name, repair_type, row_id, before_value, after_value, created_at)
                        SELECT '{self._sanitize_literal(plan.get("plan_id", ""))}', '{self._sanitize_literal(column_name)}', 'conflict_repair',
                               TO_VARCHAR(tgt.{self._quote_ident(row_id_column)}),
                               TO_VARIANT(tgt.{col}),
                               TO_VARIANT(grouped.target_value),
                               CURRENT_TIMESTAMP()
                        FROM {ctx.table_ref} AS tgt
                        JOIN grouped
                        ON {" AND ".join([f"tgt.{self._quote_ident(name)} = grouped.{self._quote_ident(name)}" for name in group_by])}
                        """
                    else:
                        audit_insert = f"""
                        WITH base AS ({ctx.analysis_query}),
                        counts AS (
                            SELECT {group_exprs}, {col} AS value, COUNT(*) AS value_count
                            FROM base
                            WHERE {col} IS NOT NULL
                            GROUP BY {group_exprs}, {col}
                        ),
                        ranked AS (
                            SELECT *, ROW_NUMBER() OVER (PARTITION BY {group_exprs} ORDER BY value_count DESC) AS rn
                            FROM counts
                        ),
                        winners AS (
                            SELECT {group_exprs}, value AS target_value
                            FROM ranked
                            WHERE rn = 1
                        )
                        INSERT INTO {audit_table} (plan_id, column_name, repair_type, row_id, before_value, after_value, created_at)
                        SELECT '{self._sanitize_literal(plan.get("plan_id", ""))}', '{self._sanitize_literal(column_name)}', 'conflict_repair',
                               TO_VARCHAR(tgt.{self._quote_ident(row_id_column)}),
                               TO_VARIANT(tgt.{col}),
                               TO_VARIANT(winners.target_value),
                               CURRENT_TIMESTAMP()
                        FROM {ctx.table_ref} AS tgt
                        JOIN winners
                        ON {" AND ".join([f"tgt.{self._quote_ident(name)} = winners.{self._quote_ident(name)}" for name in group_by])}
                        """
                    await self.sf.execute_query(audit_insert)
                await self.sf.execute_query(update_query)
                repair_results.append(
                    {
                        "type": "conflict_repair",
                        "status": "applied",
                        "strategy": planned_conflict_strategy,
                        "group_by_columns": group_by,
                        "targeted_groups": conflict_groups,
                    }
                )
        elif conflict_groups and not group_by:
            repair_results.append(
                {
                    "type": "conflict_repair",
                    "status": "skipped",
                    "strategy": planned_conflict_strategy,
                    "reason": "group_by_columns_missing",
                }
            )

        applied_any = any(item.get("status") == "applied" for item in repair_results)
        plan_update = dict(plan) if isinstance(plan, dict) else {}
        if applied_any:
            plan_update["applied"] = True
            plan_update["applied_at"] = datetime.utcnow().isoformat()
        analysis_update = {"repair_results": repair_results, "repair_plan": plan_update}
        await self._update_column_analysis(ctx, analysis_update)
        return {
            "column": column_name,
            "repairs": repair_results,
            "approved": approved,
        }

    @tool
    async def numeric_analysis_agent(
        self,
        table_asset_id: int,
        column_name: str,
        focus: str | None = None,
    ) -> dict[str, Any]:
        """Agent specialized in numeric/temporal column analysis."""
        ctx = await self._load_context(table_asset_id, column_name)
        prompt = f"""
You analyze numeric/temporal columns. Decide which tools to call.

Column: {column_name}
Semantic type: {ctx.column_meta.semantic_type}
Focus: {focus or "numeric"}

Rules:
- Start with analyze_numeric_distribution.
- Use analyze_numeric_correlations and analyze_numeric_periodicity when helpful.
- Scan data quality with scan_nulls and plan_data_repairs if needed.
- Only call apply_data_repairs if approval is recorded in overrides.
- Generate visuals/insights only when requested or useful.
- You must call at least one tool (always call analyze_numeric_distribution first).
""".strip()
        summary = await self._run_sub_agent(
            name="Numeric Analysis Agent",
            system_prompt="You are a numeric analysis agent. Use the smallest tool set needed.",
            tools=[
                self.analyze_numeric_distribution,
                self.analyze_numeric_correlations,
                self.analyze_numeric_periodicity,
                self.scan_nulls,
                self.plan_data_repairs,
                self.require_user_approval,
                self.apply_data_repairs,
                self.generate_numeric_visuals,
                self.generate_numeric_insights,
            ],
            prompt=prompt,
            table_asset_id=table_asset_id,
            column_name=column_name,
        )
        return {"column": column_name, "summary": summary}

    @tool
    async def categorical_analysis_agent(
        self,
        table_asset_id: int,
        column_name: str,
        focus: str | None = None,
    ) -> dict[str, Any]:
        """Agent specialized in categorical column analysis."""
        ctx = await self._load_context(table_asset_id, column_name)
        prompt = f"""
You analyze categorical columns. Decide which tools to call.

Column: {column_name}
Semantic type: {ctx.column_meta.semantic_type}
Focus: {focus or "categorical"}

Rules:
- Start with analyze_categorical_groups.
- Use scan_nulls/scan_conflicts for quality checks.
- Use plan_data_repairs for repair planning.
- Only call apply_data_repairs if approval is recorded in overrides.
- Generate visuals/insights if they add value.
- You must call at least one tool (always call analyze_categorical_groups first).
""".strip()
        summary = await self._run_sub_agent(
            name="Categorical Analysis Agent",
            system_prompt="You are a categorical analysis agent. Use the smallest tool set needed.",
            tools=[
                self.analyze_categorical_groups,
                self.scan_nulls,
                self.scan_conflicts,
                self.plan_data_repairs,
                self.require_user_approval,
                self.apply_data_repairs,
                self.generate_categorical_visuals,
                self.generate_categorical_insights,
            ],
            prompt=prompt,
            table_asset_id=table_asset_id,
            column_name=column_name,
        )
        return {"column": column_name, "summary": summary}

    @tool
    async def text_analysis_agent(
        self,
        table_asset_id: int,
        column_name: str,
        focus: str | None = None,
    ) -> dict[str, Any]:
        """Agent specialized in text column analysis."""
        ctx = await self._load_context(table_asset_id, column_name)
        prompt = f"""
You analyze text columns. Decide which tools to call.

Column: {column_name}
Semantic type: {ctx.column_meta.semantic_type}
Focus: {focus or "text"}

Rules:
- Use summarize_text_column to capture summaries.
- Run scan_nulls to detect missing text.
- Only call row_level_extract_text if an instruction exists in overrides.
- Use plan_data_repairs and require_user_approval for data fixes.
- Only call apply_data_repairs if approval is recorded in overrides.
- You must call at least one tool (always call summarize_text_column).
""".strip()
        summary = await self._run_sub_agent(
            name="Text Analysis Agent",
            system_prompt="You are a text analysis agent. Use the smallest tool set needed.",
            tools=[
                self.scan_nulls,
                self.summarize_text_column,
                self.row_level_extract_text,
                self.plan_data_repairs,
                self.require_user_approval,
                self.apply_data_repairs,
            ],
            prompt=prompt,
            table_asset_id=table_asset_id,
            column_name=column_name,
        )
        return {"column": column_name, "summary": summary}

    @tool
    async def image_analysis_agent(
        self,
        table_asset_id: int,
        column_name: str,
        focus: str | None = None,
    ) -> dict[str, Any]:
        """Agent specialized in image column analysis."""
        prompt = f"""
You analyze image columns.

Column: {column_name}
Focus: {focus or "image"}

Rules:
- Call describe_image_column to probe image availability; it will return skipped if missing.
- You must call at least one tool.
""".strip()
        summary = await self._run_sub_agent(
            name="Image Analysis Agent",
            system_prompt="You are an image analysis agent. Use the smallest tool set needed.",
            tools=[self.describe_image_column],
            prompt=prompt,
            table_asset_id=table_asset_id,
            column_name=column_name,
        )
        return {"column": column_name, "summary": summary}

    @tool
    async def data_quality_agent(
        self,
        table_asset_id: int,
        column_name: str,
        focus: str | None = None,
    ) -> dict[str, Any]:
        """Agent specialized in data quality checks and repair planning."""
        prompt = f"""
You handle data quality for column {column_name}.
Focus: {focus or "quality"}

Rules:
- Run scan_nulls first, then scan_conflicts if a grouping is provided.
- Create repair plans with plan_data_repairs.
- Only call apply_data_repairs if approval is recorded in overrides.
- You must call at least one tool (always call scan_nulls first).
""".strip()
        summary = await self._run_sub_agent(
            name="Data Quality Agent",
            system_prompt="You are a data quality agent. Use the smallest tool set needed.",
            tools=[
                self.scan_nulls,
                self.scan_conflicts,
                self.plan_data_repairs,
                self.require_user_approval,
                self.apply_data_repairs,
            ],
            prompt=prompt,
            table_asset_id=table_asset_id,
            column_name=column_name,
        )
        return {"column": column_name, "summary": summary}

    @tool
    async def run_column_agent(
        self,
        table_asset_id: int,
        column_name: str,
        focus: str | None = None,
    ) -> dict[str, Any]:
        """Agent-run column analysis that selects tools based on column metadata."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}

        log_buffer = ColumnWorkflowLogBuffer()
        log_buffer.set_default_context(table_asset_id, column_name)
        self._set_log_buffer(log_buffer)
        sync_stop = asyncio.Event()
        sync_task = asyncio.create_task(self._sync_logs_loop(ctx, log_buffer, sync_stop))
        try:
            log_buffer.add_entry(
                "workflow_log",
                "Agent invocation starting",
                {"column": column_name, "table_asset_id": table_asset_id},
            )
            model = self._build_strands_model()

            agent = Agent(
                name="Column Analysis Orchestrator",
                system_prompt=(
                    "You are a column analysis orchestrator. Decide which specialist agent tools "
                    "to call. Use the smallest set of tools to deliver useful metadata updates. "
                    "Prefer windowed sampling for cost control; only use full scans when necessary. "
                    "You must invoke at least one tool; do not respond without tool calls."
                ),
                tools=[
                    self.numeric_analysis_agent,
                    self.categorical_analysis_agent,
                    self.text_analysis_agent,
                    self.image_analysis_agent,
                    self.data_quality_agent,
                    self.basic_column_stats,
                    self.ai_sql_agent,
                ],
                hooks=self._get_log_hooks(),
                model=model,
            )

            prompt = f"""
Analyze column '{column_name}' (table_asset_id={table_asset_id}) with focus={focus or ctx.column_meta.semantic_type}.

Context:
- semantic_type: {ctx.column_meta.semantic_type}
- confidence: {ctx.column_meta.confidence}
- time_column: {ctx.time_column}
- overrides: {json.dumps(overrides)}

Guidance:
- Route numeric/temporal columns to numeric_analysis_agent.
- Route categorical columns to categorical_analysis_agent.
- Route text columns to text_analysis_agent.
- Route image columns to image_analysis_agent.
- Run data_quality_agent when nulls/conflicts or repairs are requested.
- If focus is "repairs", prioritize data_quality_agent and apply_data_repairs after approval.
- Only apply repairs if overrides data_fix_approved is true.
- Use ai_sql_agent only when natural language reasoning is required.
- Include focus in specialist agent calls when helpful.
- Always pass table_asset_id={table_asset_id} and column_name='{column_name}' to tool calls.
- Always invoke at least one tool; if unsure, call basic_column_stats.
- Provide a short JSON summary of which tools you ran and key findings.
""".strip()
            try:
                response = await agent.invoke_async(
                    prompt,
                    invocation_state={
                        "table_asset_id": table_asset_id,
                        "column_name": column_name,
                    },
                )
            except Exception as exc:
                summary = f"Agent run failed: {exc}"
                await self._update_column_analysis(
                    ctx,
                    {"agent_summary": summary, "agent_focus": focus, "agent_error": str(exc)},
                )
                return {
                    "column": column_name,
                    "summary": summary,
                    "logs": log_buffer.entries,
                    "tool_calls": log_buffer.tool_calls,
                    "error": str(exc),
                }
            summary = getattr(response, "content", None) or str(response)
            if not log_buffer.tool_calls:
                await self._run_fallback_tool(ctx, "no_tool_calls")
            await self._update_column_analysis(ctx, {"agent_summary": summary, "agent_focus": focus})
            log_buffer.add_entry(
                "workflow_log",
                "Agent invocation completed",
                {"column": column_name, "tool_calls": len(log_buffer.tool_calls)},
            )
            return {
                "column": column_name,
                "summary": summary,
                "logs": log_buffer.entries,
                "tool_calls": log_buffer.tool_calls,
            }
        finally:
            sync_stop.set()
            if sync_task:
                await sync_task
            self._set_log_buffer(None)

    @tool
    async def ai_sql_agent(self, prompt: str) -> dict[str, Any]:
        """Run the AI SQL Strands agent for ad-hoc column reasoning."""
        agent = create_aisql_agent(self.sf, hooks=self._get_log_hooks())
        response = await agent.invoke_async(prompt)
        summary = getattr(response, "content", None) or str(response)
        return {"prompt": prompt, "response": summary}

    @tool
    async def summarize_text_column(self, table_asset_id: int, column_name: str) -> dict[str, Any]:
        """Summarize text column using AI_SUMMARIZE_AGG with token estimate."""
        ctx = await self._load_context(table_asset_id, column_name)
        col = self._quote_ident(column_name)
        token_info = await self._estimate_column_tokens(ctx.analysis_query, col)

        summary_query = f"""
        WITH base AS (
            {ctx.analysis_query}
        )
        SELECT AI_SUMMARIZE_AGG({col}) AS summary
        FROM base
        WHERE {col} IS NOT NULL
        """
        result = await self.sf.execute_query(summary_query)
        summary = result[0]["SUMMARY"] if result else ""

        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        analysis.update({
            "summary": summary,
            "summary_token_estimate": token_info,
        })
        await self._update_column_analysis(ctx, analysis)

        return {"column": column_name, "summary": summary, "token_estimate": token_info}

    @tool
    async def row_level_extract_text(self, table_asset_id: int, column_name: str) -> dict[str, Any]:
        """Row-level AI_COMPLETE extraction for text columns; writes to new column."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        instruction = overrides.get("row_level_instruction")
        if not instruction:
            return {"column": column_name, "skipped": True, "reason": "row_level_instruction_missing"}

        if not ctx.table_ref:
            return {"column": column_name, "skipped": True, "reason": "table_ref_missing"}

        output_column = (
            overrides.get("row_level_output_column")
            or overrides.get("output_column")
            or f"{column_name}_extracted"
        )
        col = self._quote_ident(column_name)
        token_info = await self._estimate_column_tokens(ctx.analysis_query, col)
        instruction_tokens = await self._estimate_tokens_for_prompt(str(instruction))
        safe_instruction = self._sanitize_literal(instruction)
        response_format = overrides.get("row_level_schema") or overrides.get("row_level_response_format")

        await self._ensure_column(ctx.table_ref, output_column)
        prompt_expr = f"CONCAT('{safe_instruction}', ' ', TO_VARCHAR({col}))"
        complete_expr = f"AI_COMPLETE('{self.model_id}', {prompt_expr})"
        if response_format:
            if isinstance(response_format, str):
                try:
                    response_format = json.loads(response_format)
                except json.JSONDecodeError:
                    response_format = None
            if isinstance(response_format, dict):
                if "schema" not in response_format:
                    response_format = {"type": "json", "schema": response_format}
                response_json = json.dumps(response_format)
                response_literal = self._sanitize_literal(response_json)
                complete_expr = (
                    f"AI_COMPLETE('{self.model_id}', {prompt_expr}, NULL, PARSE_JSON('{response_literal}'))"
                )

        update_query = f"""
        UPDATE {ctx.table_ref}
        SET {self._quote_ident(output_column)} = {complete_expr}
        WHERE {col} IS NOT NULL
        """
        await self.sf.execute_query(update_query)
        await self._ensure_feature_column_metadata(
            table_asset_id=ctx.table_asset_id,
            output_column=output_column,
            source_column=column_name,
            feature_type="row_level_extract",
        )

        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        feature_outputs = list(analysis.get("feature_outputs", []))
        feature_outputs = [
            item for item in feature_outputs
            if item.get("output_column") != output_column
        ]
        feature_outputs.append({
            "type": "row_level_extract",
            "output_column": output_column,
            "source_column": column_name,
            "instruction": instruction,
        })
        total_tokens = token_info.get("token_count", 0) + instruction_tokens * token_info.get("row_count", 0)
        analysis.update({
            "row_level_output": output_column,
            "row_level_token_estimate": {
                **token_info,
                "instruction_tokens": instruction_tokens,
                "total_tokens": total_tokens,
            },
            "feature_outputs": feature_outputs,
        })
        await self._update_column_analysis(ctx, analysis)

        return {"column": column_name, "output_column": output_column, "token_estimate": token_info}

    @tool
    async def describe_image_column(self, table_asset_id: int, column_name: str) -> dict[str, Any]:
        """Row-level AI_COMPLETE image descriptions; writes to new column."""
        ctx = await self._load_context(table_asset_id, column_name)
        if not ctx.table_ref:
            analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
            errors = list(analysis.get("errors", []))
            errors.append({
                "step": "describe_images",
                "error": "table_ref_missing",
                "detail": "Image descriptions require a physical table reference.",
            })
            analysis.update({"errors": errors})
            await self._update_column_analysis(ctx, analysis)
            return {"column": column_name, "skipped": True, "reason": "table_ref_missing"}

        overrides = ctx.column_meta.overrides or {}
        output_column = (
            overrides.get("image_output_column")
            or overrides.get("output_column")
            or f"{column_name}_description"
        )
        col = self._quote_ident(column_name)
        token_info = await self._estimate_column_tokens(ctx.analysis_query, col)
        instruction_text = "Describe the image in under 200 characters. If it cannot be accessed, respond with 'image_unavailable'."
        instruction_tokens = await self._estimate_tokens_for_prompt(instruction_text)
        instruction = self._sanitize_literal(instruction_text)
        file_expr = self._resolve_image_file_expr(ctx, col)
        image_model = overrides.get("image_model") or self.image_model_id
        supported_image_models = {
            "claude-4-opus",
            "claude-4-sonnet",
            "claude-3-7-sonnet",
            "claude-3-5-sonnet",
            "llama4-maverick",
            "llama4-scout",
            "openai-o4-mini",
            "openai-gpt-4.1",
            "pixtral-large",
        }
        if image_model not in supported_image_models:
            image_model = self.image_model_id

        if not file_expr:
            analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
            errors = list(analysis.get("errors", []))
            errors.append({
                "step": "describe_images",
                "error": "image_stage_missing",
                "detail": "Provide image_stage in table/column overrides or store FILE objects.",
            })
            analysis.update({"errors": errors})
            await self._update_column_analysis(ctx, analysis)
            return {"column": column_name, "skipped": True, "reason": "image_stage_missing"}

        await self._ensure_column(ctx.table_ref, output_column)
        update_query = f"""
        UPDATE {ctx.table_ref}
        SET {self._quote_ident(output_column)} = AI_COMPLETE(
            '{self._sanitize_literal(str(image_model))}',
            '{instruction}',
            {file_expr}
        )
        WHERE {col} IS NOT NULL
        """
        await self.sf.execute_query(update_query)
        await self._ensure_feature_column_metadata(
            table_asset_id=ctx.table_asset_id,
            output_column=output_column,
            source_column=column_name,
            feature_type="image_description",
        )

        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        feature_outputs = list(analysis.get("feature_outputs", []))
        feature_outputs = [
            item for item in feature_outputs
            if item.get("output_column") != output_column
        ]
        feature_outputs.append({
            "type": "image_description",
            "output_column": output_column,
            "source_column": column_name,
            "model": image_model,
        })
        total_tokens = token_info.get("row_count", 0) * instruction_tokens
        analysis.update({
            "image_descriptions_column": output_column,
            "row_level_token_estimate": {
                **token_info,
                "instruction_tokens": instruction_tokens,
                "total_tokens": total_tokens,
            },
            "feature_outputs": feature_outputs,
        })
        await self._update_column_analysis(ctx, analysis)

        return {"column": column_name, "output_column": output_column, "token_estimate": token_info}

    @tool
    async def basic_column_stats(self, table_asset_id: int, column_name: str) -> dict[str, Any]:
        """Basic stats for id/binary/spatial columns."""
        ctx = await self._load_context(table_asset_id, column_name)
        col = self._quote_ident(column_name)

        stats_query = f"""
        WITH base AS (
            {ctx.analysis_query}
        )
        SELECT
            COUNT(*) AS total_count,
            COUNT(DISTINCT {col}) AS distinct_count,
            COUNT_IF({col} IS NULL) AS null_count
        FROM base
        """
        stats_rows = await self.sf.execute_query(stats_query)
        stats = stats_rows[0] if stats_rows else {}

        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        analysis.update({"basic_stats": stats})
        await self._update_column_analysis(ctx, analysis)

        return {"column": column_name, "stats": stats}

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
        LIMIT 500
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
            LIMIT 8
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
            if total_count > top_sum:
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
                    "Top categories shown with long-tail grouped as Other",
                    f"Total non-null rows: {total_count}",
                ],
                source_columns=[x_column],
                x_title=x_column,
                y_title="Count",
            )

        y_ident = self._quote_ident(y_column)
        numeric_query = f"""
        WITH base AS (
            {ctx.analysis_query}
        ), top AS (
            SELECT {x_ident} AS category
            FROM base
            WHERE {x_ident} IS NOT NULL
            GROUP BY category
            ORDER BY COUNT(*) DESC
            LIMIT 8
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
        return f"TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR({col}))"

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
            return {"error": str(exc)}

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

    async def _list_temporal_columns(self, table_asset_id: int) -> list[str]:
        result = await self.db.execute(
            select(ColumnMetadata.column_name).where(
                ColumnMetadata.table_asset_id == table_asset_id,
                ColumnMetadata.semantic_type == "temporal",
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
            time_col = f"TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR({self._quote_ident(ctx.time_column)}))"
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
        return f'"{identifier.replace("\"", "\"\"")}"'

    def _strip_limit_clause(self, query: str) -> str:
        trimmed = str(query).strip().rstrip(";")
        pattern = re.compile(r"\s+LIMIT\s+\d+(\s+OFFSET\s+\d+)?\s*$", re.IGNORECASE)
        return pattern.sub("", trimmed).strip()


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
        allow_fallback = bool((column_meta.overrides or {}).get("allow_preset_fallback"))

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
            if allow_fallback and updated_meta and not self._analysis_ready(updated_meta):
                fallback_used = True
                await self._run_direct(ColumnWorkflowTools(self.sf, self.ai_sql, self.db), updated_meta)

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

        return {
            "workflow_id": workflow_id,
            "status": status,
            "workflow_state": workflow_state,
            "column": column_name,
            "semantic_type": column_meta.semantic_type,
            "fallback_used": fallback_used,
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
