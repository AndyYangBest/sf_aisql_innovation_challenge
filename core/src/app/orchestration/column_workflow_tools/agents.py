"""Column workflow tool mixin."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from strands import Agent, tool

from ..column_workflow_logging import ColumnWorkflowLogBuffer
from ..strands_aisql_agent import create_aisql_agent


class ColumnWorkflowAgentsMixin:
    """Tool mixin."""

    @tool
    async def numeric_analysis_agent(
        self,
        table_asset_id: int,
        column_name: str,
        focus: str | None = None,
    ) -> dict[str, Any]:
        """Agent specialized in numeric/temporal column analysis."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        metadata_payload = ctx.column_meta.metadata_payload or {}
        analysis_payload = (
            metadata_payload.get("analysis", {})
            if isinstance(metadata_payload, dict)
            else {}
        )
        nulls_payload = (
            analysis_payload.get("nulls", {})
            if isinstance(analysis_payload, dict)
            else {}
        )
        conflicts_payload = (
            analysis_payload.get("conflicts", {})
            if isinstance(analysis_payload, dict)
            else {}
        )
        known_null_rate = nulls_payload.get("null_rate") or metadata_payload.get(
            "null_rate"
        )
        known_null_count = nulls_payload.get("null_count") or metadata_payload.get(
            "null_count"
        )
        known_conflict_rate = conflicts_payload.get("conflict_rate")
        apply_target = overrides.get("data_fix_target") or overrides.get("apply_mode")
        prompt = f"""
        You analyze numeric/temporal columns. Decide which tools to call.

        Table asset id: {table_asset_id}
        Column: {column_name}
        Semantic type: {ctx.column_meta.semantic_type}
        Focus: {focus or "numeric"}
        Known null rate: {known_null_rate}
        Known null count: {known_null_count}
        Known conflict rate: {known_conflict_rate}
        Apply target: {apply_target}

        Rules:
        - Always use the provided table_asset_id ({table_asset_id}) and column_name ({column_name}) in tool calls.
        - Start with analyze_numeric_distribution.
        - Use analyze_numeric_correlations and analyze_numeric_periodicity when helpful.
        - Always run scan_nulls after distribution (quality scan is required).
        - Use generate_column_summary when a concise summary is requested or overrides include summary_focus/user_notes.
        - If scan_nulls or known nulls/conflicts are non-zero, run plan_data_repairs.
        - Do not apply repairs directly; only plan and request approval.
        - Always call generate_numeric_visuals and generate_numeric_insights.
        - Consider calling generate_chart_candidates when visuals are requested.
        - When multiple independent tools are needed, call them in the same response to allow parallel execution.
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
                self.generate_numeric_visuals,
                self.generate_numeric_insights,
                self.generate_column_summary,
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
        overrides = ctx.column_meta.overrides or {}
        apply_target = overrides.get("data_fix_target") or overrides.get("apply_mode")
        prompt = f"""
You analyze categorical columns. Decide which tools to call.

Table asset id: {table_asset_id}
Column: {column_name}
Semantic type: {ctx.column_meta.semantic_type}
Focus: {focus or "categorical"}
Apply target: {apply_target}

Rules:
- Always use the provided table_asset_id ({table_asset_id}) and column_name ({column_name}) in tool calls.
- Start with analyze_categorical_groups.
- Always run scan_nulls after grouping (quality scan is required).
- Use scan_conflicts when group_by columns are available.
- Use plan_data_repairs when scan results indicate issues.
- Use generate_column_summary when a concise summary is requested or overrides include summary_focus/user_notes.
- Do not apply repairs directly; only plan and request approval.
        - Always call generate_categorical_visuals and generate_categorical_insights.
        - Consider calling generate_chart_candidates when visuals are requested.
- When multiple independent tools are needed, call them in the same response to allow parallel execution.
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
                self.generate_categorical_visuals,
                self.generate_categorical_insights,
                self.generate_column_summary,
            ],
            prompt=prompt,
            table_asset_id=table_asset_id,
            column_name=column_name,
        )
        return {"column": column_name, "summary": summary}

    @tool
    async def visual_analysis_agent(
        self,
        table_asset_id: int,
        column_name: str,
        focus: str | None = None,
    ) -> dict[str, Any]:
        """Agent specialized in chart/visual generation."""
        ctx = await self._load_context(table_asset_id, column_name)
        prompt = f"""
You generate visual outputs and chart candidates.

Table asset id: {table_asset_id}
Column: {column_name}
Semantic type: {ctx.column_meta.semantic_type}
Focus: {focus or "visuals"}
Time column: {ctx.time_column}

Rules:
- Always use the provided table_asset_id ({table_asset_id}) and column_name ({column_name}) in tool calls.
- Use generate_numeric_visuals for numeric/temporal columns.
- Use generate_categorical_visuals for categorical columns.
- Use generate_chart_candidates to propose additional charts for the table. Prefer to call it with visuals.
- When multiple independent tools are needed, call them in the same response to allow parallel execution.
- You must call at least one tool.
""".strip()
        summary = await self._run_sub_agent(
            name="Visual Analysis Agent",
            system_prompt="You are a visualization agent. Use the smallest tool set needed.",
            tools=[
                self.generate_numeric_visuals,
                self.generate_categorical_visuals,
                self.generate_chart_candidates,
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
        overrides = ctx.column_meta.overrides or {}
        apply_target = overrides.get("data_fix_target") or overrides.get("apply_mode")
        prompt = f"""
You analyze text columns. Decide which tools to call.

Table asset id: {table_asset_id}
Column: {column_name}
Semantic type: {ctx.column_meta.semantic_type}
Focus: {focus or "text"}
Apply target: {apply_target}

Rules:
- Always use the provided table_asset_id ({table_asset_id}) and column_name ({column_name}) in tool calls.
- Use summarize_text_column to capture summaries.
- Run scan_nulls to detect missing text.
- Use generate_column_summary when a concise summary is requested or overrides include summary_focus/user_notes.
- Only call row_level_extract_text if an instruction exists in overrides.
- Use plan_data_repairs and require_user_approval for data fixes. Do not apply repairs directly.
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
                self.generate_column_summary,
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

        Table asset id: {table_asset_id}
        Column: {column_name}
        Focus: {focus or "image"}

        Rules:
        - Always use the provided table_asset_id ({table_asset_id}) and column_name ({column_name}) in tool calls.
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
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        metadata_payload = ctx.column_meta.metadata_payload or {}
        analysis_payload = (
            metadata_payload.get("analysis", {})
            if isinstance(metadata_payload, dict)
            else {}
        )
        nulls_payload = (
            analysis_payload.get("nulls", {})
            if isinstance(analysis_payload, dict)
            else {}
        )
        conflicts_payload = (
            analysis_payload.get("conflicts", {})
            if isinstance(analysis_payload, dict)
            else {}
        )
        known_null_rate = nulls_payload.get("null_rate") or metadata_payload.get(
            "null_rate"
        )
        known_null_count = nulls_payload.get("null_count") or metadata_payload.get(
            "null_count"
        )
        known_conflict_rate = conflicts_payload.get("conflict_rate")
        apply_target = overrides.get("data_fix_target") or overrides.get("apply_mode")
        prompt = f"""
        You handle data quality for column {column_name}.
        Table asset id: {table_asset_id}
        Focus: {focus or "quality"}
        Known null rate: {known_null_rate}
        Known null count: {known_null_count}
        Known conflict rate: {known_conflict_rate}
        Apply target: {apply_target}

        Rules:
        - Always use the provided table_asset_id ({table_asset_id}) and column_name ({column_name}) in tool calls.
        - Run scan_nulls first, then scan_conflicts if a grouping is provided.
        - Create repair plans with plan_data_repairs when issues are detected.
        - Do not apply repairs directly; only plan and request approval.
        - Only call require_user_approval after plan_data_repairs returns a plan.
        - When multiple independent tools are needed, call them in the same response to allow parallel execution.
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
        metadata_payload = ctx.column_meta.metadata_payload or {}
        analysis_payload = (
            metadata_payload.get("analysis", {})
            if isinstance(metadata_payload, dict)
            else {}
        )
        nulls_payload = (
            analysis_payload.get("nulls", {})
            if isinstance(analysis_payload, dict)
            else {}
        )
        conflicts_payload = (
            analysis_payload.get("conflicts", {})
            if isinstance(analysis_payload, dict)
            else {}
        )
        known_null_rate = nulls_payload.get("null_rate") or metadata_payload.get(
            "null_rate"
        )
        known_null_count = nulls_payload.get("null_count") or metadata_payload.get(
            "null_count"
        )
        known_conflict_rate = conflicts_payload.get("conflict_rate")

        log_buffer = ColumnWorkflowLogBuffer()
        log_buffer.set_default_context(table_asset_id, column_name)
        self._set_log_buffer(log_buffer)
        sync_stop = asyncio.Event()
        sync_task = asyncio.create_task(
            self._sync_logs_loop(ctx, log_buffer, sync_stop)
        )
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
                    self.visual_analysis_agent,
                    self.text_analysis_agent,
                    self.image_analysis_agent,
                    self.data_quality_agent,
                    self.basic_column_stats,
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
            - known_null_rate: {known_null_rate}
            - known_null_count: {known_null_count}
            - known_conflict_rate: {known_conflict_rate}

Guidance:
- Route numeric/temporal columns to numeric_analysis_agent.
- Route categorical columns to categorical_analysis_agent.
- Route visualization requests or visual-heavy runs to visual_analysis_agent. If visuals are requested, prefer visual_analysis_agent.
- Route text columns to text_analysis_agent.
- Route image columns to image_analysis_agent.
- Run data_quality_agent when nulls/conflicts are known or repairs are requested.
- If focus is "repairs", prioritize data_quality_agent and apply_data_repairs after approval.
- If overrides include data_fix_target="fixing_table", repairs should write to a fixing table (do not apply automatically).
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
                    {
                        "agent_summary": summary,
                        "agent_focus": focus,
                        "agent_error": str(exc),
                    },
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
                log_buffer.add_entry(
                    "warning",
                    "Agent produced no tool calls; invoking fallback tool",
                    {"column": column_name, "table_asset_id": table_asset_id},
                )
                await self._run_fallback_tool(ctx, "no_tool_calls")
            await self._update_column_analysis(
                ctx, {"agent_summary": summary, "agent_focus": focus}
            )
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
