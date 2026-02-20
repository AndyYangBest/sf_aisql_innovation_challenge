"""Column workflow tool mixin."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from sqlalchemy import select

from strands import Agent, tool

from ..column_workflow_logging import ColumnWorkflowLogBuffer
from ..strands_aisql_agent import create_aisql_agent
from ...models.agent_prompt import AgentPrompt
from ...prompts.agent_prompt_templates import (
    AGENT_PROMPT_TEMPLATES,
    AGENT_SYSTEM_PROMPT_TEMPLATES,
)


class ColumnWorkflowAgentsMixin:
    """Tool mixin."""

    async def _load_agent_prompt(self, agent_name: str) -> AgentPrompt | None:
        try:
            result = await self.db.execute(
                select(AgentPrompt).where(
                    AgentPrompt.agent_name == agent_name,
                    AgentPrompt.is_active.is_(True),
                )
            )
            return result.scalar_one_or_none()
        except Exception:
            return None

    def _format_prompt_template(self, template: str, context: dict[str, Any]) -> str:
        class _SafeDict(dict):
            def __missing__(self, key: str) -> str:
                return ""

        try:
            return template.format_map(_SafeDict(context))
        except Exception:
            return template

    async def _resolve_agent_prompt(
        self,
        agent_name: str,
        default_prompt: str,
        default_system_prompt: str | None,
        context: dict[str, Any],
    ) -> tuple[str, str | None]:
        record = await self._load_agent_prompt(agent_name)
        if not record:
            return (
                self._format_prompt_template(default_prompt, context),
                self._format_prompt_template(default_system_prompt or "", context)
                if default_system_prompt
                else None,
            )
        prompt = self._format_prompt_template(record.prompt, context)
        system_prompt = (
            self._format_prompt_template(record.system_prompt, context)
            if record.system_prompt
            else default_system_prompt
        )
        return prompt, system_prompt

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
        default_prompt = AGENT_PROMPT_TEMPLATES["numeric_analysis_agent"]
        context = {
            "table_asset_id": table_asset_id,
            "column_name": column_name,
            "semantic_type": ctx.column_meta.semantic_type,
            "focus": focus or "numeric",
            "known_null_rate": known_null_rate,
            "known_null_count": known_null_count,
            "known_conflict_rate": known_conflict_rate,
            "apply_target": apply_target,
        }
        prompt, system_prompt = await self._resolve_agent_prompt(
            "numeric_analysis_agent",
            default_prompt,
            AGENT_SYSTEM_PROMPT_TEMPLATES.get("numeric_analysis_agent"),
            context,
        )
        summary = await self._run_sub_agent(
            name="Numeric Analysis Agent",
            system_prompt=system_prompt,
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
        default_prompt = AGENT_PROMPT_TEMPLATES["categorical_analysis_agent"]
        context = {
            "table_asset_id": table_asset_id,
            "column_name": column_name,
            "semantic_type": ctx.column_meta.semantic_type,
            "focus": focus or "categorical",
            "apply_target": apply_target,
        }
        prompt, system_prompt = await self._resolve_agent_prompt(
            "categorical_analysis_agent",
            default_prompt,
            AGENT_SYSTEM_PROMPT_TEMPLATES.get("categorical_analysis_agent"),
            context,
        )
        summary = await self._run_sub_agent(
            name="Categorical Analysis Agent",
            system_prompt=system_prompt,
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
        default_prompt = AGENT_PROMPT_TEMPLATES["visual_analysis_agent"]
        context = {
            "table_asset_id": table_asset_id,
            "column_name": column_name,
            "semantic_type": ctx.column_meta.semantic_type,
            "focus": focus or "visuals",
            "time_column": ctx.time_column,
        }
        prompt, system_prompt = await self._resolve_agent_prompt(
            "visual_analysis_agent",
            default_prompt,
            AGENT_SYSTEM_PROMPT_TEMPLATES.get("visual_analysis_agent"),
            context,
        )
        summary = await self._run_sub_agent(
            name="Visual Analysis Agent",
            system_prompt=system_prompt,
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
        default_prompt = AGENT_PROMPT_TEMPLATES["text_analysis_agent"]
        context = {
            "table_asset_id": table_asset_id,
            "column_name": column_name,
            "semantic_type": ctx.column_meta.semantic_type,
            "focus": focus or "text",
            "apply_target": apply_target,
        }
        prompt, system_prompt = await self._resolve_agent_prompt(
            "text_analysis_agent",
            default_prompt,
            AGENT_SYSTEM_PROMPT_TEMPLATES.get("text_analysis_agent"),
            context,
        )
        summary = await self._run_sub_agent(
            name="Text Analysis Agent",
            system_prompt=system_prompt,
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
        default_prompt = AGENT_PROMPT_TEMPLATES["image_analysis_agent"]
        context = {
            "table_asset_id": table_asset_id,
            "column_name": column_name,
            "focus": focus or "image",
        }
        prompt, system_prompt = await self._resolve_agent_prompt(
            "image_analysis_agent",
            default_prompt,
            AGENT_SYSTEM_PROMPT_TEMPLATES.get("image_analysis_agent"),
            context,
        )
        summary = await self._run_sub_agent(
            name="Image Analysis Agent",
            system_prompt=system_prompt,
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
        default_prompt = AGENT_PROMPT_TEMPLATES["data_quality_agent"]
        context = {
            "table_asset_id": table_asset_id,
            "column_name": column_name,
            "focus": focus or "quality",
            "known_null_rate": known_null_rate,
            "known_null_count": known_null_count,
            "known_conflict_rate": known_conflict_rate,
            "apply_target": apply_target,
        }
        prompt, system_prompt = await self._resolve_agent_prompt(
            "data_quality_agent",
            default_prompt,
            AGENT_SYSTEM_PROMPT_TEMPLATES.get("data_quality_agent"),
            context,
        )
        summary = await self._run_sub_agent(
            name="Data Quality Agent",
            system_prompt=system_prompt,
            tools=[
                self.scan_nulls,
                self.scan_conflicts,
                self.plan_data_repairs,
                self.repair_rationale_agent,
                self.require_user_approval,
            ],
            prompt=prompt,
            table_asset_id=table_asset_id,
            column_name=column_name,
        )
        return {"column": column_name, "summary": summary}

    @tool
    async def repair_rationale_agent(
        self,
        table_asset_id: int,
        column_name: str,
    ) -> dict[str, Any]:
        """Agent specialized in generating repair rationale reports."""
        default_prompt = AGENT_PROMPT_TEMPLATES["repair_rationale_agent"]
        context = {"table_asset_id": table_asset_id, "column_name": column_name}
        prompt, system_prompt = await self._resolve_agent_prompt(
            "repair_rationale_agent",
            default_prompt,
            AGENT_SYSTEM_PROMPT_TEMPLATES.get("repair_rationale_agent"),
            context,
        )
        summary = await self._run_sub_agent(
            name="Repair Rationale Agent",
            system_prompt=system_prompt,
            tools=[self.generate_repair_rationale],
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
        await self._ensure_analysis_snapshot(ctx, column_name)
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

            default_system_prompt = AGENT_SYSTEM_PROMPT_TEMPLATES.get("run_column_agent")
            default_prompt = AGENT_PROMPT_TEMPLATES["run_column_agent"]
            context = {
                "table_asset_id": table_asset_id,
                "column_name": column_name,
                "semantic_type": ctx.column_meta.semantic_type,
                "focus": focus or ctx.column_meta.semantic_type,
                "confidence": ctx.column_meta.confidence,
                "time_column": ctx.time_column,
                "overrides": json.dumps(overrides),
                "known_null_rate": known_null_rate,
                "known_null_count": known_null_count,
                "known_conflict_rate": known_conflict_rate,
            }
            prompt, system_prompt = await self._resolve_agent_prompt(
                "run_column_agent",
                default_prompt,
                default_system_prompt,
                context,
            )

            agent = Agent(
                name="Column Analysis Orchestrator",
                system_prompt=system_prompt or default_system_prompt,
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
