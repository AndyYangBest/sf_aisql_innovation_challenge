"""Column workflow tool mixin."""

from __future__ import annotations

from typing import Any
import re
from strands import tool

class ColumnWorkflowInsightsMixin:
    """Tool mixin."""

    def _normalize_insight_items(self, raw_items: Any, max_items: int = 3) -> list[str]:
        if raw_items is None:
            return []
        items: list[Any]
        if isinstance(raw_items, list):
            items = raw_items
        else:
            items = [raw_items]

        normalized: list[str] = []
        for item in items:
            if item is None:
                continue
            text = str(item).strip()
            if not text:
                continue
            # Split on common bullet/numbered patterns or newlines.
            parts = re.split(r"(?:\n+|^\\s*[-•]\\s+|\\s*\\d+\\.\\s+)", text)
            for part in parts:
                cleaned = part.strip(" \t-•")
                if not cleaned:
                    continue
                normalized.append(cleaned)
                if len(normalized) >= max_items:
                    return normalized[:max_items]
        return normalized[:max_items]

    @tool
    async def generate_column_summary(self, table_asset_id: int, column_name: str) -> dict[str, Any]:
        """Generate a concise column summary using AI_AGG."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        payload = {
            "column": column_name,
            "semantic_type": ctx.column_meta.semantic_type,
            "stats": analysis.get("stats"),
            "nulls": analysis.get("nulls"),
            "conflicts": analysis.get("conflicts"),
            "visuals": analysis.get("visuals", []),
            "repair_plan": analysis.get("repair_plan"),
            "structure_type": ctx.structure_type,
        }

        instruction = (
            "Summarize the column in plain language. Return JSON with keys: "
            "summary (string, <=120 words), key_points (array, max 4), risks (array, max 3)."
        )
        focus_hint = overrides.get("summary_focus")
        user_notes = overrides.get("summary_user_notes")
        if focus_hint:
            instruction += f" Focus on: {focus_hint}."
        if user_notes:
            instruction += f" User notes: {user_notes}."

        token_estimate = await self._estimate_ai_agg_tokens(payload, instruction)
        summary = await self._run_ai_agg(payload, instruction)

        normalized_summary = summary.get("summary") if isinstance(summary, dict) else summary
        normalized_points = summary.get("key_points", []) if isinstance(summary, dict) else []
        normalized_risks = summary.get("risks", []) if isinstance(summary, dict) else []

        analysis.update(
            {
                "summary": normalized_summary,
                "summary_key_points": normalized_points,
                "summary_risks": normalized_risks,
                "summary_token_estimate": token_estimate,
            }
        )
        await self._update_column_analysis(ctx, analysis)
        return {
            "column": column_name,
            "summary": normalized_summary,
            "key_points": normalized_points,
            "risks": normalized_risks,
            "token_estimate": token_estimate,
        }

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
            "Summarize numeric column insights based on the provided stats and visuals. "
            "Return JSON with keys: insights (array of Markdown bullet strings, max 3), "
            "caveats (array, max 3). Avoid tool/meta lists. Each insight should be 1–2 sentences."
        )
        focus_hint = overrides.get("insights_focus") or overrides.get("insight_focus")
        user_notes = overrides.get("insights_user_notes") or overrides.get("insight_user_notes")
        if focus_hint:
            instruction += f" Focus on: {focus_hint}."
        if user_notes:
            instruction += f" User notes: {user_notes}."
        token_estimate = await self._estimate_ai_agg_tokens(payload, instruction)
        insights = await self._run_ai_agg(payload, instruction)

        normalized_insights = self._normalize_insight_items(
            insights.get("insights") if isinstance(insights, dict) else insights
        )
        normalized_caveats = self._normalize_insight_items(
            insights.get("caveats") if isinstance(insights, dict) else []
        )
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
            "Return JSON with keys: insights (array of Markdown bullet strings, max 3), "
            "caveats (array, max 3). Avoid tool/meta lists. Each insight should be 1–2 sentences."
        )
        focus_hint = overrides.get("insights_focus") or overrides.get("insight_focus")
        user_notes = overrides.get("insights_user_notes") or overrides.get("insight_user_notes")
        if focus_hint:
            instruction += f" Focus on: {focus_hint}."
        if user_notes:
            instruction += f" User notes: {user_notes}."
        token_estimate = await self._estimate_ai_agg_tokens(payload, instruction)
        insights = await self._run_ai_agg(payload, instruction)

        normalized_insights = self._normalize_insight_items(
            insights.get("insights") if isinstance(insights, dict) else insights
        )
        normalized_caveats = self._normalize_insight_items(
            insights.get("caveats") if isinstance(insights, dict) else []
        )
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
