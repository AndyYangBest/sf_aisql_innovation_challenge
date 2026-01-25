"""Column workflow tool mixin."""

from __future__ import annotations

from typing import Any
from strands import tool

class ColumnWorkflowInsightsMixin:
    """Tool mixin."""

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

