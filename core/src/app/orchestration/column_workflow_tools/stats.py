"""Column workflow tool mixin."""

from __future__ import annotations

from typing import Any
from strands import tool

class ColumnWorkflowStatsMixin:
    """Tool mixin."""

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

