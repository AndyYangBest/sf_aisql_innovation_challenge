"""Column-level analysis workflows using Strands workflow tool."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import re

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from strands import Agent, tool
from strands.models.openai import OpenAIModel
from strands_tools import workflow

from ..core.config import settings
from ..models.column_metadata import ColumnMetadata
from ..models.table_asset_metadata import TableAssetMetadata
from ..services.modular_ai_sql_service import ModularAISQLService
from ..services.snowflake_service import SnowflakeService

logger = logging.getLogger(__name__)


@dataclass
class ColumnContext:
    table_asset_id: int
    column_name: str
    base_query: str
    table_ref: str | None
    time_column: str | None
    structure_type: str | None
    column_meta: ColumnMetadata
    table_meta: TableAssetMetadata


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

    @tool
    async def generate_numeric_visuals(self, table_asset_id: int, column_name: str) -> dict[str, Any]:
        """Generate visuals for numeric/temporal columns and persist them."""
        ctx = await self._load_context(table_asset_id, column_name)
        col = self._quote_ident(column_name)

        is_temporal = ctx.column_meta.semantic_type == "temporal"
        time_expr = None
        if is_temporal:
            time_expr = self._resolve_temporal_expr(ctx, col)
            stats_query = f"""
            WITH base AS (
                {ctx.base_query}
            )
            SELECT
                MIN({time_expr}) AS min_value,
                MAX({time_expr}) AS max_value,
                COUNT_IF({time_expr} IS NOT NULL) AS total_count
            FROM base
            WHERE {time_expr} IS NOT NULL
            """
        else:
            stats_query = f"""
            WITH base AS (
                {ctx.base_query}
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
        histogram_data: list[dict[str, Any]] = []
        hist_query: str | None = None
        if not is_temporal and stats.get("MIN_VALUE") is not None and stats.get("MAX_VALUE") is not None:
            hist_query = f"""
            WITH base AS (
                {ctx.base_query}
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
                        "Distribution based on 20 bins",
                        f"Min: {stats.get('min_value')}, Max: {stats.get('max_value')}",
                    ],
                    source_columns=[column_name],
                )
            )

        time_col = None
        if is_temporal:
            time_col = time_expr or col
        elif ctx.time_column:
            time_col = self._quote_ident(ctx.time_column)
        if time_col:
            time_query = f"""
            WITH base AS (
                {ctx.base_query}
            )
            SELECT
                DATE_TRUNC('day', {time_col}) AS time_bucket,
                {"COUNT(*) AS count" if is_temporal else f"AVG({col}) AS avg_value"}
            FROM base
            WHERE {time_col} IS NOT NULL
            {"AND " + col + " IS NOT NULL" if not is_temporal else ""}
            GROUP BY time_bucket
            ORDER BY time_bucket
            LIMIT 500
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
                )
            )

        analysis = {
            "visuals": visuals,
            "stats": stats,
            "queries": {
                "stats_query": stats_query.strip(),
            },
        }
        if histogram_data and hist_query:
            analysis["queries"]["hist_query"] = hist_query.strip()
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

        total_query = f"""
        WITH base AS (
            {ctx.base_query}
        )
        SELECT COUNT(*) AS total_count
        FROM base
        WHERE {col} IS NOT NULL
        """
        total_rows = await self.sf.execute_query(total_query)
        total_count = self._coerce_int(total_rows[0]["TOTAL_COUNT"]) if total_rows else 0

        top_query = f"""
        WITH base AS (
            {ctx.base_query}
        )
        SELECT {col} AS category, COUNT(*) AS count
        FROM base
        WHERE {col} IS NOT NULL
        GROUP BY category
        ORDER BY count DESC
        LIMIT 8
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
        await self._update_column_analysis(ctx, analysis)

        return {"column": column_name, "visuals": visuals, "total_count": total_count}

    @tool
    async def generate_numeric_insights(self, table_asset_id: int, column_name: str) -> dict[str, Any]:
        """Generate AI insights for numeric/temporal columns based on visuals."""
        ctx = await self._load_context(table_asset_id, column_name)
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
        token_estimate = await self._estimate_ai_agg_tokens(payload, instruction)
        insights = await self._run_ai_agg(payload, instruction)

        analysis.update({"insights": insights, "insight_token_estimate": token_estimate})
        await self._update_column_analysis(ctx, analysis)
        return {"column": column_name, "insights": insights, "token_estimate": token_estimate}

    @tool
    async def generate_categorical_insights(self, table_asset_id: int, column_name: str) -> dict[str, Any]:
        """Generate AI insights for categorical columns based on visuals."""
        ctx = await self._load_context(table_asset_id, column_name)
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
        token_estimate = await self._estimate_ai_agg_tokens(payload, instruction)
        insights = await self._run_ai_agg(payload, instruction)

        analysis.update({"insights": insights, "insight_token_estimate": token_estimate})
        await self._update_column_analysis(ctx, analysis)
        return {"column": column_name, "insights": insights, "token_estimate": token_estimate}

    @tool
    async def summarize_text_column(self, table_asset_id: int, column_name: str) -> dict[str, Any]:
        """Summarize text column using AI_SUMMARIZE_AGG with token estimate."""
        ctx = await self._load_context(table_asset_id, column_name)
        col = self._quote_ident(column_name)
        token_info = await self._estimate_column_tokens(ctx.base_query, col)

        summary_query = f"""
        WITH base AS (
            {ctx.base_query}
        )
        SELECT SNOWFLAKE.CORTEX.SUMMARIZE_AGG({col}) AS summary
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

        output_column = overrides.get("output_column") or f"{column_name}_extracted"
        col = self._quote_ident(column_name)
        token_info = await self._estimate_column_tokens(ctx.base_query, col)
        safe_instruction = self._sanitize_literal(instruction)

        await self._ensure_column(ctx.table_ref, output_column)
        update_query = f"""
        UPDATE {ctx.table_ref}
        SET {self._quote_ident(output_column)} = SNOWFLAKE.CORTEX.COMPLETE(
            '{self.model_id}',
            PROMPT('{safe_instruction}', {col})
        )
        """
        await self.sf.execute_query(update_query)

        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        analysis.update({
            "row_level_output": output_column,
            "row_level_token_estimate": token_info,
        })
        await self._update_column_analysis(ctx, analysis)

        return {"column": column_name, "output_column": output_column, "token_estimate": token_info}

    @tool
    async def describe_image_column(self, table_asset_id: int, column_name: str) -> dict[str, Any]:
        """Row-level AI_COMPLETE image descriptions; writes to new column."""
        ctx = await self._load_context(table_asset_id, column_name)
        if not ctx.table_ref:
            return {"column": column_name, "skipped": True, "reason": "table_ref_missing"}

        output_column = f"{column_name}_description"
        col = self._quote_ident(column_name)
        token_info = await self._estimate_column_tokens(ctx.base_query, col)
        instruction = self._sanitize_literal("Describe the image in under 200 characters.")

        await self._ensure_column(ctx.table_ref, output_column)
        update_query = f"""
        UPDATE {ctx.table_ref}
        SET {self._quote_ident(output_column)} = SNOWFLAKE.CORTEX.COMPLETE(
            '{self.model_id}',
            PROMPT('{instruction}', {col})
        )
        """
        await self.sf.execute_query(update_query)

        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        analysis.update({
            "image_descriptions_column": output_column,
            "row_level_token_estimate": token_info,
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
            {ctx.base_query}
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
        if not table_meta:
            raise ValueError("Table metadata missing; initialize column metadata first.")

        column_result = await self.db.execute(
            select(ColumnMetadata).where(
                ColumnMetadata.table_asset_id == table_asset_id,
                ColumnMetadata.column_name == column_name,
            )
        )
        column_meta = column_result.scalar_one_or_none()
        if not column_meta:
            raise ValueError("Column metadata missing; initialize column metadata first.")

        base_query = (table_meta.metadata_payload or {}).get("base_query")
        if not base_query:
            raise ValueError("Base query missing from table metadata.")

        table_ref = None
        meta_payload = column_meta.metadata_payload or {}
        if meta_payload.get("table_ref"):
            table_ref = meta_payload.get("table_ref")

        time_column = (table_meta.metadata_payload or {}).get("time_column")
        structure_type = table_meta.structure_type

        return ColumnContext(
            table_asset_id=table_asset_id,
            column_name=column_name,
            base_query=base_query,
            table_ref=table_ref,
            time_column=time_column,
            structure_type=structure_type,
            column_meta=column_meta,
            table_meta=table_meta,
        )

    async def _update_column_analysis(self, ctx: ColumnContext, analysis_update: dict[str, Any]) -> None:
        metadata = ctx.column_meta.metadata_payload or {}
        analysis = metadata.get("analysis", {})
        analysis.update(analysis_update)
        metadata["analysis"] = analysis
        ctx.column_meta.metadata_payload = metadata
        ctx.column_meta.last_updated = datetime.now(timezone.utc)
        await self.db.commit()
        await self.db.refresh(ctx.column_meta)

    async def estimate_workflow_tokens(self, table_asset_id: int, column_name: str) -> dict[str, Any]:
        ctx = await self._load_context(table_asset_id, column_name)
        semantic_type = ctx.column_meta.semantic_type
        estimates: list[dict[str, Any]] = []

        if semantic_type in {"numeric", "temporal"}:
            instruction = "Summarize numeric column insights based on stats and visuals."
            try:
                payload = await self._build_numeric_insights_payload(ctx, column_name)
            except Exception:
                payload = {"column": column_name, "stats": {}, "visuals": []}
            estimates.append({
                "task": "generate_insights",
                "token_count": await self._estimate_ai_agg_tokens(payload, instruction),
            })

        if semantic_type == "categorical":
            instruction = "Summarize category distribution insights based on stats and visuals."
            try:
                payload = await self._build_categorical_insights_payload(ctx, column_name)
            except Exception:
                payload = {"column": column_name, "stats": {}, "visuals": []}
            estimates.append({
                "task": "generate_insights",
                "token_count": await self._estimate_ai_agg_tokens(payload, instruction),
            })

        if semantic_type == "text":
            summary_tokens = await self._estimate_column_tokens(ctx.base_query, self._quote_ident(column_name))
            estimates.append({
                "task": "summarize_text",
                **summary_tokens,
            })
            if (ctx.column_meta.overrides or {}).get("row_level_instruction"):
                row_tokens = await self._estimate_column_tokens(ctx.base_query, self._quote_ident(column_name))
                estimates.append({
                    "task": "row_level_extract",
                    **row_tokens,
                })

        if semantic_type == "image":
            row_tokens = await self._estimate_column_tokens(ctx.base_query, self._quote_ident(column_name))
            estimates.append({
                "task": "describe_images",
                **row_tokens,
            })

        total_tokens = sum(int(item.get("token_count", 0)) for item in estimates)
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
    ) -> dict[str, Any]:
        return {
            "id": f"chart_{uuid.uuid4().hex}",
            "chartType": chart_type,
            "title": title,
            "xKey": x_key,
            "yKey": y_key,
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

    def _resolve_temporal_expr(self, ctx: ColumnContext, col: str) -> str:
        sql_type = (ctx.column_meta.metadata_payload or {}).get("sql_type")
        if sql_type:
            normalized = str(sql_type).upper()
            if "TIMESTAMP" in normalized or "DATE" in normalized or "TIME" in normalized:
                return col
        return f"TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR({col}))"

    async def _estimate_column_tokens(self, base_query: str, column_expr: str) -> dict[str, Any]:
        query = f"""
        WITH base AS (
            {base_query}
        )
        SELECT
            COUNT(*) AS row_count,
            SUM(SNOWFLAKE.CORTEX.COUNT_TOKENS('{self.model_id}', {column_expr})) AS token_count
        FROM base
        WHERE {column_expr} IS NOT NULL
        """
        try:
            result = await self.sf.execute_query(query)
        except Exception:
            count_query = f"""
            WITH base AS (
                {base_query}
            )
            SELECT COUNT(*) AS row_count
            FROM base
            WHERE {column_expr} IS NOT NULL
            """
            try:
                count_result = await self.sf.execute_query(count_query)
                return {"row_count": count_result[0].get("ROW_COUNT", 0) if count_result else 0, "token_count": 0}
            except Exception:
                return {"row_count": 0, "token_count": 0}

        if not result:
            return {"row_count": 0, "token_count": 0}
        return {
            "row_count": result[0].get("ROW_COUNT", 0),
            "token_count": result[0].get("TOKEN_COUNT") or 0,
        }

    async def _estimate_tokens_for_prompt(self, prompt: str) -> int:
        safe_prompt = self._sanitize_literal(prompt)
        query = f"SELECT SNOWFLAKE.CORTEX.COUNT_TOKENS('{self.model_id}', '{safe_prompt}') AS TOKEN_COUNT"
        try:
            result = await self.sf.execute_query(query)
        except Exception:
            return 0
        return int(result[0]["TOKEN_COUNT"]) if result else 0

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
                {ctx.base_query}
            )
            SELECT
                MIN({time_expr}) AS min_value,
                MAX({time_expr}) AS max_value,
                COUNT_IF({time_expr} IS NOT NULL) AS total_count
            FROM base
            WHERE {time_expr} IS NOT NULL
            """
        else:
            stats_query = f"""
            WITH base AS (
                {ctx.base_query}
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
                {ctx.base_query}
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
                )
            )

        time_col = None
        if is_temporal:
            time_col = time_expr or col
        elif ctx.time_column:
            time_col = self._quote_ident(ctx.time_column)
        if time_col:
            time_query = f"""
            WITH base AS (
                {ctx.base_query}
            )
            SELECT
                DATE_TRUNC('day', {time_col}) AS time_bucket,
                {"COUNT(*) AS count" if is_temporal else f"AVG({col}) AS avg_value"}
            FROM base
            WHERE {time_col} IS NOT NULL
            {"AND " + col + " IS NOT NULL" if not is_temporal else ""}
            GROUP BY time_bucket
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
            visuals.append(
                self._build_chart_spec(
                    chart_type="line",
                    title=f"{column_name} over time",
                    x_key="time_bucket",
                    y_key="count" if is_temporal else "avg_value",
                    data=time_data,
                    narrative=["Daily trend based on counts" if is_temporal else "Daily trend based on average values"],
                    source_columns=[column_name] if is_temporal else [ctx.time_column, column_name],
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
            {ctx.base_query}
        )
        SELECT COUNT(*) AS total_count
        FROM base
        WHERE {col} IS NOT NULL
        """
        total_rows = await self.sf.execute_query(total_query)
        total_count = self._coerce_int(total_rows[0]["TOTAL_COUNT"]) if total_rows else 0

        top_query = f"""
        WITH base AS (
            {ctx.base_query}
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
        result = await self.ai_sql.sf.execute_query(query)
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


class ColumnWorkflowOrchestrator:
    """Runs a Strands workflow for a single column."""

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

        model = OpenAIModel(
            model_id=model_id,
            params={
                "max_tokens": 4096,
                "temperature": 0.2,
            },
        )

        tools = ColumnWorkflowTools(snowflake_service, ai_sql_service, db)
        self.coordinator = Agent(
            name="Column Workflow Coordinator",
            system_prompt=(
                "You run column-specific analysis workflows. "
                "Always call the provided tool with the exact parameters."
            ),
            tools=[
                workflow,
                tools.generate_numeric_visuals,
                tools.generate_categorical_visuals,
                tools.generate_numeric_insights,
                tools.generate_categorical_insights,
                tools.summarize_text_column,
                tools.row_level_extract_text,
                tools.describe_image_column,
                tools.basic_column_stats,
            ],
            model=model,
        )

    async def run_column_workflow(
        self,
        table_asset_id: int,
        column_name: str,
    ) -> dict[str, Any]:
        column_meta = await self._get_column_meta(table_asset_id, column_name)
        if not column_meta:
            raise ValueError("Column metadata not found")

        tasks = self._build_tasks(column_meta)
        workflow_id = f"column_{table_asset_id}_{column_name}_{uuid.uuid4().hex[:8]}"

        await asyncio.to_thread(
            self.coordinator.tool.workflow,
            action="create",
            workflow_id=workflow_id,
            tasks=tasks,
        )
        await asyncio.to_thread(
            self.coordinator.tool.workflow,
            action="start",
            workflow_id=workflow_id,
        )
        status = await asyncio.to_thread(
            self.coordinator.tool.workflow,
            action="status",
            workflow_id=workflow_id,
        )

        workflow_file = Path.home() / ".strands" / "workflows" / f"{workflow_id}.json"
        workflow_state = None
        if workflow_file.exists():
            try:
                workflow_data = json.loads(workflow_file.read_text())
                workflow_state = workflow_data.get("status")
            except Exception:
                workflow_state = None

        updated_meta = await self._get_column_meta(table_asset_id, column_name)
        fallback_used = False
        if updated_meta and not self._analysis_ready(updated_meta):
            fallback_used = True
            tools = ColumnWorkflowTools(self.sf, self.ai_sql, self.db)
            await self._run_direct(tools, updated_meta)

        await self._record_workflow_status(
            column_meta=updated_meta or column_meta,
            workflow_id=workflow_id,
            status=status,
            workflow_state=workflow_state,
            fallback_used=fallback_used,
        )

        return {
            "workflow_id": workflow_id,
            "status": status,
            "workflow_state": workflow_state,
            "column": column_name,
            "semantic_type": column_meta.semantic_type,
            "fallback_used": fallback_used,
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
    ) -> None:
        metadata = column_meta.metadata_payload or {}
        workflow_meta = metadata.get("workflow", {})
        workflow_meta.update(
            {
                "workflow_id": workflow_id,
                "status": status,
                "workflow_state": workflow_state,
                "fallback_used": fallback_used,
                "last_run_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        metadata["workflow"] = workflow_meta
        column_meta.metadata_payload = metadata
        column_meta.last_updated = datetime.now(timezone.utc)
        await self.db.commit()
        await self.db.refresh(column_meta)

    def _build_tasks(self, column_meta: ColumnMetadata) -> list[dict[str, Any]]:
        column_name = column_meta.column_name
        table_asset_id = column_meta.table_asset_id
        semantic_type = column_meta.semantic_type

        if semantic_type in {"numeric", "temporal"}:
            return [
                {
                    "task_id": "generate_visuals",
                    "tools": ["generate_numeric_visuals"],
                    "description": f"Generate visuals for {column_name}",
                    "system_prompt": f"Call generate_numeric_visuals with table_asset_id={table_asset_id} and column_name='{column_name}'.",
                    "dependencies": [],
                },
                {
                    "task_id": "generate_insights",
                    "tools": ["generate_numeric_insights"],
                    "description": f"Generate insights for {column_name}",
                    "system_prompt": f"Call generate_numeric_insights with table_asset_id={table_asset_id} and column_name='{column_name}'.",
                    "dependencies": ["generate_visuals"],
                },
            ]

        if semantic_type == "categorical":
            return [
                {
                    "task_id": "generate_visuals",
                    "tools": ["generate_categorical_visuals"],
                    "description": f"Generate visuals for {column_name}",
                    "system_prompt": f"Call generate_categorical_visuals with table_asset_id={table_asset_id} and column_name='{column_name}'.",
                    "dependencies": [],
                },
                {
                    "task_id": "generate_insights",
                    "tools": ["generate_categorical_insights"],
                    "description": f"Generate insights for {column_name}",
                    "system_prompt": f"Call generate_categorical_insights with table_asset_id={table_asset_id} and column_name='{column_name}'.",
                    "dependencies": ["generate_visuals"],
                },
            ]

        if semantic_type == "text":
            tasks = [
                {
                    "task_id": "summarize_text",
                    "tools": ["summarize_text_column"],
                    "description": f"Summarize text for {column_name}",
                    "system_prompt": f"Call summarize_text_column with table_asset_id={table_asset_id} and column_name='{column_name}'.",
                    "dependencies": [],
                }
            ]
            if (column_meta.overrides or {}).get("row_level_instruction"):
                tasks.append(
                    {
                        "task_id": "row_level_extract",
                        "tools": ["row_level_extract_text"],
                        "description": f"Row-level extraction for {column_name}",
                        "system_prompt": f"Call row_level_extract_text with table_asset_id={table_asset_id} and column_name='{column_name}'.",
                        "dependencies": ["summarize_text"],
                    }
                )
            return tasks

        if semantic_type == "image":
            return [
                {
                    "task_id": "describe_images",
                    "tools": ["describe_image_column"],
                    "description": f"Describe images for {column_name}",
                    "system_prompt": f"Call describe_image_column with table_asset_id={table_asset_id} and column_name='{column_name}'.",
                    "dependencies": [],
                }
            ]

        return [
            {
                "task_id": "basic_stats",
                "tools": ["basic_column_stats"],
                "description": f"Basic stats for {column_name}",
                "system_prompt": f"Call basic_column_stats with table_asset_id={table_asset_id} and column_name='{column_name}'.",
                "dependencies": [],
            }
        ]

    def _analysis_ready(self, column_meta: ColumnMetadata) -> bool:
        metadata = column_meta.metadata_payload or {}
        analysis = metadata.get("analysis", {})
        semantic_type = column_meta.semantic_type

        if semantic_type in {"numeric", "temporal"}:
            return bool(analysis.get("visuals")) or bool(analysis.get("insights"))
        if semantic_type == "categorical":
            return bool(analysis.get("visuals")) or bool(analysis.get("insights"))
        if semantic_type == "text":
            return bool(analysis.get("summary"))
        if semantic_type == "image":
            return bool(analysis.get("image_descriptions_column"))
        return bool(analysis.get("basic_stats"))

    async def _run_direct(self, tools: ColumnWorkflowTools, column_meta: ColumnMetadata) -> None:
        table_asset_id = column_meta.table_asset_id
        column_name = column_meta.column_name
        semantic_type = column_meta.semantic_type

        if semantic_type in {"numeric", "temporal"}:
            await tools.generate_numeric_visuals(table_asset_id, column_name)
            await tools.generate_numeric_insights(table_asset_id, column_name)
            return

        if semantic_type == "categorical":
            await tools.generate_categorical_visuals(table_asset_id, column_name)
            await tools.generate_categorical_insights(table_asset_id, column_name)
            return

        if semantic_type == "text":
            await tools.summarize_text_column(table_asset_id, column_name)
            if (column_meta.overrides or {}).get("row_level_instruction"):
                await tools.row_level_extract_text(table_asset_id, column_name)
            return

        if semantic_type == "image":
            await tools.describe_image_column(table_asset_id, column_name)
            return

        await tools.basic_column_stats(table_asset_id, column_name)
