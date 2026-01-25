"""Column workflow tool mixin."""

from __future__ import annotations

import logging
from typing import Any
from strands import tool

logger = logging.getLogger(__name__)

class ColumnWorkflowVisualsMixin:
    """Tool mixin."""

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

