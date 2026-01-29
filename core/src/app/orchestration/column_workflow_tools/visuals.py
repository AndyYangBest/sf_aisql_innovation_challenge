from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import select

from strands import tool

from ...models.column_metadata import ColumnMetadata
from ...services.chart_service import ChartService
from ...services.eda_service import EDAService

logger = logging.getLogger(__name__)


class ColumnWorkflowVisualsMixin:
    """Tool mixin."""

    @tool
    async def generate_chart_candidates(
        self,
        table_asset_id: int,
        column_name: str | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        """Generate chart candidate configurations for the table."""
        ctx = await self._load_context(table_asset_id, column_name or "")
        if not ctx.table_ref:
            return {
                "table_asset_id": table_asset_id,
                "chart_candidates": [],
                "error": "table_ref_missing",
            }

        table_ref = ctx.table_ref
        database = None
        schema = None
        table_name = table_ref

        if "." in table_ref:
            parts = table_ref.split(".")
            if len(parts) == 3:
                database, schema, table_name = parts
            elif len(parts) == 2:
                schema, table_name = parts

        try:
            columns = await self.sf.get_table_columns(table_name, database=database, schema=schema)
        except Exception as exc:
            logger.warning("Failed to load table columns for chart candidates: %s", exc)
            return {
                "table_asset_id": table_asset_id,
                "chart_candidates": [],
                "error": f"column_load_failed:{exc}",
            }

        meta_result = await self.db.execute(
            select(ColumnMetadata).where(ColumnMetadata.table_asset_id == table_asset_id)
        )
        meta_rows = {row.column_name: row for row in meta_result.scalars().all()}

        column_profiles: list[dict[str, Any]] = []
        for col in columns:
            column_name = col.get("COLUMN_NAME") or col.get("column_name")
            data_type = col.get("DATA_TYPE") or col.get("data_type") or ""
            cardinality = 1.0
            meta = meta_rows.get(column_name)
            if meta:
                payload = meta.metadata_payload or {}
                analysis = payload.get("analysis", {}) if isinstance(payload, dict) else {}
                unique_count = (
                    analysis.get("unique_count")
                    or analysis.get("distinct_count")
                    or payload.get("unique_count")
                    or payload.get("distinct_count")
                )
                total_count = (
                    analysis.get("total_count")
                    or payload.get("total_count")
                    or payload.get("row_count")
                    or payload.get("count")
                )
                try:
                    unique_val = float(unique_count)
                    total_val = float(total_count)
                    if total_val > 0:
                        cardinality = unique_val / total_val
                except (TypeError, ValueError):
                    pass

            column_profiles.append(
                {
                    "COLUMN_NAME": column_name,
                    "DATA_TYPE": data_type,
                    "cardinality": cardinality,
                }
            )

        chart_service = ChartService(EDAService(self.sf))
        candidates = await chart_service.generate_chart_candidates(table_ref, column_profiles)
        if limit and limit > 0:
            candidates = candidates[:limit]

        await self._update_column_analysis(
            ctx,
            {
                "chart_candidates": candidates,
            },
        )
        return {"table_asset_id": table_asset_id, "chart_candidates": candidates}

    @tool
    async def generate_numeric_visuals(
        self, table_asset_id: int, column_name: str
    ) -> dict[str, Any]:
        """Generate visuals for numeric/temporal columns and persist them."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        col = self._quote_ident(column_name)

        is_temporal = ctx.column_meta.semantic_type == "temporal"
        used_time_columns: set[str] = set()
        time_expr = None
        column_sql_type = ""
        if isinstance(ctx.column_meta.metadata_payload, dict):
            column_sql_type = str(ctx.column_meta.metadata_payload.get("sql_type") or "").lower()
        temporal_meta = await self._list_temporal_columns_with_meta(ctx.table_asset_id)
        temporal_columns = [item["column"] for item in temporal_meta if item.get("column")]
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
                COUNT(*) AS total_count
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
                STDDEV({col}) AS stddev_value,
                COUNT(*) AS total_count
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

        # 注意：Snowflake 返回字段通常是大写；你这里 SELECT 里用了 min_value/max_value，
        # 但返回 key 仍可能是 MIN_VALUE/MAX_VALUE（取决于 driver/设置）。
        # 所以这里同时兼容两种 key，避免“图表老生成不对”的隐形来源。
        def _get_stat(key_lower: str) -> Any:
            return raw_stats.get(key_lower) or raw_stats.get(key_lower.upper())

        stats = {
            "min_value": _get_stat("min_value"),
            "max_value": _get_stat("max_value"),
        }
        if is_temporal:
            stats["total_count"] = self._coerce_int(_get_stat("total_count"))
        else:
            stats["avg_value"] = _get_stat("avg_value")
            stats["stddev_value"] = _get_stat("stddev_value")
            stats["total_count"] = self._coerce_int(_get_stat("total_count"))

        # Custom visuals override
        visual_plan = self._extract_visual_overrides(ctx)
        custom_visuals, custom_errors = await self._build_custom_visuals(
            ctx, visual_plan
        )
        if custom_visuals is not None:
            if custom_errors:
                analysis_errors.extend(custom_errors)
            analysis: dict[str, Any] = {
                "visuals": custom_visuals,
                "stats": stats,
                "queries": {"stats_query": stats_query.strip()},
            }
            if analysis_errors:
                analysis["errors"] = analysis_errors
            await self._update_column_analysis(ctx, analysis)
            return {"column": column_name, "visuals": custom_visuals, "stats": stats}

        visuals: list[dict[str, Any]] = []
        histogram_data: list[dict[str, Any]] = []
        hist_query: str | None = None

        # ---- Fix 1: 直方图判断 key 修复 + min==max guard ----
        min_v = stats.get("min_value")
        max_v = stats.get("max_value")

        # 只有数值列才画直方图；且 min/max 都有；且 min!=max（否则 WIDTH_BUCKET 可能异常/无意义）
        if (
            (not is_temporal)
            and (min_v is not None)
            and (max_v is not None)
            and (min_v != max_v)
        ):
            bin_override = (
                overrides.get("visual_hist_bins")
                or overrides.get("visual_bin_count")
            )
            total_count = stats.get("total_count") or 0
            if bin_override is not None:
                bin_count = max(5, self._coerce_int(bin_override, 20))
            elif total_count:
                bin_count = min(60, max(20, int(total_count ** 0.5)))
            else:
                bin_count = 20
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
                WIDTH_BUCKET(base.{col}, stats.min_value, stats.max_value, {bin_count}) AS bin,
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
                {
                    "bin": (row.get("BIN") or row.get("bin")),
                    "count": self._coerce_int(row.get("COUNT") or row.get("count")),
                }
                for row in histogram_rows
            ]

            if histogram_data:
                visuals.append(
                    self._build_chart_spec(
                        chart_type="bar",
                        title=f"Distribution of {column_name}",
                        x_key="bin",
                        y_key="count",
                        data=histogram_data,
                        narrative=[
                            f"Distribution based on {bin_count} bins",
                            f"Min: {min_v}, Max: {max_v}",
                        ],
                        source_columns=[column_name],
                        x_title=f"{column_name} (bin)",
                        y_title="Count",
                    )
                )
        elif (
            (not is_temporal)
            and (min_v is not None)
            and (max_v is not None)
            and (min_v == max_v)
        ):
            # 可选：给一个更友好的提示，避免用户觉得“怎么没直方图”
            analysis_errors.append(
                {
                    "step": "histogram_skipped",
                    "detail": "Histogram skipped because min_value == max_value (all values identical).",
                }
            )

        # ---- Time series ----
        time_col = None
        fallback_query: str | None = None

        if is_temporal:
            time_col = time_expr or col
        elif ctx.time_column:
            time_col = self._resolve_temporal_expr(ctx, self._quote_ident(ctx.time_column))
            used_time_columns.add(ctx.time_column)

        if time_col:
            bucket_override = (
                overrides.get("visual_time_bucket")
                or overrides.get("visual_time_granularity")
            )
            default_bucket = self._default_time_bucket(
                column_name if is_temporal else ctx.time_column
            )
            bucket = str(bucket_override or default_bucket or "day").lower()
            if bucket not in {"hour", "day", "week", "month", "raw"}:
                bucket = "day"
            limit_override = (
                overrides.get("visual_time_limit") or overrides.get("visual_point_limit")
            )
            if isinstance(limit_override, str) and limit_override.lower() == "all":
                time_limit = None
            elif limit_override in (-1, 0):
                time_limit = None
            elif limit_override is not None:
                time_limit = max(1, self._coerce_int(limit_override, 500))
            else:
                time_limit = None

            def _build_time_query(bucket_key: str) -> tuple[str, str]:
                if bucket_key == "raw":
                    time_bucket_expr = f"{time_col}"
                else:
                    time_bucket_expr = f"DATE_TRUNC('{bucket_key}', {time_col})"
                query = f"""
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
                {"" if time_limit is None else f"LIMIT {int(time_limit)}"}
                """
                return query, time_bucket_expr

            time_query, _time_bucket_expr = _build_time_query(bucket)

            try:
                time_rows = await self.sf.execute_query(time_query)
            except Exception as exc:
                logger.warning("Time series query failed for %s: %s", column_name, exc)
                analysis_errors.append({"step": "time_query", "error": str(exc)})
                time_rows = []

            # 如果只得到 0/1 个点，尝试更细粒度或 raw
            if len(time_rows) <= 1 and bucket != "raw":
                fallback_buckets = ["hour", "raw"] if bucket != "hour" else ["raw"]
                for fallback_bucket in fallback_buckets:
                    fallback_query, _ = _build_time_query(fallback_bucket)
                    try:
                        fallback_rows = await self.sf.execute_query(fallback_query)
                    except Exception as exc:
                        logger.warning(
                            "Time series fallback query failed for %s: %s",
                            column_name,
                            exc,
                        )
                        analysis_errors.append(
                            {"step": "time_query_fallback", "error": str(exc)}
                        )
                        continue
                    if len(fallback_rows) > 1:
                        time_rows = fallback_rows
                        bucket = fallback_bucket
                        time_query = fallback_query
                        break

            if is_temporal:
                time_data = [
                    {
                        "time_bucket": row.get("TIME_BUCKET") or row.get("time_bucket"),
                        "count": self._coerce_int(row.get("COUNT") or row.get("count")),
                    }
                    for row in time_rows
                ]
            else:
                time_data = [
                    {
                        "time_bucket": row.get("TIME_BUCKET") or row.get("time_bucket"),
                        "avg_value": self._coerce_float(
                            row.get("AVG_VALUE") or row.get("avg_value")
                        ),
                    }
                    for row in time_rows
                ]

            if time_data and len(time_data) > 1:
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
                            (
                                "Daily trend based on counts"
                                if is_temporal
                                else "Daily trend based on average values"
                            ),
                            "Look for seasonality or breaks in trend",
                        ],
                        source_columns=(
                            [column_name]
                            if is_temporal
                            else [ctx.time_column, column_name]
                        ),
                        x_title=time_title,
                        y_title=y_title,
                    )
                )
            elif is_temporal:
                # temporal 列解析失败时 fallback
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
                    logger.warning(
                        "Temporal fallback query failed for %s: %s", column_name, exc
                    )
                    analysis_errors.append(
                        {"step": "temporal_fallback_query", "error": str(exc)}
                    )
                    fallback_rows = []

                fallback_data = [
                    {
                        "category": row.get("CATEGORY") or row.get("category"),
                        "count": self._coerce_int(row.get("COUNT") or row.get("count")),
                    }
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
                    analysis_errors.append(
                        {
                            "step": "temporal_parse_fallback",
                            "detail": "No valid timestamps parsed; used raw value distribution.",
                        }
                    )

        # ---- Extra temporal columns for numeric ----
        if not is_temporal:

            # 防止 visuals 爆炸：最多补几个时间列趋势（默认 4，可 override）
            extra_limit = self._coerce_int(
                overrides.get("visual_time_extra_limit"), 4
            )
            if extra_limit <= 0:
                extra_limit = 0
            added = 0
            bucket_override = (
                overrides.get("visual_time_bucket")
                or overrides.get("visual_time_granularity")
            )
            default_bucket = self._default_time_bucket(
                temporal_columns[0] if temporal_columns else None
            )
            bucket = str(bucket_override or default_bucket or "day").lower()
            if bucket not in {"hour", "day", "week", "month"}:
                bucket = "day"
            limit_override = (
                overrides.get("visual_time_limit") or overrides.get("visual_point_limit")
            )
            if isinstance(limit_override, str) and limit_override.lower() == "all":
                time_limit = None
            elif limit_override in (-1, 0):
                time_limit = None
            elif limit_override is not None:
                time_limit = max(1, self._coerce_int(limit_override, 500))
            else:
                time_limit = None

            for temporal_column in temporal_columns:
                if added >= extra_limit:
                    break
                if (
                    temporal_column in used_time_columns
                    or temporal_column == column_name
                ):
                    continue
                temporal_expr = self._resolve_temporal_expr(
                    ctx, self._quote_ident(temporal_column)
                )
                column_bucket = (
                    bucket
                    if bucket_override
                    else str(self._default_time_bucket(temporal_column) or "day").lower()
                )
                if column_bucket not in {"hour", "day", "week", "month"}:
                    column_bucket = "day"

                def _build_extra_query(bucket_key: str) -> tuple[str, str]:
                    time_bucket_expr = (
                        f"{temporal_expr}"
                        if bucket_key == "raw"
                        else f"DATE_TRUNC('{bucket_key}', {temporal_expr})"
                    )
                    query = f"""
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
                    {"" if time_limit is None else f"LIMIT {int(time_limit)}"}
                    """
                    return query, time_bucket_expr

                extra_time_query, _ = _build_extra_query(column_bucket)
                try:
                    extra_rows = await self.sf.execute_query(extra_time_query)
                except Exception as exc:
                    logger.warning(
                        "Extra time series query failed for %s: %s", column_name, exc
                    )
                    analysis_errors.append(
                        {"step": "extra_time_query", "error": str(exc)}
                    )
                    extra_rows = []

                extra_data = [
                    {
                        "time_bucket": row.get("TIME_BUCKET") or row.get("time_bucket"),
                        "avg_value": self._coerce_float(
                            row.get("AVG_VALUE") or row.get("avg_value")
                        ),
                    }
                    for row in extra_rows
                ]

                if len(extra_data) <= 1 and column_bucket != "raw":
                    fallback_buckets = (
                        ["hour", "raw"] if column_bucket != "hour" else ["raw"]
                    )
                    for fallback_bucket in fallback_buckets:
                        fallback_query, _ = _build_extra_query(fallback_bucket)
                        try:
                            fallback_rows = await self.sf.execute_query(fallback_query)
                        except Exception as exc:
                            logger.warning(
                                "Extra time series fallback failed for %s: %s",
                                column_name,
                                exc,
                            )
                            analysis_errors.append(
                                {"step": "extra_time_query_fallback", "error": str(exc)}
                            )
                            continue
                        fallback_data = [
                            {
                                "time_bucket": row.get("TIME_BUCKET") or row.get("time_bucket"),
                                "avg_value": self._coerce_float(
                                    row.get("AVG_VALUE") or row.get("avg_value")
                                ),
                            }
                            for row in fallback_rows
                        ]
                        if len(fallback_data) > 1:
                            extra_data = fallback_data
                            column_bucket = fallback_bucket
                            break

                if extra_data and len(extra_data) > 1:
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
                    added += 1

        # ---- Extra categorical breakdowns for numeric ----
        if not is_temporal:
            category_limit_override = overrides.get("visual_category_limit")
            if isinstance(category_limit_override, str) and category_limit_override.lower() == "all":
                category_limit = None
            elif category_limit_override in (-1, 0):
                category_limit = None
            elif category_limit_override is not None:
                category_limit = max(5, self._coerce_int(category_limit_override, 12))
            else:
                category_limit = 12
            full_threshold = self._coerce_int(
                overrides.get("visual_category_full_threshold"), 50
            )
            categorical_columns = await self._list_categorical_columns(
                ctx.table_asset_id, max_columns=3
            )
            for category_meta in categorical_columns:
                category_column = str(category_meta.get("column") or "")
                unique_count = category_meta.get("unique_count")
                if not category_column or category_column == column_name:
                    continue
                if category_limit is None:
                    limit_for_column = None
                elif unique_count is not None and unique_count <= full_threshold:
                    limit_for_column = None
                else:
                    limit_for_column = category_limit
                cat_col = self._quote_ident(category_column)
                limit_clause = "" if limit_for_column is None else f"LIMIT {int(limit_for_column)}"
                cat_query = f"""
                WITH base AS (
                    {ctx.analysis_query}
                )
                SELECT
                    {cat_col} AS category,
                    AVG({col}) AS avg_value,
                    COUNT(*) AS count
                FROM base
                WHERE {cat_col} IS NOT NULL
                AND {col} IS NOT NULL
                GROUP BY {cat_col}
                ORDER BY count DESC
                {limit_clause}
                """
                try:
                    cat_rows = await self.sf.execute_query(cat_query)
                except Exception as exc:
                    logger.warning(
                        "Category breakdown query failed for %s: %s",
                        column_name,
                        exc,
                    )
                    analysis_errors.append({"step": "category_breakdown", "error": str(exc)})
                    cat_rows = []

                cat_data = [
                    {
                        "category": row.get("CATEGORY") or row.get("category"),
                        "avg_value": self._coerce_float(
                            row.get("AVG_VALUE") or row.get("avg_value")
                        ),
                        "count": self._coerce_int(row.get("COUNT") or row.get("count")),
                    }
                    for row in cat_rows
                ]
                if cat_data:
                    visuals.append(
                        self._build_chart_spec(
                            chart_type="bar",
                            title=f"{column_name} by {category_column}",
                            x_key="category",
                            y_key="avg_value",
                            data=cat_data,
                            narrative=[
                                f"Average {column_name} by {category_column}",
                                "Top categories by frequency",
                            ],
                            source_columns=[category_column, column_name],
                            x_title=category_column,
                            y_title=f"Average {column_name}",
                        )
                    )

        analysis: dict[str, Any] = {
            "visuals": visuals,
            "stats": stats,
            "queries": {"stats_query": stats_query.strip()},
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
        return {"column": column_name, "visuals": visuals, "stats": stats}

    @tool
    async def generate_categorical_visuals(
        self, table_asset_id: int, column_name: str
    ) -> dict[str, Any]:
        """Generate visuals for categorical columns and persist them."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        col = self._quote_ident(column_name)
        analysis_errors: list[dict[str, Any]] = []

        visual_plan = self._extract_visual_overrides(ctx)
        custom_visuals, custom_errors = await self._build_custom_visuals(
            ctx, visual_plan
        )
        if custom_visuals is not None:
            if custom_errors:
                analysis_errors.extend(custom_errors)

            # 不要返回 total_count=0 误导；至少把 total_query 也跑一下，保持一致
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
                total_count = self._coerce_int(
                    (total_rows[0].get("TOTAL_COUNT") if total_rows else 0)
                )
            except Exception as exc:
                logger.warning("Total count query failed for %s: %s", column_name, exc)
                analysis_errors.append({"step": "total_query", "error": str(exc)})
                total_count = 0

            analysis: dict[str, Any] = {
                "visuals": custom_visuals,
                "stats": {"total_count": total_count},
                "queries": {},
            }
            if analysis_errors:
                analysis["errors"] = analysis_errors
            await self._update_column_analysis(ctx, analysis)
            return {
                "column": column_name,
                "visuals": custom_visuals,
                "total_count": total_count,
            }

        # 更稳：字符串类目常见空字符串；这里做一个轻量过滤（不会影响非字符串列）
        # 如果你不想动语义，可删掉 AND TRIM(...) != '' 这行
        total_query = f"""
        WITH base AS (
            {ctx.analysis_query}
        )
        SELECT COUNT(*) AS total_count
        FROM base
        WHERE {col} IS NOT NULL
        AND TRIM(TO_VARCHAR({col})) != ''
        """
        try:
            total_rows = await self.sf.execute_query(total_query)
        except Exception as exc:
            logger.warning("Total count query failed for %s: %s", column_name, exc)
            analysis_errors.append({"step": "total_query", "error": str(exc)})
            total_rows = []
        total_count = (
            self._coerce_int(total_rows[0].get("TOTAL_COUNT")) if total_rows else 0
        )

        distinct_count: int | None = None
        distinct_query = f"""
        WITH base AS (
            {ctx.analysis_query}
        )
        SELECT COUNT(DISTINCT {col}) AS distinct_count
        FROM base
        WHERE {col} IS NOT NULL
        AND TRIM(TO_VARCHAR({col})) != ''
        """
        try:
            distinct_rows = await self.sf.execute_query(distinct_query)
            distinct_count = (
                self._coerce_int(
                    (distinct_rows[0].get("DISTINCT_COUNT") or distinct_rows[0].get("distinct_count"))
                )
                if distinct_rows
                else None
            )
        except Exception as exc:
            logger.warning("Distinct count query failed for %s: %s", column_name, exc)
            analysis_errors.append({"step": "distinct_query", "error": str(exc)})

        limit_override = (
            overrides.get("visual_top_n")
            or overrides.get("categorical_visual_top_n")
            or overrides.get("visual_category_limit")
        )
        top_limit: int | None
        if isinstance(limit_override, str) and limit_override.lower() == "all":
            top_limit = None
        elif limit_override in (-1, 0, ""):
            top_limit = None
        elif limit_override is not None:
            top_limit = max(1, self._coerce_int(limit_override, 50))
        else:
            # Default to full category list; users can override to limit rows.
            top_limit = None

        top_query = f"""
        WITH base AS (
            {ctx.analysis_query}
        )
        SELECT {col} AS category, COUNT(*) AS count
        FROM base
        WHERE {col} IS NOT NULL
        AND TRIM(TO_VARCHAR({col})) != ''
        GROUP BY category
        ORDER BY count DESC
        {"" if top_limit is None else f"LIMIT {top_limit}"}
        """
        try:
            top_rows_raw = await self.sf.execute_query(top_query)
        except Exception as exc:
            logger.warning("Top categories query failed for %s: %s", column_name, exc)
            analysis_errors.append({"step": "top_query", "error": str(exc)})
            top_rows_raw = []

        top_rows = [
            {
                "category": row.get("CATEGORY") or row.get("category"),
                "count": self._coerce_int(row.get("COUNT") or row.get("count")),
            }
            for row in top_rows_raw
        ]
        top_sum = sum(row.get("count", 0) for row in top_rows)
        add_other = (
            top_limit is not None
            and (distinct_count is None or top_limit < distinct_count)
            and total_count > top_sum
        )
        if add_other:
            top_rows.append({"category": "Other", "count": total_count - top_sum})

        visuals: list[dict[str, Any]] = [
            self._build_chart_spec(
                chart_type="bar",
                title=f"Top categories for {column_name}",
                x_key="category",
                y_key="count",
                data=top_rows,
                narrative=[
                    "Top categories shown with long-tail grouped as Other",
                    f"Total non-null rows (excluding blanks): {total_count}",
                ],
                source_columns=[column_name],
                x_title=column_name,
                y_title="Count",
            )
        ]

        # ✅ 修复缩进 & 参数对齐
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
            temporal_expr = self._resolve_temporal_expr(
                ctx, self._quote_ident(temporal_column)
            )
            bucket_override = (
                overrides.get("visual_time_bucket")
                or overrides.get("visual_time_granularity")
            )
            default_bucket = self._default_time_bucket(temporal_column)
            bucket = str(bucket_override or default_bucket or "day").lower()
            if bucket not in {"hour", "day", "week", "month"}:
                bucket = "day"
            limit_override = (
                overrides.get("visual_time_limit") or overrides.get("visual_point_limit")
            )
            if isinstance(limit_override, str) and limit_override.lower() == "all":
                time_limit = None
            elif limit_override in (-1, 0):
                time_limit = None
            elif limit_override is not None:
                time_limit = max(1, self._coerce_int(limit_override, 500))
            else:
                time_limit = None
            def _build_time_query(bucket_key: str) -> tuple[str, str]:
                time_bucket_expr = (
                    f"{temporal_expr}"
                    if bucket_key == "raw"
                    else f"DATE_TRUNC('{bucket_key}', {temporal_expr})"
                )
                query = f"""
                WITH base AS (
                    {ctx.analysis_query}
                )
                SELECT
                    TO_VARCHAR({time_bucket_expr}) AS time_bucket,
                    COUNT(*) AS count
                FROM base
                WHERE {temporal_expr} IS NOT NULL
                AND {col} IS NOT NULL
                AND TRIM(TO_VARCHAR({col})) != ''
                GROUP BY {time_bucket_expr}
                ORDER BY time_bucket
                {"" if time_limit is None else f"LIMIT {int(time_limit)}"}
                """
                return query, time_bucket_expr

            time_query, _ = _build_time_query(bucket)
            try:
                time_rows = await self.sf.execute_query(time_query)
            except Exception as exc:
                logger.warning(
                    "Categorical time series query failed for %s: %s", column_name, exc
                )
                analysis_errors.append(
                    {"step": "categorical_time_query", "error": str(exc)}
                )
                time_rows = []

            time_data = [
                {
                    "time_bucket": row.get("TIME_BUCKET") or row.get("time_bucket"),
                    "count": self._coerce_int(row.get("COUNT") or row.get("count")),
                }
                for row in time_rows
            ]
            if len(time_data) <= 1 and bucket != "raw":
                fallback_buckets = ["hour", "raw"] if bucket != "hour" else ["raw"]
                for fallback_bucket in fallback_buckets:
                    fallback_query, _ = _build_time_query(fallback_bucket)
                    try:
                        fallback_rows = await self.sf.execute_query(fallback_query)
                    except Exception as exc:
                        logger.warning(
                            "Categorical time series fallback failed for %s: %s",
                            column_name,
                            exc,
                        )
                        analysis_errors.append(
                            {"step": "categorical_time_query_fallback", "error": str(exc)}
                        )
                        continue
                    fallback_data = [
                        {
                            "time_bucket": row.get("TIME_BUCKET") or row.get("time_bucket"),
                            "count": self._coerce_int(row.get("COUNT") or row.get("count")),
                        }
                        for row in fallback_rows
                    ]
                    if len(fallback_data) > 1:
                        time_data = fallback_data
                        bucket = fallback_bucket
                        break

            if time_data and len(time_data) > 1:
                visuals.append(
                    self._build_chart_spec(
                        chart_type="line",
                        # ✅ 标题别误导：这是“该列非空记录数随时间变化”
                        title=f"{column_name} non-null count over {temporal_column}",
                        x_key="time_bucket",
                        y_key="count",
                        data=time_data,
                        narrative=[
                            "Daily trend of rows where this category column is present",
                            f"Time dimension: {temporal_column}",
                        ],
                        source_columns=[temporal_column, column_name],
                        x_title=temporal_column,
                        y_title="Count",
                    )
                )

        numeric_columns = [
            name
            for name in await self._list_numeric_columns(ctx.table_asset_id)
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
                AND TRIM(TO_VARCHAR({col})) != ''
                GROUP BY category
                ORDER BY COUNT(*) DESC
                {"" if top_limit is None else f"LIMIT {top_limit}"}
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
                logger.warning(
                    "Categorical numeric query failed for %s: %s", column_name, exc
                )
                analysis_errors.append(
                    {"step": "categorical_numeric_query", "error": str(exc)}
                )
                numeric_rows = []

            numeric_data = [
                {
                    "category": row.get("CATEGORY") or row.get("category"),
                    "avg_value": self._coerce_float(
                        row.get("AVG_VALUE") or row.get("avg_value")
                    ),
                }
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
                            f"Average {numeric_column} across top categories (by frequency)",
                            "Use to compare category-level magnitude",
                        ],
                        source_columns=[column_name, numeric_column],
                        x_title=column_name,
                        y_title=f"Average {numeric_column}",
                    )
                )

        analysis: dict[str, Any] = {
            "visuals": visuals,
            "stats": {
                "total_count": total_count,
                "distinct_count": distinct_count,
            },
            "queries": {
                "top_query": top_query.strip(),
                "total_query": total_query.strip(),
                "distinct_query": distinct_query.strip(),
            },
        }
        if analysis_errors:
            analysis["errors"] = analysis_errors

        await self._update_column_analysis(ctx, analysis)
        return {"column": column_name, "visuals": visuals, "total_count": total_count}
