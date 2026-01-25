"""Column workflow tool mixin."""

from __future__ import annotations

from typing import Any
from strands import tool

class ColumnWorkflowAnalysisMixin:
    """Tool mixin."""

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

