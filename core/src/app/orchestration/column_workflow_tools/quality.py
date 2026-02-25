"""Column workflow tool mixin."""

from __future__ import annotations

from datetime import datetime
import json
import re
import uuid
from typing import Any
from strands import tool

class ColumnWorkflowQualityMixin:
    """Tool mixin."""

    def _normalize_group_by(self, value: Any) -> list[str]:
        if not value:
            return []
        if isinstance(value, str):
            items = [item.strip() for item in value.split(",") if item.strip()]
        elif isinstance(value, (list, tuple, set)):
            items = list(value)
        else:
            items = []
        return [str(item) for item in items if item]

    def _is_id_like_group_column(self, name: str, semantic_type: str | None = None) -> bool:
        lowered = str(name or "").strip().lower()
        if not lowered:
            return False
        if str(semantic_type or "").strip().lower() == "id":
            return True
        if lowered in {"id", "uuid", "pk", "primary_key"}:
            return True
        if lowered.endswith("id") or "_id" in lowered:
            return True
        if "uuid" in lowered:
            return True
        return bool(re.search(r"(^|_)(id|key|code)$", lowered))

    def _is_likely_id_grouping_conflict(
        self,
        group_by_columns: list[str],
        conflicts: dict[str, Any],
        semantic_map: dict[str, str] | None = None,
    ) -> bool:
        if not group_by_columns:
            return False
        if str(conflicts.get("type") or "") != "group_by_inconsistency":
            return False
        semantic_map = semantic_map or {}
        id_like_count = sum(
            1
            for col in group_by_columns
            if self._is_id_like_group_column(col, semantic_map.get(col))
        )
        if id_like_count <= 0:
            return False
        conflict_rate = self._coerce_float(conflicts.get("conflict_rate"), 0.0) or 0.0
        conflict_groups = self._coerce_int(conflicts.get("conflict_groups"), 0)
        group_count = self._coerce_int(conflicts.get("group_count"), 0)
        max_distinct = self._coerce_int(conflicts.get("max_distinct"), 0)
        return (
            conflict_groups > 0
            and group_count > 0
            and conflict_groups == group_count
            and conflict_rate >= 0.95
            and max_distinct >= 5
        )

    async def _infer_alternative_conflict_group_by_columns(
        self,
        ctx: Any,
        column_name: str,
        current_group_by: list[str],
        *,
        max_columns: int = 2,
    ) -> list[str]:
        try:
            all_columns = await self._list_all_columns(ctx.table_asset_id)
            semantic_map = await self._get_semantic_type_map(ctx.table_asset_id)
        except Exception:
            return []
        excluded = {str(column_name).strip().lower()}
        excluded.update(str(item).strip().lower() for item in (current_group_by or []) if item)

        scored: list[tuple[float, str]] = []
        for raw_name in all_columns or []:
            name = str(raw_name or "").strip()
            if not name:
                continue
            lowered = name.lower()
            if lowered in excluded:
                continue
            semantic = str(semantic_map.get(name) or "").lower()
            if self._is_id_like_group_column(name, semantic):
                continue
            score = 0.0
            if semantic == "categorical":
                score += 55.0
            elif semantic == "text":
                score += 42.0
            elif semantic == "temporal":
                score += 38.0
            elif semantic == "numeric":
                score += 8.0
            else:
                score += 4.0

            if self._looks_like_year_name(name):
                score += 34.0
            if any(
                token in lowered
                for token in (
                    "model",
                    "segment",
                    "category",
                    "class",
                    "type",
                    "style",
                    "brand",
                    "series",
                    "year",
                    "month",
                    "quarter",
                    "region",
                    "market",
                    "country",
                    "state",
                )
            ):
                score += 26.0
            if any(
                token in lowered
                for token in (
                    "amount",
                    "price",
                    "fare",
                    "distance",
                    "duration",
                    "score",
                    "count",
                    "ratio",
                    "rate",
                    "pct",
                )
            ):
                score -= 20.0

            if score > 0:
                scored.append((score, name))

        scored.sort(key=lambda item: (item[0], item[1].lower()), reverse=True)
        selected: list[str] = []
        for score, name in scored:
            if score < 25:
                continue
            selected.append(name)
            if len(selected) >= max(1, max_columns):
                break
        return selected

    async def _persist_conflict_group_by_override(
        self,
        ctx: Any,
        group_by_columns: list[str],
    ) -> bool:
        group_by = [str(item) for item in (group_by_columns or []) if item]
        if not group_by:
            return False
        try:
            await self.db.refresh(ctx.column_meta)
            overrides = dict(ctx.column_meta.overrides or {})
            if overrides.get("conflict_group_columns") == group_by:
                return False
            overrides["conflict_group_columns"] = group_by
            ctx.column_meta.overrides = overrides
            await self.db.commit()
            await self.db.refresh(ctx.column_meta)
            return True
        except Exception:
            await self.db.rollback()
            return False

    def _normalize_plan_payload(self, plan: Any) -> dict[str, Any]:
        if isinstance(plan, str):
            try:
                plan = json.loads(plan)
            except json.JSONDecodeError:
                return {}
        return plan if isinstance(plan, dict) else {}

    def _normalize_conflict_value_samples(self, payload: Any) -> list[dict[str, Any]]:
        raw_values = payload
        if isinstance(raw_values, str):
            text = raw_values.strip()
            if not text:
                return []
            try:
                raw_values = json.loads(text)
            except json.JSONDecodeError:
                return []
        if isinstance(raw_values, dict):
            raw_values = [raw_values]
        if not isinstance(raw_values, list):
            return []

        normalized: list[dict[str, Any]] = []
        for item in raw_values:
            parsed_item = item
            if isinstance(parsed_item, str):
                text = parsed_item.strip()
                if not text:
                    continue
                try:
                    parsed_item = json.loads(text)
                except json.JSONDecodeError:
                    parsed_item = {"value": text}

            if isinstance(parsed_item, dict):
                value = parsed_item.get("value")
                count = self._coerce_int(parsed_item.get("count"), 0)
            else:
                value = parsed_item
                count = 0

            if value is None:
                continue
            normalized_entry: dict[str, Any] = {"value": value}
            if count > 0:
                normalized_entry["count"] = count
            normalized.append(normalized_entry)

        return normalized

    def _coerce_table_ref(
        self,
        value: Any,
        ctx_table_ref: str | None,
        *,
        allow_cross_schema: bool = False,
    ) -> str | None:
        if value is None:
            return None
        raw = str(value).strip()
        if not raw:
            return None
        if not re.fullmatch(r'[A-Za-z0-9_."$]+', raw):
            return None
        parts = self._split_table_ref(raw)
        if not parts:
            return None
        if ctx_table_ref and not allow_cross_schema:
            ctx_parts = self._split_table_ref(ctx_table_ref)
            if len(parts) >= 2 and len(ctx_parts) >= 2:
                if parts[:-1] != ctx_parts[:-1]:
                    return None
        return raw

    def _coerce_sql_type(self, value: Any, default: str = "VARIANT") -> str:
        raw = str(value or "").strip()
        if not raw:
            return default
        if not re.fullmatch(r"[A-Za-z0-9_(), ]+", raw):
            return default
        return raw

    def _resolve_fixing_table_config(
        self,
        ctx: Any,
        column_name: str,
        overrides: dict[str, Any],
        plan: dict[str, Any] | None,
    ) -> dict[str, Any]:
        override_table = (
            overrides.get("data_fix_target_table")
            or overrides.get("fixing_table_name")
            or overrides.get("fixing_table")
        )
        target_table = self._coerce_table_ref(override_table, ctx.table_ref)
        if not target_table:
            target_table = self._build_fixing_table_ref(ctx.table_ref, column_name)

        column_mode = (
            overrides.get("data_fix_column_mode")
            or overrides.get("fixing_column_mode")
            or "overwrite"
        )
        column_mode = str(column_mode).lower()
        if column_mode in {"new", "new_column", "append", "copy"}:
            column_mode = "new_column"
        else:
            column_mode = "overwrite"

        target_column = column_name
        if column_mode == "new_column":
            override_column = (
                overrides.get("data_fix_target_column")
                or overrides.get("fixing_column_name")
            )
            target_column = str(override_column) if override_column else f"{column_name}__fixed"

        create_mode = (
            overrides.get("data_fix_table_mode")
            or overrides.get("fixing_table_create_mode")
            or "replace"
        )
        create_mode = str(create_mode).lower()
        if create_mode in {"create_if_missing", "if_missing", "if_not_exists", "create"}:
            create_mode = "create_if_missing"
        else:
            create_mode = "replace"

        return {
            "target_table": target_table,
            "target_column": target_column,
            "column_mode": column_mode,
            "create_mode": create_mode,
        }

    def _snapshots_compatible(
        self, plan_snapshot: dict[str, Any] | None, current_snapshot: dict[str, Any] | None
    ) -> bool:
        if not isinstance(plan_snapshot, dict) or not isinstance(current_snapshot, dict):
            return False
        if plan_snapshot.get("error") or current_snapshot.get("error"):
            return False
        plan_fp = plan_snapshot.get("query_fingerprint")
        current_fp = current_snapshot.get("query_fingerprint")
        if plan_fp and current_fp and plan_fp != current_fp:
            return False

        def _coerce_optional_int(value: Any) -> int | None:
            if value is None:
                return None
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, int):
                return value
            if isinstance(value, float):
                return int(value)
            try:
                return int(float(str(value)))
            except (TypeError, ValueError):
                return None

        for key in ("total_count", "null_count", "conflict_rows"):
            left = _coerce_optional_int(plan_snapshot.get(key))
            right = _coerce_optional_int(current_snapshot.get(key))
            if left is not None and right is not None and left != right:
                return False
        return True

    async def _infer_conflict_group_by_columns(
        self,
        ctx: Any,
        column_name: str,
        *,
        max_columns: int = 1,
    ) -> list[str]:
        """Infer stable grouping keys for conflict detection when user did not configure any."""
        try:
            all_columns = await self._list_all_columns(ctx.table_asset_id)
            semantic_map = await self._get_semantic_type_map(ctx.table_asset_id)
        except Exception:
            return []
        scored: list[tuple[float, str]] = []
        for raw_name in all_columns or []:
            name = str(raw_name or "").strip()
            if not name or name.lower() == column_name.lower():
                continue
            lowered = name.lower()
            semantic = str(semantic_map.get(name) or "").lower()
            score = 0.0
            if semantic == "id":
                score += 120.0
            elif semantic == "temporal":
                score += 24.0
            elif semantic in {"categorical", "text"}:
                score += 6.0
            elif semantic in {"numeric", "image", "binary"}:
                score -= 28.0

            if lowered in {"legid", "routeid", "bookingid", "itineraryid", "pnr"}:
                score += 60.0
            if lowered.endswith("id") or "_id" in lowered or "uuid" in lowered:
                score += 45.0
            if any(token in lowered for token in ("date", "time", "timestamp")):
                score += 8.0
            if any(
                token in lowered
                for token in (
                    "fare",
                    "price",
                    "distance",
                    "duration",
                    "amount",
                    "count",
                    "rate",
                    "score",
                )
            ):
                score -= 24.0
            if score > 0:
                scored.append((score, name))

        scored.sort(key=lambda item: (item[0], item[1].lower()), reverse=True)
        if not scored:
            return []
        selected: list[str] = []
        for score, name in scored:
            if score < 40:
                continue
            selected.append(name)
            if len(selected) >= max(1, max_columns):
                break
        return selected

    def _confusable_signature(self, value: str) -> str:
        normalized = re.sub(r"[^A-Z0-9]+", "", str(value or "").upper())
        if not normalized:
            return ""
        table = str.maketrans(
            {
                "0": "O",
                "1": "I",
                "2": "Z",
                "5": "S",
                "6": "G",
                "8": "B",
                "9": "G",
            }
        )
        return normalized.translate(table)

    async def _detect_categorical_value_conflicts(
        self,
        ctx: Any,
        column_name: str,
        sample_size: int,
        *,
        max_candidates: int = 400,
        max_pairs: int = 12,
    ) -> list[dict[str, Any]]:
        """Detect likely categorical typos / near-duplicates (e.g. SFO vs SF0)."""
        col = self._quote_ident(column_name)
        base_query = self._build_windowed_query(
            ctx,
            sample_size,
            None,
            None,
            focus_column=column_name,
            prefer_non_null=True,
        )
        query = f"""
        WITH base AS (
            {base_query}
        )
        SELECT
            TO_VARCHAR({col}) AS category,
            COUNT(*) AS count
        FROM base
        WHERE {col} IS NOT NULL
        GROUP BY 1
        ORDER BY count DESC
        LIMIT {int(max_candidates)}
        """
        try:
            rows = await self.sf.execute_query(query)
        except Exception:
            return []
        entries: list[dict[str, Any]] = []
        total = 0
        for row in rows or []:
            value = str(row.get("CATEGORY") or row.get("category") or "").strip()
            count = self._coerce_int(row.get("COUNT") or row.get("count"))
            if not value or count <= 0:
                continue
            total += count
            entries.append({"value": value, "count": count, "sig": self._confusable_signature(value)})
        if not entries:
            return []

        rare_threshold = max(2, int(total * 0.01))
        anomalies: list[dict[str, Any]] = []
        seen_pairs: set[tuple[str, str]] = set()

        by_sig: dict[str, list[dict[str, Any]]] = {}
        for item in entries:
            sig = item.get("sig") or ""
            if not sig:
                continue
            by_sig.setdefault(sig, []).append(item)

        for group in by_sig.values():
            if len(group) < 2:
                continue
            ranked = sorted(group, key=lambda item: item.get("count", 0), reverse=True)
            canonical = ranked[0]
            canonical_value = str(canonical.get("value"))
            canonical_count = self._coerce_int(canonical.get("count"))
            for variant in ranked[1:]:
                variant_value = str(variant.get("value"))
                variant_count = self._coerce_int(variant.get("count"))
                if variant_value == canonical_value:
                    continue
                pair = tuple(sorted((variant_value, canonical_value)))
                if pair in seen_pairs:
                    continue
                if variant_count > max(rare_threshold, int(canonical_count * 0.35)):
                    continue
                anomalies.append(
                    {
                        "value": variant_value,
                        "value_count": variant_count,
                        "likely_canonical": canonical_value,
                        "canonical_count": canonical_count,
                        "reason": "confusable_characters",
                    }
                )
                seen_pairs.add(pair)
                if len(anomalies) >= max_pairs:
                    return anomalies
        return anomalies

    async def _detect_year_value_conflicts(
        self,
        ctx: Any,
        column_name: str,
        sample_size: int,
        *,
        max_pairs: int = 12,
        window_days: int | None = None,
        time_column: str | None = None,
    ) -> dict[str, Any] | None:
        """Detect out-of-range / malformed values for year-like columns."""
        if not self._looks_like_year_name(column_name):
            return None
        min_year, max_year = self._year_value_bounds()
        col = self._quote_ident(column_name)
        target_time_column = time_column or ctx.time_column
        base_query = self._build_windowed_query(
            ctx,
            sample_size,
            window_days,
            target_time_column,
            focus_column=column_name,
            prefer_non_null=True,
        )
        raw_expr = "TRIM(value_text)"
        cte = f"""
        WITH base AS (
            {base_query}
        ),
        value_counts AS (
            SELECT
                TO_VARCHAR({col}) AS value_text,
                COUNT(*) AS value_count
            FROM base
            WHERE {col} IS NOT NULL
            GROUP BY 1
        ),
        tagged AS (
            SELECT
                value_text,
                value_count,
                {raw_expr} AS value_trimmed,
                IFF(
                    REGEXP_LIKE({raw_expr}, '^[+-]?[0-9]+([.][0-9]+)?$'),
                    TRY_TO_NUMBER({raw_expr}),
                    NULL
                ) AS numeric_value,
                TRY_TO_NUMBER(REGEXP_SUBSTR({raw_expr}, '^[0-9]{{4}}')) AS extracted_year
            FROM value_counts
        )
        """.strip()
        valid_numeric_expr = (
            f"numeric_value IS NOT NULL "
            f"AND ROUND(numeric_value, 0) = numeric_value "
            f"AND numeric_value BETWEEN {min_year} AND {max_year}"
        )
        valid_date_expr = (
            f"extracted_year IS NOT NULL "
            f"AND extracted_year BETWEEN {min_year} AND {max_year} "
            "AND REGEXP_LIKE(value_trimmed, '^[0-9]{4}([-/][0-9]{1,2}([-/][0-9]{1,2})?)?([ T].*)?$')"
        )
        valid_expr = f"(({valid_numeric_expr}) OR ({valid_date_expr}))"

        summary_query = f"""
        {cte}
        SELECT
            COALESCE(SUM(value_count), 0) AS non_null_count,
            COALESCE(SUM(CASE WHEN NOT ({valid_expr}) THEN value_count ELSE 0 END), 0) AS anomaly_count
        FROM tagged
        """.strip()
        sample_query = f"""
        {cte}
        SELECT
            value_text,
            value_count,
            numeric_value,
            extracted_year,
            CASE
                WHEN numeric_value IS NOT NULL AND ROUND(numeric_value, 0) != numeric_value THEN 'non_integer_year'
                WHEN numeric_value IS NOT NULL AND ROUND(numeric_value, 0) < {min_year} THEN 'year_below_range'
                WHEN numeric_value IS NOT NULL AND ROUND(numeric_value, 0) > {max_year} THEN 'year_above_range'
                WHEN extracted_year IS NOT NULL AND extracted_year < {min_year} THEN 'year_below_range'
                WHEN extracted_year IS NOT NULL AND extracted_year > {max_year} THEN 'year_above_range'
                ELSE 'not_year_format'
            END AS reason
        FROM tagged
        WHERE NOT ({valid_expr})
        ORDER BY value_count DESC, value_text
        LIMIT {int(max_pairs)}
        """.strip()
        try:
            summary_rows = await self.sf.execute_query(summary_query)
            summary_row = summary_rows[0] if summary_rows else {}
            non_null_count = self._coerce_int(
                summary_row.get("NON_NULL_COUNT") or summary_row.get("non_null_count")
            )
            anomaly_count = self._coerce_int(
                summary_row.get("ANOMALY_COUNT") or summary_row.get("anomaly_count")
            )
            if anomaly_count <= 0:
                return None
            sample_rows = await self.sf.execute_query(sample_query)
        except Exception:
            return None

        suggested_range = f"{min_year}-{max_year}"
        anomalies: list[dict[str, Any]] = []
        for row in sample_rows or []:
            raw_value = row.get("VALUE_TEXT") or row.get("value_text")
            value_count = self._coerce_int(row.get("VALUE_COUNT") or row.get("value_count"))
            reason = str(row.get("REASON") or row.get("reason") or "invalid_year")
            numeric_value = self._coerce_float(
                row.get("NUMERIC_VALUE") or row.get("numeric_value")
            )
            extracted_year = self._coerce_float(
                row.get("EXTRACTED_YEAR") or row.get("extracted_year")
            )
            parsed_year = None
            if numeric_value is not None and float(numeric_value).is_integer():
                parsed_year = int(numeric_value)
            elif extracted_year is not None and float(extracted_year).is_integer():
                parsed_year = int(extracted_year)
            anomalies.append(
                {
                    "value": raw_value,
                    "value_count": value_count,
                    "likely_canonical": suggested_range,
                    "canonical_count": None,
                    "reason": reason,
                    "parsed_year": parsed_year,
                }
            )

        anomaly_rate = round(anomaly_count / non_null_count, 6) if non_null_count else None
        return {
            "range_min": min_year,
            "range_max": max_year,
            "non_null_count": non_null_count,
            "anomaly_count": anomaly_count,
            "anomaly_rate": anomaly_rate,
            "value_conflicts": anomalies,
        }

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
        base_query = self._build_windowed_query(
            ctx,
            sample_size,
            window_days,
            target_time_column,
        )
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
        snapshot_id = self._get_analysis_snapshot_id(ctx)
        nulls = {
            "total_count": total_count,
            "null_count": null_count,
            "null_rate": null_rate,
            "snapshot_id": snapshot_id,
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
        sample_group_limit: int = 12,
        sample_value_limit: int = 8,
        window_days: int | None = None,
        time_column: str | None = None,
        auto_regroup: bool = True,
    ) -> dict[str, Any]:
        """Detect conflicting values within groups defined by other columns."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        if overrides.get("scan_conflicts_auto_regroup") is False:
            auto_regroup = False
        group_by = group_by_columns or overrides.get("conflict_group_columns") or []
        override_sample_size = overrides.get("scan_conflicts_sample_size")
        if override_sample_size is not None:
            sample_size = self._coerce_int(override_sample_size) or sample_size
        override_group_limit = overrides.get("scan_conflicts_group_limit") or overrides.get(
            "conflict_group_limit"
        )
        if override_group_limit is not None:
            sample_group_limit = (
                self._coerce_int(override_group_limit) or sample_group_limit
            )
        override_value_limit = overrides.get("scan_conflicts_value_limit") or overrides.get(
            "conflict_value_limit"
        )
        if override_value_limit is not None:
            sample_value_limit = (
                self._coerce_int(override_value_limit) or sample_value_limit
            )
        override_window_days = overrides.get("scan_conflicts_window_days")
        if override_window_days is not None:
            window_days = self._coerce_int(override_window_days)
        override_time_column = overrides.get("scan_conflicts_time_column")
        if override_time_column:
            time_column = str(override_time_column)
        if isinstance(group_by, str):
            group_by = [item.strip() for item in group_by.split(",") if item.strip()]
        group_by = [str(item) for item in group_by if item]
        auto_group_by: list[str] = []
        if not group_by:
            auto_group_by = await self._infer_conflict_group_by_columns(
                ctx, column_name, max_columns=1
            )
            group_by = auto_group_by
        year_conflict_payload = await self._detect_year_value_conflicts(
            ctx,
            column_name,
            sample_size,
            max_pairs=max(6, self._coerce_int(sample_value_limit, 8)),
            window_days=window_days,
            time_column=time_column,
        )
        year_value_conflicts: list[dict[str, Any]] = []
        if isinstance(year_conflict_payload, dict):
            year_value_conflicts = list(year_conflict_payload.get("value_conflicts") or [])
        if not group_by:
            value_conflicts: list[dict[str, Any]] = []
            if ctx.column_meta.semantic_type in {"categorical", "text"}:
                value_conflicts = await self._detect_categorical_value_conflicts(
                    ctx, column_name, sample_size
                )
            combined_value_conflicts = [*value_conflicts, *year_value_conflicts]
            if combined_value_conflicts:
                year_anomaly_count = (
                    self._coerce_int(year_conflict_payload.get("anomaly_count"))
                    if isinstance(year_conflict_payload, dict)
                    else 0
                )
                year_non_null_count = (
                    self._coerce_int(year_conflict_payload.get("non_null_count"))
                    if isinstance(year_conflict_payload, dict)
                    else 0
                )
                year_anomaly_rate = (
                    year_conflict_payload.get("anomaly_rate")
                    if isinstance(year_conflict_payload, dict)
                    else None
                )
                conflict_type = (
                    "year_value_anomaly"
                    if year_value_conflicts and not value_conflicts
                    else "categorical_value_anomaly"
                )
                definition = (
                    "Year-like column contains out-of-range or malformed year values."
                    if year_value_conflicts and not value_conflicts
                    else "Potential category typos or inconsistent spellings detected."
                )
                conflicts = {
                    "type": conflict_type,
                    "definition": definition,
                    "group_by_columns": [],
                    "group_count": year_non_null_count or None,
                    "conflict_groups": year_anomaly_count or len(combined_value_conflicts),
                    "conflict_rate": year_anomaly_rate,
                    "max_distinct": None,
                    "sample_group_limit": sample_group_limit,
                    "sample_value_limit": sample_value_limit,
                    "sample_size": sample_size,
                    "sampled_rows": sample_size,
                    "sample_size_requested": sample_size,
                    "sample_window_days": window_days,
                    "auto_group_by_columns": [],
                    "value_conflict_count": len(combined_value_conflicts),
                    "value_conflicts": combined_value_conflicts,
                }
                if year_value_conflicts and isinstance(year_conflict_payload, dict):
                    conflicts["year_anomaly_count"] = year_anomaly_count
                    conflicts["year_anomaly_rate"] = year_anomaly_rate
                    conflicts["year_valid_range"] = {
                        "min": year_conflict_payload.get("range_min"),
                        "max": year_conflict_payload.get("range_max"),
                    }
                    conflicts["year_value_conflicts"] = year_value_conflicts
                await self._update_column_analysis(ctx, {"conflicts": conflicts})
                return {"column": column_name, "conflicts": conflicts}
            conflicts = {
                "skipped": True,
                "reason": "group_by_columns_missing",
                "auto_group_by_columns": [],
            }
            await self._update_column_analysis(ctx, {"conflicts": conflicts})
            return {"column": column_name, "conflicts": conflicts}
        try:
            available_columns = {name.lower() for name in await self._list_all_columns(ctx.table_asset_id)}
        except Exception:
            available_columns = set()
        if available_columns:
            invalid_columns = [name for name in group_by if name.lower() not in available_columns]
            group_by = [name for name in group_by if name.lower() in available_columns]
            if invalid_columns:
                conflicts = {
                    "skipped": True,
                    "reason": "group_by_columns_invalid",
                    "invalid_columns": invalid_columns,
                }
                await self._update_column_analysis(ctx, {"conflicts": conflicts})
                return {"column": column_name, "conflicts": conflicts}
            if not group_by:
                conflicts = {"skipped": True, "reason": "group_by_columns_missing"}
                await self._update_column_analysis(ctx, {"conflicts": conflicts})
                return {"column": column_name, "conflicts": conflicts}

        target_time_column = time_column or ctx.time_column
        base_query = self._build_windowed_query(
            ctx,
            sample_size,
            window_days,
            target_time_column,
            focus_column=column_name,
            prefer_non_null=True,
        )
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
            (SELECT COUNT(*) FROM base WHERE {col} IS NOT NULL) AS sampled_rows,
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
        sampled_rows = self._coerce_int(row.get("SAMPLED_ROWS"))
        group_count = self._coerce_int(row.get("GROUP_COUNT"))
        conflict_groups = self._coerce_int(row.get("CONFLICT_GROUPS"))
        conflict_rate = round(conflict_groups / group_count, 6) if group_count else None
        conflicts = {
            "type": "group_by_inconsistency",
            "definition": "Multiple distinct values appear within the same group.",
            "group_by_columns": group_by,
            "auto_group_by_columns": auto_group_by or None,
            "group_count": group_count,
            "conflict_groups": conflict_groups,
            "conflict_rate": conflict_rate,
            "max_distinct": self._coerce_int(row.get("MAX_DISTINCT")),
            "sample_group_limit": sample_group_limit,
            "sample_value_limit": sample_value_limit,
            # Keep sample_size as the actual scanned row count for UI accuracy.
            "sample_size": sampled_rows,
            "sampled_rows": sampled_rows,
            "sample_size_requested": sample_size,
            "sample_window_days": window_days,
        }
        if conflict_groups and group_by and sample_group_limit and sample_value_limit:
            try:
                group_exprs = ", ".join(self._quote_ident(name) for name in group_by)
                group_aliases = ", ".join(
                    f"ranked.{self._quote_ident(name)} AS group_{idx}"
                    for idx, name in enumerate(group_by)
                )
                join_on_ranked = " AND ".join(
                    [
                        f"ranked.{self._quote_ident(name)} = conflict_groups.{self._quote_ident(name)}"
                        for name in group_by
                    ]
                )
                join_on_base = " AND ".join(
                    [
                        f"base.{self._quote_ident(name)} = conflict_groups.{self._quote_ident(name)}"
                        for name in group_by
                    ]
                )
                group_exprs_base = ", ".join(
                    f"base.{self._quote_ident(name)}" for name in group_by
                )
                group_exprs_ranked = ", ".join(
                    f"ranked.{self._quote_ident(name)}" for name in group_by
                )
                sample_query = f"""
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
                ),
                conflict_groups AS (
                    SELECT {group_exprs}, distinct_values
                    FROM grouped
                    WHERE distinct_values > 1
                    ORDER BY distinct_values DESC
                    LIMIT {int(sample_group_limit)}
                ),
                value_counts AS (
                    SELECT
                        {group_exprs_base},
                        base.{col} AS val,
                        COUNT(*) AS value_count
                    FROM base
                    JOIN conflict_groups
                    ON {join_on_base}
                    WHERE base.{col} IS NOT NULL
                    GROUP BY {group_exprs_base}, base.{col}
                ),
                ranked AS (
                    SELECT
                        {group_exprs},
                        val,
                        value_count,
                        ROW_NUMBER() OVER (
                            PARTITION BY {group_exprs}
                            ORDER BY value_count DESC, val
                        ) AS rn
                    FROM value_counts
                )
                SELECT
                    {group_aliases},
                    MAX(conflict_groups.distinct_values) AS distinct_values,
                    SUM(ranked.value_count) AS total_rows,
                    ARRAY_AGG(
                        OBJECT_CONSTRUCT('value', ranked.val, 'count', ranked.value_count)
                    ) WITHIN GROUP (ORDER BY ranked.value_count DESC, ranked.val) AS value_samples
                FROM ranked
                JOIN conflict_groups
                ON {join_on_ranked}
                WHERE ranked.rn <= {int(sample_value_limit)}
                GROUP BY {group_exprs_ranked}
                ORDER BY total_rows DESC, distinct_values DESC
                """.strip()
                sample_rows = await self.sf.execute_query(sample_query)
                sample_groups: list[dict[str, Any]] = []
                for row in sample_rows or []:
                    group_payload: dict[str, Any] = {}
                    for idx, name in enumerate(group_by):
                        key = f"GROUP_{idx}"
                        if key in row:
                            group_payload[name] = row.get(key)
                        else:
                            group_payload[name] = row.get(key.upper())
                    values = None
                    if isinstance(row, dict):
                        values = row.get("VALUE_SAMPLES")
                        if values is None:
                            values = row.get("value_samples")
                    normalized_values = self._normalize_conflict_value_samples(values)
                    distinct_values = None
                    total_rows = None
                    if isinstance(row, dict):
                        distinct_values = row.get("DISTINCT_VALUES")
                        if distinct_values is None:
                            distinct_values = row.get("distinct_values")
                        total_rows = row.get("TOTAL_ROWS")
                        if total_rows is None:
                            total_rows = row.get("total_rows")
                    sample_groups.append(
                        {
                            "group": group_payload,
                            "distinct_values": self._coerce_int(distinct_values),
                            "total_rows": self._coerce_int(total_rows),
                            "values": normalized_values,
                        }
                    )
                conflicts["sample_groups"] = sample_groups
                conflicts["sampled_groups"] = len(sample_groups)
            except Exception as exc:
                conflicts["sample_error"] = str(exc)
        value_conflicts: list[dict[str, Any]] = []
        if ctx.column_meta.semantic_type in {"categorical", "text"}:
            value_conflicts = await self._detect_categorical_value_conflicts(
                ctx, column_name, sample_size
            )
        combined_value_conflicts = [*value_conflicts, *year_value_conflicts]
        if combined_value_conflicts:
            conflicts["value_conflict_count"] = len(combined_value_conflicts)
            conflicts["value_conflicts"] = combined_value_conflicts
        if year_value_conflicts and isinstance(year_conflict_payload, dict):
            year_anomaly_count = self._coerce_int(year_conflict_payload.get("anomaly_count"))
            year_non_null_count = self._coerce_int(year_conflict_payload.get("non_null_count"))
            year_anomaly_rate = year_conflict_payload.get("anomaly_rate")
            conflicts["year_anomaly_count"] = year_anomaly_count
            conflicts["year_anomaly_rate"] = year_anomaly_rate
            conflicts["year_valid_range"] = {
                "min": year_conflict_payload.get("range_min"),
                "max": year_conflict_payload.get("range_max"),
            }
            conflicts["year_value_conflicts"] = year_value_conflicts
            if not self._coerce_int(conflicts.get("conflict_groups")):
                conflicts["type"] = "year_value_anomaly"
                conflicts["definition"] = (
                    "Year-like column contains out-of-range or malformed year values."
                )
                conflicts["group_count"] = year_non_null_count or conflicts.get("group_count")
                conflicts["conflict_groups"] = year_anomaly_count or len(year_value_conflicts)
                conflicts["conflict_rate"] = year_anomaly_rate
        elif value_conflicts and not self._coerce_int(conflicts.get("conflict_groups")):
            conflicts["type"] = "categorical_value_anomaly"
            conflicts["definition"] = (
                "Potential category typos or inconsistent spellings detected."
            )
            conflicts["conflict_groups"] = len(value_conflicts)
            conflicts["conflict_rate"] = None

        if auto_regroup and group_by:
            semantic_map: dict[str, str] = {}
            try:
                semantic_map = await self._get_semantic_type_map(ctx.table_asset_id)
            except Exception:
                semantic_map = {}
            if self._is_likely_id_grouping_conflict(group_by, conflicts, semantic_map):
                suggested_group_by = await self._infer_alternative_conflict_group_by_columns(
                    ctx,
                    column_name,
                    group_by,
                    max_columns=max(2, len(group_by)),
                )
                if suggested_group_by:
                    retry = await self.scan_conflicts(
                        table_asset_id=table_asset_id,
                        column_name=column_name,
                        group_by_columns=suggested_group_by,
                        sample_size=sample_size,
                        sample_group_limit=sample_group_limit,
                        sample_value_limit=sample_value_limit,
                        window_days=window_days,
                        time_column=time_column,
                        auto_regroup=False,
                    )
                    retry_conflicts = (
                        retry.get("conflicts")
                        if isinstance(retry, dict)
                        else None
                    )
                    if isinstance(retry_conflicts, dict):
                        enriched_conflicts = dict(retry_conflicts)
                        enriched_conflicts["auto_regroup_applied"] = True
                        enriched_conflicts["auto_regroup_reason"] = (
                            "id_like_grouping_conflict_high_rate"
                        )
                        enriched_conflicts["original_group_by_columns"] = group_by
                        enriched_conflicts["recommended_group_by_columns"] = suggested_group_by
                        persisted = await self._persist_conflict_group_by_override(
                            ctx, suggested_group_by
                        )
                        enriched_conflicts["recommended_group_by_persisted"] = persisted
                        await self._update_column_analysis(
                            ctx, {"conflicts": enriched_conflicts}
                        )
                        return {"column": column_name, "conflicts": enriched_conflicts}
                    conflicts["recommended_group_by_columns"] = suggested_group_by
                    conflicts["auto_regroup_applied"] = False
                else:
                    conflicts["recommended_group_by_columns"] = []
                    conflicts["auto_regroup_applied"] = False

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

        group_by = self._normalize_group_by(
            conflicts.get("group_by_columns") or overrides.get("conflict_group_columns")
        )

        if group_by and not conflicts.get("conflict_groups") and not conflicts.get("skipped") and not conflicts.get("error"):
            try:
                scan_result = await self.scan_conflicts(
                    table_asset_id, column_name, group_by_columns=group_by
                )
                if isinstance(scan_result, dict):
                    conflicts = scan_result.get("conflicts") or conflicts
            except Exception:
                pass

        snapshot = await self._ensure_analysis_snapshot(ctx, column_name, group_by)
        conflict_groups = self._coerce_int(conflicts.get("conflict_groups"))
        snapshot["conflict_groups"] = conflict_groups

        total_count = self._coerce_int(snapshot.get("total_count"))
        null_count = self._coerce_int(snapshot.get("null_count"))
        sample_total = self._coerce_int(nulls.get("total_count"))
        sample_nulls = self._coerce_int(nulls.get("null_count"))
        sample_snapshot_id = nulls.get("snapshot_id")
        distribution_snapshot_id = None
        if isinstance(analysis.get("distribution"), dict):
            distribution_snapshot_id = analysis.get("distribution", {}).get("snapshot_id")
        sample_null_rate = (
            round(sample_nulls / sample_total, 6) if sample_total else None
        )
        conflict_rows = snapshot.get("conflict_rows")

        inconsistency_reasons: list[str] = []
        if not total_count and sample_total:
            inconsistency_reasons.append("full_snapshot_missing")
        snapshot_id = snapshot.get("snapshot_id")
        if sample_snapshot_id and snapshot_id and sample_snapshot_id != snapshot_id:
            mismatch = True
            if total_count and sample_total and sample_total > 0:
                ratio = abs(total_count - sample_total) / max(total_count, sample_total)
                if ratio <= 0.1 and null_count == sample_nulls:
                    mismatch = False
            if mismatch:
                inconsistency_reasons.append("snapshot_mismatch_nulls")
        if distribution_snapshot_id and snapshot_id and distribution_snapshot_id != snapshot_id:
            mismatch = True
            if total_count and sample_total and sample_total > 0:
                ratio = abs(total_count - sample_total) / max(total_count, sample_total)
                if ratio <= 0.1:
                    mismatch = False
            if mismatch:
                inconsistency_reasons.append("snapshot_mismatch_distribution")
        if null_count == 0 and (sample_nulls or 0) > 0:
            inconsistency_reasons.append("full_null_zero_sample_nonzero")

        if total_count and sample_total and sample_total > 0:
            ratio = abs(total_count - sample_total) / max(total_count, sample_total)
            if ratio > 0.1:
                inconsistency_reasons.append("total_count_mismatch")

        existing_plan = analysis.get("repair_plan")
        if isinstance(existing_plan, dict):
            existing_signature = (existing_plan.get("snapshot") or {}).get("signature")
            if existing_signature and existing_signature == snapshot.get("signature"):
                existing_null_strategy = None
                existing_conflict_strategy = None
                for step in existing_plan.get("steps", []):
                    if step.get("type") == "null_repair":
                        existing_null_strategy = step.get("strategy")
                    if step.get("type") == "conflict_repair":
                        existing_conflict_strategy = step.get("strategy")
                if (
                    (existing_null_strategy == strategy or null_count == 0)
                    and (existing_conflict_strategy == conflict_plan or conflict_groups == 0)
                    and existing_plan.get("plan_id")
                    and existing_plan.get("plan_hash")
                ):
                    return {"column": column_name, "repair_plan": existing_plan}

        row_id_column = self._resolve_row_id_column(ctx)
        audit_table = (
            overrides.get("repair_audit_table")
            or overrides.get("audit_table")
            or table_overrides.get("repair_audit_table")
            or table_overrides.get("audit_table")
        )
        audit_table = self._coerce_table_ref(audit_table, ctx.table_ref)
        apply_mode = (
            overrides.get("data_fix_target")
            or overrides.get("apply_mode")
            or "fixing_table"
        )
        apply_ready = bool(ctx.table_ref) and (
            apply_mode == "fixing_table" or bool(row_id_column)
        )
        rollback = (
            {"strategy": "audit_table", "audit_table": audit_table}
            if audit_table
            else {"strategy": "time_travel" if ctx.table_ref else "none"}
        )

        plan_steps = []
        sql_previews: dict[str, Any] = {}
        requires_manual_review = False
        forbidden = False
        if inconsistency_reasons:
            requires_manual_review = True
            forbidden = True
        if null_count:
            fill_expr, fill_value = await self._compute_null_fill_value(ctx, column_name, strategy)
            strategy_key = str(strategy or "").lower()
            null_reason = None
            basis = {
                "computed_from": "analysis_query_non_nulls",
                "method": strategy_key or "manual_review",
            }
            if fill_value is None:
                # Try a contextual, group-based imputation using similar rows.
                group_columns, group_rationale = await self._suggest_group_columns(ctx, column_name)
                segment_expr, segment_cols = await self._resolve_segment_count_expr(ctx)
                sample_size = int(
                    overrides.get("null_impute_sample_size")
                    or overrides.get("impute_sample_size")
                    or 20000
                )
                if group_columns and not group_by:
                    group_by = [str(item) for item in group_columns if item]
                    try:
                        scan_result = await self.scan_conflicts(
                            table_asset_id, column_name, group_by_columns=group_by
                        )
                        if isinstance(scan_result, dict):
                            conflicts = scan_result.get("conflicts") or conflicts
                    except Exception:
                        pass
                    snapshot = await self._ensure_analysis_snapshot(ctx, column_name, group_by)
                    conflict_groups = self._coerce_int(conflicts.get("conflict_groups"))
                    snapshot["conflict_groups"] = conflict_groups
                    total_count = self._coerce_int(snapshot.get("total_count"))
                    null_count = self._coerce_int(snapshot.get("null_count"))
                    conflict_rows = snapshot.get("conflict_rows")
                if group_columns or segment_expr:
                    group_exprs = [self._quote_ident(col) for col in group_columns if col]
                    segment_expr_base = None
                    segment_expr_b2 = None
                    if segment_expr and segment_cols:
                        segment_expr_base = segment_expr
                        segment_expr_b2 = segment_expr
                        for seg_col in segment_cols:
                            quoted = self._quote_ident(seg_col)
                            segment_expr_base = segment_expr_base.replace(quoted, f"base.{quoted}")
                            segment_expr_b2 = segment_expr_b2.replace(quoted, f"b2.{quoted}")
                    if segment_expr_base and segment_expr_b2:
                        group_exprs.append(segment_expr_b2)
                    group_predicates = []
                    for col in group_columns:
                        group_predicates.append(
                            f"b2.{self._quote_ident(col)} = base.{self._quote_ident(col)}"
                        )
                    if segment_expr_base and segment_expr_b2:
                        group_predicates.append(f"{segment_expr_b2} = {segment_expr_base}")
                    base_expr = self._numeric_expr(self._quote_ident(column_name))
                    subquery = (
                        f"SELECT APPROX_PERCENTILE({base_expr}, 0.5) "
                        f"FROM {ctx.table_ref} AS b2 "
                        f"WHERE {base_expr} IS NOT NULL"
                    )
                    if group_predicates:
                        subquery += " AND " + " AND ".join(group_predicates)
                    if sample_size:
                        subquery += f" LIMIT {sample_size}"
                    fill_expr = f"({subquery})"
                    fill_value = None
                    strategy = "group_median_impute"
                    strategy_key = "group_median_impute"
                    basis = {
                        "computed_from": "group_similarity",
                        "group_columns": group_columns,
                        "segment_columns": segment_cols,
                        "sample_size": sample_size,
                        "ai_rationale": group_rationale,
                    }
                    null_reason = (
                        "Contextual imputation using similar rows grouped by "
                        f"{', '.join(group_columns) if group_columns else 'segment count'}."
                    )
                else:
                    null_reason = (
                        "All sampled values were NULL; no non-null basis available. "
                        "Consider a group-based imputation or a manual default."
                    )
            elif strategy_key in {"median_impute", "median"}:
                null_reason = "Median of non-null values (robust to outliers)."
            elif strategy_key in {"mean_impute", "mean"}:
                null_reason = "Mean of non-null values."
            elif strategy_key in {"mode_impute", "mode"}:
                null_reason = "Most frequent non-null value."
            elif strategy_key in {"zero_impute", "zero"}:
                null_reason = "Rule-based fill with zero."
            elif strategy_key in {"empty_string", "empty"}:
                null_reason = "Rule-based fill with empty string."
            elif strategy_key in {"forward_fill", "ffill"}:
                null_reason = "Forward-fill using the last observed value in time order."
            update_sql = None
            count_sql = None
            if ctx.table_ref and fill_expr:
                if strategy_key == "group_median_impute":
                    update_sql = (
                        f"UPDATE {ctx.table_ref} AS base "
                        f"SET {self._quote_ident(column_name)} = {fill_expr} "
                        f"WHERE {self._quote_ident(column_name)} IS NULL"
                    )
                else:
                    update_sql = (
                        f"UPDATE {ctx.table_ref} SET {self._quote_ident(column_name)} = {fill_expr} "
                        f"WHERE {self._quote_ident(column_name)} IS NULL"
                    )
                count_sql = (
                    f"SELECT COUNT(*) AS affected_rows FROM {ctx.table_ref} "
                    f"WHERE {self._quote_ident(column_name)} IS NULL"
                )
            plan_steps.append(
                {
                    "type": "null_repair",
                    "strategy": strategy,
                    "estimated_rows": null_count,
                    "fill_expr": fill_expr,
                    "fill_value": fill_value,
                    "requires_manual_default": fill_expr is None,
                    "reason": null_reason,
                    "basis": basis,
                    "requires_base_alias": strategy_key == "group_median_impute",
                }
            )
            sql_previews["null_repair"] = {
                "update_sql": update_sql,
                "count_sql": count_sql,
                "estimated_rows": null_count,
            }
        elif sample_nulls:
            requires_manual_review = True
            forbidden = True

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
        if inconsistency_reasons:
            summary_parts.append("Inconsistent data stats; manual review required")
        if null_count:
            summary_parts.append(f"Null repair: {strategy} for ~{null_count} rows")
        if conflict_groups:
            summary_parts.append(
                f"Conflict repair: {conflict_plan} for {conflict_groups} groups"
            )
        if not summary_parts:
            summary_parts.append("No repair actions required")

        estimated_rows = 0
        for step in plan_steps:
            if step.get("type") == "null_repair":
                estimated_rows = max(
                    estimated_rows, self._coerce_int(step.get("estimated_rows"))
                )
        if total_count and estimated_rows and estimated_rows > total_count:
            requires_manual_review = True
            forbidden = True

        if forbidden:
            apply_ready = False

        plan_id = uuid.uuid4().hex
        plan_payload = {
            "summary": "; ".join(summary_parts),
            "steps": plan_steps,
            "snapshot": snapshot,
            "row_id_column": row_id_column,
            "apply_mode": apply_mode,
            "apply_ready": apply_ready,
            "rollback": rollback,
            "sql_previews": sql_previews,
            "token_estimate": token_estimate,
            "approval_required": True,
            "requires_manual_review": requires_manual_review,
            "inconsistent": bool(inconsistency_reasons),
            "inconsistency_reasons": inconsistency_reasons,
            "forbidden": forbidden,
            "analysis_total_rows": total_count,
            "estimator_total_rows": total_count,
            "sample_null_rate": sample_null_rate,
        }
        if null_count:
            plan_payload["rationale"] = {
                "nulls": {
                    "strategy": strategy,
                    "reason": null_reason,
                    "fill_value": fill_value,
                    "basis": basis,
                }
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
    async def generate_repair_rationale(
        self, table_asset_id: int, column_name: str
    ) -> dict[str, Any]:
        """Generate a human-readable repair rationale report for the latest plan."""
        ctx = await self._load_context(table_asset_id, column_name)
        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        plan = self._normalize_plan_payload(analysis.get("repair_plan", {}))
        if isinstance(plan, dict) and (
            plan.get("forbidden")
            or plan.get("inconsistent")
            or plan.get("requires_manual_review")
        ):
            return {
                "column": column_name,
                "skipped": True,
                "reason": "plan_inconsistent",
            }
        if not isinstance(plan, dict) or not plan.get("plan_id"):
            return {"column": column_name, "status": "error", "reason": "plan_missing"}

        payload = {
            "column": column_name,
            "semantic_type": ctx.column_meta.semantic_type,
            "stats": analysis.get("stats"),
            "nulls": analysis.get("nulls"),
            "conflicts": analysis.get("conflicts"),
            "correlations": analysis.get("correlations"),
            "plan_summary": plan.get("summary"),
            "plan_steps": plan.get("steps"),
            "rationale": plan.get("rationale"),
            "structure_type": ctx.structure_type,
        }
        instruction = (
            "Create a concise repair rationale in Markdown. Return JSON with keys: "
            "summary (string, <=120 words), why_this_value (string), "
            "row_level_rules (array, max 4), alternatives (array, max 3), "
            "risks (array, max 3). "
            "Be specific about the chosen fill value and basis. "
            "If no fill value exists, say why and suggest group-based or manual fallbacks."
        )
        report = await self._run_ai_agg(payload, instruction)
        if isinstance(report, dict):
            normalized = report
        else:
            normalized = {"summary": str(report)}

        plan["rationale_report"] = normalized
        await self._update_column_analysis(ctx, {"repair_plan": plan})
        return {"column": column_name, "rationale_report": normalized}


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
        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        plan = dict(analysis.get("repair_plan", {}))
        is_approved = bool(approved if approved is not None else overrides.get(approval_key) or plan.get("approved"))
        if not plan or not plan.get("plan_id") or not plan.get("plan_hash"):
            return {
                "column": column_name,
                "approved": False,
                "approval_key": approval_key,
                "status": "pending",
                "reason": "plan_missing",
            }
        approved_plan_id = overrides.get("data_fix_plan_id") or plan.get("plan_id")
        approved_plan_hash = overrides.get("data_fix_plan_hash") or plan.get("plan_hash")
        approved_snapshot = (
            overrides.get("data_fix_snapshot_signature")
            or (plan.get("snapshot") or {}).get("signature")
        )
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
        if is_approved:
            updated_overrides = dict(overrides)
            updated_overrides[approval_key] = True
            if approved_plan_id:
                updated_overrides["data_fix_plan_id"] = approved_plan_id
            if approved_plan_hash:
                updated_overrides["data_fix_plan_hash"] = approved_plan_hash
            if approved_snapshot:
                updated_overrides["data_fix_snapshot_signature"] = approved_snapshot
            ctx.column_meta.overrides = updated_overrides
            flag_modified(ctx.column_meta, "overrides")
            await self.db.commit()
        return {"column": column_name, "approved": is_approved, "approval_key": approval_key}


    @tool
    async def apply_data_repairs(
        self,
        table_asset_id: int,
        column_name: str,
        null_strategy: str | None = None,
        conflict_strategy: str | None = None,
        plan_id: str | None = None,
        plan_hash: str | None = None,
        snapshot_signature: str | None = None,
        approval_key: str = "data_fix_approved",
    ) -> dict[str, Any]:
        """Apply approved null/conflict repair strategies to the source table."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        if plan_id or plan_hash or snapshot_signature:
            overrides = dict(overrides)
            if plan_id:
                overrides["data_fix_plan_id"] = plan_id
            if plan_hash:
                overrides["data_fix_plan_hash"] = plan_hash
            if snapshot_signature:
                overrides["data_fix_snapshot_signature"] = snapshot_signature

        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        nulls = analysis.get("nulls", {})
        conflicts = analysis.get("conflicts", {})
        plan = self._normalize_plan_payload(analysis.get("repair_plan", {}))
        if plan and (
            plan.get("forbidden")
            or plan.get("inconsistent")
            or plan.get("requires_manual_review")
        ):
            return {
                "column": column_name,
                "skipped": True,
                "reason": "plan_inconsistent",
            }
        approved = bool(overrides.get(approval_key) or (isinstance(plan, dict) and plan.get("approved")))
        if not approved:
            return {"column": column_name, "skipped": True, "reason": "approval_required"}
        if not ctx.table_ref:
            return {"column": column_name, "skipped": True, "reason": "table_ref_missing"}

        table_overrides = ctx.table_meta.overrides or {}
        row_id_column = plan.get("row_id_column") or self._resolve_row_id_column(ctx)
        audit_table = (
            overrides.get("repair_audit_table")
            or overrides.get("audit_table")
            or table_overrides.get("repair_audit_table")
            or table_overrides.get("audit_table")
        )
        audit_table = self._coerce_table_ref(audit_table, ctx.table_ref)
        audit_disabled_reason = None
        if audit_table and not row_id_column:
            audit_disabled_reason = "row_id_column_missing"
            audit_table = None

        async def record_skip(reason: str) -> dict[str, Any]:
            plan_update = dict(plan) if isinstance(plan, dict) else {}
            plan_update["apply_mode"] = "source_table"
            plan_update["apply_skipped_reason"] = reason
            plan_update["apply_skipped_details"] = {
                "approved_plan_id": overrides.get("data_fix_plan_id"),
                "approved_plan_hash": overrides.get("data_fix_plan_hash"),
                "approved_snapshot": overrides.get("data_fix_snapshot_signature"),
                "plan_id": plan.get("plan_id") if isinstance(plan, dict) else None,
                "plan_hash": plan.get("plan_hash") if isinstance(plan, dict) else None,
                "plan_snapshot": (plan.get("snapshot") or {}).get("signature") if isinstance(plan, dict) else None,
            }
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

        if not plan or not plan.get("plan_id") or not plan.get("plan_hash"):
            refreshed = await self.plan_data_repairs(
                table_asset_id, column_name, null_strategy, conflict_strategy
            )
            plan = self._normalize_plan_payload(
                refreshed.get("repair_plan") if isinstance(refreshed, dict) else None
            )
            if not plan or not plan.get("plan_id") or not plan.get("plan_hash"):
                return await record_skip("plan_missing")
        if not plan.get("plan_id") or not plan.get("plan_hash"):
            return await record_skip("plan_missing")

        if plan.get("row_id_column"):
            row_id_column = plan.get("row_id_column") or row_id_column
            if audit_table and not row_id_column:
                audit_disabled_reason = "row_id_column_missing"
                audit_table = None

        group_by = self._normalize_group_by(
            (plan.get("snapshot") or {}).get("group_by_columns")
            or conflicts.get("group_by_columns")
            or overrides.get("conflict_group_columns")
        )
        if not (plan.get("snapshot") or {}).get("signature"):
            snapshot = await self._compute_snapshot(ctx, column_name, group_by)
            plan = dict(plan)
            plan["snapshot"] = snapshot
            await self._update_column_analysis(ctx, {"repair_plan": plan})

        approval_signature_present = bool(
            overrides.get("data_fix_plan_id")
            or overrides.get("data_fix_plan_hash")
            or overrides.get("data_fix_snapshot_signature")
            or plan.get("approved_plan_id")
            or plan.get("approved_plan_hash")
            or plan.get("approved_snapshot_signature")
        )
        approved_plan_id = (
            overrides.get("data_fix_plan_id")
            or plan.get("approved_plan_id")
            or plan.get("plan_id")
        )
        approved_plan_hash = (
            overrides.get("data_fix_plan_hash")
            or plan.get("approved_plan_hash")
            or plan.get("plan_hash")
        )
        approved_snapshot = (
            overrides.get("data_fix_snapshot_signature")
            or plan.get("approved_snapshot_signature")
            or (plan.get("snapshot") or {}).get("signature")
        )
        if not approved_snapshot:
            snapshot = await self._compute_snapshot(ctx, column_name, group_by)
            if snapshot.get("signature"):
                plan = dict(plan)
                plan["snapshot"] = snapshot
                await self._update_column_analysis(ctx, {"repair_plan": plan})
                approved_snapshot = snapshot.get("signature")
        if isinstance(plan, dict):
            approved_plan_id = approved_plan_id or plan.get("plan_id")
            approved_plan_hash = approved_plan_hash or plan.get("plan_hash")
            approved_snapshot = approved_snapshot or (plan.get("snapshot") or {}).get("signature")

        if not (approved_plan_id and approved_plan_hash and approved_snapshot):
            refreshed = await self.plan_data_repairs(
                table_asset_id, column_name, null_strategy, conflict_strategy
            )
            plan = refreshed.get("repair_plan") if isinstance(refreshed, dict) else plan
            approved_plan_id = approved_plan_id or (plan.get("plan_id") if isinstance(plan, dict) else None)
            approved_plan_hash = approved_plan_hash or (plan.get("plan_hash") if isinstance(plan, dict) else None)
            approved_snapshot = approved_snapshot or (
                (plan.get("snapshot") or {}).get("signature") if isinstance(plan, dict) else None
            )
        if isinstance(plan, dict):
            approved_plan_id = approved_plan_id or plan.get("plan_id")
            approved_plan_hash = approved_plan_hash or plan.get("plan_hash")
            approved_snapshot = approved_snapshot or (plan.get("snapshot") or {}).get("signature")

        if not (approved_plan_id and approved_plan_hash and approved_snapshot):
            return await record_skip("approval_missing_plan_info")
        if approval_signature_present and (
            approved_plan_id != plan.get("plan_id")
            or approved_plan_hash != plan.get("plan_hash")
            or approved_snapshot != (plan.get("snapshot") or {}).get("signature")
        ):
            return await record_skip("approval_plan_mismatch")

        expected_snapshot = approved_snapshot or (plan.get("snapshot") or {}).get("signature")
        current_snapshot = await self._compute_snapshot(ctx, column_name, group_by)
        if expected_snapshot and current_snapshot.get("signature") != expected_snapshot:
            plan_snapshot = plan.get("snapshot") if isinstance(plan, dict) else None
            if self._snapshots_compatible(plan_snapshot, current_snapshot):
                plan = dict(plan)
                plan["snapshot"] = current_snapshot
                await self._update_column_analysis(ctx, {"repair_plan": plan})
            else:
                refreshed = await self.plan_data_repairs(
                    table_asset_id, column_name, null_strategy, conflict_strategy
                )
                plan = (
                    refreshed.get("repair_plan") if isinstance(refreshed, dict) else plan
                )
                return await record_skip("snapshot_mismatch")

        if approved and (
            overrides.get("data_fix_plan_id") is None
            or overrides.get("data_fix_plan_hash") is None
            or overrides.get("data_fix_snapshot_signature") is None
        ):
            updated_overrides = dict(overrides)
            updated_overrides.setdefault("data_fix_plan_id", approved_plan_id)
            updated_overrides.setdefault("data_fix_plan_hash", approved_plan_hash)
            updated_overrides.setdefault("data_fix_snapshot_signature", approved_snapshot)
            updated_overrides.setdefault(approval_key, True)
            ctx.column_meta.overrides = updated_overrides
            flag_modified(ctx.column_meta, "overrides")
            await self.db.commit()

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
        audit_table = audit_table or (plan.get("rollback") or {}).get("audit_table")
        audit_table = self._coerce_table_ref(audit_table, ctx.table_ref)
        if audit_table and not row_id_column:
            audit_disabled_reason = audit_disabled_reason or "row_id_column_missing"
            audit_table = None
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
                    uses_alias = bool(null_step.get("requires_base_alias"))
                    base_ref = f"{ctx.table_ref} AS base" if uses_alias else ctx.table_ref
                    audit_insert = f"""
                    INSERT INTO {audit_table} (plan_id, column_name, repair_type, row_id, before_value, after_value, created_at)
                    SELECT '{self._sanitize_literal(plan.get("plan_id", ""))}', '{self._sanitize_literal(column_name)}', 'null_repair',
                           TO_VARCHAR({self._quote_ident(row_id_column)}),
                           TO_VARIANT({col}),
                           TO_VARIANT({fill_expr}),
                           CURRENT_TIMESTAMP()
                    FROM {base_ref}
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
                plan_update["apply_mode"] = "source_table"
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
        if approved:
            plan_update["approved"] = True
            plan_update["approval_status"] = "approved"
        if applied_any:
            plan_update["applied"] = True
            plan_update["applied_at"] = datetime.utcnow().isoformat()
        plan_update["apply_mode"] = "source_table"
        if audit_disabled_reason:
            plan_update["audit_disabled_reason"] = audit_disabled_reason
        analysis_update = {"repair_results": repair_results, "repair_plan": plan_update}
        await self._update_column_analysis(ctx, analysis_update)
        return {
            "column": column_name,
            "repairs": repair_results,
            "approved": approved,
        }


    @tool
    async def apply_data_repairs_to_fixing_table(
        self,
        table_asset_id: int,
        column_name: str,
        null_strategy: str | None = None,
        conflict_strategy: str | None = None,
        plan_id: str | None = None,
        plan_hash: str | None = None,
        snapshot_signature: str | None = None,
        approval_key: str = "data_fix_approved",
    ) -> dict[str, Any]:
        """Apply approved repairs by writing to a fixing table (does not mutate source table)."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        if plan_id or plan_hash or snapshot_signature:
            overrides = dict(overrides)
            if plan_id:
                overrides["data_fix_plan_id"] = plan_id
            if plan_hash:
                overrides["data_fix_plan_hash"] = plan_hash
            if snapshot_signature:
                overrides["data_fix_snapshot_signature"] = snapshot_signature

        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        nulls = analysis.get("nulls", {})
        conflicts = analysis.get("conflicts", {})
        plan = self._normalize_plan_payload(analysis.get("repair_plan", {}))
        async def record_skip(reason: str) -> dict[str, Any]:
            plan_update = dict(plan) if isinstance(plan, dict) else {}
            plan_update["apply_mode"] = "fixing_table"
            plan_update["apply_skipped_reason"] = reason
            plan_update["apply_skipped_details"] = {
                "approved_plan_id": overrides.get("data_fix_plan_id"),
                "approved_plan_hash": overrides.get("data_fix_plan_hash"),
                "approved_snapshot": overrides.get("data_fix_snapshot_signature"),
                "plan_id": plan.get("plan_id") if isinstance(plan, dict) else None,
                "plan_hash": plan.get("plan_hash") if isinstance(plan, dict) else None,
                "plan_snapshot": (plan.get("snapshot") or {}).get("signature") if isinstance(plan, dict) else None,
            }
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

        approved = bool(overrides.get(approval_key) or (isinstance(plan, dict) and plan.get("approved")))
        if not approved:
            return await record_skip("approval_required")
        if not ctx.table_ref:
            return await record_skip("table_ref_missing")

        if not plan or not plan.get("plan_id") or not plan.get("plan_hash"):
            refreshed = await self.plan_data_repairs(
                table_asset_id, column_name, null_strategy, conflict_strategy
            )
            plan = self._normalize_plan_payload(
                refreshed.get("repair_plan") if isinstance(refreshed, dict) else None
            )
            if not plan or not plan.get("plan_id") or not plan.get("plan_hash"):
                return await record_skip("plan_missing")
        if not plan.get("plan_id") or not plan.get("plan_hash"):
            return await record_skip("plan_missing")

        group_by = self._normalize_group_by(
            (plan.get("snapshot") or {}).get("group_by_columns")
            or conflicts.get("group_by_columns")
            or overrides.get("conflict_group_columns")
        )

        if isinstance(plan, dict) and not (plan.get("snapshot") or {}).get("signature"):
            snapshot = await self._compute_snapshot(ctx, column_name, group_by)
            plan = dict(plan)
            plan["snapshot"] = snapshot
            await self._update_column_analysis(ctx, {"repair_plan": plan})

        approval_signature_present = bool(
            overrides.get("data_fix_plan_id")
            or overrides.get("data_fix_plan_hash")
            or overrides.get("data_fix_snapshot_signature")
            or plan.get("approved_plan_id")
            or plan.get("approved_plan_hash")
            or plan.get("approved_snapshot_signature")
        )
        approved_plan_id = (
            overrides.get("data_fix_plan_id")
            or plan.get("approved_plan_id")
            or plan.get("plan_id")
        )
        approved_plan_hash = (
            overrides.get("data_fix_plan_hash")
            or plan.get("approved_plan_hash")
            or plan.get("plan_hash")
        )
        approved_snapshot = (
            overrides.get("data_fix_snapshot_signature")
            or plan.get("approved_snapshot_signature")
            or (plan.get("snapshot") or {}).get("signature")
        )
        if not approved_snapshot:
            snapshot = await self._compute_snapshot(ctx, column_name, group_by)
            if snapshot.get("signature"):
                plan = dict(plan)
                plan["snapshot"] = snapshot
                await self._update_column_analysis(ctx, {"repair_plan": plan})
                approved_snapshot = snapshot.get("signature")
        if isinstance(plan, dict):
            approved_plan_id = approved_plan_id or plan.get("plan_id")
            approved_plan_hash = approved_plan_hash or plan.get("plan_hash")
            approved_snapshot = approved_snapshot or (plan.get("snapshot") or {}).get("signature")

        if not (approved_plan_id and approved_plan_hash and approved_snapshot):
            refreshed = await self.plan_data_repairs(
                table_asset_id, column_name, null_strategy, conflict_strategy
            )
            plan = refreshed.get("repair_plan") if isinstance(refreshed, dict) else plan
            approved_plan_id = approved_plan_id or (plan.get("plan_id") if isinstance(plan, dict) else None)
            approved_plan_hash = approved_plan_hash or (plan.get("plan_hash") if isinstance(plan, dict) else None)
            approved_snapshot = approved_snapshot or (
                (plan.get("snapshot") or {}).get("signature") if isinstance(plan, dict) else None
            )
        if isinstance(plan, dict):
            approved_plan_id = approved_plan_id or plan.get("plan_id")
            approved_plan_hash = approved_plan_hash or plan.get("plan_hash")
            approved_snapshot = approved_snapshot or (plan.get("snapshot") or {}).get("signature")

        if not (approved_plan_id and approved_plan_hash and approved_snapshot):
            return await record_skip("approval_missing_plan_info")
        if approval_signature_present and (
            approved_plan_id != plan.get("plan_id")
            or approved_plan_hash != plan.get("plan_hash")
            or approved_snapshot != (plan.get("snapshot") or {}).get("signature")
        ):
            return await record_skip("approval_plan_mismatch")

        expected_snapshot = approved_snapshot or (plan.get("snapshot") or {}).get("signature")
        current_snapshot = await self._compute_snapshot(ctx, column_name, group_by)
        if expected_snapshot and current_snapshot.get("signature") != expected_snapshot:
            plan_snapshot = plan.get("snapshot") if isinstance(plan, dict) else None
            if self._snapshots_compatible(plan_snapshot, current_snapshot):
                plan = dict(plan)
                plan["snapshot"] = current_snapshot
                await self._update_column_analysis(ctx, {"repair_plan": plan})
            else:
                refreshed = await self.plan_data_repairs(
                    table_asset_id, column_name, null_strategy, conflict_strategy
                )
                plan = (
                    refreshed.get("repair_plan") if isinstance(refreshed, dict) else plan
                )
                return await record_skip("snapshot_mismatch")

        if approved and (
            overrides.get("data_fix_plan_id") is None
            or overrides.get("data_fix_plan_hash") is None
            or overrides.get("data_fix_snapshot_signature") is None
        ):
            updated_overrides = dict(overrides)
            updated_overrides.setdefault("data_fix_plan_id", approved_plan_id)
            updated_overrides.setdefault("data_fix_plan_hash", approved_plan_hash)
            updated_overrides.setdefault("data_fix_snapshot_signature", approved_snapshot)
            updated_overrides.setdefault(approval_key, True)
            ctx.column_meta.overrides = updated_overrides
            flag_modified(ctx.column_meta, "overrides")
            await self.db.commit()

        row_id_column = plan.get("row_id_column") or self._resolve_row_id_column(ctx)

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
        fixing_config = self._resolve_fixing_table_config(ctx, column_name, overrides, plan)
        target_table = fixing_config["target_table"]
        target_column = fixing_config["target_column"]
        column_mode = fixing_config["column_mode"]
        create_mode = fixing_config["create_mode"]
        target_col = self._quote_ident(target_column)
        dry_run = bool(overrides.get("data_fix_dry_run") or overrides.get("repair_dry_run"))

        repair_results: list[dict[str, Any]] = []
        sql_previews = plan.get("sql_previews") or {}

        if dry_run:
            plan_update = dict(plan) if isinstance(plan, dict) else {}
            plan_update["apply_mode"] = "fixing_table"
            plan_update["target_table"] = target_table
            plan_update["target_column"] = target_column
            plan_update["column_mode"] = column_mode
            await self._update_column_analysis(
                ctx,
                {"repair_results": [{"type": "repair_apply", "status": "dry_run"}], "repair_plan": plan_update},
            )
            return {
                "column": column_name,
                "repairs": repair_results,
                "approved": approved,
                "target_table": target_table,
                "target_column": target_column,
                "column_mode": column_mode,
                "dry_run": True,
            }

        try:
            create_select = "*"
            if target_column != column_name:
                create_select = f"*, {col} AS {target_col}"
            if create_mode == "create_if_missing":
                create_sql = (
                    f"CREATE TABLE IF NOT EXISTS {target_table} "
                    f"AS SELECT {create_select} FROM {ctx.table_ref}"
                )
            else:
                create_sql = (
                    f"CREATE OR REPLACE TABLE {target_table} "
                    f"AS SELECT {create_select} FROM {ctx.table_ref}"
                )
            await self.sf.execute_query(create_sql)
            if target_column != column_name:
                column_sql_type = self._coerce_sql_type(
                    (ctx.column_meta.metadata_payload or {}).get("sql_type")
                )
                await self.sf.execute_query(
                    f"ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS {target_col} {column_sql_type}"
                )
        except Exception as exc:
            return await record_skip(f"fixing_table_create_failed:{exc}")

        null_count = self._coerce_int(nulls.get("null_count"))
        if null_count and planned_null_strategy:
            null_step = next(
                (step for step in plan.get("steps", []) if step.get("type") == "null_repair"),
                {},
            )
            fill_expr = null_step.get("fill_expr")
            fill_value = null_step.get("fill_value")
            if fill_expr is None:
                repair_results.append(
                    {
                        "type": "null_repair",
                        "status": "skipped",
                        "strategy": planned_null_strategy,
                        "reason": "fill_value_unavailable",
                    }
                )
            else:
                uses_alias = bool(null_step.get("requires_base_alias"))
                if uses_alias:
                    update_query = (
                        f"UPDATE {target_table} AS base SET {target_col} = {fill_expr} WHERE {target_col} IS NULL"
                    )
                else:
                    update_query = (
                        f"UPDATE {target_table} SET {target_col} = {fill_expr} WHERE {target_col} IS NULL"
                    )
                try:
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
                except Exception as exc:
                    repair_results.append(
                        {
                            "type": "null_repair",
                            "status": "error",
                            "strategy": planned_null_strategy,
                            "error": str(exc),
                            "update_sql": update_query,
                        }
                    )

        conflict_groups = self._coerce_int(conflicts.get("conflict_groups"))
        if conflict_groups and group_by and planned_conflict_strategy:
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
            else:
                preview = sql_previews.get("conflict_repair") or {}
                update_query = preview.get("update_sql")
                if update_query:
                    update_query = update_query.replace(ctx.table_ref, target_table)
                    if target_column != column_name:
                        update_query = update_query.replace(col, target_col)
                    try:
                        await self.sf.execute_query(update_query)
                        repair_results.append(
                            {
                                "type": "conflict_repair",
                                "status": "applied",
                                "strategy": planned_conflict_strategy,
                            }
                        )
                    except Exception as exc:
                        repair_results.append(
                            {
                                "type": "conflict_repair",
                                "status": "error",
                                "strategy": planned_conflict_strategy,
                                "error": str(exc),
                                "update_sql": update_query,
                            }
                        )
                else:
                    repair_results.append(
                        {
                            "type": "conflict_repair",
                            "status": "skipped",
                            "strategy": planned_conflict_strategy,
                            "reason": "update_sql_missing",
                        }
                    )

        plan_update = dict(plan) if isinstance(plan, dict) else {}
        if approved:
            plan_update["approved"] = True
            plan_update["approval_status"] = "approved"
        if any(item.get("status") == "applied" for item in repair_results):
            plan_update["applied"] = True
            plan_update["applied_at"] = datetime.utcnow().isoformat()
        plan_update["apply_mode"] = "fixing_table"
        plan_update["target_table"] = target_table
        plan_update["target_column"] = target_column
        plan_update["column_mode"] = column_mode
        plan_update["create_mode"] = create_mode
        analysis_update = {"repair_results": repair_results, "repair_plan": plan_update}
        await self._update_column_analysis(ctx, analysis_update)

        return {
            "column": column_name,
            "repairs": repair_results,
            "approved": approved,
            "target_table": target_table,
            "target_column": target_column,
            "column_mode": column_mode,
        }
