from __future__ import annotations

import json
import logging
import re
from typing import Any

from sqlalchemy import select

from strands import tool

from ...models.column_metadata import ColumnMetadata
from ...services.chart_service import ChartService
from ...services.eda_service import EDAService

logger = logging.getLogger(__name__)
MIN_TIME_POINTS = 3
MIN_ABS_CORRELATION_DEFAULT = 0.08


class ColumnWorkflowVisualsMixin:
    """Tool mixin."""

    def _resolve_min_abs_correlation(
        self,
        overrides: dict[str, Any] | None = None,
        default: float = MIN_ABS_CORRELATION_DEFAULT,
    ) -> float:
        raw = (overrides or {}).get("visual_correlation_min_abs")
        parsed = self._coerce_float(raw)
        if parsed is None:
            parsed = default
        try:
            value = float(parsed)
        except (TypeError, ValueError):
            value = default
        return max(0.0, min(value, 1.0))

    def _build_year_anomaly_warning(
        self,
        year_anomaly_context: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if not isinstance(year_anomaly_context, dict):
            return None
        anomaly_count = self._coerce_int(year_anomaly_context.get("anomaly_count"), 0)
        if anomaly_count <= 0:
            return None
        valid_range = year_anomaly_context.get("valid_range") or {}
        min_range = self._coerce_int(valid_range.get("min")) if isinstance(valid_range, dict) else None
        max_range = self._coerce_int(valid_range.get("max")) if isinstance(valid_range, dict) else None
        samples_raw = year_anomaly_context.get("samples")
        samples: list[dict[str, Any]] = []
        if isinstance(samples_raw, list):
            for item in samples_raw[:6]:
                if not isinstance(item, dict):
                    continue
                value = item.get("value")
                if value is None:
                    continue
                entry: dict[str, Any] = {"value": value}
                value_count = self._coerce_int(item.get("value_count"), 0)
                if value_count > 0:
                    entry["count"] = value_count
                reason = str(item.get("reason") or "").strip()
                if reason:
                    entry["reason"] = reason
                parsed_year = item.get("parsed_year")
                if parsed_year is not None:
                    entry["parsed_year"] = parsed_year
                samples.append(entry)
        range_text = (
            f"{min_range}-{max_range}"
            if min_range is not None and max_range is not None
            else "expected year range"
        )
        return {
            "code": "year_out_of_range_filtered",
            "severity": "warning",
            "title": "Out-of-range years filtered",
            "message": (
                f"Filtered {anomaly_count} rows with year values outside {range_text}."
            ),
            "anomaly_count": anomaly_count,
            "valid_range": {
                "min": min_range,
                "max": max_range,
            },
            "sample_values": samples,
        }

    def _append_chart_warning(
        self,
        chart: dict[str, Any],
        warning: dict[str, Any] | None,
    ) -> None:
        if not isinstance(chart, dict) or not isinstance(warning, dict):
            return
        existing = chart.get("warnings")
        warnings: list[dict[str, Any]] = []
        if isinstance(existing, list):
            warnings = [item for item in existing if isinstance(item, dict)]
        warning_code = str(warning.get("code") or "").strip().lower()
        for item in warnings:
            existing_code = str(item.get("code") or "").strip().lower()
            if warning_code and existing_code == warning_code:
                return
        warnings.append(warning)
        chart["warnings"] = warnings

    def _normalize_category_label(self, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        if "||" not in text:
            return text
        parts = [part.strip() for part in text.split("||") if part and part.strip()]
        if not parts:
            return text
        deduped: list[str] = []
        seen: set[str] = set()
        for part in parts:
            key = part.lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(part)
        if len(deduped) == 1:
            return deduped[0]
        preview = " | ".join(deduped[:2])
        if len(deduped) > 2:
            preview += " | ..."
        return preview

    def _extract_numeric_values(self, data: list[dict[str, Any]], key: str | None) -> list[float]:
        if not key:
            return []
        values: list[float] = []
        for row in data or []:
            if not isinstance(row, dict):
                continue
            raw = row.get(key)
            if raw is None or isinstance(raw, bool):
                continue
            try:
                values.append(float(raw))
            except (TypeError, ValueError):
                continue
        return values

    def _count_unique_axis_values(self, data: list[dict[str, Any]], key: str | None) -> int:
        if not key:
            return 0
        unique_values: set[str] = set()
        for row in data or []:
            if not isinstance(row, dict):
                continue
            raw = row.get(key)
            if raw is None:
                continue
            text = str(raw).strip()
            if not text:
                continue
            unique_values.add(text)
        return len(unique_values)

    def _filter_low_signal_visuals(
        self,
        visuals: list[dict[str, Any]],
        overrides: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if not visuals:
            return [], []
        line_min_points = max(
            MIN_TIME_POINTS, self._coerce_int(overrides.get("visual_line_min_points"), 8)
        )
        bar_min_categories = max(
            2, self._coerce_int(overrides.get("visual_bar_min_categories"), 2)
        )

        kept: list[dict[str, Any]] = []
        dropped: list[dict[str, Any]] = []
        for visual in visuals:
            if not isinstance(visual, dict):
                continue
            data = visual.get("data") or []
            if not isinstance(data, list) or not data:
                dropped.append(
                    {
                        "id": visual.get("id"),
                        "title": visual.get("title"),
                        "reason": "empty_data",
                    }
                )
                continue
            chart_type = str(visual.get("chartType") or visual.get("chart_type") or "").lower()
            x_key = visual.get("xKey") or visual.get("x_key")
            y_key = visual.get("yKey") or visual.get("y_key")
            point_count = len(data)
            unique_x = self._count_unique_axis_values(data, str(x_key) if x_key else None)

            if chart_type in {"line", "area"}:
                if point_count < line_min_points or unique_x < line_min_points:
                    dropped.append(
                        {
                            "id": visual.get("id"),
                            "title": visual.get("title"),
                            "reason": f"sparse_time_points:{point_count}",
                        }
                    )
                    continue

            if chart_type == "bar":
                if unique_x < bar_min_categories:
                    dropped.append(
                        {
                            "id": visual.get("id"),
                            "title": visual.get("title"),
                            "reason": f"insufficient_categories:{unique_x}",
                        }
                    )
                    continue
                values = self._extract_numeric_values(data, str(y_key) if y_key else None)
                if values and unique_x >= 3 and max(values) == min(values):
                    dropped.append(
                        {
                            "id": visual.get("id"),
                            "title": visual.get("title"),
                            "reason": "flat_distribution",
                        }
                    )
                    continue

            if chart_type == "heatmap":
                y_axis_key = str(y_key) if y_key else None
                x_axis_key = str(x_key) if x_key else None
                unique_y = self._count_unique_axis_values(data, y_axis_key)
                value_key = (
                    visual.get("valueKey")
                    or visual.get("value_key")
                    or "correlation"
                )
                values = self._extract_numeric_values(data, str(value_key))
                if unique_x < 2 or unique_y < 2 or not values:
                    dropped.append(
                        {
                            "id": visual.get("id"),
                            "title": visual.get("title"),
                            "reason": "invalid_matrix",
                        }
                    )
                    continue

            kept.append(visual)

        if kept:
            return kept, dropped
        # Keep one chart as fallback instead of returning an empty list.
        return visuals[:1], dropped

    def _fallback_heatmap_insight(
        self,
        column_name: str,
        matrix_columns: list[str],
        pairs: list[dict[str, Any]],
    ) -> str:
        if not pairs:
            return (
                f"Correlation heatmap for {column_name} has insufficient valid pairwise "
                "correlations after recomputation."
            )
        top = pairs[0]
        top_corr = self._coerce_float(top.get("correlation"), 0.0) or 0.0
        top_left = str(top.get("feature_x") or "")
        top_right = str(top.get("feature_y") or "")
        abs_max = max(abs(self._coerce_float(item.get("correlation"), 0.0) or 0.0) for item in pairs)
        if abs_max < 0.15:
            return (
                f"Recomputed correlations for {column_name} are mostly weak "
                f"(strongest pair {top_left} vs {top_right}, r={top_corr:.3f})."
            )
        return (
            f"Strongest recomputed relationship is {top_left} vs {top_right} "
            f"(r={top_corr:.3f}) across {len(matrix_columns)} selected columns."
        )

    def _extract_unique_heatmap_pairs(
        self, heatmap_data: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        unique_pairs: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for row in heatmap_data or []:
            left = str(row.get("feature_x") or "").strip()
            right = str(row.get("feature_y") or "").strip()
            if not left or not right or left == right:
                continue
            corr = self._coerce_float(row.get("correlation"))
            if corr is None:
                continue
            key = tuple(sorted((left, right)))
            if key in seen:
                continue
            seen.add(key)
            unique_pairs.append(
                {
                    "feature_x": left,
                    "feature_y": right,
                    "correlation": round(float(corr), 4),
                    "abs_correlation": round(abs(float(corr)), 4),
                }
            )
        unique_pairs.sort(
            key=lambda item: abs(float(item.get("correlation") or 0.0)),
            reverse=True,
        )
        return unique_pairs

    def _prune_heatmap_by_target_signal(
        self,
        target_column: str,
        matrix_columns: list[str],
        heatmap_data: list[dict[str, Any]],
        min_abs_correlation: float,
    ) -> tuple[list[str], list[dict[str, Any]], list[dict[str, Any]]]:
        if not matrix_columns or len(matrix_columns) < 2 or not heatmap_data:
            return matrix_columns, heatmap_data, []

        target = str(target_column or "").strip() or str(matrix_columns[0] or "").strip()
        if not target:
            return matrix_columns, heatmap_data, []

        pairs = self._extract_unique_heatmap_pairs(heatmap_data)
        abs_to_target: dict[str, float] = {}
        for item in pairs:
            left = str(item.get("feature_x") or "").strip()
            right = str(item.get("feature_y") or "").strip()
            corr = abs(self._coerce_float(item.get("correlation"), 0.0) or 0.0)
            if left == target and right:
                abs_to_target[right] = max(abs_to_target.get(right, 0.0), corr)
            elif right == target and left:
                abs_to_target[left] = max(abs_to_target.get(left, 0.0), corr)

        peers = [str(column) for column in matrix_columns if str(column) and str(column) != target]
        if not peers:
            return matrix_columns, heatmap_data, []

        strong_peers = [
            peer
            for peer in peers
            if (abs_to_target.get(peer) or 0.0) >= max(0.0, float(min_abs_correlation))
        ]

        # Ensure the chart remains renderable even when all peers are weak.
        if not strong_peers:
            best_peer = max(peers, key=lambda peer: abs_to_target.get(peer, 0.0))
            strong_peers = [best_peer]

        kept_columns = [target, *strong_peers]
        kept_set = set(kept_columns)
        filtered_data = [
            row
            for row in heatmap_data
            if str(row.get("feature_x") or "") in kept_set
            and str(row.get("feature_y") or "") in kept_set
        ]

        dropped_columns = [
            {
                "column": peer,
                "abs_target_correlation": round(abs_to_target.get(peer, 0.0), 4),
            }
            for peer in peers
            if peer not in strong_peers
        ]

        if len(kept_columns) < 2 or not filtered_data:
            return matrix_columns, heatmap_data, []
        return kept_columns, filtered_data, dropped_columns

    async def _compute_correlation_heatmap_data(
        self,
        ctx: Any,
        matrix_columns: list[str],
        sample_size: int | None = None,
        window_days: int | None = None,
    ) -> list[dict[str, Any]]:
        if len(matrix_columns) < 2:
            return []
        pair_selects: list[str] = []
        pair_alias_map: dict[str, tuple[str, str]] = {}
        for left_idx, left_col in enumerate(matrix_columns):
            left_expr = self._numeric_expr(self._quote_ident(left_col))
            for right_idx in range(left_idx + 1, len(matrix_columns)):
                right_col = matrix_columns[right_idx]
                right_expr = self._numeric_expr(self._quote_ident(right_col))
                alias = f"corr_{left_idx}_{right_idx}"
                pair_selects.append(f"CORR({left_expr}, {right_expr}) AS {alias}")
                pair_alias_map[alias] = (left_col, right_col)
        if not pair_selects:
            return []
        base_query = self._build_windowed_query(
            ctx, sample_size, window_days, ctx.time_column
        )
        matrix_query = f"""
        WITH base AS (
            {base_query}
        )
        SELECT
            {", ".join(pair_selects)}
        FROM base
        """.strip()
        matrix_rows = await self.sf.execute_query(matrix_query)
        matrix_row = matrix_rows[0] if matrix_rows else {}
        heatmap_data: list[dict[str, Any]] = []
        for matrix_col in matrix_columns:
            heatmap_data.append(
                {
                    "feature_x": matrix_col,
                    "feature_y": matrix_col,
                    "correlation": 1.0,
                }
            )
        for alias, (left_col, right_col) in pair_alias_map.items():
            raw_value = matrix_row.get(alias.upper())
            if raw_value is None:
                raw_value = matrix_row.get(alias)
            corr_value = self._coerce_float(raw_value)
            if corr_value is None:
                continue
            rounded = round(float(corr_value), 4)
            heatmap_data.append(
                {
                    "feature_x": left_col,
                    "feature_y": right_col,
                    "correlation": rounded,
                }
            )
            heatmap_data.append(
                {
                    "feature_x": right_col,
                    "feature_y": left_col,
                    "correlation": rounded,
                }
            )
        return heatmap_data

    def _score_correlation_combo(
        self,
        matrix_columns: list[str],
        heatmap_data: list[dict[str, Any]],
    ) -> dict[str, Any]:
        pairs = self._extract_unique_heatmap_pairs(heatmap_data)
        pair_count = len(pairs)
        total_possible = max(1, (len(matrix_columns) * (len(matrix_columns) - 1)) // 2)
        if pair_count <= 0:
            return {
                "pair_count": 0,
                "total_possible_pairs": total_possible,
                "coverage": 0.0,
                "mean_abs_corr": 0.0,
                "strong_pair_count": 0,
                "near_zero_pair_count": 0,
                "score": -1.0,
                "top_pairs": [],
            }
        coverage = pair_count / total_possible
        target_column = str(matrix_columns[0] or "").strip() if matrix_columns else ""
        abs_values = [abs(float(item.get("correlation") or 0.0)) for item in pairs]
        target_abs_values = [
            abs(float(item.get("correlation") or 0.0))
            for item in pairs
            if str(item.get("feature_x") or "") == target_column
            or str(item.get("feature_y") or "") == target_column
        ]
        mean_abs_corr = sum(abs_values) / len(abs_values)
        target_mean_abs_corr = (
            sum(target_abs_values) / len(target_abs_values) if target_abs_values else 0.0
        )
        strong_pair_count = sum(1 for value in abs_values if value >= 0.3)
        near_zero_pair_count = sum(1 for value in abs_values if value <= 0.05)
        target_near_zero_count = sum(1 for value in target_abs_values if value <= 0.05)
        strong_ratio = strong_pair_count / pair_count
        near_zero_ratio = near_zero_pair_count / pair_count
        target_near_zero_ratio = (
            target_near_zero_count / len(target_abs_values) if target_abs_values else 0.0
        )
        score = (
            mean_abs_corr * 1.0
            + target_mean_abs_corr * 1.35
            + strong_ratio * 0.7
            + coverage * 0.25
            - near_zero_ratio * 0.8
            - target_near_zero_ratio * 1.0
        )
        return {
            "pair_count": pair_count,
            "total_possible_pairs": total_possible,
            "coverage": round(coverage, 4),
            "mean_abs_corr": round(mean_abs_corr, 4),
            "target_mean_abs_corr": round(target_mean_abs_corr, 4),
            "strong_pair_count": strong_pair_count,
            "near_zero_pair_count": near_zero_pair_count,
            "target_near_zero_count": target_near_zero_count,
            "score": round(score, 6),
            "top_pairs": pairs[:6],
        }

    def _build_correlation_combo_candidates(
        self,
        column_name: str,
        ranked_peers: list[dict[str, Any]],
        peer_limit: int,
    ) -> list[list[str]]:
        if peer_limit <= 0:
            return []
        peers_ranked = [str(item.get("column")) for item in ranked_peers if item.get("column")]
        if not peers_ranked:
            return []
        peers_ranked = peers_ranked[: max(peer_limit * 2, peer_limit)]
        positives = [
            str(item.get("column"))
            for item in ranked_peers
            if item.get("column") and (self._coerce_float(item.get("correlation"), 0.0) or 0.0) >= 0
        ]
        negatives = [
            str(item.get("column"))
            for item in ranked_peers
            if item.get("column") and (self._coerce_float(item.get("correlation"), 0.0) or 0.0) < 0
        ]
        moderate = [
            str(item.get("column"))
            for item in ranked_peers
            if item.get("column") and abs(self._coerce_float(item.get("correlation"), 0.0) or 0.0) >= 0.2
        ]

        size_candidates: list[int] = []
        for value in (3, 5, peer_limit):
            if value <= 0:
                continue
            size_candidates.append(min(value, len(peers_ranked), peer_limit))
        size_candidates = [size for size in size_candidates if size > 0]
        if not size_candidates:
            size_candidates = [min(len(peers_ranked), peer_limit)]

        combos: list[list[str]] = []
        for size in size_candidates:
            base = peers_ranked[:size]
            if base:
                combos.append([column_name, *base])

            diversified: list[str] = []
            pos_take = min(len(positives), max(1, size // 2))
            neg_take = min(len(negatives), max(1, size - pos_take))
            diversified.extend(positives[:pos_take])
            diversified.extend([value for value in negatives[:neg_take] if value not in diversified])
            for peer in peers_ranked:
                if len(diversified) >= size:
                    break
                if peer in diversified:
                    continue
                diversified.append(peer)
            if diversified:
                combos.append([column_name, *diversified[:size]])

        if moderate:
            combos.append([column_name, *moderate[: min(len(moderate), peer_limit)]])

        unique_combos: list[list[str]] = []
        seen: set[tuple[str, ...]] = set()
        for combo in combos:
            normalized: list[str] = []
            for col in combo:
                text = str(col or "").strip()
                if not text or text in normalized:
                    continue
                normalized.append(text)
            if len(normalized) < 2:
                continue
            key = tuple(normalized)
            if key in seen:
                continue
            seen.add(key)
            unique_combos.append(normalized[: peer_limit + 1])
            if len(unique_combos) >= 8:
                break
        return unique_combos

    async def _select_best_correlation_combo(
        self,
        column_name: str,
        candidates: list[dict[str, Any]],
    ) -> tuple[str | None, str | None]:
        if not candidates:
            return None, None
        payload = {
            "target_column": column_name,
            "candidates": [
                {
                    "id": candidate.get("id"),
                    "columns": candidate.get("columns"),
                    "metrics": candidate.get("metrics"),
                }
                for candidate in candidates
            ],
        }
        instruction = (
            "You are a correlation-selection agent. Choose the best candidate to keep in the final heatmap. "
            "Prefer combinations with meaningful, non-trivial relationships and fewer near-zero pairwise values. "
            "Keep diverse signal (both positive and negative if present), but avoid noisy weak-only sets. "
            "Return JSON with keys: selected_id (string), rationale (string)."
        )
        prompt = (
            f"{instruction} Context JSON: {json.dumps(payload, default=str)}"
        )
        response_format = {
            "type": "object",
            "properties": {
                "selected_id": {"type": "string"},
                "rationale": {"type": "string"},
            },
        }
        try:
            raw = await self.ai_sql.ai_complete(
                self.model_id, prompt, response_format=response_format
            )
        except Exception:
            return None, None
        if isinstance(raw, dict):
            result = raw
        else:
            text = str(raw or "").strip()
            if not text:
                return None, None
            result: dict[str, Any] | None = None
            try:
                loaded = json.loads(text)
                if isinstance(loaded, dict):
                    result = loaded
            except json.JSONDecodeError:
                match = re.search(r"\{.*\}", text, re.DOTALL)
                if match:
                    try:
                        loaded = json.loads(match.group(0))
                        if isinstance(loaded, dict):
                            result = loaded
                    except json.JSONDecodeError:
                        result = None
            if result is None:
                return None, None
        if not isinstance(result, dict):
            return None, None
        selected_id = str(result.get("selected_id") or "").strip()
        rationale = str(result.get("rationale") or "").strip()
        if not selected_id:
            return None, rationale or None
        return selected_id, rationale or None

    async def _generate_heatmap_ai_insight(
        self,
        column_name: str,
        matrix_columns: list[str],
        heatmap_data: list[dict[str, Any]],
    ) -> str:
        unique_pairs = self._extract_unique_heatmap_pairs(heatmap_data)
        payload = {
            "target_column": column_name,
            "selected_columns": matrix_columns,
            "pairwise_top": unique_pairs[:12],
        }
        prompt = (
            "You are a quantitative analyst. Summarize this recomputed correlation heatmap in 1-2 concise sentences. "
            "Mention strongest positive/negative relationships and whether overall signal is weak/moderate/strong. "
            "Do not invent external context. Return JSON only: {\"insight\":\"...\"}. "
            f"Context: {json.dumps(payload, default=str)}"
        )
        response_format = {
            "type": "object",
            "properties": {
                "insight": {"type": "string"},
            },
        }
        fallback = self._fallback_heatmap_insight(column_name, matrix_columns, unique_pairs)
        try:
            raw = await self.ai_sql.ai_complete(
                self.model_id, prompt, response_format=response_format
            )
            text = str(raw or "").strip()
            if not text:
                return fallback
            parsed: dict[str, Any] | None = None
            try:
                loaded = json.loads(text)
                if isinstance(loaded, dict):
                    parsed = loaded
            except json.JSONDecodeError:
                match = re.search(r"\{.*\}", text, re.DOTALL)
                if match:
                    try:
                        loaded = json.loads(match.group(0))
                        if isinstance(loaded, dict):
                            parsed = loaded
                    except json.JSONDecodeError:
                        parsed = None
            insight = str((parsed or {}).get("insight") or "").strip()
            return insight or fallback
        except Exception:
            return fallback

    @tool
    async def recompute_correlation_heatmap(
        self,
        table_asset_id: int,
        column_name: str,
        columns: list[str] | None = None,
        sample_size: int | None = None,
        window_days: int | None = None,
    ) -> dict[str, Any]:
        """Recompute a correlation heatmap for selected numeric columns and regenerate insight."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        if sample_size is None:
            override_sample_size = overrides.get("numeric_correlations_sample_size")
            if override_sample_size is not None:
                sample_size = self._coerce_int(override_sample_size)
        if window_days is None:
            override_window_days = overrides.get("numeric_correlations_window_days")
            if override_window_days is not None:
                window_days = self._coerce_int(override_window_days)

        numeric_columns = await self._list_numeric_columns(table_asset_id)
        if column_name not in numeric_columns:
            numeric_columns = [column_name, *numeric_columns]
        canonical_map = {name.lower(): name for name in numeric_columns}

        heatmap_col_limit = self._coerce_int(
            overrides.get("visual_correlation_heatmap_columns"), 8
        )
        if heatmap_col_limit < 2:
            heatmap_col_limit = 2

        matrix_columns: list[str] = [column_name]
        requested = columns if isinstance(columns, list) else []
        for raw in requested:
            text = str(raw or "").strip()
            if not text:
                continue
            canonical = canonical_map.get(text.lower())
            if not canonical or canonical in matrix_columns:
                continue
            matrix_columns.append(canonical)
            if len(matrix_columns) >= heatmap_col_limit:
                break

        if len(matrix_columns) < 2:
            for peer in numeric_columns:
                if peer in matrix_columns:
                    continue
                matrix_columns.append(peer)
                if len(matrix_columns) >= min(heatmap_col_limit, 2):
                    break

        if len(matrix_columns) < 2:
            return {
                "column": column_name,
                "skipped": True,
                "reason": "insufficient_numeric_columns",
            }
        heatmap_data = await self._compute_correlation_heatmap_data(
            ctx,
            matrix_columns,
            sample_size=sample_size,
            window_days=window_days,
        )
        if not heatmap_data:
            return {
                "column": column_name,
                "skipped": True,
                "reason": "insufficient_pairs",
                "source_columns": matrix_columns,
            }
        min_abs_correlation = self._resolve_min_abs_correlation(overrides)
        matrix_columns, heatmap_data, dropped_columns = self._prune_heatmap_by_target_signal(
            column_name,
            matrix_columns,
            heatmap_data,
            min_abs_correlation,
        )
        if len(matrix_columns) < 2 or not heatmap_data:
            return {
                "column": column_name,
                "skipped": True,
                "reason": "weak_correlations_only",
                "source_columns": [column_name],
                "dropped_columns": dropped_columns,
                "min_abs_correlation": min_abs_correlation,
            }

        narrative = [
            "Pairwise correlation matrix across selected numeric columns",
            "Color indicates strength and direction from -1 to 1",
        ]
        if dropped_columns:
            narrative.append(
                f"Filtered {len(dropped_columns)} weak columns below |r|<{round(min_abs_correlation, 3)} vs target."
            )
        chart = self._build_chart_spec(
            chart_type="heatmap",
            title=f"Correlation heatmap for {column_name}",
            x_key="feature_x",
            y_key="feature_y",
            data=heatmap_data,
            narrative=narrative,
            source_columns=matrix_columns,
            x_title="Feature",
            y_title="Feature",
        )
        chart["valueKey"] = "correlation"
        insight = await self._generate_heatmap_ai_insight(
            column_name, matrix_columns, heatmap_data
        )
        if insight:
            chart["insight"] = insight
        return {
            "column": column_name,
            "chart": chart,
            "insight": insight,
            "source_columns": matrix_columns,
            "sample_size": sample_size,
            "window_days": window_days,
            "dropped_columns": dropped_columns,
            "min_abs_correlation": min_abs_correlation,
        }

    async def _select_visuals_with_ai(
        self,
        ctx: Any,
        column_name: str,
        visuals: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        overrides = ctx.column_meta.overrides or {}
        enabled = overrides.get("visual_ai_select")
        if enabled is False:
            return None
        if not visuals:
            return None

        auto_threshold = self._coerce_int(
            overrides.get("visual_ai_select_threshold"),
            8,
        )
        if enabled is not True and len(visuals) <= max(2, auto_threshold):
            # Keep all visuals by default; selection is most useful when chart count is large.
            return None

        limit_override = (
            overrides.get("visual_ai_select_limit")
            or overrides.get("visual_recommend_limit")
        )
        if limit_override is None:
            limit = min(8, max(4, len(visuals) // 2))
        else:
            limit = self._coerce_int(limit_override, 4)
        limit = min(limit, len(visuals))
        if limit <= 0:
            return None

        if len(visuals) <= limit:
            selected_ids = [str(visual.get("id")) for visual in visuals if visual.get("id")]
            return {
                "selected_ids": selected_ids,
                "rationale": "All charts retained (below selection limit).",
                "limit": limit,
                "total": len(visuals),
                "model": getattr(self, "model_id", "ai_complete"),
            }

        max_input = self._coerce_int(
            overrides.get("visual_ai_select_max_input")
            or overrides.get("visual_recommend_max_input"),
            12,
        )
        candidates = visuals[: max_input if max_input > 0 else len(visuals)]

        summaries: list[dict[str, Any]] = []
        for visual in candidates:
            data = visual.get("data") or []
            x_key = visual.get("xKey") or visual.get("x_key")
            y_key = visual.get("yKey") or visual.get("y_key")
            series = visual.get("series") or []
            series_keys = [
                str(item.get("key"))
                for item in series
                if isinstance(item, dict) and item.get("key")
            ]
            if not series_keys and y_key:
                series_keys = [str(y_key)]

            values: list[float] = []
            for row in data:
                if not isinstance(row, dict):
                    continue
                for key in series_keys:
                    raw = row.get(key)
                    if raw is None or isinstance(raw, bool):
                        continue
                    try:
                        values.append(float(raw))
                    except (TypeError, ValueError):
                        continue
            stats = None
            if values:
                stats = {
                    "min": min(values),
                    "max": max(values),
                    "mean": sum(values) / len(values),
                }

            sample_rows: list[dict[str, Any]] = []
            for row in data[:3]:
                if not isinstance(row, dict):
                    continue
                payload: dict[str, Any] = {}
                if x_key:
                    payload["x"] = row.get(x_key)
                for key in series_keys[:3]:
                    payload[key] = row.get(key)
                sample_rows.append(payload)

            summaries.append(
                {
                    "id": visual.get("id"),
                    "title": visual.get("title"),
                    "chart_type": visual.get("chartType") or visual.get("chart_type"),
                    "x_title": visual.get("xTitle") or visual.get("x_title"),
                    "y_title": visual.get("yTitle") or visual.get("y_title"),
                    "points": len(data),
                    "series": [
                        {
                            "key": item.get("key"),
                            "label": item.get("label"),
                            "highlight": item.get("highlight"),
                        }
                        for item in series
                        if isinstance(item, dict)
                    ],
                    "stats": stats,
                    "narrative": visual.get("narrative") or [],
                    "sample": sample_rows,
                }
            )

        prompt = (
            "You are selecting the most useful charts to show for a single column analysis. "
            f"Target column: {column_name}. "
            f"Pick up to {limit} chart IDs from the candidates. "
            "Prefer charts that are informative, non-redundant, highlight the target column, "
            "and include a clear time trend or distribution when available. "
            "If a chart uses year-like fields, reject options that look epoch-misparsed (e.g. 1970 artifacts). "
            "Return JSON: {\"selected_ids\": [\"id1\"], \"rationale\": \"...\"}."
        )
        prompt += f" Candidates: {json.dumps(summaries)}"
        response_format = {
            "type": "object",
            "properties": {
                "selected_ids": {"type": "array", "items": {"type": "string"}},
                "rationale": {"type": "string"},
            },
        }

        def _extract_json(raw_text: str) -> dict[str, Any] | None:
            text = raw_text.strip()
            if not text:
                return None
            if text.startswith("```"):
                text = text.strip("`").strip()
            try:
                parsed_obj = json.loads(text)
                if isinstance(parsed_obj, dict):
                    return parsed_obj
            except json.JSONDecodeError:
                pass
            match = re.search(r"\{.*\}", text, re.DOTALL)
            if match:
                try:
                    parsed_obj = json.loads(match.group(0))
                    if isinstance(parsed_obj, dict):
                        return parsed_obj
                except json.JSONDecodeError:
                    pass
            match = re.search(r"\[.*\]", text, re.DOTALL)
            if match:
                try:
                    parsed_obj = json.loads(match.group(0))
                    if isinstance(parsed_obj, dict):
                        return parsed_obj
                except json.JSONDecodeError:
                    pass
            return None

        parsed: dict[str, Any] | None = None
        error_info: dict[str, Any] | None = None
        raw_preview: str | None = None
        try:
            raw = await self.ai_sql.ai_complete(
                self.model_id, prompt, response_format=response_format
            )
            raw_text = str(raw or "").strip()
            raw_preview = raw_text[:400] if raw_text else None
            if raw_text:
                parsed = _extract_json(raw_text)
            if parsed is None:
                error_info = {"error": "ai_complete_parse_failed", "raw_preview": raw_preview}
        except Exception as exc:
            logger.warning("AI_COMPLETE chart selection failed: %s", exc)
            error_info = {"error": str(exc)}

        if parsed is None:
            # Retry once with a shorter prompt and simplified candidates.
            simple_candidates = [
                {
                    "id": item.get("id"),
                    "title": item.get("title"),
                    "chart_type": item.get("chart_type"),
                    "points": item.get("points"),
                }
                for item in candidates
            ]
            retry_prompt = (
                "Select the best chart IDs for the target column. "
                f"Target column: {column_name}. "
                f"Pick up to {limit} chart IDs. "
                "Avoid charts with obvious year/epoch parsing artifacts. "
                "Return JSON only: {\"selected_ids\": [\"id1\"], \"rationale\": \"...\"}."
            )
            retry_prompt += f" Candidates: {json.dumps(simple_candidates)}"
            try:
                raw_retry = await self.ai_sql.ai_complete(
                    self.model_id, retry_prompt, response_format=response_format
                )
                raw_retry_text = str(raw_retry or "").strip()
                raw_preview = raw_retry_text[:400] if raw_retry_text else raw_preview
                if raw_retry_text:
                    parsed = _extract_json(raw_retry_text)
                if parsed is None and error_info is None:
                    error_info = {"error": "ai_complete_retry_failed", "raw_preview": raw_preview}
            except Exception as exc:
                logger.warning("AI_COMPLETE chart selection retry failed: %s", exc)
                error_info = {"error": f"ai_complete_retry_error:{exc}"}

        selected_ids: list[str] = []
        rationale = None
        if isinstance(parsed, dict):
            selected_ids = [
                str(value)
                for value in (parsed.get("selected_ids") or [])
                if value
            ]
            rationale = parsed.get("rationale")

        candidate_ids = [str(visual.get("id")) for visual in candidates if visual.get("id")]
        selected_ids = [value for value in selected_ids if value in candidate_ids]

        fallback_reason: str | None = None
        if not selected_ids:
            if parsed is None:
                # Avoid rigid chart sets when AI selection output cannot be parsed.
                fallback_reason = "AI selection unavailable; keeping all generated charts."
                return {
                    "selected_ids": [],
                    "rationale": fallback_reason,
                    "limit": limit,
                    "total": len(visuals),
                    "model": getattr(self, "model_id", "ai_complete"),
                    "error": error_info,
                }

            scored: list[tuple[float, str, str, tuple[str, ...]]] = []
            for visual in candidates:
                visual_id = visual.get("id")
                if not visual_id:
                    continue
                visual_id_str = str(visual_id)
                data = visual.get("data") or []
                point_count = len(data) if isinstance(data, list) else 0
                chart_type = str(visual.get("chartType") or visual.get("chart_type") or "bar").lower()
                title = str(visual.get("title") or "").lower()
                series = visual.get("series") or []
                source_columns = visual.get("sourceColumns") or visual.get("source_columns") or []
                source_key = (
                    tuple(str(item) for item in source_columns[:2])
                    if isinstance(source_columns, list)
                    else ()
                )

                score = float(point_count)
                if chart_type == "line":
                    score += 8.0 if point_count >= MIN_TIME_POINTS else -4.0
                elif chart_type == "bar":
                    score += 4.0 if point_count >= 2 else -4.0
                elif chart_type in {"area", "pie"}:
                    score += 2.0
                if isinstance(series, list) and len(series) > 1:
                    score += 4.0
                if "correlation" in title:
                    score += 5.0
                if "distribution" in title:
                    score += 4.0
                if "over time" in title or "trend" in title:
                    score += 4.0

                scored.append((score, visual_id_str, chart_type, source_key))

            scored.sort(key=lambda item: item[0], reverse=True)
            chosen: list[str] = []
            seen_types: set[str] = set()
            seen_sources: set[tuple[str, ...]] = set()
            initial_target = max(1, limit // 2)
            for _score, visual_id, chart_type, source_key in scored:
                if len(chosen) >= limit:
                    break
                type_is_new = chart_type not in seen_types
                source_is_new = bool(source_key) and source_key not in seen_sources
                if type_is_new or source_is_new or len(chosen) < initial_target:
                    chosen.append(visual_id)
                    seen_types.add(chart_type)
                    if source_key:
                        seen_sources.add(source_key)

            if len(chosen) < limit:
                for _score, visual_id, _chart_type, _source_key in scored:
                    if visual_id in chosen:
                        continue
                    chosen.append(visual_id)
                    if len(chosen) >= limit:
                        break
            selected_ids = chosen[:limit]
            fallback_reason = "Selected charts using signal-based fallback ranking."

        return {
            "selected_ids": selected_ids,
            "rationale": rationale or fallback_reason or "Selected charts based on AI ranking.",
            "limit": limit,
            "total": len(visuals),
            "model": getattr(self, "model_id", "ai_complete"),
            "error": error_info,
        }

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
        min_points = self._coerce_int(overrides.get("visual_time_min_points"), MIN_TIME_POINTS)
        if min_points < 2:
            min_points = MIN_TIME_POINTS
        temporal_meta = await self._list_temporal_columns_with_meta(ctx.table_asset_id)
        temporal_meta = [
            item
            for item in temporal_meta
            if item.get("column")
            and (
                item.get("unique_count") is None
                or self._coerce_int(item.get("unique_count"), min_points) >= min_points
            )
        ]
        temporal_meta.sort(
            key=lambda item: self._coerce_int(item.get("unique_count"), 0), reverse=True
        )
        temporal_columns = [item["column"] for item in temporal_meta if item.get("column")]
        time_column_ok = True
        if ctx.time_column:
            time_meta = next(
                (item for item in temporal_meta if item.get("column") == ctx.time_column),
                None,
            )
            if time_meta and time_meta.get("unique_count") is not None:
                time_column_ok = (
                    self._coerce_int(time_meta.get("unique_count"), 0) >= min_points
                )
        analysis_errors: list[dict[str, Any]] = []
        year_anomaly_context: dict[str, Any] | None = None
        year_anomaly_column: str | None = None
        if is_temporal and self._looks_like_year_name(column_name):
            year_anomaly_column = column_name
        elif (not is_temporal) and ctx.time_column and self._looks_like_year_name(ctx.time_column):
            year_anomaly_column = ctx.time_column
        year_conflict_detector = getattr(self, "_detect_year_value_conflicts", None)
        if year_anomaly_column and callable(year_conflict_detector):
            try:
                year_scan_sample = self._coerce_int(
                    overrides.get("visual_year_scan_sample_size")
                    or overrides.get("scan_conflicts_sample_size"),
                    20000,
                )
                year_scan_window = overrides.get("visual_year_scan_window_days")
                year_window_days = (
                    self._coerce_int(year_scan_window)
                    if year_scan_window is not None
                    else None
                )
                year_scan = await year_conflict_detector(
                    ctx,
                    year_anomaly_column,
                    year_scan_sample,
                    max_pairs=8,
                    window_days=year_window_days,
                    time_column=ctx.time_column,
                )
                if isinstance(year_scan, dict):
                    year_anomaly_context = {
                        "column": year_anomaly_column,
                        "anomaly_count": self._coerce_int(year_scan.get("anomaly_count")),
                        "anomaly_rate": year_scan.get("anomaly_rate"),
                        "valid_range": {
                            "min": year_scan.get("range_min"),
                            "max": year_scan.get("range_max"),
                        },
                        "samples": year_scan.get("value_conflicts") or [],
                    }
            except Exception as exc:
                analysis_errors.append(
                    {"step": "year_value_validation", "error": str(exc)}
                )

        if is_temporal:
            time_expr = self._resolve_temporal_expr(ctx, col, column_name)
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
                "panel_visual_sampling": None,
                "queries": {"stats_query": stats_query.strip()},
            }
            if year_anomaly_context:
                analysis["year_value_anomalies"] = year_anomaly_context
            if analysis_errors:
                analysis["errors"] = analysis_errors
            await self._update_column_analysis(ctx, analysis)
            return {"column": column_name, "visuals": custom_visuals, "stats": stats}

        visuals: list[dict[str, Any]] = []
        histogram_data: list[dict[str, Any]] = []
        hist_query: str | None = None
        panel_visual_sampling: dict[str, Any] | None = None

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
        elif ctx.time_column and time_column_ok:
            time_col = self._resolve_temporal_expr(
                ctx, self._quote_ident(ctx.time_column), ctx.time_column
            )
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
            if bucket not in {"year", "month", "week", "day", "hour", "raw"}:
                bucket = "day"
            target_points = self._coerce_int(
                overrides.get("visual_time_target_points"), 96
            )
            if target_points < min_points:
                target_points = min_points
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
                time_bucket_select = self._format_time_bucket_sql(
                    bucket_key, time_bucket_expr
                )
                effective_limit = time_limit
                if bucket_key == "raw" and effective_limit is None:
                    raw_limit = self._coerce_int(
                        overrides.get("visual_time_raw_limit"), 1200
                    )
                    effective_limit = max(200, raw_limit)
                query = f"""
                WITH base AS (
                    {ctx.analysis_query}
                )
                SELECT
                    {time_bucket_select} AS time_bucket,
                    {"COUNT(*) AS count" if is_temporal else f"AVG({col}) AS avg_value"}
                FROM base
                WHERE {time_col} IS NOT NULL
                {"AND " + col + " IS NOT NULL" if not is_temporal else ""}
                GROUP BY {time_bucket_expr}
                ORDER BY time_bucket
                {"" if effective_limit is None else f"LIMIT {int(effective_limit)}"}
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

            # If points are still sparse, try a finer bucket to surface more detail.
            if len(time_rows) < target_points and bucket != "raw":
                finer_candidates: list[str] = []
                if bucket == "year":
                    finer_candidates = ["month", "week", "day", "hour", "raw"]
                elif bucket == "month":
                    finer_candidates = ["week", "day", "hour", "raw"]
                elif bucket == "week":
                    finer_candidates = ["day", "hour", "raw"]
                elif bucket == "day":
                    finer_candidates = ["hour", "raw"]
                elif bucket == "hour":
                    finer_candidates = ["raw"]
                for finer_bucket in finer_candidates:
                    if finer_bucket == bucket:
                        continue
                    finer_query, _ = _build_time_query(finer_bucket)
                    try:
                        finer_rows = await self.sf.execute_query(finer_query)
                    except Exception as exc:
                        logger.warning(
                            "Time series finer-bucket query failed for %s: %s",
                            column_name,
                            exc,
                        )
                        analysis_errors.append(
                            {"step": "time_query_finer_bucket", "error": str(exc)}
                        )
                        continue
                    if len(finer_rows) > len(time_rows):
                        time_rows = finer_rows
                        bucket = finer_bucket
                        time_query = finer_query
                    if len(time_rows) >= target_points:
                        break

            if len(time_rows) < min_points:
                analysis_errors.append(
                    {"step": "time_query", "error": f"insufficient_points:{len(time_rows)}"}
                )
                time_rows = []

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

            if time_data and len(time_data) >= min_points:
                time_title = column_name if is_temporal else (ctx.time_column or "time")
                y_title = "Count" if is_temporal else f"Average {column_name}"
                time_narrative = [
                    (
                        "Daily trend based on counts"
                        if is_temporal
                        else "Daily trend based on average values"
                    ),
                    "Look for seasonality or breaks in trend",
                ]
                if (
                    year_anomaly_context
                    and year_anomaly_column
                    and (
                        (is_temporal and year_anomaly_column == column_name)
                        or ((not is_temporal) and year_anomaly_column == ctx.time_column)
                    )
                ):
                    min_range = (year_anomaly_context.get("valid_range") or {}).get("min")
                    max_range = (year_anomaly_context.get("valid_range") or {}).get("max")
                    anomaly_count = self._coerce_int(
                        year_anomaly_context.get("anomaly_count")
                    )
                    time_narrative.append(
                        f"Filtered {anomaly_count} out-of-range year values (expected {min_range}-{max_range})"
                    )
                time_chart = self._build_chart_spec(
                    chart_type="line",
                    title=f"{column_name} over time",
                    x_key="time_bucket",
                    y_key="count" if is_temporal else "avg_value",
                    data=time_data,
                    narrative=time_narrative,
                    source_columns=(
                        [column_name]
                        if is_temporal
                        else [ctx.time_column, column_name]
                    ),
                    x_title=time_title,
                    y_title=y_title,
                )
                if (
                    year_anomaly_context
                    and year_anomaly_column
                    and (
                        (is_temporal and year_anomaly_column == column_name)
                        or ((not is_temporal) and year_anomaly_column == ctx.time_column)
                    )
                ):
                    self._append_chart_warning(
                        time_chart,
                        self._build_year_anomaly_warning(year_anomaly_context),
                    )
                visuals.append(time_chart)
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
                        "category": self._normalize_category_label(
                            row.get("CATEGORY") or row.get("category")
                        ),
                        "count": self._coerce_int(row.get("COUNT") or row.get("count")),
                    }
                    for row in fallback_rows
                    if self._normalize_category_label(
                        row.get("CATEGORY") or row.get("category")
                    )
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

        # ---- Multi-series temporal trend (normalize when scales diverge) ----
        if not is_temporal:
            multi_time_column = ctx.time_column or (temporal_columns[0] if temporal_columns else None)
            if multi_time_column:
                multi_time_expr = self._resolve_temporal_expr(
                    ctx, self._quote_ident(multi_time_column), multi_time_column
                )
                bucket_override = (
                    overrides.get("visual_time_bucket")
                    or overrides.get("visual_time_granularity")
                )
                default_bucket = self._default_time_bucket(multi_time_column)
                bucket = str(bucket_override or default_bucket or "day").lower()
                if bucket not in {"year", "month", "week", "day", "hour", "raw"}:
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

                max_series = self._coerce_int(
                    overrides.get("visual_multi_series_limit")
                    or overrides.get("visual_multi_series_max_series"),
                    4,
                )
                if max_series < 2:
                    max_series = 2

                analysis_payload = ctx.column_meta.metadata_payload or {}
                analysis_block = (
                    analysis_payload.get("analysis")
                    if isinstance(analysis_payload, dict)
                    else {}
                ) or {}
                correlations = analysis_block.get("correlations") or {}
                correlated: list[str] = []
                for bucket_name in ("positive", "negative"):
                    for item in correlations.get(bucket_name, []) or []:
                        candidate = item.get("column") if isinstance(item, dict) else None
                        if candidate and candidate != column_name and candidate not in correlated:
                            correlated.append(candidate)

                series_columns: list[str] = [column_name]
                for candidate in correlated:
                    if len(series_columns) >= max_series:
                        break
                    series_columns.append(candidate)

                numeric_candidates = await self._list_numeric_columns(ctx.table_asset_id)
                if len(numeric_candidates) < 2 and ctx.table_ref:
                    # Fallback: infer numeric columns from schema when metadata is sparse.
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
                        columns = await self.sf.get_table_columns(
                            table_name, database=database, schema=schema
                        )
                        numeric_types = (
                            "number",
                            "decimal",
                            "numeric",
                            "int",
                            "integer",
                            "float",
                            "double",
                            "real",
                        )
                        fallback_candidates: list[str] = []
                        for col in columns:
                            col_name = col.get("COLUMN_NAME") or col.get("column_name")
                            data_type = str(
                                col.get("DATA_TYPE") or col.get("data_type") or ""
                            ).lower()
                            if not col_name or col_name == column_name:
                                continue
                            if any(token in data_type for token in numeric_types):
                                fallback_candidates.append(str(col_name))
                        if fallback_candidates:
                            numeric_candidates = fallback_candidates
                    except Exception as exc:
                        logger.warning(
                            "Numeric column fallback failed for %s: %s", column_name, exc
                        )

                if len(series_columns) < max_series:
                    for candidate in numeric_candidates:
                        if candidate == column_name or candidate in series_columns:
                            continue
                        series_columns.append(candidate)
                        if len(series_columns) >= max_series:
                            break

                if len(series_columns) >= 2:
                    alias_map: dict[str, str] = {}
                    select_parts: list[str] = []
                    for idx, name in enumerate(series_columns):
                        alias = f"series_{idx}_{self._normalize_identifier(name)}"
                        alias_map[name] = alias
                        select_parts.append(
                            f"AVG({self._numeric_expr(self._quote_ident(name))}) AS {alias}"
                        )

                    if bucket == "raw":
                        time_bucket_expr = f"{multi_time_expr}"
                    else:
                        time_bucket_expr = f"DATE_TRUNC('{bucket}', {multi_time_expr})"
                    time_bucket_select = self._format_time_bucket_sql(
                        bucket, time_bucket_expr
                    )
                    multi_query = f"""
                    WITH base AS (
                        {ctx.analysis_query}
                    )
                    SELECT
                        {time_bucket_select} AS time_bucket,
                        {", ".join(select_parts)}
                    FROM base
                    WHERE {multi_time_expr} IS NOT NULL
                    GROUP BY {time_bucket_expr}
                    ORDER BY time_bucket
                    {"" if time_limit is None else f"LIMIT {int(time_limit)}"}
                    """
                    try:
                        multi_rows = await self.sf.execute_query(multi_query)
                    except Exception as exc:
                        logger.warning("Multi-series query failed for %s: %s", column_name, exc)
                        analysis_errors.append({"step": "multi_series_query", "error": str(exc)})
                        multi_rows = []

                    series_data: list[dict[str, Any]] = []
                    for row in multi_rows or []:
                        payload: dict[str, Any] = {
                            "time_bucket": row.get("TIME_BUCKET") or row.get("time_bucket")
                        }
                        for name, alias in alias_map.items():
                            payload[alias] = self._coerce_float(
                                row.get(alias.upper()) or row.get(alias)
                            )
                        series_data.append(payload)

                    if series_data and len(series_data) >= min_points:
                        series_stats: dict[str, list[float]] = {
                            alias: []
                            for alias in alias_map.values()
                        }
                        for row in series_data:
                            for alias in alias_map.values():
                                value = row.get(alias)
                                if value is not None:
                                    series_stats[alias].append(float(value))
                        ranges = [
                            (max(values) - min(values))
                            for values in series_stats.values()
                            if values
                        ]
                        normalize_override = overrides.get("visual_multi_series_normalize")
                        normalize = bool(normalize_override) if normalize_override is not None else False
                        non_zero = [value for value in ranges if value and value > 0]
                        if not normalize and len(non_zero) > 1:
                            ratio = max(non_zero) / min(non_zero)
                            if ratio >= 25:
                                normalize = True

                        if normalize:
                            for alias, values in series_stats.items():
                                if not values:
                                    continue
                                mean_val = sum(values) / len(values)
                                variance = sum((val - mean_val) ** 2 for val in values) / len(values)
                                std_val = variance ** 0.5 if variance > 0 else 0.0
                                for row in series_data:
                                    raw = row.get(alias)
                                    if raw is None:
                                        continue
                                    if std_val == 0:
                                        row[alias] = 0.0
                                    else:
                                        row[alias] = (float(raw) - mean_val) / std_val

                        series_spec = [
                            {
                                "key": alias_map[name],
                                "label": name,
                                "highlight": name == column_name,
                            }
                            for name in series_columns
                        ]
                        y_title = "Normalized value (z-score)" if normalize else "Average value"
                        spec = self._build_chart_spec(
                            chart_type="line",
                            title=f"{column_name} vs peer metrics over time",
                            x_key="time_bucket",
                            y_key=alias_map[column_name],
                            data=series_data,
                            narrative=[
                                "Multiple numeric series plotted on the same timeline",
                                "Highlighted series is the active column",
                                "Values normalized to compare trend shapes" if normalize else "Values shown in original scale",
                            ],
                            source_columns=[multi_time_column] + series_columns,
                            x_title=multi_time_column,
                            y_title=y_title,
                        )
                        spec["series"] = series_spec
                        if normalize:
                            spec["yScale"] = "linear"
                        visuals.append(spec)

        # ---- Panel entity trend (category × year × value) ----
        if (
            not is_temporal
            and str(ctx.structure_type or "").lower() == "panel"
        ):
            panel_time_column = ctx.time_column or (temporal_columns[0] if temporal_columns else None)
            panel_category_columns = await self._list_categorical_columns(
                ctx.table_asset_id, max_columns=6
            )
            panel_category_meta = None
            if panel_category_columns:
                def _panel_rank(item: dict[str, Any]) -> tuple[int, int]:
                    name = str(item.get("column") or "").lower()
                    unique_count = self._coerce_int(item.get("unique_count"), 0)
                    id_hint = any(
                        token in name
                        for token in (
                            "id",
                            "entity",
                            "model",
                            "store",
                            "customer",
                            "user",
                            "account",
                            "product",
                            "sku",
                        )
                    )
                    return (1 if id_hint else 0, unique_count)

                panel_candidates = [
                    item
                    for item in panel_category_columns
                    if item.get("column")
                    and str(item.get("column")) != column_name
                    and str(item.get("column")) != str(panel_time_column or "")
                ]
                if panel_candidates:
                    panel_category_meta = max(panel_candidates, key=_panel_rank)

            if panel_time_column and panel_category_meta:
                panel_time_expr = self._resolve_temporal_expr(
                    ctx, self._quote_ident(panel_time_column), panel_time_column
                )
                panel_category_column = str(panel_category_meta.get("column"))
                panel_category_col = self._quote_ident(panel_category_column)
                category_limit = self._coerce_int(
                    overrides.get("visual_panel_category_limit")
                    or overrides.get("visual_panel_max_categories"),
                    6,
                )
                if category_limit < 2:
                    category_limit = 2
                panel_bucket_override = (
                    overrides.get("visual_panel_time_bucket")
                    or overrides.get("visual_time_bucket")
                )
                panel_bucket = str(panel_bucket_override or "year").lower()
                if panel_bucket not in {"year", "month", "week", "day", "hour", "raw"}:
                    panel_bucket = "year"
                panel_time_bucket_expr = (
                    panel_time_expr
                    if panel_bucket == "raw"
                    else f"DATE_TRUNC('{panel_bucket}', {panel_time_expr})"
                )
                panel_time_bucket_select = self._format_time_bucket_sql(
                    panel_bucket, "bucket_value"
                )
                panel_query = f"""
                WITH base AS (
                    {ctx.analysis_query}
                ),
                top_categories AS (
                    SELECT
                        {panel_category_col} AS category,
                        COUNT(*) AS row_count
                    FROM base
                    WHERE {panel_time_expr} IS NOT NULL
                    AND {panel_category_col} IS NOT NULL
                    AND {col} IS NOT NULL
                    GROUP BY {panel_category_col}
                    ORDER BY row_count DESC
                    LIMIT {int(category_limit)}
                ),
                panel_points AS (
                    SELECT
                        {panel_time_bucket_expr} AS bucket_value,
                        TO_VARCHAR(base.{panel_category_col}) AS category,
                        AVG({col}) AS avg_value,
                        COUNT(*) AS point_count
                    FROM base
                    JOIN top_categories
                    ON base.{panel_category_col} = top_categories.category
                    WHERE {panel_time_expr} IS NOT NULL
                    AND base.{panel_category_col} IS NOT NULL
                    AND {col} IS NOT NULL
                    GROUP BY bucket_value, TO_VARCHAR(base.{panel_category_col})
                )
                SELECT
                    {panel_time_bucket_select} AS time_bucket,
                    category,
                    avg_value,
                    point_count
                FROM panel_points
                ORDER BY time_bucket ASC, point_count DESC, category ASC
                """
                try:
                    panel_rows = await self.sf.execute_query(panel_query)
                except Exception as exc:
                    logger.warning("Panel trend query failed for %s: %s", column_name, exc)
                    analysis_errors.append(
                        {"step": "panel_trend_query", "error": str(exc)}
                    )
                    panel_rows = []

                if panel_rows:
                    bucket_payload: dict[str, dict[str, float]] = {}
                    category_totals: dict[str, int] = {}
                    for row in panel_rows:
                        raw_bucket = row.get("TIME_BUCKET") or row.get("time_bucket")
                        raw_category = row.get("CATEGORY") or row.get("category")
                        avg_value = self._coerce_float(
                            row.get("AVG_VALUE") or row.get("avg_value")
                        )
                        point_count = self._coerce_int(
                            row.get("POINT_COUNT") or row.get("point_count"), 0
                        )
                        if raw_bucket is None or raw_category is None or avg_value is None:
                            continue
                        bucket_key = str(raw_bucket)
                        category_label = self._normalize_category_label(raw_category)
                        if not category_label:
                            continue
                        bucket_payload.setdefault(bucket_key, {})[category_label] = float(avg_value)
                        category_totals[category_label] = category_totals.get(category_label, 0) + max(point_count, 1)

                    selected_categories = [
                        label
                        for label, _score in sorted(
                            category_totals.items(),
                            key=lambda item: item[1],
                            reverse=True,
                        )[:category_limit]
                    ]

                    if selected_categories and len(bucket_payload) >= min_points:
                        alias_map: dict[str, str] = {}
                        for idx, label in enumerate(selected_categories):
                            alias = f"panel_series_{idx}_{self._normalize_identifier(label)}"
                            alias_map[label] = alias

                        panel_series_data: list[dict[str, Any]] = []
                        for bucket_key in sorted(bucket_payload.keys()):
                            payload: dict[str, Any] = {"time_bucket": bucket_key}
                            for label in selected_categories:
                                payload[alias_map[label]] = bucket_payload[bucket_key].get(label)
                            panel_series_data.append(payload)

                        if len(panel_series_data) >= min_points and len(selected_categories) >= 2:
                            panel_spec = self._build_chart_spec(
                                chart_type="line",
                                title=f"{column_name} panel trend by {panel_category_column}",
                                x_key="time_bucket",
                                y_key=alias_map[selected_categories[0]],
                                data=panel_series_data,
                                narrative=[
                                    f"Top {len(selected_categories)} {panel_category_column} groups plotted over {panel_bucket}",
                                    "Compares entity-level trajectories on the same timeline",
                                ],
                                source_columns=[panel_time_column, panel_category_column, column_name],
                                x_title=panel_time_column,
                                y_title=f"Average {column_name}",
                            )
                            panel_spec["series"] = [
                                {
                                    "key": alias_map[label],
                                    "label": label,
                                    "highlight": idx == 0,
                                }
                                for idx, label in enumerate(selected_categories)
                            ]
                            if (
                                year_anomaly_context
                                and year_anomaly_column
                                and panel_time_column == year_anomaly_column
                            ):
                                self._append_chart_warning(
                                    panel_spec,
                                    self._build_year_anomaly_warning(year_anomaly_context),
                                )
                            visuals.append(panel_spec)
                            panel_visual_sampling = {
                                "time_column": panel_time_column,
                                "category_column": panel_category_column,
                                "bucket": panel_bucket,
                                "category_count": len(selected_categories),
                                "selected_categories": selected_categories,
                                "point_count": len(panel_series_data),
                            }

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
            if bucket not in {"year", "month", "week", "day", "hour", "raw"}:
                bucket = "day"
            target_points = self._coerce_int(
                overrides.get("visual_time_target_points"), 96
            )
            if target_points < min_points:
                target_points = min_points
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
                    ctx, self._quote_ident(temporal_column), temporal_column
                )
                column_bucket = (
                    bucket
                    if bucket_override
                    else str(self._default_time_bucket(temporal_column) or "day").lower()
                )
                if column_bucket not in {"year", "month", "week", "day", "hour", "raw"}:
                    column_bucket = "day"

                def _build_extra_query(bucket_key: str) -> tuple[str, str]:
                    time_bucket_expr = (
                        f"{temporal_expr}"
                        if bucket_key == "raw"
                        else f"DATE_TRUNC('{bucket_key}', {temporal_expr})"
                    )
                    time_bucket_select = self._format_time_bucket_sql(
                        bucket_key, time_bucket_expr
                    )
                    effective_limit = time_limit
                    if bucket_key == "raw" and effective_limit is None:
                        raw_limit = self._coerce_int(
                            overrides.get("visual_time_raw_limit"), 1200
                        )
                        effective_limit = max(200, raw_limit)
                    query = f"""
                    WITH base AS (
                        {ctx.analysis_query}
                    )
                    SELECT
                        {time_bucket_select} AS time_bucket,
                        AVG({col}) AS avg_value
                    FROM base
                    WHERE {temporal_expr} IS NOT NULL
                    AND {col} IS NOT NULL
                    GROUP BY {time_bucket_expr}
                    ORDER BY time_bucket
                    {"" if effective_limit is None else f"LIMIT {int(effective_limit)}"}
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

                if len(extra_data) < target_points and column_bucket != "raw":
                    finer_candidates: list[str] = []
                    if column_bucket == "year":
                        finer_candidates = ["month", "week", "day", "hour", "raw"]
                    elif column_bucket == "month":
                        finer_candidates = ["week", "day", "hour", "raw"]
                    elif column_bucket == "week":
                        finer_candidates = ["day", "hour", "raw"]
                    elif column_bucket == "day":
                        finer_candidates = ["hour", "raw"]
                    elif column_bucket == "hour":
                        finer_candidates = ["raw"]
                    for finer_bucket in finer_candidates:
                        if finer_bucket == column_bucket:
                            continue
                        finer_query, _ = _build_extra_query(finer_bucket)
                        try:
                            finer_rows = await self.sf.execute_query(finer_query)
                        except Exception as exc:
                            logger.warning(
                                "Extra time series finer-bucket failed for %s: %s",
                                column_name,
                                exc,
                            )
                            analysis_errors.append(
                                {
                                    "step": "extra_time_query_finer_bucket",
                                    "error": str(exc),
                                }
                            )
                            continue
                        finer_data = [
                            {
                                "time_bucket": row.get("TIME_BUCKET") or row.get("time_bucket"),
                                "avg_value": self._coerce_float(
                                    row.get("AVG_VALUE") or row.get("avg_value")
                                ),
                            }
                            for row in finer_rows
                        ]
                        if len(finer_data) > len(extra_data):
                            extra_data = finer_data
                            column_bucket = finer_bucket
                        if len(extra_data) >= target_points:
                            break
                if len(extra_data) < min_points:
                    continue

                if extra_data and len(extra_data) >= min_points:
                    extra_chart = self._build_chart_spec(
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
                    if (
                        year_anomaly_context
                        and year_anomaly_column
                        and temporal_column == year_anomaly_column
                    ):
                        self._append_chart_warning(
                            extra_chart,
                            self._build_year_anomaly_warning(year_anomaly_context),
                        )
                    visuals.append(extra_chart)
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

                cat_accumulator: dict[str, dict[str, float]] = {}
                for row in cat_rows:
                    label = self._normalize_category_label(
                        row.get("CATEGORY") or row.get("category")
                    )
                    if not label:
                        continue
                    avg_value = self._coerce_float(
                        row.get("AVG_VALUE") or row.get("avg_value")
                    )
                    count = self._coerce_int(row.get("COUNT") or row.get("count"))
                    if avg_value is None:
                        continue
                    weight = float(max(count, 1))
                    item = cat_accumulator.setdefault(
                        label, {"weighted_sum": 0.0, "count": 0.0}
                    )
                    item["weighted_sum"] += float(avg_value) * weight
                    item["count"] += weight
                cat_data = [
                    {
                        "category": label,
                        "avg_value": round(
                            entry["weighted_sum"] / max(entry["count"], 1.0), 6
                        ),
                        "count": int(entry["count"]),
                    }
                    for label, entry in cat_accumulator.items()
                    if entry["count"] > 0
                ]
                # Keep category selection frequency-based, but display bars by metric magnitude.
                cat_data.sort(
                    key=lambda item: (
                        self._coerce_float(item.get("avg_value"), 0.0),
                        self._coerce_int(item.get("count"), 0),
                    ),
                    reverse=True,
                )
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

        # ---- Correlation summary chart (top absolute correlations) ----
        analysis_payload = ctx.column_meta.metadata_payload or {}
        analysis_block = (
            analysis_payload.get("analysis") if isinstance(analysis_payload, dict) else {}
        ) or {}
        correlations = analysis_block.get("correlations") or {}
        corr_items = correlations.get("all") or []
        if not corr_items:
            corr_items = []
            for bucket_name in ("positive", "negative", "weak"):
                corr_items.extend(correlations.get(bucket_name, []) or [])
        corr_limit = self._coerce_int(
            overrides.get("visual_correlation_limit")
            or overrides.get("visual_correlation_max"),
            6,
        )
        corr_data: list[dict[str, Any]] = []
        if corr_items:
            seen_cols: set[str] = set()
            ranked = []
            for item in corr_items:
                if not isinstance(item, dict):
                    continue
                col_name = item.get("column")
                corr_value = self._coerce_float(item.get("correlation"))
                if not col_name or corr_value is None:
                    continue
                if col_name in seen_cols:
                    continue
                seen_cols.add(col_name)
                ranked.append({"column": col_name, "correlation": corr_value})
            ranked.sort(key=lambda entry: abs(entry["correlation"]), reverse=True)
            for item in ranked[: max(1, corr_limit)]:
                corr_data.append(
                    {
                        "feature": item["column"],
                        "correlation": round(float(item["correlation"]), 4),
                    }
                )
        if corr_data:
            visuals.append(
                self._build_chart_spec(
                    chart_type="bar",
                    title=f"Correlation with {column_name}",
                    x_key="feature",
                    y_key="correlation",
                    data=corr_data,
                    narrative=[
                        "Correlation coefficients against top numeric peers",
                        "Positive values move with the column; negative move against it",
                    ],
                    source_columns=[column_name]
                    + [row.get("feature") for row in corr_data if row.get("feature")],
                    x_title="Feature",
                    y_title="Correlation",
                )
            )

        # ---- Correlation heatmap (auto combo candidates + AI selection) ----
        heatmap_col_limit = self._coerce_int(
            overrides.get("visual_correlation_heatmap_columns"), 8
        )
        if heatmap_col_limit < 2:
            heatmap_col_limit = 2
        heatmap_peer_limit = max(1, heatmap_col_limit - 1)
        min_abs_correlation = self._resolve_min_abs_correlation(overrides)

        ranked_peers: list[dict[str, Any]] = []
        fallback_ranked_peers: list[dict[str, Any]] = []
        seen_heatmap_peers: set[str] = set()
        for item in corr_items:
            if not isinstance(item, dict):
                continue
            peer = str(item.get("column") or "").strip()
            corr_value = self._coerce_float(item.get("correlation"))
            if not peer or peer == column_name or peer in seen_heatmap_peers:
                continue
            if corr_value is None:
                continue
            seen_heatmap_peers.add(peer)
            entry = {"column": peer, "correlation": corr_value}
            fallback_ranked_peers.append(entry)
            if abs(float(corr_value)) >= min_abs_correlation:
                ranked_peers.append(entry)
        ranked_peers.sort(
            key=lambda entry: abs(float(entry.get("correlation") or 0.0)),
            reverse=True,
        )
        fallback_ranked_peers.sort(
            key=lambda entry: abs(float(entry.get("correlation") or 0.0)),
            reverse=True,
        )

        if not ranked_peers and fallback_ranked_peers:
            ranked_peers = fallback_ranked_peers[
                : max(1, min(heatmap_peer_limit, 3))
            ]

        if not ranked_peers:
            fallback_numeric = [
                name
                for name in await self._list_numeric_columns(ctx.table_asset_id)
                if name != column_name
            ]
            for peer in fallback_numeric[: max(heatmap_peer_limit * 2, heatmap_peer_limit)]:
                ranked_peers.append({"column": peer, "correlation": 0.0})

        candidate_combos = self._build_correlation_combo_candidates(
            column_name, ranked_peers, heatmap_peer_limit
        )
        if not candidate_combos:
            seed_combo = [column_name]
            for item in ranked_peers:
                peer = str(item.get("column") or "").strip()
                if not peer or peer in seed_combo:
                    continue
                seed_combo.append(peer)
                if len(seed_combo) >= heatmap_col_limit:
                    break
            if len(seed_combo) >= 2:
                candidate_combos = [seed_combo]

        combo_limit = self._coerce_int(
            overrides.get("visual_correlation_combo_candidates"),
            5,
        )
        if combo_limit <= 0:
            combo_limit = 1
        candidate_combos = candidate_combos[: min(combo_limit, 8)]

        correlation_sample_size: int | None = None
        sample_size_override = overrides.get("numeric_correlations_sample_size")
        if sample_size_override is not None:
            correlation_sample_size = self._coerce_int(sample_size_override)
        correlation_window_days: int | None = None
        window_days_override = overrides.get("numeric_correlations_window_days")
        if window_days_override is not None:
            correlation_window_days = self._coerce_int(window_days_override)

        combo_results: list[dict[str, Any]] = []
        for idx, combo in enumerate(candidate_combos):
            if len(combo) < 2:
                continue
            try:
                heatmap_data = await self._compute_correlation_heatmap_data(
                    ctx,
                    combo,
                    sample_size=correlation_sample_size,
                    window_days=correlation_window_days,
                )
            except Exception as exc:
                logger.warning(
                    "Correlation heatmap combo query failed for %s (combo %s): %s",
                    column_name,
                    idx,
                    exc,
                )
                analysis_errors.append(
                    {
                        "step": "correlation_heatmap_combo",
                        "combo_index": idx,
                        "columns": combo,
                        "error": str(exc),
                    }
                )
                continue
            if not heatmap_data:
                continue
            metrics = self._score_correlation_combo(combo, heatmap_data)
            combo_results.append(
                {
                    "id": f"combo_{idx}",
                    "columns": combo,
                    "data": heatmap_data,
                    "metrics": metrics,
                }
            )

        if combo_results:
            combo_results.sort(
                key=lambda item: float((item.get("metrics") or {}).get("score") or -1.0),
                reverse=True,
            )
            selected_combo = combo_results[0]
            selection_rationale: str | None = None

            ai_combo_selection = overrides.get("visual_correlation_combo_ai_select")
            if len(combo_results) > 1 and ai_combo_selection is not False:
                selected_id, ai_rationale = await self._select_best_correlation_combo(
                    column_name, combo_results
                )
                if selected_id:
                    matched = next(
                        (
                            candidate
                            for candidate in combo_results
                            if str(candidate.get("id")) == selected_id
                        ),
                        None,
                    )
                    if matched:
                        selected_combo = matched
                if ai_rationale:
                    selection_rationale = ai_rationale

            selected_columns = list(selected_combo.get("columns") or [])
            selected_data = list(selected_combo.get("data") or [])
            selected_columns, selected_data, dropped_columns = self._prune_heatmap_by_target_signal(
                column_name,
                selected_columns,
                selected_data,
                min_abs_correlation,
            )
            if selected_data:
                narrative = [
                    "Pairwise correlation matrix across auto-selected numeric columns",
                    "Color indicates strength and direction from -1 to 1",
                ]
                if dropped_columns:
                    narrative.append(
                        f"Filtered {len(dropped_columns)} weak columns below |r|<{round(min_abs_correlation, 3)} vs target."
                    )
                heatmap_spec = self._build_chart_spec(
                    chart_type="heatmap",
                    title=f"Correlation heatmap for {column_name}",
                    x_key="feature_x",
                    y_key="feature_y",
                    data=selected_data,
                    narrative=narrative,
                    source_columns=selected_columns,
                    x_title="Feature",
                    y_title="Feature",
                )
                heatmap_spec["valueKey"] = "correlation"
                heatmap_spec["selection"] = {
                    "mode": "auto_combo_selection",
                    "candidate_count": len(combo_results),
                    "selected_id": selected_combo.get("id"),
                    "metrics": selected_combo.get("metrics"),
                    "rationale": selection_rationale,
                    "min_abs_correlation": min_abs_correlation,
                    "dropped_columns": dropped_columns,
                }
                if selection_rationale:
                    heatmap_spec["narrative"] = [
                        *list(heatmap_spec.get("narrative") or []),
                        f"Selection rationale: {selection_rationale}",
                    ]
                insight = await self._generate_heatmap_ai_insight(
                    column_name,
                    selected_columns,
                    selected_data,
                )
                if insight:
                    heatmap_spec["insight"] = insight
                visuals.append(heatmap_spec)
        elif candidate_combos:
            analysis_errors.append(
                {
                    "step": "correlation_heatmap",
                    "error": "no_valid_correlation_combo",
                    "candidate_count": len(candidate_combos),
                }
            )

        filtered_visuals, dropped_visuals = self._filter_low_signal_visuals(
            visuals, overrides
        )
        if dropped_visuals:
            analysis_errors.append(
                {
                    "step": "visual_quality_filter",
                    "dropped_count": len(dropped_visuals),
                    "dropped_preview": dropped_visuals[:8],
                }
            )
        visuals = filtered_visuals

        analysis: dict[str, Any] = {
            "visuals": visuals,
            "stats": stats,
            "panel_visual_sampling": panel_visual_sampling,
            "queries": {"stats_query": stats_query.strip()},
        }
        if year_anomaly_context:
            analysis["year_value_anomalies"] = year_anomaly_context
        if analysis_errors:
            analysis["errors"] = analysis_errors
        if histogram_data and hist_query:
            analysis["queries"]["hist_query"] = hist_query.strip()
        if time_col:
            analysis["queries"]["time_query"] = time_query.strip()
        if fallback_query and is_temporal:
            analysis["queries"]["temporal_fallback_query"] = fallback_query.strip()

        selection = await self._select_visuals_with_ai(ctx, column_name, visuals)
        if selection:
            selected_ids = set(selection.get("selected_ids") or [])
            if selected_ids:
                for visual in visuals:
                    visual["selected"] = visual.get("id") in selected_ids
        # Always overwrite visual_selection to avoid stale "4 of 7" metadata from previous runs.
        analysis["visual_selection"] = selection if selection else None

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

        top_counts: dict[str, int] = {}
        for row in top_rows_raw:
            label = self._normalize_category_label(
                row.get("CATEGORY") or row.get("category")
            )
            if not label:
                continue
            count = self._coerce_int(row.get("COUNT") or row.get("count"))
            top_counts[label] = top_counts.get(label, 0) + count
        top_rows = [
            {"category": label, "count": count}
            for label, count in top_counts.items()
        ]
        top_rows.sort(key=lambda item: self._coerce_int(item.get("count")), reverse=True)
        if top_limit is not None and len(top_rows) > top_limit:
            kept_rows = top_rows[:top_limit]
            other_count = sum(
                self._coerce_int(item.get("count")) for item in top_rows[top_limit:]
            )
            if other_count > 0:
                kept_rows.append({"category": "Other", "count": other_count})
            top_rows = kept_rows
        top_sum = sum(self._coerce_int(row.get("count")) for row in top_rows)
        if total_count > top_sum:
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

        pie_limit_override = overrides.get("visual_pie_limit") or overrides.get("pie_limit")
        if isinstance(pie_limit_override, str) and pie_limit_override.lower() == "all":
            pie_limit = None
        elif pie_limit_override in ("", 0, -1, None):
            pie_limit = 8
        else:
            pie_limit = max(3, self._coerce_int(pie_limit_override, 8))

        pie_rows = top_rows
        if pie_limit is not None and len(top_rows) > pie_limit:
            pie_rows = top_rows[:pie_limit]
        pie_sum = sum(row.get("count", 0) for row in pie_rows)
        if total_count > pie_sum:
            pie_rows = pie_rows + [{"category": "Other", "count": total_count - pie_sum}]

        if len(pie_rows) >= 2:
            visuals.append(
                self._build_chart_spec(
                    chart_type="pie",
                    title=f"Share of {column_name} categories",
                    x_key="category",
                    y_key="count",
                    data=pie_rows,
                    narrative=["Pie chart for top categories with remaining grouped as Other"],
                    source_columns=[column_name],
                    x_title=column_name,
                    y_title="Count",
                )
            )

        min_points = self._coerce_int(overrides.get("visual_time_min_points"), MIN_TIME_POINTS)
        if min_points < 2:
            min_points = MIN_TIME_POINTS
        temporal_meta = await self._list_temporal_columns_with_meta(ctx.table_asset_id)
        temporal_meta = [
            item
            for item in temporal_meta
            if item.get("column")
            and (
                item.get("unique_count") is None
                or self._coerce_int(item.get("unique_count"), min_points) >= min_points
            )
        ]
        temporal_meta.sort(
            key=lambda item: self._coerce_int(item.get("unique_count"), 0), reverse=True
        )
        temporal_columns = [item["column"] for item in temporal_meta]
        for temporal_column in temporal_columns:
            temporal_expr = self._resolve_temporal_expr(
                ctx, self._quote_ident(temporal_column), temporal_column
            )
            bucket_override = (
                overrides.get("visual_time_bucket")
                or overrides.get("visual_time_granularity")
            )
            default_bucket = self._default_time_bucket(temporal_column)
            bucket = str(bucket_override or default_bucket or "day").lower()
            if bucket not in {"year", "month", "week", "day", "hour", "raw"}:
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
                time_bucket_select = self._format_time_bucket_sql(
                    bucket_key, time_bucket_expr
                )
                query = f"""
                WITH base AS (
                    {ctx.analysis_query}
                )
                SELECT
                    {time_bucket_select} AS time_bucket,
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

            if len(time_data) < min_points:
                continue

            if time_data and len(time_data) >= min_points:
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

            numeric_accumulator: dict[str, dict[str, float]] = {}
            for row in numeric_rows:
                label = self._normalize_category_label(
                    row.get("CATEGORY") or row.get("category")
                )
                if not label:
                    continue
                avg_value = self._coerce_float(
                    row.get("AVG_VALUE") or row.get("avg_value")
                )
                if avg_value is None:
                    continue
                item = numeric_accumulator.setdefault(label, {"sum": 0.0, "n": 0.0})
                item["sum"] += float(avg_value)
                item["n"] += 1.0
            numeric_data = [
                {
                    "category": label,
                    "avg_value": round(item["sum"] / max(item["n"], 1.0), 6),
                }
                for label, item in numeric_accumulator.items()
                if item["n"] > 0
            ]
            numeric_data.sort(
                key=lambda item: self._coerce_float(item.get("avg_value"), 0.0),
                reverse=True,
            )
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

        filtered_visuals, dropped_visuals = self._filter_low_signal_visuals(
            visuals, overrides
        )
        if dropped_visuals:
            analysis_errors.append(
                {
                    "step": "visual_quality_filter",
                    "dropped_count": len(dropped_visuals),
                    "dropped_preview": dropped_visuals[:8],
                }
            )
        visuals = filtered_visuals

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
        selection = await self._select_visuals_with_ai(ctx, column_name, visuals)
        if selection:
            selected_ids = set(selection.get("selected_ids") or [])
            if selected_ids:
                for visual in visuals:
                    visual["selected"] = visual.get("id") in selected_ids
        # Always overwrite visual_selection to avoid stale recommendations from previous runs.
        analysis["visual_selection"] = selection if selection else None
        if analysis_errors:
            analysis["errors"] = analysis_errors

        await self._update_column_analysis(ctx, analysis)
        return {"column": column_name, "visuals": visuals, "total_count": total_count}
