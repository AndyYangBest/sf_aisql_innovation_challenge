"""Column workflow tool mixin."""

from __future__ import annotations

import ast
import json
import logging
import re
from typing import Any, Iterable

from strands import tool

logger = logging.getLogger(__name__)


class ColumnWorkflowInsightsMixin:
    """Tool mixin."""

    def _tokenize_identifier(self, value: str | None, limit: int = 10) -> list[str]:
        if not value:
            return []
        tokens = [
            token.strip().lower()
            for token in re.split(r"[^A-Za-z0-9]+", str(value))
            if token and token.strip()
        ]
        return tokens[: max(1, limit)]

    def _table_name_hint(self, table_ref: str | None) -> str | None:
        if not table_ref:
            return None
        cleaned = str(table_ref).strip()
        if not cleaned:
            return None
        parts = [part.strip().strip('"') for part in cleaned.split(".") if part.strip()]
        if not parts:
            return None
        return parts[-1]

    async def _build_ai_count_context(
        self,
        ctx: Any,
        column_name: str,
        analysis: dict[str, Any],
    ) -> dict[str, Any]:
        snapshot = await self._ensure_analysis_snapshot(ctx, column_name)
        snapshot = snapshot if isinstance(snapshot, dict) else {}

        full_total_rows = self._coerce_int(snapshot.get("total_count"))
        full_null_rows = self._coerce_int(snapshot.get("null_count"))
        full_null_rate = (
            round(full_null_rows / full_total_rows, 6) if full_total_rows else None
        )

        raw_distribution = analysis.get("distribution")
        distribution = raw_distribution if isinstance(raw_distribution, dict) else {}
        distribution_total_rows = self._coerce_int(distribution.get("total_count"))

        raw_nulls = analysis.get("nulls")
        nulls = raw_nulls if isinstance(raw_nulls, dict) else {}
        sampled_rows = self._coerce_int(nulls.get("total_count"))
        sampled_null_rows = self._coerce_int(nulls.get("null_count"))
        sampled_null_rate = self._coerce_float(nulls.get("null_rate"))
        if sampled_null_rate is None and sampled_rows:
            sampled_null_rate = round(sampled_null_rows / max(sampled_rows, 1), 6)

        if full_total_rows:
            authoritative_total_rows = full_total_rows
            authoritative_source = "snapshot"
        elif distribution_total_rows:
            authoritative_total_rows = distribution_total_rows
            authoritative_source = "distribution"
        else:
            authoritative_total_rows = sampled_rows
            authoritative_source = "null_scan_sample"

        null_scan_uses_sampling = bool(
            sampled_rows and (not full_total_rows or sampled_rows < full_total_rows)
        )

        return {
            "authoritative_total_rows": authoritative_total_rows,
            "authoritative_total_rows_source": authoritative_source,
            "full_snapshot_total_rows": full_total_rows,
            "full_snapshot_null_rows": full_null_rows,
            "full_snapshot_null_rate": full_null_rate,
            "null_scan_sampled_rows": sampled_rows,
            "null_scan_sampled_null_rows": sampled_null_rows,
            "null_scan_sampled_null_rate": sampled_null_rate,
            "null_scan_uses_sampling": null_scan_uses_sampling,
            "reporting_rule": (
                "Use authoritative_total_rows for dataset size. "
                "Treat null_scan_* fields as sampled metrics."
            ),
        }

    def _normalize_nulls_for_ai(
        self,
        analysis: dict[str, Any],
        count_context: dict[str, Any],
    ) -> dict[str, Any]:
        raw_nulls = analysis.get("nulls")
        nulls = dict(raw_nulls) if isinstance(raw_nulls, dict) else {}
        sampled_rows = self._coerce_int(nulls.pop("total_count", None))
        sampled_null_rows = self._coerce_int(nulls.pop("null_count", None))
        sampled_null_rate = self._coerce_float(nulls.pop("null_rate", None))
        if sampled_null_rate is None and sampled_rows:
            sampled_null_rate = round(sampled_null_rows / max(sampled_rows, 1), 6)

        normalized = {
            "sampled_rows": sampled_rows
            if sampled_rows
            else self._coerce_int(count_context.get("null_scan_sampled_rows")),
            "sampled_null_rows": sampled_null_rows
            if sampled_null_rows
            else self._coerce_int(count_context.get("null_scan_sampled_null_rows")),
            "sampled_null_rate": sampled_null_rate
            if sampled_null_rate is not None
            else self._coerce_float(count_context.get("null_scan_sampled_null_rate")),
            "full_total_rows": self._coerce_int(
                count_context.get("full_snapshot_total_rows")
            ),
            "full_null_rows": self._coerce_int(
                count_context.get("full_snapshot_null_rows")
            ),
            "full_null_rate": self._coerce_float(
                count_context.get("full_snapshot_null_rate")
            ),
            "uses_sampling": bool(count_context.get("null_scan_uses_sampling")),
            "reporting_note": (
                "sampled_* metrics may not represent full-table totals; "
                "use count_context.authoritative_total_rows for total record count."
            ),
        }
        if nulls:
            normalized["extra"] = nulls
        return normalized

    def _extract_insights_from_json_blob(self, text: str) -> list[str]:
        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```[a-zA-Z]*", "", cleaned).strip()
            cleaned = cleaned.rsplit("```", 1)[0].strip()
        cleaned = cleaned.lstrip("-•* ").strip()
        if "insights" not in cleaned.lower():
            return []
        for parser in (json.loads, ast.literal_eval):
            try:
                parsed = parser(cleaned)
            except Exception:
                continue
            if isinstance(parsed, dict):
                insights = None
                for key, value in parsed.items():
                    if str(key).strip().lower() == "insights":
                        insights = value
                        break
                if isinstance(insights, list):
                    return [str(item).strip() for item in insights if str(item).strip()]
                if isinstance(insights, str) and insights.strip():
                    return [insights.strip()]
        # Fallback: regex extract list content
        match = re.search(
            r'insights"?\s*[:=]\s*\[(.*?)\]',
            cleaned,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if match:
            blob = match.group(1)
            wrapped = f"[{blob}]"
            for parser in (json.loads, ast.literal_eval):
                try:
                    parsed = parser(wrapped)
                except Exception:
                    continue
                if isinstance(parsed, list):
                    extracted = [
                        str(item).strip() for item in parsed if str(item).strip()
                    ]
                    if extracted:
                        return extracted
            quoted = re.findall(r'"([^"]+)"|\'([^\']+)\'', blob)
            if quoted:
                extracted = [
                    (left or right).strip()
                    for left, right in quoted
                    if (left or right).strip()
                ]
                if extracted:
                    return extracted
        return []

    def _split_freeform_items(self, text: str) -> list[str]:
        bullet_items: list[str] = []
        for line in str(text).splitlines():
            candidate_line = line.strip()
            if not candidate_line:
                continue
            matched = re.match(r"^(?:[-•*]|\d+[.)])\s+(.*)$", candidate_line)
            if matched:
                candidate = matched.group(1).strip()
                if candidate:
                    bullet_items.append(candidate)
        if bullet_items:
            return bullet_items

        parts = re.split(
            r"(?:\n+|^\s*[-•*]\s+|^\s*\d+[.)]\s+)",
            str(text),
            flags=re.MULTILINE,
        )
        normalized = [
            part.strip(" \t-•*\"'") for part in parts if part and part.strip()
        ]
        if normalized:
            return normalized
        return [str(text).strip()] if str(text).strip() else []

    def _extract_json_dict(self, raw_text: str) -> dict[str, Any] | None:
        text = str(raw_text or "").strip()
        if not text:
            return None
        if text.startswith("```"):
            text = re.sub(r"^```[a-zA-Z0-9_-]*", "", text).strip()
            if text.endswith("```"):
                text = text[:-3].strip()
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass

        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            return None
        try:
            parsed = json.loads(match.group(0))
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None

    def _compact_payload_for_ai_complete(
        self,
        payload: dict[str, Any],
        level: int = 0,
    ) -> dict[str, Any]:
        list_limit = 120 if level <= 0 else 40 if level == 1 else 15
        key_limit = 80 if level <= 0 else 45 if level == 1 else 25
        string_limit = 4000 if level <= 0 else 1200 if level == 1 else 500

        def _compact(value: Any, depth: int = 0) -> Any:
            if isinstance(value, dict):
                compacted: dict[str, Any] = {}
                items = list(value.items())
                depth_key_limit = key_limit if depth < 3 else max(10, key_limit // 2)
                for index, (key, nested) in enumerate(items):
                    if index >= depth_key_limit:
                        compacted["_truncated_keys"] = len(items) - depth_key_limit
                        break
                    compacted[str(key)] = _compact(nested, depth + 1)
                return compacted
            if isinstance(value, list):
                depth_list_limit = list_limit if depth < 3 else max(8, list_limit // 2)
                compacted_items = [
                    _compact(item, depth + 1) for item in value[:depth_list_limit]
                ]
                if len(value) > depth_list_limit:
                    compacted_items.append(
                        {"_truncated_items": len(value) - depth_list_limit}
                    )
                return compacted_items
            if isinstance(value, str):
                cleaned = value.strip()
                if len(cleaned) <= string_limit:
                    return cleaned
                return (
                    f"{cleaned[:string_limit]}..."
                    f" [truncated {len(cleaned) - string_limit} chars]"
                )
            return value

        return _compact(payload if isinstance(payload, dict) else {})

    async def _run_ai_complete_with_payload(
        self,
        payload: dict[str, Any],
        instruction: str,
        response_format: dict[str, Any],
    ) -> dict[str, Any]:
        last_error: Exception | None = None
        for level in (0, 1, 2):
            compact_payload = self._compact_payload_for_ai_complete(payload, level=level)
            context_blob = json.dumps(
                compact_payload, ensure_ascii=False, default=str
            )
            prompt = (
                f"{instruction.strip()}\n\n"
                "Return JSON only.\n"
                f"Context JSON: {context_blob}"
            )
            try:
                raw = await self.ai_sql.ai_complete(
                    self.model_id, prompt, response_format=response_format
                )
            except Exception as exc:
                last_error = exc
                continue
            if isinstance(raw, dict):
                return raw
            parsed = self._extract_json_dict(str(raw or ""))
            if isinstance(parsed, dict):
                return parsed
        if last_error is not None:
            logger.warning("AI_COMPLETE insights call failed: %s", last_error)
        return {}

    def _ensure_markdown_bullets(self, items: list[str]) -> list[str]:
        normalized: list[str] = []
        for item in items:
            text = item.strip()
            if not text:
                continue
            if not text.lstrip().startswith(("-", "•")):
                text = f"- {text}"
            normalized.append(text)
        return normalized

    def _merge_unique_items(self, items: Iterable[str], max_items: int) -> list[str]:
        seen: set[str] = set()
        merged: list[str] = []
        for item in items:
            cleaned = item.strip()
            if not cleaned:
                continue
            key = cleaned.lower()
            if key in seen:
                continue
            seen.add(key)
            merged.append(cleaned)
            if len(merged) >= max_items:
                break
        return merged

    def _normalize_insight_items(self, raw_items: Any, max_items: int = 3) -> list[str]:
        if raw_items is None:
            return []
        items: list[Any]
        if isinstance(raw_items, list):
            items = raw_items
        else:
            items = [raw_items]

        normalized: list[str] = []
        skip_pattern = re.compile(
            r"(would you like|please let me know|feel free|if you need|approval|approve the plan)",
            re.IGNORECASE,
        )
        for item in items:
            if item is None:
                continue
            text = str(item).strip()
            if not text:
                continue
            json_insights = self._extract_insights_from_json_blob(text)
            if json_insights:
                normalized.extend(json_insights)
                if len(normalized) >= max_items:
                    return normalized[:max_items]
                continue
            parts = self._split_freeform_items(text)
            for part in parts:
                cleaned = part.strip(" \t-•*\"'")
                if not cleaned:
                    continue
                if skip_pattern.search(cleaned):
                    continue
                normalized.append(cleaned)
                if len(normalized) >= max_items:
                    return normalized[:max_items]
        return normalized[:max_items]

    def _ensure_min_items(self, items: list[str], min_items: int) -> list[str]:
        if len(items) >= min_items or not items:
            return items
        expanded: list[str] = []
        for item in items:
            for sentence in re.split(r"(?<=[.!?])\s+", item):
                cleaned = sentence.strip()
                if not cleaned:
                    continue
                if cleaned not in expanded:
                    expanded.append(cleaned)
                if len(expanded) >= min_items:
                    return expanded
        return expanded if expanded else items

    def _ensure_mode_runs_every_time(
        self,
        modes: list[str],
        rounds: int,
        required_mode: str = "domain_hypothesis",
    ) -> tuple[list[str], int]:
        normalized_modes: list[str] = []
        for mode in modes or []:
            text = str(mode or "").strip()
            if text:
                normalized_modes.append(text)
        if not normalized_modes:
            normalized_modes = [required_mode]

        lowered = [mode.lower() for mode in normalized_modes]
        required_mode_lower = required_mode.lower()
        if required_mode_lower not in lowered:
            normalized_modes.append(required_mode)
            lowered.append(required_mode_lower)

        required_index = lowered.index(required_mode_lower)
        min_rounds = required_index + 1
        effective_rounds = max(1, rounds, min_rounds)
        return normalized_modes, effective_rounds

    def _summarize_visuals(self, visuals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        summaries: list[dict[str, Any]] = []
        for visual in visuals or []:
            if not isinstance(visual, dict):
                continue
            data = visual.get("data") or []
            point_count = len(data) if isinstance(data, list) else 0
            x_key = visual.get("xKey")
            y_key = visual.get("yKey")
            y_values: list[float] = []
            x_values: list[Any] = []
            if isinstance(data, list) and x_key and y_key:
                for row in data:
                    if not isinstance(row, dict):
                        continue
                    x_values.append(row.get(x_key))
                    raw_y = row.get(y_key)
                    if raw_y is None or isinstance(raw_y, bool):
                        continue
                    if isinstance(raw_y, (int, float)):
                        y_values.append(float(raw_y))
                        continue
                    try:
                        y_values.append(float(str(raw_y)))
                    except (TypeError, ValueError):
                        continue
            summary = {
                "id": visual.get("id"),
                "chart_type": visual.get("chartType"),
                "title": visual.get("title"),
                "x_key": x_key,
                "y_key": y_key,
                "point_count": point_count,
                "x_unique_count": len({v for v in x_values if v is not None}),
                "y_min": min(y_values) if y_values else None,
                "y_max": max(y_values) if y_values else None,
            }
            summaries.append(summary)
        return summaries

    def _extract_series_keys(self, visual: dict[str, Any]) -> list[str]:
        series = visual.get("series") or []
        highlight_keys = [
            str(item.get("key"))
            for item in series
            if isinstance(item, dict) and item.get("key") and item.get("highlight")
        ]
        if highlight_keys:
            return highlight_keys
        keys = [
            str(item.get("key"))
            for item in series
            if isinstance(item, dict) and item.get("key")
        ]
        if keys:
            return keys
        y_key = visual.get("yKey") or visual.get("y_key")
        return [str(y_key)] if y_key else []

    def _extract_numeric_series(
        self, data: list[dict[str, Any]], key: str
    ) -> list[float]:
        values: list[float] = []
        for row in data or []:
            if not isinstance(row, dict):
                continue
            raw = row.get(key)
            if raw is None or isinstance(raw, bool):
                continue
            if isinstance(raw, (int, float)):
                values.append(float(raw))
                continue
            try:
                values.append(float(str(raw)))
            except (TypeError, ValueError):
                continue
        return values

    def _compute_trend(self, values: list[float]) -> str:
        if len(values) < 2:
            return "flat"
        start = values[0]
        end = values[-1]
        if start == end:
            return "flat"
        delta = end - start
        if abs(delta) < max(abs(start), 1.0) * 0.02:
            return "flat"
        return "upward" if delta > 0 else "downward"

    def _fallback_visual_insight(self, visual: dict[str, Any], column_name: str) -> str:
        title = str(visual.get("title") or column_name or "Chart")
        chart_type = str(
            visual.get("chartType") or visual.get("chart_type") or "bar"
        ).lower()
        data = visual.get("data") or []
        x_key = visual.get("xKey") or visual.get("x_key")
        if chart_type == "heatmap":
            value_key = (
                visual.get("valueKey") or visual.get("value_key") or "correlation"
            )
            values = self._extract_numeric_series(data, str(value_key))
            if not values:
                return (
                    f"{title} summarizes pairwise relationships but has no valid "
                    "correlation values."
                )
            v_min = min(values)
            v_max = max(values)
            return (
                f"{title} shows pairwise correlations ranging from {round(v_min, 3)} "
                f"to {round(v_max, 3)}."
            )
        series_keys = self._extract_series_keys(visual)
        primary_key = (
            series_keys[0]
            if series_keys
            else (visual.get("yKey") or visual.get("y_key"))
        )

        if not isinstance(data, list) or not primary_key:
            return f"{title} summarizes {column_name} but lacks enough numeric points for a deeper read."

        values = self._extract_numeric_series(data, str(primary_key))
        if not values:
            return f"{title} summarizes {column_name} but lacks enough numeric points for a deeper read."

        v_min = min(values)
        v_max = max(values)
        point_count = len(values)
        if "correlation" in title.lower() or str(primary_key).lower() == "correlation":
            if x_key:
                max_idx = values.index(v_max)
                min_idx = values.index(v_min)
                max_label = data[max_idx].get(x_key) if max_idx < len(data) else None
                min_label = data[min_idx].get(x_key) if min_idx < len(data) else None
                if max_label is not None and min_label is not None:
                    return (
                        f"Correlations span {round(v_min, 3)} to {round(v_max, 3)}; "
                        f"strongest positive is {max_label}, strongest negative is {min_label}."
                    )
            return (
                f"Correlations span {round(v_min, 3)} to {round(v_max, 3)}, "
                "highlighting the most related numeric peers."
            )

        if chart_type in ("line", "area"):
            trend = self._compute_trend(values)
            return (
                f"{title} ranges from {round(v_min, 2)} to {round(v_max, 2)} "
                f"across {point_count} points, with an overall {trend} trend."
            )

        if chart_type == "bar":
            if x_key:
                max_idx = values.index(v_max)
                max_label = data[max_idx].get(x_key) if max_idx < len(data) else None
                if max_label is not None:
                    return (
                        f"{title} spans {round(v_min, 2)}-{round(v_max, 2)}; "
                        f"highest category is {max_label}."
                    )
            return (
                f"{title} spans {round(v_min, 2)}-{round(v_max, 2)} across categories."
            )

        return (
            f"{title} ranges from {round(v_min, 2)} to {round(v_max, 2)} "
            f"across {point_count} points."
        )

    def _build_panel_structure_insight(
        self,
        analysis: dict[str, Any],
        column_name: str,
    ) -> str | None:
        panel_payload = analysis.get("panel_visual_sampling")
        if not isinstance(panel_payload, dict):
            return None
        time_column = str(panel_payload.get("time_column") or "").strip()
        category_column = str(panel_payload.get("category_column") or "").strip()
        if not time_column or not category_column:
            return None
        bucket = str(panel_payload.get("bucket") or "year").strip()
        category_count = self._coerce_int(panel_payload.get("category_count"), 0)
        point_count = self._coerce_int(panel_payload.get("point_count"), 0)
        selected_categories_raw = panel_payload.get("selected_categories")
        selected_categories = (
            [str(item) for item in selected_categories_raw if item]
            if isinstance(selected_categories_raw, list)
            else []
        )
        top_preview = ", ".join(selected_categories[:3])
        if len(selected_categories) > 3:
            top_preview = f"{top_preview}, ..."

        summary = (
            f"Panel trend analysis uses {time_column} × {category_column} "
            f"({category_count or len(selected_categories)} groups) at {bucket} granularity, "
            f"providing a 3D view of {column_name} across time and entities."
        )
        if top_preview:
            summary += f" Example groups: {top_preview}."
        if point_count:
            summary += f" Includes {point_count} aggregated time points."
        return summary

    def _apply_visual_keep_heuristics(
        self, visuals: list[dict[str, Any]]
    ) -> dict[str, bool]:
        keep_map: dict[str, bool] = {}
        for visual in visuals or []:
            if not isinstance(visual, dict):
                continue
            visual_id = visual.get("id")
            if not visual_id:
                continue
            data = visual.get("data") or []
            point_count = len(data) if isinstance(data, list) else 0
            chart_type = visual.get("chartType")
            if chart_type == "line" and point_count < 3:
                keep_map[str(visual_id)] = False
                continue
            if chart_type == "bar" and point_count <= 1:
                keep_map[str(visual_id)] = False
                continue
        return keep_map

    async def _run_insights_passes(
        self,
        payload: dict[str, Any],
        instruction_base: str,
        rounds: int,
        modes: list[str],
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        response_format = {
            "type": "object",
            "properties": {
                "insights": {"type": "array", "items": {"type": "string"}},
                "caveats": {"type": "array", "items": {"type": "string"}},
                "visual_insights": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "visual_id": {"type": "string"},
                            "insight": {"type": "string"},
                            "keep": {"type": "boolean"},
                        },
                    },
                },
            },
        }
        for idx in range(max(1, rounds)):
            mode = modes[idx % len(modes)] if modes else "overview"
            mode_hint = ""
            if mode == "distribution_outliers":
                mode_hint = (
                    "Focus on distribution shape, tail behavior, outliers, and whether max values "
                    "are plausible or potentially anomalous."
                )
            elif mode == "correlation_drivers":
                mode_hint = (
                    "Focus on correlation structure, potential drivers, and whether correlations "
                    "may be confounded by grouping or seasonality."
                )
            elif mode == "business_implications":
                mode_hint = "Focus on business implications, operational risks, and where decisions could be impacted."
            elif mode == "temporal_patterns":
                mode_hint = "Focus on temporal patterns, seasonality hypotheses, and whether trends align with other variables."
            elif mode == "domain_hypothesis":
                mode_hint = (
                    "Infer the likely dataset/business domain from column metadata and existing AI summaries. "
                    "State hypotheses explicitly with confidence level, and extend to plausible business opportunities "
                    "or risks beyond directly observed metrics."
                )
            instruction = instruction_base
            if mode_hint:
                instruction = f"{instruction_base} {mode_hint}"
            result = await self._run_ai_complete_with_payload(
                payload, instruction, response_format
            )
            if isinstance(result, dict):
                results.append(result)
        return results

    @tool
    async def generate_column_summary(
        self, table_asset_id: int, column_name: str
    ) -> dict[str, Any]:
        """Generate a concise column summary using AI_COMPLETE."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        count_context = await self._build_ai_count_context(ctx, column_name, analysis)
        nulls_payload = self._normalize_nulls_for_ai(analysis, count_context)
        payload = {
            "column": column_name,
            "semantic_type": ctx.column_meta.semantic_type,
            "stats": analysis.get("stats"),
            "nulls": nulls_payload,
            "conflicts": analysis.get("conflicts"),
            "visuals": analysis.get("visuals", []),
            "repair_plan": analysis.get("repair_plan"),
            "structure_type": ctx.structure_type,
            "count_context": count_context,
        }

        instruction = (
            "Summarize the column in plain language. Return JSON with keys: "
            "summary (string, <=120 words), key_points (array, max 4), risks (array, max 3). "
            "When referencing dataset size, use count_context.authoritative_total_rows only. "
            "Do NOT describe nulls.sampled_rows as total records; label sampled metrics explicitly."
        )
        focus_hint = overrides.get("summary_focus")
        user_notes = overrides.get("summary_user_notes")
        if focus_hint:
            instruction += f" Focus on: {focus_hint}."
        if user_notes:
            instruction += f" User notes: {user_notes}."

        token_estimate = await self._estimate_ai_agg_tokens(payload, instruction)
        response_format = {
            "type": "object",
            "properties": {
                "summary": {"type": "string"},
                "key_points": {"type": "array", "items": {"type": "string"}},
                "risks": {"type": "array", "items": {"type": "string"}},
            },
        }
        summary = await self._run_ai_complete_with_payload(
            payload, instruction, response_format
        )

        normalized_summary = (
            summary.get("summary") if isinstance(summary, dict) else summary
        )
        normalized_points = (
            summary.get("key_points", []) if isinstance(summary, dict) else []
        )
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
    async def generate_numeric_insights(
        self, table_asset_id: int, column_name: str
    ) -> dict[str, Any]:
        """Generate AI insights for numeric/temporal columns based on visuals."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        count_context = await self._build_ai_count_context(ctx, column_name, analysis)
        nulls_payload = self._normalize_nulls_for_ai(analysis, count_context)
        visuals = analysis.get("visuals", []) or []
        visual_summaries = self._summarize_visuals(visuals)
        column_meta_payload = ctx.column_meta.metadata_payload or {}
        sql_type = str(column_meta_payload.get("sql_type") or "")
        metadata_context = {
            "column_name": column_name,
            "column_name_tokens": self._tokenize_identifier(column_name),
            "semantic_type": ctx.column_meta.semantic_type,
            "sql_type": sql_type,
            "confidence": ctx.column_meta.confidence,
            "structure_type": ctx.structure_type,
            "table_ref": ctx.table_ref,
            "table_name_hint": self._table_name_hint(ctx.table_ref),
            "provenance": ctx.column_meta.provenance or {},
        }
        agg_context = {
            "summary": analysis.get("summary"),
            "summary_key_points": analysis.get("summary_key_points"),
            "summary_risks": analysis.get("summary_risks"),
            "existing_insights": analysis.get("insights"),
        }
        payload = {
            "column": column_name,
            "column_metadata": metadata_context,
            "agg_context": agg_context,
            "count_context": count_context,
            "stats": analysis.get("stats"),
            "distribution": analysis.get("distribution"),
            "distribution_analysis": analysis.get("distribution_analysis"),
            "temporal_distribution": analysis.get("temporal_distribution"),
            "correlations": analysis.get("correlations"),
            "periodicity": analysis.get("periodicity"),
            "nulls": nulls_payload,
            "repair_plan": analysis.get("repair_plan"),
            "visual_summaries": visual_summaries,
            "panel_visual_sampling": analysis.get("panel_visual_sampling"),
            "year_value_anomalies": analysis.get("year_value_anomalies"),
            "structure_type": ctx.structure_type,
        }

        instruction = """
        You are a Senior Data Analyst. Your job is to generate high-density, data-grounded insights by reverse-engineering the business process and data-generating mechanism using ONLY the provided inputs: stats, distribution, distribution_analysis, temporal_distribution, correlations, periodicity, nulls, visuals, column_metadata, agg_context. Do NOT use external context or real-world news unless it is explicitly provided.

        GOAL: Produce deep insights that explain *why* patterns occur, infer the most likely dataset/business domain, and surface testable hypotheses—while staying strictly evidence-tethered.

        CORE REQUIREMENTS
        1) Mechanism-first insights (no restating):
        - Every insight must include: (a) an observed fact anchored in the provided metrics, (b) a plausible mechanism/logic chain explaining why, and (c) a business implication that extrapolates beyond the raw metric but remains plausible and evidence-tied.
        - Prioritize: heavy tails / skew, outliers, threshold effects, multi-modality (mixture distributions), drift vs seasonality, lagged effects, structural missingness (MNAR signals), zero-inflation, and segment-level heterogeneity.

        2) Business domain inference (mandatory, but probabilistic):
        - Use column_metadata + agg_context + distribution/periodicity signatures to infer the most likely domain (e.g., e-commerce orders/payments, subscription retention, ads performance, logistics fulfillment, financial transactions, IoT telemetry, customer support tickets).
        - Each inference must cite concrete anchors (e.g., weekly periodicity, month-end spikes, right-skewed monetary amounts, count data with many zeros, funnel-like correlations).
        - Use probabilistic language; do not present as certain.

        3) Separate Observed Facts vs Hypotheses (forced formatting):
        - The insights array MUST include both:
            - bullets starting with "Observed facts:" (strictly data-supported statements + brief mechanism)
            - bullets starting with "Hypothesis [High|Medium|Low]:" (testable, include supporting evidence and at least one alternative explanation/confounder).
        - Include at least ONE explicit hypothesis bullet with a confidence tag.

        4) Logic-mining priority (apply in order):
        a) Data-generating process: Which fields look like outcome vs drivers vs grouping dimensions vs timestamps vs IDs? Any evidence of deduping, sampling, or definition changes?
        b) Metric structure: Look for multiplicative identities (e.g., GMV=price*qty), funnel structures (impression→click→conversion), or SLA constructs (created→resolved duration).
        c) Correlations: For each notable correlation (direction + strength), provide one plausible mechanism AND one alternative explanation (confounding, segmentation effects, Simpson’s paradox risk).
        d) Temporal: Distinguish drift (trend) from seasonality (periodicity). If periodicity is strong, propose the likely source (workday effects, billing cycles, ops cadence, batch processing).

        5) Year anomaly rule (strict):
        - Only mention out-of-range year issues when year_value_anomalies.anomaly_count > 0.
        - If anomaly_count is 0 or missing, DO NOT claim year-range problems.
        - When epoch artifacts are present (e.g., 1970-01-01), do NOT interpret them as real business dates; treat only as parsing/quality artifacts.

        6) Visuals must be referenced and pruned:
        - Produce visual_insights objects with: visual_id, insight, keep.
        - If a visual adds little beyond stats or is misleading (e.g., outliers squash scale, poor binning, low N), set keep=false and explain why.
        - Keep at least ONE high-information visual and explain its incremental contribution to the mechanism inference.

        7) Output constraints (must comply):
        - Return ONLY JSON with keys: insights (array of Markdown bullet strings, max 4), caveats (array, max 3), visual_insights (array of objects with visual_id, insight, keep).
        - Each insight must be 1–2 sentences, high information density, and explain *why* (mechanism, outliers, correlations).
        - Do NOT include generic prompts like "let me know" or approval requests.
        - Do NOT mention real-world news unless external context is provided.
        - For row counts, use count_context.authoritative_total_rows as the dataset size.
        - Treat nulls.sampled_* as sample scan metrics only, never as full-table totals.

        OUTPUT FORMAT
        Return JSON:
        {
        "insights": ["- ...", "- ..."],
        "caveats": ["..."],
        "visual_insights": [{"visual_id":"...", "insight":"...", "keep":true}]
        }
        """
        if str(ctx.structure_type or "").lower() == "panel":
            instruction += (
                " This is panel data: explicitly compare entity/category trajectories "
                "over time (prefer year-level interpretation for long horizons) and "
                "highlight cross-entity heterogeneity."
            )
        focus_hint = overrides.get("insights_focus") or overrides.get("insight_focus")
        user_notes = overrides.get("insights_user_notes") or overrides.get(
            "insight_user_notes"
        )
        external_context = overrides.get("insights_external_context") or overrides.get(
            "insight_external_context"
        )
        use_case = overrides.get("insights_use_case")
        rounds = max(1, self._coerce_int(overrides.get("insights_rounds"), 1))
        max_items = max(1, self._coerce_int(overrides.get("insights_max_items"), 4))
        min_items = max(2, self._coerce_int(overrides.get("insights_min_items"), 2))
        if min_items > max_items:
            max_items = min_items
        modes_override = overrides.get("insights_modes")
        if isinstance(modes_override, str):
            modes = [mode.strip() for mode in modes_override.split(",") if mode.strip()]
        elif isinstance(modes_override, list):
            modes = [str(mode).strip() for mode in modes_override if str(mode).strip()]
        else:
            modes = [
                "distribution_outliers",
                "correlation_drivers",
                "business_implications",
                "temporal_patterns",
                "domain_hypothesis",
            ]
        modes, rounds = self._ensure_mode_runs_every_time(
            modes, rounds, required_mode="domain_hypothesis"
        )

        if focus_hint:
            instruction += f" Focus on: {focus_hint}."
        if user_notes:
            instruction += f" User notes: {user_notes}."
        if external_context:
            instruction += f" External context: {external_context}."
        if use_case:
            instruction += f" Use-case context: {use_case}."
        token_estimate = await self._estimate_ai_agg_tokens(payload, instruction)
        insight_results = await self._run_insights_passes(
            payload, instruction, rounds, modes
        )

        merged_insights: list[str] = []
        merged_caveats: list[str] = []
        visual_insights = []
        visual_keep_map: dict[str, bool] = {}
        visuals_by_id = {
            str(visual.get("id")): visual
            for visual in visuals
            if isinstance(visual, dict) and visual.get("id")
        }
        for insights in insight_results:
            normalized = self._normalize_insight_items(
                insights.get("insights"), max_items=max_items
            )
            merged_insights.extend(normalized)
            merged_caveats.extend(
                self._normalize_insight_items(insights.get("caveats") or [])
            )
            visual_items = insights.get("visual_insights") or []
            if isinstance(visual_items, dict):
                visual_items = [visual_items]
            if isinstance(visual_items, list):
                for item in visual_items:
                    if not isinstance(item, dict):
                        continue
                    visual_id = (
                        item.get("visual_id")
                        or item.get("visualId")
                        or item.get("id")
                        or item.get("chart_id")
                    )
                    if not visual_id:
                        continue
                    insight_text = str(item.get("insight") or "").strip()
                    keep_flag = item.get("keep")
                    if keep_flag is not None:
                        if keep_flag:
                            visual_keep_map[str(visual_id)] = True
                        else:
                            visual_keep_map.setdefault(str(visual_id), False)
                    if insight_text:
                        visual_insights.append(
                            {"visual_id": str(visual_id), "insight": insight_text}
                        )

        normalized_insights = self._merge_unique_items(
            merged_insights, max_items=max_items
        )
        normalized_insights = self._ensure_min_items(
            normalized_insights, min_items=min_items
        )
        normalized_insights = self._ensure_markdown_bullets(normalized_insights)
        normalized_caveats = self._merge_unique_items(merged_caveats, max_items=3)
        normalized_caveats = self._ensure_markdown_bullets(normalized_caveats)

        def build_numeric_fallback() -> list[str]:
            fallback: list[str] = []
            distribution = analysis.get("distribution") or {}
            nulls = analysis.get("nulls") or {}
            correlations = analysis.get("correlations") or {}
            periodicity = analysis.get("periodicity") or {}
            year_anomalies = analysis.get("year_value_anomalies") or {}

            mean_val = self._coerce_float(distribution.get("mean_value"))
            median_val = self._coerce_float(distribution.get("p50"))
            shape = distribution.get("shape")
            if shape and shape != "unknown":
                fallback.append(
                    f"Distribution appears {str(shape).replace('_', ' ')}, "
                    "suggesting the data may be unevenly concentrated."
                )
            elif (
                mean_val is not None
                and median_val is not None
                and mean_val != median_val
            ):
                if mean_val > median_val:
                    fallback.append(
                        "Mean exceeds median, indicating a right-skewed tail and possible outliers."
                    )
                else:
                    fallback.append(
                        "Mean below median suggests a left-skewed distribution."
                    )

            null_rate = self._coerce_float(nulls.get("null_rate"))
            if null_rate is None:
                null_count = self._coerce_int(distribution.get("null_count"))
                total_count = self._coerce_int(distribution.get("total_count"))
                if total_count:
                    null_rate = round(null_count / total_count, 4)
            if null_rate:
                fallback.append(
                    f"Null rate is about {round(null_rate * 100, 2)}%, "
                    "which may affect downstream averages and trends."
                )

            corr_items = correlations.get("all") or []
            if corr_items:
                ranked = [
                    item
                    for item in corr_items
                    if isinstance(item, dict) and item.get("correlation") is not None
                ]
                ranked.sort(
                    key=lambda item: abs(float(item.get("correlation") or 0)),
                    reverse=True,
                )
                if ranked:
                    top = ranked[0]
                    corr_val = float(top.get("correlation") or 0)
                    direction = "positive" if corr_val >= 0 else "negative"
                    fallback.append(
                        f"Top {direction} correlation is with {top.get('column')}, "
                        f"r={round(corr_val, 3)}."
                    )

            if periodicity.get("detected"):
                lag = periodicity.get("dominant_period")
                strength = periodicity.get("strength")
                if lag is not None and strength is not None:
                    fallback.append(
                        f"Detected periodicity at lag {lag} with strength {round(float(strength), 3)}."
                    )

            anomaly_count = self._coerce_int(year_anomalies.get("anomaly_count"))
            if anomaly_count > 0:
                valid_range = year_anomalies.get("valid_range") or {}
                range_min = valid_range.get("min")
                range_max = valid_range.get("max")
                fallback.append(
                    f"Detected {anomaly_count} out-of-range year values"
                    f"{f' (expected {range_min}-{range_max})' if range_min and range_max else ''}, "
                    "so temporal interpretation should exclude those anomalies."
                )

            return fallback

        if len(normalized_insights) < min_items:
            fallback = build_numeric_fallback()
            if fallback:
                normalized_insights = self._merge_unique_items(
                    normalized_insights + fallback, max_items=max_items
                )
                normalized_insights = self._ensure_min_items(
                    normalized_insights, min_items=min_items
                )
                normalized_insights = self._ensure_markdown_bullets(normalized_insights)

        if str(ctx.structure_type or "").lower() == "panel":
            panel_insight = self._build_panel_structure_insight(analysis, column_name)
            if panel_insight:
                normalized_insights = self._merge_unique_items(
                    normalized_insights + [panel_insight], max_items=max_items
                )
                normalized_insights = self._ensure_markdown_bullets(normalized_insights)

        # Ensure every visual has a keep decision and an insight.
        missing_visuals = []
        existing_visual_ids = {item.get("visual_id") for item in visual_insights}
        for summary in visual_summaries:
            visual_id = summary.get("id")
            if not visual_id:
                continue
            if visual_id not in visual_keep_map:
                visual_keep_map[str(visual_id)] = True
            if visual_id not in existing_visual_ids:
                missing_visuals.append(summary)

        if missing_visuals:
            visuals_instruction = (
                "Provide one insight per visual summary and decide keep/remove. "
                "Return JSON: visual_insights (array of {visual_id, insight, keep}). "
                "If point_count < 3 for line charts or x_unique_count <= 1 for bar charts, set keep=false."
            )
            visuals_payload = {
                "visual_summaries": missing_visuals,
                "column": column_name,
            }
            visuals_response_format = {
                "type": "object",
                "properties": {
                    "visual_insights": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "visual_id": {"type": "string"},
                                "insight": {"type": "string"},
                                "keep": {"type": "boolean"},
                            },
                        },
                    }
                },
            }
            visuals_result = await self._run_ai_complete_with_payload(
                visuals_payload, visuals_instruction, visuals_response_format
            )
            visual_items = (
                visuals_result.get("visual_insights")
                if isinstance(visuals_result, dict)
                else []
            )
            if isinstance(visual_items, dict):
                visual_items = [visual_items]
            if isinstance(visual_items, list):
                for item in visual_items:
                    if not isinstance(item, dict):
                        continue
                    visual_id = item.get("visual_id") or item.get("id")
                    if not visual_id:
                        continue
                    insight_text = str(item.get("insight") or "").strip()
                    keep_flag = item.get("keep")
                    if keep_flag is not None:
                        if keep_flag:
                            visual_keep_map[str(visual_id)] = True
                        else:
                            visual_keep_map.setdefault(str(visual_id), False)
                    if insight_text:
                        visual_insights.append(
                            {"visual_id": str(visual_id), "insight": insight_text}
                        )

        existing_visual_ids = {item.get("visual_id") for item in visual_insights}
        for visual_id, visual in visuals_by_id.items():
            if visual_id in existing_visual_ids:
                continue
            fallback = self._fallback_visual_insight(visual, column_name)
            if fallback:
                visual_insights.append({"visual_id": visual_id, "insight": fallback})

        # Apply heuristics to remove low-signal visuals.
        heuristic_keep = self._apply_visual_keep_heuristics(visuals)
        if heuristic_keep:
            visual_keep_map.update(heuristic_keep)

        analysis_update = {
            "insights": normalized_insights,
            "caveats": normalized_caveats,
            "insight_token_estimate": token_estimate,
            "visual_insights": visual_insights,
            "visual_keep": visual_keep_map,
        }

        await self._update_column_analysis(ctx, analysis_update)
        return {
            "column": column_name,
            "insights": normalized_insights,
            "caveats": normalized_caveats,
            "token_estimate": token_estimate,
            "visual_insights": visual_insights,
            "visual_keep": visual_keep_map,
        }

    @tool
    async def generate_categorical_insights(
        self, table_asset_id: int, column_name: str
    ) -> dict[str, Any]:
        """Generate AI insights for categorical columns based on visuals."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        count_context = await self._build_ai_count_context(ctx, column_name, analysis)
        nulls_payload = self._normalize_nulls_for_ai(analysis, count_context)
        visuals = analysis.get("visuals", []) or []
        visual_summaries = self._summarize_visuals(visuals)
        column_meta_payload = ctx.column_meta.metadata_payload or {}
        sql_type = str(column_meta_payload.get("sql_type") or "")
        metadata_context = {
            "column_name": column_name,
            "column_name_tokens": self._tokenize_identifier(column_name),
            "semantic_type": ctx.column_meta.semantic_type,
            "sql_type": sql_type,
            "confidence": ctx.column_meta.confidence,
            "structure_type": ctx.structure_type,
            "table_ref": ctx.table_ref,
            "table_name_hint": self._table_name_hint(ctx.table_ref),
            "provenance": ctx.column_meta.provenance or {},
        }
        agg_context = {
            "summary": analysis.get("summary"),
            "summary_key_points": analysis.get("summary_key_points"),
            "summary_risks": analysis.get("summary_risks"),
            "existing_insights": analysis.get("insights"),
        }
        payload = {
            "column": column_name,
            "column_metadata": metadata_context,
            "agg_context": agg_context,
            "count_context": count_context,
            "stats": analysis.get("stats"),
            "distribution": analysis.get("distribution"),
            "correlations": analysis.get("correlations"),
            "periodicity": analysis.get("periodicity"),
            "nulls": nulls_payload,
            "repair_plan": analysis.get("repair_plan"),
            "visual_summaries": visual_summaries,
        }

        instruction = """
        You are a Senior Data Analyst. Generate high-density, data-grounded insights using ONLY the provided inputs: stats, distribution, nulls, visuals, column_metadata, and agg_context. Do NOT use external context or real-world news unless it is explicitly provided.

        OBJECTIVE
        Produce insights that go beyond descriptive summaries by explaining plausible data-generating mechanisms, inferring the most likely dataset/business domain, and surfacing testable hypotheses and business implications—while staying tethered to the evidence available.

        REQUIREMENTS
        1) Evidence-first, mechanism-driven insights (no restating):
        - Each insight must include (a) an observed pattern anchored in stats/distribution/nulls/visuals, and (b) a brief “why” explanation using distribution logic (e.g., long tail, skew, dominance, mixture/multi-modality, zero-inflation, truncation/censoring, outliers).
        - Avoid generic narration; prioritize the *structural cause* of the pattern (e.g., a few entities dominate due to Pareto dynamics, process bottlenecks create heavy tails, filtering rules create truncation).

        2) Infer the likely business domain (mandatory, probabilistic):
        - Use column_metadata + agg_context + distribution signatures to infer what the dataset most likely represents (e.g., e-commerce orders/payments, subscription usage, ads metrics, logistics, support tickets, transactions, telemetry).
        - Every domain inference must cite at least one anchor from the provided inputs (e.g., “right-skewed monetary-like field”, “count field with many zeros”, “entity-level dominance in a Pareto-like plot”, “missingness concentrated in optional fields”).
        - Use probabilistic language (e.g., “most consistent with…”, “likely…”). Do not present as certain.

        3) Hypotheses must be explicit and testable:
        - Clearly label speculative statements as hypotheses using the exact prefix: "Hypothesis [High|Medium|Low]:".
        - Include at least ONE hypothesis bullet with a confidence tag, and briefly state the supporting evidence AND one plausible alternative explanation (confounder/measurement artifact/segment mix).

        4) Missingness has meaning:
        - Use nulls to infer process behavior (e.g., optional capture fields, system integration gaps, conditional workflows).
        - Distinguish likely “random” vs “structural” missingness when the null patterns suggest it, without overclaiming.

        5) Visuals must be evaluated for incremental value:
        - Produce visual_insights with fields: visual_id, insight, keep.
        - If a visual is redundant with stats or misleading (e.g., outliers compress scale, bins obscure multi-modality, too sparse), set keep=false and explain why.
        - Keep at least ONE visual that adds incremental understanding and explain what it reveals beyond the raw stats.

        6) Business implication beyond descriptive stats (mandatory):
        - Include at least ONE business implication that extrapolates beyond the metric (e.g., concentration risk, targeting opportunities, operational bottlenecks, pricing/packaging implications), but remains plausible and evidence-tied.

        OUTPUT CONSTRAINTS
        - Return ONLY JSON with keys:
        - insights: array of Markdown bullet strings (max 4)
        - caveats: array (max 3)
        - visual_insights: array of objects {visual_id, insight, keep}
        - Each insight must be 1–2 sentences and explicitly explain *why* (e.g., long tail, dominance, skew, outliers).
        - Do NOT include generic prompts like "let me know" or approval requests.
        - If you recommend removing a visual, set keep=false.
        - For row counts, use count_context.authoritative_total_rows as the dataset size.
        - Treat nulls.sampled_* as sample scan metrics only, never as full-table totals.

        OUTPUT FORMAT
        {
        "insights": ["- ...", "- ..."],
        "caveats": ["..."],
        "visual_insights": [{"visual_id":"...", "insight":"...", "keep":true}]
        }
        """
        focus_hint = overrides.get("insights_focus") or overrides.get("insight_focus")
        user_notes = overrides.get("insights_user_notes") or overrides.get(
            "insight_user_notes"
        )
        external_context = overrides.get("insights_external_context") or overrides.get(
            "insight_external_context"
        )
        use_case = overrides.get("insights_use_case")
        rounds = max(1, self._coerce_int(overrides.get("insights_rounds"), 1))
        max_items = max(1, self._coerce_int(overrides.get("insights_max_items"), 4))
        min_items = max(2, self._coerce_int(overrides.get("insights_min_items"), 2))
        if min_items > max_items:
            max_items = min_items
        modes_override = overrides.get("insights_modes")
        if isinstance(modes_override, str):
            modes = [mode.strip() for mode in modes_override.split(",") if mode.strip()]
        elif isinstance(modes_override, list):
            modes = [str(mode).strip() for mode in modes_override if str(mode).strip()]
        else:
            modes = [
                "distribution_outliers",
                "business_implications",
                "domain_hypothesis",
            ]
        modes, rounds = self._ensure_mode_runs_every_time(
            modes, rounds, required_mode="domain_hypothesis"
        )

        if focus_hint:
            instruction += f" Focus on: {focus_hint}."
        if user_notes:
            instruction += f" User notes: {user_notes}."
        if external_context:
            instruction += f" External context: {external_context}."
        if use_case:
            instruction += f" Use-case context: {use_case}."
        token_estimate = await self._estimate_ai_agg_tokens(payload, instruction)
        insight_results = await self._run_insights_passes(
            payload, instruction, rounds, modes
        )

        merged_insights: list[str] = []
        merged_caveats: list[str] = []
        visual_insights = []
        visual_keep_map: dict[str, bool] = {}
        visuals_by_id = {
            str(visual.get("id")): visual
            for visual in visuals
            if isinstance(visual, dict) and visual.get("id")
        }
        for insights in insight_results:
            merged_insights.extend(
                self._normalize_insight_items(
                    insights.get("insights"), max_items=max_items
                )
            )
            merged_caveats.extend(
                self._normalize_insight_items(insights.get("caveats") or [])
            )
            visual_items = insights.get("visual_insights") or []
            if isinstance(visual_items, dict):
                visual_items = [visual_items]
            if isinstance(visual_items, list):
                for item in visual_items:
                    if not isinstance(item, dict):
                        continue
                    visual_id = (
                        item.get("visual_id")
                        or item.get("visualId")
                        or item.get("id")
                        or item.get("chart_id")
                    )
                    if not visual_id:
                        continue
                    insight_text = str(item.get("insight") or "").strip()
                    keep_flag = item.get("keep")
                    if keep_flag is not None:
                        if keep_flag:
                            visual_keep_map[str(visual_id)] = True
                        else:
                            visual_keep_map.setdefault(str(visual_id), False)
                    if insight_text:
                        visual_insights.append(
                            {"visual_id": str(visual_id), "insight": insight_text}
                        )

        normalized_insights = self._merge_unique_items(
            merged_insights, max_items=max_items
        )
        normalized_insights = self._ensure_min_items(
            normalized_insights, min_items=min_items
        )
        normalized_insights = self._ensure_markdown_bullets(normalized_insights)
        normalized_caveats = self._merge_unique_items(merged_caveats, max_items=3)
        normalized_caveats = self._ensure_markdown_bullets(normalized_caveats)

        def build_categorical_fallback() -> list[str]:
            fallback: list[str] = []
            groups = analysis.get("category_groups") or {}
            top_categories = groups.get("top_categories") or []
            coverage = self._coerce_float(groups.get("coverage"))
            distinct_count = self._coerce_int(groups.get("distinct_count"))
            tail_count = self._coerce_int(groups.get("tail_count"))

            if coverage is not None:
                fallback.append(
                    f"Top categories cover about {round(coverage * 100, 1)}% of non-null rows, "
                    "indicating how concentrated the head is."
                )
            if distinct_count:
                fallback.append(
                    f"The column has around {distinct_count} distinct values, "
                    "suggesting its categorical granularity."
                )
            if tail_count and top_categories:
                top_name = top_categories[0].get("category")
                fallback.append(
                    f"The long tail accounts for roughly {tail_count} rows; "
                    f"top value '{top_name}' dominates the head."
                )
            return fallback

        if len(normalized_insights) < min_items:
            fallback = build_categorical_fallback()
            if fallback:
                normalized_insights = self._merge_unique_items(
                    normalized_insights + fallback, max_items=max_items
                )
                normalized_insights = self._ensure_min_items(
                    normalized_insights, min_items=min_items
                )
                normalized_insights = self._ensure_markdown_bullets(normalized_insights)

        missing_visuals = []
        existing_visual_ids = {item.get("visual_id") for item in visual_insights}
        for summary in visual_summaries:
            visual_id = summary.get("id")
            if not visual_id:
                continue
            if visual_id not in visual_keep_map:
                visual_keep_map[str(visual_id)] = True
            if visual_id not in existing_visual_ids:
                missing_visuals.append(summary)

        if missing_visuals:
            visuals_instruction = (
                "Provide one insight per visual summary and decide keep/remove. "
                "Return JSON: visual_insights (array of {visual_id, insight, keep}). "
                "If point_count < 3 for line charts or x_unique_count <= 1 for bar charts, set keep=false."
            )
            visuals_payload = {
                "visual_summaries": missing_visuals,
                "column": column_name,
            }
            visuals_response_format = {
                "type": "object",
                "properties": {
                    "visual_insights": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "visual_id": {"type": "string"},
                                "insight": {"type": "string"},
                                "keep": {"type": "boolean"},
                            },
                        },
                    }
                },
            }
            visuals_result = await self._run_ai_complete_with_payload(
                visuals_payload, visuals_instruction, visuals_response_format
            )
            visual_items = (
                visuals_result.get("visual_insights")
                if isinstance(visuals_result, dict)
                else []
            )
            if isinstance(visual_items, dict):
                visual_items = [visual_items]
            if isinstance(visual_items, list):
                for item in visual_items:
                    if not isinstance(item, dict):
                        continue
                    visual_id = item.get("visual_id") or item.get("id")
                    if not visual_id:
                        continue
                    insight_text = str(item.get("insight") or "").strip()
                    keep_flag = item.get("keep")
                    if keep_flag is not None:
                        if keep_flag:
                            visual_keep_map[str(visual_id)] = True
                        else:
                            visual_keep_map.setdefault(str(visual_id), False)
                    if insight_text:
                        visual_insights.append(
                            {"visual_id": str(visual_id), "insight": insight_text}
                        )

        existing_visual_ids = {item.get("visual_id") for item in visual_insights}
        for visual_id, visual in visuals_by_id.items():
            if visual_id in existing_visual_ids:
                continue
            fallback = self._fallback_visual_insight(visual, column_name)
            if fallback:
                visual_insights.append({"visual_id": visual_id, "insight": fallback})

        heuristic_keep = self._apply_visual_keep_heuristics(visuals)
        if heuristic_keep:
            visual_keep_map.update(heuristic_keep)

        analysis_update = {
            "insights": normalized_insights,
            "caveats": normalized_caveats,
            "insight_token_estimate": token_estimate,
            "visual_insights": visual_insights,
            "visual_keep": visual_keep_map,
        }

        await self._update_column_analysis(ctx, analysis_update)
        return {
            "column": column_name,
            "insights": normalized_insights,
            "caveats": normalized_caveats,
            "token_estimate": token_estimate,
            "visual_insights": visual_insights,
            "visual_keep": visual_keep_map,
        }
