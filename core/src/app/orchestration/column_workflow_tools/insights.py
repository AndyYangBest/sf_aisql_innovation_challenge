"""Column workflow tool mixin."""

from __future__ import annotations

from typing import Any, Iterable
import re
import json
import ast
from strands import tool

class ColumnWorkflowInsightsMixin:
    """Tool mixin."""

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
                        str(item).strip()
                        for item in parsed
                        if str(item).strip()
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
        normalized = [part.strip(" \t-•*\"'") for part in parts if part and part.strip()]
        if normalized:
            return normalized
        return [str(text).strip()] if str(text).strip() else []

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
        chart_type = str(visual.get("chartType") or visual.get("chart_type") or "bar").lower()
        data = visual.get("data") or []
        x_key = visual.get("xKey") or visual.get("x_key")
        if chart_type == "heatmap":
            value_key = (
                visual.get("valueKey")
                or visual.get("value_key")
                or "correlation"
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
        primary_key = series_keys[0] if series_keys else (visual.get("yKey") or visual.get("y_key"))

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

    def _apply_visual_keep_heuristics(self, visuals: list[dict[str, Any]]) -> dict[str, bool]:
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
                mode_hint = (
                    "Focus on business implications, operational risks, and where decisions could be impacted."
                )
            elif mode == "temporal_patterns":
                mode_hint = (
                    "Focus on temporal patterns, seasonality hypotheses, and whether trends align with other variables."
                )
            instruction = instruction_base
            if mode_hint:
                instruction = f"{instruction_base} {mode_hint}"
            result = await self._run_ai_agg(payload, instruction)
            if isinstance(result, dict):
                results.append(result)
        return results

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
        visuals = analysis.get("visuals", []) or []
        visual_summaries = self._summarize_visuals(visuals)
        payload = {
            "column": column_name,
            "stats": analysis.get("stats"),
            "distribution": analysis.get("distribution"),
            "distribution_analysis": analysis.get("distribution_analysis"),
            "temporal_distribution": analysis.get("temporal_distribution"),
            "correlations": analysis.get("correlations"),
            "periodicity": analysis.get("periodicity"),
            "nulls": analysis.get("nulls"),
            "repair_plan": analysis.get("repair_plan"),
            "visual_summaries": visual_summaries,
            "structure_type": ctx.structure_type,
        }

        instruction = (
            "You are a senior data analyst. Provide deep, data-grounded insights using the stats, "
            "distribution, distribution_analysis, temporal_distribution, correlations, periodicity, nulls, and visuals. "
            "Do NOT include generic prompts like 'let me know' or approval requests. "
            "If external context is provided, you may reference it; otherwise do not mention real-world news. "
            "Return JSON with keys: insights (array of Markdown bullet strings, max 4), "
            "caveats (array, max 3), visual_insights (array of objects with visual_id, insight, keep). "
            "Each insight should be 1–2 sentences and explain *why* (e.g., skew causes, outliers, correlations). "
            "If you recommend removing a visual, set keep=false."
        )
        focus_hint = overrides.get("insights_focus") or overrides.get("insight_focus")
        user_notes = overrides.get("insights_user_notes") or overrides.get("insight_user_notes")
        external_context = overrides.get("insights_external_context") or overrides.get("insight_external_context")
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
            modes = ["distribution_outliers", "correlation_drivers", "business_implications", "temporal_patterns"]
        if focus_hint:
            instruction += f" Focus on: {focus_hint}."
        if user_notes:
            instruction += f" User notes: {user_notes}."
        if external_context:
            instruction += f" External context: {external_context}."
        if use_case:
            instruction += f" Use-case context: {use_case}."
        token_estimate = await self._estimate_ai_agg_tokens(payload, instruction)
        insight_results = await self._run_insights_passes(payload, instruction, rounds, modes)

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
            merged_caveats.extend(self._normalize_insight_items(insights.get("caveats") or []))
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
                        visual_insights.append({"visual_id": str(visual_id), "insight": insight_text})

        normalized_insights = self._merge_unique_items(merged_insights, max_items=max_items)
        normalized_insights = self._ensure_min_items(normalized_insights, min_items=min_items)
        normalized_insights = self._ensure_markdown_bullets(normalized_insights)
        normalized_caveats = self._merge_unique_items(merged_caveats, max_items=3)
        normalized_caveats = self._ensure_markdown_bullets(normalized_caveats)

        def build_numeric_fallback() -> list[str]:
            fallback: list[str] = []
            distribution = analysis.get("distribution") or {}
            nulls = analysis.get("nulls") or {}
            correlations = analysis.get("correlations") or {}
            periodicity = analysis.get("periodicity") or {}

            mean_val = self._coerce_float(distribution.get("mean_value"))
            median_val = self._coerce_float(distribution.get("p50"))
            shape = distribution.get("shape")
            if shape and shape != "unknown":
                fallback.append(
                    f"Distribution appears {str(shape).replace('_', ' ')}, "
                    "suggesting the data may be unevenly concentrated."
                )
            elif mean_val is not None and median_val is not None and mean_val != median_val:
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
                    item for item in corr_items
                    if isinstance(item, dict) and item.get("correlation") is not None
                ]
                ranked.sort(key=lambda item: abs(float(item.get("correlation") or 0)), reverse=True)
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

            return fallback

        if len(normalized_insights) < min_items:
            fallback = build_numeric_fallback()
            if fallback:
                normalized_insights = self._merge_unique_items(
                    normalized_insights + fallback, max_items=max_items
                )
                normalized_insights = self._ensure_min_items(normalized_insights, min_items=min_items)
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
            visuals_payload = {"visual_summaries": missing_visuals, "column": column_name}
            visuals_result = await self._run_ai_agg(visuals_payload, visuals_instruction)
            visual_items = visuals_result.get("visual_insights") if isinstance(visuals_result, dict) else []
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
                        visual_insights.append({"visual_id": str(visual_id), "insight": insight_text})

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
    async def generate_categorical_insights(self, table_asset_id: int, column_name: str) -> dict[str, Any]:
        """Generate AI insights for categorical columns based on visuals."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        visuals = analysis.get("visuals", []) or []
        visual_summaries = self._summarize_visuals(visuals)
        payload = {
            "column": column_name,
            "stats": analysis.get("stats"),
            "distribution": analysis.get("distribution"),
            "correlations": analysis.get("correlations"),
            "periodicity": analysis.get("periodicity"),
            "nulls": analysis.get("nulls"),
            "repair_plan": analysis.get("repair_plan"),
            "visual_summaries": visual_summaries,
        }

        instruction = (
            "You are a senior data analyst. Provide deep, data-grounded insights using the stats, "
            "distribution, nulls, and visuals. "
            "Do NOT include generic prompts like 'let me know' or approval requests. "
            "If external context is provided, you may reference it; otherwise do not mention real-world news. "
            "Return JSON with keys: insights (array of Markdown bullet strings, max 4), "
            "caveats (array, max 3), visual_insights (array of objects with visual_id, insight, keep). "
            "Each insight should be 1–2 sentences and explain *why* (e.g., long tail, dominance, skew). "
            "If you recommend removing a visual, set keep=false."
        )
        focus_hint = overrides.get("insights_focus") or overrides.get("insight_focus")
        user_notes = overrides.get("insights_user_notes") or overrides.get("insight_user_notes")
        external_context = overrides.get("insights_external_context") or overrides.get("insight_external_context")
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
            modes = ["distribution_outliers", "business_implications"]
        if focus_hint:
            instruction += f" Focus on: {focus_hint}."
        if user_notes:
            instruction += f" User notes: {user_notes}."
        if external_context:
            instruction += f" External context: {external_context}."
        if use_case:
            instruction += f" Use-case context: {use_case}."
        token_estimate = await self._estimate_ai_agg_tokens(payload, instruction)
        insight_results = await self._run_insights_passes(payload, instruction, rounds, modes)

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
            merged_caveats.extend(self._normalize_insight_items(insights.get("caveats") or []))
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
                        visual_insights.append({"visual_id": str(visual_id), "insight": insight_text})

        normalized_insights = self._merge_unique_items(merged_insights, max_items=max_items)
        normalized_insights = self._ensure_min_items(normalized_insights, min_items=min_items)
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
                normalized_insights = self._ensure_min_items(normalized_insights, min_items=min_items)
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
            visuals_payload = {"visual_summaries": missing_visuals, "column": column_name}
            visuals_result = await self._run_ai_agg(visuals_payload, visuals_instruction)
            visual_items = visuals_result.get("visual_insights") if isinstance(visuals_result, dict) else []
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
                        visual_insights.append({"visual_id": str(visual_id), "insight": insight_text})

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
