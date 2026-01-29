"""Column workflow tool mixin."""

from __future__ import annotations

from datetime import datetime
import json
import uuid
from typing import Any
from strands import tool

class ColumnWorkflowQualityMixin:
    """Tool mixin."""

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
        base_query = self._build_windowed_query(ctx, sample_size, window_days, target_time_column)
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
        window_days: int | None = None,
        time_column: str | None = None,
    ) -> dict[str, Any]:
        """Detect conflicting values within groups defined by other columns."""
        ctx = await self._load_context(table_asset_id, column_name)
        overrides = ctx.column_meta.overrides or {}
        group_by = group_by_columns or overrides.get("conflict_group_columns") or []
        override_sample_size = overrides.get("scan_conflicts_sample_size")
        if override_sample_size is not None:
            sample_size = self._coerce_int(override_sample_size) or sample_size
        override_window_days = overrides.get("scan_conflicts_window_days")
        if override_window_days is not None:
            window_days = self._coerce_int(override_window_days)
        override_time_column = overrides.get("scan_conflicts_time_column")
        if override_time_column:
            time_column = str(override_time_column)
        if isinstance(group_by, str):
            group_by = [item.strip() for item in group_by.split(",") if item.strip()]
        group_by = [str(item) for item in group_by if item]
        if not group_by:
            conflicts = {"skipped": True, "reason": "group_by_columns_missing"}
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
        base_query = self._build_windowed_query(ctx, sample_size, window_days, target_time_column)
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
        group_count = self._coerce_int(row.get("GROUP_COUNT"))
        conflict_groups = self._coerce_int(row.get("CONFLICT_GROUPS"))
        conflict_rate = round(conflict_groups / group_count, 6) if group_count else None
        conflicts = {
            "group_by_columns": group_by,
            "group_count": group_count,
            "conflict_groups": conflict_groups,
            "conflict_rate": conflict_rate,
            "max_distinct": self._coerce_int(row.get("MAX_DISTINCT")),
        }
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

        group_by = conflicts.get("group_by_columns") or overrides.get("conflict_group_columns") or []
        if isinstance(group_by, str):
            group_by = [item.strip() for item in group_by.split(",") if item.strip()]
        group_by = [str(item) for item in group_by if item]

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
        if sample_snapshot_id and sample_snapshot_id != snapshot.get("snapshot_id"):
            inconsistency_reasons.append("snapshot_mismatch_nulls")
        if distribution_snapshot_id and distribution_snapshot_id != snapshot.get("snapshot_id"):
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
                update_sql = f"UPDATE {ctx.table_ref} SET {self._quote_ident(column_name)} = {fill_expr} WHERE {self._quote_ident(column_name)} IS NULL"
                count_sql = f"SELECT COUNT(*) AS affected_rows FROM {ctx.table_ref} WHERE {self._quote_ident(column_name)} IS NULL"
            plan_steps.append(
                {
                    "type": "null_repair",
                    "strategy": strategy,
                    "estimated_rows": null_count,
                    "fill_expr": fill_expr,
                    "fill_value": fill_value,
                    "requires_manual_default": fill_value is None,
                    "reason": null_reason,
                    "basis": basis,
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
        plan = analysis.get("repair_plan", {})
        if isinstance(plan, str):
            try:
                plan = json.loads(plan)
            except json.JSONDecodeError:
                plan = {}
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
        plan = analysis.get("repair_plan", {})
        if isinstance(plan, str):
            try:
                plan = json.loads(plan)
            except json.JSONDecodeError:
                plan = {}
        if isinstance(plan, str):
            try:
                plan = json.loads(plan)
            except json.JSONDecodeError:
                plan = {}
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
        approved = bool(overrides.get(approval_key) or (isinstance(plan, dict) and plan.get("approved")))
        if not approved:
            return {"column": column_name, "skipped": True, "reason": "approval_required"}
        if not ctx.table_ref:
            return {"column": column_name, "skipped": True, "reason": "table_ref_missing"}

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
            plan = refreshed.get("repair_plan") if isinstance(refreshed, dict) else None
            if not plan or not plan.get("plan_id") or not plan.get("plan_hash"):
                return await record_skip("plan_missing")
        if not plan.get("plan_id") or not plan.get("plan_hash"):
            return await record_skip("plan_missing")

        group_by = (plan.get("snapshot") or {}).get("group_by_columns") or conflicts.get(
            "group_by_columns"
        ) or overrides.get("conflict_group_columns") or []
        if isinstance(group_by, str):
            group_by = [item.strip() for item in group_by.split(",") if item.strip()]
        group_by = [str(item) for item in group_by if item]

        if isinstance(plan, dict) and not (plan.get("snapshot") or {}).get("signature"):
            snapshot = await self._compute_snapshot(ctx, column_name, group_by)
            plan = dict(plan)
            plan["snapshot"] = snapshot
            await self._update_column_analysis(ctx, {"repair_plan": plan})

        group_by = (plan.get("snapshot") or {}).get("group_by_columns") or conflicts.get(
            "group_by_columns"
        ) or overrides.get("conflict_group_columns") or []
        if isinstance(group_by, str):
            group_by = [item.strip() for item in group_by.split(",") if item.strip()]
        group_by = [str(item) for item in group_by if item]

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
        audit_table = (plan.get("rollback") or {}).get("audit_table") or overrides.get(
            "repair_audit_table"
        )
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
                    audit_insert = f"""
                    INSERT INTO {audit_table} (plan_id, column_name, repair_type, row_id, before_value, after_value, created_at)
                    SELECT '{self._sanitize_literal(plan.get("plan_id", ""))}', '{self._sanitize_literal(column_name)}', 'null_repair',
                           TO_VARCHAR({self._quote_ident(row_id_column)}),
                           TO_VARIANT({col}),
                           TO_VARIANT({fill_expr}),
                           CURRENT_TIMESTAMP()
                    FROM {ctx.table_ref}
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
        plan = analysis.get("repair_plan", {})
        if isinstance(plan, str):
            try:
                plan = json.loads(plan)
            except json.JSONDecodeError:
                plan = {}
        approved = bool(overrides.get(approval_key) or (isinstance(plan, dict) and plan.get("approved")))
        if not approved:
            return {"column": column_name, "skipped": True, "reason": "approval_required"}
        if not ctx.table_ref:
            return {"column": column_name, "skipped": True, "reason": "table_ref_missing"}

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

        if not plan or not plan.get("plan_id") or not plan.get("plan_hash"):
            refreshed = await self.plan_data_repairs(
                table_asset_id, column_name, null_strategy, conflict_strategy
            )
            plan = refreshed.get("repair_plan") if isinstance(refreshed, dict) else None
            if not plan or not plan.get("plan_id") or not plan.get("plan_hash"):
                return await record_skip("plan_missing")
        if not plan.get("plan_id") or not plan.get("plan_hash"):
            return await record_skip("plan_missing")

        group_by = (plan.get("snapshot") or {}).get("group_by_columns") or conflicts.get(
            "group_by_columns"
        ) or overrides.get("conflict_group_columns") or []
        if isinstance(group_by, str):
            group_by = [item.strip() for item in group_by.split(",") if item.strip()]
        group_by = [str(item) for item in group_by if item]

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
        target_table = self._build_fixing_table_ref(ctx.table_ref, column_name)
        dry_run = bool(overrides.get("data_fix_dry_run") or overrides.get("repair_dry_run"))

        repair_results: list[dict[str, Any]] = []
        sql_previews = plan.get("sql_previews") or {}

        if dry_run:
            plan_update = dict(plan) if isinstance(plan, dict) else {}
            plan_update["apply_mode"] = "fixing_table"
            plan_update["target_table"] = target_table
            await self._update_column_analysis(
                ctx,
                {"repair_results": [{"type": "repair_apply", "status": "dry_run"}], "repair_plan": plan_update},
            )
            return {
                "column": column_name,
                "repairs": repair_results,
                "approved": approved,
                "target_table": target_table,
                "dry_run": True,
            }

        await self.sf.execute_query(
            f"CREATE OR REPLACE TABLE {target_table} AS SELECT * FROM {ctx.table_ref}"
        )

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
                update_query = (
                    f"UPDATE {target_table} SET {col} = {fill_expr} WHERE {col} IS NULL"
                )
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
                    update_query = update_query.replace(ctx.table_ref, target_table, 1)
                    await self.sf.execute_query(update_query)
                    repair_results.append(
                        {
                            "type": "conflict_repair",
                            "status": "applied",
                            "strategy": planned_conflict_strategy,
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
        if any(item.get("status") == "applied" for item in repair_results):
            plan_update["applied"] = True
            plan_update["applied_at"] = datetime.utcnow().isoformat()
        plan_update["apply_mode"] = "fixing_table"
        plan_update["target_table"] = target_table
        analysis_update = {"repair_results": repair_results, "repair_plan": plan_update}
        await self._update_column_analysis(ctx, analysis_update)

        return {
            "column": column_name,
            "repairs": repair_results,
            "approved": approved,
            "target_table": target_table,
        }
