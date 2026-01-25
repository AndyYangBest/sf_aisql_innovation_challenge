"""Column workflow tool mixin."""

from __future__ import annotations

import json
from typing import Any
from strands import tool

class ColumnWorkflowTextImageMixin:
    """Tool mixin."""

    @tool
    async def summarize_text_column(self, table_asset_id: int, column_name: str) -> dict[str, Any]:
        """Summarize text column using AI_SUMMARIZE_AGG with token estimate."""
        ctx = await self._load_context(table_asset_id, column_name)
        col = self._quote_ident(column_name)
        token_info = await self._estimate_column_tokens(ctx.analysis_query, col)

        summary_query = f"""
        WITH base AS (
            {ctx.analysis_query}
        )
        SELECT AI_SUMMARIZE_AGG({col}) AS summary
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

        output_column = (
            overrides.get("row_level_output_column")
            or overrides.get("output_column")
            or f"{column_name}_extracted"
        )
        col = self._quote_ident(column_name)
        token_info = await self._estimate_column_tokens(ctx.analysis_query, col)
        instruction_tokens = await self._estimate_tokens_for_prompt(str(instruction))
        safe_instruction = self._sanitize_literal(instruction)
        response_format = overrides.get("row_level_schema") or overrides.get("row_level_response_format")

        await self._ensure_column(ctx.table_ref, output_column)
        prompt_expr = f"CONCAT('{safe_instruction}', ' ', TO_VARCHAR({col}))"
        complete_expr = f"AI_COMPLETE('{self.model_id}', {prompt_expr})"
        if response_format:
            if isinstance(response_format, str):
                try:
                    response_format = json.loads(response_format)
                except json.JSONDecodeError:
                    response_format = None
            if isinstance(response_format, dict):
                if "schema" not in response_format:
                    response_format = {"type": "json", "schema": response_format}
                response_json = json.dumps(response_format)
                response_literal = self._sanitize_literal(response_json)
                complete_expr = (
                    f"AI_COMPLETE('{self.model_id}', {prompt_expr}, NULL, PARSE_JSON('{response_literal}'))"
                )

        update_query = f"""
        UPDATE {ctx.table_ref}
        SET {self._quote_ident(output_column)} = {complete_expr}
        WHERE {col} IS NOT NULL
        """
        await self.sf.execute_query(update_query)
        await self._ensure_feature_column_metadata(
            table_asset_id=ctx.table_asset_id,
            output_column=output_column,
            source_column=column_name,
            feature_type="row_level_extract",
        )

        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        feature_outputs = list(analysis.get("feature_outputs", []))
        feature_outputs = [
            item for item in feature_outputs
            if item.get("output_column") != output_column
        ]
        feature_outputs.append({
            "type": "row_level_extract",
            "output_column": output_column,
            "source_column": column_name,
            "instruction": instruction,
        })
        total_tokens = token_info.get("token_count", 0) + instruction_tokens * token_info.get("row_count", 0)
        analysis.update({
            "row_level_output": output_column,
            "row_level_token_estimate": {
                **token_info,
                "instruction_tokens": instruction_tokens,
                "total_tokens": total_tokens,
            },
            "feature_outputs": feature_outputs,
        })
        await self._update_column_analysis(ctx, analysis)

        return {"column": column_name, "output_column": output_column, "token_estimate": token_info}


    @tool
    async def describe_image_column(self, table_asset_id: int, column_name: str) -> dict[str, Any]:
        """Row-level AI_COMPLETE image descriptions; writes to new column."""
        ctx = await self._load_context(table_asset_id, column_name)
        if not ctx.table_ref:
            analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
            errors = list(analysis.get("errors", []))
            errors.append({
                "step": "describe_images",
                "error": "table_ref_missing",
                "detail": "Image descriptions require a physical table reference.",
            })
            analysis.update({"errors": errors})
            await self._update_column_analysis(ctx, analysis)
            return {"column": column_name, "skipped": True, "reason": "table_ref_missing"}

        overrides = ctx.column_meta.overrides or {}
        output_column = (
            overrides.get("image_output_column")
            or overrides.get("output_column")
            or f"{column_name}_description"
        )
        col = self._quote_ident(column_name)
        token_info = await self._estimate_column_tokens(ctx.analysis_query, col)
        instruction_text = "Describe the image in under 200 characters. If it cannot be accessed, respond with 'image_unavailable'."
        instruction_tokens = await self._estimate_tokens_for_prompt(instruction_text)
        instruction = self._sanitize_literal(instruction_text)
        file_expr = self._resolve_image_file_expr(ctx, col)
        image_model = overrides.get("image_model") or self.image_model_id
        supported_image_models = {
            "claude-4-opus",
            "claude-4-sonnet",
            "claude-3-7-sonnet",
            "claude-3-5-sonnet",
            "llama4-maverick",
            "llama4-scout",
            "openai-o4-mini",
            "openai-gpt-4.1",
            "pixtral-large",
        }
        if image_model not in supported_image_models:
            image_model = self.image_model_id

        if not file_expr:
            analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
            errors = list(analysis.get("errors", []))
            errors.append({
                "step": "describe_images",
                "error": "image_stage_missing",
                "detail": "Provide image_stage in table/column overrides or store FILE objects.",
            })
            analysis.update({"errors": errors})
            await self._update_column_analysis(ctx, analysis)
            return {"column": column_name, "skipped": True, "reason": "image_stage_missing"}

        await self._ensure_column(ctx.table_ref, output_column)
        update_query = f"""
        UPDATE {ctx.table_ref}
        SET {self._quote_ident(output_column)} = AI_COMPLETE(
            '{self._sanitize_literal(str(image_model))}',
            '{instruction}',
            {file_expr}
        )
        WHERE {col} IS NOT NULL
        """
        await self.sf.execute_query(update_query)
        await self._ensure_feature_column_metadata(
            table_asset_id=ctx.table_asset_id,
            output_column=output_column,
            source_column=column_name,
            feature_type="image_description",
        )

        analysis = (ctx.column_meta.metadata_payload or {}).get("analysis", {})
        feature_outputs = list(analysis.get("feature_outputs", []))
        feature_outputs = [
            item for item in feature_outputs
            if item.get("output_column") != output_column
        ]
        feature_outputs.append({
            "type": "image_description",
            "output_column": output_column,
            "source_column": column_name,
            "model": image_model,
        })
        total_tokens = token_info.get("row_count", 0) * instruction_tokens
        analysis.update({
            "image_descriptions_column": output_column,
            "row_level_token_estimate": {
                **token_info,
                "instruction_tokens": instruction_tokens,
                "total_tokens": total_tokens,
            },
            "feature_outputs": feature_outputs,
        })
        await self._update_column_analysis(ctx, analysis)

        return {"column": column_name, "output_column": output_column, "token_estimate": token_info}

