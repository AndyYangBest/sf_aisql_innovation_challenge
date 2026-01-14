"""Column semantics-driven metadata service."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..core.config import settings
from ..models.table_asset import TableAsset
from ..models.column_metadata import ColumnMetadata
from ..models.table_asset_metadata import TableAssetMetadata
from .data_type_detector import DataTypeDetector, DataTypeCategory
from .snowflake_service import SnowflakeService
from .modular_ai_sql_service import ModularAISQLService


TIME_SQL_TYPES = {"DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ", "DATETIME", "TIME"}
TEXT_SQL_TYPES = {"VARCHAR", "TEXT", "STRING"}
NUMERIC_SQL_TYPES = {"NUMBER", "FLOAT", "INTEGER", "DECIMAL", "DOUBLE", "NUMERIC"}

SEMANTIC_TYPES = [
    "numeric",
    "categorical",
    "temporal",
    "text",
    "spatial",
    "binary",
    "image",
    "id",
]


class ColumnMetadataService:
    """Service to build and cache column metadata with rule-first inference."""

    def __init__(
        self,
        db: AsyncSession,
        snowflake_service: SnowflakeService,
        ai_sql_service: ModularAISQLService,
    ) -> None:
        self.db = db
        self.sf = snowflake_service
        self.ai_sql = ai_sql_service
        self.detector = DataTypeDetector()
        self.model_id = settings.SNOWFLAKE_CORTEX_MODEL or "mistral-large2"

    async def get_cached_metadata(self, table_asset_id: int) -> tuple[TableAssetMetadata | None, list[ColumnMetadata]]:
        table_meta_result = await self.db.execute(
            select(TableAssetMetadata).where(TableAssetMetadata.table_asset_id == table_asset_id)
        )
        table_meta = table_meta_result.scalar_one_or_none()

        columns_result = await self.db.execute(
            select(ColumnMetadata).where(ColumnMetadata.table_asset_id == table_asset_id)
        )
        columns = list(columns_result.scalars().all())

        return table_meta, columns

    async def initialize_metadata(self, table_asset_id: int, force: bool = False) -> tuple[TableAssetMetadata, list[ColumnMetadata]]:
        table_meta, columns = await self.get_cached_metadata(table_asset_id)
        if table_meta and columns and not force:
            return table_meta, columns

        asset_result = await self.db.execute(
            select(TableAsset).where(
                TableAsset.id == table_asset_id,
                TableAsset.is_deleted == False,
            )
        )
        asset = asset_result.scalar_one_or_none()
        if not asset:
            raise ValueError(f"Table asset {table_asset_id} not found")

        base_query, table_ref, is_query = self._build_base_query(asset)
        schema_info = await self._get_schema_info(base_query, is_query)

        structure_info = self._detect_table_structure(schema_info)
        sample_query = self._build_sample_query(
            base_query=base_query,
            structure_type=structure_info["structure_type"],
            time_column=structure_info.get("time_column"),
            entity_column=structure_info.get("entity_column"),
            sample_size=structure_info.get("sample_size", 50),
        )
        sample_rows = await self.sf.execute_query(sample_query)

        column_metadata = await self._infer_columns(
            schema_info=schema_info,
            sample_rows=sample_rows,
            table_ref=table_ref,
            base_query=base_query,
        )

        table_metadata = await self._upsert_table_metadata(
            table_asset_id=table_asset_id,
            structure_info=structure_info,
            base_query=base_query,
            sample_query=sample_query,
        )

        saved_columns = await self._upsert_column_metadata(table_asset_id, column_metadata)
        await self.db.commit()

        return table_metadata, saved_columns

    def _build_base_query(self, asset: TableAsset) -> tuple[str, str | None, bool]:
        source_sql = (asset.source_sql or "").strip().rstrip(";")
        database = asset.database
        schema = asset.schema

        if self._looks_like_table_ref(source_sql):
            table_ref = source_sql
            if database and schema and "." not in table_ref:
                table_ref = f"{database}.{schema}.{table_ref}"
            base_query = f"SELECT * FROM {table_ref}"
            return base_query, table_ref, False

        table_ref = None
        return source_sql, table_ref, True

    def _looks_like_table_ref(self, text: str) -> bool:
        if not text:
            return False
        if " " in text or "\n" in text or "\t" in text:
            return False
        return re.match(r"^[A-Za-z0-9_\.]+$", text) is not None

    async def _get_schema_info(self, base_query: str, is_query: bool) -> list[dict[str, Any]]:
        if not is_query:
            table_ref = self._extract_table_name(base_query)
            if not table_ref:
                raise ValueError("Unable to determine table name from base query")
            database, schema, table_name = self._split_table_ref(table_ref)
            return await self.sf.get_table_columns(table_name, database=database, schema=schema)

        sample_query = f"SELECT * FROM ({base_query}) LIMIT 1"
        sample = await self.sf.execute_query(sample_query)

        describe_query = "DESCRIBE RESULT LAST_QUERY_ID()"
        try:
            schema_info = await self.sf.execute_query(describe_query)
            return [
                {
                    "COLUMN_NAME": row.get("name", row.get("NAME")),
                    "DATA_TYPE": row.get("type", row.get("TYPE")),
                    "IS_NULLABLE": "YES",
                    "ORDINAL_POSITION": idx + 1,
                }
                for idx, row in enumerate(schema_info)
            ]
        except Exception:
            if not sample:
                raise ValueError("Query returned no results; cannot infer schema")
            return [
                {
                    "COLUMN_NAME": col,
                    "DATA_TYPE": "VARIANT",
                    "IS_NULLABLE": "YES",
                    "ORDINAL_POSITION": idx + 1,
                }
                for idx, col in enumerate(sample[0].keys())
            ]

    def _extract_table_name(self, base_query: str) -> str | None:
        match = re.search(r"FROM\s+([A-Za-z0-9_\.]+)", base_query, re.IGNORECASE)
        if match:
            return match.group(1)
        return None

    def _split_table_ref(self, table_ref: str) -> tuple[str | None, str | None, str]:
        parts = table_ref.split(".")
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        if len(parts) == 2:
            return None, parts[0], parts[1]
        return None, None, table_ref

    def _detect_table_structure(self, schema_info: list[dict[str, Any]]) -> dict[str, Any]:
        columns = [col["COLUMN_NAME"] for col in schema_info]
        name_lower = {col: col.lower() for col in columns}
        type_by_column = {col["COLUMN_NAME"]: col.get("DATA_TYPE") for col in schema_info}

        time_columns = [
            col for col in columns
            if (
                col.lower().endswith("_at")
                or any(token in name_lower[col] for token in ["date", "time", "timestamp", "created", "updated", "year", "month", "day"]) 
                or type_by_column.get(col) in TIME_SQL_TYPES
            )
        ]

        entity_columns = [
            col for col in columns
            if any(token in name_lower[col] for token in ["id", "user", "account", "device", "shop", "store", "customer", "member", "session"]) 
            and col not in time_columns
        ]

        structure_type = "iid"
        if time_columns and entity_columns:
            structure_type = "panel"
        elif time_columns:
            structure_type = "time_series"

        sample_size = 50
        if structure_type == "panel":
            sample_size = 120

        sampling_strategy = {
            "time_series": "time_window",
            "panel": "entity_time_stratified",
            "iid": "random",
        }[structure_type]

        return {
            "structure_type": structure_type,
            "time_column": time_columns[0] if time_columns else None,
            "entity_column": entity_columns[0] if entity_columns else None,
            "sample_size": sample_size,
            "sampling_strategy": sampling_strategy,
        }

    def _build_sample_query(
        self,
        base_query: str,
        structure_type: str,
        time_column: str | None,
        entity_column: str | None,
        sample_size: int,
    ) -> str:
        if structure_type == "time_series" and time_column:
            return f"SELECT * FROM ({base_query}) ORDER BY {self._quote_ident(time_column)} LIMIT {sample_size}"

        if structure_type == "panel" and entity_column:
            entity_limit = max(5, min(20, sample_size // 5))
            per_entity = max(2, sample_size // entity_limit)
            order_column = self._quote_ident(time_column) if time_column else self._quote_ident(entity_column)
            return f"""
            WITH base AS (
                {base_query}
            ), entities AS (
                SELECT DISTINCT {self._quote_ident(entity_column)} AS entity_id
                FROM base
                WHERE {self._quote_ident(entity_column)} IS NOT NULL
                ORDER BY RANDOM()
                LIMIT {entity_limit}
            )
            SELECT *
            FROM base
            WHERE {self._quote_ident(entity_column)} IN (SELECT entity_id FROM entities)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY {self._quote_ident(entity_column)}
                ORDER BY {order_column}
            ) <= {per_entity}
            """

        return f"SELECT * FROM ({base_query}) ORDER BY RANDOM() LIMIT {sample_size}"

    async def _infer_columns(
        self,
        schema_info: list[dict[str, Any]],
        sample_rows: list[dict[str, Any]],
        table_ref: str | None,
        base_query: str,
    ) -> list[dict[str, Any]]:
        columns_metadata: list[dict[str, Any]] = []
        sample_size = len(sample_rows)

        for col in schema_info:
            col_name = col["COLUMN_NAME"]
            sql_type = col.get("DATA_TYPE", "VARIANT")
            values = [row.get(col_name) for row in sample_rows]
            non_null_values = [v for v in values if v is not None]
            unique_count = len(set(non_null_values))
            null_count = sample_size - len(non_null_values)

            samples = self._build_type_aware_samples(sql_type, non_null_values)

            type_inference = self.detector.infer_column_type(
                column_name=col_name,
                sql_type=sql_type,
                sample_values=samples,
                unique_count=unique_count,
                total_count=sample_size,
                null_count=null_count,
            )

            semantic_type, confidence = self._map_semantic_type(
                col_name,
                sql_type,
                non_null_values,
                type_inference["inferred_type"],
                type_inference["confidence"],
            )

            ai_context = None
            if semantic_type == "unknown" or confidence < 0.55:
                ai_context = await self._ai_refine_type(
                    col_name=col_name,
                    sql_type=sql_type,
                    samples=samples,
                    current_type=semantic_type,
                )
                if ai_context:
                    semantic_type = ai_context.get("type", semantic_type)
                    confidence = max(confidence, ai_context.get("confidence", confidence))

            image_description = None
            if semantic_type == "image" and samples:
                image_description = await self._describe_image_sample(samples[0])

            metadata_payload = {
                "sql_type": sql_type,
                "sample_size": sample_size,
                "unique_count": unique_count,
                "null_count": null_count,
                "null_rate": (null_count / sample_size) if sample_size else 0,
                "examples": samples,
                "type_inference": type_inference,
                "ai_refinement": ai_context,
                "image_description": image_description,
                "table_ref": table_ref,
            }

            provenance = {
                "base_query": base_query,
                "sample_size": sample_size,
                "rule_first": True,
                "used_ai": ai_context is not None,
            }

            columns_metadata.append(
                {
                    "column_name": col_name,
                    "semantic_type": semantic_type,
                    "confidence": confidence,
                    "metadata": metadata_payload,
                    "provenance": provenance,
                    "examples": samples,
                    "overrides": None,
                }
            )

        return columns_metadata

    def _build_type_aware_samples(self, sql_type: str, values: list[Any]) -> list[Any]:
        if not values:
            return []

        if sql_type in NUMERIC_SQL_TYPES:
            numeric_values = [v for v in values if isinstance(v, (int, float))]
            if not numeric_values:
                return values[:5]
            numeric_values_sorted = sorted(numeric_values)
            extremes = [numeric_values_sorted[0], numeric_values_sorted[-1]]
            mid = numeric_values_sorted[len(numeric_values_sorted) // 2]
            return list(dict.fromkeys(extremes + [mid]))[:5]

        if sql_type in TIME_SQL_TYPES:
            ordered = sorted(values)
            extremes = [ordered[0], ordered[-1]] if len(ordered) > 1 else ordered
            return list(dict.fromkeys(extremes))[:5]

        if sql_type in TEXT_SQL_TYPES:
            normalized = [str(v) for v in values]
            counts: dict[str, int] = {}
            for val in normalized:
                counts[val] = counts.get(val, 0) + 1
            top_values = sorted(counts.items(), key=lambda item: item[1], reverse=True)
            deduped = list(dict.fromkeys(normalized))
            samples = [val for val, _ in top_values[:3]]
            samples.extend(deduped[:5])
            return list(dict.fromkeys(samples))[:5]

        return list(dict.fromkeys(values))[:5]

    def _map_semantic_type(
        self,
        column_name: str,
        sql_type: str,
        values: list[Any],
        inferred_type: DataTypeCategory,
        confidence: float,
    ) -> tuple[str, float]:
        column_lower = column_name.lower()

        if self._looks_like_image_column(column_lower, values):
            return "image", max(confidence, 0.7)

        if inferred_type in {
            DataTypeCategory.CONTINUOUS_NUMERIC,
            DataTypeCategory.DISCRETE_NUMERIC,
            DataTypeCategory.RATIO_PERCENTAGE,
        }:
            return "numeric", confidence

        if inferred_type in {DataTypeCategory.NOMINAL_CATEGORICAL, DataTypeCategory.ORDINAL_CATEGORICAL}:
            return "categorical", confidence

        if inferred_type in {
            DataTypeCategory.DATETIME,
            DataTypeCategory.DATE,
            DataTypeCategory.TIME,
            DataTypeCategory.TIMESTAMP,
            DataTypeCategory.TEMPORAL_CYCLIC,
        }:
            return "temporal", confidence

        if inferred_type in {DataTypeCategory.TEXT_SHORT, DataTypeCategory.TEXT_LONG, DataTypeCategory.TEXT_STRUCTURED}:
            return "text", confidence

        if inferred_type == DataTypeCategory.GEOSPATIAL:
            return "spatial", confidence

        if inferred_type == DataTypeCategory.BINARY:
            return "binary", confidence

        if inferred_type == DataTypeCategory.IDENTIFIER:
            return "id", confidence

        if sql_type in TIME_SQL_TYPES:
            return "temporal", max(confidence, 0.6)

        if sql_type in NUMERIC_SQL_TYPES:
            return "numeric", max(confidence, 0.5)

        if sql_type in TEXT_SQL_TYPES:
            return "text", max(confidence, 0.5)

        return "unknown", confidence

    def _looks_like_image_column(self, column_lower: str, values: list[Any]) -> bool:
        if any(token in column_lower for token in ["image", "img", "photo", "picture", "thumbnail", "avatar"]):
            return True

        for value in values[:5]:
            if isinstance(value, str) and re.search(r"\.(png|jpg|jpeg|gif|webp)(\?.*)?$", value, re.IGNORECASE):
                return True
        return False

    async def _ai_refine_type(self, col_name: str, sql_type: str, samples: list[Any], current_type: str) -> dict[str, Any] | None:
        prompt = (
            "You are a data profiling assistant. "
            "Choose the best semantic type for this column from: "
            + ", ".join(SEMANTIC_TYPES)
            + ".\n"
            f"Column name: {col_name}\n"
            f"SQL type: {sql_type}\n"
            f"Samples: {samples}\n"
            f"Current guess: {current_type}\n"
            "Respond as JSON: {\"type\": \"...\", \"confidence\": 0.0-1.0, \"rationale\": \"...\"}."
        )

        tokens = await self._estimate_tokens_for_prompt(prompt)
        try:
            result = await self.ai_sql.ai_complete(
                model=self.model_id,
                prompt=prompt,
                response_format={
                    "type": "object",
                    "properties": {
                        "type": {"type": "string"},
                        "confidence": {"type": "number"},
                        "rationale": {"type": "string"},
                    },
                },
            )
        except Exception:
            return None

        try:
            parsed = json.loads(result)
            parsed["token_estimate"] = tokens
            return parsed
        except json.JSONDecodeError:
            return {"type": current_type, "confidence": 0.0, "rationale": "parse_failed", "token_estimate": tokens}

    async def _describe_image_sample(self, sample_value: Any) -> dict[str, Any]:
        prompt = (
            "Describe the image referenced below in under 200 characters. "
            "If the image cannot be accessed, respond with 'image_unavailable'.\n"
            f"Image reference: {sample_value}"
        )
        tokens = await self._estimate_tokens_for_prompt(prompt)
        try:
            response = await self.ai_sql.ai_complete(
                model=self.model_id,
                prompt=prompt,
            )
            description = response.strip()
        except Exception:
            description = "image_unavailable"
        if len(description) > 200:
            description = description[:197] + "..."
        return {
            "description": description,
            "token_estimate": tokens,
            "source": "ai_complete_prompt",
        }

    async def _estimate_tokens_for_prompt(self, prompt: str, model: str | None = None) -> int:
        safe_prompt = self._sanitize_literal(prompt)
        model_id = model or self.model_id
        query = f"SELECT SNOWFLAKE.CORTEX.COUNT_TOKENS('{model_id}', '{safe_prompt}') as TOKEN_COUNT"
        try:
            result = await self.sf.execute_query(query)
        except Exception:
            return 0
        return int(result[0]["TOKEN_COUNT"]) if result else 0

    def _sanitize_literal(self, text: str) -> str:
        cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", " ", text)
        cleaned = cleaned.replace("\\", "\\\\").replace("'", "''")
        cleaned = cleaned.replace("\r", "\\n").replace("\n", "\\n").replace("\t", " ")
        return cleaned

    async def _upsert_table_metadata(
        self,
        table_asset_id: int,
        structure_info: dict[str, Any],
        base_query: str,
        sample_query: str,
    ) -> TableAssetMetadata:
        result = await self.db.execute(
            select(TableAssetMetadata).where(TableAssetMetadata.table_asset_id == table_asset_id)
        )
        existing = result.scalar_one_or_none()
        metadata_payload = {
            "base_query": base_query,
            "sample_query": sample_query,
            "time_column": structure_info.get("time_column"),
            "entity_column": structure_info.get("entity_column"),
            "sample_size": structure_info.get("sample_size", 50),
        }

        if existing:
            existing.structure_type = structure_info["structure_type"]
            existing.sampling_strategy = structure_info.get("sampling_strategy", structure_info["structure_type"])
            existing.metadata_payload = metadata_payload
            existing.last_updated = datetime.now(timezone.utc)
            return existing

        table_meta = TableAssetMetadata()
        table_meta.table_asset_id = table_asset_id
        table_meta.structure_type = structure_info["structure_type"]
        table_meta.sampling_strategy = structure_info.get("sampling_strategy", structure_info["structure_type"])
        table_meta.metadata_payload = metadata_payload
        table_meta.overrides = None
        table_meta.last_updated = datetime.now(timezone.utc)
        self.db.add(table_meta)
        return table_meta

    async def _upsert_column_metadata(
        self, table_asset_id: int, columns: list[dict[str, Any]]
    ) -> list[ColumnMetadata]:
        existing_result = await self.db.execute(
            select(ColumnMetadata).where(ColumnMetadata.table_asset_id == table_asset_id)
        )
        existing = {col.column_name: col for col in existing_result.scalars().all()}

        saved: list[ColumnMetadata] = []
        for payload in columns:
            col_name = payload["column_name"]
            if col_name in existing:
                record = existing[col_name]
                record.semantic_type = payload["semantic_type"]
                record.confidence = payload["confidence"]
                record.metadata_payload = payload.get("metadata")
                record.provenance = payload.get("provenance")
                record.examples = payload.get("examples")
                record.overrides = payload.get("overrides")
                record.last_updated = datetime.now(timezone.utc)
            else:
                record = ColumnMetadata()
                record.table_asset_id = table_asset_id
                record.column_name = col_name
                record.semantic_type = payload["semantic_type"]
                record.confidence = payload["confidence"]
                record.metadata_payload = payload.get("metadata")
                record.provenance = payload.get("provenance")
                record.examples = payload.get("examples")
                record.overrides = payload.get("overrides")
                record.last_updated = datetime.now(timezone.utc)
                self.db.add(record)
            saved.append(record)

        return saved

    def _quote_ident(self, identifier: str) -> str:
        return f'"{identifier.replace("\"", "\"\"")}"'
