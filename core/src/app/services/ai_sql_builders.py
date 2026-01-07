"""Modular query builders for Snowflake Cortex AI SQL functions.

This module provides composable building blocks for creating AISQL queries.
Each builder can be used independently or combined to create complex queries.
"""

from abc import ABC, abstractmethod
from typing import Any


# ============================================================================
# Base Query Builder
# ============================================================================


class QueryBuilder(ABC):
    """Base class for all query builders."""

    def __init__(self):
        self._query_parts = []

    @abstractmethod
    def build(self) -> str:
        """Build and return the complete SQL query."""
        pass

    def __str__(self) -> str:
        return self.build()


# ============================================================================
# Core AISQL Function Builders
# ============================================================================


class AICompleteBuilder:
    """Builder for AI_COMPLETE function calls."""

    def __init__(self, model: str, prompt: str):
        self.model = model
        self.prompt = prompt
        self.response_format = None

    def with_response_format(self, schema: dict[str, Any]) -> "AICompleteBuilder":
        """Add structured output format."""
        self.response_format = schema
        return self

    def build(self) -> str:
        """Build AI_COMPLETE function call."""
        # Escape single quotes in prompt
        escaped_prompt = self.prompt.replace("'", "''")

        if self.response_format:
            # Convert dict to JSON string for Snowflake
            import json
            format_json = json.dumps(self.response_format).replace("'", "''")
            return f"""SNOWFLAKE.CORTEX.COMPLETE(
                '{self.model}',
                '{escaped_prompt}',
                PARSE_JSON('{format_json}')
            )"""
        return f"SNOWFLAKE.CORTEX.COMPLETE('{self.model}', '{escaped_prompt}')"


class AIClassifyBuilder:
    """Builder for AI_CLASSIFY function calls."""

    def __init__(self, content: str, categories: list[str]):
        self.content = content
        self.categories = categories
        self.prompt_prefix = None

    def with_prompt_prefix(self, prefix: str) -> "AIClassifyBuilder":
        """Add prompt prefix."""
        self.prompt_prefix = prefix
        return self

    def build(self) -> str:
        """Build AI_CLASSIFY function call."""
        categories_str = ", ".join(f"'{cat}'" for cat in self.categories)
        content_expr = (
            f"'{self.prompt_prefix}' || {self.content}" if self.prompt_prefix else self.content
        )

        return f"""AI_CLASSIFY(
            {content_expr},
            ARRAY_CONSTRUCT({categories_str})
        )"""


class AIFilterBuilder:
    """Builder for AI_FILTER function calls."""

    def __init__(self, condition: str, *columns: str):
        self.condition = condition
        self.columns = columns

    def build(self) -> str:
        """Build AI_FILTER function call."""
        columns_str = ", ".join(self.columns)
        return f"AI_FILTER(PROMPT('{self.condition}', {columns_str}))"


class AIAggregateBuilder:
    """Builder for AI_AGG function calls."""

    def __init__(self, column: str, prompt: str):
        self.column = column
        self.prompt = prompt

    def build(self) -> str:
        """Build AI_AGG function call."""
        return f"AI_AGG({self.column}, '{self.prompt}')"


class AISentimentBuilder:
    """Builder for AI_SENTIMENT function calls."""

    def __init__(self, text_column: str):
        self.text_column = text_column

    def build(self) -> str:
        """Build AI_SENTIMENT function call."""
        return f"AI_SENTIMENT({self.text_column})"


class SummarizeBuilder:
    """Builder for SUMMARIZE function calls."""

    def __init__(self, text_column: str):
        self.text_column = text_column

    def build(self) -> str:
        """Build SUMMARIZE function call."""
        return f"SNOWFLAKE.CORTEX.SUMMARIZE({self.text_column})"


class AITranscribeBuilder:
    """Builder for AI_TRANSCRIBE function calls."""

    def __init__(self, audio_column: str):
        self.audio_column = audio_column

    def build(self) -> str:
        """Build AI_TRANSCRIBE function call."""
        return f"AI_TRANSCRIBE({self.audio_column})"


# ============================================================================
# Query Composition Builders
# ============================================================================


class SelectQueryBuilder(QueryBuilder):
    """Builder for SELECT queries with AISQL functions."""

    def __init__(self, table_name: str):
        super().__init__()
        self.table_name = table_name
        self.select_columns = []
        self.where_conditions = []
        self.group_by_columns = []
        self.order_by_columns = []
        self.limit_value = None

    def select(self, *columns: str | tuple[str, str]) -> "SelectQueryBuilder":
        """Add columns to SELECT clause.

        Args:
            columns: Column names or (expression, alias) tuples
        """
        for col in columns:
            if isinstance(col, tuple):
                expr, alias = col
                self.select_columns.append(f"{expr} as {alias}")
            else:
                self.select_columns.append(col)
        return self

    def select_ai_function(self, builder: Any, alias: str) -> "SelectQueryBuilder":
        """Add an AISQL function to SELECT clause.

        Args:
            builder: An AISQL function builder instance
            alias: Column alias for the result
        """
        self.select_columns.append(f"{builder.build()} as {alias}")
        return self

    def where(self, condition: str) -> "SelectQueryBuilder":
        """Add WHERE condition."""
        self.where_conditions.append(condition)
        return self

    def where_ai_filter(self, builder: AIFilterBuilder) -> "SelectQueryBuilder":
        """Add AI_FILTER as WHERE condition."""
        self.where_conditions.append(builder.build())
        return self

    def group_by(self, *columns: str) -> "SelectQueryBuilder":
        """Add GROUP BY columns."""
        self.group_by_columns.extend(columns)
        return self

    def order_by(self, *columns: str) -> "SelectQueryBuilder":
        """Add ORDER BY columns."""
        self.order_by_columns.extend(columns)
        return self

    def limit(self, n: int) -> "SelectQueryBuilder":
        """Set LIMIT."""
        self.limit_value = n
        return self

    def build(self) -> str:
        """Build complete SELECT query."""
        query_parts = []

        # SELECT clause
        columns_str = ",\n            ".join(self.select_columns) if self.select_columns else "*"
        query_parts.append(f"SELECT\n            {columns_str}")

        # FROM clause
        query_parts.append(f"        FROM {self.table_name}")

        # WHERE clause
        if self.where_conditions:
            where_str = " AND ".join(f"({cond})" for cond in self.where_conditions)
            query_parts.append(f"        WHERE {where_str}")

        # GROUP BY clause
        if self.group_by_columns:
            group_str = ", ".join(self.group_by_columns)
            query_parts.append(f"        GROUP BY {group_str}")

        # ORDER BY clause
        if self.order_by_columns:
            order_str = ", ".join(self.order_by_columns)
            query_parts.append(f"        ORDER BY {order_str}")

        # LIMIT clause
        if self.limit_value:
            query_parts.append(f"        LIMIT {self.limit_value}")

        return "\n".join(query_parts)


class CTEQueryBuilder(QueryBuilder):
    """Builder for queries with Common Table Expressions (CTEs)."""

    def __init__(self):
        super().__init__()
        self.ctes = []
        self.final_query = None

    def with_cte(self, name: str, query_builder: QueryBuilder) -> "CTEQueryBuilder":
        """Add a CTE to the query.

        Args:
            name: CTE name
            query_builder: Query builder for the CTE
        """
        self.ctes.append((name, query_builder))
        return self

    def final_select(self, query_builder: QueryBuilder) -> "CTEQueryBuilder":
        """Set the final SELECT query."""
        self.final_query = query_builder
        return self

    def build(self) -> str:
        """Build complete query with CTEs."""
        query_parts = []

        if self.ctes:
            cte_parts = []
            for name, builder in self.ctes:
                cte_query = builder.build()
                cte_parts.append(f"{name} AS (\n{cte_query}\n)")

            query_parts.append("WITH " + ",\n".join(cte_parts))

        if self.final_query:
            query_parts.append(self.final_query.build())

        return "\n".join(query_parts)


# ============================================================================
# Specialized Composite Builders
# ============================================================================


class SemanticJoinBuilder(QueryBuilder):
    """Builder for semantic JOINs using AI_FILTER."""

    def __init__(
        self,
        left_table: str,
        right_table: str,
        left_column: str,
        right_column: str,
        join_condition: str,
    ):
        super().__init__()
        self.left_table = left_table
        self.right_table = right_table
        self.left_column = left_column
        self.right_column = right_column
        self.join_condition = join_condition
        self.join_type = "LEFT JOIN"
        self.additional_columns = []
        self.limit_value = 100

    def with_join_type(self, join_type: str) -> "SemanticJoinBuilder":
        """Set JOIN type (INNER, LEFT, RIGHT, FULL)."""
        self.join_type = join_type
        return self

    def select_additional(self, *columns: str) -> "SemanticJoinBuilder":
        """Add additional columns to select."""
        self.additional_columns.extend(columns)
        return self

    def limit(self, n: int) -> "SemanticJoinBuilder":
        """Set LIMIT."""
        self.limit_value = n
        return self

    def build(self) -> str:
        """Build semantic JOIN query."""
        select_cols = [
            f"l.{self.left_column} as left_content",
            f"r.{self.right_column} as right_content",
        ]
        select_cols.extend(self.additional_columns)

        select_str = ",\n            ".join(select_cols)

        return f"""
        SELECT
            {select_str}
        FROM {self.left_table} l
        {self.join_type} {self.right_table} r
            ON AI_FILTER(PROMPT('{self.join_condition}', l.{self.left_column}, r.{self.right_column}))
        LIMIT {self.limit_value}
        """


class StructuredExtractionBuilder(QueryBuilder):
    """Builder for extracting structured data from unstructured text."""

    def __init__(
        self,
        table_name: str,
        text_column: str,
        extraction_prompt: str,
        schema: dict[str, Any],
    ):
        super().__init__()
        self.table_name = table_name
        self.text_column = text_column
        self.extraction_prompt = extraction_prompt
        self.schema = schema
        self.model = "claude-3-7-sonnet"
        self.limit_value = 100

    def with_model(self, model: str) -> "StructuredExtractionBuilder":
        """Set LLM model."""
        self.model = model
        return self

    def limit(self, n: int) -> "StructuredExtractionBuilder":
        """Set LIMIT."""
        self.limit_value = n
        return self

    def build(self) -> str:
        """Build structured extraction query."""
        return f"""
        SELECT
            {self.text_column},
            SNOWFLAKE.CORTEX.COMPLETE(
                model => '{self.model}',
                prompt => PROMPT('{self.extraction_prompt}', {self.text_column}),
                response_format => {self.schema}
            ) as extracted_data
        FROM {self.table_name}
        LIMIT {self.limit_value}
        """


# ============================================================================
# Convenience Factory Functions
# ============================================================================


def ai_complete(model: str, prompt: str) -> AICompleteBuilder:
    """Create AI_COMPLETE builder."""
    return AICompleteBuilder(model, prompt)


def ai_classify(content: str, categories: list[str]) -> AIClassifyBuilder:
    """Create AI_CLASSIFY builder."""
    return AIClassifyBuilder(content, categories)


def ai_filter(condition: str, *columns: str) -> AIFilterBuilder:
    """Create AI_FILTER builder."""
    return AIFilterBuilder(condition, *columns)


def ai_aggregate(column: str, prompt: str) -> AIAggregateBuilder:
    """Create AI_AGG builder."""
    return AIAggregateBuilder(column, prompt)


def ai_sentiment(text_column: str) -> AISentimentBuilder:
    """Create AI_SENTIMENT builder."""
    return AISentimentBuilder(text_column)


def summarize(text_column: str) -> SummarizeBuilder:
    """Create SUMMARIZE builder."""
    return SummarizeBuilder(text_column)


def ai_transcribe(audio_column: str) -> AITranscribeBuilder:
    """Create AI_TRANSCRIBE builder."""
    return AITranscribeBuilder(audio_column)


def select(table_name: str) -> SelectQueryBuilder:
    """Create SELECT query builder."""
    return SelectQueryBuilder(table_name)


def with_cte() -> CTEQueryBuilder:
    """Create CTE query builder."""
    return CTEQueryBuilder()


def semantic_join(
    left_table: str,
    right_table: str,
    left_column: str,
    right_column: str,
    join_condition: str,
) -> SemanticJoinBuilder:
    """Create semantic JOIN builder."""
    return SemanticJoinBuilder(left_table, right_table, left_column, right_column, join_condition)


def extract_structured(
    table_name: str,
    text_column: str,
    extraction_prompt: str,
    schema: dict[str, Any],
) -> StructuredExtractionBuilder:
    """Create structured extraction builder."""
    return StructuredExtractionBuilder(table_name, text_column, extraction_prompt, schema)
