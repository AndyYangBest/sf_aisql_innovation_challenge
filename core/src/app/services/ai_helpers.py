"""
AI service helper utilities.

This module provides reusable utilities for AI-powered features,
including metadata generation, data analysis, and insight extraction.
"""

import json
import logging
from typing import Any

from ..config.prompts import build_metadata_prompt
from ..services.modular_ai_sql_service import ModularAISQLService


class AIMetadataGenerator:
    """Helper class for generating metadata using AI."""

    def __init__(self, ai_service: ModularAISQLService):
        """Initialize with AI service."""
        self.ai_service = ai_service
        self.logger = logging.getLogger(__name__)

    async def suggest_table_metadata(
        self,
        sql: str,
        columns: list[dict[str, str]] | None = None,
        sample_rows: list[dict[str, Any]] | None = None,
        model: str = "mistral-large2",
    ) -> dict[str, Any]:
        """
        Generate metadata suggestions for a SQL query.

        Args:
            sql: SQL query to analyze
            columns: Column definitions from query result
            sample_rows: Sample rows from query result
            model: AI model to use for generation

        Returns:
            Dictionary containing suggested metadata:
            - table_name: Suggested table name
            - tags: List of relevant tags
            - summary: Brief description
            - use_cases: List of potential use cases

        Raises:
            ValueError: If AI response cannot be parsed
        """
        # Build prompt using centralized template
        prompt = build_metadata_prompt(sql, columns, sample_rows)

        # Escape single quotes for Snowflake
        escaped_prompt = prompt.replace("'", "''")
        escaped_model = model.replace("'", "''")

        # Build SQL query with escaped prompt as literal
        query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{escaped_model}',
            '{escaped_prompt}'
        ) as response
        """

        # Execute query directly
        result = await self.ai_service.sf.execute_query(query)
        response = result[0]["RESPONSE"] if result else ""

        # Parse and validate response; fall back quietly on malformed output
        metadata = None
        try:
            cleaned = response.strip()
            if cleaned.startswith("```"):
                cleaned = cleaned.strip("`").lstrip("json").strip()
            metadata = json.loads(cleaned)
        except json.JSONDecodeError as e:
            self.logger.warning("Failed to parse AI response as JSON: %s", e)

        required_fields = ["table_name", "tags", "summary", "use_cases"]
        if metadata and all(field in metadata for field in required_fields):
            return metadata

        # Fallback to deterministic, non-AI metadata to avoid hard failures
        fallback = smart_metadata_fallback(sql)
        self.logger.info("Using smart metadata fallback")
        return fallback


class AIDataAnalyzer:
    """Helper class for analyzing data using AI."""

    def __init__(self, ai_service: ModularAISQLService):
        """Initialize with AI service."""
        self.ai_service = ai_service

    async def analyze_table_structure(
        self,
        table_name: str,
        columns: list[dict[str, str]],
        sample_data: list[dict[str, Any]],
        model: str = "mistral-large2",
    ) -> dict[str, Any]:
        """
        Analyze table structure and provide insights.

        Args:
            table_name: Name of the table
            columns: Column definitions
            sample_data: Sample rows
            model: AI model to use

        Returns:
            Analysis results including quality assessment and suggestions
        """
        # TODO: Implement using prompts.ANALYZE_TABLE_STRUCTURE_PROMPT
        raise NotImplementedError("To be implemented")


class AIInsightGenerator:
    """Helper class for generating business insights using AI."""

    def __init__(self, ai_service: ModularAISQLService):
        """Initialize with AI service."""
        self.ai_service = ai_service

    async def generate_insights(
        self,
        data_summary: str,
        column_stats: dict[str, Any],
        model: str = "mistral-large2",
    ) -> list[str]:
        """
        Generate business insights from data.

        Args:
            data_summary: Summary of the data
            column_stats: Statistics for each column
            model: AI model to use

        Returns:
            List of insight strings
        """
        # TODO: Implement using prompts.GENERATE_INSIGHTS_PROMPT
        raise NotImplementedError("To be implemented")


def extract_table_name_from_sql(sql: str) -> str | None:
    """
    Extract table name from SQL query using regex.

    Args:
        sql: SQL query string

    Returns:
        Extracted table name or None if not found
    """
    import re
    table_match = re.search(r'FROM\s+([A-Za-z0-9_\.]+)', sql, re.IGNORECASE)
    if table_match:
        full_name = table_match.group(1)
        # Return just the table name (last part after dots)
        return full_name.split('.')[-1].lower()
    return None


def smart_metadata_fallback(
    sql: str,
    table_name: str | None = None,
) -> dict[str, Any]:
    """
    Generate metadata using SQL parsing when AI is unavailable.

    This is a smart fallback that analyzes the SQL structure to generate
    meaningful metadata without requiring AI.

    Args:
        sql: SQL query to analyze
        table_name: Optional table name hint

    Returns:
        Dictionary with metadata fields
    """
    import re

    # Extract table name from SQL
    detected_table = extract_table_name_from_sql(sql)
    base_name = detected_table or table_name or "new_table"

    # Parse table name into words
    table_words = base_name.replace('_', ' ').split()

    # Generate tags from table name
    tags = [word for word in table_words if len(word) > 2]

    # Add SQL operation tags
    sql_lower = sql.lower()
    if 'where' in sql_lower:
        tags.append('filtered')
    if 'join' in sql_lower:
        tags.append('joined')
    if 'group by' in sql_lower:
        tags.append('aggregated')
    if 'order by' in sql_lower:
        tags.append('sorted')

    # Limit to 5 unique tags
    tags = list(dict.fromkeys(tags))[:5]

    # Generate meaningful name
    if len(table_words) > 1:
        suggested_name = base_name
    else:
        suggested_name = f"{base_name}_analysis"

    # Generate summary
    operation = "retrieves data"
    if 'group by' in sql_lower:
        operation = "aggregates and analyzes data"
    elif 'join' in sql_lower:
        operation = "combines data from multiple sources"
    elif 'where' in sql_lower:
        operation = "filters and retrieves specific data"

    summary = f"This query {operation} from {base_name.replace('_', ' ')}."

    # Generate use cases
    use_cases = [
        f"{base_name.replace('_', ' ').title()} Analysis",
        "Data Exploration",
        "Reporting Dashboard"
    ]

    return {
        "table_name": suggested_name,
        "tags": tags,
        "summary": summary,
        "use_cases": use_cases,
    }
