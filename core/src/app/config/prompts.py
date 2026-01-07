"""
Centralized prompt templates for AI services.

This module contains all AI prompts used across the application.
Prompts are stored as templates with variables that can be substituted at runtime.
"""

from typing import Any


class PromptTemplate:
    """Base class for prompt templates with variable substitution."""

    def __init__(self, template: str):
        self.template = template

    def format(self, **kwargs: Any) -> str:
        """Format the template with provided variables."""
        return self.template.format(**kwargs)


# ============================================================================
# Metadata Generation Prompts
# ============================================================================

SUGGEST_METADATA_PROMPT = """Analyze this SQL query and the actual data it returns to suggest meaningful metadata.

SQL Query:
{sql_query}

{columns_section}

{sample_data_section}

Based on the SQL query, columns, and actual data, please provide:
1. A concise, descriptive table name (2-4 words, snake_case) that reflects what this data represents
2. Relevant tags (3-5 keywords) based on the actual column names and data content
3. A brief summary (1-2 sentences) describing what insights this data provides
4. Potential use cases (2-3 specific examples) based on the data content

Respond ONLY with valid JSON in this exact format:
{{
    "table_name": "descriptive_name_here",
    "tags": ["tag1", "tag2", "tag3"],
    "summary": "Clear description of what this data shows and its business value",
    "use_cases": ["Specific use case 1", "Specific use case 2", "Specific use case 3"]
}}"""


# ============================================================================
# Data Analysis Prompts
# ============================================================================

ANALYZE_TABLE_STRUCTURE_PROMPT = """Analyze the structure and content of this database table.

Table Name: {table_name}
Columns: {columns}
Sample Data: {sample_data}

Provide insights about:
1. Data quality
2. Potential relationships with other tables
3. Suggested indexes
4. Business context
"""


GENERATE_SQL_FROM_DESCRIPTION_PROMPT = """Generate a SQL query based on this natural language description.

Description: {description}
Available Tables: {available_tables}
Database Schema: {schema}

Generate an optimized SQL query that fulfills the requirement.
"""


# ============================================================================
# Insight Generation Prompts
# ============================================================================

GENERATE_INSIGHTS_PROMPT = """Analyze this data and generate business insights.

Data Summary:
{data_summary}

Column Statistics:
{column_stats}

Generate 3-5 key insights that would be valuable for business decision making.
"""


# ============================================================================
# Helper Functions
# ============================================================================

def build_columns_section(columns: list[dict[str, str]] | None, max_columns: int = 20) -> str:
    """Build the columns section of a prompt."""
    if not columns:
        return ""

    column_list = ", ".join([
        f"{col['name']} ({col['type']})"
        for col in columns[:max_columns]
    ])

    return f"""Columns in the result:
{column_list}
"""


def build_sample_data_section(
    sample_rows: list[dict[str, Any]] | None,
    max_rows: int = 3,
    max_cols_per_row: int = 5
) -> str:
    """Build the sample data section of a prompt."""
    if not sample_rows or len(sample_rows) == 0:
        return ""

    lines = ["Sample data (first {n} rows):".format(n=min(len(sample_rows), max_rows))]

    for i, row in enumerate(sample_rows[:max_rows]):
        # Format row data, limiting to first N columns
        row_items = list(row.items())[:max_cols_per_row]
        row_str = ", ".join([f"{k}: {v}" for k, v in row_items])
        lines.append(f"Row {i+1}: {row_str}")

    return "\n".join(lines)


def build_metadata_prompt(
    sql: str,
    columns: list[dict[str, str]] | None = None,
    sample_rows: list[dict[str, Any]] | None = None,
) -> str:
    """
    Build a complete metadata suggestion prompt.

    Args:
        sql: The SQL query to analyze
        columns: Column definitions from query result
        sample_rows: Sample rows from query result

    Returns:
        Formatted prompt ready for AI
    """
    return SUGGEST_METADATA_PROMPT.format(
        sql_query=sql,
        columns_section=build_columns_section(columns),
        sample_data_section=build_sample_data_section(sample_rows),
    )
