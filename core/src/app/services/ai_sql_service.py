"""Snowflake AI SQL Functions service."""

from typing import Any


class AIAnalysisService:
    """Service for using Snowflake AI SQL Functions."""

    def __init__(self, snowflake_service):
        """Initialize with Snowflake service."""
        self.sf = snowflake_service

    async def generate_column_description(
        self, table_name: str, column_name: str, sample_values: list[Any]
    ) -> str:
        """Use Snowflake AI to generate column description."""
        sample_str = ", ".join(str(v) for v in sample_values[:10])
        prompt = f"""
        Analyze this database column:
        Table: {table_name}
        Column: {column_name}
        Sample values: {sample_str}

        Provide a concise description of what this column represents.
        """

        query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            '{prompt}'
        ) as description
        """
        result = await self.sf.execute_query(query)
        return result[0]["DESCRIPTION"] if result else ""

    async def classify_table_purpose(
        self, table_name: str, columns: list[str]
    ) -> dict[str, Any]:
        """Classify table purpose using Snowflake AI."""
        columns_str = ", ".join(columns)
        prompt = f"""
        Classify the purpose of this table:
        Table: {table_name}
        Columns: {columns_str}

        Classify into one of: transactional, analytical, reference, log, operational
        Also provide a brief explanation.

        Return JSON format: {{"category": "...", "explanation": "..."}}
        """

        query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            '{prompt}'
        ) as classification
        """
        result = await self.sf.execute_query(query)
        return result[0] if result else {}

    async def suggest_data_quality_checks(
        self, table_name: str, column_profiles: list[dict[str, Any]]
    ) -> list[str]:
        """Suggest data quality checks based on column profiles."""
        profile_summary = str(column_profiles)[:1000]  # Limit prompt size
        prompt = f"""
        Based on these column profiles for table {table_name}:
        {profile_summary}

        Suggest 3-5 important data quality checks to perform.
        Return as a JSON array of strings.
        """

        query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            '{prompt}'
        ) as suggestions
        """
        result = await self.sf.execute_query(query)
        return result[0]["SUGGESTIONS"] if result else []

    async def generate_chart_recommendation(
        self, columns: list[dict[str, Any]], table_stats: dict[str, Any]
    ) -> dict[str, Any]:
        """Generate chart recommendations using AI."""
        prompt = f"""
        Given these columns and statistics:
        Columns: {columns}
        Stats: {table_stats}

        Recommend 3-5 visualizations that would be most insightful.
        For each chart, specify:
        - chart_type (bar, line, scatter, histogram, boxplot)
        - x_axis (column name)
        - y_axis (column name or aggregation)
        - rationale (why this chart is valuable)
        - potential_insight (what we might learn)

        Return as JSON array.
        """

        query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            '{prompt}'
        ) as recommendations
        """
        result = await self.sf.execute_query(query)
        return result[0] if result else {}
