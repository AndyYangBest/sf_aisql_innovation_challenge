"""EDA (Exploratory Data Analysis) Agents for Table Assets.

This module implements task-level agents that perform comprehensive EDA on table_assets
stored in PostgreSQL, leveraging Snowflake Cortex AI SQL for intelligent analysis.

Architecture:
    Layer A: SQL Facts (schema, stats, samples from Snowflake)
    Layer B: AI SQL Interpretation (AI_COMPLETE, AI_CLASSIFY, AI_AGGREGATE)
    Layer C: Task-Level Agents (this module)
    Layer D: Strands Workflow Orchestration (eda_workflows.py)

Agents:
    - TableProfilerAgent: Extract schema, statistics, and samples
    - InsightAgent: Generate AI-powered insights and recommendations
    - ChartGeneratorAgent: Create visualization specifications
    - AnnotationDocAgent: Generate documentation and annotations
"""

from typing import Any
import json

from strands import Agent, tool
from sqlalchemy.ext.asyncio import AsyncSession

from ..models.table_asset import TableAsset
from ..services.modular_ai_sql_service import ModularAISQLService
from ..services.snowflake_service import SnowflakeService
from ..services.data_type_detector import DataTypeDetector


# ============================================================================
# Layer A: SQL Facts Extraction Utilities
# ============================================================================


class SnowflakeProfiler:
    """Utility class for extracting table profiles from Snowflake."""

    def __init__(self, snowflake_service: SnowflakeService):
        self.sf = snowflake_service
        self.type_detector = DataTypeDetector()

    async def get_table_profile(
        self,
        table_ref: str,
        sample_size: int = 100,
    ) -> dict[str, Any]:
        """Extract comprehensive table profile from Snowflake.

        Args:
            table_ref: Fully qualified table name or SQL query
            sample_size: Number of sample rows to include

        Returns:
            Profile payload with schema, stats, and samples
        """
        # Determine if table_ref is a query or table name
        is_query = "SELECT" in table_ref.upper() or "FROM" in table_ref.upper()

        if is_query:
            base_query = f"({table_ref})"
        else:
            base_query = table_ref

        # Step 1: Get schema information
        # For queries, we need to infer schema from results
        # For table names, we can query INFORMATION_SCHEMA

        if is_query:
            # Infer schema from query results
            sample_query = f"SELECT * FROM {base_query} LIMIT 1"
            sample = await self.sf.execute_query(sample_query)

            if not sample:
                raise ValueError(f"Query returned no results: {table_ref}")

            # Use DESCRIBE to get proper types
            describe_query = f"DESCRIBE RESULT LAST_QUERY_ID()"
            try:
                schema_info = await self.sf.execute_query(describe_query)
                # Rename columns to match expected format
                schema_info = [
                    {
                        "COLUMN_NAME": row.get("name", row.get("NAME")),
                        "DATA_TYPE": row.get("type", row.get("TYPE")),
                        "IS_NULLABLE": "YES",
                        "ORDINAL_POSITION": idx + 1,
                    }
                    for idx, row in enumerate(schema_info)
                ]
            except Exception:
                # Final fallback: infer from column names
                schema_info = [
                    {
                        "COLUMN_NAME": col,
                        "DATA_TYPE": "VARIANT",
                        "IS_NULLABLE": "YES",
                        "ORDINAL_POSITION": idx + 1,
                    }
                    for idx, col in enumerate(sample[0].keys())
                ]
        else:
            # Query INFORMATION_SCHEMA for table
            schema_query = f"""
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_ref.split('.')[-1]}'
            ORDER BY ORDINAL_POSITION
            """
            schema_info = await self.sf.execute_query(schema_query)

        # Step 2: Get basic statistics
        columns = [col["COLUMN_NAME"] for col in schema_info]
        numeric_columns = [
            col["COLUMN_NAME"]
            for col in schema_info
            if col["DATA_TYPE"] in ("NUMBER", "FLOAT", "INTEGER", "DECIMAL")
        ]
        text_columns = [
            col["COLUMN_NAME"]
            for col in schema_info
            if col["DATA_TYPE"] in ("VARCHAR", "TEXT", "STRING")
        ]
        date_columns = [
            col["COLUMN_NAME"]
            for col in schema_info
            if col["DATA_TYPE"] in ("DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ")
        ]

        # Build statistics query
        stats_parts = [
            "COUNT(*) as total_rows",
        ]

        for col in columns:
            stats_parts.append(f"COUNT(DISTINCT {col}) as {col}_distinct")
            stats_parts.append(f"COUNT_IF({col} IS NULL) as {col}_nulls")

        for col in numeric_columns[:5]:  # Limit to first 5 numeric columns
            stats_parts.extend([
                f"MIN({col}) as {col}_min",
                f"MAX({col}) as {col}_max",
                f"AVG({col}) as {col}_avg",
                f"APPROX_PERCENTILE({col}, 0.5) as {col}_median",
            ])

        stats_query = f"""
        SELECT {', '.join(stats_parts)}
        FROM {base_query}
        """

        stats = await self.sf.execute_query(stats_query)
        stats_dict = stats[0] if stats else {}

        # Step 3: Get sample rows
        sample_query = f"""
        SELECT *
        FROM {base_query}
        QUALIFY ROW_NUMBER() OVER (ORDER BY RANDOM()) <= {sample_size}
        """

        samples = await self.sf.execute_query(sample_query)

        # Step 4: Infer semantic types for each column
        column_type_inferences = []
        for col in schema_info:
            col_name = col["COLUMN_NAME"]
            col_type = col["DATA_TYPE"]

            # Get sample values for this column
            col_samples = [row.get(col_name) for row in samples if col_name in row]

            # Get statistics for this column
            unique_count = stats_dict.get(f"{col_name}_distinct", 0)
            null_count = stats_dict.get(f"{col_name}_nulls", 0)
            total_count = stats_dict.get("TOTAL_ROWS", 0)

            # Infer semantic type
            try:
                type_inference = self.type_detector.infer_column_type(
                    column_name=col_name,
                    sql_type=col_type,
                    sample_values=col_samples,
                    unique_count=unique_count,
                    total_count=total_count,
                    null_count=null_count,
                )
                column_type_inferences.append({
                    "column_name": col_name,
                    "sql_type": col_type,
                    **type_inference,
                })
            except Exception as e:
                # If type inference fails, continue with basic info
                column_type_inferences.append({
                    "column_name": col_name,
                    "sql_type": col_type,
                    "inferred_type": "unknown",
                    "confidence": 0.0,
                    "error": str(e),
                })

        # Step 5: Detect data structure type
        has_time_column = len(date_columns) > 0
        has_entity_column = any("id" in col.lower() or "key" in col.lower() for col in columns)

        # Try to detect if data is sorted by time
        is_sorted_by_time = False
        if has_time_column and date_columns:
            try:
                time_col = date_columns[0]
                check_query = f"""
                WITH first_last AS (
                    (SELECT {time_col} as val FROM {base_query} ORDER BY {time_col} LIMIT 1)
                    UNION ALL
                    (SELECT {time_col} as val FROM {base_query} ORDER BY {time_col} DESC LIMIT 1)
                )
                SELECT MIN(val) as first_val, MAX(val) as last_val FROM first_last
                """
                result = await self.sf.execute_query(check_query)
                if result and len(result) > 0:
                    first_val = result[0].get("FIRST_VAL")
                    last_val = result[0].get("LAST_VAL")
                    is_sorted_by_time = first_val is not None and last_val is not None and first_val < last_val
            except Exception:
                pass

        structure_type = self.type_detector.detect_data_structure(
            has_time_column=has_time_column,
            has_entity_column=has_entity_column,
            is_sorted_by_time=is_sorted_by_time,
        )

        # Step 6: Package into profile payload
        profile = {
            "table_ref": table_ref,
            "schema": schema_info,
            "statistics": stats_dict,
            "samples": samples[:10],  # Include first 10 for context
            "metadata": {
                "total_rows": stats_dict.get("TOTAL_ROWS", 0),
                "total_columns": len(columns),
                "numeric_columns": numeric_columns,
                "text_columns": text_columns,
                "date_columns": date_columns,
                "has_time_series": len(date_columns) > 0,
                # Add semantic type information
                "column_type_inferences": column_type_inferences,
                "data_structure_type": structure_type,
                "has_entity_column": has_entity_column,
                "is_sorted_by_time": is_sorted_by_time,
            },
        }

        return profile


# ============================================================================
# Layer C: Task-Level EDA Agents
# ============================================================================


class TableProfilerAgent(Agent):
    """Agent for extracting comprehensive table profiles.

    This agent extracts schema, statistics, and sample data from tables,
    creating a rich context for downstream analysis.
    """

    def __init__(
        self,
        snowflake_service: SnowflakeService,
        name: str = "Table Profiler Agent",
        **kwargs,
    ):
        self.profiler = SnowflakeProfiler(snowflake_service)
        super().__init__(
            model="claude-3-7-sonnet",
            name=name,
            system_prompt="""You are a data profiling expert. Your job is to extract
comprehensive profiles of tables including schema, statistics, and sample data.
You provide structured, accurate profiles that serve as input for downstream analysis.""",
            **kwargs,
        )

    @tool
    async def profile_table(
        self,
        table_ref: str,
        sample_size: int = 100,
    ) -> dict[str, Any]:
        """Extract comprehensive table profile.

        Args:
            table_ref: Table name or SQL query
            sample_size: Number of sample rows

        Returns:
            Complete table profile with schema, stats, and samples
        """
        profile = await self.profiler.get_table_profile(table_ref, sample_size)
        return {
            "agent": "TableProfilerAgent",
            "profile": profile,
            "summary": f"Profiled table with {profile['metadata']['total_rows']} rows "
            f"and {profile['metadata']['total_columns']} columns",
        }


class InsightAgent(Agent):
    """Agent for generating AI-powered insights from table profiles.

    This agent analyzes table profiles and generates actionable insights,
    identifies data quality issues, and suggests next steps.
    """

    def __init__(
        self,
        ai_sql_service: ModularAISQLService,
        name: str = "Insight Agent",
        **kwargs,
    ):
        self.ai_sql = ai_sql_service
        super().__init__(
            model="claude-3-7-sonnet",
            name=name,
            system_prompt="""You are a data insights expert. You analyze table profiles
and generate actionable insights, identify patterns, detect data quality issues,
and recommend next steps for analysis.""",
            **kwargs,
        )

    @tool
    async def generate_insights(
        self,
        profile: dict[str, Any],
        user_goal: str | None = None,
    ) -> dict[str, Any]:
        """Generate AI-powered insights from table profile using AI_AGG."""
        # Serialize profile once; AI_AGG consumes VARIANT, avoiding long prompt literals
        profile_json = json.dumps(profile)
        profile_json_safe = profile_json.replace("$$", "$ $")  # keep dollar-quoting intact

        instruction = f"""
You are a data analyst. Based on the provided table profile JSON and optional user goal, produce JSON with:
- key_findings: 3-5 bullet findings (array of strings)
- data_quality_issues: array of objects {{issue, severity (low|medium|high), affected_columns[]}}
- recommendations: array of strings
- suggested_sql: array of objects {{description, sql}}
If user_goal is provided, align findings/recs to it. Return ONLY valid JSON.
User Goal: {user_goal or "N/A"}
""".strip()

        query = f"""
        WITH data AS (
            SELECT PARSE_JSON($${profile_json_safe}$$) AS profile_json
        )
        SELECT AI_AGG(
            profile_json,
            $${instruction}$$
        ) AS RESPONSE
        FROM data;
        """

        result = await self.ai_sql.sf.execute_query(query)
        raw_response = result[0]["RESPONSE"] if result else None
        insights: dict[str, Any] = {}
        if isinstance(raw_response, dict):
            insights = raw_response
        elif isinstance(raw_response, list):
            insights = {"key_findings": raw_response}
        elif raw_response:
            try:
                parsed = json.loads(raw_response)
                insights = {"key_findings": parsed} if isinstance(parsed, list) else parsed
            except json.JSONDecodeError:
                insights = {}

        return {
            "agent": "InsightAgent",
            "insights": insights,
            "summary": f"Generated {len(insights.get('key_findings', []))} key findings "
            f"and {len(insights.get('recommendations', []))} recommendations",
        }


class ChartGeneratorAgent(Agent):
    """Agent for generating visualization specifications.

    This agent creates chart specifications (Vega-Lite/ECharts) and the SQL
    queries needed to power those visualizations.
    """

    def __init__(
        self,
        ai_sql_service: ModularAISQLService,
        name: str = "Chart Generator Agent",
        **kwargs,
    ):
        self.ai_sql = ai_sql_service
        super().__init__(
            model="claude-3-7-sonnet",
            name=name,
            system_prompt="""You are a data visualization expert. You create chart
specifications and SQL queries for visualizations based on table profiles and user goals.""",
            **kwargs,
        )

    @tool
    async def generate_charts(
        self,
        profile: dict[str, Any],
        user_goal: str | None = None,
        max_charts: int = 3,
    ) -> dict[str, Any]:
        """Generate visualization specifications and SQL queries.

        Args:
            profile: Table profile from TableProfilerAgent
            user_goal: Optional visualization goal
            max_charts: Maximum number of charts to generate

        Returns:
            Chart specifications with SQL queries and narratives
        """
        profile_json = json.dumps(profile)
        profile_json_safe = profile_json.replace("$$", "$ $")

        instruction = f"""
You are a visualization expert. Based on the table profile JSON and optional user goal, produce up to {max_charts} chart specs as JSON:
- charts: array of objects {{title, chart_type (bar|line|scatter|pie), sql, narrative[]}}
- sql should query the source table, aligned to the profile info
- narrative: 2-3 bullets explaining the chart
User Goal: {user_goal or "N/A"}
Return ONLY valid JSON.
""".strip()

        query = f"""
        WITH data AS (
            SELECT PARSE_JSON($${profile_json_safe}$$) AS profile_json
        )
        SELECT AI_AGG(
            profile_json,
            $${instruction}$$
        ) AS RESPONSE
        FROM data;
        """

        result = await self.ai_sql.sf.execute_query(query)
        raw_response = result[0]["RESPONSE"] if result else None
        charts: dict[str, Any] = {}
        if isinstance(raw_response, dict):
            charts = raw_response
        elif isinstance(raw_response, list):
            charts = {"charts": raw_response}
        elif raw_response:
            try:
                parsed = json.loads(raw_response)
                charts = {"charts": parsed} if isinstance(parsed, list) else parsed
            except json.JSONDecodeError:
                charts = {}

        return {
            "agent": "ChartGeneratorAgent",
            "charts": charts.get("charts", []),
            "summary": f"Generated {len(charts.get('charts', []))} visualization specifications",
        }


class AnnotationDocAgent(Agent):
    """Agent for generating documentation and annotations.

    This agent creates comprehensive documentation including summaries,
    use cases, and annotations for table assets.
    """

    def __init__(
        self,
        ai_sql_service: ModularAISQLService,
        name: str = "Annotation Doc Agent",
        **kwargs,
    ):
        self.ai_sql = ai_sql_service
        super().__init__(
            model="claude-3-7-sonnet",
            name=name,
            system_prompt="""You are a technical documentation expert. You create
clear, comprehensive documentation for data assets including summaries, use cases,
and annotations.""",
            **kwargs,
        )

    @tool
    async def generate_documentation(
        self,
        profile: dict[str, Any],
        insights: dict[str, Any] | None = None,
        charts: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Generate comprehensive documentation for a table asset.

        Args:
            profile: Table profile from TableProfilerAgent
            insights: Optional insights from InsightAgent
            charts: Optional charts from ChartGeneratorAgent

        Returns:
            Structured documentation with summary, use cases, and annotations
        """
        data_payload = {
            "table_ref": profile.get("table_ref"),
            "metadata": profile.get("metadata", {}),
            "insights": insights or {},
            "charts": charts or [],
        }
        payload_json = json.dumps(data_payload)
        payload_json_safe = payload_json.replace("$$", "$ $")

        instruction = """
You are a documentation writer. Based on the table profile, insights, and charts, produce JSON with:
- summary: 2-3 sentences
- use_cases: 3-5 items
- tags: 5-8 keywords
- markdown_doc: full markdown covering table description, key insights, and how to use the charts
Return ONLY valid JSON with those fields.
""".strip()

        query = f"""
        WITH data AS (
            SELECT PARSE_JSON($${payload_json_safe}$$) AS payload_json
        )
        SELECT AI_AGG(
            payload_json,
            $${instruction}$$
        ) AS RESPONSE
        FROM data;
        """

        result = await self.ai_sql.sf.execute_query(query)
        raw_response = result[0]["RESPONSE"] if result else None
        if isinstance(raw_response, dict):
            documentation = raw_response
        elif raw_response:
            try:
                documentation = json.loads(raw_response)
            except json.JSONDecodeError:
                documentation = {}
        else:
            documentation = {}

        return {
            "agent": "AnnotationDocAgent",
            "documentation": documentation,
            "summary": f"Generated documentation with {len(documentation.get('use_cases', []))} use cases",
        }


# ============================================================================
# Factory Functions
# ============================================================================


def create_table_profiler_agent(snowflake_service: SnowflakeService) -> TableProfilerAgent:
    """Create a TableProfilerAgent instance."""
    return TableProfilerAgent(snowflake_service)


def create_insight_agent(ai_sql_service: ModularAISQLService) -> InsightAgent:
    """Create an InsightAgent instance."""
    return InsightAgent(ai_sql_service)


def create_chart_generator_agent(ai_sql_service: ModularAISQLService) -> ChartGeneratorAgent:
    """Create a ChartGeneratorAgent instance."""
    return ChartGeneratorAgent(ai_sql_service)


def create_annotation_doc_agent(ai_sql_service: ModularAISQLService) -> AnnotationDocAgent:
    """Create an AnnotationDocAgent instance."""
    return AnnotationDocAgent(ai_sql_service)
