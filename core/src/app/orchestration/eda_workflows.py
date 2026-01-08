"""EDA Workflow Orchestration using Strands Agents Workflow Tool.

This module implements Layer D: Strands Workflow Orchestration for EDA tasks.
It uses the Strands workflow tool to manage multi-agent task execution with
automatic dependency resolution, parallel execution, and state management.

Workflows:
    - EDA_OVERVIEW: Comprehensive analysis (profile → insights → charts → docs)
    - EDA_TIME_SERIES: Time-series focused analysis
    - EDA_DATA_QUALITY: Data quality focused analysis

Router Logic:
    - Rule-based routing (has date columns? → TIME_SERIES)
    - AI-powered routing (AI_CLASSIFY for complex decisions)
"""

from typing import Any, Literal
import json
import uuid
import os
from pathlib import Path
import ast
import operator

from strands import Agent, tool
from strands_tools import workflow
from strands.models.openai import OpenAIModel
from sqlalchemy.ext.asyncio import AsyncSession

from ..models.table_asset import TableAsset
from ..services.modular_ai_sql_service import ModularAISQLService
from ..services.snowflake_service import SnowflakeService
from ..services.data_type_detector import DataTypeDetector
from ..services.eda_workflow_persistence import EDAWorkflowPersistenceService
from .eda_agents import SnowflakeProfiler
from .eda_hooks import create_default_eda_hooks


# ============================================================================
# Workflow Type Definitions
# ============================================================================

WorkflowType = Literal["EDA_OVERVIEW", "EDA_TIME_SERIES", "EDA_DATA_QUALITY"]


# ============================================================================
# Workflow Router
# ============================================================================


class EDAWorkflowRouter:
    """Routes table analysis requests to appropriate workflows.

    Uses a combination of:
    1. Rule-based routing (fast, deterministic)
    2. AI-powered routing (flexible, handles edge cases)
    """

    def __init__(
        self,
        snowflake_service: SnowflakeService,
        ai_sql_service: ModularAISQLService,
    ):
        self.sf = snowflake_service
        self.ai_sql = ai_sql_service

    async def route_workflow(
        self,
        table_asset: TableAsset,
        user_intent: str | None = None,
    ) -> WorkflowType:
        """Determine which workflow to use for a table asset.

        Args:
            table_asset: The table asset to analyze
            user_intent: Optional user intent (e.g., "check quality", "find trends")

        Returns:
            Workflow type to use
        """
        # Step 1: Quick rule-based checks
        if user_intent:
            intent_lower = user_intent.lower()
            if any(kw in intent_lower for kw in ["quality", "clean", "validate", "check"]):
                return "EDA_DATA_QUALITY"
            if any(kw in intent_lower for kw in ["trend", "time", "series", "temporal"]):
                return "EDA_TIME_SERIES"

        # Step 2: Analyze table schema to detect patterns
        try:
            # Check for date/timestamp columns
            date_check_query = f"""
            SELECT COUNT(*) as date_col_count
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_asset.name}'
            AND DATA_TYPE IN ('DATE', 'TIMESTAMP', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ')
            """
            date_result = await self.sf.execute_query(date_check_query)
            has_date_columns = date_result[0].get("DATE_COL_COUNT", 0) > 0

            if has_date_columns:
                return "EDA_TIME_SERIES"

        except Exception:
            pass

        # Default: EDA_OVERVIEW
        return "EDA_OVERVIEW"


# ============================================================================
# Strands Workflow-Based EDA Orchestrator
# ============================================================================


class EDAWorkflowTools:
    """Tool set exposed to Strands workflows for table analysis."""

    def __init__(self, snowflake_service: SnowflakeService):
        self.sf = snowflake_service
        self.profiler = SnowflakeProfiler(snowflake_service)
        self.type_detector = DataTypeDetector()
        repo_root = Path(__file__).resolve().parents[4]
        self.output_dir = repo_root / "output"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @tool
    async def sql(self, query: str) -> dict[str, Any]:
        """Execute a read-only SQL query against Snowflake.

        Args:
            query: SQL query to run (SELECT/WITH/SHOW/DESCRIBE/EXPLAIN only)
        """
        normalized = query.strip().upper()
        allowed_prefixes = ("SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN")
        if not normalized.startswith(allowed_prefixes):
            return {
                "error": "Only read-only queries are allowed (SELECT/WITH/SHOW/DESCRIBE/EXPLAIN).",
                "query": query,
            }

        results = await self.sf.execute_query(query)
        return {
            "row_count": len(results),
            "rows": results,
        }

    @tool
    async def profile_table(self, table_ref: str, sample_size: int = 100) -> dict[str, Any]:
        """Generate a structured table profile (schema, stats, samples).

        Args:
            table_ref: Fully qualified table name or SQL query
            sample_size: Number of sample rows
        """
        profile = await self.profiler.get_table_profile(table_ref, sample_size)
        return profile

    @tool
    def calculator(self, expression: str) -> dict[str, Any]:
        """Evaluate a simple numeric expression safely."""
        try:
            node = ast.parse(expression, mode="eval")
            value = self._safe_eval(node.body)
            return {"expression": expression, "value": value}
        except Exception as exc:
            return {"error": f"calculator error: {exc}"}

    @tool
    def python_repl(self, code: str) -> dict[str, Any]:
        """Restricted Python execution (expressions only)."""
        try:
            node = ast.parse(code, mode="eval")
            value = self._safe_eval(node.body)
            return {"result": value}
        except Exception as exc:
            return {"error": f"python_repl error: {exc}"}

    @tool
    def file_write(self, path: str, content: str) -> dict[str, Any]:
        """Write content to a file under the output/ directory."""
        target = Path(path)
        if not target.is_absolute():
            target = self.output_dir / target

        try:
            target.resolve().relative_to(self.output_dir.resolve())
        except ValueError:
            return {"error": "file_write only supports paths under output/."}

        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        return {"path": str(target), "bytes": len(content)}

    @tool
    def infer_column_type(
        self,
        column_name: str,
        sql_type: str,
        sample_values: list[Any],
        unique_count: int,
        total_count: int,
        null_count: int,
    ) -> dict[str, Any]:
        """Infer the semantic data type of a column based on evidence.

        This tool analyzes column characteristics to determine its semantic type
        (e.g., identifier, categorical, continuous numeric, datetime, etc.).

        Args:
            column_name: Name of the column (used for semantic hints)
            sql_type: SQL data type (NUMBER, VARCHAR, etc.)
            sample_values: Sample values from the column (list of actual values)
            unique_count: Number of unique values in the column
            total_count: Total number of rows
            null_count: Number of null values

        Returns:
            Dictionary with:
            - inferred_type: Semantic type (e.g., "identifier", "nominal_categorical")
            - confidence: Confidence score (0.0-1.0)
            - evidence: Evidence chain used for inference
            - recommendations: Actionable recommendations for handling this type

        Example:
            result = infer_column_type(
                column_name="customer_id",
                sql_type="NUMBER",
                sample_values=[1, 2, 3, 4, 5],
                unique_count=5,
                total_count=5,
                null_count=0
            )
            # Returns: {"inferred_type": "identifier", "confidence": 0.9, ...}
        """
        result = self.type_detector.infer_column_type(
            column_name=column_name,
            sql_type=sql_type,
            sample_values=sample_values,
            unique_count=unique_count,
            total_count=total_count,
            null_count=null_count,
        )
        return result

    @tool
    def detect_data_structure(
        self,
        has_time_column: bool,
        has_entity_column: bool,
        is_sorted_by_time: bool,
    ) -> dict[str, Any]:
        """Detect the overall data structure type of a dataset.

        This tool determines whether data is cross-sectional, time-series,
        panel data, or event sequence based on structural characteristics.

        Args:
            has_time_column: Whether dataset has temporal columns (date/timestamp)
            has_entity_column: Whether dataset has entity/ID columns
            is_sorted_by_time: Whether data is sorted chronologically

        Returns:
            Dictionary with:
            - structure_type: Type of data structure (cross_sectional, time_series, panel, event_sequence)
            - description: Human-readable description
            - analysis_approach: Recommended analysis methods
            - considerations: Important considerations for this structure type

        Example:
            result = detect_data_structure(
                has_time_column=True,
                has_entity_column=True,
                is_sorted_by_time=True
            )
            # Returns: {"structure_type": "panel", ...}
        """
        structure_type = self.type_detector.detect_data_structure(
            has_time_column=has_time_column,
            has_entity_column=has_entity_column,
            is_sorted_by_time=is_sorted_by_time,
        )
        return {
            "structure_type": structure_type,
            "description": f"Detected as {structure_type} data structure",
        }

    @tool
    def suggest_sampling_strategy(
        self,
        total_rows: int,
        has_time_column: bool = False,
        target_sample_size: int = 1000,
    ) -> dict[str, Any]:
        """Suggest optimal sampling strategy for data analysis.

        This tool recommends the best sampling approach based on data size
        and characteristics, avoiding problematic methods like Fibonacci sampling.

        Args:
            total_rows: Total number of rows in the dataset
            has_time_column: Whether dataset has time columns (affects strategy)
            target_sample_size: Desired sample size (default: 1000)

        Returns:
            Dictionary with:
            - strategy: Recommended strategy (full_scan, head_tail, systematic, random)
            - reason: Explanation for the recommendation
            - parameters: Strategy-specific parameters (e.g., sample_every_n)

        Example:
            result = suggest_sampling_strategy(
                total_rows=1000000,
                has_time_column=True,
                target_sample_size=1000
            )
            # Returns: {"strategy": "head_tail", "reason": "Time series data...", ...}
        """
        strategy_info = self.type_detector.suggest_sampling_strategy(
            total_rows=total_rows,
            has_time_column=has_time_column,
            target_sample_size=target_sample_size,
        )
        return strategy_info

    def _safe_eval(self, node):
        operators = {
            ast.Add: operator.add,
            ast.Sub: operator.sub,
            ast.Mult: operator.mul,
            ast.Div: operator.truediv,
            ast.FloorDiv: operator.floordiv,
            ast.Mod: operator.mod,
            ast.Pow: operator.pow,
            ast.USub: operator.neg,
            ast.UAdd: operator.pos,
        }

        if isinstance(node, ast.BinOp) and type(node.op) in operators:
            return operators[type(node.op)](
                self._safe_eval(node.left),
                self._safe_eval(node.right),
            )
        if isinstance(node, ast.UnaryOp) and type(node.op) in operators:
            return operators[type(node.op)](self._safe_eval(node.operand))
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return node.value
        raise ValueError("Unsupported expression")


class EDAOrchestrator:
    """Main orchestrator for EDA workflows using Strands workflow tool.

    This class:
    1. Routes requests to appropriate workflows
    2. Creates workflow tasks with proper dependencies
    3. Executes workflows using Strands workflow tool
    4. Returns structured results
    """

    def __init__(
        self,
        snowflake_service: SnowflakeService,
        ai_sql_service: ModularAISQLService,
        enable_hooks: bool = True,
        db: AsyncSession | None = None,  # Optional database session for persistence
    ):
        self.sf = snowflake_service
        self.ai_sql = ai_sql_service
        self.router = EDAWorkflowRouter(snowflake_service, ai_sql_service)
        self.db = db  # Store database session

        # Create hooks for monitoring and logging
        hooks = create_default_eda_hooks() if enable_hooks else []

        # Prefer OpenAI provider if API key is available
        openai_key = os.getenv("OPENAI_API_KEY")
        openai_model_id = (
            os.getenv("STRANDS_MODEL_ID")
            or os.getenv("OPENAI_MODEL_ID")
            or "gpt-4o-mini"
        )
        openai_model = (
            OpenAIModel(
                model_id=openai_model_id,
                params={
                    "max_tokens": 8192,  # Increased from 2048 to handle large profiles
                    "temperature": 0.2,
                },
            )
            if openai_key
            else None
        )

        workflow_tools = EDAWorkflowTools(snowflake_service)

        # Create workflow coordinator agent with hooks
        self.coordinator = Agent(
            name="EDA Workflow Coordinator",
            system_prompt="""You are an EDA workflow coordinator. You manage the execution
                                of exploratory data analysis workflows by coordinating multiple specialized agents.

                                You have access to powerful data type detection tools:
                                - infer_column_type: Infer semantic types of columns (identifier, categorical, numeric, etc.)
                                - detect_data_structure: Detect if data is time-series, panel, cross-sectional, etc.
                                - suggest_sampling_strategy: Get optimal sampling strategy for large datasets

                                Use these tools to enhance your analysis and provide better insights.""",
            tools=[
                workflow,
                workflow_tools.infer_column_type,
                workflow_tools.detect_data_structure,
                workflow_tools.suggest_sampling_strategy,
            ],
            hooks=hooks,
            model=openai_model,
        )

    def _create_eda_overview_tasks(
        self,
        table_asset: TableAsset,
        user_goal: str | None = None,
    ) -> list[dict[str, Any]]:
        """Create tasks for EDA_OVERVIEW workflow.

        Workflow: Profile → Insights → Charts → Documentation
        """
        # Store context for tasks
        context = {
            "table_ref": table_asset.source_sql,
            "table_name": table_asset.name,
            "user_goal": user_goal or "Comprehensive EDA analysis",
        }

        tasks = [
            {
                "task_id": "profile_table",
                "description": f"Profile the table '{table_asset.name}' to extract schema, statistics, sample data, AND semantic type inference for each column. Table SQL: {table_asset.source_sql}",
                "system_prompt": """You are a data profiling expert. Extract comprehensive table profiles including:

                1. Schema (column names and SQL types)
                2. Statistics (row count, null counts, distinct values, cardinality)
                3. Sample data
                4. **Semantic type inference** - The profile_table tool now automatically includes:
                - column_type_inferences: Semantic types for each column (identifier, categorical, numeric, etc.)
                - data_structure_type: Overall data structure (time_series, panel, cross_sectional, etc.)

                The profile you receive will already contain semantic type information in metadata.column_type_inferences.
                Each column inference includes:
                - inferred_type: Semantic type (e.g., "identifier", "nominal_categorical", "continuous_numeric")
                - confidence: Confidence score (0.0-1.0)
                - recommendations: How to handle this type

                Use the profile_table tool to fetch the complete profile with type inference.
                Return results as JSON with keys: schema, statistics, samples, metadata (including column_type_inferences and data_structure_type).""",
                "priority": 5,
                "dependencies": [],
            },
            {
                "task_id": "generate_insights",
                "description": f"Analyze the table profile INCLUDING semantic type information and generate insights about data patterns, quality issues, and recommendations. User goal: {user_goal or 'general analysis'}",
                "system_prompt": """You are a data insights expert. Analyze table profiles with semantic type information.

The profile you receive includes column_type_inferences with semantic types for each column.
Use this information to:
- Identify mismatched types (e.g., numeric data stored as text)
- Detect identifier columns that shouldn't be used in calculations
- Find categorical columns that need encoding
- Spot time-series data that needs temporal analysis
- Detect high-cardinality columns (likely IDs)
- Identify low-cardinality columns (good for grouping)

Pay special attention to:
- Columns with low confidence scores (may need manual review)
- Recommendations from type inference
- Data structure type (time_series, panel, etc.) for analysis approach

Return JSON with keys: key_findings (array), data_quality_issues (array of objects with issue and severity), recommendations (array).""",
                "priority": 4,
                "dependencies": ["profile_table"],
            },
            {
                "task_id": "generate_charts",
                "description": f"Create visualization specifications for the table. Generate 2-3 appropriate charts based on the data types (use semantic type information) and user goal: {user_goal or 'general visualization'}",
                "system_prompt": """You are a data visualization expert. Create chart specifications using semantic type information.

The profile includes column_type_inferences that tell you the semantic type of each column:
- identifier: Don't visualize (use as labels/keys)
- categorical: Use for grouping (bar charts, pie charts)
- continuous_numeric: Use for distributions (histograms, scatter plots)
- discrete_numeric: Use for counts (bar charts)
- datetime: Use for time-series (line charts, time plots)

Choose appropriate chart types based on semantic types:
- Categorical × Numeric → Bar chart
- Numeric × Numeric → Scatter plot
- Datetime × Numeric → Line chart
- Categorical distribution → Pie chart

Return JSON with key: charts (array of objects with title, chart_type, sql, narrative).""",
                "priority": 3,
                "dependencies": ["profile_table"],
            },
            {
                "task_id": "generate_documentation",
                "description": f"Generate comprehensive documentation for the table including summary, use cases, tags, and markdown documentation. Incorporate semantic type information.",
                "system_prompt": """You are a technical documentation expert. Create clear documentation incorporating semantic type information.

The profile includes:
- column_type_inferences: Semantic types for each column
- data_structure_type: Overall data structure (time_series, panel, etc.)

Use this information to:
- Describe the table's purpose based on column types
- Suggest use cases based on data structure type
- Tag the table appropriately (e.g., "time-series", "categorical-data", "high-dimensional")
- Document key columns and their semantic meanings
- Include recommendations from type inference

Return JSON with keys: summary (2-3 sentences), use_cases (array), tags (array), markdown_doc.""",
                "priority": 2,
                "dependencies": ["generate_insights", "generate_charts"],
            },
        ]

        return tasks

    def _create_eda_time_series_tasks(
        self,
        table_asset: TableAsset,
        user_goal: str | None = None,
    ) -> list[dict[str, Any]]:
        """Create tasks for EDA_TIME_SERIES workflow.

        Workflow: Profile → Charts (time-focused) → Insights (temporal) → Documentation
        """
        tasks = [
            {
                "task_id": "profile_table",
                "description": f"Profile the table '{table_asset.name}' with focus on time-series columns and semantic type inference. Table SQL: {table_asset.source_sql}",
                "system_prompt": """You are a data profiling expert specializing in time-series data.
Extract schema, statistics, and identify date/timestamp columns. The profile_table tool automatically includes:
- column_type_inferences: Semantic types (will identify datetime, temporal_cyclic types)
- data_structure_type: Should detect as time_series or panel

Pay special attention to temporal columns and their semantic types.
Use the profile_table tool to fetch real data; do not fabricate.
Return JSON with schema, statistics, samples, metadata (including type inferences).""",
                "priority": 5,
                "dependencies": [],
            },
            {
                "task_id": "generate_charts",
                "description": f"Create time-series visualizations showing trends, patterns, and temporal distributions using semantic type information. User goal: {user_goal or 'time-series analysis'}",
                "system_prompt": """You are a time-series visualization expert. Create 3-4 charts
focusing on temporal patterns, trends, and time-based distributions.

Use the column_type_inferences to identify:
- datetime/timestamp columns: Use as x-axis for time-series plots
- temporal_cyclic columns: Use for seasonality analysis
- continuous_numeric columns: Use as y-axis for trends

Prioritize line charts and time-series plots.
Return JSON with charts array.""",
                "priority": 4,
                "dependencies": ["profile_table"],
            },
            {
                "task_id": "generate_insights",
                "description": f"Analyze temporal patterns, trends, seasonality, and anomalies in the time-series data using semantic type information.",
                "system_prompt": """You are a time-series analysis expert. Identify trends, seasonality,
anomalies, and temporal patterns.

Use the semantic type information to:
- Focus on datetime columns for temporal analysis
- Identify cyclic patterns in temporal_cyclic columns
- Detect time-dependent relationships
- Check if data_structure_type is time_series or panel

Return JSON with key_findings, data_quality_issues, recommendations.""",
                "priority": 3,
                "dependencies": ["profile_table", "generate_charts"],
            },
            {
                "task_id": "generate_documentation",
                "description": f"Generate time-series focused documentation highlighting temporal insights and semantic types.",
                "system_prompt": """You are a technical documentation expert specializing in time-series data.
Create documentation emphasizing temporal patterns and trends.

Incorporate semantic type information:
- Highlight datetime columns and their ranges
- Document temporal_cyclic patterns
- Explain data_structure_type (time_series vs panel)
- Include time-series specific recommendations

Return JSON with summary, use_cases, tags (include "time-series"), markdown_doc.""",
                "priority": 2,
                "dependencies": ["generate_insights"],
            },
        ]

        return tasks

    def _create_eda_data_quality_tasks(
        self,
        table_asset: TableAsset,
        user_goal: str | None = None,
    ) -> list[dict[str, Any]]:
        """Create tasks for EDA_DATA_QUALITY workflow.

        Workflow: Profile → Insights (quality-focused) → Documentation (quality report)
        """
        tasks = [
            {
                "task_id": "profile_table",
                "description": f"Profile the table '{table_asset.name}' with focus on data quality metrics and semantic type inference. Table SQL: {table_asset.source_sql}",
                "system_prompt": """You are a data quality profiling expert. Extract schema, statistics
with emphasis on null counts, distinct values, and potential quality issues. The profile_table tool automatically includes:
- column_type_inferences: Semantic types with confidence scores
- data_structure_type: Overall data structure

Pay attention to:
- Low confidence scores (may indicate quality issues)
- Type mismatches (e.g., numeric stored as text)
- Identifier columns with duplicates

Use the profile_table tool to fetch real data; do not fabricate.
Return JSON with schema, statistics, samples, metadata (including type inferences).""",
                "priority": 5,
                "dependencies": [],
            },
            {
                "task_id": "generate_insights",
                "description": f"Perform comprehensive data quality analysis identifying issues, anomalies, and providing recommendations using semantic type information. User goal: {user_goal or 'data quality check'}",
                "system_prompt": """You are a data quality expert. Identify quality issues including
nulls, duplicates, outliers, inconsistencies, and data integrity problems.

Use semantic type information to detect quality issues:
- identifier columns with duplicates (should be unique)
- categorical columns with too many categories (data entry errors?)
- numeric columns stored as text (type mismatch)
- datetime columns with invalid dates
- Low confidence scores (ambiguous types, needs review)

Assign severity levels (low/medium/high) based on:
- Impact on analysis
- Data integrity violations
- Type confidence scores

Return JSON with key_findings, data_quality_issues (with severity), recommendations.""",
                "priority": 4,
                "dependencies": ["profile_table"],
            },
            {
                "task_id": "generate_documentation",
                "description": f"Generate a data quality report with findings, issues, and remediation recommendations incorporating semantic type information.",
                "system_prompt": """You are a data quality documentation expert. Create a quality report
highlighting issues, their impact, and remediation steps.

Incorporate semantic type information:
- Document type mismatches and their implications
- Explain quality issues in context of semantic types
- Provide type-specific remediation recommendations
- Include confidence scores for ambiguous columns

Return JSON with summary, use_cases (focus on quality improvement), tags (include "data-quality"), markdown_doc.""",
                "priority": 3,
                "dependencies": ["generate_insights"],
            },
        ]

        return tasks

    async def run_eda(
        self,
        table_asset: TableAsset,
        user_intent: str | None = None,
        workflow_type: WorkflowType | None = None,
        user_id: int | None = None,
    ) -> dict[str, Any]:
        """Run EDA on a table asset using Strands workflow tool.

        Args:
            table_asset: Table asset to analyze
            user_intent: Optional user intent/goal
            workflow_type: Optional explicit workflow type (overrides routing)
            user_id: Optional user ID who triggered the workflow

        Returns:
            Complete EDA results with all artifacts
        """
        # Route to appropriate workflow
        if workflow_type is None:
            workflow_type = await self.router.route_workflow(table_asset, user_intent)

        print(f"\n{'='*60}")
        print(f"🔬 Starting EDA Workflow: {workflow_type}")
        print(f"📊 Table: {table_asset.name}")
        if user_intent:
            print(f"🎯 Goal: {user_intent}")
        print(f"{'='*60}\n")

        # Create workflow ID
        workflow_id = f"eda_{table_asset.id}_{uuid.uuid4().hex[:8]}"

        # Select tasks based on workflow type
        if workflow_type == "EDA_OVERVIEW":
            tasks = self._create_eda_overview_tasks(table_asset, user_intent)
        elif workflow_type == "EDA_TIME_SERIES":
            tasks = self._create_eda_time_series_tasks(table_asset, user_intent)
        elif workflow_type == "EDA_DATA_QUALITY":
            tasks = self._create_eda_data_quality_tasks(table_asset, user_intent)
        else:
            raise ValueError(f"Unknown workflow type: {workflow_type}")

        # Create database record if db session is available
        persistence = None
        if self.db:
            persistence = EDAWorkflowPersistenceService(self.db)
            try:
                await persistence.create_execution(
                    workflow_id=workflow_id,
                    workflow_type=workflow_type,
                    table_asset_id=table_asset.id,
                    user_intent=user_intent,
                    user_id=user_id,
                    tasks_total=len(tasks),
                )
                print(f"[Persistence] ✓ Created database record for workflow")
            except Exception as e:
                print(f"[Persistence] Warning: Could not create database record: {e}")

        # Create workflow using Strands workflow tool
        print(f"[Strands Workflow] Creating workflow '{workflow_id}' with {len(tasks)} tasks...")

        try:
            # Directly call the workflow tool
            create_result = self.coordinator.tool.workflow(
                action="create",
                workflow_id=workflow_id,
                tasks=tasks,
            )
            print(f"[Strands Workflow] ✓ Workflow created")

            # Start workflow execution
            print(f"[Strands Workflow] Starting workflow execution...")
            start_result = self.coordinator.tool.workflow(
                action="start",
                workflow_id=workflow_id,
            )
            print(f"[Strands Workflow] ✓ Workflow started")

            # Get workflow status and results
            print(f"[Strands Workflow] Retrieving workflow results...")
            status = self.coordinator.tool.workflow(
                action="status",
                workflow_id=workflow_id,
            )

            # Read workflow file directly for results
            workflow_file = Path.home() / ".strands" / "workflows" / f"{workflow_id}.json"
            workflow_data = None
            if workflow_file.exists():
                try:
                    with open(workflow_file, "r") as f:
                        workflow_data = json.load(f)
                except Exception as e:
                    print(f"[Warning] Could not read workflow file: {e}")

            print(f"\n{'='*60}")
            print(f"✅ EDA Workflow Complete!")
            print(f"{'='*60}\n")

            # Parse and structure results
            artifacts = self._extract_artifacts_from_workflow_data(workflow_data) if workflow_data else {}
            summary = self._generate_summary_from_workflow_data(workflow_data) if workflow_data else {}

            # Update database record with results
            if persistence:
                try:
                    await persistence.complete_execution(
                        workflow_id=workflow_id,
                        artifacts=artifacts,
                        summary=summary,
                    )
                    print(f"[Persistence] ✓ Updated database record with results")
                except Exception as e:
                    print(f"[Persistence] Warning: Could not update database record: {e}")

            results = {
                "workflow": workflow_type,
                "workflow_id": workflow_id,
                "table_asset_id": table_asset.id,
                "table_name": table_asset.name,
                "status": status,
                "artifacts": artifacts,
                "summary": summary,
            }

            return results

        except Exception as e:
            # Mark workflow as failed in database
            if persistence:
                try:
                    await persistence.fail_execution(
                        workflow_id=workflow_id,
                        error_message=str(e),
                    )
                    print(f"[Persistence] ✓ Marked workflow as failed in database")
                except Exception as persist_error:
                    print(f"[Persistence] Warning: Could not mark workflow as failed: {persist_error}")
            raise

    def _extract_artifacts_from_workflow_data(self, workflow_data: dict[str, Any]) -> dict[str, Any]:
        """Extract artifacts from workflow data file."""
        artifacts = {}

        if not workflow_data:
            return artifacts

        # Extract from task_results
        task_results = workflow_data.get("task_results", {})
        for task_id, task_data in task_results.items():
            if task_data.get("status") == "completed" and "result" in task_data:
                # Parse the result - it's a list with text content
                result_list = task_data["result"]
                if isinstance(result_list, list) and len(result_list) > 0:
                    # Extract text and try to parse JSON from it
                    text_content = result_list[0].get("text", "")
                    artifacts[task_id] = {
                        "status": "completed",
                        "text": text_content,
                        "metrics": task_data.get("metrics", ""),
                    }

        return artifacts

    def _generate_summary_from_workflow_data(self, workflow_data: dict[str, Any]) -> dict[str, Any]:
        """Generate summary from workflow data file."""
        summary = {
            "completed": False,
            "progress": 0,
            "tasks_completed": 0,
            "tasks_total": 0,
        }

        if not workflow_data:
            return summary

        # Check workflow status
        summary["completed"] = workflow_data.get("status") == "completed"

        # Count tasks
        tasks = workflow_data.get("tasks", [])
        task_results = workflow_data.get("task_results", {})

        summary["tasks_total"] = len(tasks)
        summary["tasks_completed"] = sum(
            1 for task_data in task_results.values()
            if task_data.get("status") == "completed"
        )

        # Calculate progress
        if summary["tasks_total"] > 0:
            summary["progress"] = int((summary["tasks_completed"] / summary["tasks_total"]) * 100)

        return summary


# ============================================================================
# Factory Function
# ============================================================================


def create_eda_orchestrator(
    snowflake_service: SnowflakeService,
    ai_sql_service: ModularAISQLService,
    db: AsyncSession | None = None,
) -> EDAOrchestrator:
    """Create an EDA orchestrator instance using Strands workflow tool.

    Args:
        snowflake_service: Snowflake service instance
        ai_sql_service: AI SQL service instance
        db: Optional database session for persistence

    Usage:
        orchestrator = create_eda_orchestrator(sf_service, ai_sql_service, db=db)
        results = await orchestrator.run_eda(table_asset, user_intent="find trends")
    """
    return EDAOrchestrator(snowflake_service, ai_sql_service, db=db)
