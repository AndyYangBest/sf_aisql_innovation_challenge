"""Snowflake Cortex AI SQL Functions service.

This service provides comprehensive access to all Snowflake Cortex AI SQL functions:
- AI_COMPLETE: LLM text generation and completion
- AI_TRANSCRIBE: Audio transcription
- AI_CLASSIFY: Multi-label classification
- AI_FILTER: Intelligent data filtering
- AI_AGG: AI-powered aggregation across rows
- AI_SENTIMENT: Sentiment analysis
- SUMMARIZE: Text summarization
- PROMPT: Template-based prompting
"""

from typing import Any


class AISQLService:
    """Service for using Snowflake Cortex AI SQL Functions."""

    def __init__(self, snowflake_service):
        """Initialize with Snowflake service."""
        self.sf = snowflake_service

    # ============================================================================
    # AI_COMPLETE: LLM Text Generation
    # ============================================================================

    async def ai_complete(
        self,
        model: str,
        prompt: str,
        response_format: dict[str, Any] | None = None,
    ) -> str:
        """Execute AI_COMPLETE for text generation.

        Args:
            model: LLM model name (claude-3-7-sonnet, mistral-large, pixtral-large, etc.)
            prompt: The prompt text or PROMPT() function result
            response_format: Optional JSON schema for structured output

        Returns:
            Generated text or structured JSON response
        """
        if response_format:
            query = f"""
            SELECT AI_COMPLETE(
                model => '{model}',
                prompt => '{prompt}',
                response_format => {response_format}
            ) as response
            """
        else:
            query = f"""
            SELECT AI_COMPLETE('{model}', '{prompt}') as response
            """

        result = await self.sf.execute_query(query)
        return result[0]["RESPONSE"] if result else ""

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
        SELECT AI_COMPLETE('claude-3-7-sonnet', '{prompt}') as description
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
        SELECT AI_COMPLETE(
            model => 'claude-3-7-sonnet',
            prompt => '{prompt}',
            response_format => {{
                'type': 'json',
                'schema': {{
                    'type': 'object',
                    'properties': {{
                        'category': {{'type': 'string'}},
                        'explanation': {{'type': 'string'}}
                    }}
                }}
            }}
        ) as classification
        """
        result = await self.sf.execute_query(query)
        return result[0] if result else {}

    # ============================================================================
    # AI_TRANSCRIBE: Audio Processing
    # ============================================================================

    async def ai_transcribe(self, audio_file_column: str, table_name: str) -> list[dict[str, Any]]:
        """Transcribe audio files using AI_TRANSCRIBE.

        Args:
            audio_file_column: Column name containing audio files
            table_name: Table containing the audio files

        Returns:
            List of transcription results with text and duration
        """
        query = f"""
        SELECT
            {audio_file_column},
            AI_TRANSCRIBE({audio_file_column}) as transcription,
            transcription['text']::VARCHAR as transcription_text,
            transcription['audio_duration']::FLOAT as audio_duration_seconds
        FROM {table_name}
        LIMIT 100
        """

        result = await self.sf.execute_query(query)
        return result

    # ============================================================================
    # AI_CLASSIFY: Multi-label Classification
    # ============================================================================

    async def ai_classify(
        self,
        content_column: str,
        categories: list[str],
        table_name: str,
        prompt_prefix: str = "Classify the following content: ",
    ) -> list[dict[str, Any]]:
        """Classify content into multiple labels.

        Args:
            content_column: Column containing content to classify
            categories: List of possible categories
            table_name: Source table
            prompt_prefix: Optional prompt prefix

        Returns:
            Classification results with labels and confidence scores
        """
        categories_str = ", ".join(f"'{cat}'" for cat in categories)

        query = f"""
        SELECT
            {content_column},
            AI_CLASSIFY(
                '{prompt_prefix}' || {content_column},
                ARRAY_CONSTRUCT({categories_str})
            ) as classification,
            classification['labels'][0] as primary_label,
            classification['scores'][0] as primary_confidence
        FROM {table_name}
        LIMIT 100
        """

        result = await self.sf.execute_query(query)
        return result

    # ============================================================================
    # AI_FILTER: Intelligent Filtering
    # ============================================================================

    async def ai_filter(
        self,
        filter_condition: str,
        table_name: str,
        columns: list[str],
    ) -> list[dict[str, Any]]:
        """Filter data using AI-based conditions.

        Args:
            filter_condition: Natural language filter condition with placeholders
            table_name: Source table
            columns: Columns to select

        Returns:
            Filtered results
        """
        columns_str = ", ".join(columns)

        query = f"""
        SELECT {columns_str}
        FROM {table_name}
        WHERE AI_FILTER(PROMPT('{filter_condition}', {columns[0]}))
        LIMIT 100
        """

        result = await self.sf.execute_query(query)
        return result

    # ============================================================================
    # AI_AGG: AI-powered Aggregation
    # ============================================================================

    async def ai_aggregate(
        self,
        column_to_aggregate: str,
        aggregation_prompt: str,
        table_name: str,
        group_by: str | None = None,
    ) -> list[dict[str, Any]]:
        """Aggregate data using AI across multiple rows.

        Args:
            column_to_aggregate: Column to aggregate
            aggregation_prompt: Prompt describing the aggregation task
            table_name: Source table
            group_by: Optional column to group by

        Returns:
            Aggregated insights
        """
        if group_by:
            query = f"""
            SELECT
                {group_by},
                COUNT(*) as total_records,
                AI_AGG({column_to_aggregate}, '{aggregation_prompt}') as insights
            FROM {table_name}
            GROUP BY {group_by}
            ORDER BY total_records DESC
            LIMIT 10
            """
        else:
            query = f"""
            SELECT
                COUNT(*) as total_records,
                AI_AGG({column_to_aggregate}, '{aggregation_prompt}') as insights
            FROM {table_name}
            """

        result = await self.sf.execute_query(query)
        return result

    # ============================================================================
    # AI_SENTIMENT: Sentiment Analysis
    # ============================================================================

    async def ai_sentiment(
        self,
        text_column: str,
        table_name: str,
    ) -> list[dict[str, Any]]:
        """Analyze sentiment of text content.

        Args:
            text_column: Column containing text to analyze
            table_name: Source table

        Returns:
            Sentiment analysis results with categories and scores
        """
        query = f"""
        SELECT
            {text_column},
            AI_SENTIMENT({text_column}) as sentiment,
            sentiment['categories'][0]['sentiment']::VARCHAR as primary_sentiment,
            sentiment['categories'][0]['score']::FLOAT as sentiment_score
        FROM {table_name}
        LIMIT 100
        """

        result = await self.sf.execute_query(query)
        return result

    # ============================================================================
    # SUMMARIZE: Text Summarization
    # ============================================================================

    async def summarize(
        self,
        text_column: str,
        table_name: str,
    ) -> list[dict[str, Any]]:
        """Summarize text content.

        Args:
            text_column: Column containing text to summarize
            table_name: Source table

        Returns:
            Summarized text
        """
        query = f"""
        SELECT
            {text_column},
            SNOWFLAKE.CORTEX.SUMMARIZE({text_column}) as summary
        FROM {table_name}
        LIMIT 100
        """

        result = await self.sf.execute_query(query)
        return result

    # ============================================================================
    # Advanced Use Cases
    # ============================================================================

    async def semantic_join(
        self,
        left_table: str,
        right_table: str,
        left_column: str,
        right_column: str,
        join_condition: str,
    ) -> list[dict[str, Any]]:
        """Perform semantic JOIN using AI_FILTER.

        Example: Join customer issues with solution articles using AI understanding.

        Args:
            left_table: First table
            right_table: Second table
            left_column: Column from first table
            right_column: Column from second table
            join_condition: Natural language join condition with {0} and {1} placeholders

        Returns:
            Joined results
        """
        query = f"""
        SELECT
            l.{left_column} as left_content,
            r.{right_column} as right_content
        FROM {left_table} l
        LEFT JOIN {right_table} r
            ON AI_FILTER(PROMPT('{join_condition}', l.{left_column}, r.{right_column}))
        LIMIT 100
        """

        result = await self.sf.execute_query(query)
        return result

    async def extract_structured_data(
        self,
        text_column: str,
        table_name: str,
        extraction_prompt: str,
        schema: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Extract structured data from unstructured text.

        Args:
            text_column: Column containing unstructured text
            table_name: Source table
            extraction_prompt: Prompt describing what to extract
            schema: JSON schema for the extracted data

        Returns:
            Structured extracted data
        """
        query = f"""
        SELECT
            {text_column},
            AI_COMPLETE(
                model => 'claude-3-7-sonnet',
                prompt => PROMPT('{extraction_prompt}', {text_column}),
                response_format => {schema}
            ) as extracted_data
        FROM {table_name}
        LIMIT 100
        """

        result = await self.sf.execute_query(query)
        return result

    # Legacy methods kept for backwards compatibility
    async def suggest_data_quality_checks(
        self, table_name: str, column_profiles: list[dict[str, Any]]
    ) -> list[str]:
        """Suggest data quality checks based on column profiles."""
        profile_summary = str(column_profiles)[:1000]
        prompt = f"""
        Based on these column profiles for table {table_name}:
        {profile_summary}

        Suggest 3-5 important data quality checks to perform.
        Return as a JSON array of strings.
        """

        query = f"""
        SELECT AI_COMPLETE('claude-3-7-sonnet', '{prompt}') as suggestions
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
        SELECT AI_COMPLETE('claude-3-7-sonnet', '{prompt}') as recommendations
        """
        result = await self.sf.execute_query(query)
        return result[0] if result else {}


# Alias for backwards compatibility
AIAnalysisService = AISQLService
