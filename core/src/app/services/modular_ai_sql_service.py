"""Refactored Snowflake Cortex AI SQL service using modular builders.

This service leverages the composable query builders for cleaner, more maintainable code.
"""

from typing import Any

from .ai_sql_builders import (
    AIAggregateBuilder,
    AIClassifyBuilder,
    AICompleteBuilder,
    AIFilterBuilder,
    AISentimentBuilder,
    AITranscribeBuilder,
    SelectQueryBuilder,
    SemanticJoinBuilder,
    StructuredExtractionBuilder,
    SummarizeBuilder,
    ai_aggregate,
    ai_classify,
    ai_complete,
    ai_filter,
    ai_sentiment,
    ai_transcribe,
    extract_structured,
    select,
    semantic_join,
    summarize,
)


class ModularAISQLService:
    """Modular AI SQL service using composable query builders.

    This service provides methods that use the modular builders,
    making it easy to create custom AISQL queries by composing builders.
    """

    def __init__(self, snowflake_service):
        """Initialize with Snowflake service."""
        self.sf = snowflake_service

    # ============================================================================
    # Core AISQL Functions
    # ============================================================================

    async def ai_complete(
        self,
        model: str,
        prompt: str,
        response_format: dict[str, Any] | None = None,
    ) -> str:
        """Execute AI_COMPLETE for text generation.

        Example:
            result = await service.ai_complete(
                'claude-3-7-sonnet',
                'What is the capital of France?'
            )
        """
        builder = ai_complete(model, prompt)
        if response_format:
            builder = builder.with_response_format(response_format)

        query = f"SELECT {builder.build()} as response"
        result = await self.sf.execute_query(query)
        return result[0]["RESPONSE"] if result else ""

    async def ai_classify(
        self,
        content_column: str,
        categories: list[str],
        table_name: str,
        prompt_prefix: str | None = None,
    ) -> list[dict[str, Any]]:
        """Classify content into categories.

        Example:
            results = await service.ai_classify(
                'product_name',
                ['Electronics', 'Clothing', 'Food'],
                'products'
            )
        """
        classifier = ai_classify(content_column, categories)
        if prompt_prefix:
            classifier = classifier.with_prompt_prefix(prompt_prefix)

        query = (
            select(table_name)
            .select(content_column)
            .select_ai_function(classifier, "classification")
            .select("classification['labels'][0]", "primary_label")
            .select("classification['scores'][0]", "primary_confidence")
            .limit(100)
            .build()
        )

        return await self.sf.execute_query(query)

    async def ai_filter(
        self,
        filter_condition: str,
        table_name: str,
        columns: list[str],
    ) -> list[dict[str, Any]]:
        """Filter data using AI-based conditions.

        Example:
            results = await service.ai_filter(
                'product description mentions sustainability',
                'products',
                ['product_name', 'description']
            )
        """
        filter_builder = ai_filter(filter_condition, columns[0])

        query = select(table_name).select(*columns).where_ai_filter(filter_builder).limit(100).build()

        return await self.sf.execute_query(query)

    async def ai_aggregate(
        self,
        column_to_aggregate: str,
        aggregation_prompt: str,
        table_name: str,
        group_by: str | None = None,
    ) -> list[dict[str, Any]]:
        """Aggregate data using AI.

        Example:
            results = await service.ai_aggregate(
                'customer_feedback',
                'Identify the top 3 common themes',
                'feedback',
                group_by='product_category'
            )
        """
        agg = ai_aggregate(column_to_aggregate, aggregation_prompt)

        query_builder = (
            select(table_name).select("COUNT(*)", "total_records").select_ai_function(agg, "insights")
        )

        if group_by:
            query_builder = query_builder.select(group_by).group_by(group_by).order_by("total_records DESC").limit(10)

        return await self.sf.execute_query(query_builder.build())

    async def ai_sentiment(
        self,
        text_column: str,
        table_name: str,
    ) -> list[dict[str, Any]]:
        """Analyze sentiment of text.

        Example:
            results = await service.ai_sentiment('review_text', 'reviews')
        """
        sentiment = ai_sentiment(text_column)

        query = (
            select(table_name)
            .select(text_column)
            .select_ai_function(sentiment, "sentiment")
            .select("sentiment['categories'][0]['sentiment']::VARCHAR", "primary_sentiment")
            .select("sentiment['categories'][0]['score']::FLOAT", "sentiment_score")
            .limit(100)
            .build()
        )

        return await self.sf.execute_query(query)

    async def summarize(
        self,
        text_column: str,
        table_name: str,
    ) -> list[dict[str, Any]]:
        """Summarize text content.

        Example:
            results = await service.summarize('article_text', 'articles')
        """
        summary = summarize(text_column)

        query = (
            select(table_name)
            .select(text_column)
            .select_ai_function(summary, "summary")
            .limit(100)
            .build()
        )

        return await self.sf.execute_query(query)

    async def ai_transcribe(
        self,
        audio_file_column: str,
        table_name: str,
    ) -> list[dict[str, Any]]:
        """Transcribe audio files.

        Example:
            results = await service.ai_transcribe('audio_file', 'recordings')
        """
        transcribe = ai_transcribe(audio_file_column)

        query = (
            select(table_name)
            .select(audio_file_column)
            .select_ai_function(transcribe, "transcription")
            .select("transcription['text']::VARCHAR", "transcription_text")
            .select("transcription['audio_duration']::FLOAT", "audio_duration_seconds")
            .limit(100)
            .build()
        )

        return await self.sf.execute_query(query)

    # ============================================================================
    # Advanced Composite Operations
    # ============================================================================

    async def semantic_join(
        self,
        left_table: str,
        right_table: str,
        left_column: str,
        right_column: str,
        join_condition: str,
    ) -> list[dict[str, Any]]:
        """Perform semantic JOIN using AI understanding.

        Example:
            results = await service.semantic_join(
                'customer_issues',
                'solution_articles',
                'issue_description',
                'solution_text',
                'The issue {0} can be solved by {1}'
            )
        """
        query = semantic_join(
            left_table, right_table, left_column, right_column, join_condition
        ).build()

        return await self.sf.execute_query(query)

    async def extract_structured_data(
        self,
        text_column: str,
        table_name: str,
        extraction_prompt: str,
        schema: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Extract structured data from unstructured text.

        Example:
            schema = {
                'type': 'object',
                'properties': {
                    'product_name': {'type': 'string'},
                    'price': {'type': 'number'},
                    'category': {'type': 'string'}
                }
            }
            results = await service.extract_structured_data(
                'raw_text',
                'listings',
                'Extract product information',
                schema
            )
        """
        query = extract_structured(table_name, text_column, extraction_prompt, schema).build()

        return await self.sf.execute_query(query)

    # ============================================================================
    # Builder Access Methods (for custom queries)
    # ============================================================================

    def create_select_query(self, table_name: str) -> SelectQueryBuilder:
        """Create a custom SELECT query builder.

        Example:
            query_builder = service.create_select_query('products')
            query_builder.select('product_id', 'product_name')
            query_builder.select_ai_function(
                ai_sentiment('description'),
                'sentiment'
            )
            query_builder.limit(50)
            results = await service.execute_custom_query(query_builder)
        """
        return select(table_name)

    async def execute_custom_query(self, builder) -> list[dict[str, Any]]:
        """Execute a custom query built with builders.

        Args:
            builder: Any query builder instance

        Returns:
            Query results
        """
        query = builder.build()
        return await self.sf.execute_query(query)

    # ============================================================================
    # Convenience Methods for Common Patterns
    # ============================================================================

    async def multi_sentiment_analysis(
        self, table_name: str, text_columns: list[str]
    ) -> list[dict[str, Any]]:
        """Analyze sentiment for multiple text columns.

        Example:
            results = await service.multi_sentiment_analysis(
                'reviews',
                ['title', 'body', 'author_comment']
            )
        """
        query_builder = select(table_name)

        for col in text_columns:
            sentiment = ai_sentiment(col)
            query_builder.select_ai_function(sentiment, f"{col}_sentiment")

        query_builder.limit(100)
        return await self.sf.execute_query(query_builder.build())

    async def classify_and_summarize(
        self,
        table_name: str,
        text_column: str,
        categories: list[str],
    ) -> list[dict[str, Any]]:
        """Classify content and generate summary in one query.

        Example:
            results = await service.classify_and_summarize(
                'articles',
                'content',
                ['Technology', 'Business', 'Science']
            )
        """
        classifier = ai_classify(text_column, categories)
        summary_builder = summarize(text_column)

        query = (
            select(table_name)
            .select(text_column)
            .select_ai_function(classifier, "classification")
            .select_ai_function(summary_builder, "summary")
            .select("classification['labels'][0]", "primary_category")
            .limit(50)
            .build()
        )

        return await self.sf.execute_query(query)

    async def aggregate_with_sentiment(
        self,
        table_name: str,
        text_column: str,
        group_by_column: str,
    ) -> list[dict[str, Any]]:
        """Aggregate sentiment by category.

        Example:
            results = await service.aggregate_with_sentiment(
                'reviews',
                'review_text',
                'product_category'
            )
        """
        sentiment = ai_sentiment(text_column)

        query = (
            select(table_name)
            .select(group_by_column)
            .select("COUNT(*)", "total_reviews")
            .select_ai_function(sentiment, "avg_sentiment")
            .group_by(group_by_column)
            .order_by("total_reviews DESC")
            .limit(20)
            .build()
        )

        return await self.sf.execute_query(query)

    # ============================================================================
    # Query Builder Factory Methods
    # ============================================================================

    @staticmethod
    def build_ai_complete(model: str, prompt: str) -> AICompleteBuilder:
        """Create AI_COMPLETE builder."""
        return ai_complete(model, prompt)

    @staticmethod
    def build_ai_classify(content: str, categories: list[str]) -> AIClassifyBuilder:
        """Create AI_CLASSIFY builder."""
        return ai_classify(content, categories)

    @staticmethod
    def build_ai_filter(condition: str, *columns: str) -> AIFilterBuilder:
        """Create AI_FILTER builder."""
        return ai_filter(condition, *columns)

    @staticmethod
    def build_ai_aggregate(column: str, prompt: str) -> AIAggregateBuilder:
        """Create AI_AGG builder."""
        return ai_aggregate(column, prompt)

    @staticmethod
    def build_ai_sentiment(text_column: str) -> AISentimentBuilder:
        """Create AI_SENTIMENT builder."""
        return ai_sentiment(text_column)

    @staticmethod
    def build_summarize(text_column: str) -> SummarizeBuilder:
        """Create SUMMARIZE builder."""
        return summarize(text_column)

    @staticmethod
    def build_ai_transcribe(audio_column: str) -> AITranscribeBuilder:
        """Create AI_TRANSCRIBE builder."""
        return ai_transcribe(audio_column)
