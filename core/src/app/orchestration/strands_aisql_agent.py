"""Strands Agents Integration for AI SQL.

This module integrates AI SQL capabilities with the Strands Agents framework.
Install: pip install strands-agents strands-agents-tools

Usage:
    from app.orchestration.strands_aisql_agent import AISQLStrandsAgent

    agent = AISQLStrandsAgent(snowflake_service)
    response = await agent.run("Analyze sentiment of customer reviews")
"""

from typing import Any

from strands import Agent, tool
from strands.types.content import Message

from ..services.modular_ai_sql_service import ModularAISQLService
from ..services.snowflake_service import SnowflakeService


class AISQLStrandsAgent(Agent):
    """AI SQL Agent built with Strands Agents framework.

    This agent provides all 17+ AI SQL capabilities as Strands tools,
    allowing natural language interaction with Snowflake Cortex AI.

    Example:
        agent = AISQLStrandsAgent(snowflake_service)
        response = await agent.run("Analyze sentiment of reviews in the reviews table")
    """

    def __init__(
        self,
        snowflake_service: SnowflakeService,
        name: str = "AI SQL Agent",
        model: str = "claude-3-7-sonnet",
        **kwargs,
    ):
        """Initialize AI SQL Strands Agent.

        Args:
            snowflake_service: Snowflake service instance
            name: Agent name
            model: LLM model to use
            **kwargs: Additional Agent configuration
        """
        # Initialize AI SQL service
        self.ai_sql_service = ModularAISQLService(snowflake_service)

        # Initialize Strands Agent
        super().__init__(
            model=model,
            name=name,
            system_prompt=self._build_instructions(),
            **kwargs,
        )

    def _build_instructions(self) -> str:
        """Build comprehensive instructions for the agent."""
        return """You are an AI SQL expert specializing in Snowflake Cortex AI operations.

You have access to 17+ powerful AI SQL tools organized by category:

**TEXT ANALYSIS**
- ai_sentiment: Analyze emotional tone and sentiment
- ai_classify: Categorize text into predefined labels
- ai_filter: Filter data using natural language conditions
- ai_similarity: Calculate semantic similarity between texts

**TEXT TRANSFORMATION**
- ai_translate: Translate between languages
- ai_redact: Remove personally identifiable information (PII)
- summarize: Generate concise summaries
- ai_complete: Generate text using LLMs

**DATA EXTRACTION**
- ai_extract: Extract specific information from text
- extract_structured_data: Extract data into JSON schema
- ai_parse_document: Parse PDFs and images (OCR)
- ai_transcribe: Transcribe audio/video to text

**AGGREGATION**
- ai_aggregate: Aggregate text with natural language instructions
- ai_summarize_agg: Aggregate and summarize multiple rows

**SEMANTIC OPERATIONS**
- ai_embed: Generate embedding vectors
- semantic_join: Join tables using semantic understanding

**UTILITY**
- ai_count_tokens: Count tokens for cost estimation

When a user asks you to perform an AI SQL operation:
1. Identify which tool(s) are most appropriate
2. Extract the necessary parameters (table name, columns, etc.)
3. Call the tool with the correct parameters
4. Explain the results in a clear, helpful way

Always ask for clarification if table names or column names are not specified.
"""

    # ========================================================================
    # TEXT ANALYSIS TOOLS
    # ========================================================================

    @tool
    async def ai_sentiment(
        self,
        table_name: str,
        text_column: str,
    ) -> dict[str, Any]:
        """Analyze sentiment of text in a table column.

        Use this to understand emotional tone, detect positive/negative feedback,
        or measure customer satisfaction.

        Args:
            table_name: Name of the table to analyze
            text_column: Column containing text to analyze

        Returns:
            Sentiment analysis results with scores and categories
        """
        results = await self.ai_sql_service.ai_sentiment(text_column, table_name)
        return {
            "tool": "ai_sentiment",
            "table": table_name,
            "column": text_column,
            "results": results,
            "count": len(results),
        }

    @tool
    async def ai_classify(
        self,
        table_name: str,
        content_column: str,
        categories: list[str],
        prompt_prefix: str | None = None,
    ) -> dict[str, Any]:
        """Classify text into predefined categories.

        Use this for content categorization, tagging, routing, or organizing data.

        Args:
            table_name: Name of the table
            content_column: Column containing text to classify
            categories: List of category labels (e.g., ["Positive", "Negative", "Neutral"])
            prompt_prefix: Optional context to add before classification

        Returns:
            Classification results with labels and confidence scores
        """
        results = await self.ai_sql_service.ai_classify(
            content_column, categories, table_name, prompt_prefix
        )
        return {
            "tool": "ai_classify",
            "table": table_name,
            "column": content_column,
            "categories": categories,
            "results": results,
            "count": len(results),
        }

    @tool
    async def ai_filter(
        self,
        table_name: str,
        filter_condition: str,
        columns: list[str],
    ) -> dict[str, Any]:
        """Filter data using natural language conditions.

        Use this for semantic search, finding specific content, or complex filtering
        that's hard to express in SQL.

        Args:
            table_name: Name of the table to filter
            filter_condition: Natural language filter (e.g., "mentions shipping issues")
            columns: Columns to consider in filtering

        Returns:
            Filtered results matching the condition
        """
        results = await self.ai_sql_service.ai_filter(
            filter_condition, table_name, columns
        )
        return {
            "tool": "ai_filter",
            "table": table_name,
            "condition": filter_condition,
            "columns": columns,
            "results": results,
            "count": len(results),
        }

    @tool
    async def ai_similarity(
        self,
        table_name: str,
        column1: str,
        column2: str,
    ) -> dict[str, Any]:
        """Calculate semantic similarity between two text columns.

        Use this to find similar content, detect duplicates, or match related items.

        Args:
            table_name: Name of the table
            column1: First text column
            column2: Second text column

        Returns:
            Similarity scores between the columns
        """
        results = await self.ai_sql_service.ai_similarity(table_name, column1, column2)
        return {
            "tool": "ai_similarity",
            "table": table_name,
            "columns": [column1, column2],
            "results": results,
            "count": len(results),
        }

    # ========================================================================
    # TEXT TRANSFORMATION TOOLS
    # ========================================================================

    @tool
    async def ai_translate(
        self,
        table_name: str,
        text_column: str,
        source_lang: str,
        target_lang: str,
    ) -> dict[str, Any]:
        """Translate text from one language to another.

        Use this for localization, multilingual support, or content translation.

        Args:
            table_name: Name of the table
            text_column: Column containing text to translate
            source_lang: Source language code (e.g., 'en', 'es', 'fr')
            target_lang: Target language code

        Returns:
            Translated text results
        """
        results = await self.ai_sql_service.ai_translate(
            text_column, table_name, source_lang, target_lang
        )
        return {
            "tool": "ai_translate",
            "table": table_name,
            "column": text_column,
            "from": source_lang,
            "to": target_lang,
            "results": results,
            "count": len(results),
        }

    @tool
    async def ai_redact(
        self,
        table_name: str,
        text_column: str,
        pii_types: list[str] | None = None,
    ) -> dict[str, Any]:
        """Remove personally identifiable information (PII) from text.

        Use this for privacy compliance, data anonymization, or GDPR requirements.

        Args:
            table_name: Name of the table
            text_column: Column containing text to redact
            pii_types: Types of PII to redact (e.g., ["EMAIL", "PHONE_NUMBER"])

        Returns:
            Redacted text with PII removed
        """
        results = await self.ai_sql_service.ai_redact(
            text_column, table_name, pii_types
        )
        return {
            "tool": "ai_redact",
            "table": table_name,
            "column": text_column,
            "pii_types": pii_types or "all",
            "results": results,
            "count": len(results),
        }

    @tool
    async def summarize(
        self,
        table_name: str,
        text_column: str,
    ) -> dict[str, Any]:
        """Generate concise summaries of text.

        Use this for content condensation, executive summaries, or quick overviews.

        Args:
            table_name: Name of the table
            text_column: Column containing text to summarize

        Returns:
            Summarized text results
        """
        results = await self.ai_sql_service.summarize(text_column, table_name)
        return {
            "tool": "summarize",
            "table": table_name,
            "column": text_column,
            "results": results,
            "count": len(results),
        }

    @tool
    async def ai_complete(
        self,
        model: str,
        prompt: str,
        response_format: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Generate text completions using LLMs.

        Use this for creative writing, question answering, or any text generation task.

        Args:
            model: LLM model name (e.g., 'claude-3-7-sonnet', 'gpt-4')
            prompt: The prompt/question to send to the model
            response_format: Optional JSON schema for structured output

        Returns:
            Generated text response
        """
        result = await self.ai_sql_service.ai_complete(model, prompt, response_format)
        return {
            "tool": "ai_complete",
            "model": model,
            "prompt": prompt[:100] + "..." if len(prompt) > 100 else prompt,
            "response": result,
        }

    # ========================================================================
    # DATA EXTRACTION TOOLS
    # ========================================================================

    @tool
    async def ai_extract(
        self,
        table_name: str,
        content_column: str,
        instruction: str,
    ) -> dict[str, Any]:
        """Extract specific information from text using natural language instructions.

        Use this to pull out emails, dates, names, prices, or any specific data.

        Args:
            table_name: Name of the table
            content_column: Column containing text to extract from
            instruction: What to extract (e.g., "Extract all email addresses")

        Returns:
            Extracted information
        """
        results = await self.ai_sql_service.ai_extract(
            content_column, table_name, instruction
        )
        return {
            "tool": "ai_extract",
            "table": table_name,
            "column": content_column,
            "instruction": instruction,
            "results": results,
            "count": len(results),
        }

    @tool
    async def extract_structured_data(
        self,
        table_name: str,
        text_column: str,
        extraction_prompt: str,
        schema: dict[str, Any],
    ) -> dict[str, Any]:
        """Extract structured data from unstructured text using a JSON schema.

        Use this to convert free-form text into structured, queryable data.

        Args:
            table_name: Name of the table
            text_column: Column containing unstructured text
            extraction_prompt: Instructions for extraction
            schema: JSON schema defining output structure

        Returns:
            Structured data matching the schema
        """
        results = await self.ai_sql_service.extract_structured_data(
            text_column, table_name, extraction_prompt, schema
        )
        return {
            "tool": "extract_structured_data",
            "table": table_name,
            "column": text_column,
            "schema": schema,
            "results": results,
            "count": len(results),
        }

    @tool
    async def ai_parse_document(
        self,
        table_name: str,
        file_path_column: str,
        mode: str = "layout",
    ) -> dict[str, Any]:
        """Parse documents (PDF, images) to extract text using OCR or layout analysis.

        Use this to digitize scanned documents, extract text from images, or parse PDFs.

        Args:
            table_name: Name of the table
            file_path_column: Column containing file paths
            mode: Parsing mode - 'layout' or 'ocr'

        Returns:
            Parsed text content from documents
        """
        results = await self.ai_sql_service.ai_parse_document(
            file_path_column, table_name, mode
        )
        return {
            "tool": "ai_parse_document",
            "table": table_name,
            "column": file_path_column,
            "mode": mode,
            "results": results,
            "count": len(results),
        }

    @tool
    async def ai_transcribe(
        self,
        table_name: str,
        audio_file_column: str,
    ) -> dict[str, Any]:
        """Transcribe audio/video files to text with timestamps and speaker labels.

        Use this for meeting transcriptions, call center analysis, or video subtitles.

        Args:
            table_name: Name of the table
            audio_file_column: Column containing audio/video file paths

        Returns:
            Transcribed text with metadata
        """
        results = await self.ai_sql_service.ai_transcribe(
            audio_file_column, table_name
        )
        return {
            "tool": "ai_transcribe",
            "table": table_name,
            "column": audio_file_column,
            "results": results,
            "count": len(results),
        }

    # ========================================================================
    # AGGREGATION TOOLS
    # ========================================================================

    @tool
    async def ai_aggregate(
        self,
        table_name: str,
        column_to_aggregate: str,
        aggregation_prompt: str,
        group_by: str | None = None,
    ) -> dict[str, Any]:
        """Aggregate text data using natural language instructions.

        Use this to find themes, patterns, or insights across multiple rows.

        Args:
            table_name: Name of the table
            column_to_aggregate: Column to aggregate
            aggregation_prompt: What insights to extract (e.g., "top 3 common themes")
            group_by: Optional column to group by

        Returns:
            Aggregated insights
        """
        results = await self.ai_sql_service.ai_aggregate(
            column_to_aggregate, aggregation_prompt, table_name, group_by
        )
        return {
            "tool": "ai_aggregate",
            "table": table_name,
            "column": column_to_aggregate,
            "prompt": aggregation_prompt,
            "group_by": group_by,
            "results": results,
            "count": len(results),
        }

    @tool
    async def ai_summarize_agg(
        self,
        table_name: str,
        text_column: str,
        group_by: str | None = None,
    ) -> dict[str, Any]:
        """Aggregate and summarize multiple rows of text into a single summary.

        Use this to create overviews of large amounts of text data.

        Args:
            table_name: Name of the table
            text_column: Column containing text to aggregate
            group_by: Optional column to group by

        Returns:
            Aggregated summaries
        """
        results = await self.ai_sql_service.ai_summarize_agg(
            text_column, table_name, group_by
        )
        return {
            "tool": "ai_summarize_agg",
            "table": table_name,
            "column": text_column,
            "group_by": group_by,
            "results": results,
            "count": len(results),
        }

    # ========================================================================
    # SEMANTIC OPERATIONS TOOLS
    # ========================================================================

    @tool
    async def ai_embed(
        self,
        table_name: str,
        content_column: str,
        model: str = "e5-base-v2",
    ) -> dict[str, Any]:
        """Generate embedding vectors for text content.

        Use this for similarity search, clustering, or building semantic indexes.

        Args:
            table_name: Name of the table
            content_column: Column containing text to embed
            model: Embedding model name

        Returns:
            Embedding vectors
        """
        results = await self.ai_sql_service.ai_embed(content_column, table_name, model)
        return {
            "tool": "ai_embed",
            "table": table_name,
            "column": content_column,
            "model": model,
            "results": results,
            "count": len(results),
        }

    @tool
    async def semantic_join(
        self,
        left_table: str,
        right_table: str,
        left_column: str,
        right_column: str,
        join_condition: str,
    ) -> dict[str, Any]:
        """Join tables using semantic understanding rather than exact matches.

        Use this to match related content, link questions to answers, or find connections.

        Args:
            left_table: Left table name
            right_table: Right table name
            left_column: Column from left table
            right_column: Column from right table
            join_condition: Natural language join condition

        Returns:
            Semantically joined results
        """
        results = await self.ai_sql_service.semantic_join(
            left_table, right_table, left_column, right_column, join_condition
        )
        return {
            "tool": "semantic_join",
            "left_table": left_table,
            "right_table": right_table,
            "condition": join_condition,
            "results": results,
            "count": len(results),
        }

    # ========================================================================
    # UTILITY TOOLS
    # ========================================================================

    @tool
    async def ai_count_tokens(
        self,
        table_name: str,
        model: str,
        text_column: str,
    ) -> dict[str, Any]:
        """Count tokens in text for a specific model.

        Use this to prevent exceeding model limits or estimate costs.

        Args:
            table_name: Name of the table
            model: Model name to count tokens for
            text_column: Column containing text

        Returns:
            Token counts
        """
        results = await self.ai_sql_service.ai_count_tokens(
            model, text_column, table_name
        )
        return {
            "tool": "ai_count_tokens",
            "table": table_name,
            "model": model,
            "column": text_column,
            "results": results,
            "count": len(results),
        }


# ============================================================================
# Factory Function
# ============================================================================


def create_aisql_agent(
    snowflake_service: SnowflakeService,
    name: str = "AI SQL Agent",
    model: str = "claude-3-7-sonnet",
    **kwargs,
) -> AISQLStrandsAgent:
    """Create an AI SQL Strands Agent.

    Usage:
        from app.orchestration.strands_aisql_agent import create_aisql_agent

        agent = create_aisql_agent(snowflake_service)
        response = await agent.run("Analyze sentiment of customer reviews")
        print(response.content)

    Args:
        snowflake_service: Snowflake service instance
        name: Agent name
        model: LLM model to use
        **kwargs: Additional Agent configuration

    Returns:
        Configured AI SQL Strands Agent
    """
    return AISQLStrandsAgent(
        snowflake_service=snowflake_service,
        name=name,
        model=model,
        **kwargs,
    )


# ============================================================================
# Example Usage
# ============================================================================


async def example_usage():
    """Example of using the AI SQL Strands Agent."""
    from app.services.snowflake_service import SnowflakeService

    # Initialize services
    sf_service = SnowflakeService(connection)

    # Create agent
    agent = create_aisql_agent(sf_service)

    # Example 1: Sentiment Analysis
    response = await agent.run(
        "Analyze the sentiment of customer reviews in the reviews table, "
        "specifically the review_text column"
    )
    print(response.content)

    # Example 2: Classification
    response = await agent.run(
        "Classify products in the products table into Electronics, Clothing, or Food "
        "based on the product_description column"
    )
    print(response.content)

    # Example 3: Translation
    response = await agent.run(
        "Translate all product descriptions from English to Spanish in the products table"
    )
    print(response.content)

    # Example 4: Semantic Search
    response = await agent.run(
        "Find all reviews that mention shipping issues in the reviews table"
    )
    print(response.content)

    # Example 5: Multi-turn conversation
    messages = [
        Message(role="user", content="Show me sentiment analysis of reviews"),
        Message(role="assistant", content=response.content),
        Message(role="user", content="Now translate the negative ones to Spanish"),
    ]
    response = await agent.run(messages)
    print(response.content)
