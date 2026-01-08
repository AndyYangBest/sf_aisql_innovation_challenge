"""AI SQL Tools - Pluggable tool wrappers for AI SQL capabilities.

This module provides a clean, framework-agnostic tool interface that can be
easily adapted to any agent framework (LangChain, LlamaIndex, Strands, etc.).
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Coroutine

from ..services.modular_ai_sql_service import ModularAISQLService


class ToolCategory(str, Enum):
    """Tool categories for organization."""

    TEXT_GENERATION = "text_generation"
    TEXT_ANALYSIS = "text_analysis"
    TEXT_TRANSFORMATION = "text_transformation"
    DATA_EXTRACTION = "data_extraction"
    AGGREGATION = "aggregation"
    SEMANTIC_OPERATIONS = "semantic_operations"
    UTILITY = "utility"


@dataclass
class ToolDefinition:
    """Definition of an AI SQL tool.

    This is a framework-agnostic tool definition that can be adapted
    to any agent framework's tool format.
    """

    name: str
    description: str
    category: ToolCategory
    parameters: dict[str, Any]
    function: Callable[..., Coroutine[Any, Any, Any]]
    examples: list[str]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "name": self.name,
            "description": self.description,
            "category": self.category.value,
            "parameters": self.parameters,
            "examples": self.examples,
        }


class AISQLToolkit:
    """Toolkit providing all AI SQL capabilities as pluggable tools.

    This class wraps the ModularAISQLService methods into a clean tool interface
    that can be easily integrated with any agent framework.

    Usage:
        toolkit = AISQLToolkit(ai_sql_service)
        tools = toolkit.get_all_tools()

        # Use with your agent framework
        for tool in tools:
            agent.register_tool(tool.name, tool.function, tool.description)
    """

    def __init__(self, ai_sql_service: ModularAISQLService):
        """Initialize toolkit with AI SQL service."""
        self.service = ai_sql_service
        self._tools: dict[str, ToolDefinition] = {}
        self._register_all_tools()

    def _register_all_tools(self):
        """Register all available tools."""
        # Text Generation Tools
        self._register_tool(
            name="ai_complete",
            description="Generate text completions using LLMs. Use for creative writing, question answering, or any text generation task.",
            category=ToolCategory.TEXT_GENERATION,
            parameters={
                "model": {
                    "type": "string",
                    "description": "LLM model name (e.g., 'claude-3-7-sonnet', 'gpt-4')",
                    "required": True,
                },
                "prompt": {
                    "type": "string",
                    "description": "The prompt/question to send to the model",
                    "required": True,
                },
                "response_format": {
                    "type": "object",
                    "description": "Optional JSON schema for structured output",
                    "required": False,
                },
            },
            function=self.service.ai_complete,
            examples=[
                "Generate a product description for a laptop",
                "Answer: What is the capital of France?",
                "Summarize this text in 3 sentences",
            ],
        )

        # Text Analysis Tools
        self._register_tool(
            name="ai_classify",
            description="Classify text into predefined categories. Use for content categorization, tagging, or routing.",
            category=ToolCategory.TEXT_ANALYSIS,
            parameters={
                "content_column": {
                    "type": "string",
                    "description": "Column containing text to classify",
                    "required": True,
                },
                "categories": {
                    "type": "array",
                    "description": "List of category labels",
                    "required": True,
                },
                "table_name": {
                    "type": "string",
                    "description": "Table name to query",
                    "required": True,
                },
                "prompt_prefix": {
                    "type": "string",
                    "description": "Optional prefix to add context",
                    "required": False,
                },
            },
            function=self.service.ai_classify,
            examples=[
                "Classify customer feedback into: Positive, Negative, Neutral",
                "Categorize products into: Electronics, Clothing, Food",
                "Tag support tickets as: Bug, Feature Request, Question",
            ],
        )

        self._register_tool(
            name="ai_sentiment",
            description="Analyze sentiment of text. Returns overall sentiment and category-specific scores.",
            category=ToolCategory.TEXT_ANALYSIS,
            parameters={
                "text_column": {
                    "type": "string",
                    "description": "Column containing text to analyze",
                    "required": True,
                },
                "table_name": {
                    "type": "string",
                    "description": "Table name to query",
                    "required": True,
                },
            },
            function=self.service.ai_sentiment,
            examples=[
                "Analyze sentiment of customer reviews",
                "Detect emotional tone in social media posts",
                "Measure satisfaction in survey responses",
            ],
        )

        self._register_tool(
            name="ai_filter",
            description="Filter data using natural language conditions. Use for semantic search or complex filtering.",
            category=ToolCategory.TEXT_ANALYSIS,
            parameters={
                "filter_condition": {
                    "type": "string",
                    "description": "Natural language filter condition",
                    "required": True,
                },
                "table_name": {
                    "type": "string",
                    "description": "Table name to filter",
                    "required": True,
                },
                "columns": {
                    "type": "array",
                    "description": "Columns to consider in filtering",
                    "required": True,
                },
            },
            function=self.service.ai_filter,
            examples=[
                "Find products that mention sustainability",
                "Filter reviews that discuss shipping issues",
                "Find articles about artificial intelligence",
            ],
        )

        # Text Transformation Tools
        self._register_tool(
            name="ai_translate",
            description="Translate text between languages. Supports major world languages.",
            category=ToolCategory.TEXT_TRANSFORMATION,
            parameters={
                "text_column": {
                    "type": "string",
                    "description": "Column containing text to translate",
                    "required": True,
                },
                "table_name": {
                    "type": "string",
                    "description": "Table name to query",
                    "required": True,
                },
                "source_lang": {
                    "type": "string",
                    "description": "Source language code (e.g., 'en', 'es', 'fr')",
                    "required": True,
                },
                "target_lang": {
                    "type": "string",
                    "description": "Target language code",
                    "required": True,
                },
            },
            function=self.service.ai_translate,
            examples=[
                "Translate product descriptions from English to Spanish",
                "Convert customer feedback from French to English",
                "Localize content for international markets",
            ],
        )

        self._register_tool(
            name="ai_redact",
            description="Remove personally identifiable information (PII) from text. Use for privacy compliance.",
            category=ToolCategory.TEXT_TRANSFORMATION,
            parameters={
                "text_column": {
                    "type": "string",
                    "description": "Column containing text to redact",
                    "required": True,
                },
                "table_name": {
                    "type": "string",
                    "description": "Table name to query",
                    "required": True,
                },
                "pii_types": {
                    "type": "array",
                    "description": "Types of PII to redact (EMAIL, PHONE_NUMBER, etc.)",
                    "required": False,
                },
            },
            function=self.service.ai_redact,
            examples=[
                "Remove email addresses from customer comments",
                "Redact phone numbers and addresses from support tickets",
                "Anonymize user data for analysis",
            ],
        )

        self._register_tool(
            name="summarize",
            description="Generate concise summaries of text. Use for content condensation.",
            category=ToolCategory.TEXT_TRANSFORMATION,
            parameters={
                "text_column": {
                    "type": "string",
                    "description": "Column containing text to summarize",
                    "required": True,
                },
                "table_name": {
                    "type": "string",
                    "description": "Table name to query",
                    "required": True,
                },
            },
            function=self.service.summarize,
            examples=[
                "Summarize long articles",
                "Create executive summaries of reports",
                "Condense customer feedback",
            ],
        )

        # Data Extraction Tools
        self._register_tool(
            name="ai_extract",
            description="Extract specific information from text using natural language instructions.",
            category=ToolCategory.DATA_EXTRACTION,
            parameters={
                "content_column": {
                    "type": "string",
                    "description": "Column containing text to extract from",
                    "required": True,
                },
                "table_name": {
                    "type": "string",
                    "description": "Table name to query",
                    "required": True,
                },
                "instruction": {
                    "type": "string",
                    "description": "What to extract (e.g., 'Extract all email addresses')",
                    "required": True,
                },
            },
            function=self.service.ai_extract,
            examples=[
                "Extract email addresses from text",
                "Pull out product names and prices",
                "Find dates and locations mentioned",
            ],
        )

        self._register_tool(
            name="extract_structured_data",
            description="Extract structured data from unstructured text using a schema. Returns JSON.",
            category=ToolCategory.DATA_EXTRACTION,
            parameters={
                "text_column": {
                    "type": "string",
                    "description": "Column containing unstructured text",
                    "required": True,
                },
                "table_name": {
                    "type": "string",
                    "description": "Table name to query",
                    "required": True,
                },
                "extraction_prompt": {
                    "type": "string",
                    "description": "Instructions for extraction",
                    "required": True,
                },
                "schema": {
                    "type": "object",
                    "description": "JSON schema defining output structure",
                    "required": True,
                },
            },
            function=self.service.extract_structured_data,
            examples=[
                "Extract product info (name, price, category) from listings",
                "Parse invoice data into structured format",
                "Convert resumes into standardized JSON",
            ],
        )

        self._register_tool(
            name="ai_parse_document",
            description="Parse documents (PDF, images) to extract text using OCR or layout analysis.",
            category=ToolCategory.DATA_EXTRACTION,
            parameters={
                "file_path_column": {
                    "type": "string",
                    "description": "Column containing file paths",
                    "required": True,
                },
                "table_name": {
                    "type": "string",
                    "description": "Table name to query",
                    "required": True,
                },
                "mode": {
                    "type": "string",
                    "description": "Parsing mode: 'layout' or 'ocr'",
                    "required": False,
                },
            },
            function=self.service.ai_parse_document,
            examples=[
                "Extract text from scanned documents",
                "Parse PDF invoices",
                "Convert images to text",
            ],
        )

        self._register_tool(
            name="ai_transcribe",
            description="Transcribe audio/video files to text with optional timestamps and speaker labels.",
            category=ToolCategory.DATA_EXTRACTION,
            parameters={
                "audio_file_column": {
                    "type": "string",
                    "description": "Column containing audio/video file paths",
                    "required": True,
                },
                "table_name": {
                    "type": "string",
                    "description": "Table name to query",
                    "required": True,
                },
            },
            function=self.service.ai_transcribe,
            examples=[
                "Transcribe customer service calls",
                "Convert meeting recordings to text",
                "Generate subtitles for videos",
            ],
        )

        # Aggregation Tools
        self._register_tool(
            name="ai_aggregate",
            description="Aggregate text data using natural language instructions. Use for insights across multiple rows.",
            category=ToolCategory.AGGREGATION,
            parameters={
                "column_to_aggregate": {
                    "type": "string",
                    "description": "Column to aggregate",
                    "required": True,
                },
                "aggregation_prompt": {
                    "type": "string",
                    "description": "What insights to extract",
                    "required": True,
                },
                "table_name": {
                    "type": "string",
                    "description": "Table name to query",
                    "required": True,
                },
                "group_by": {
                    "type": "string",
                    "description": "Optional column to group by",
                    "required": False,
                },
            },
            function=self.service.ai_aggregate,
            examples=[
                "Identify top 3 common themes in feedback",
                "Find recurring issues in support tickets",
                "Summarize key points across all reviews",
            ],
        )

        self._register_tool(
            name="ai_summarize_agg",
            description="Aggregate and summarize multiple rows of text into a single summary.",
            category=ToolCategory.AGGREGATION,
            parameters={
                "text_column": {
                    "type": "string",
                    "description": "Column containing text to aggregate",
                    "required": True,
                },
                "table_name": {
                    "type": "string",
                    "description": "Table name to query",
                    "required": True,
                },
                "group_by": {
                    "type": "string",
                    "description": "Optional column to group by",
                    "required": False,
                },
            },
            function=self.service.ai_summarize_agg,
            examples=[
                "Summarize all customer feedback for a product",
                "Create overview of support tickets by category",
                "Aggregate news articles by topic",
            ],
        )

        # Semantic Operations
        self._register_tool(
            name="ai_embed",
            description="Generate embedding vectors for text. Use for similarity search and clustering.",
            category=ToolCategory.SEMANTIC_OPERATIONS,
            parameters={
                "content_column": {
                    "type": "string",
                    "description": "Column containing text to embed",
                    "required": True,
                },
                "table_name": {
                    "type": "string",
                    "description": "Table name to query",
                    "required": True,
                },
                "model": {
                    "type": "string",
                    "description": "Embedding model name",
                    "required": False,
                },
            },
            function=self.service.ai_embed,
            examples=[
                "Create embeddings for semantic search",
                "Generate vectors for clustering",
                "Build similarity index",
            ],
        )

        self._register_tool(
            name="ai_similarity",
            description="Calculate semantic similarity between two text columns. Returns similarity score.",
            category=ToolCategory.SEMANTIC_OPERATIONS,
            parameters={
                "table_name": {
                    "type": "string",
                    "description": "Table name to query",
                    "required": True,
                },
                "column1": {
                    "type": "string",
                    "description": "First text column",
                    "required": True,
                },
                "column2": {
                    "type": "string",
                    "description": "Second text column",
                    "required": True,
                },
            },
            function=self.service.ai_similarity,
            examples=[
                "Find similar product descriptions",
                "Match questions to answers",
                "Detect duplicate content",
            ],
        )

        self._register_tool(
            name="semantic_join",
            description="Join tables using semantic understanding rather than exact matches.",
            category=ToolCategory.SEMANTIC_OPERATIONS,
            parameters={
                "left_table": {
                    "type": "string",
                    "description": "Left table name",
                    "required": True,
                },
                "right_table": {
                    "type": "string",
                    "description": "Right table name",
                    "required": True,
                },
                "left_column": {
                    "type": "string",
                    "description": "Column from left table",
                    "required": True,
                },
                "right_column": {
                    "type": "string",
                    "description": "Column from right table",
                    "required": True,
                },
                "join_condition": {
                    "type": "string",
                    "description": "Natural language join condition",
                    "required": True,
                },
            },
            function=self.service.semantic_join,
            examples=[
                "Match customer issues to solution articles",
                "Link products to relevant reviews",
                "Connect questions to knowledge base entries",
            ],
        )

        # Utility Tools
        self._register_tool(
            name="ai_count_tokens",
            description="Count tokens in text for a specific model. Use to prevent exceeding model limits.",
            category=ToolCategory.UTILITY,
            parameters={
                "model": {
                    "type": "string",
                    "description": "Model name to count tokens for",
                    "required": True,
                },
                "text_column": {
                    "type": "string",
                    "description": "Column containing text",
                    "required": True,
                },
                "table_name": {
                    "type": "string",
                    "description": "Table name to query",
                    "required": True,
                },
            },
            function=self.service.ai_count_tokens,
            examples=[
                "Check if prompts fit within model limits",
                "Calculate token usage for cost estimation",
                "Validate input size before processing",
            ],
        )

    def _register_tool(
        self,
        name: str,
        description: str,
        category: ToolCategory,
        parameters: dict[str, Any],
        function: Callable[..., Coroutine[Any, Any, Any]],
        examples: list[str],
    ):
        """Register a tool in the toolkit."""
        tool = ToolDefinition(
            name=name,
            description=description,
            category=category,
            parameters=parameters,
            function=function,
            examples=examples,
        )
        self._tools[name] = tool

    def get_tool(self, name: str) -> ToolDefinition | None:
        """Get a specific tool by name."""
        return self._tools.get(name)

    def get_all_tools(self) -> list[ToolDefinition]:
        """Get all registered tools."""
        return list(self._tools.values())

    def get_tools_by_category(self, category: ToolCategory) -> list[ToolDefinition]:
        """Get tools filtered by category."""
        return [tool for tool in self._tools.values() if tool.category == category]

    def get_tool_names(self) -> list[str]:
        """Get list of all tool names."""
        return list(self._tools.keys())

    def get_tool_descriptions(self) -> dict[str, str]:
        """Get mapping of tool names to descriptions."""
        return {name: tool.description for name, tool in self._tools.items()}

    def to_dict(self) -> dict[str, Any]:
        """Export toolkit as dictionary."""
        return {
            "tools": [tool.to_dict() for tool in self._tools.values()],
            "categories": [cat.value for cat in ToolCategory],
        }
