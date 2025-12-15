"""Schemas for AI SQL API operations."""

from pydantic import BaseModel, Field

# ============================================================================
# AI_COMPLETE Schemas
# ============================================================================


class AICompleteRequest(BaseModel):
    """Request for AI_COMPLETE text generation."""

    model: str = Field(..., description="LLM model name (claude-3-7-sonnet, mistral-large, pixtral-large)")
    prompt: str = Field(..., description="The prompt text")
    response_format: dict | None = Field(None, description="Optional JSON schema for structured output")


class AICompleteResponse(BaseModel):
    """Response from AI_COMPLETE."""

    response: str = Field(..., description="Generated text or structured JSON")


# ============================================================================
# AI_TRANSCRIBE Schemas
# ============================================================================


class AITranscribeRequest(BaseModel):
    """Request for audio transcription."""

    audio_file_column: str = Field(..., description="Column name containing audio files")
    table_name: str = Field(..., description="Table name")


class TranscriptionResult(BaseModel):
    """Single transcription result."""

    transcription_text: str
    audio_duration_seconds: float


class AITranscribeResponse(BaseModel):
    """Response from AI_TRANSCRIBE."""

    results: list[TranscriptionResult]


# ============================================================================
# AI_CLASSIFY Schemas
# ============================================================================


class AIClassifyRequest(BaseModel):
    """Request for AI classification."""

    content_column: str = Field(..., description="Column containing content to classify")
    categories: list[str] = Field(..., description="List of possible categories")
    table_name: str = Field(..., description="Table name")
    prompt_prefix: str = Field("Classify the following content: ", description="Optional prompt prefix")


class ClassificationResult(BaseModel):
    """Single classification result."""

    primary_label: str
    primary_confidence: float


class AIClassifyResponse(BaseModel):
    """Response from AI_CLASSIFY."""

    results: list[ClassificationResult]


# ============================================================================
# AI_FILTER Schemas
# ============================================================================


class AIFilterRequest(BaseModel):
    """Request for AI-based filtering."""

    filter_condition: str = Field(..., description="Natural language filter condition")
    table_name: str = Field(..., description="Table name")
    columns: list[str] = Field(..., description="Columns to select")


class AIFilterResponse(BaseModel):
    """Response from AI_FILTER."""

    results: list[dict]


# ============================================================================
# AI_AGG Schemas
# ============================================================================


class AIAggregateRequest(BaseModel):
    """Request for AI-powered aggregation."""

    column_to_aggregate: str = Field(..., description="Column to aggregate")
    aggregation_prompt: str = Field(..., description="Prompt describing the aggregation task")
    table_name: str = Field(..., description="Table name")
    group_by: str | None = Field(None, description="Optional column to group by")


class AggregationResult(BaseModel):
    """Single aggregation result."""

    total_records: int
    insights: str


class AIAggregateResponse(BaseModel):
    """Response from AI_AGG."""

    results: list[AggregationResult]


# ============================================================================
# AI_SENTIMENT Schemas
# ============================================================================


class AISentimentRequest(BaseModel):
    """Request for sentiment analysis."""

    text_column: str = Field(..., description="Column containing text to analyze")
    table_name: str = Field(..., description="Table name")


class SentimentResult(BaseModel):
    """Single sentiment result."""

    primary_sentiment: str
    sentiment_score: float


class AISentimentResponse(BaseModel):
    """Response from AI_SENTIMENT."""

    results: list[SentimentResult]


# ============================================================================
# SUMMARIZE Schemas
# ============================================================================


class SummarizeRequest(BaseModel):
    """Request for text summarization."""

    text_column: str = Field(..., description="Column containing text to summarize")
    table_name: str = Field(..., description="Table name")


class SummaryResult(BaseModel):
    """Single summary result."""

    summary: str


class SummarizeResponse(BaseModel):
    """Response from SUMMARIZE."""

    results: list[SummaryResult]


# ============================================================================
# Semantic JOIN Schemas
# ============================================================================


class SemanticJoinRequest(BaseModel):
    """Request for semantic JOIN using AI_FILTER."""

    left_table: str = Field(..., description="First table")
    right_table: str = Field(..., description="Second table")
    left_column: str = Field(..., description="Column from first table")
    right_column: str = Field(..., description="Column from second table")
    join_condition: str = Field(
        ..., description="Natural language join condition with {0} and {1} placeholders"
    )


class SemanticJoinResponse(BaseModel):
    """Response from semantic JOIN."""

    results: list[dict]


# ============================================================================
# Structured Extraction Schemas
# ============================================================================


class ExtractStructuredDataRequest(BaseModel):
    """Request for extracting structured data from text."""

    text_column: str = Field(..., description="Column containing unstructured text")
    table_name: str = Field(..., description="Table name")
    extraction_prompt: str = Field(..., description="Prompt describing what to extract")
    schema: dict = Field(..., description="JSON schema for the extracted data")


class ExtractStructuredDataResponse(BaseModel):
    """Response from structured data extraction."""

    results: list[dict]
