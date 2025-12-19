"""API endpoints for Snowflake Cortex AI SQL functions."""

from fastapi import APIRouter, Depends, HTTPException

from ...api.dependencies import rate_limiter_dependency
from ...schemas.ai_sql import (
    AIAggregateRequest,
    AIAggregateResponse,
    AIClassifyRequest,
    AIClassifyResponse,
    AICompleteRequest,
    AICompleteResponse,
    AIFilterRequest,
    AIFilterResponse,
    AISentimentRequest,
    AISentimentResponse,
    AITranscribeRequest,
    AITranscribeResponse,
    ExtractStructuredDataRequest,
    ExtractStructuredDataResponse,
    SemanticJoinRequest,
    SemanticJoinResponse,
    SummarizeRequest,
    SummarizeResponse,
)
from ...services.modular_ai_sql_service import ModularAISQLService
from ...services.snowflake_service import SnowflakeService

router = APIRouter(prefix="/ai-sql", tags=["AI SQL"])

# Service initialization will be done via dependency injection
# For now, we'll initialize in each endpoint


async def get_ai_sql_service() -> ModularAISQLService:
    """Get AI SQL service instance."""
    snowflake_service = SnowflakeService()
    return ModularAISQLService(snowflake_service)


# ============================================================================
# AI_COMPLETE Endpoints
# ============================================================================


@router.post(
    "/complete",
    response_model=AICompleteResponse,
    dependencies=[Depends(rate_limiter_dependency)],
    summary="AI Complete - LLM Text Generation",
    description="Generate text using LLM models (Claude, Mistral, etc.)",
)
async def ai_complete(
    request: AICompleteRequest,
    service: ModularAISQLService = Depends(get_ai_sql_service),
) -> AICompleteResponse:
    """Execute AI_COMPLETE for text generation."""
    try:
        response = await service.ai_complete(
            model=request.model,
            prompt=request.prompt,
            response_format=request.response_format,
        )
        return AICompleteResponse(response=response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# AI_TRANSCRIBE Endpoints
# ============================================================================


@router.post(
    "/transcribe",
    response_model=AITranscribeResponse,
    dependencies=[Depends(rate_limiter_dependency)],
    summary="AI Transcribe - Audio to Text",
    description="Transcribe audio files to text",
)
async def ai_transcribe(
    request: AITranscribeRequest,
    service: ModularAISQLService = Depends(get_ai_sql_service),
) -> AITranscribeResponse:
    """Transcribe audio files."""
    try:
        results = await service.ai_transcribe(
            audio_file_column=request.audio_file_column,
            table_name=request.table_name,
        )
        return AITranscribeResponse(results=results)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# AI_CLASSIFY Endpoints
# ============================================================================


@router.post(
    "/classify",
    response_model=AIClassifyResponse,
    dependencies=[Depends(rate_limiter_dependency)],
    summary="AI Classify - Multi-label Classification",
    description="Classify content into multiple categories",
)
async def ai_classify(
    request: AIClassifyRequest,
    service: ModularAISQLService = Depends(get_ai_sql_service),
) -> AIClassifyResponse:
    """Classify content using AI."""
    try:
        results = await service.ai_classify(
            content_column=request.content_column,
            categories=request.categories,
            table_name=request.table_name,
            prompt_prefix=request.prompt_prefix,
        )
        return AIClassifyResponse(results=results)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# AI_FILTER Endpoints
# ============================================================================


@router.post(
    "/filter",
    response_model=AIFilterResponse,
    dependencies=[Depends(rate_limiter_dependency)],
    summary="AI Filter - Intelligent Data Filtering",
    description="Filter data using natural language conditions",
)
async def ai_filter(
    request: AIFilterRequest,
    service: ModularAISQLService = Depends(get_ai_sql_service),
) -> AIFilterResponse:
    """Filter data using AI."""
    try:
        results = await service.ai_filter(
            filter_condition=request.filter_condition,
            table_name=request.table_name,
            columns=request.columns,
        )
        return AIFilterResponse(results=results)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# AI_AGG Endpoints
# ============================================================================


@router.post(
    "/aggregate",
    response_model=AIAggregateResponse,
    dependencies=[Depends(rate_limiter_dependency)],
    summary="AI Aggregate - AI-Powered Aggregation",
    description="Aggregate data across rows using AI",
)
async def ai_aggregate(
    request: AIAggregateRequest,
    service: ModularAISQLService = Depends(get_ai_sql_service),
) -> AIAggregateResponse:
    """Aggregate data using AI."""
    try:
        results = await service.ai_aggregate(
            column_to_aggregate=request.column_to_aggregate,
            aggregation_prompt=request.aggregation_prompt,
            table_name=request.table_name,
            group_by=request.group_by,
        )
        return AIAggregateResponse(results=results)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# AI_SENTIMENT Endpoints
# ============================================================================


@router.post(
    "/sentiment",
    response_model=AISentimentResponse,
    dependencies=[Depends(rate_limiter_dependency)],
    summary="AI Sentiment - Sentiment Analysis",
    description="Analyze sentiment of text content",
)
async def ai_sentiment(
    request: AISentimentRequest,
    service: ModularAISQLService = Depends(get_ai_sql_service),
) -> AISentimentResponse:
    """Analyze sentiment."""
    try:
        results = await service.ai_sentiment(
            text_column=request.text_column,
            table_name=request.table_name,
        )
        return AISentimentResponse(results=results)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# SUMMARIZE Endpoints
# ============================================================================


@router.post(
    "/summarize",
    response_model=SummarizeResponse,
    dependencies=[Depends(rate_limiter_dependency)],
    summary="Summarize - Text Summarization",
    description="Summarize text content",
)
async def summarize(
    request: SummarizeRequest,
    service: ModularAISQLService = Depends(get_ai_sql_service),
) -> SummarizeResponse:
    """Summarize text."""
    try:
        results = await service.summarize(
            text_column=request.text_column,
            table_name=request.table_name,
        )
        return SummarizeResponse(results=results)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Advanced Use Case Endpoints
# ============================================================================


@router.post(
    "/semantic-join",
    response_model=SemanticJoinResponse,
    dependencies=[Depends(rate_limiter_dependency)],
    summary="Semantic JOIN - AI-Powered Table Joining",
    description="Join tables using natural language conditions",
)
async def semantic_join(
    request: SemanticJoinRequest,
    service: ModularAISQLService = Depends(get_ai_sql_service),
) -> SemanticJoinResponse:
    """Perform semantic JOIN."""
    try:
        results = await service.semantic_join(
            left_table=request.left_table,
            right_table=request.right_table,
            left_column=request.left_column,
            right_column=request.right_column,
            join_condition=request.join_condition,
        )
        return SemanticJoinResponse(results=results)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/extract-structured",
    response_model=ExtractStructuredDataResponse,
    dependencies=[Depends(rate_limiter_dependency)],
    summary="Extract Structured Data - Convert Text to Structured Format",
    description="Extract structured data from unstructured text",
)
async def extract_structured_data(
    request: ExtractStructuredDataRequest,
    service: ModularAISQLService = Depends(get_ai_sql_service),
) -> ExtractStructuredDataResponse:
    """Extract structured data from text."""
    try:
        results = await service.extract_structured_data(
            text_column=request.text_column,
            table_name=request.table_name,
            extraction_prompt=request.extraction_prompt,
            schema=request.schema,
        )
        return ExtractStructuredDataResponse(results=results)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
