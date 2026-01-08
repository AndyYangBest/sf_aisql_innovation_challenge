"""API endpoints for Snowflake Cortex AI SQL functions."""

from typing import Any
from fastapi import APIRouter, Depends, HTTPException
import json
from pydantic import BaseModel, Field

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


# ============================================================================
# Request/Response Models for New Endpoints
# ============================================================================


class ExecuteSQLRequest(BaseModel):
    """Request model for executing SQL queries."""
    sql: str = Field(..., description="SQL query to execute")
    limit: int = Field(50, ge=1, le=1000, description="Maximum number of rows to return")


class ExecuteSQLResponse(BaseModel):
    """Response model for SQL execution."""
    success: bool
    columns: list[dict[str, str]]
    rows: list[dict[str, Any]]
    row_count: int
    error: str | None = None


class SuggestMetadataRequest(BaseModel):
    """Request model for AI metadata suggestion."""
    sql: str = Field(..., description="SQL query to analyze")
    table_name: str | None = Field(None, description="Suggested table name from SQL")
    columns: list[dict[str, str]] | None = Field(None, description="Column definitions from query result")
    sample_rows: list[dict[str, Any]] | None = Field(None, description="Sample rows from query result")


class SuggestMetadataResponse(BaseModel):
    """Response model for metadata suggestion."""
    success: bool
    suggested_name: str
    suggested_tags: list[str]
    ai_summary: str | None = None
    use_cases: list[str] = []
    error: str | None = None


# Service initialization will be done via dependency injection
# For now, we'll initialize in each endpoint


async def get_ai_sql_service() -> ModularAISQLService:
    """Get AI SQL service instance."""
    snowflake_service = SnowflakeService()
    return ModularAISQLService(snowflake_service)


async def get_snowflake_service() -> SnowflakeService:
    """Get Snowflake service instance."""
    return SnowflakeService()


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


# ============================================================================
# SQL Execution & Metadata Suggestion Endpoints
# ============================================================================


@router.post(
    "/execute",
    response_model=ExecuteSQLResponse,
    dependencies=[Depends(rate_limiter_dependency)],
    summary="Execute SQL Query",
    description="Execute a SQL query on Snowflake and return results (limited to specified rows)",
)
async def execute_sql(
    request: ExecuteSQLRequest,
    service: SnowflakeService = Depends(get_snowflake_service),
) -> ExecuteSQLResponse:
    """Execute SQL query and return results."""
    async def run_json_fallback(clean_sql: str) -> ExecuteSQLResponse:
        """Fallback: return rows via JSON serialization to dodge driver parsing issues."""
        json_sql = f"""
        SELECT TO_JSON(OBJECT_CONSTRUCT(*)) AS ROW_JSON
        FROM (
          {clean_sql}
        )
        """
        try:
            json_rows = await service.execute_query(json_sql)
            parsed_rows = []
            for r in json_rows:
                raw = r.get("ROW_JSON")
                if raw is None:
                    parsed_rows.append({})
                    continue
                try:
                    parsed_rows.append(json.loads(raw))
                except Exception:
                    parsed_rows.append({"ROW_JSON": raw})

            columns = [{"name": k, "type": "VARIANT"} for k in (parsed_rows[0].keys() if parsed_rows else [])]
            return ExecuteSQLResponse(
                success=True,
                columns=columns,
                rows=parsed_rows,
                row_count=len(parsed_rows),
                error=None,
            )
        except Exception as fallback_error:
            return ExecuteSQLResponse(
                success=False,
                columns=[],
                rows=[],
                row_count=0,
                error=str(fallback_error),
            )

    try:
        # Add LIMIT to SQL if not already present
        sql = request.sql.strip()
        if not sql.upper().endswith(";"):
            sql = sql.rstrip(";")

        # Check if LIMIT already exists
        if "LIMIT" not in sql.upper():
            sql = f"{sql} LIMIT {request.limit}"

        wrapped_sql = f"SELECT * FROM ({sql})"

        # Execute query
        rows = await service.execute_query(wrapped_sql)

        # Extract column names from first row (if exists)
        columns = [{"name": col, "type": "VARCHAR"} for col in rows[0].keys()] if rows else []

        return ExecuteSQLResponse(
            success=True,
            columns=columns,
            rows=rows,
            row_count=len(rows),
            error=None,
        )
    except Exception as e:
        # Fallback: JSON serialization path to avoid timestamp parsing issues
        err_text = str(e)
        if "Timestamp" in err_text or "seconds_since_epoch" in err_text or "100035" in err_text:
            return await run_json_fallback(sql)
        return ExecuteSQLResponse(
            success=False,
            columns=[],
            rows=[],
            row_count=0,
            error=err_text,
        )


@router.post(
    "/suggest-metadata",
    response_model=SuggestMetadataResponse,
    dependencies=[Depends(rate_limiter_dependency)],
    summary="Suggest Metadata using AI",
    description="Use AI to suggest table name, tags, summary, and use cases based on SQL query",
)
async def suggest_metadata(
    request: SuggestMetadataRequest,
    ai_service: ModularAISQLService = Depends(get_ai_sql_service),
) -> SuggestMetadataResponse:
    """
    Generate metadata suggestions using AI or smart fallback.

    This endpoint uses AI to analyze SQL queries and their results to generate
    meaningful metadata. If AI is unavailable, it falls back to SQL parsing.

    Prompts are managed in app/config/prompts.py for easy modification.
    """
    from ...services.ai_helpers import AIMetadataGenerator, smart_metadata_fallback

    try:
        # Initialize AI metadata generator
        generator = AIMetadataGenerator(ai_service)

        print(f"[AI Metadata] Generating suggestions for SQL: {request.sql[:100]}...")
        print(f"[AI Metadata] Columns: {len(request.columns) if request.columns else 0}")
        print(f"[AI Metadata] Sample rows: {len(request.sample_rows) if request.sample_rows else 0}")

        # Try AI-powered generation
        metadata = await generator.suggest_table_metadata(
            sql=request.sql,
            columns=request.columns,
            sample_rows=request.sample_rows,
        )

        print(f"[AI Metadata] Successfully generated AI metadata")
        print(f"[AI Metadata] Name: {metadata['table_name']}, Tags: {metadata['tags']}")

        return SuggestMetadataResponse(
            success=True,
            suggested_name=metadata["table_name"],
            suggested_tags=metadata["tags"],
            ai_summary=metadata["summary"],
            use_cases=metadata["use_cases"],
            error=None,
        )

    except Exception as e:
        # Log error and use smart fallback without noisy traceback
        print(f"[AI Metadata] AI generation failed: {str(e)}, using smart fallback")

        # Generate metadata using SQL parsing fallback
        metadata = smart_metadata_fallback(request.sql, request.table_name)

        print(f"[AI Metadata] Smart fallback generated metadata")
        print(f"[AI Metadata] Name: {metadata['table_name']}, Tags: {metadata['tags']}")

        return SuggestMetadataResponse(
            success=True,
            suggested_name=metadata["table_name"],
            suggested_tags=metadata["tags"],
            ai_summary=metadata["summary"],
            use_cases=metadata["use_cases"],
            error=None,
        )
