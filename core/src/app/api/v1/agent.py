"""API endpoints for AI SQL Agent orchestration layer.

This module provides REST API endpoints for interacting with the AI SQL Agent.
"""

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from ...core.db.database import get_snowflake_service
from ...services.snowflake_service import SnowflakeService
from ..agent import AgentSession, AISQLAgent

router = APIRouter(prefix="/agent", tags=["AI SQL Agent"])

# In-memory session storage (replace with Redis/DB in production)
_sessions: dict[str, AgentSession] = {}


# ============================================================================
# Request/Response Models
# ============================================================================


class AgentRequest(BaseModel):
    """Request to the AI SQL Agent."""

    query: str = Field(..., description="Natural language query")
    session_id: str | None = Field(None, description="Session ID for context persistence")
    context: dict[str, Any] | None = Field(
        None, description="Additional context (table names, schemas, etc.)"
    )


class AgentResponse(BaseModel):
    """Response from the AI SQL Agent."""

    session_id: str
    tool_used: str | None = None
    parameters: dict[str, Any] | None = None
    results: Any = None
    summary: str | None = None
    sql_preview: str | None = None
    error: str | None = None
    suggestions: list[str] | None = None


class ToolsListResponse(BaseModel):
    """List of available tools."""

    tools: list[dict[str, Any]]
    categories: list[str]


class SessionContextUpdate(BaseModel):
    """Update session context."""

    context: dict[str, Any]


class SessionInfo(BaseModel):
    """Session information."""

    session_id: str
    context: dict[str, Any]
    conversation_length: int
    results_count: int


# ============================================================================
# Helper Functions
# ============================================================================


def get_or_create_session(
    session_id: str | None,
    sf_service: SnowflakeService,
    initial_context: dict[str, Any] | None = None,
) -> AgentSession:
    """Get existing session or create new one."""
    if session_id and session_id in _sessions:
        session = _sessions[session_id]
        if initial_context:
            session.update_context(initial_context)
        return session

    # Create new session
    import uuid

    new_session_id = session_id or str(uuid.uuid4())
    agent = AISQLAgent(sf_service)
    session = AgentSession(agent, new_session_id, initial_context)
    _sessions[new_session_id] = session
    return session


# ============================================================================
# API Endpoints
# ============================================================================


@router.post("/query", response_model=AgentResponse)
async def process_agent_query(
    request: AgentRequest,
    sf_service: SnowflakeService = Depends(get_snowflake_service),
):
    """Process a natural language query using the AI SQL Agent.

    The agent will:
    1. Understand your intent
    2. Select the appropriate AI SQL tool
    3. Extract parameters from your query
    4. Execute the operation
    5. Return results with a human-readable summary

    Example requests:
    - "Analyze sentiment of customer reviews in the reviews table"
    - "Classify products into Electronics, Clothing, or Food categories"
    - "Translate product descriptions from English to Spanish"
    - "Find all reviews that mention shipping issues"
    """
    try:
        session = get_or_create_session(request.session_id, sf_service, request.context)

        result = await session.process(request.query)

        # Get suggestions for next actions
        suggestions = None
        if not result.get("error"):
            suggestions = await session.suggest_next()

        return AgentResponse(
            session_id=session.session_id,
            tool_used=result.get("tool_used"),
            parameters=result.get("parameters"),
            results=result.get("results"),
            summary=result.get("summary"),
            sql_preview=result.get("sql_preview"),
            error=result.get("error"),
            suggestions=suggestions,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tools", response_model=ToolsListResponse)
async def list_available_tools(
    sf_service: SnowflakeService = Depends(get_snowflake_service),
):
    """List all available AI SQL tools with descriptions and examples.

    Returns tools organized by category:
    - Text Generation
    - Text Analysis
    - Text Transformation
    - Data Extraction
    - Aggregation
    - Semantic Operations
    - Utility
    """
    agent = AISQLAgent(sf_service)
    tools_info = agent.get_available_tools()
    return ToolsListResponse(
        tools=tools_info["tools"], categories=tools_info["categories"]
    )


@router.get("/tools/category/{category}")
async def get_tools_by_category(
    category: str,
    sf_service: SnowflakeService = Depends(get_snowflake_service),
):
    """Get tools filtered by category.

    Categories:
    - text_generation
    - text_analysis
    - text_transformation
    - data_extraction
    - aggregation
    - semantic_operations
    - utility
    """
    agent = AISQLAgent(sf_service)
    tools = agent.get_tool_by_category(category)

    if not tools:
        raise HTTPException(
            status_code=404,
            detail=f"Category '{category}' not found or has no tools",
        )

    return {"category": category, "tools": tools}


@router.get("/session/{session_id}", response_model=SessionInfo)
async def get_session_info(session_id: str):
    """Get information about a session."""
    if session_id not in _sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    session = _sessions[session_id]
    return SessionInfo(
        session_id=session.session_id,
        context=session.get_context(),
        conversation_length=len(session.agent.get_conversation_history()),
        results_count=len(session.get_results_history()),
    )


@router.put("/session/{session_id}/context")
async def update_session_context(session_id: str, update: SessionContextUpdate):
    """Update context for a session.

    Use this to provide additional information like:
    - Available tables and schemas
    - Default columns to use
    - User preferences
    """
    if session_id not in _sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    session = _sessions[session_id]
    session.update_context(update.context)

    return {"message": "Context updated", "context": session.get_context()}


@router.delete("/session/{session_id}")
async def delete_session(session_id: str):
    """Delete a session and clear its history."""
    if session_id not in _sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    del _sessions[session_id]
    return {"message": "Session deleted"}


@router.get("/session/{session_id}/history")
async def get_session_history(session_id: str):
    """Get conversation history for a session."""
    if session_id not in _sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    session = _sessions[session_id]
    return {
        "session_id": session_id,
        "conversation": session.agent.get_conversation_history(),
        "results": session.get_results_history(),
    }


@router.post("/session/{session_id}/suggest")
async def suggest_next_actions(session_id: str):
    """Get suggestions for next actions based on session history."""
    if session_id not in _sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    session = _sessions[session_id]
    suggestions = await session.suggest_next()

    return {"session_id": session_id, "suggestions": suggestions}


# ============================================================================
# Health Check
# ============================================================================


@router.get("/health")
async def agent_health_check():
    """Check if the agent orchestration layer is healthy."""
    return {
        "status": "healthy",
        "active_sessions": len(_sessions),
        "version": "1.0.0",
    }
