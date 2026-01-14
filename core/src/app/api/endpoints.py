"""
API Endpoints Configuration
Central registry of all available API endpoints
"""

from enum import Enum
from typing import Literal


class APIMethod(str, Enum):
    """HTTP methods for API endpoints."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"


class APIEndpoint:
    """Represents an API endpoint with metadata."""

    def __init__(
        self,
        path: str,
        method: APIMethod,
        description: str,
        tags: list[str] | None = None,
    ):
        self.path = path
        self.method = method
        self.description = description
        self.tags = tags or []

    def __repr__(self) -> str:
        return f"{self.method.value} {self.path}"


# ============================================================================
# Table Assets Endpoints
# ============================================================================

TABLE_ASSETS_ENDPOINTS = {
    "list": APIEndpoint(
        path="/api/v1/table-assets",
        method=APIMethod.GET,
        description="Get all table assets with pagination and filters",
        tags=["table-assets", "list"],
    ),
    "get": APIEndpoint(
        path="/api/v1/table-assets/{asset_id}",
        method=APIMethod.GET,
        description="Get a specific table asset by ID",
        tags=["table-assets", "get"],
    ),
    "create": APIEndpoint(
        path="/api/v1/table-assets",
        method=APIMethod.POST,
        description="Create a new table asset",
        tags=["table-assets", "create"],
    ),
    "update": APIEndpoint(
        path="/api/v1/table-assets/{asset_id}",
        method=APIMethod.PUT,
        description="Update an existing table asset",
        tags=["table-assets", "update"],
    ),
    "delete": APIEndpoint(
        path="/api/v1/table-assets/{asset_id}",
        method=APIMethod.DELETE,
        description="Delete a table asset (soft delete)",
        tags=["table-assets", "delete"],
    ),
}

# ============================================================================
# Snowflake Tables Endpoints
# ============================================================================

TABLES_ENDPOINTS = {
    "list": APIEndpoint(
        path="/api/v1/tables",
        method=APIMethod.GET,
        description="Get all Snowflake tables",
        tags=["tables", "snowflake"],
    ),
    "columns": APIEndpoint(
        path="/api/v1/tables/{table_name}/columns",
        method=APIMethod.GET,
        description="Get columns for a specific table",
        tags=["tables", "columns"],
    ),
    "sample": APIEndpoint(
        path="/api/v1/tables/{table_name}/sample",
        method=APIMethod.GET,
        description="Get sample data from a table",
        tags=["tables", "sample"],
    ),
}

# ============================================================================
# AI SQL Endpoints
# ============================================================================

AI_SQL_ENDPOINTS = {
    "execute": APIEndpoint(
        path="/api/v1/ai-sql/execute",
        method=APIMethod.POST,
        description="Execute a SQL query on Snowflake (limited to 50 rows)",
        tags=["ai-sql", "execute"],
    ),
    "suggest_metadata": APIEndpoint(
        path="/api/v1/ai-sql/suggest-metadata",
        method=APIMethod.POST,
        description="Use AI to suggest table name, tags, and metadata from SQL",
        tags=["ai-sql", "metadata", "ai"],
    ),
    "complete": APIEndpoint(
        path="/api/v1/ai-sql/complete",
        method=APIMethod.POST,
        description="AI text completion using LLM",
        tags=["ai-sql", "ai", "complete"],
    ),
    "classify": APIEndpoint(
        path="/api/v1/ai-sql/classify",
        method=APIMethod.POST,
        description="AI classification",
        tags=["ai-sql", "ai", "classify"],
    ),
    "filter": APIEndpoint(
        path="/api/v1/ai-sql/filter",
        method=APIMethod.POST,
        description="AI-powered data filtering",
        tags=["ai-sql", "ai", "filter"],
    ),
    "aggregate": APIEndpoint(
        path="/api/v1/ai-sql/aggregate",
        method=APIMethod.POST,
        description="AI-powered aggregation",
        tags=["ai-sql", "ai", "aggregate"],
    ),
    "sentiment": APIEndpoint(
        path="/api/v1/ai-sql/sentiment",
        method=APIMethod.POST,
        description="Sentiment analysis",
        tags=["ai-sql", "ai", "sentiment"],
    ),
    "summarize": APIEndpoint(
        path="/api/v1/ai-sql/summarize",
        method=APIMethod.POST,
        description="Text summarization",
        tags=["ai-sql", "ai", "summarize"],
    ),
    "transcribe": APIEndpoint(
        path="/api/v1/ai-sql/transcribe",
        method=APIMethod.POST,
        description="Audio transcription",
        tags=["ai-sql", "ai", "transcribe"],
    ),
    "semantic_join": APIEndpoint(
        path="/api/v1/ai-sql/semantic-join",
        method=APIMethod.POST,
        description="AI-powered semantic table joining",
        tags=["ai-sql", "ai", "join"],
    ),
    "extract_structured": APIEndpoint(
        path="/api/v1/ai-sql/extract-structured",
        method=APIMethod.POST,
        description="Extract structured data from text",
        tags=["ai-sql", "ai", "extract"],
    ),
}

# ============================================================================
# Column Metadata Endpoints
# ============================================================================

COLUMN_METADATA_ENDPOINTS = {
    "get": APIEndpoint(
        path="/api/v1/column-metadata/{table_asset_id}",
        method=APIMethod.GET,
        description="Fetch cached column metadata for a table asset",
        tags=["column-metadata", "cache"],
    ),
    "initialize": APIEndpoint(
        path="/api/v1/column-metadata/{table_asset_id}/initialize",
        method=APIMethod.POST,
        description="Initialize column metadata with sampling and inference",
        tags=["column-metadata", "init"],
    ),
    "override": APIEndpoint(
        path="/api/v1/column-metadata/{table_asset_id}/override",
        method=APIMethod.PUT,
        description="Override column metadata with user-provided hints",
        tags=["column-metadata", "override"],
    ),
    "table_override": APIEndpoint(
        path="/api/v1/column-metadata/{table_asset_id}/table-override",
        method=APIMethod.PUT,
        description="Override table metadata with user-provided hints",
        tags=["column-metadata", "override"],
    ),
}

# ============================================================================
# Column Workflow Endpoints
# ============================================================================

COLUMN_WORKFLOW_ENDPOINTS = {
    "estimate": APIEndpoint(
        path="/api/v1/column-workflows/{table_asset_id}/{column_name}/estimate",
        method=APIMethod.POST,
        description="Estimate token usage for a column workflow",
        tags=["column-workflows", "estimate"],
    ),
    "run": APIEndpoint(
        path="/api/v1/column-workflows/{table_asset_id}/{column_name}/run",
        method=APIMethod.POST,
        description="Run a column workflow using Strands",
        tags=["column-workflows", "run"],
    ),
}

# ============================================================================
# Health & System Endpoints
# ============================================================================

SYSTEM_ENDPOINTS = {
    "health": APIEndpoint(
        path="/api/v1/health",
        method=APIMethod.GET,
        description="Health check endpoint",
        tags=["system", "health"],
    ),
}

# ============================================================================
# All Endpoints Registry
# ============================================================================

ALL_ENDPOINTS = {
    "table_assets": TABLE_ASSETS_ENDPOINTS,
    "tables": TABLES_ENDPOINTS,
    "ai_sql": AI_SQL_ENDPOINTS,
    "column_metadata": COLUMN_METADATA_ENDPOINTS,
    "column_workflows": COLUMN_WORKFLOW_ENDPOINTS,
    "system": SYSTEM_ENDPOINTS,
}


def get_endpoint(category: str, name: str) -> APIEndpoint | None:
    """Get an endpoint by category and name."""
    return ALL_ENDPOINTS.get(category, {}).get(name)


def list_endpoints(category: str | None = None) -> list[APIEndpoint]:
    """List all endpoints, optionally filtered by category."""
    if category:
        return list(ALL_ENDPOINTS.get(category, {}).values())

    result = []
    for endpoints in ALL_ENDPOINTS.values():
        result.extend(endpoints.values())
    return result


def print_endpoints() -> None:
    """Print all endpoints in a readable format."""
    for category, endpoints in ALL_ENDPOINTS.items():
        print(f"\n{category.upper()} Endpoints:")
        print("=" * 80)
        for name, endpoint in endpoints.items():
            print(f"  {endpoint.method.value:6} {endpoint.path}")
            print(f"         {endpoint.description}")
            print(f"         Tags: {', '.join(endpoint.tags)}")
            print()


if __name__ == "__main__":
    # Print all endpoints when run as a script
    print_endpoints()
