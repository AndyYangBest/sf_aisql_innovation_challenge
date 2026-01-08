"""API endpoints for EDA (Exploratory Data Analysis) workflows.

This module provides REST API endpoints for running EDA workflows on table_assets
using the Strands-based agent orchestration system.
"""

from typing import Any, Literal
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ...core.db.database import get_async_db_session
from ...models.table_asset import TableAsset
from ...services.snowflake_service import SnowflakeService
from ...services.modular_ai_sql_service import ModularAISQLService
from ...orchestration.eda_workflows import (
    create_eda_orchestrator,
    WorkflowType,
)

router = APIRouter(prefix="/eda", tags=["EDA Workflows"])


# ============================================================================
# Request/Response Models
# ============================================================================


class EDARequest(BaseModel):
    """Request to run EDA on a table asset."""

    table_asset_id: int = Field(..., description="ID of the table asset to analyze")
    user_intent: str | None = Field(
        None,
        description="Optional user goal (e.g., 'find trends', 'check quality')",
    )
    workflow_type: WorkflowType | None = Field(
        None,
        description="Optional explicit workflow type (overrides auto-routing)",
    )


class EDAResponse(BaseModel):
    """Response from EDA workflow execution."""

    success: bool
    workflow: str
    table_asset_id: int
    table_name: str
    artifacts: dict[str, Any]
    summary: dict[str, Any]
    error: str | None = None


class WorkflowListResponse(BaseModel):
    """List of available EDA workflows."""

    workflows: list[dict[str, Any]]


class TableAssetListResponse(BaseModel):
    """List of table assets available for EDA."""

    table_assets: list[dict[str, Any]]
    total: int


# ============================================================================
# Dependency Injection
# ============================================================================


async def get_snowflake_service() -> SnowflakeService:
    """Get Snowflake service instance."""
    return SnowflakeService()


async def get_ai_sql_service(
    sf_service: SnowflakeService = Depends(get_snowflake_service),
) -> ModularAISQLService:
    """Get AI SQL service instance."""
    return ModularAISQLService(sf_service)


# ============================================================================
# API Endpoints
# ============================================================================


@router.post("/run", response_model=EDAResponse)
async def run_eda_workflow(
    request: EDARequest,
    db: AsyncSession = Depends(get_async_db_session),
    sf_service: SnowflakeService = Depends(get_snowflake_service),
    ai_sql_service: ModularAISQLService = Depends(get_ai_sql_service),
):
    """Run EDA workflow on a table asset.

    This endpoint:
    1. Retrieves the table asset from PostgreSQL
    2. Routes to appropriate workflow (or uses explicit workflow_type)
    3. Executes the workflow using Strands agents
    4. Persists workflow execution to database
    5. Returns comprehensive EDA results

    Workflows:
    - EDA_OVERVIEW: General comprehensive analysis (default)
    - EDA_TIME_SERIES: Time-series and trend analysis
    - EDA_DATA_QUALITY: Data quality validation and checks

    Example request:
    ```json
    {
        "table_asset_id": 1,
        "user_intent": "find trends and patterns",
        "workflow_type": null  // Auto-route
    }
    ```
    """
    try:
        # Fetch table asset from database
        result = await db.execute(
            select(TableAsset).where(
                TableAsset.id == request.table_asset_id,
                TableAsset.is_deleted == False,
            )
        )
        table_asset = result.scalar_one_or_none()

        if not table_asset:
            raise HTTPException(
                status_code=404,
                detail=f"Table asset with ID {request.table_asset_id} not found",
            )

        # Create orchestrator with database session for persistence
        orchestrator = create_eda_orchestrator(sf_service, ai_sql_service, db=db)

        # Run EDA workflow
        results = await orchestrator.run_eda(
            table_asset=table_asset,
            user_intent=request.user_intent,
            workflow_type=request.workflow_type,
        )

        return EDAResponse(
            success=True,
            workflow=results["workflow"],
            table_asset_id=results["table_asset_id"],
            table_name=results["table_name"],
            artifacts=results["artifacts"],
            summary=results["summary"],
            error=None,
        )

    except HTTPException:
        raise
    except Exception as e:
        return EDAResponse(
            success=False,
            workflow="",
            table_asset_id=request.table_asset_id,
            table_name="",
            artifacts={},
            summary={},
            error=str(e),
        )


@router.get("/workflows", response_model=WorkflowListResponse)
async def list_workflows():
    """List all available EDA workflows with descriptions.

    Returns information about each workflow type including:
    - Name and description
    - When to use it
    - What artifacts it produces
    """
    workflows = [
        {
            "type": "EDA_OVERVIEW",
            "name": "Comprehensive Overview",
            "description": "General-purpose EDA with profiling, insights, charts, and documentation",
            "use_when": "Default workflow for comprehensive table analysis",
            "artifacts": ["profile", "insights", "charts", "documentation"],
            "steps": [
                "Profile table (schema, stats, samples)",
                "Generate AI-powered insights",
                "Create visualization specifications",
                "Generate comprehensive documentation",
            ],
        },
        {
            "type": "EDA_TIME_SERIES",
            "name": "Time Series Analysis",
            "description": "Focused on temporal patterns, trends, and seasonality",
            "use_when": "Table has date/timestamp columns or user wants trend analysis",
            "artifacts": ["profile", "charts", "insights", "documentation"],
            "steps": [
                "Profile table with time-series focus",
                "Generate time-series visualizations",
                "Analyze temporal patterns and trends",
                "Generate time-series documentation",
            ],
        },
        {
            "type": "EDA_DATA_QUALITY",
            "name": "Data Quality Check",
            "description": "Focused on validation, completeness, and quality issues",
            "use_when": "User wants to check data quality or validate data",
            "artifacts": ["profile", "insights", "documentation"],
            "steps": [
                "Profile table for quality metrics",
                "Identify quality issues (nulls, duplicates, outliers)",
                "Generate quality report with recommendations",
            ],
        },
    ]

    return WorkflowListResponse(workflows=workflows)


@router.get("/table-assets", response_model=TableAssetListResponse)
async def list_table_assets(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_async_db_session),
):
    """List all table assets available for EDA.

    Returns table assets from PostgreSQL that can be analyzed.
    """
    try:
        # Query table assets
        result = await db.execute(
            select(TableAsset)
            .where(TableAsset.is_deleted == False)
            .order_by(TableAsset.created_at.desc())
            .offset(skip)
            .limit(limit)
        )
        table_assets = result.scalars().all()

        # Count total
        count_result = await db.execute(
            select(TableAsset).where(TableAsset.is_deleted == False)
        )
        total = len(count_result.scalars().all())

        # Format response
        assets_list = [
            {
                "id": asset.id,
                "name": asset.name,
                "source_sql": asset.source_sql[:100] + "..."
                if len(asset.source_sql) > 100
                else asset.source_sql,
                "database": asset.database,
                "schema": asset.schema,
                "tags": asset.tags or [],
                "owner": asset.owner,
                "ai_summary": asset.ai_summary,
                "use_cases": asset.use_cases or [],
                "created_at": asset.created_at.isoformat() if asset.created_at else None,
            }
            for asset in table_assets
        ]

        return TableAssetListResponse(
            table_assets=assets_list,
            total=total,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/table-assets/{table_asset_id}")
async def get_table_asset(
    table_asset_id: int,
    db: AsyncSession = Depends(get_async_db_session),
):
    """Get details of a specific table asset."""
    try:
        result = await db.execute(
            select(TableAsset).where(
                TableAsset.id == table_asset_id,
                TableAsset.is_deleted == False,
            )
        )
        table_asset = result.scalar_one_or_none()

        if not table_asset:
            raise HTTPException(
                status_code=404,
                detail=f"Table asset with ID {table_asset_id} not found",
            )

        return {
            "id": table_asset.id,
            "name": table_asset.name,
            "source_sql": table_asset.source_sql,
            "database": table_asset.database,
            "schema": table_asset.schema,
            "tags": table_asset.tags or [],
            "owner": table_asset.owner,
            "ai_summary": table_asset.ai_summary,
            "use_cases": table_asset.use_cases or [],
            "created_at": table_asset.created_at.isoformat() if table_asset.created_at else None,
            "updated_at": table_asset.updated_at.isoformat() if table_asset.updated_at else None,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/quick-profile/{table_asset_id}")
async def quick_profile(
    table_asset_id: int,
    db: AsyncSession = Depends(get_async_db_session),
    sf_service: SnowflakeService = Depends(get_snowflake_service),
):
    """Run a quick profile on a table asset (just Layer A: SQL facts).

    This is faster than full EDA and useful for quick checks.
    """
    try:
        # Fetch table asset
        result = await db.execute(
            select(TableAsset).where(
                TableAsset.id == table_asset_id,
                TableAsset.is_deleted == False,
            )
        )
        table_asset = result.scalar_one_or_none()

        if not table_asset:
            raise HTTPException(
                status_code=404,
                detail=f"Table asset with ID {table_asset_id} not found",
            )

        # Create profiler and run quick profile
        from ...orchestration.eda_agents import SnowflakeProfiler

        profiler = SnowflakeProfiler(sf_service)
        profile = await profiler.get_table_profile(
            table_ref=table_asset.source_sql,
            sample_size=50,
        )

        return {
            "success": True,
            "table_asset_id": table_asset_id,
            "table_name": table_asset.name,
            "profile": profile,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Health Check
# ============================================================================


@router.get("/health")
async def eda_health_check():
    """Check if the EDA orchestration system is healthy."""
    return {
        "status": "healthy",
        "workflows_available": ["EDA_OVERVIEW", "EDA_TIME_SERIES", "EDA_DATA_QUALITY"],
        "agents": [
            "TableProfilerAgent",
            "InsightAgent",
            "ChartGeneratorAgent",
            "AnnotationDocAgent",
        ],
        "version": "1.0.0",
    }
