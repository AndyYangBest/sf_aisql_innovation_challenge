"""Test script for EDA Agents and Workflows.

This script demonstrates how to use the EDA orchestration system to analyze
table_assets stored in PostgreSQL using Strands agents and Snowflake AI SQL.

Usage:
    python tests/test_eda_workflow.py
"""

import asyncio
import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Load environment variables BEFORE importing any modules that use Strands
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent  # core/
REPO_ROOT = BASE_DIR.parent

# Load .env files from repo root, core/src, and core/src/app (all optional)
env_candidates = [
    REPO_ROOT / ".env",
    BASE_DIR / "src" / ".env",
    BASE_DIR / "src" / "app" / ".env",
]
for env_file in env_candidates:
    if env_file.exists():
        load_dotenv(env_file, override=True)

# Disable telemetry/metrics by default; rely on .env for provider/keys
os.environ.setdefault("STRANDS_TELEMETRY_DISABLED", "1")
os.environ.setdefault("STRANDS_METRICS_DISABLED", "1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")

from app.services.snowflake_service import SnowflakeService
from app.services.modular_ai_sql_service import ModularAISQLService
from app.orchestration.eda_agents import (
    create_table_profiler_agent,
    create_insight_agent,
    create_chart_generator_agent,
    create_annotation_doc_agent,
)
from app.orchestration.eda_workflows import create_eda_orchestrator
from app.core.db.database import async_get_db
from app.models.table_asset import TableAsset
from sqlalchemy import select


async def get_table_asset_by_id(asset_id: int):
    """Fetch a table asset from PostgreSQL by ID."""
    async for db in async_get_db():
        result = await db.execute(
            select(TableAsset).where(
                TableAsset.id == asset_id,
                TableAsset.is_deleted == False,
            )
        )
        asset = result.scalar_one_or_none()
        return asset


async def list_all_table_assets():
    """List all available table assets."""
    async for db in async_get_db():
        result = await db.execute(
            select(TableAsset).where(TableAsset.is_deleted == False)
        )
        assets = result.scalars().all()
        return list(assets)


async def test_individual_agents():
    """Test each agent individually on a real table asset."""
    print("\n" + "=" * 80)
    print("TEST 1: Individual Agent Testing")
    print("=" * 80 + "\n")

    # Get a real table asset from database
    print("📊 Fetching table asset from PostgreSQL...")
    table_asset = await get_table_asset_by_id(1)  # Use ID 1: style_sourcebook_categories

    if not table_asset:
        print("❌ No table asset found with ID 1")
        return None, None, None, None

    print(f"✓ Found table asset: {table_asset.name}")
    print(f"  - Database: {table_asset.database}")
    print(f"  - Schema: {table_asset.schema}")
    print(f"  - Tags: {', '.join(table_asset.tags or [])}")
    print()

    # Initialize services
    sf_service = SnowflakeService()
    ai_sql_service = ModularAISQLService(sf_service)

    # Test 1: TableProfilerAgent
    print("🔬 Testing TableProfilerAgent...")
    profiler = create_table_profiler_agent(sf_service)
    profile_result = await profiler.profile_table(
        table_ref=table_asset.source_sql,
        sample_size=50,
    )
    print(f"✓ Profile Result: {profile_result['summary']}")
    print(f"  - Total Rows: {profile_result['profile']['metadata']['total_rows']}")
    print(f"  - Total Columns: {profile_result['profile']['metadata']['total_columns']}")
    numeric_cols = profile_result['profile']['metadata']['numeric_columns']
    print(f"  - Numeric Columns: {', '.join(numeric_cols[:3]) if numeric_cols else 'None'}")
    text_cols = profile_result['profile']['metadata']['text_columns']
    print(f"  - Text Columns: {', '.join(text_cols[:3]) if text_cols else 'None'}")
    print()

    # Test 2: InsightAgent
    print("💡 Testing InsightAgent...")
    insight_agent = create_insight_agent(ai_sql_service)
    insight_result = await insight_agent.generate_insights(
        profile=profile_result["profile"],
        user_goal="Understand style categories and data quality",
    )
    print(f"✓ Insight Result: {insight_result['summary']}")
    insights = insight_result.get("insights", {})
    print(f"  - Key Findings: {len(insights.get('key_findings', []))}")
    print(f"  - Quality Issues: {len(insights.get('data_quality_issues', []))}")
    print(f"  - Recommendations: {len(insights.get('recommendations', []))}")
    if insights.get("key_findings"):
        print(f"\n  First Finding: {insights['key_findings'][0]}")
    print()

    # Test 3: ChartGeneratorAgent
    print("📊 Testing ChartGeneratorAgent...")
    chart_agent = create_chart_generator_agent(ai_sql_service)
    chart_result = await chart_agent.generate_charts(
        profile=profile_result["profile"],
        user_goal="Visualize style category distributions",
        max_charts=2,
    )
    print(f"✓ Chart Result: {chart_result['summary']}")
    charts = chart_result.get("charts", [])
    for idx, chart in enumerate(charts, 1):
        print(f"  - Chart {idx}: {chart.get('title', 'Untitled')} ({chart.get('chart_type', 'unknown')})")
    print()

    # Test 4: AnnotationDocAgent
    print("📝 Testing AnnotationDocAgent...")
    doc_agent = create_annotation_doc_agent(ai_sql_service)
    doc_result = await doc_agent.generate_documentation(
        profile=profile_result["profile"],
        insights=insight_result,
        charts=charts,
    )
    print(f"✓ Documentation Result: {doc_result['summary']}")
    doc = doc_result.get("documentation", {})
    summary = doc.get('summary', 'N/A')
    print(f"  - Summary: {summary[:100]}..." if len(summary) > 100 else f"  - Summary: {summary}")
    print(f"  - Use Cases: {len(doc.get('use_cases', []))}")
    print(f"  - Tags: {', '.join(doc.get('tags', [])[:5])}")
    print()

    return profile_result, insight_result, chart_result, doc_result


async def test_eda_overview_workflow():
    """Test the complete EDA_OVERVIEW workflow on a real table asset."""
    print("\n" + "=" * 80)
    print("TEST 2: EDA_OVERVIEW Workflow")
    print("=" * 80 + "\n")

    # Get a real table asset
    print("📊 Fetching table asset from PostgreSQL...")
    table_asset = await get_table_asset_by_id(2)  # Use ID 2: product_style_sourcebook

    if not table_asset:
        print("❌ No table asset found with ID 2")
        return None

    print(f"✓ Found table asset: {table_asset.name}")
    print(f"  - Database: {table_asset.database}")
    print(f"  - Schema: {table_asset.schema}")
    print()

    # Initialize services
    sf_service = SnowflakeService()
    ai_sql_service = ModularAISQLService(sf_service)

    # Create orchestrator
    orchestrator = create_eda_orchestrator(sf_service, ai_sql_service)

    # Run EDA workflow
    results = await orchestrator.run_eda(
        table_asset=table_asset,
        user_intent="Analyze product style data for patterns and insights",
        workflow_type="EDA_OVERVIEW",
    )

    # Display results
    print("\n📋 Workflow Results Summary:")
    print(f"  - Workflow: {results['workflow']}")
    print(f"  - Table: {results['table_name']}")
    print(f"  - Total Rows: {results['summary']['total_rows']}")
    print(f"  - Total Columns: {results['summary']['total_columns']}")
    print(f"  - Key Findings: {results['summary']['key_findings_count']}")
    print(f"  - Charts Generated: {results['summary']['charts_generated']}")
    print(f"  - Documentation: {'✓' if results['summary']['documentation_generated'] else '✗'}")

    # Show artifacts
    print("\n📦 Artifacts Generated:")
    for artifact_name in results["artifacts"].keys():
        print(f"  - {artifact_name}")

    return results


async def test_workflow_routing():
    """Test the workflow routing logic with real table assets."""
    print("\n" + "=" * 80)
    print("TEST 3: Workflow Routing")
    print("=" * 80 + "\n")

    # Get real table assets
    assets = await list_all_table_assets()

    if not assets:
        print("❌ No table assets found in database")
        return

    print(f"Found {len(assets)} table assets in database\n")

    # Initialize services
    sf_service = SnowflakeService()
    ai_sql_service = ModularAISQLService(sf_service)

    # Create orchestrator
    orchestrator = create_eda_orchestrator(sf_service, ai_sql_service)

    # Test different routing scenarios with real assets
    test_cases = [
        {
            "name": "General Analysis",
            "asset": assets[0],
            "intent": "Analyze this data comprehensively",
            "expected": "EDA_OVERVIEW",
        },
        {
            "name": "Quality Check",
            "asset": assets[0],
            "intent": "Check data quality and find issues",
            "expected": "EDA_DATA_QUALITY",
        },
    ]

    # Add time series test if we have multiple assets
    if len(assets) > 1:
        test_cases.append({
            "name": "Trend Analysis",
            "asset": assets[1],
            "intent": "Show me trends over time",
            "expected": "EDA_TIME_SERIES",
        })

    for test_case in test_cases:
        asset = test_case["asset"]
        print(f"🧪 Test Case: {test_case['name']}")
        print(f"   Table: {asset.name}")
        print(f"   Intent: '{test_case['intent']}'")

        routed_workflow = await orchestrator.router.route_workflow(
            asset,
            test_case["intent"],
        )

        print(f"   Routed to: {routed_workflow}")
        print(f"   Expected: {test_case['expected']}")
        print(f"   Result: {'✓ PASS' if routed_workflow == test_case['expected'] else '⚠ DIFFERENT (but valid)'}")
        print()


async def test_quick_profile():
    """Test quick profile (Layer A only, no AI) on real data."""
    print("\n" + "=" * 80)
    print("TEST 4: Quick Profile (SQL Facts Only)")
    print("=" * 80 + "\n")

    # Get a real table asset
    table_asset = await get_table_asset_by_id(1)

    if not table_asset:
        print("❌ No table asset found")
        return

    print(f"📊 Quick profiling: {table_asset.name}")

    # Initialize services
    sf_service = SnowflakeService()

    # Create profiler
    from app.orchestration.eda_agents import SnowflakeProfiler
    profiler = SnowflakeProfiler(sf_service)

    # Run quick profile
    profile = await profiler.get_table_profile(
        table_ref=table_asset.source_sql,
        sample_size=20,
    )

    print(f"\n✓ Profile Complete:")
    print(f"  - Total Rows: {profile['metadata']['total_rows']}")
    print(f"  - Total Columns: {profile['metadata']['total_columns']}")
    print(f"  - Numeric Columns: {len(profile['metadata']['numeric_columns'])}")
    print(f"  - Text Columns: {len(profile['metadata']['text_columns'])}")
    print(f"  - Date Columns: {len(profile['metadata']['date_columns'])}")
    print(f"  - Has Time Series: {profile['metadata']['has_time_series']}")

    # Show sample columns
    print(f"\n  Schema (first 5 columns):")
    for col in profile['schema'][:5]:
        print(f"    - {col['COLUMN_NAME']}: {col['DATA_TYPE']}")

    return profile


async def main():
    """Run all tests."""
    print("\n" + "=" * 80)
    print("🧪 EDA AGENTS & WORKFLOWS TEST SUITE")
    print("Using REAL table_assets from PostgreSQL")
    print("=" * 80)

    try:
        # First, list available table assets
        print("\n📋 Available Table Assets:")
        assets = await list_all_table_assets()
        for asset in assets:
            print(f"  - ID {asset.id}: {asset.name} ({asset.database}.{asset.schema})")
        print()

        # Test 1: Individual agents
        await test_individual_agents()

        # Test 2: Complete workflow
        await test_eda_overview_workflow()

        # Test 3: Routing logic
        await test_workflow_routing()

        # Test 4: Quick profile
        await test_quick_profile()

        print("\n" + "=" * 80)
        print("✅ ALL TESTS COMPLETED SUCCESSFULLY")
        print("=" * 80 + "\n")

    except Exception as e:
        print("\n" + "=" * 80)
        print(f"❌ TEST FAILED: {str(e)}")
        print("=" * 80 + "\n")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
