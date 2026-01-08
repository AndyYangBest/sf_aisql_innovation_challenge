"""Simplified test for EDA Workflows with Type Detection.

This test focuses on workflow execution with detailed logging via hooks.
"""

import asyncio
import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Load environment variables
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = BASE_DIR.parent

env_candidates = [
    REPO_ROOT / ".env",
    BASE_DIR / "src" / ".env",
    BASE_DIR / "src" / "app" / ".env",
]
for env_file in env_candidates:
    if env_file.exists():
        load_dotenv(env_file, override=True)

os.environ.setdefault("STRANDS_TELEMETRY_DISABLED", "1")
os.environ.setdefault("STRANDS_METRICS_DISABLED", "1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")

from app.services.snowflake_service import SnowflakeService
from app.services.modular_ai_sql_service import ModularAISQLService
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


async def test_workflow_with_type_detection(asset_id: int, workflow_type: str = None):
    """Test a single workflow with type detection integration.

    Args:
        asset_id: ID of table asset to analyze
        workflow_type: Optional workflow type (EDA_OVERVIEW, EDA_TIME_SERIES, EDA_DATA_QUALITY)
    """
    print("\n" + "=" * 80)
    print(f"🧪 TESTING WORKFLOW WITH TYPE DETECTION AND PERSISTENCE")
    print("=" * 80 + "\n")

    # Get table asset and database session
    print(f"📊 Fetching table asset ID {asset_id} from PostgreSQL...")

    async for db in async_get_db():
        result = await db.execute(
            select(TableAsset).where(
                TableAsset.id == asset_id,
                TableAsset.is_deleted == False,
            )
        )
        table_asset = result.scalar_one_or_none()

        if not table_asset:
            print(f"❌ No table asset found with ID {asset_id}")
            return None

        print(f"✓ Found table asset: {table_asset.name}")
        print(f"  - Database: {table_asset.database}")
        print(f"  - Schema: {table_asset.schema}")
        print(f"  - Source SQL: {table_asset.source_sql[:100]}...")
        if table_asset.tags:
            print(f"  - Tags: {', '.join(table_asset.tags)}")
        print()

        # Initialize services
        sf_service = SnowflakeService()
        ai_sql_service = ModularAISQLService(sf_service)

        # Create orchestrator with database session for persistence
        orchestrator = create_eda_orchestrator(sf_service, ai_sql_service, db=db)

        # Run workflow
        print(f"🚀 Starting workflow execution with persistence...")
        print(f"   Workflow type: {workflow_type or 'Auto-routed'}")
        print()

        results = await orchestrator.run_eda(
            table_asset=table_asset,
            user_intent="Comprehensive analysis with semantic type detection",
            workflow_type=workflow_type,
        )

        # Display results
        print("\n" + "=" * 80)
        print("📋 WORKFLOW RESULTS")
        print("=" * 80 + "\n")

        print(f"Workflow Type: {results['workflow']}")
        print(f"Workflow ID: {results['workflow_id']}")
        print(f"Table: {results['table_name']}")
        print()

        # Display summary
        summary = results.get("summary", {})
        print("Summary:")
        print(f"  - Completed: {summary.get('completed', False)}")
        print(f"  - Progress: {summary.get('progress', 0)}%")
        print(f"  - Tasks Completed: {summary.get('tasks_completed', 0)}/{summary.get('tasks_total', 0)}")
        print()

        # Display artifacts
        artifacts = results.get("artifacts", {})
        print(f"Artifacts Generated: {len(artifacts)}")
        for artifact_name, artifact_data in artifacts.items():
            print(f"  ✓ {artifact_name}")

            # Show type inference info if available
            if artifact_name == "profile_table" and isinstance(artifact_data, dict):
                text_content = artifact_data.get("text", "")

                # Try to extract JSON from the text
                import re
                json_match = re.search(r'```json\s*(\{.*?\})\s*```', text_content, re.DOTALL)
                if json_match:
                    try:
                        import json
                        profile_json = json.loads(json_match.group(1))
                        metadata = profile_json.get("metadata", {})

                        # Show data structure type
                        structure_type = metadata.get("data_structure_type")
                        if structure_type:
                            print(f"    → Data Structure: {structure_type}")

                        # Show column type inferences
                        type_inferences = metadata.get("column_type_inferences", [])
                        if type_inferences:
                            print(f"    → Column Type Inferences: {len(type_inferences)} columns")
                            # Show first 5 as examples
                            for inference in type_inferences[:5]:
                                col_name = inference.get("column_name", "?")
                                inferred_type = inference.get("inferred_type", "?")
                                confidence = inference.get("confidence", 0)
                                print(f"      • {col_name}: {inferred_type} (confidence: {confidence:.2f})")
                            if len(type_inferences) > 5:
                                print(f"      ... and {len(type_inferences) - 5} more columns")
                    except Exception as e:
                        print(f"    [Could not parse profile JSON: {e}]")

        # Verify persistence
        print("\n" + "=" * 80)
        print("🗄️ VERIFYING DATABASE PERSISTENCE")
        print("=" * 80 + "\n")

        from app.services.eda_workflow_persistence import EDAWorkflowPersistenceService
        persistence = EDAWorkflowPersistenceService(db)

        execution = await persistence.get_execution(results['workflow_id'])
        if execution:
            print(f"✓ Workflow execution found in database:")
            print(f"  - ID: {execution.id}")
            print(f"  - Workflow ID: {execution.workflow_id}")
            print(f"  - Status: {execution.status}")
            print(f"  - Progress: {execution.progress}%")
            print(f"  - Tasks: {execution.tasks_completed}/{execution.tasks_total}")
            print(f"  - Data Structure Type: {execution.data_structure_type}")
            if execution.column_type_inferences:
                print(f"  - Column Type Inferences: {len(execution.column_type_inferences)} columns")
            print(f"  - Started: {execution.started_at}")
            print(f"  - Completed: {execution.completed_at}")
            if execution.duration_seconds:
                print(f"  - Duration: {execution.duration_seconds:.2f}s")
        else:
            print("❌ Workflow execution NOT found in database")

        print("\n" + "=" * 80)
        print("✅ WORKFLOW TEST COMPLETED")
        print("=" * 80 + "\n")

        return results


async def main():
    """Run workflow tests."""
    print("\n" + "=" * 80)
    print("🧪 EDA WORKFLOW TEST WITH TYPE DETECTION")
    print("=" * 80)

    # List available assets
    print("\n📋 Available Table Assets:")
    assets = await list_all_table_assets()
    for asset in assets:
        print(f"  - ID {asset.id}: {asset.name} ({asset.database}.{asset.schema})")
    print()

    if not assets:
        print("❌ No table assets found in database")
        return

    # Test with first asset (EDA_OVERVIEW workflow)
    print("\n" + "=" * 80)
    print("TEST 1: EDA_OVERVIEW Workflow")
    print("=" * 80)
    await test_workflow_with_type_detection(
        asset_id=assets[0].id,
        workflow_type="EDA_OVERVIEW"
    )

    # Test with second asset if available (auto-routing)
    if len(assets) > 1:
        print("\n" + "=" * 80)
        print("TEST 2: Auto-Routed Workflow")
        print("=" * 80)
        await test_workflow_with_type_detection(
            asset_id=assets[1].id,
            workflow_type=None  # Let router decide
        )

    print("\n" + "=" * 80)
    print("✅ ALL TESTS COMPLETED")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
