#!/usr/bin/env python3
"""
Table Analyzer - Simulates frontend requests to AI SQL endpoints
Analyzes table structure using AI_COMPLETE with configurable database/table selection
"""

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).parent / "core" / "src"))

from app.core.db.database import get_snowflake_connection
from app.services.modular_ai_sql_service import ModularAISQLService
from app.services.snowflake_service import SnowflakeService


class DatabaseConfig:
    """Load and manage database configuration from YAML."""

    def __init__(self, config_path: str = "database_config.yaml"):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

    @property
    def user_info(self) -> dict[str, Any]:
        """Get user information."""
        return self.config.get("user", {})

    @property
    def database(self) -> str:
        """Get target database."""
        return self.config["analysis_target"]["database"]

    @property
    def schema(self) -> str:
        """Get target schema."""
        return self.config["analysis_target"]["schema"]

    @property
    def enabled_tables(self) -> list[dict[str, Any]]:
        """Get list of enabled tables."""
        return [
            t
            for t in self.config["analysis_target"]["tables"]
            if t.get("enabled", True)
        ]

    @property
    def ai_model(self) -> str:
        """Get AI model name."""
        return self.config.get("ai_config", {}).get("model", "mistral-large")


class TableAnalyzer:
    """Analyze database tables using AI_COMPLETE endpoint."""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.sf_service = SnowflakeService()
        self.ai_service = ModularAISQLService(self.sf_service)
        self.results = []

    async def sample_table(
        self, table_name: str, sample_size: int = 10
    ) -> list[dict[str, Any]]:
        """Sample random rows from a table."""
        full_table_name = f"{self.config.database}.{self.config.schema}.{table_name}"

        # First get column info to handle timestamps
        try:
            columns = await self.sf_service.get_table_columns(table_name)

            # Build select with timestamp conversion
            select_parts = []
            for col in columns:
                col_name = col['COLUMN_NAME']
                col_type = col['DATA_TYPE']

                if 'TIMESTAMP' in col_type or 'DATE' in col_type or 'TIME' in col_type:
                    # Convert timestamps to strings to avoid conversion errors
                    select_parts.append(f"TO_CHAR({col_name}) as {col_name}")
                else:
                    select_parts.append(col_name)

            select_clause = ", ".join(select_parts)

            query = f"""
            SELECT {select_clause}
            FROM {full_table_name}
            SAMPLE ({sample_size} ROWS)
            """

            result = await self.sf_service.execute_query(query)
            return result
        except Exception as e:
            print(f"  ✗ Error sampling table {table_name}: {e}")
            return []

    def format_sample_for_ai(
        self, table_name: str, sample_data: list[dict[str, Any]]
    ) -> str:
        """Format sample data for AI analysis."""
        if not sample_data:
            return f"Table: {table_name}\nNo data available."

        # Get column names and types
        columns = list(sample_data[0].keys())

        # Format sample rows
        sample_rows = []
        for i, row in enumerate(sample_data[:5], 1):  # Show only first 5 rows in prompt
            sample_rows.append(f"Row {i}: {json.dumps(row, default=str, indent=2)}")

        prompt = f"""
Table: {table_name}
Total Columns: {len(columns)}
Columns: {', '.join(columns)}

Sample Data (first 5 of {len(sample_data)} rows):
{chr(10).join(sample_rows)}

Please analyze this table and provide:
1. Column names and their inferred data types
2. Brief description of what each column represents
3. Overall purpose of this table
4. Any data quality observations

Return your analysis in JSON format:
{{
  "table_name": "...",
  "total_columns": ...,
  "columns": [
    {{"name": "...", "type": "...", "description": "..."}},
    ...
  ],
  "table_purpose": "...",
  "data_quality_notes": "..."
}}
"""
        return prompt

    async def analyze_table(self, table_info: dict[str, Any]) -> dict[str, Any]:
        """Analyze a single table using AI_COMPLETE."""
        table_name = table_info["name"]
        sample_size = table_info.get("sample_size", 10)

        print(f"\n{'=' * 70}")
        print(f"📊 Analyzing: {table_name}")
        print(f"{'=' * 70}")

        # Step 1: Sample the table
        print(f"  🔍 Sampling {sample_size} rows...")
        sample_data = await self.sample_table(table_name, sample_size)

        if not sample_data:
            return {
                "table_name": table_name,
                "status": "error",
                "error": "Failed to sample table data",
            }

        print(f"  ✓ Retrieved {len(sample_data)} rows with {len(sample_data[0])} columns")

        # Step 2: Format prompt
        prompt = self.format_sample_for_ai(table_name, sample_data)

        # Step 3: Call AI_COMPLETE endpoint (simulated as direct service call)
        print(f"  🤖 Calling AI_COMPLETE with model: {self.config.ai_model}...")
        try:
            # This simulates calling POST /api/v1/ai-sql/complete
            ai_response = await self.ai_service.ai_complete(
                model=self.config.ai_model, prompt=prompt
            )

            print(f"  ✓ AI analysis completed")

            # Try to parse JSON response
            try:
                analysis = json.loads(ai_response)
            except json.JSONDecodeError:
                # If not JSON, wrap it
                analysis = {"raw_response": ai_response}

            return {
                "table_name": table_name,
                "status": "success",
                "sample_size": len(sample_data),
                "actual_columns": list(sample_data[0].keys()),
                "ai_analysis": analysis,
            }

        except Exception as e:
            print(f"  ✗ AI analysis failed: {e}")
            return {
                "table_name": table_name,
                "status": "error",
                "error": str(e),
            }

    async def analyze_all_tables(self) -> list[dict[str, Any]]:
        """Analyze all enabled tables."""
        enabled_tables = self.config.enabled_tables

        print(f"\n{'█' * 70}")
        print(f"  Table Structure Analyzer")
        print(f"  Database: {self.config.database}")
        print(f"  User: {self.config.user_info.get('username', 'Unknown')}")
        print(f"  Role: {self.config.user_info.get('role', 'Unknown')}")
        print(f"  AI Model: {self.config.ai_model}")
        print(f"{'█' * 70}")

        print(f"\n📋 Tables to analyze: {len(enabled_tables)}")
        for table in enabled_tables:
            print(f"  • {table['name']} ({table.get('sample_size', 10)} rows)")

        results = []
        for table_info in enabled_tables:
            result = await self.analyze_table(table_info)
            results.append(result)

        return results

    def print_summary(self, results: list[dict[str, Any]]):
        """Print summary of analysis results."""
        print(f"\n{'=' * 70}")
        print(f"📈 ANALYSIS SUMMARY")
        print(f"{'=' * 70}")

        successful = [r for r in results if r["status"] == "success"]
        failed = [r for r in results if r["status"] == "error"]

        print(f"\n✓ Successful: {len(successful)}/{len(results)}")
        print(f"✗ Failed: {len(failed)}/{len(results)}")

        if successful:
            print(f"\n{'─' * 70}")
            print("Detailed Results:")
            print(f"{'─' * 70}")

            for result in successful:
                print(f"\n🔹 {result['table_name']}")
                print(f"   Sample Size: {result.get('sample_size', 'N/A')} rows")
                print(f"   Actual Columns: {len(result.get('actual_columns', []))}")

                ai_analysis = result.get("ai_analysis", {})
                if isinstance(ai_analysis, dict):
                    print(f"   Table Purpose: {ai_analysis.get('table_purpose', 'N/A')}")

                    if "columns" in ai_analysis:
                        print(f"   AI Identified Columns:")
                        for col in ai_analysis["columns"][:3]:  # Show first 3
                            print(f"     • {col.get('name', 'N/A')}: {col.get('type', 'N/A')}")
                        if len(ai_analysis["columns"]) > 3:
                            print(f"     ... and {len(ai_analysis['columns']) - 3} more")

                    if "data_quality_notes" in ai_analysis:
                        print(f"   Quality Notes: {ai_analysis['data_quality_notes'][:100]}...")
                else:
                    print(f"   Raw Response: {str(ai_analysis)[:200]}...")

        if failed:
            print(f"\n{'─' * 70}")
            print("Failed Tables:")
            print(f"{'─' * 70}")
            for result in failed:
                print(f"  ✗ {result['table_name']}: {result.get('error', 'Unknown error')}")

    def save_results(self, results: list[dict[str, Any]], output_file: str = "analysis_results.json"):
        """Save results to JSON file."""
        output_path = Path(output_file)
        with open(output_path, "w") as f:
            json.dump(
                {
                    "config": {
                        "database": self.config.database,
                        "schema": self.config.schema,
                        "user": self.config.user_info,
                        "ai_model": self.config.ai_model,
                    },
                    "results": results,
                },
                f,
                indent=2,
                default=str,
            )
        print(f"\n💾 Results saved to: {output_path.absolute()}")


async def main():
    """Main execution."""
    try:
        # Load configuration
        config = DatabaseConfig()

        # Create analyzer
        analyzer = TableAnalyzer(config)

        # Run analysis
        results = await analyzer.analyze_all_tables()

        # Print summary
        analyzer.print_summary(results)

        # Save results
        analyzer.save_results(results)

        print(f"\n{'=' * 70}")
        print("✓ Analysis complete!")
        print(f"{'=' * 70}\n")

        return 0

    except FileNotFoundError:
        print("✗ Error: database_config.yaml not found!")
        print("  Please ensure the configuration file exists.")
        return 1
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
