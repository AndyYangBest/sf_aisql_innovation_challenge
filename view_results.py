#!/usr/bin/env python3
"""
Results Viewer - Beautiful display of AI analysis results
"""

import json
import sys
from pathlib import Path


def print_header(text: str, char: str = "="):
    """Print a formatted header."""
    print(f"\n{char * 80}")
    print(f"  {text}")
    print(f"{char * 80}")


def print_table_analysis(result: dict):
    """Print detailed analysis for a single table."""
    table_name = result.get("table_name", "Unknown")
    status = result.get("status", "unknown")

    print(f"\n{'─' * 80}")
    print(f"📊 TABLE: {table_name}")
    print(f"{'─' * 80}")

    if status == "error":
        print(f"❌ Status: FAILED")
        print(f"Error: {result.get('error', 'Unknown error')}")
        return

    print(f"✅ Status: SUCCESS")
    print(f"Sample Size: {result.get('sample_size', 'N/A')} rows")

    actual_columns = result.get("actual_columns", [])
    print(f"Total Columns: {len(actual_columns)}")

    ai_analysis = result.get("ai_analysis", {})

    if not isinstance(ai_analysis, dict):
        print(f"\n⚠️ Raw AI Response:\n{ai_analysis}")
        return

    # Table Purpose
    table_purpose = ai_analysis.get("table_purpose", "N/A")
    print(f"\n📝 Table Purpose:")
    print(f"   {table_purpose}")

    # Columns
    columns = ai_analysis.get("columns", [])
    if columns:
        print(f"\n📋 Column Analysis ({len(columns)} columns):")
        for i, col in enumerate(columns, 1):
            name = col.get("name", "N/A")
            dtype = col.get("type", "N/A")
            desc = col.get("description", "N/A")
            print(f"   {i:2d}. {name:25s} [{dtype:15s}]")
            print(f"       → {desc}")

    # Data Quality
    quality_notes = ai_analysis.get("data_quality_notes", "")
    if quality_notes:
        print(f"\n🔍 Data Quality Observations:")
        # Wrap long text
        words = quality_notes.split()
        line = "   "
        for word in words:
            if len(line) + len(word) + 1 > 80:
                print(line)
                line = "   " + word
            else:
                line += " " + word if line != "   " else word
        if line.strip():
            print(line)


def main():
    """Main viewer."""
    results_file = Path("analysis_results.json")

    if not results_file.exists():
        print("❌ Error: analysis_results.json not found!")
        print("   Please run analyze_tables.py first.")
        return 1

    with open(results_file) as f:
        data = json.load(f)

    config = data.get("config", {})
    results = data.get("results", [])

    # Print header
    print_header("📊 AI SQL TABLE ANALYSIS RESULTS", "█")

    print(f"\n🔧 Configuration:")
    print(f"   Database: {config.get('database', 'N/A')}")
    print(f"   Schema: {config.get('schema', 'N/A')}")
    print(f"   User: {config.get('user', {}).get('username', 'N/A')}")
    print(f"   Role: {config.get('user', {}).get('role', 'N/A')}")
    print(f"   AI Model: {config.get('ai_model', 'N/A')}")

    # Summary stats
    successful = [r for r in results if r.get("status") == "success"]
    failed = [r for r in results if r.get("status") == "error"]

    print(f"\n📈 Summary:")
    print(f"   Total Tables: {len(results)}")
    print(f"   ✅ Successful: {len(successful)}")
    print(f"   ❌ Failed: {len(failed)}")

    # Print each table
    print_header("DETAILED TABLE ANALYSIS")

    for result in results:
        print_table_analysis(result)

    # Final summary
    print(f"\n{'=' * 80}")
    print(f"  ✨ Analysis Complete - {len(successful)}/{len(results)} tables processed successfully")
    print(f"{'=' * 80}\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
