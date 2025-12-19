#!/usr/bin/env python3
"""
Test actual AI SQL function calls against Snowflake with real data.
"""

import asyncio
import sys
sys.path.insert(0, '/Users/andyyang/synogize/Challenge/sf_aisql_innovation_challenge/core/src')

from app.services.snowflake_service import SnowflakeService
from app.services.modular_ai_sql_service import ModularAISQLService


async def test_ai_complete():
    """Test AI_COMPLETE function."""
    print("\n" + "=" * 60)
    print("Test 1: AI_COMPLETE - Text Generation")
    print("=" * 60)

    sf = SnowflakeService()
    ai_service = ModularAISQLService(sf)

    try:
        result = await ai_service.ai_complete(
            model="mistral-large",
            prompt="Explain what Snowflake Cortex AI SQL functions are in one sentence."
        )
        print(f"✓ Result: {result}")
        return True
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        return False


async def test_with_fashion_data():
    """Test AI functions with FASHION_PRODUCTS data."""
    print("\n" + "=" * 60)
    print("Test 2: Query FASHION_PRODUCTS table")
    print("=" * 60)

    sf = SnowflakeService()

    try:
        # First check if table exists and has data
        query = """
        SELECT COUNT(*) as count
        FROM RETAIL_DB.PUBLIC.FASHION_PRODUCTS
        """
        result = await sf.execute_query(query)
        print(f"✓ FASHION_PRODUCTS has {result[0]['COUNT']} rows")

        # Get sample data
        query = """
        SELECT *
        FROM RETAIL_DB.PUBLIC.FASHION_PRODUCTS
        LIMIT 3
        """
        result = await sf.execute_query(query)
        print(f"✓ Sample data columns: {list(result[0].keys())}")
        print(f"✓ First row preview: {str(result[0])[:200]}...")

        return True
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        return False


async def test_ai_classify_with_data():
    """Test AI_CLASSIFY on actual data."""
    print("\n" + "=" * 60)
    print("Test 3: AI_CLASSIFY on sample text")
    print("=" * 60)

    sf = SnowflakeService()

    try:
        # Create a temp table with sample data for testing
        query = """
        SELECT AI_CLASSIFY(
            'This product is amazing and high quality!',
            ARRAY_CONSTRUCT('positive', 'negative', 'neutral')
        ) as classification
        """
        result = await sf.execute_query(query)
        print(f"✓ Classification result: {result[0]}")
        return True
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        return False


async def test_ai_sentiment():
    """Test AI_SENTIMENT function."""
    print("\n" + "=" * 60)
    print("Test 4: AI_SENTIMENT Analysis")
    print("=" * 60)

    sf = SnowflakeService()

    try:
        query = """
        SELECT AI_SENTIMENT('I absolutely love this product! Best purchase ever!') as sentiment
        """
        result = await sf.execute_query(query)
        print(f"✓ Sentiment analysis: {result[0]}")
        return True
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        return False


async def test_summarize():
    """Test SUMMARIZE function."""
    print("\n" + "=" * 60)
    print("Test 5: SUMMARIZE Text")
    print("=" * 60)

    sf = SnowflakeService()

    try:
        query = """
        SELECT SNOWFLAKE.CORTEX.SUMMARIZE(
            'Snowflake Cortex AI SQL brings large language models and AI functions directly into SQL.
            This means you can analyze text, classify data, and generate insights without moving data
            out of Snowflake. Functions include AI_COMPLETE for text generation, AI_CLASSIFY for
            categorization, AI_SENTIMENT for sentiment analysis, and many more.'
        ) as summary
        """
        result = await sf.execute_query(query)
        print(f"✓ Summary: {result[0]['SUMMARY']}")
        return True
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        return False


async def test_check_databases():
    """Check what databases and tables we have."""
    print("\n" + "=" * 60)
    print("Test 6: Check Available Databases & Tables")
    print("=" * 60)

    sf = SnowflakeService()

    try:
        # Check databases
        for db in ['FINANCE_DB', 'TRAVEL_DB', 'RETAIL_DB']:
            try:
                query = f"USE DATABASE {db}"
                await sf.execute_query(query)

                query = "SHOW TABLES"
                tables = await sf.execute_query(query)
                print(f"\n✓ {db}:")
                for table in tables:
                    table_name = table['name']
                    count_query = f"SELECT COUNT(*) as cnt FROM {table_name}"
                    count_result = await sf.execute_query(count_query)
                    print(f"  - {table_name}: {count_result[0]['CNT']:,} rows")
            except Exception as e:
                print(f"  ✗ {db}: {str(e)[:100]}")

        return True
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        return False


async def main():
    """Run all tests."""
    print("\n" + "█" * 60)
    print("   Snowflake Cortex AI SQL Function Tests")
    print("   Testing with Real Data & Live Connections")
    print("█" * 60)

    results = []

    # Test database connectivity and data first
    results.append(await test_check_databases())
    results.append(await test_with_fashion_data())

    # Test individual AI functions
    results.append(await test_ai_complete())
    results.append(await test_ai_classify_with_data())
    results.append(await test_ai_sentiment())
    results.append(await test_summarize())

    # Summary
    print("\n" + "=" * 60)
    passed = sum(results)
    total = len(results)
    print(f"Results: {passed}/{total} tests passed")
    print("=" * 60)

    if passed == total:
        print("\n✓✓✓ All AI SQL functions working correctly! ✓✓✓")
        return 0
    else:
        print(f"\n⚠ {total - passed} test(s) failed. Check Snowflake connection and data.")
        return 1


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
