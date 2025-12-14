"""EDA (Exploratory Data Analysis) service."""

from typing import Any


class EDAService:
    """Service for automated exploratory data analysis."""

    def __init__(self, snowflake_service):
        """Initialize with Snowflake service."""
        self.sf = snowflake_service

    async def get_table_profile(self, table_name: str) -> dict[str, Any]:
        """Get comprehensive table-level profile."""
        query = f"""
        SELECT
            COUNT(*) as row_count,
            COUNT(DISTINCT *) as unique_rows
        FROM {table_name}
        """
        stats = await self.sf.execute_query(query)

        columns = await self.sf.get_table_columns(table_name)

        return {
            "table_name": table_name,
            "row_count": stats[0]["ROW_COUNT"] if stats else 0,
            "unique_rows": stats[0]["UNIQUE_ROWS"] if stats else 0,
            "column_count": len(columns),
            "columns": columns,
        }

    async def get_column_profile(
        self, table_name: str, column_name: str, data_type: str
    ) -> dict[str, Any]:
        """Get detailed column-level profile with statistics."""
        profile = {
            "column_name": column_name,
            "data_type": data_type,
        }

        # Basic null analysis
        null_query = f"""
        SELECT
            COUNT(*) as total_count,
            COUNT({column_name}) as non_null_count,
            COUNT(DISTINCT {column_name}) as unique_count
        FROM {table_name}
        """
        null_stats = await self.sf.execute_query(null_query)
        if null_stats:
            total = null_stats[0]["TOTAL_COUNT"]
            non_null = null_stats[0]["NON_NULL_COUNT"]
            profile["null_count"] = total - non_null
            profile["null_rate"] = (total - non_null) / total if total > 0 else 0
            profile["unique_count"] = null_stats[0]["UNIQUE_COUNT"]
            profile["cardinality"] = (
                null_stats[0]["UNIQUE_COUNT"] / total if total > 0 else 0
            )

        # Numeric statistics
        if data_type in ["NUMBER", "FLOAT", "INTEGER", "DECIMAL"]:
            numeric_query = f"""
            SELECT
                MIN({column_name}) as min_value,
                MAX({column_name}) as max_value,
                AVG({column_name}) as mean_value,
                MEDIAN({column_name}) as median_value,
                STDDEV({column_name}) as stddev_value
            FROM {table_name}
            """
            numeric_stats = await self.sf.execute_query(numeric_query)
            if numeric_stats:
                profile.update(numeric_stats[0])

        # Sample values
        sample_query = f"""
        SELECT {column_name}
        FROM {table_name}
        WHERE {column_name} IS NOT NULL
        LIMIT 10
        """
        samples = await self.sf.execute_query(sample_query)
        profile["sample_values"] = [s[column_name] for s in samples]

        return profile

    async def detect_anomalies(
        self, table_name: str, column_name: str, data_type: str
    ) -> list[str]:
        """Detect potential anomalies in a column."""
        anomalies = []

        if data_type in ["NUMBER", "FLOAT", "INTEGER", "DECIMAL"]:
            # Check for outliers using IQR method
            query = f"""
            WITH stats AS (
                SELECT
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {column_name}) as q1,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {column_name}) as q3
                FROM {table_name}
                WHERE {column_name} IS NOT NULL
            )
            SELECT
                COUNT(*) as outlier_count
            FROM {table_name}, stats
            WHERE {column_name} < (q1 - 1.5 * (q3 - q1))
               OR {column_name} > (q3 + 1.5 * (q3 - q1))
            """
            result = await self.sf.execute_query(query)
            if result and result[0]["OUTLIER_COUNT"] > 0:
                anomalies.append(
                    f"Found {result[0]['OUTLIER_COUNT']} potential outliers"
                )

        return anomalies
