"""EDA (Exploratory Data Analysis) service."""

from typing import Any

from .data_type_detector import DataTypeDetector, SamplingStrategy


class EDAService:
    """Service for automated exploratory data analysis."""

    def __init__(self, snowflake_service):
        """Initialize with Snowflake service."""
        self.sf = snowflake_service
        self.type_detector = DataTypeDetector()

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
        """Get detailed column-level profile with statistics and type inference."""
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
        sample_values = [s[column_name] for s in samples]
        profile["sample_values"] = sample_values

        # Infer semantic data type
        if null_stats:
            type_inference = self.type_detector.infer_column_type(
                column_name=column_name,
                sql_type=data_type,
                sample_values=sample_values,
                unique_count=null_stats[0]["UNIQUE_COUNT"],
                total_count=null_stats[0]["TOTAL_COUNT"],
                null_count=total - non_null,
            )
            profile["type_inference"] = type_inference

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

    async def get_smart_sample(
        self,
        table_name: str,
        column_name: str,
        total_rows: int,
        target_size: int = 1000,
        has_time_column: bool = False,
    ) -> list[Any]:
        """
        Get smart sample based on data characteristics.

        Args:
            table_name: Name of the table
            column_name: Column to sample
            total_rows: Total number of rows
            target_size: Target sample size
            has_time_column: Whether table has time column

        Returns:
            List of sampled values
        """
        # Get sampling strategy recommendation
        strategy_info = self.type_detector.suggest_sampling_strategy(
            total_rows=total_rows,
            has_time_column=has_time_column,
            target_sample_size=target_size,
        )

        strategy = strategy_info["strategy"]

        if strategy == "full_scan":
            query = f"SELECT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL"
        elif strategy == SamplingStrategy.HEAD_TAIL:
            head_size = strategy_info["head_size"]
            tail_size = strategy_info["tail_size"]
            middle_size = strategy_info["middle_size"]
            middle_offset = (total_rows - middle_size) // 2

            query = f"""
            (SELECT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL LIMIT {head_size})
            UNION ALL
            (SELECT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL LIMIT {middle_size} OFFSET {middle_offset})
            UNION ALL
            (SELECT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL ORDER BY ROWID DESC LIMIT {tail_size})
            """
        elif strategy == SamplingStrategy.SYSTEMATIC:
            sample_every_n = strategy_info["sample_every_n"]
            query = f"""
            SELECT {column_name}
            FROM (
                SELECT {column_name}, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as rn
                FROM {table_name}
                WHERE {column_name} IS NOT NULL
            )
            WHERE MOD(rn, {sample_every_n}) = 0
            LIMIT {target_size}
            """
        else:  # RANDOM
            query = f"""
            SELECT {column_name}
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
            SAMPLE ({target_size} ROWS)
            """

        results = await self.sf.execute_query(query)
        return [r[column_name] for r in results]

    async def detect_data_structure(self, table_name: str) -> dict[str, Any]:
        """
        Detect the overall data structure type of the table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with structure type and characteristics
        """
        # Get column information
        columns = await self.sf.get_table_columns(table_name)
        column_names = [col["name"].lower() for col in columns]

        # Check for time columns
        time_keywords = [
            "date",
            "time",
            "timestamp",
            "created",
            "updated",
            "year",
            "month",
        ]
        has_time_column = any(
            any(keyword in col_name for keyword in time_keywords)
            for col_name in column_names
        )

        # Check for entity/ID columns
        id_keywords = ["id", "key", "uuid", "guid", "code"]
        has_entity_column = any(
            any(keyword in col_name for keyword in id_keywords)
            for col_name in column_names
        )

        # Check if data is sorted by time (sample first and last rows)
        is_sorted_by_time = False
        if has_time_column:
            time_col = next(
                (
                    col
                    for col in column_names
                    if any(keyword in col for keyword in time_keywords)
                ),
                None,
            )
            if time_col:
                check_query = f"""
                WITH first_last AS (
                    (SELECT {time_col} as val FROM {table_name} ORDER BY {time_col} LIMIT 1)
                    UNION ALL
                    (SELECT {time_col} as val FROM {table_name} ORDER BY {time_col} DESC LIMIT 1)
                )
                SELECT MIN(val) as first_val, MAX(val) as last_val FROM first_last
                """
                result = await self.sf.execute_query(check_query)
                if result:
                    is_sorted_by_time = result[0]["FIRST_VAL"] < result[0]["LAST_VAL"]

        # Determine structure type
        structure_type = self.type_detector.detect_data_structure(
            has_time_column=has_time_column,
            has_entity_column=has_entity_column,
            is_sorted_by_time=is_sorted_by_time,
        )

        return {
            "structure_type": structure_type,
            "has_time_column": has_time_column,
            "has_entity_column": has_entity_column,
            "is_sorted_by_time": is_sorted_by_time,
            "characteristics": self._get_structure_characteristics(structure_type),
        }

    def _get_structure_characteristics(self, structure_type: str) -> dict[str, Any]:
        """Get characteristics and recommendations for data structure type."""
        characteristics = {
            "cross_sectional": {
                "description": "Single time point, multiple entities",
                "analysis_approach": "Standard statistical analysis, clustering, classification",
                "sampling": "Random sampling is appropriate",
                "considerations": [
                    "Rows are independent",
                    "Can shuffle data freely",
                    "Standard train/test split works",
                ],
            },
            "time_series": {
                "description": "Single entity over time",
                "analysis_approach": "Time series analysis, forecasting, trend detection",
                "sampling": "Use head/tail sampling, preserve temporal order",
                "considerations": [
                    "Order matters - do not shuffle",
                    "Use walk-forward validation",
                    "Consider seasonality and trends",
                ],
            },
            "panel": {
                "description": "Multiple entities over time",
                "analysis_approach": "Panel data analysis, fixed/random effects models",
                "sampling": "Stratified by entity, preserve time order within entity",
                "considerations": [
                    "Both cross-sectional and time-series properties",
                    "Group by entity for analysis",
                    "Consider entity-specific effects",
                ],
            },
            "event_sequence": {
                "description": "Events with timestamps (irregular intervals)",
                "analysis_approach": "Event sequence analysis, survival analysis",
                "sampling": "Sample events, not time points",
                "considerations": [
                    "Focus on event occurrence, not regular intervals",
                    "Consider inter-event times",
                    "May need event aggregation",
                ],
            },
        }

        return characteristics.get(
            structure_type,
            {
                "description": "Unknown structure",
                "analysis_approach": "Exploratory analysis needed",
                "sampling": "Use random sampling",
                "considerations": ["Investigate data structure further"],
            },
        )

    async def analyze_all_columns(self, table_name: str) -> dict[str, Any]:
        """
        Comprehensive analysis of all columns in a table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with complete analysis
        """
        # Get table profile
        table_profile = await self.get_table_profile(table_name)

        # Detect data structure
        structure_info = await self.detect_data_structure(table_name)

        # Analyze each column
        column_analyses = []
        for col in table_profile["columns"]:
            col_name = col["name"]
            col_type = col["type"]

            # Get column profile with type inference
            col_profile = await self.get_column_profile(table_name, col_name, col_type)

            # Detect anomalies
            anomalies = await self.detect_anomalies(table_name, col_name, col_type)
            col_profile["anomalies"] = anomalies

            column_analyses.append(col_profile)

        return {
            "table_profile": table_profile,
            "structure_info": structure_info,
            "column_analyses": column_analyses,
            "summary": self._generate_analysis_summary(
                table_profile, structure_info, column_analyses
            ),
        }

    def _generate_analysis_summary(
        self,
        table_profile: dict,
        structure_info: dict,
        column_analyses: list[dict],
    ) -> dict[str, Any]:
        """Generate a summary of the analysis."""
        # Count types
        type_counts = {}
        high_null_columns = []
        identifier_columns = []
        categorical_columns = []
        numeric_columns = []

        for col in column_analyses:
            if "type_inference" in col:
                inferred_type = col["type_inference"]["inferred_type"]
                type_counts[inferred_type] = type_counts.get(inferred_type, 0) + 1

                if col["null_rate"] > 0.3:
                    high_null_columns.append(col["column_name"])

                if "identifier" in inferred_type:
                    identifier_columns.append(col["column_name"])
                elif "categorical" in inferred_type:
                    categorical_columns.append(col["column_name"])
                elif "numeric" in inferred_type:
                    numeric_columns.append(col["column_name"])

        return {
            "total_rows": table_profile["row_count"],
            "total_columns": table_profile["column_count"],
            "data_structure": structure_info["structure_type"],
            "type_distribution": type_counts,
            "high_null_columns": high_null_columns,
            "identifier_columns": identifier_columns,
            "categorical_columns": categorical_columns,
            "numeric_columns": numeric_columns,
            "recommendations": self._generate_recommendations(
                structure_info, column_analyses
            ),
        }

    def _generate_recommendations(
        self, structure_info: dict, column_analyses: list[dict]
    ) -> list[str]:
        """Generate actionable recommendations based on analysis."""
        recommendations = []

        # Structure-based recommendations
        structure_type = structure_info["structure_type"]
        if structure_type == "time_series":
            recommendations.append(
                "Use time-aware validation (walk-forward) instead of random split"
            )
            recommendations.append("Consider extracting temporal features")
        elif structure_type == "panel":
            recommendations.append("Group analysis by entity ID")
            recommendations.append("Consider entity-specific models or fixed effects")

        # Column-based recommendations
        high_cardinality_cols = [
            col["column_name"]
            for col in column_analyses
            if col.get("cardinality", 0) > 0.9
        ]
        if high_cardinality_cols:
            recommendations.append(
                f"High cardinality columns detected: {', '.join(high_cardinality_cols[:3])} - likely identifiers"
            )

        low_cardinality_cols = [
            col["column_name"]
            for col in column_analyses
            if 0 < col.get("cardinality", 1) < 0.05
        ]
        if low_cardinality_cols:
            recommendations.append(
                f"Low cardinality columns: {', '.join(low_cardinality_cols[:3])} - consider encoding"
            )

        return recommendations
