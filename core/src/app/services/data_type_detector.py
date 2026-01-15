"""Data type detection and inference service for column analysis."""

import re
import math
from datetime import datetime
from enum import Enum
from typing import Any


class DataTypeCategory(str, Enum):
    """Comprehensive data type categories."""

    # Numeric types
    CONTINUOUS_NUMERIC = "continuous_numeric"
    DISCRETE_NUMERIC = "discrete_numeric"
    RATIO_PERCENTAGE = "ratio_percentage"

    # Categorical types
    NOMINAL_CATEGORICAL = "nominal_categorical"
    ORDINAL_CATEGORICAL = "ordinal_categorical"
    BINARY = "binary"

    # Temporal types
    DATETIME = "datetime"
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"
    TEMPORAL_CYCLIC = "temporal_cyclic"  # hour, day_of_week, month

    # Text types
    TEXT_SHORT = "text_short"  # < 100 chars
    TEXT_LONG = "text_long"  # > 100 chars
    TEXT_STRUCTURED = "text_structured"  # JSON, XML, etc.

    # Identifier types
    IDENTIFIER = "identifier"  # ID, UUID, etc.
    FOREIGN_KEY = "foreign_key"

    # Spatial types
    GEOSPATIAL = "geospatial"  # lat/lon, address, etc.

    # Sequence types
    TIME_SERIES = "time_series"
    EVENT_SEQUENCE = "event_sequence"

    # Unknown
    UNKNOWN = "unknown"


class DataStructureType(str, Enum):
    """Data structure classification."""

    CROSS_SECTIONAL = "cross_sectional"  # Single time point, multiple entities
    TIME_SERIES = "time_series"  # Single entity over time
    PANEL = "panel"  # Multiple entities over time
    EVENT_SEQUENCE = "event_sequence"  # Events with timestamps
    STREAMING = "streaming"  # Real-time data


class SamplingStrategy(str, Enum):
    """Sampling strategies for large datasets."""

    HEAD_TAIL = "head_tail"  # First N + Last N rows
    RANDOM = "random"  # Random sampling
    STRATIFIED = "stratified"  # Stratified by groups
    SYSTEMATIC = "systematic"  # Every Nth row
    RESERVOIR = "reservoir"  # Reservoir sampling for streaming
    SLIDING_WINDOW = "sliding_window"  # For time series


class DataTypeDetector:
    """Intelligent data type detection and inference."""

    def __init__(self):
        """Initialize detector with patterns."""
        self.datetime_patterns = [
            r"\d{4}-\d{2}-\d{2}",  # YYYY-MM-DD
            r"\d{2}/\d{2}/\d{4}",  # MM/DD/YYYY
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",  # ISO 8601
            r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",  # YYYY-MM-DD HH:MM:SS
        ]
        self.id_patterns = [
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",  # UUID
            r"^[A-Z]{2,5}\d{6,}$",  # ID codes
        ]
        self.geo_patterns = [
            r"^-?\d{1,3}\.\d+,\s*-?\d{1,3}\.\d+$",  # lat,lon
        ]

    def infer_column_type(
        self,
        column_name: str,
        sql_type: str,
        sample_values: list[Any],
        unique_count: int,
        total_count: int,
        null_count: int,
    ) -> dict[str, Any]:
        """
        Infer the semantic data type of a column.

        Args:
            column_name: Name of the column
            sql_type: SQL data type (NUMBER, VARCHAR, etc.)
            sample_values: Sample values from the column
            unique_count: Number of unique values
            total_count: Total number of rows
            null_count: Number of null values

        Returns:
            Dictionary with inferred type and confidence
        """
        cardinality = unique_count / total_count if total_count > 0 else 0
        non_null_samples = [v for v in sample_values if v is not None]

        # Evidence chain for type inference
        evidence = {
            "column_name": column_name,
            "sql_type": sql_type,
            "cardinality": cardinality,
            "unique_count": unique_count,
            "total_count": total_count,
            "null_count": null_count,
            "null_rate": null_count / total_count if total_count > 0 else 0,
        }

        # Step 1: Check column name semantics
        name_hints = self._analyze_column_name(column_name)
        evidence["name_hints"] = name_hints

        # Step 2: Analyze value patterns
        value_analysis = self._analyze_values(non_null_samples, sql_type)
        evidence["value_analysis"] = value_analysis

        # Step 3: Determine type based on evidence
        inferred_type = self._determine_type(
            sql_type, cardinality, unique_count, total_count, name_hints, value_analysis
        )

        # Step 4: Calculate confidence
        confidence = self._calculate_confidence(evidence, inferred_type)

        return {
            "inferred_type": inferred_type,
            "confidence": confidence,
            "evidence": evidence,
            "recommendations": self._get_recommendations(inferred_type, evidence),
        }

    def _analyze_column_name(self, column_name: str) -> dict[str, bool]:
        """Extract semantic hints from column name."""
        name_lower = column_name.lower()
        return {
            "is_id": any(
                x in name_lower for x in ["id", "key", "uuid", "guid", "code"]
            ),
            "is_temporal": any(
                x in name_lower
                for x in [
                    "date",
                    "time",
                    "timestamp",
                    "created",
                    "updated",
                    "year",
                    "month",
                    "day",
                ]
            ),
            "is_geo": any(
                x in name_lower
                for x in ["lat", "lon", "longitude", "latitude", "address", "location"]
            ),
            "is_categorical": any(
                x in name_lower
                for x in ["type", "category", "status", "level", "grade", "class"]
            ),
            "is_ratio": any(
                x in name_lower for x in ["rate", "ratio", "percent", "pct"]
            ),
            "is_count": any(x in name_lower for x in ["count", "num", "quantity"]),
        }

    def _analyze_values(self, samples: list[Any], sql_type: str) -> dict[str, Any]:
        """Analyze sample values for patterns."""
        if not samples:
            return {"has_samples": False}

        analysis = {"has_samples": True, "sample_size": len(samples)}

        # Convert to strings for pattern matching
        str_samples = [str(v) for v in samples]

        # Check for datetime patterns
        datetime_matches = sum(
            1
            for s in str_samples
            if any(re.search(p, s) for p in self.datetime_patterns)
        )
        analysis["datetime_pattern_ratio"] = datetime_matches / len(samples)

        # Check for ID patterns
        id_matches = sum(
            1 for s in str_samples if any(re.match(p, s) for p in self.id_patterns)
        )
        analysis["id_pattern_ratio"] = id_matches / len(samples)

        # Check for geo patterns
        geo_matches = sum(
            1 for s in str_samples if any(re.match(p, s) for p in self.geo_patterns)
        )
        analysis["geo_pattern_ratio"] = geo_matches / len(samples)

        # Numeric analysis
        numeric_samples = []
        for value in samples:
            if value is None or isinstance(value, bool):
                continue
            if isinstance(value, (int, float)):
                num = float(value)
                if math.isfinite(num):
                    numeric_samples.append(num)
                continue
            if isinstance(value, str):
                raw = value.strip()
                if not raw:
                    continue
                if not re.match(r"^[\d\s\-\+\(\)\.,%$eE]+$", raw):
                    continue
                cleaned = raw.replace(",", "")
                is_negative_paren = cleaned.startswith("(") and cleaned.endswith(")")
                cleaned = cleaned.strip("()").replace("$", "").replace("%", "")
                try:
                    num = float(cleaned)
                    if is_negative_paren:
                        num = -num
                except (TypeError, ValueError):
                    continue
                if math.isfinite(num):
                    numeric_samples.append(num)
                continue
            try:
                num = float(value)
                if math.isfinite(num):
                    numeric_samples.append(num)
            except (TypeError, ValueError):
                continue
        analysis["numeric_ratio"] = len(numeric_samples) / len(samples)

        if numeric_samples and sql_type in ["NUMBER", "FLOAT", "INTEGER", "DECIMAL", "NUMERIC", "DOUBLE", "VARIANT"]:
            if numeric_samples:
                analysis["all_integers"] = all(
                    float(v).is_integer() for v in numeric_samples
                )
                analysis["all_positive"] = all(v >= 0 for v in numeric_samples)
                analysis["in_0_1_range"] = all(0 <= v <= 1 for v in numeric_samples)
                analysis["in_0_100_range"] = all(0 <= v <= 100 for v in numeric_samples)

                # Check for sequential pattern (potential ID)
                sorted_samples = sorted(numeric_samples)
                diffs = [
                    sorted_samples[i + 1] - sorted_samples[i]
                    for i in range(len(sorted_samples) - 1)
                ]
                analysis["is_sequential"] = len(set(diffs)) == 1 if diffs else False

        # Text analysis
        if sql_type in ["VARCHAR", "TEXT", "STRING"]:
            lengths = [len(str(v)) for v in samples]
            analysis["avg_length"] = sum(lengths) / len(lengths) if lengths else 0
            analysis["max_length"] = max(lengths) if lengths else 0
            analysis["min_length"] = min(lengths) if lengths else 0
            analysis["length_variance"] = (
                sum((l - analysis["avg_length"]) ** 2 for l in lengths) / len(lengths)
                if lengths
                else 0
            )

        return analysis

    def _determine_type(
        self,
        sql_type: str,
        cardinality: float,
        unique_count: int,
        total_count: int,
        name_hints: dict,
        value_analysis: dict,
    ) -> DataTypeCategory:
        """Determine the most likely data type based on evidence."""
        # Priority 1: Temporal types
        if name_hints.get("is_temporal") or value_analysis.get(
            "datetime_pattern_ratio", 0
        ) > 0.8:
            return DataTypeCategory.DATETIME

        # Priority 2: Identifier types
        if (
            name_hints.get("is_id")
            or value_analysis.get("id_pattern_ratio", 0) > 0.8
            or (cardinality > 0.95 and unique_count == total_count)
        ):
            return DataTypeCategory.IDENTIFIER

        # Priority 3: Geospatial
        if name_hints.get("is_geo") or value_analysis.get("geo_pattern_ratio", 0) > 0.8:
            return DataTypeCategory.GEOSPATIAL

        # Priority 4: Binary
        if unique_count == 2:
            return DataTypeCategory.BINARY

        # Priority 5: Numeric types
        numeric_ratio = value_analysis.get("numeric_ratio", 0)
        numeric_hint = name_hints.get("is_count") or name_hints.get("is_ratio")
        if (
            sql_type in ["NUMBER", "FLOAT", "INTEGER", "DECIMAL", "NUMERIC", "DOUBLE"]
            or numeric_ratio >= 0.6
            or (numeric_ratio >= 0.4 and numeric_hint)
        ):
            # Ratio/Percentage
            if name_hints.get("is_ratio") or value_analysis.get("in_0_1_range"):
                return DataTypeCategory.RATIO_PERCENTAGE

            # Discrete vs Continuous
            if value_analysis.get("all_integers") and name_hints.get("is_count"):
                return DataTypeCategory.DISCRETE_NUMERIC

            if value_analysis.get("all_integers") and cardinality < 0.1:
                return DataTypeCategory.DISCRETE_NUMERIC

            return DataTypeCategory.CONTINUOUS_NUMERIC

        # Priority 6: Categorical
        if cardinality < 0.05 or unique_count < 20:
            if name_hints.get("is_categorical"):
                return DataTypeCategory.ORDINAL_CATEGORICAL
            return DataTypeCategory.NOMINAL_CATEGORICAL

        # Priority 7: Text types
        if sql_type in ["VARCHAR", "TEXT", "STRING"]:
            avg_length = value_analysis.get("avg_length", 0)
            if avg_length > 100:
                return DataTypeCategory.TEXT_LONG
            return DataTypeCategory.TEXT_SHORT

        return DataTypeCategory.UNKNOWN

    def _calculate_confidence(
        self, evidence: dict, inferred_type: DataTypeCategory
    ) -> float:
        """Calculate confidence score for the inference."""
        confidence = 0.5  # Base confidence

        # Boost confidence based on evidence alignment
        name_hints = evidence.get("name_hints", {})
        value_analysis = evidence.get("value_analysis", {})

        # Strong indicators
        if inferred_type == DataTypeCategory.DATETIME:
            if value_analysis.get("datetime_pattern_ratio", 0) > 0.9:
                confidence += 0.4
            if name_hints.get("is_temporal"):
                confidence += 0.1

        elif inferred_type == DataTypeCategory.IDENTIFIER:
            if evidence["cardinality"] > 0.99:
                confidence += 0.3
            if name_hints.get("is_id"):
                confidence += 0.2

        elif inferred_type in [
            DataTypeCategory.NOMINAL_CATEGORICAL,
            DataTypeCategory.ORDINAL_CATEGORICAL,
        ]:
            if evidence["cardinality"] < 0.01:
                confidence += 0.3
            if name_hints.get("is_categorical"):
                confidence += 0.2

        elif inferred_type == DataTypeCategory.BINARY:
            if evidence["unique_count"] == 2:
                confidence += 0.4

        # Penalize if null rate is very high
        if evidence["null_rate"] > 0.5:
            confidence -= 0.2

        return max(0.0, min(1.0, confidence))

    def _get_recommendations(
        self, inferred_type: DataTypeCategory, evidence: dict
    ) -> list[str]:
        """Get recommendations for handling this data type."""
        recommendations = []

        if inferred_type == DataTypeCategory.IDENTIFIER:
            recommendations.append("Do not use for mathematical operations")
            recommendations.append("Consider as index or join key")

        elif inferred_type in [
            DataTypeCategory.NOMINAL_CATEGORICAL,
            DataTypeCategory.ORDINAL_CATEGORICAL,
        ]:
            recommendations.append("Consider one-hot encoding or label encoding")
            recommendations.append("Check for rare categories (< 1% frequency)")

        elif inferred_type == DataTypeCategory.DATETIME:
            recommendations.append("Extract temporal features (year, month, day, etc.)")
            recommendations.append("Consider time-based sorting and windowing")

        elif inferred_type == DataTypeCategory.CONTINUOUS_NUMERIC:
            recommendations.append("Check for outliers using IQR or Z-score")
            recommendations.append("Consider normalization or standardization")

        elif inferred_type == DataTypeCategory.TEXT_LONG:
            recommendations.append("Consider NLP techniques (TF-IDF, embeddings)")
            recommendations.append("May require text preprocessing")

        if evidence["null_rate"] > 0.1:
            recommendations.append(
                f"High null rate ({evidence['null_rate']:.1%}) - consider imputation strategy"
            )

        return recommendations

    def suggest_sampling_strategy(
        self,
        total_rows: int,
        has_time_column: bool = False,
        is_sorted: bool = False,
        target_sample_size: int = 1000,
    ) -> dict[str, Any]:
        """
        Suggest optimal sampling strategy for data type detection.

        Args:
            total_rows: Total number of rows in the dataset
            has_time_column: Whether dataset has a time column
            is_sorted: Whether data is sorted
            target_sample_size: Desired sample size

        Returns:
            Dictionary with sampling strategy and parameters
        """
        if total_rows <= target_sample_size:
            return {
                "strategy": "full_scan",
                "reason": "Dataset is small enough for full scan",
                "sample_size": total_rows,
            }

        # For time series data
        if has_time_column:
            return {
                "strategy": SamplingStrategy.HEAD_TAIL,
                "reason": "Time series data - sample from beginning, middle, and end",
                "head_size": target_sample_size // 3,
                "middle_size": target_sample_size // 3,
                "tail_size": target_sample_size // 3,
                "total_sample_size": target_sample_size,
            }

        # For very large datasets
        if total_rows > 1_000_000:
            return {
                "strategy": SamplingStrategy.SYSTEMATIC,
                "reason": "Very large dataset - systematic sampling for coverage",
                "sample_every_n": total_rows // target_sample_size,
                "sample_size": target_sample_size,
            }

        # Default: random sampling
        return {
            "strategy": SamplingStrategy.RANDOM,
            "reason": "Standard random sampling for unbiased representation",
            "sample_size": target_sample_size,
        }

    def detect_data_structure(
        self, has_time_column: bool, has_entity_column: bool, is_sorted_by_time: bool
    ) -> DataStructureType:
        """
        Detect the overall data structure type.

        Args:
            has_time_column: Whether dataset has temporal column
            has_entity_column: Whether dataset has entity/ID column
            is_sorted_by_time: Whether data is sorted by time

        Returns:
            Data structure type
        """
        if not has_time_column:
            return DataStructureType.CROSS_SECTIONAL

        if has_time_column and not has_entity_column:
            return DataStructureType.TIME_SERIES

        if has_time_column and has_entity_column:
            return DataStructureType.PANEL

        return DataStructureType.CROSS_SECTIONAL
