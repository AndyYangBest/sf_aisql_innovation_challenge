"""Chart generation and recommendation service."""

from typing import Any


class ChartService:
    """Service for generating chart candidates and configurations."""

    def __init__(self, ai_service, eda_service):
        """Initialize with AI and EDA services."""
        self.ai_service = ai_service
        self.eda_service = eda_service

    def identify_dimension_columns(
        self, columns: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Identify columns suitable as chart dimensions (categorical/time)."""
        dimensions = []
        for col in columns:
            data_type = col.get("DATA_TYPE", "")
            cardinality = col.get("cardinality", 1.0)

            # Categorical: low cardinality string/number
            if data_type in ["VARCHAR", "STRING", "TEXT"] and cardinality < 0.1:
                dimensions.append({"column": col["COLUMN_NAME"], "type": "categorical"})

            # Time dimension
            elif data_type in ["DATE", "TIMESTAMP", "DATETIME"]:
                dimensions.append({"column": col["COLUMN_NAME"], "type": "temporal"})

        return dimensions

    def identify_metric_columns(
        self, columns: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Identify columns suitable as metrics (numeric)."""
        metrics = []
        for col in columns:
            data_type = col.get("DATA_TYPE", "")
            if data_type in ["NUMBER", "FLOAT", "INTEGER", "DECIMAL", "DOUBLE"]:
                metrics.append(
                    {
                        "column": col["COLUMN_NAME"],
                        "type": "numeric",
                        "aggregations": ["sum", "avg", "count", "min", "max"],
                    }
                )
        return metrics

    async def generate_chart_candidates(
        self, table_name: str, column_profiles: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Generate candidate chart configurations."""
        dimensions = self.identify_dimension_columns(column_profiles)
        metrics = self.identify_metric_columns(column_profiles)

        candidates = []

        # Bar charts: categorical dimension vs metric
        for dim in [d for d in dimensions if d["type"] == "categorical"]:
            for metric in metrics:
                candidates.append(
                    {
                        "chart_type": "bar",
                        "x_axis": dim["column"],
                        "y_axis": f"AVG({metric['column']})",
                        "title": f"{metric['column']} by {dim['column']}",
                        "rationale": f"Compare average {metric['column']} across {dim['column']} categories",
                        "config": {
                            "dimension": dim["column"],
                            "metric": metric["column"],
                            "aggregation": "avg",
                        },
                    }
                )

        # Line charts: temporal dimension vs metric
        for dim in [d for d in dimensions if d["type"] == "temporal"]:
            for metric in metrics:
                candidates.append(
                    {
                        "chart_type": "line",
                        "x_axis": dim["column"],
                        "y_axis": f"SUM({metric['column']})",
                        "title": f"{metric['column']} over time",
                        "rationale": f"Track {metric['column']} trends over {dim['column']}",
                        "config": {
                            "dimension": dim["column"],
                            "metric": metric["column"],
                            "aggregation": "sum",
                        },
                    }
                )

        # Histograms: metric distribution
        for metric in metrics[:3]:  # Limit to first 3 metrics
            candidates.append(
                {
                    "chart_type": "histogram",
                    "x_axis": metric["column"],
                    "y_axis": "COUNT(*)",
                    "title": f"Distribution of {metric['column']}",
                    "rationale": f"Understand the distribution and frequency of {metric['column']} values",
                    "config": {"metric": metric["column"], "bins": 20},
                }
            )

        # Scatter plots: metric vs metric
        if len(metrics) >= 2:
            for i, metric1 in enumerate(metrics[:2]):
                for metric2 in metrics[i + 1 : 3]:
                    candidates.append(
                        {
                            "chart_type": "scatter",
                            "x_axis": metric1["column"],
                            "y_axis": metric2["column"],
                            "title": f"{metric1['column']} vs {metric2['column']}",
                            "rationale": f"Explore correlation between {metric1['column']} and {metric2['column']}",
                            "config": {
                                "x_metric": metric1["column"],
                                "y_metric": metric2["column"],
                            },
                        }
                    )

        return candidates[:10]  # Return top 10 candidates

    def organize_dashboard(
        self, charts: list[dict[str, Any]], table_name: str
    ) -> list[dict[str, Any]]:
        """Organize charts into themed dashboards."""
        dashboards = []

        # Dashboard 1: Overview
        overview_charts = [c for c in charts if c["chart_type"] in ["bar", "line"]][
            :4
        ]
        if overview_charts:
            dashboards.append(
                {
                    "theme": "Overview",
                    "description": f"Key metrics and trends for {table_name}",
                    "charts": overview_charts,
                    "audience": "General stakeholders",
                }
            )

        # Dashboard 2: Distribution Analysis
        dist_charts = [c for c in charts if c["chart_type"] == "histogram"][:3]
        if dist_charts:
            dashboards.append(
                {
                    "theme": "Distribution Analysis",
                    "description": f"Statistical distributions in {table_name}",
                    "charts": dist_charts,
                    "audience": "Data analysts",
                }
            )

        # Dashboard 3: Correlations
        corr_charts = [c for c in charts if c["chart_type"] == "scatter"][:3]
        if corr_charts:
            dashboards.append(
                {
                    "theme": "Correlation Analysis",
                    "description": f"Relationships between metrics in {table_name}",
                    "charts": corr_charts,
                    "audience": "Data scientists",
                }
            )

        return dashboards
