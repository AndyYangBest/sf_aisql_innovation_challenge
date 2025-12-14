"""Snowflake connection and query service.

This service provides high-level methods for interacting with Snowflake
for data analysis, AI SQL functions, and metadata retrieval.
"""

from typing import Any

from ..core.db.database import SnowflakeConnection, get_snowflake_connection


class SnowflakeService:
    """Service for interacting with Snowflake for data analysis."""

    def __init__(self, sf_conn: SnowflakeConnection | None = None):
        """Initialize Snowflake service.

        Args:
            sf_conn: Optional SnowflakeConnection instance. If None, uses global connection.
        """
        self.sf_conn = sf_conn or get_snowflake_connection()

    async def execute_query(self, query: str) -> list[dict[str, Any]]:
        """Execute a query and return results as list of dicts.

        Args:
            query: SQL query string

        Returns:
            List of dictionaries representing query results
        """
        return await self.sf_conn.execute_query_async(query)

    async def get_tables(self, database: str | None = None, schema: str | None = None) -> list[dict[str, Any]]:
        """Get list of tables in specified database/schema.

        Args:
            database: Database name (uses current if None)
            schema: Schema name (uses current if None)

        Returns:
            List of table metadata dictionaries
        """
        db_clause = f"TABLE_CATALOG = '{database}'" if database else "TRUE"
        schema_clause = f"TABLE_SCHEMA = '{schema}'" if schema else "TRUE"

        query = f"""
        SELECT
            TABLE_CATALOG AS DATABASE_NAME,
            TABLE_SCHEMA AS SCHEMA_NAME,
            TABLE_NAME,
            TABLE_TYPE,
            ROW_COUNT,
            BYTES,
            CREATED,
            LAST_ALTERED,
            COMMENT
        FROM INFORMATION_SCHEMA.TABLES
        WHERE {db_clause}
          AND {schema_clause}
        ORDER BY TABLE_NAME
        """
        return await self.execute_query(query)

    async def get_table_columns(
        self, table_name: str, database: str | None = None, schema: str | None = None
    ) -> list[dict[str, Any]]:
        """Get column information for a specific table.

        Args:
            table_name: Name of the table
            database: Database name (uses current if None)
            schema: Schema name (uses current if None)

        Returns:
            List of column metadata dictionaries
        """
        db_clause = f"TABLE_CATALOG = '{database}'" if database else "TRUE"
        schema_clause = f"TABLE_SCHEMA = '{schema}'" if schema else "TRUE"

        query = f"""
        SELECT
            COLUMN_NAME,
            ORDINAL_POSITION,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE {db_clause}
          AND {schema_clause}
          AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        return await self.execute_query(query)

    async def get_sample_data(
        self, table_name: str, limit: int = 100, database: str | None = None, schema: str | None = None
    ) -> list[dict[str, Any]]:
        """Get sample rows from a table.

        Args:
            table_name: Name of the table
            limit: Maximum number of rows to return
            database: Database name (uses current if None)
            schema: Schema name (uses current if None)

        Returns:
            List of sample row dictionaries
        """
        full_table_name = table_name
        if database and schema:
            full_table_name = f"{database}.{schema}.{table_name}"
        elif schema:
            full_table_name = f"{schema}.{table_name}"

        query = f"""
        SELECT *
        FROM {full_table_name}
        LIMIT {limit}
        """
        return await self.execute_query(query)

    async def get_table_row_count(
        self, table_name: str, database: str | None = None, schema: str | None = None
    ) -> int:
        """Get total row count for a table.

        Args:
            table_name: Name of the table
            database: Database name (uses current if None)
            schema: Schema name (uses current if None)

        Returns:
            Total number of rows
        """
        full_table_name = table_name
        if database and schema:
            full_table_name = f"{database}.{schema}.{table_name}"
        elif schema:
            full_table_name = f"{schema}.{table_name}"

        query = f"SELECT COUNT(*) as ROW_COUNT FROM {full_table_name}"
        result = await self.execute_query(query)
        return result[0]["ROW_COUNT"] if result else 0

