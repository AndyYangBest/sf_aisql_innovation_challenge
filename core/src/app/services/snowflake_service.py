"""Snowflake connection and query service.

This service provides high-level methods for interacting with Snowflake
for data analysis, AI SQL functions, and metadata retrieval.
"""

import logging
from typing import Any


def _escape_literal(value: str) -> str:
    return value.replace("'", "''")


def _quote_identifier(value: str) -> str:
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


def _information_schema_source(database: str | None, object_name: str) -> str:
    if database:
        return f"{_quote_identifier(database)}.INFORMATION_SCHEMA.{object_name}"
    return f"INFORMATION_SCHEMA.{object_name}"

from ..core.db.database import SnowflakeConnection, get_snowflake_connection

logger = logging.getLogger(__name__)


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
        if database and schema:
            source_query = (
                "SHOW TABLES IN SCHEMA "
                f"{_quote_identifier(database)}.{_quote_identifier(schema)}"
            )
        elif database:
            source_query = f"SHOW TABLES IN DATABASE {_quote_identifier(database)}"
        else:
            source_query = "SHOW TABLES IN ACCOUNT"

        rows = await self.execute_query(source_query)

        results: list[dict[str, Any]] = []
        for row in rows:
            table_name = row.get("name") or row.get("NAME") or row.get("table_name") or row.get("TABLE_NAME")
            database_name = row.get("database_name") or row.get("DATABASE_NAME")
            schema_name = row.get("schema_name") or row.get("SCHEMA_NAME")
            if schema and schema_name and str(schema_name).upper() != str(schema).upper():
                continue
            if not table_name or not database_name or not schema_name:
                continue
            results.append(
                {
                    "DATABASE_NAME": database_name,
                    "SCHEMA_NAME": schema_name,
                    "TABLE_NAME": table_name,
                    "TABLE_TYPE": row.get("kind") or row.get("KIND") or "TABLE",
                    "ROW_COUNT": row.get("rows") or row.get("ROWS") or row.get("ROW_COUNT"),
                    "BYTES": row.get("bytes") or row.get("BYTES"),
                    "CREATED": row.get("created_on") or row.get("CREATED_ON") or row.get("CREATED"),
                    "LAST_ALTERED": row.get("last_altered") or row.get("LAST_ALTERED"),
                    "COMMENT": row.get("comment") or row.get("COMMENT"),
                }
            )
        results.sort(key=lambda item: (item.get("DATABASE_NAME", ""), item.get("TABLE_NAME", "")))
        return results

    async def get_databases(self) -> list[dict[str, Any]]:
        """Get list of available databases."""
        rows = await self.execute_query("SHOW DATABASES")
        results: list[dict[str, Any]] = []
        for row in rows:
            name = (
                row.get("name")
                or row.get("NAME")
                or row.get("database_name")
                or row.get("DATABASE_NAME")
            )
            if name:
                results.append({"DATABASE_NAME": name})
        results.sort(key=lambda r: r["DATABASE_NAME"])
        return results

    async def get_schemas(self, database: str) -> list[dict[str, Any]]:
        """Get list of schemas for a given database."""
        db_ident = _quote_identifier(database)
        rows = await self.execute_query(f"SHOW SCHEMAS IN DATABASE {db_ident}")
        results: list[dict[str, Any]] = []
        for row in rows:
            name = (
                row.get("name")
                or row.get("NAME")
                or row.get("schema_name")
                or row.get("SCHEMA_NAME")
            )
            if name:
                results.append({"SCHEMA_NAME": name})
        results.sort(key=lambda r: r["SCHEMA_NAME"])
        return results

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
        if database and schema:
            scoped_name = (
                f"{_quote_identifier(database)}.{_quote_identifier(schema)}."
                f"{_quote_identifier(table_name)}"
            )
        elif schema:
            scoped_name = f"{_quote_identifier(schema)}.{_quote_identifier(table_name)}"
        else:
            scoped_name = _quote_identifier(table_name)

        try:
            rows = await self.execute_query(f"SHOW COLUMNS IN TABLE {scoped_name}")
            mapped_rows: list[dict[str, Any]] = []
            for row in rows:
                column_name = row.get("column_name") or row.get("COLUMN_NAME") or row.get("name") or row.get("NAME")
                data_type = row.get("data_type") or row.get("DATA_TYPE") or row.get("type") or row.get("TYPE")
                if not column_name:
                    continue
                mapped_rows.append(
                    {
                        "COLUMN_NAME": column_name,
                        "ORDINAL_POSITION": row.get("ordinal_position") or row.get("ORDINAL_POSITION"),
                        "DATA_TYPE": data_type,
                        "IS_NULLABLE": row.get("is_nullable") or row.get("IS_NULLABLE"),
                        "COLUMN_DEFAULT": row.get("default") or row.get("COLUMN_DEFAULT"),
                        "CHARACTER_MAXIMUM_LENGTH": row.get("character_maximum_length"),
                        "NUMERIC_PRECISION": row.get("numeric_precision"),
                        "NUMERIC_SCALE": row.get("numeric_scale"),
                        "COMMENT": row.get("comment") or row.get("COMMENT"),
                    }
                )
            if mapped_rows:
                return mapped_rows
        except Exception as exc:
            logger.warning("SHOW COLUMNS failed for %s: %s", scoped_name, exc)

        table_source = _information_schema_source(database, "COLUMNS")
        db_clause = f"TABLE_CATALOG = '{_escape_literal(database)}'" if database else "TRUE"
        schema_clause = f"TABLE_SCHEMA = '{_escape_literal(schema)}'" if schema else "TRUE"
        table_name_clause = _escape_literal(table_name)

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
        FROM {table_source}
        WHERE {db_clause}
          AND {schema_clause}
          AND TABLE_NAME = '{table_name_clause}'
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
