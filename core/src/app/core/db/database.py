from collections.abc import AsyncGenerator
from typing import Any

import snowflake.connector
from snowflake.connector import DictCursor
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.ext.asyncio.session import AsyncSession
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass

from ..config import settings


class Base(DeclarativeBase, MappedAsDataclass):
    pass


# PostgreSQL database for application metadata (dashboards, users, etc.)
DATABASE_URI = settings.POSTGRES_URI
DATABASE_PREFIX = settings.POSTGRES_ASYNC_PREFIX
DATABASE_URL = f"{DATABASE_PREFIX}{DATABASE_URI}"

async_engine = create_async_engine(DATABASE_URL, echo=False, future=True)
local_session = async_sessionmaker(bind=async_engine, class_=AsyncSession, expire_on_commit=False)


async def async_get_db() -> AsyncGenerator[AsyncSession, None]:
    """Get PostgreSQL database session for metadata storage."""
    async with local_session() as db:
        yield db


# ============================================================================
# Snowflake Connection for Data Analysis
# ============================================================================

class SnowflakeConnection:
    """Snowflake connection manager for AI SQL and data analysis."""

    def __init__(self):
        self._connection: snowflake.connector.SnowflakeConnection | None = None

    def get_connection(self) -> snowflake.connector.SnowflakeConnection:
        """Get or create Snowflake connection."""
        if self._connection is None or self._connection.is_closed():
            params = settings.SNOWFLAKE_CONNECTOR_PARAMS
            self._connection = snowflake.connector.connect(**params)
        return self._connection

    def execute_query(self, query: str) -> list[dict[str, Any]]:
        """Execute a query and return results as list of dicts."""
        conn = self.get_connection()
        cursor = conn.cursor(DictCursor)
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()

    async def execute_query_async(self, query: str) -> list[dict[str, Any]]:
        """Execute query asynchronously (runs in thread pool)."""
        import asyncio

        return await asyncio.to_thread(self.execute_query, query)

    def close(self) -> None:
        """Close Snowflake connection."""
        if self._connection and not self._connection.is_closed():
            self._connection.close()
            self._connection = None


# Global Snowflake connection instance
snowflake_connection = SnowflakeConnection()


def get_snowflake_connection() -> SnowflakeConnection:
    """Dependency injection for Snowflake connection."""
    return snowflake_connection
