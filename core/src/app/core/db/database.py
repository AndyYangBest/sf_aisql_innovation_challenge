from collections.abc import AsyncGenerator
from typing import Any
import json

import snowflake.connector
from snowflake.connector.errors import ProgrammingError
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


# Alias for consistency with FastAPI dependency naming
get_async_db_session = async_get_db


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
        # Disable TIMESTAMP_TYPE_MAPPING to return timestamps as strings
        cursor = conn.cursor()
        cursor.execute("ALTER SESSION SET TIMESTAMP_TYPE_MAPPING = 'TIMESTAMP_NTZ'")
        cursor.execute("ALTER SESSION SET TIMESTAMP_OUTPUT_FORMAT = 'AUTO'")
        cursor.execute("ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_MERGE = FALSE")
        try:
            cursor.execute("ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_TIME = FALSE")
        except ProgrammingError:
            # Older/unsupported params—safe to ignore
            pass

        def _run(q: str):
            cursor.execute(q)

        try:
            try:
                _run(query)
            except ProgrammingError as primary_error:
                # Retry with a safer wrapper and stricter timestamp formatting for problematic data
                cursor.execute("ALTER SESSION SET TIMESTAMP_TYPE_MAPPING = 'TIMESTAMP_LTZ'")
                cursor.execute("ALTER SESSION SET TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6Z'")
                safe_query = f"SELECT * FROM ({query})"
                try:
                    _run(safe_query)
                except Exception:
                    # Final fallback: convert entire row to JSON to bypass type parsing
                    try:
                        fallback_query = f"SELECT TO_JSON(OBJECT_CONSTRUCT(*)) AS ROW_JSON FROM ({query})"
                        _run(fallback_query)
                        rows = cursor.fetchall()
                        return [json.loads(r[0]) if r and r[0] else {} for r in rows]
                    except Exception:
                        # Give up and re-raise the original error to be handled gracefully upstream
                        raise primary_error
            except Exception as primary_error:
                safe_query = f"SELECT * FROM ({query})"
                try:
                    _run(safe_query)
                except Exception:
                    raise primary_error

            # Use fetch_pandas_all() to get data as DataFrame
            # This handles large numbers and timestamps better
            try:
                import pandas as pd
                df = cursor.fetch_pandas_all()

                # Replace NaN and NaT with None for JSON serialization
                df = df.replace({pd.NaT: None, pd.NA: None})
                df = df.where(pd.notna(df), None)

                # Convert DataFrame to list of dicts
                # orient='records' creates list of row dicts
                results = df.to_dict(orient='records')

                # Post-process to ensure all values are JSON serializable
                for row in results:
                    for key, value in row.items():
                        if value is None or pd.isna(value):
                            row[key] = None
                        elif isinstance(value, (pd.Timestamp, pd.Timedelta)):
                            # Convert pandas Timestamp to ISO string
                            try:
                                row[key] = value.isoformat()
                            except:
                                row[key] = str(value)
                        elif hasattr(value, 'isoformat'):
                            # Handle datetime objects
                            try:
                                row[key] = value.isoformat()
                            except:
                                row[key] = str(value)
                        elif isinstance(value, bytes):
                            try:
                                row[key] = value.decode('utf-8')
                            except:
                                row[key] = value.hex()
                        # Convert numpy int64/float64 to Python native types
                        elif hasattr(value, 'item'):
                            try:
                                row[key] = value.item()
                            except:
                                row[key] = str(value)

                return results

            except (ImportError, Exception) as e:
                # Fallback if pandas is not available or other error occurs
                print(f"Pandas fetch failed: {e}, using fallback method")

                # Get column names
                columns = [col[0] for col in cursor.description] if cursor.description else []

                # Try to fetch rows
                try:
                    rows = cursor.fetchall()
                except Exception as fetch_error:
                    print(f"Fetchall failed: {fetch_error}")
                    # If even fetchall fails, return empty
                    return []

                results = []
                for row in rows:
                    row_dict = {}
                    for col_name, value in zip(columns, row):
                        if value is None:
                            row_dict[col_name] = None
                        else:
                            # Convert everything to string as fallback
                            try:
                                row_dict[col_name] = str(value)
                            except:
                                row_dict[col_name] = "<unconvertible>"
                    results.append(row_dict)

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
