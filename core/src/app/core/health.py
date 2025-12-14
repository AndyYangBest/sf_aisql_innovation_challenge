import logging

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .db.database import SnowflakeConnection

LOGGER = logging.getLogger(__name__)


async def check_database_health(db: AsyncSession) -> bool:
    """Check PostgreSQL database health."""
    try:
        await db.execute(text("SELECT 1"))
        return True
    except Exception as e:
        LOGGER.exception(f"Database health check failed with error: {e}")
        return False


async def check_redis_health(redis: Redis) -> bool:
    """Check Redis connection health."""
    try:
        await redis.ping()
        return True
    except Exception as e:
        LOGGER.exception(f"Redis health check failed with error: {e}")
        return False


async def check_snowflake_health(sf_conn: SnowflakeConnection) -> bool:
    """Check Snowflake connection health."""
    try:
        # Simple query to verify connection
        result = await sf_conn.execute_query_async("SELECT CURRENT_VERSION()")
        if result and len(result) > 0:
            version = result[0].get("CURRENT_VERSION()", "unknown")
            LOGGER.info(f"Snowflake health check passed. Version: {version}")
            return True
        return False
    except Exception as e:
        LOGGER.exception(f"Snowflake health check failed with error: {e}")
        return False
