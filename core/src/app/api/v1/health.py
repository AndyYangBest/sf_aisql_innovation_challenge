import logging
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from ...core.config import settings
from ...core.db.database import SnowflakeConnection, async_get_db, get_snowflake_connection
from ...core.health import check_database_health, check_redis_health, check_snowflake_health
from ...core.schemas import HealthCheck, ReadyCheck
from ...core.utils.cache import async_get_redis

router = APIRouter(tags=["health"])

STATUS_HEALTHY = "healthy"
STATUS_UNHEALTHY = "unhealthy"

LOGGER = logging.getLogger(__name__)


@router.get("/health", response_model=HealthCheck)
async def health():
    http_status = status.HTTP_200_OK
    response = {
        "status": STATUS_HEALTHY,
        "environment": settings.ENVIRONMENT.value,
        "version": settings.APP_VERSION,
        "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
    }

    return JSONResponse(status_code=http_status, content=response)


@router.get("/ready", response_model=ReadyCheck)
async def ready(
    redis: Annotated[Redis, Depends(async_get_redis)],
    db: Annotated[AsyncSession, Depends(async_get_db)],
    sf_conn: Annotated[SnowflakeConnection, Depends(get_snowflake_connection)],
):
    # Check PostgreSQL database
    database_status = await check_database_health(db=db)
    LOGGER.debug(f"Database health check status: {database_status}")

    # Check Redis
    redis_status = await check_redis_health(redis=redis)
    LOGGER.debug(f"Redis health check status: {redis_status}")

    # Check Snowflake
    snowflake_status = await check_snowflake_health(sf_conn=sf_conn)
    LOGGER.debug(f"Snowflake health check status: {snowflake_status}")

    # Overall status - all must be healthy
    overall_status = (
        STATUS_HEALTHY if database_status and redis_status and snowflake_status else STATUS_UNHEALTHY
    )
    http_status = status.HTTP_200_OK if overall_status == STATUS_HEALTHY else status.HTTP_503_SERVICE_UNAVAILABLE

    response = {
        "status": overall_status,
        "environment": settings.ENVIRONMENT.value,
        "version": settings.APP_VERSION,
        "app": STATUS_HEALTHY,
        "database": STATUS_HEALTHY if database_status else STATUS_UNHEALTHY,
        "redis": STATUS_HEALTHY if redis_status else STATUS_UNHEALTHY,
        "snowflake": STATUS_HEALTHY if snowflake_status else STATUS_UNHEALTHY,
        "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
    }

    return JSONResponse(status_code=http_status, content=response)

