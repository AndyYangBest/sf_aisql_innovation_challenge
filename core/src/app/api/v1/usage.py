"""API endpoints for Snowflake usage (credits)."""

from fastapi import APIRouter, HTTPException, Query

from ...services.snowflake_service import SnowflakeService
from ...core.db.database import SnowflakeAuthenticationError

router = APIRouter(prefix="/usage", tags=["Usage"])


@router.get(
    "/credits",
    summary="Get Snowflake credit usage",
    description="Fetch Snowflake credits used from ACCOUNT_USAGE.METERING_HISTORY for the last N days",
)
async def get_credit_usage(
    days: int = Query(7, ge=1, le=90, description="Lookback window in days (1-90)"),
):
    """
    Returns:
      {
        "success": true,
        "data": {
          "days": 7,
          "total_credits": 12.345,
          "by_day": [{"day": "2026-01-25", "credits_used": 1.234}, ...]
        }
      }
    """
    try:
        service = SnowflakeService()
        query = f"""
        SELECT
            TO_VARCHAR(DATE_TRUNC('day', START_TIME), 'YYYY-MM-DD') AS day,
            SUM(CREDITS_USED) AS credits_used
        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
        WHERE START_TIME >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
        GROUP BY 1
        ORDER BY 1 DESC
        """
        rows = await service.execute_query(query)

        by_day: list[dict[str, object]] = []
        total = 0.0
        for row in rows or []:
            day = row.get("DAY") or row.get("day")
            credits = row.get("CREDITS_USED") or row.get("credits_used") or 0
            try:
                credits_f = float(credits) if credits is not None else 0.0
            except Exception:
                credits_f = 0.0
            total += credits_f
            by_day.append({"day": day, "credits_used": round(credits_f, 6)})

        return {"success": True, "data": {"days": days, "total_credits": round(total, 6), "by_day": by_day}}
    except SnowflakeAuthenticationError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except Exception as e:
        # Common cause: missing MONITOR USAGE privilege for ACCOUNT_USAGE views
        raise HTTPException(status_code=500, detail=str(e))

