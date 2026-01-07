"""Rate limit schemas."""

import re
from pydantic import BaseModel


class RateLimitRead(BaseModel):
    """Rate limit read schema."""

    id: int
    tier_id: int
    path: str
    limit: int
    period: int


def sanitize_path(path: str) -> str:
    """Sanitize path for rate limiting by removing dynamic segments.

    Converts paths like /api/v1/users/123 to /api/v1/users/{id}
    """
    # Replace numeric IDs with {id}
    path = re.sub(r'/\d+', '/{id}', path)
    # Replace UUIDs with {id}
    path = re.sub(r'/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '/{id}', path, flags=re.IGNORECASE)
    return path
