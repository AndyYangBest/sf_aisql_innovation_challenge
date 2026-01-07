"""Security module for authentication."""

from enum import Enum
from fastapi.security import OAuth2PasswordBearer


class TokenType(str, Enum):
    """Token types."""

    ACCESS = "access"
    REFRESH = "refresh"


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)


async def verify_token(token: str, token_type: TokenType, db):
    """Verify token - placeholder implementation."""
    return None
