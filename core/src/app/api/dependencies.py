import base64
import importlib.util
import json
import threading
import time
from collections.abc import AsyncGenerator
from typing import Annotated, Any

from fastapi import Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from ..core.config import settings
from ..core.db.database import SnowflakeConnection, async_get_db
from ..core.exceptions.http_exceptions import ForbiddenException, RateLimitException, UnauthorizedException
from ..core.logger import logging
from ..core.security import TokenType, oauth2_scheme, verify_token
from ..core.utils.rate_limit import rate_limiter
from ..crud.crud_users import crud_users
from ..schemas.rate_limit import sanitize_path
from ..services.modular_ai_sql_service import ModularAISQLService
from ..services.snowflake_service import SnowflakeService

logger = logging.getLogger(__name__)

DEFAULT_LIMIT = settings.DEFAULT_RATE_LIMIT_LIMIT
DEFAULT_PERIOD = settings.DEFAULT_RATE_LIMIT_PERIOD
SNOWFLAKE_CONFIG_HEADER = "X-Snowflake-Config"
_DEFAULT_SSO_CACHE_TTL_SECONDS = 43200

_sso_connection_cache: dict[str, SnowflakeConnection] = {}
_sso_connection_last_used: dict[str, float] = {}
_sso_cache_lock = threading.Lock()
_sso_keyring_warning_emitted = False


def _decode_snowflake_config(raw_value: str) -> dict[str, Any]:
    try:
        decoded = base64.b64decode(raw_value).decode("utf-8")
        payload = json.loads(decoded)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid Snowflake config header") from exc

    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Invalid Snowflake config payload")
    return payload


def _get_required_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise HTTPException(status_code=400, detail=f"Missing Snowflake config field: {key}")
    return value.strip()


def _get_optional_string(payload: dict[str, Any], key: str, default_value: str) -> str:
    value = payload.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return default_value


def _normalize_snowflake_account(account: str) -> str:
    normalized = account.strip()
    normalized = normalized.removeprefix("https://").removeprefix("http://")
    normalized = normalized.split("/", 1)[0]
    if normalized.endswith(".snowflakecomputing.com"):
        normalized = normalized[: -len(".snowflakecomputing.com")]
    return normalized


def _extract_snowflake_connector_params(request: Request) -> dict[str, Any] | None:
    raw_config = request.headers.get(SNOWFLAKE_CONFIG_HEADER)
    if not raw_config:
        return None

    payload = _decode_snowflake_config(raw_config)
    account = _normalize_snowflake_account(_get_required_string(payload, "account"))
    user = _get_required_string(payload, "user")
    authenticator = _get_optional_string(payload, "authenticator", "")
    warehouse = _get_optional_string(payload, "warehouse", settings.SNOWFLAKE_WAREHOUSE or "")
    database = _get_optional_string(payload, "database", settings.SNOWFLAKE_DATABASE or "")
    schema = _get_optional_string(payload, "schema", settings.SNOWFLAKE_SCHEMA or "")
    role = _get_optional_string(payload, "role", settings.SNOWFLAKE_ROLE or "")
    params: dict[str, Any] = {
        "account": account,
        "user": user,
        "client_session_keep_alive": settings.SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE,
        "session_parameters": {
            "QUERY_TAG": settings.SNOWFLAKE_QUERY_TAG,
            "STATEMENT_TIMEOUT_IN_SECONDS": settings.SNOWFLAKE_STATEMENT_TIMEOUT_IN_SECONDS,
            "ABORT_DETACHED_QUERY": settings.SNOWFLAKE_ABORT_DETACHED_QUERY,
        },
    }
    if warehouse:
        params["warehouse"] = warehouse
    if database:
        params["database"] = database
    if schema:
        params["schema"] = schema
    if role:
        params["role"] = role

    if authenticator:
        params["authenticator"] = authenticator
    if authenticator.lower() != "externalbrowser":
        params["password"] = _get_required_string(payload, "password")
    elif settings.SNOWFLAKE_SSO_ENABLE_TOKEN_CACHE:
        params["client_store_temporary_credential"] = True

    return params


def _build_sso_cache_key(connector_params: dict[str, Any]) -> str:
    safe = {
        key: value
        for key, value in connector_params.items()
        if key not in {"password", "private_key", "private_key_file"}
    }
    return json.dumps(safe, sort_keys=True, default=str)


def _prune_stale_sso_connections(now: float, ttl_seconds: int) -> None:
    stale_keys = [
        key
        for key, last_used in _sso_connection_last_used.items()
        if now - last_used > ttl_seconds
    ]
    for key in stale_keys:
        conn = _sso_connection_cache.pop(key, None)
        _sso_connection_last_used.pop(key, None)
        if conn:
            conn.close()


def _get_or_create_sso_connection(connector_params: dict[str, Any]) -> SnowflakeConnection:
    global _sso_keyring_warning_emitted
    key = _build_sso_cache_key(connector_params)
    now = time.time()
    ttl_seconds = max(
        60,
        int(getattr(settings, "SNOWFLAKE_SSO_CACHE_TTL_SECONDS", _DEFAULT_SSO_CACHE_TTL_SECONDS)),
    )
    if (
        not _sso_keyring_warning_emitted
        and str(connector_params.get("authenticator", "")).lower() == "externalbrowser"
        and importlib.util.find_spec("keyring") is None
    ):
        logger.warning(
            "Snowflake SSO is running without keyring; token cache persistence is unavailable "
            "(install snowflake-connector-python[secure-local-storage] to reduce repeated logins)."
        )
        _sso_keyring_warning_emitted = True
    with _sso_cache_lock:
        _prune_stale_sso_connections(now, ttl_seconds)
        conn = _sso_connection_cache.get(key)
        if conn is None:
            conn = SnowflakeConnection(connector_params=connector_params)
            _sso_connection_cache[key] = conn
            logger.info(
                "Created new Snowflake SSO connection cache entry (ttl=%ss, entries=%s)",
                ttl_seconds,
                len(_sso_connection_cache),
            )
        else:
            logger.debug("Reusing cached Snowflake SSO connection")
        _sso_connection_last_used[key] = now
        return conn


async def get_snowflake_service(request: Request) -> AsyncGenerator[SnowflakeService, None]:
    connector_params = _extract_snowflake_connector_params(request)
    if connector_params is None:
        yield SnowflakeService()
        return

    authenticator = str(connector_params.get("authenticator", "")).lower()
    if authenticator == "externalbrowser":
        connection = _get_or_create_sso_connection(connector_params)
        yield SnowflakeService(sf_conn=connection)
        return

    connection = SnowflakeConnection(connector_params=connector_params)
    try:
        yield SnowflakeService(sf_conn=connection)
    finally:
        connection.close()


async def get_ai_sql_service(
    sf_service: Annotated[SnowflakeService, Depends(get_snowflake_service)],
) -> ModularAISQLService:
    return ModularAISQLService(sf_service)


async def get_current_user(
    token: Annotated[str, Depends(oauth2_scheme)], db: Annotated[AsyncSession, Depends(async_get_db)]
) -> dict[str, Any]:
    token_data = await verify_token(token, TokenType.ACCESS, db)
    if token_data is None:
        raise UnauthorizedException("User not authenticated.")

    if "@" in token_data.username_or_email:
        user = await crud_users.get(db=db, email=token_data.username_or_email, is_deleted=False)
    else:
        user = await crud_users.get(db=db, username=token_data.username_or_email, is_deleted=False)

    if user:
        return user

    raise UnauthorizedException("User not authenticated.")


async def get_optional_user(request: Request, db: AsyncSession = Depends(async_get_db)) -> dict | None:
    token = request.headers.get("Authorization")
    if not token:
        return None

    try:
        token_type, _, token_value = token.partition(" ")
        if token_type.lower() != "bearer" or not token_value:
            return None

        token_data = await verify_token(token_value, TokenType.ACCESS, db)
        if token_data is None:
            return None

        return await get_current_user(token_value, db=db)

    except HTTPException as http_exc:
        if http_exc.status_code != 401:
            logger.error(f"Unexpected HTTPException in get_optional_user: {http_exc.detail}")
        return None

    except Exception as exc:
        logger.error(f"Unexpected error in get_optional_user: {exc}")
        return None


async def get_current_superuser(current_user: Annotated[dict, Depends(get_current_user)]) -> dict:
    if not current_user["is_superuser"]:
        raise ForbiddenException("You do not have enough privileges.")

    return current_user


async def rate_limiter_dependency(
    request: Request, db: Annotated[AsyncSession, Depends(async_get_db)], user: dict | None = Depends(get_optional_user)
) -> None:
    if hasattr(request.app.state, "initialization_complete"):
        await request.app.state.initialization_complete.wait()

    path = sanitize_path(request.url.path)
    if user:
        user_id = user["id"]
        limit, period = DEFAULT_LIMIT, DEFAULT_PERIOD
    else:
        user_id = request.client.host if request.client else "unknown"
        limit, period = DEFAULT_LIMIT, DEFAULT_PERIOD

    is_limited = await rate_limiter.is_rate_limited(db=db, user_id=user_id, path=path, limit=limit, period=period)
    if is_limited:
        raise RateLimitException("Rate limit exceeded.")
