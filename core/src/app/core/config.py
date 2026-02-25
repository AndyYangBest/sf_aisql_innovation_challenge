import os
from enum import Enum
from typing import Any

from pydantic import SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    APP_NAME: str = "FastAPI app"
    APP_DESCRIPTION: str | None = None
    APP_VERSION: str | None = None
    LICENSE_NAME: str | None = None
    CONTACT_NAME: str | None = None
    CONTACT_EMAIL: str | None = None


class CryptSettings(BaseSettings):
    SECRET_KEY: SecretStr = SecretStr("secret-key")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7


class DatabaseSettings(BaseSettings):
    pass


class SQLiteSettings(DatabaseSettings):
    SQLITE_URI: str = "./sql_app.db"
    SQLITE_SYNC_PREFIX: str = "sqlite:///"
    SQLITE_ASYNC_PREFIX: str = "sqlite+aiosqlite:///"


class MySQLSettings(DatabaseSettings):
    MYSQL_USER: str = "username"
    MYSQL_PASSWORD: str = "password"
    MYSQL_SERVER: str = "localhost"
    MYSQL_PORT: int = 5432
    MYSQL_DB: str = "dbname"
    MYSQL_SYNC_PREFIX: str = "mysql://"
    MYSQL_ASYNC_PREFIX: str = "mysql+aiomysql://"
    MYSQL_URL: str | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def MYSQL_URI(self) -> str:
        credentials = f"{self.MYSQL_USER}:{self.MYSQL_PASSWORD}"
        location = f"{self.MYSQL_SERVER}:{self.MYSQL_PORT}/{self.MYSQL_DB}"
        return f"{credentials}@{location}"


class PostgresSettings(DatabaseSettings):
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_SERVER: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "postgres"
    POSTGRES_SYNC_PREFIX: str = "postgresql://"
    POSTGRES_ASYNC_PREFIX: str = "postgresql+asyncpg://"
    POSTGRES_URL: str | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def POSTGRES_URI(self) -> str:
        credentials = f"{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
        location = f"{self.POSTGRES_SERVER}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        return f"{credentials}@{location}"


class SnowflakeSettings(BaseSettings):
    """Snowflake connection settings for AI SQL and data analysis."""

    SNOWFLAKE_ACCOUNT: str = "WKUKTVG-CX42955"
    SNOWFLAKE_USER: str = "andy"
    SNOWFLAKE_USERNAME: str | None = None
    SNOWFLAKE_PASSWORD: SecretStr | None = SecretStr("MyJOBPass123!!!")
    SNOWFLAKE_AUTHENTICATOR: str | None = None
    SNOWFLAKE_PRIVATE_KEY_PATH: str | None = None
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE: SecretStr | None = None
    SNOWFLAKE_WAREHOUSE: str = "AI_SQL_COMP"
    SNOWFLAKE_DATABASE: str = "AI_SQL_COMP"
    SNOWFLAKE_SCHEMA: str = "PUBLIC"
    SNOWFLAKE_ROLE: str = "ACCOUNTADMIN"
    SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE: bool = True
    SNOWFLAKE_QUERY_TAG: str = "PLATTII_AI_ASSISTANT"
    SNOWFLAKE_STATEMENT_TIMEOUT_IN_SECONDS: int = 1800
    SNOWFLAKE_ABORT_DETACHED_QUERY: bool = False
    SNOWFLAKE_SSO_CACHE_TTL_SECONDS: int = 43200
    SNOWFLAKE_SSO_ENABLE_TOKEN_CACHE: bool = True
    SNOWFLAKE_CORTEX_MODEL: str = "claude-sonnet-4-5"
    SNOWFLAKE_CORTEX_COMPLETE_MAX_TOKENS: int = 8192
    SNOWFLAKE_CORTEX_IMAGE_MODEL: str = "pixtral-large"

    def _normalize_account(self, account: str) -> str:
        normalized = account.strip()
        normalized = normalized.removeprefix("https://").removeprefix("http://")
        normalized = normalized.split("/", 1)[0]
        if normalized.endswith(".snowflakecomputing.com"):
            normalized = normalized[: -len(".snowflakecomputing.com")]
        return normalized

    @computed_field  # type: ignore[prop-decorator]
    @property
    def SNOWFLAKE_EFFECTIVE_USER(self) -> str:
        username = (self.SNOWFLAKE_USERNAME or "").strip()
        return username or self.SNOWFLAKE_USER

    def _load_private_key_bytes(self) -> bytes | None:
        private_key_path = (self.SNOWFLAKE_PRIVATE_KEY_PATH or "").strip()
        if not private_key_path:
            return None

        from cryptography.hazmat.primitives import serialization

        with open(private_key_path, "rb") as key_file:
            key_data = key_file.read()

        passphrase = None
        if self.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE:
            secret = self.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE.get_secret_value()
            if secret:
                passphrase = secret.encode()

        private_key = serialization.load_pem_private_key(key_data, password=passphrase)
        return private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    # Optional: For Snowflake SQLAlchemy integration
    @computed_field  # type: ignore[prop-decorator]
    @property
    def SNOWFLAKE_CONNECTION_STRING(self) -> str:
        """Generate Snowflake connection string for SQLAlchemy."""
        password = self.SNOWFLAKE_PASSWORD.get_secret_value() if self.SNOWFLAKE_PASSWORD else ""
        user = self.SNOWFLAKE_EFFECTIVE_USER
        credentials = f"{user}:{password}" if password else user
        account = self._normalize_account(self.SNOWFLAKE_ACCOUNT)
        return (
            f"snowflake://{credentials}@"
            f"{account}/{self.SNOWFLAKE_DATABASE}/{self.SNOWFLAKE_SCHEMA}"
            f"?warehouse={self.SNOWFLAKE_WAREHOUSE}&role={self.SNOWFLAKE_ROLE}"
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def SNOWFLAKE_CONNECTOR_PARAMS(self) -> dict[str, Any]:
        """Get Snowflake connector parameters as dict."""
        params: dict[str, Any] = {
            "account": self._normalize_account(self.SNOWFLAKE_ACCOUNT),
            "user": self.SNOWFLAKE_EFFECTIVE_USER,
            "warehouse": self.SNOWFLAKE_WAREHOUSE,
            "database": self.SNOWFLAKE_DATABASE,
            "schema": self.SNOWFLAKE_SCHEMA,
            "role": self.SNOWFLAKE_ROLE,
            "client_session_keep_alive": self.SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE,
            "session_parameters": {
                "QUERY_TAG": self.SNOWFLAKE_QUERY_TAG,
                "STATEMENT_TIMEOUT_IN_SECONDS": self.SNOWFLAKE_STATEMENT_TIMEOUT_IN_SECONDS,
                "ABORT_DETACHED_QUERY": self.SNOWFLAKE_ABORT_DETACHED_QUERY,
            },
        }

        authenticator = (self.SNOWFLAKE_AUTHENTICATOR or "").strip()
        if authenticator:
            params["authenticator"] = authenticator

        if authenticator.lower() == "externalbrowser":
            return params

        password = self.SNOWFLAKE_PASSWORD.get_secret_value() if self.SNOWFLAKE_PASSWORD else ""
        if password:
            params["password"] = password
            return params

        private_key_bytes = self._load_private_key_bytes()
        if private_key_bytes:
            params["private_key"] = private_key_bytes

        return params


class ModelProviderSettings(BaseSettings):
    """Model provider configuration for Strands/OpenAI."""

    OPENAI_API_KEY: SecretStr | None = None
    OPENAI_MODEL_ID: str | None = None
    STRANDS_MODEL_ID: str | None = None
    STRANDS_MODEL_PROVIDER: str | None = None


class FirstUserSettings(BaseSettings):
    ADMIN_NAME: str = "admin"
    ADMIN_EMAIL: str = "admin@admin.com"
    ADMIN_USERNAME: str = "admin"
    ADMIN_PASSWORD: str = "!Ch4ng3Th1sP4ssW0rd!"


class TestSettings(BaseSettings):
    ...


class RedisCacheSettings(BaseSettings):
    REDIS_CACHE_HOST: str = "localhost"
    REDIS_CACHE_PORT: int = 6379

    @computed_field  # type: ignore[prop-decorator]
    @property
    def REDIS_CACHE_URL(self) -> str:
        return f"redis://{self.REDIS_CACHE_HOST}:{self.REDIS_CACHE_PORT}"


class ClientSideCacheSettings(BaseSettings):
    CLIENT_CACHE_MAX_AGE: int = 60


class RedisQueueSettings(BaseSettings):
    REDIS_QUEUE_HOST: str = "localhost"
    REDIS_QUEUE_PORT: int = 6379


class RedisRateLimiterSettings(BaseSettings):
    REDIS_RATE_LIMIT_HOST: str = "localhost"
    REDIS_RATE_LIMIT_PORT: int = 6379

    @computed_field  # type: ignore[prop-decorator]
    @property
    def REDIS_RATE_LIMIT_URL(self) -> str:
        return f"redis://{self.REDIS_RATE_LIMIT_HOST}:{self.REDIS_RATE_LIMIT_PORT}"


class DefaultRateLimitSettings(BaseSettings):
    DEFAULT_RATE_LIMIT_LIMIT: int = 10
    DEFAULT_RATE_LIMIT_PERIOD: int = 3600


class CRUDAdminSettings(BaseSettings):
    CRUD_ADMIN_ENABLED: bool = True
    CRUD_ADMIN_MOUNT_PATH: str = "/admin"

    CRUD_ADMIN_ALLOWED_IPS_LIST: list[str] | None = None
    CRUD_ADMIN_ALLOWED_NETWORKS_LIST: list[str] | None = None
    CRUD_ADMIN_MAX_SESSIONS: int = 10
    CRUD_ADMIN_SESSION_TIMEOUT: int = 1440
    SESSION_SECURE_COOKIES: bool = True

    CRUD_ADMIN_TRACK_EVENTS: bool = True
    CRUD_ADMIN_TRACK_SESSIONS: bool = True

    CRUD_ADMIN_REDIS_ENABLED: bool = False
    CRUD_ADMIN_REDIS_HOST: str = "localhost"
    CRUD_ADMIN_REDIS_PORT: int = 6379
    CRUD_ADMIN_REDIS_DB: int = 0
    CRUD_ADMIN_REDIS_PASSWORD: str | None = "None"
    CRUD_ADMIN_REDIS_SSL: bool = False


class EnvironmentOption(str, Enum):
    LOCAL = "local"
    STAGING = "staging"
    PRODUCTION = "production"


class EnvironmentSettings(BaseSettings):
    ENVIRONMENT: EnvironmentOption = EnvironmentOption.LOCAL


class CORSSettings(BaseSettings):
    CORS_ORIGINS: list[str] = ["*"]
    CORS_METHODS: list[str] = ["*"]
    CORS_HEADERS: list[str] = ["*"]


class Settings(
    AppSettings,
    SQLiteSettings,
    PostgresSettings,
    SnowflakeSettings,
    ModelProviderSettings,
    CryptSettings,
    FirstUserSettings,
    TestSettings,
    RedisCacheSettings,
    ClientSideCacheSettings,
    RedisQueueSettings,
    RedisRateLimiterSettings,
    DefaultRateLimitSettings,
    CRUDAdminSettings,
    EnvironmentSettings,
    CORSSettings,
):
    model_config = SettingsConfigDict(
        env_file=os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..", ".env"),
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )


settings = Settings()
