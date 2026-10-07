from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    api_key: str = Field(default="dev-api-key", alias="API_KEY")
    database_url: str = Field(
        default="postgresql+asyncpg://payments:payments@postgres:5432/payments",
        alias="DATABASE_URL",
    )
    rabbitmq_url: str = Field(default="amqp://guest:guest@rabbitmq:5672/", alias="RABBITMQ_URL")
    outbox_poll_interval_seconds: float = Field(default=1.0, alias="OUTBOX_POLL_INTERVAL_SECONDS")

    model_config = SettingsConfigDict(populate_by_name=True, env_file=".env", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()
