from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Server settings
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = False

    # Security
    webhook_secret_key: str

    # Notion API
    notion_api_token: str
    cronograma_database_id: str
    gastos_database_id: str
    pasajes_database_id: str

    @property
    def notion_headers(self) -> dict[str, str]:
        """Return headers for Notion API requests."""
        return {
            "Authorization": f"Bearer {self.notion_api_token}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json",
        }


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


def clear_settings_cache() -> None:
    """Clear the settings cache for testing."""
    get_settings.cache_clear()
