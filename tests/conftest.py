from __future__ import annotations

import os
from collections.abc import Generator
from unittest.mock import AsyncMock, patch

import pytest
import respx
from fastapi.testclient import TestClient
from httpx import Response

os.environ.setdefault("WEBHOOK_SECRET_KEY", "test-secret-key")
os.environ.setdefault("NOTION_API_TOKEN", "secret_test_token")
os.environ.setdefault("CRONOGRAMA_DATABASE_ID", "test-cronograma-db-id")
os.environ.setdefault("GASTOS_DATABASE_ID", "test-gastos-db-id")

from notion_hook.clients.notion import NotionClient
from notion_hook.config import Settings, get_settings


@pytest.fixture
def settings() -> Settings:
    """Return test settings."""
    return Settings(
        webhook_secret_key="test-secret-key",
        notion_api_token="secret_test_token",
        cronograma_database_id="test-cronograma-db-id",
        gastos_database_id="test-gastos-db-id",
        debug=True,
    )


@pytest.fixture
def mock_notion_client(settings: Settings) -> AsyncMock:
    """Return a mocked NotionClient."""
    client = AsyncMock(spec=NotionClient)
    client.settings = settings
    client.find_cronograma_by_dates = AsyncMock(return_value=[])
    client.update_gastos_cronograma_relation = AsyncMock(return_value={})
    client.get_page = AsyncMock(return_value={})
    client.update_page = AsyncMock(return_value={})
    client.query_database = AsyncMock(return_value=[])
    return client


@pytest.fixture
def test_client() -> Generator[TestClient, None, None]:
    """Return a test client for the FastAPI app with mocked Notion API."""
    from notion_hook.app import app
    from notion_hook.workflows.cronograma_sync import CronogramaSyncWorkflow
    from notion_hook.workflows.registry import WorkflowRegistry

    mock_client = AsyncMock(spec=NotionClient)
    mock_client.settings = get_settings()
    mock_client.find_cronograma_by_dates = AsyncMock(return_value=[])
    mock_client.update_gastos_cronograma_relation = AsyncMock(return_value={})

    registry = WorkflowRegistry(mock_client)
    registry.register(CronogramaSyncWorkflow)

    with respx.mock(assert_all_called=False) as respx_mock:
        respx_mock.post(
            "https://api.notion.com/v1/databases/test-cronograma-db-id/query"
        ).mock(return_value=Response(200, json={"results": [], "has_more": False}))

        respx_mock.patch(url__startswith="https://api.notion.com/v1/pages/").mock(
            return_value=Response(200, json={"id": "test-page-id"})
        )

        with (
            patch("notion_hook.app._workflow_registry", registry),
            patch("notion_hook.app._notion_client", mock_client),
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            yield client


@pytest.fixture
def auth_headers() -> dict[str, str]:
    """Return valid auth headers."""
    return {"X-Calvo-Key": "test-secret-key"}
