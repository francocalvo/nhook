from __future__ import annotations

import os
from collections.abc import Generator
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
import respx
from fastapi.testclient import TestClient
from httpx import Response

os.environ["WEBHOOK_SECRET_KEY"] = "test-secret-key"
os.environ["NOTION_API_TOKEN"] = "secret_test_token"
os.environ["CRONOGRAMA_DATABASE_ID"] = "test-cronograma-db-id"
os.environ["GASTOS_DATABASE_ID"] = "test-gastos-db-id"
os.environ["PASAJES_DATABASE_ID"] = "test-pasajes-db-id"

from notion_hook.clients.notion import NotionClient
from notion_hook.config import Settings, clear_settings_cache


@pytest.fixture(autouse=True)
def clear_cache():
    """Clear settings cache before each test."""
    clear_settings_cache()
    yield


@pytest.fixture
def settings() -> Settings:
    """Return test settings."""
    return Settings(
        webhook_secret_key="test-secret-key",
        notion_api_token="secret_test_token",
        cronograma_database_id="test-cronograma-db-id",
        gastos_database_id="test-gastos-db-id",
        pasajes_database_id="test-pasajes-db-id",
        debug=True,
    )


@pytest.fixture
def mock_notion_client(settings: Settings) -> AsyncMock:
    """Return a mocked NotionClient."""
    client = AsyncMock(spec=NotionClient)
    client.settings = settings
    client.find_cronograma_by_dates = AsyncMock(return_value=[])
    client.update_gastos_cronograma_relation = AsyncMock(return_value={})
    client.update_pasajes_cronograma_relation = AsyncMock(return_value={})
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

    test_settings = Settings(
        webhook_secret_key="test-secret-key",
        notion_api_token="secret_test_token",
        cronograma_database_id="test-cronograma-db-id",
        gastos_database_id="test-gastos-db-id",
        pasajes_database_id="test-pasajes-db-id",
        debug=True,
    )

    mock_client = AsyncMock(spec=NotionClient)
    mock_client.settings = test_settings
    mock_client.find_cronograma_by_dates = AsyncMock(return_value=[])
    mock_client.update_gastos_cronograma_relation = AsyncMock(return_value={})
    mock_client.update_pasajes_cronograma_relation = AsyncMock(return_value={})

    registry = WorkflowRegistry(mock_client)
    registry.register(CronogramaSyncWorkflow)

    with respx.mock(assert_all_called=False) as respx_mock:
        respx_mock.post(
            f"https://api.notion.com/v1/databases/{test_settings.cronograma_database_id}/query"
        ).mock(return_value=Response(200, json={"results": [], "has_more": False}))

        respx_mock.patch(url__startswith="https://api.notion.com/v1/pages/").mock(
            return_value=Response(200, json={"id": "test-page-id"})
        )

        with (
            patch("notion_hook.config.get_settings", return_value=test_settings),
            patch("notion_hook.core.auth.get_settings", return_value=test_settings),
            patch("notion_hook.app._workflow_registry", registry),
            patch("notion_hook.app._notion_client", mock_client),
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            yield client


@pytest.fixture
def auth_headers() -> dict[str, str]:
    """Return valid auth headers."""
    return {
        "X-Calvo-Key": "test-secret-key",
        "X-Calvo-Workflow": "atracciones-cronograma",
    }


def make_notion_webhook_payload(
    page_id: str = "test-page-id",
    date_start: str | None = "2026-03-14",
    date_end: str | None = None,
    extra_properties: dict[str, Any] | None = None,
) -> dict:
    """Create a realistic Notion webhook payload.

    Args:
        page_id: The Notion page ID.
        date_start: Start date string in YYYY-MM-DD format, or None.
        date_end: End date string in YYYY-MM-DD format, or None.
        extra_properties: Additional properties to include.

    Returns:
        Dictionary representing Notion's webhook payload structure.
    """
    payload = {
        "source": {
            "type": "automation",
            "automation_id": "automation-123",
            "action_id": "action-456",
            "event_id": "event-789",
            "attempt": 1,
        },
        "data": {
            "object": "page",
            "id": page_id,
            "created_time": "2026-01-27T00:24:00.000Z",
            "last_edited_time": "2026-01-27T20:52:00.000Z",
            "created_by": {
                "object": "user",
                "id": "b5d45151-0759-4d92-8780-c7b0f004008b",
            },
            "last_edited_by": {
                "object": "user",
                "id": "b5d45151-0759-4d92-8780-c7b0f004008b",
            },
            "parent": {
                "type": "data_source_id",
                "data_source_id": "2e2f6e7f-0572-8010-9fb8-000b7db49de1",
                "database_id": "2e2f6e7f-0572-800c-962a-e8c9bf6cca51",
            },
            "archived": False,
            "in_trash": False,
            "properties": {},
        },
    }

    if extra_properties:
        payload["data"]["properties"].update(extra_properties)

    if date_start is None and date_end is None:
        payload["data"]["properties"]["Date"] = {
            "id": "date-property-id",
            "type": "date",
            "date": None,
        }
    elif date_start is not None:
        payload["data"]["properties"]["Date"] = {
            "id": "date-property-id",
            "type": "date",
            "date": {"start": date_start, "end": date_end, "time_zone": None},
        }

    return payload
