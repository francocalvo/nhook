from __future__ import annotations

import os
from unittest.mock import AsyncMock

import pytest

os.environ["WEBHOOK_SECRET_KEY"] = "test-secret-key"
os.environ["NOTION_API_TOKEN"] = "secret_test_token"
os.environ["CRONOGRAMA_DATABASE_ID"] = "test-cronograma-db-id"
os.environ["GASTOS_DATABASE_ID"] = "test-gastos-db-id"
os.environ["PASAJES_DATABASE_ID"] = "test-pasajes-db-id"

from notion_hook.config import Settings, clear_settings_cache
from notion_hook.core.database import DatabaseClient
from notion_hook.models.gastos import Gasto
from notion_hook.models.webhook import WorkflowContext
from notion_hook.workflows.gastos_sync import GastosSyncWorkflow
from tests.conftest import make_notion_webhook_payload


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
        max_retries=2,
        retry_delay=0.01,
    )


@pytest.fixture
def mock_notion_client() -> AsyncMock:
    """Return a mocked NotionClient."""
    return AsyncMock()


@pytest.fixture
def mock_db_client(settings: Settings) -> AsyncMock:
    """Return a mocked DatabaseClient."""
    client = AsyncMock(spec=DatabaseClient)
    client.settings = settings
    client.get_gasto = AsyncMock(return_value=None)
    client.create_gasto = AsyncMock()
    client.update_gasto = AsyncMock(return_value=True)
    client.delete_gasto = AsyncMock(return_value=True)
    client.log_failure = AsyncMock()
    return client


class TestGastosSyncWorkflow:
    """Tests for GastosSyncWorkflow."""

    def test_matches_with_correct_workflow_name(
        self, mock_notion_client: AsyncMock, mock_db_client: AsyncMock
    ) -> None:
        """Test workflow matches when workflow name is gastos-sync."""
        workflow = GastosSyncWorkflow(mock_notion_client, mock_db_client)
        context = WorkflowContext(
            page_id="test-id",
            payload={},
            workflow_name="gastos-sync",
        )
        assert workflow.matches(context) is True

    def test_does_not_match_wrong_workflow_name(
        self, mock_notion_client: AsyncMock, mock_db_client: AsyncMock
    ) -> None:
        """Test workflow doesn't match wrong workflow name."""
        workflow = GastosSyncWorkflow(mock_notion_client, mock_db_client)
        context = WorkflowContext(
            page_id="test-id",
            payload={},
            workflow_name="cronograma",
        )
        assert workflow.matches(context) is False

    @pytest.mark.asyncio
    async def test_execute_create_operation(
        self, mock_notion_client: AsyncMock, mock_db_client: AsyncMock
    ) -> None:
        """Test CREATE operation handling."""
        workflow = GastosSyncWorkflow(mock_notion_client, mock_db_client)
        payload = make_notion_webhook_payload(
            page_id="test-page-1",
            extra_properties={
                "Payment Method": {
                    "id": "pm-id",
                    "type": "select",
                    "select": {"id": "opt-1", "name": "Cash"},
                },
                "Description": {
                    "id": "desc-id",
                    "type": "title",
                    "title": [{"text": {"content": "Test expense"}}],
                },
                "Amount": {
                    "id": "amount-id",
                    "type": "number",
                    "number": 100.0,
                },
                "Date": {
                    "id": "date-id",
                    "type": "date",
                    "date": {"start": "2024-01-01"},
                },
            },
        )
        mock_db_client.get_gasto.return_value = None

        context = WorkflowContext(
            page_id="test-page-1",
            payload=payload,
            workflow_name="gastos-sync",
        )

        result = await workflow.execute(context)

        assert result["operation"] == "create"
        assert result["success"] is True
        mock_db_client.create_gasto.assert_called_once()
        created_gasto = mock_db_client.create_gasto.call_args[0][0]
        assert created_gasto.page_id == "test-page-1"
        assert created_gasto.payment_method == "Cash"
        assert created_gasto.description == "Test expense"
        assert created_gasto.amount == 100.0

    @pytest.mark.asyncio
    async def test_execute_update_operation(
        self, mock_notion_client: AsyncMock, mock_db_client: AsyncMock
    ) -> None:
        """Test UPDATE operation handling."""
        workflow = GastosSyncWorkflow(mock_notion_client, mock_db_client)
        payload = make_notion_webhook_payload(
            page_id="test-page-2",
            extra_properties={
                "Amount": {
                    "id": "amount-id",
                    "type": "number",
                    "number": 200.0,
                },
            },
        )
        mock_db_client.get_gasto.return_value = Gasto(
            page_id="test-page-2",
            payment_method="Cash",
            description="Test",
            amount=100.0,
            date="2024-01-01",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )

        context = WorkflowContext(
            page_id="test-page-2",
            payload=payload,
            workflow_name="gastos-sync",
        )

        result = await workflow.execute(context)

        assert result["operation"] == "update"
        assert result["success"] is True
        mock_db_client.update_gasto.assert_called_once()
        updated_gasto = mock_db_client.update_gasto.call_args[0][0]
        assert updated_gasto.amount == 200.0

    @pytest.mark.asyncio
    async def test_execute_delete_operation(
        self, mock_notion_client: AsyncMock, mock_db_client: AsyncMock
    ) -> None:
        """Test DELETE operation handling."""
        workflow = GastosSyncWorkflow(mock_notion_client, mock_db_client)
        payload = {
            "data": {
                "id": "test-page-3",
                "properties": {},
            },
        }

        context = WorkflowContext(
            page_id="test-page-3",
            payload=payload,
            workflow_name="gastos-sync",
        )

        result = await workflow.execute(context)

        assert result["operation"] == "delete"
        assert result["success"] is True
        mock_db_client.delete_gasto.assert_called_once_with("test-page-3")

    @pytest.mark.asyncio
    async def test_property_parsing_with_missing_fields(
        self, mock_notion_client: AsyncMock, mock_db_client: AsyncMock
    ) -> None:
        """Test property parsing with missing fields."""
        workflow = GastosSyncWorkflow(mock_notion_client, mock_db_client)
        payload = make_notion_webhook_payload(
            page_id="test-page-4",
            date_start=None,
            date_end=None,
            extra_properties={},
        )
        mock_db_client.get_gasto.return_value = None

        context = WorkflowContext(
            page_id="test-page-4",
            payload=payload,
            workflow_name="gastos-sync",
        )

        result = await workflow.execute(context)

        assert result["operation"] == "create"
        mock_db_client.create_gasto.assert_called_once()
        created_gasto = mock_db_client.create_gasto.call_args[0][0]
        assert created_gasto.payment_method is None
        assert created_gasto.description is None
        assert created_gasto.amount is None
        assert created_gasto.date is None

    @pytest.mark.asyncio
    async def test_execute_with_case_insensitive_properties(
        self, mock_notion_client: AsyncMock, mock_db_client: AsyncMock
    ) -> None:
        """Test case-insensitive property parsing."""
        workflow = GastosSyncWorkflow(mock_notion_client, mock_db_client)
        payload = make_notion_webhook_payload(
            page_id="test-page-5",
            extra_properties={
                "payment_method": {
                    "id": "pm-id",
                    "type": "select",
                    "select": {"id": "opt-1", "name": "Credit Card"},
                },
                "amount": {
                    "id": "amount-id",
                    "type": "number",
                    "number": 150.0,
                },
            },
        )
        mock_db_client.get_gasto.return_value = None

        context = WorkflowContext(
            page_id="test-page-5",
            payload=payload,
            workflow_name="gastos-sync",
        )

        result = await workflow.execute(context)

        assert result["operation"] == "create"
        created_gasto = mock_db_client.create_gasto.call_args[0][0]
        assert created_gasto.payment_method == "Credit Card"
        assert created_gasto.amount == 150.0
