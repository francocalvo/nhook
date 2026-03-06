from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from notion_hook.models.gastos import Gasto
from notion_hook.models.notion_db import Ciudad
from notion_hook.models.webhook import WorkflowContext
from notion_hook.workflows.gastos_sync import GastosSyncWorkflow


class TestGastosSyncWorkflow:
    """Tests for the GastosSyncWorkflow with city resolution."""

    def test_matches_with_workflow_name(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test workflow matches when workflow name is gastos-sync."""
        workflow = GastosSyncWorkflow(mock_notion_client, mock_database_client)
        context = WorkflowContext(
            page_id="test-id",
            payload={"data": {"properties": {}}},
            workflow_name="gastos-sync",
        )
        assert workflow.matches(context) is True

    def test_does_not_match_with_wrong_name(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test workflow does not match when workflow name is different."""
        workflow = GastosSyncWorkflow(mock_notion_client, mock_database_client)
        context = WorkflowContext(
            page_id="test-id",
            payload={"data": {"properties": {}}},
            workflow_name="other-sync",
        )
        assert workflow.matches(context) is False

    @pytest.mark.asyncio
    async def test_create_resolves_ciudad_name(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test that ciudad name is resolved before creating gasto."""
        # Setup ciudad cache
        cached_ciudad = Ciudad(
            page_id="ciudad-123",
            name="Rome",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        mock_database_client.get_ciudad.return_value = cached_ciudad

        workflow = GastosSyncWorkflow(mock_notion_client, mock_database_client)

        gasto = Gasto(
            page_id="gasto-123",
            payment_method="Credit Card",
            description="Test expense",
            category="Food",
            amount=50.0,
            date="2024-01-15",
            persona="Franco",
            ciudad_page_id="ciudad-123",
            ciudad=None,  # Not resolved yet
            created_at="2024-01-15T10:00:00Z",
            updated_at="2024-01-15T10:00:00Z",
        )

        await workflow._create(gasto)

        # Verify ciudad was resolved
        assert gasto.ciudad == "Rome"
        mock_database_client.create_gasto.assert_called_once_with(gasto)

    @pytest.mark.asyncio
    async def test_update_resolves_ciudad_name(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test that ciudad name is resolved before updating gasto."""
        # Setup ciudad cache
        cached_ciudad = Ciudad(
            page_id="ciudad-456",
            name="Paris",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        mock_database_client.get_ciudad.return_value = cached_ciudad

        workflow = GastosSyncWorkflow(mock_notion_client, mock_database_client)

        gasto = Gasto(
            page_id="gasto-456",
            payment_method="Cash",
            description="Updated expense",
            category="Transport",
            amount=25.0,
            date="2024-01-20",
            persona="Mica",
            ciudad_page_id="ciudad-456",
            ciudad=None,  # Not resolved yet
            created_at="2024-01-20T10:00:00Z",
            updated_at="2024-01-20T10:00:00Z",
        )

        result = await workflow._update(gasto)

        # Verify ciudad was resolved
        assert gasto.ciudad == "Paris"
        mock_database_client.update_gasto.assert_called_once_with(gasto)
        assert result is True

    @pytest.mark.asyncio
    async def test_create_handles_missing_ciudad(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test handling gasto without ciudad relation."""
        workflow = GastosSyncWorkflow(mock_notion_client, mock_database_client)

        gasto = Gasto(
            page_id="gasto-789",
            payment_method="Credit Card",
            description="No city expense",
            category="Food",
            amount=30.0,
            date="2024-01-25",
            persona="Franco",
            ciudad_page_id=None,
            ciudad=None,
            created_at="2024-01-25T10:00:00Z",
            updated_at="2024-01-25T10:00:00Z",
        )

        await workflow._create(gasto)

        # Should not call ciudad resolver
        mock_database_client.get_ciudad.assert_not_called()
        # Should still create the gasto
        mock_database_client.create_gasto.assert_called_once()

    @pytest.mark.asyncio
    async def test_parse_extracts_ciudad_page_id(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test parsing gasto with ciudad relation from Notion."""
        workflow = GastosSyncWorkflow(mock_notion_client, mock_database_client)

        payload = {
            "data": {
                "properties": {
                    "Expense": {"title": [{"plain_text": "Test expense"}]},
                    "Amount": {"number": 100.0},
                    "Date": {"date": {"start": "2024-01-15"}},
                    "Ciudad": {"relation": [{"id": "ciudad-abc"}]},
                },
                "created_time": "2024-01-15T10:00:00Z",
                "last_edited_time": "2024-01-15T10:00:00Z",
            }
        }

        gasto = workflow._parse("gasto-abc", payload)

        assert gasto.page_id == "gasto-abc"
        assert gasto.ciudad_page_id == "ciudad-abc"
        assert gasto.ciudad is None  # Not resolved during parsing

    @pytest.mark.asyncio
    async def test_parse_handles_missing_ciudad_relation(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test parsing gasto without ciudad relation."""
        workflow = GastosSyncWorkflow(mock_notion_client, mock_database_client)

        payload = {
            "data": {
                "properties": {
                    "Expense": {"title": [{"plain_text": "Test expense"}]},
                    "Amount": {"number": 100.0},
                    "Date": {"date": {"start": "2024-01-15"}},
                },
                "created_time": "2024-01-15T10:00:00Z",
                "last_edited_time": "2024-01-15T10:00:00Z",
            }
        }

        gasto = workflow._parse("gasto-def", payload)

        assert gasto.page_id == "gasto-def"
        assert gasto.ciudad_page_id is None
        assert gasto.ciudad is None

    @pytest.mark.asyncio
    async def test_execute_full_workflow_with_ciudad(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test complete workflow execution with ciudad resolution."""
        # Setup mocks
        mock_database_client.get_gasto.return_value = None  # New gasto

        cached_ciudad = Ciudad(
            page_id="ciudad-xyz",
            name="Barcelona",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        mock_database_client.get_ciudad.return_value = cached_ciudad

        workflow = GastosSyncWorkflow(mock_notion_client, mock_database_client)

        context = WorkflowContext(
            page_id="gasto-xyz",
            payload={
                "data": {
                    "properties": {
                        "Expense": {"title": [{"plain_text": "Full workflow test"}]},
                        "Amount": {"number": 75.0},
                        "Date": {"date": {"start": "2024-02-01"}},
                        "Ciudad": {"relation": [{"id": "ciudad-xyz"}]},
                    },
                    "created_time": "2024-02-01T10:00:00Z",
                    "last_edited_time": "2024-02-01T10:00:00Z",
                }
            },
            workflow_name="gastos-sync",
        )

        result = await workflow.execute(context)

        assert result["operation"] == "create"
        # Verify gasto was created with resolved ciudad
        created_gasto = mock_database_client.create_gasto.call_args[0][0]
        assert created_gasto.ciudad == "Barcelona"
        assert created_gasto.ciudad_page_id == "ciudad-xyz"
