from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from notion_hook.models.gastos import Gasto
from notion_hook.models.notion_db import Ciudad
from notion_hook.services.gastos_reload import GastosReloadService


class TestGastosReloadServiceCiudad:
    """Tests for GastosReloadService ciudad resolution."""

    @pytest.mark.asyncio
    async def test_process_batch_resolves_ciudades(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test that batch processing resolves ciudad names."""

        # Setup mock ciudades in cache
        def get_ciudad_side_effect(page_id: str) -> Ciudad | None:
            ciudades = {
                "ciudad-1": Ciudad(
                    page_id="ciudad-1",
                    name="Rome",
                    created_at="2024-01-01T00:00:00Z",
                    updated_at="2024-01-01T00:00:00Z",
                ),
                "ciudad-2": Ciudad(
                    page_id="ciudad-2",
                    name="Paris",
                    created_at="2024-01-01T00:00:00Z",
                    updated_at="2024-01-01T00:00:00Z",
                ),
            }
            return ciudades.get(page_id)

        mock_database_client.get_ciudad.side_effect = get_ciudad_side_effect
        mock_database_client.sync_gastos_batch.return_value = (2, 0, 0, 0)

        service = GastosReloadService(mock_notion_client, mock_database_client)

        # Create a mock job
        from notion_hook.services.gastos_reload import ReloadJob

        job = ReloadJob(job_id="test-job")

        # Create batch of Notion pages
        batch = [
            {
                "id": "gasto-1",
                "properties": {
                    "Expense": {"title": [{"plain_text": "Expense 1"}]},
                    "Amount": {"number": 100.0},
                    "Date": {"date": {"start": "2024-01-15"}},
                    "Ciudad": {"relation": [{"id": "ciudad-1"}]},
                },
                "created_time": "2024-01-15T10:00:00Z",
                "last_edited_time": "2024-01-15T10:00:00Z",
            },
            {
                "id": "gasto-2",
                "properties": {
                    "Expense": {"title": [{"plain_text": "Expense 2"}]},
                    "Amount": {"number": 200.0},
                    "Date": {"date": {"start": "2024-01-16"}},
                    "Ciudad": {"relation": [{"id": "ciudad-2"}]},
                },
                "created_time": "2024-01-16T10:00:00Z",
                "last_edited_time": "2024-01-16T10:00:00Z",
            },
        ]

        await service._process_batch(job, batch, update_if_changed=False)

        # Verify gastos were synced with resolved ciudades
        sync_call = mock_database_client.sync_gastos_batch.call_args
        gastos = sync_call[0][0]

        assert len(gastos) == 2
        assert gastos[0].ciudad == "Rome"
        assert gastos[0].ciudad_page_id == "ciudad-1"
        assert gastos[1].ciudad == "Paris"
        assert gastos[1].ciudad_page_id == "ciudad-2"

    @pytest.mark.asyncio
    async def test_process_batch_handles_missing_ciudad(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test batch processing handles gastos without ciudad."""
        mock_database_client.get_ciudad.return_value = None
        mock_database_client.sync_gastos_batch.return_value = (1, 0, 0, 0)

        service = GastosReloadService(mock_notion_client, mock_database_client)

        from notion_hook.services.gastos_reload import ReloadJob

        job = ReloadJob(job_id="test-job")

        batch = [
            {
                "id": "gasto-3",
                "properties": {
                    "Expense": {"title": [{"plain_text": "No city expense"}]},
                    "Amount": {"number": 50.0},
                    "Date": {"date": {"start": "2024-01-17"}},
                    # No Ciudad relation
                },
                "created_time": "2024-01-17T10:00:00Z",
                "last_edited_time": "2024-01-17T10:00:00Z",
            },
        ]

        await service._process_batch(job, batch, update_if_changed=False)

        # Verify gasto was synced without ciudad
        sync_call = mock_database_client.sync_gastos_batch.call_args
        gastos = sync_call[0][0]

        assert len(gastos) == 1
        assert gastos[0].ciudad is None
        assert gastos[0].ciudad_page_id is None

    @pytest.mark.asyncio
    async def test_resolve_ciudades_batch_fetches_from_notion(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test that ciudad resolver fetches from Notion when not cached."""
        mock_database_client.get_ciudad.return_value = None

        notion_page = {
            "id": "ciudad-new",
            "properties": {"Name": {"title": [{"plain_text": "Madrid"}]}},
            "created_time": "2024-01-01T00:00:00Z",
            "last_edited_time": "2024-01-01T00:00:00Z",
        }
        mock_notion_client.get_page.return_value = notion_page

        service = GastosReloadService(mock_notion_client, mock_database_client)

        gastos = [
            Gasto(
                page_id="gasto-new",
                payment_method="Credit Card",
                description="New city expense",
                category="Food",
                amount=75.0,
                date="2024-01-20",
                persona="Franco",
                ciudad_page_id="ciudad-new",
                ciudad=None,
                created_at="2024-01-20T10:00:00Z",
                updated_at="2024-01-20T10:00:00Z",
            ),
        ]

        await service._resolve_ciudades_batch(gastos)

        # Verify ciudad was fetched and resolved
        assert gastos[0].ciudad == "Madrid"
        mock_notion_client.get_page.assert_called_once_with("ciudad-new")
        # Should cache the fetched ciudad
        mock_database_client.create_ciudad.assert_called_once()

    @pytest.mark.asyncio
    async def test_resolve_ciudades_batch_handles_duplicates(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test that duplicate ciudad IDs are resolved efficiently."""
        cached_ciudad = Ciudad(
            page_id="ciudad-dup",
            name="Lisbon",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        mock_database_client.get_ciudad.return_value = cached_ciudad

        service = GastosReloadService(mock_notion_client, mock_database_client)

        gastos = [
            Gasto(
                page_id="gasto-1",
                payment_method="Cash",
                description="Expense 1",
                category="Transport",
                amount=20.0,
                date="2024-01-21",
                persona="Mica",
                ciudad_page_id="ciudad-dup",
                ciudad=None,
                created_at="2024-01-21T10:00:00Z",
                updated_at="2024-01-21T10:00:00Z",
            ),
            Gasto(
                page_id="gasto-2",
                payment_method="Credit Card",
                description="Expense 2",
                category="Food",
                amount=30.0,
                date="2024-01-22",
                persona="Franco",
                ciudad_page_id="ciudad-dup",
                ciudad=None,
                created_at="2024-01-22T10:00:00Z",
                updated_at="2024-01-22T10:00:00Z",
            ),
        ]

        await service._resolve_ciudades_batch(gastos)

        # Both gastos should have ciudad resolved
        assert gastos[0].ciudad == "Lisbon"
        assert gastos[1].ciudad == "Lisbon"
        # Should only query cache once (deduplication)
        mock_database_client.get_ciudad.assert_called_once_with("ciudad-dup")
