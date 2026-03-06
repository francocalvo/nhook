from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from notion_hook.models.notion_db import Ciudad
from notion_hook.services.city_resolver import CityResolver


class TestCityResolver:
    """Tests for the CityResolver service."""

    @pytest.mark.asyncio
    async def test_resolve_ciudad_names_empty_list(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test resolving empty list returns empty dict."""
        resolver = CityResolver(mock_notion_client, mock_database_client)
        result = await resolver.resolve_ciudad_names([])
        assert result == {}

    @pytest.mark.asyncio
    async def test_resolve_ciudad_names_all_none(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test resolving list of None values returns empty dict."""
        resolver = CityResolver(mock_notion_client, mock_database_client)
        result = await resolver.resolve_ciudad_names([None, None, None])
        assert result == {}

    @pytest.mark.asyncio
    async def test_resolve_ciudad_names_from_cache(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test resolving ciudad names from local cache."""
        cached_ciudad = Ciudad(
            page_id="ciudad-1",
            name="Rome",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        mock_database_client.get_ciudad.return_value = cached_ciudad

        resolver = CityResolver(mock_notion_client, mock_database_client)
        result = await resolver.resolve_ciudad_names(["ciudad-1"])

        assert result == {"ciudad-1": "Rome"}
        mock_database_client.get_ciudad.assert_called_once_with("ciudad-1")
        mock_notion_client.get_page.assert_not_called()

    @pytest.mark.asyncio
    async def test_resolve_ciudad_names_from_notion(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test resolving ciudad names from Notion API when not cached."""
        # First call checks cache, second call from _cache_ciudad
        mock_database_client.get_ciudad.side_effect = [None, None]

        notion_page = {
            "id": "ciudad-1",
            "properties": {"Name": {"title": [{"plain_text": "Paris"}]}},
            "created_time": "2024-01-01T00:00:00Z",
            "last_edited_time": "2024-01-01T00:00:00Z",
        }
        mock_notion_client.get_page.return_value = notion_page

        resolver = CityResolver(mock_notion_client, mock_database_client)
        result = await resolver.resolve_ciudad_names(["ciudad-1"])

        assert result == {"ciudad-1": "Paris"}
        # Should query cache (once for initial check, once for caching)
        assert mock_database_client.get_ciudad.call_count == 2
        mock_notion_client.get_page.assert_called_once_with("ciudad-1")
        # Should cache the fetched ciudad
        mock_database_client.create_ciudad.assert_called_once()

    @pytest.mark.asyncio
    async def test_resolve_ciudad_names_updates_cached_ciudad(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test updating cached ciudad when name changes."""
        cached_ciudad = Ciudad(
            page_id="ciudad-1",
            name="Old Name",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        mock_database_client.get_ciudad.return_value = cached_ciudad

        notion_page = {
            "id": "ciudad-1",
            "properties": {"Name": {"title": [{"plain_text": "New Name"}]}},
            "created_time": "2024-01-01T00:00:00Z",
            "last_edited_time": "2024-01-02T00:00:00Z",
        }
        mock_notion_client.get_page.return_value = notion_page

        resolver = CityResolver(mock_notion_client, mock_database_client)
        # First call uses cache
        result1 = await resolver.resolve_ciudad_names(["ciudad-1"])
        assert result1 == {"ciudad-1": "Old Name"}

        # Clear cache mock to simulate fetching from Notion
        mock_database_client.get_ciudad.return_value = None
        result2 = await resolver.resolve_ciudad_names(["ciudad-1"])
        assert result2 == {"ciudad-1": "New Name"}

    @pytest.mark.asyncio
    async def test_resolve_ciudad_names_handles_notion_error(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test handling errors when fetching from Notion."""
        mock_database_client.get_ciudad.return_value = None
        mock_notion_client.get_page.side_effect = Exception("Notion API error")

        resolver = CityResolver(mock_notion_client, mock_database_client)
        result = await resolver.resolve_ciudad_names(["ciudad-1"])

        assert result == {"ciudad-1": None}

    @pytest.mark.asyncio
    async def test_resolve_ciudad_names_deduplicates(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test that duplicate ciudad IDs are deduplicated."""
        cached_ciudad = Ciudad(
            page_id="ciudad-1",
            name="Rome",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        mock_database_client.get_ciudad.return_value = cached_ciudad

        resolver = CityResolver(mock_notion_client, mock_database_client)
        result = await resolver.resolve_ciudad_names(
            ["ciudad-1", "ciudad-1", "ciudad-1"]
        )

        assert result == {"ciudad-1": "Rome"}
        # Should only query cache once
        mock_database_client.get_ciudad.assert_called_once_with("ciudad-1")

    @pytest.mark.asyncio
    async def test_resolve_ciudad_names_mixed_cache_and_fetch(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test resolving mix of cached and uncached ciudades."""

        call_count = [0]

        def get_ciudad_side_effect(page_id: str) -> Ciudad | None:
            call_count[0] += 1
            if page_id == "ciudad-1":
                return Ciudad(
                    page_id="ciudad-1",
                    name="Rome",
                    created_at="2024-01-01T00:00:00Z",
                    updated_at="2024-01-01T00:00:00Z",
                )
            # ciudad-2 not cached initially
            return None

        mock_database_client.get_ciudad.side_effect = get_ciudad_side_effect

        notion_page = {
            "id": "ciudad-2",
            "properties": {"Name": {"title": [{"plain_text": "Paris"}]}},
            "created_time": "2024-01-01T00:00:00Z",
            "last_edited_time": "2024-01-01T00:00:00Z",
        }
        mock_notion_client.get_page.return_value = notion_page

        resolver = CityResolver(mock_notion_client, mock_database_client)
        result = await resolver.resolve_ciudad_names(
            ["ciudad-1", "ciudad-2", None, "ciudad-1"]
        )

        assert result == {"ciudad-1": "Rome", "ciudad-2": "Paris"}
        # Should query cache for ciudad-1 (once), ciudad-2 (once for initial
        # check, once for caching). Total: 3 calls (ciudad-1 once, ciudad-2 twice)
        assert mock_database_client.get_ciudad.call_count == 3
        # Should only fetch ciudad-2 from Notion
        mock_notion_client.get_page.assert_called_once_with("ciudad-2")

    @pytest.mark.asyncio
    async def test_cache_ciudad_creates_new(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test caching a new ciudad."""
        mock_database_client.get_ciudad.return_value = None

        ciudad = Ciudad(
            page_id="ciudad-1",
            name="Rome",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )

        resolver = CityResolver(mock_notion_client, mock_database_client)
        await resolver._cache_ciudad(ciudad)

        mock_database_client.create_ciudad.assert_called_once_with(ciudad)
        mock_database_client.update_ciudad.assert_not_called()

    @pytest.mark.asyncio
    async def test_cache_ciudad_updates_existing(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test updating existing cached ciudad."""
        existing_ciudad = Ciudad(
            page_id="ciudad-1",
            name="Old Name",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        mock_database_client.get_ciudad.return_value = existing_ciudad

        new_ciudad = Ciudad(
            page_id="ciudad-1",
            name="New Name",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-02T00:00:00Z",
        )

        resolver = CityResolver(mock_notion_client, mock_database_client)
        await resolver._cache_ciudad(new_ciudad)

        mock_database_client.update_ciudad.assert_called_once_with(new_ciudad)
        mock_database_client.create_ciudad.assert_not_called()

    @pytest.mark.asyncio
    async def test_cache_ciudad_skips_update_when_unchanged(
        self, mock_notion_client: AsyncMock, mock_database_client: AsyncMock
    ) -> None:
        """Test skipping update when ciudad name hasn't changed."""
        existing_ciudad = Ciudad(
            page_id="ciudad-1",
            name="Rome",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        mock_database_client.get_ciudad.return_value = existing_ciudad

        same_ciudad = Ciudad(
            page_id="ciudad-1",
            name="Rome",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )

        resolver = CityResolver(mock_notion_client, mock_database_client)
        await resolver._cache_ciudad(same_ciudad)

        mock_database_client.update_ciudad.assert_not_called()
        mock_database_client.create_ciudad.assert_not_called()
