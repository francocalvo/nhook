from __future__ import annotations

from datetime import date
from typing import Any

import httpx

from notion_hook.config import Settings
from notion_hook.core.exceptions import NotionClientError
from notion_hook.core.logging import get_logger

logger = get_logger("clients.notion")

NOTION_API_BASE = "https://api.notion.com/v1"


class NotionClient:
    """Async client for Notion API operations."""

    def __init__(self, settings: Settings) -> None:
        """Initialize the Notion client.

        Args:
            settings: Application settings with Notion credentials.
        """
        self.settings = settings
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> NotionClient:
        """Enter async context and create HTTP client."""
        self._client = httpx.AsyncClient(
            base_url=NOTION_API_BASE,
            headers=self.settings.notion_headers,
            timeout=30.0,
        )
        return self

    async def __aexit__(self, *args: object) -> None:
        """Exit async context and close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    @property
    def client(self) -> httpx.AsyncClient:
        """Get the HTTP client, raising if not initialized."""
        if self._client is None:
            raise NotionClientError("NotionClient not initialized. Use async with.")
        return self._client

    async def get_page(self, page_id: str) -> dict[str, Any]:
        """Get a single page by ID.

        Args:
            page_id: The Notion page ID.

        Returns:
            The page data.

        Raises:
            NotionClientError: If the request fails.
        """
        response = await self.client.get(f"/pages/{page_id}")
        if response.status_code != 200:
            raise NotionClientError(
                f"Failed to get page {page_id}: {response.text}",
                status_code=response.status_code,
            )
        return response.json()

    async def update_page(
        self, page_id: str, properties: dict[str, Any]
    ) -> dict[str, Any]:
        """Update a page's properties.

        Args:
            page_id: The Notion page ID.
            properties: The properties to update.

        Returns:
            The updated page data.

        Raises:
            NotionClientError: If the request fails.
        """
        response = await self.client.patch(
            f"/pages/{page_id}",
            json={"properties": properties},
        )
        if response.status_code != 200:
            raise NotionClientError(
                f"Failed to update page {page_id}: {response.text}",
                status_code=response.status_code,
            )
        return response.json()

    async def query_database(
        self,
        database_id: str,
        filter_obj: dict[str, Any] | None = None,
        sorts: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Query a database with optional filters.

        Args:
            database_id: The Notion database ID.
            filter_obj: Optional filter object.
            sorts: Optional sort configuration.

        Returns:
            List of matching pages.

        Raises:
            NotionClientError: If the request fails.
        """
        body: dict[str, Any] = {}
        if filter_obj:
            body["filter"] = filter_obj
        if sorts:
            body["sorts"] = sorts

        all_results: list[dict[str, Any]] = []
        has_more = True
        start_cursor: str | None = None

        while has_more:
            if start_cursor:
                body["start_cursor"] = start_cursor

            response = await self.client.post(
                f"/databases/{database_id}/query",
                json=body,
            )
            if response.status_code != 200:
                raise NotionClientError(
                    f"Failed to query database {database_id}: {response.text}",
                    status_code=response.status_code,
                )

            data = response.json()
            all_results.extend(data.get("results", []))
            has_more = data.get("has_more", False)
            start_cursor = data.get("next_cursor")

        return all_results

    async def find_cronograma_by_dates(self, dates: list[date]) -> list[dict[str, Any]]:
        """Find Cronograma entries matching the given dates.

        The Cronograma database has a 'Día' title property with dates
        in 'yyyy-mm-dd' format.

        Args:
            dates: List of dates to find entries for.

        Returns:
            List of matching Cronograma pages.
        """
        if not dates:
            return []

        date_strings = [d.isoformat() for d in dates]
        logger.debug(f"Finding Cronograma entries for dates: {date_strings}")

        filter_conditions = [
            {"property": "Día", "title": {"equals": date_str}}
            for date_str in date_strings
        ]

        if len(filter_conditions) == 1:
            filter_obj = filter_conditions[0]
        else:
            filter_obj = {"or": filter_conditions}

        results = await self.query_database(
            self.settings.cronograma_database_id,
            filter_obj=filter_obj,
        )

        logger.info(f"Found {len(results)} Cronograma entries for {len(dates)} dates")
        return results

    async def update_gastos_cronograma_relation(
        self, page_id: str, cronograma_page_ids: list[str]
    ) -> dict[str, Any]:
        """Update the Cronograma relation on a Gastos page.

        Args:
            page_id: The Gastos page ID.
            cronograma_page_ids: List of Cronograma page IDs to relate.

        Returns:
            The updated page data.
        """
        relation_value = [{"id": pid} for pid in cronograma_page_ids]

        properties = {
            "Cronograma": {"relation": relation_value},
        }

        num_relations = len(cronograma_page_ids)
        logger.info(f"Updating Gastos {page_id} with {num_relations} relations")
        return await self.update_page(page_id, properties)

    async def update_pasajes_cronograma_relation(
        self, page_id: str, cronograma_page_ids: list[str]
    ) -> dict[str, Any]:
        """Update the Cronograma relation on a Pasajes page.

        Args:
            page_id: The Pasajes page ID.
            cronograma_page_ids: List of Cronograma page IDs to relate.

        Returns:
            The updated page data.
        """
        relation_value = [{"id": pid} for pid in cronograma_page_ids]

        properties = {
            "Cronograma": {"relation": relation_value},
        }

        num_relations = len(cronograma_page_ids)
        logger.info(f"Updating Pasajes {page_id} with {num_relations} relations")
        return await self.update_page(page_id, properties)
