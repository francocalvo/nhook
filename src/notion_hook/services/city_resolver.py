from __future__ import annotations

from notion_hook.clients.notion import NotionClient
from notion_hook.core.database import DatabaseClient
from notion_hook.core.logging import get_logger
from notion_hook.models.notion_db import Ciudad

logger = get_logger("services.city_resolver")


class CityResolver:
    """Resolves ciudad names from ciudad_page_ids with local caching."""

    def __init__(
        self,
        notion_client: NotionClient,
        database_client: DatabaseClient,
    ) -> None:
        """Initialize the CityResolver.

        Args:
            notion_client: The Notion API client.
            database_client: The database client.
        """
        self.notion_client = notion_client
        self.database_client = database_client

    async def resolve_ciudad_names(
        self, ciudad_page_ids: list[str | None]
    ) -> dict[str | None, str | None]:
        """Resolve ciudad names for a list of ciudad_page_ids.

        Looks up ciudad names in local cache first, then fetches from Notion
        if not found. Caches fetched ciudades in local database.

        Args:
            ciudad_page_ids: List of ciudad page IDs (may contain None).

        Returns:
            Mapping of ciudad_page_id to ciudad name (None values preserved).
        """
        # Filter out None values and deduplicate
        unique_ids = {pid for pid in ciudad_page_ids if pid is not None}

        if not unique_ids:
            return {}

        result: dict[str | None, str | None] = {}
        uncached_ids: list[str] = []

        # Try to get from local cache first
        for ciudad_id in unique_ids:
            cached = await self.database_client.get_ciudad(ciudad_id)
            if cached and cached.name:
                result[ciudad_id] = cached.name
                logger.debug(f"Ciudad cache hit: {ciudad_id} -> {cached.name}")
            else:
                uncached_ids.append(ciudad_id)

        # Fetch uncached ciudades from Notion
        if uncached_ids:
            logger.info(f"Fetching {len(uncached_ids)} ciudades from Notion")
            for ciudad_id in uncached_ids:
                try:
                    page = await self.notion_client.get_page(ciudad_id)
                    ciudad = Ciudad.from_notion_page(page)

                    # Cache in local database
                    await self._cache_ciudad(ciudad)

                    result[ciudad_id] = ciudad.name
                    logger.debug(
                        f"Ciudad fetched and cached: {ciudad_id} -> {ciudad.name}"
                    )
                except Exception as e:
                    logger.warning(f"Failed to fetch ciudad {ciudad_id}: {e}")
                    result[ciudad_id] = None

        return result

    async def _cache_ciudad(self, ciudad: Ciudad) -> None:
        """Cache a ciudad in the local database.

        Args:
            ciudad: The ciudad to cache.
        """
        try:
            existing = await self.database_client.get_ciudad(ciudad.page_id)
            if existing:
                # Update if changed
                if existing.name != ciudad.name:
                    await self.database_client.update_ciudad(ciudad)
                    logger.debug(f"Updated cached ciudad: {ciudad.page_id}")
            else:
                # Insert new
                await self.database_client.create_ciudad(ciudad)
                logger.debug(f"Cached new ciudad: {ciudad.page_id}")
        except Exception as e:
            logger.warning(f"Failed to cache ciudad {ciudad.page_id}: {e}")
