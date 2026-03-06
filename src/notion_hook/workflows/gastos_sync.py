from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from notion_hook.models.gastos import Gasto
from notion_hook.services.city_resolver import CityResolver
from notion_hook.workflows.local_db_sync import LocalDatabaseSyncWorkflow


class GastosSyncWorkflow(LocalDatabaseSyncWorkflow[Gasto]):
    """Workflow to sync Gastos entries from Notion to local database."""

    name = "gastos-sync"
    description = "Sync Gastos to local SQLite database"

    def __init__(self, notion_client: Any, database_client: Any = None) -> None:
        """Initialize the workflow.

        Args:
            notion_client: The Notion API client.
            database_client: The database client (optional for matching).
        """
        super().__init__(notion_client, database_client)
        self._city_resolver: CityResolver | None = None

    def _get_city_resolver(self) -> CityResolver:
        """Get or create the city resolver.

        Returns:
            The CityResolver instance.
        """
        if self._city_resolver is None:
            if self.database_client is None:
                raise RuntimeError("Database client required for city resolution")
            self._city_resolver = CityResolver(self.notion_client, self.database_client)
        return self._city_resolver

    async def _get_existing(self, page_id: str) -> Gasto | None:
        return await self.database_client.get_gasto(page_id)

    def _parse(self, page_id: str, payload: dict[str, Any]) -> Gasto:
        data = payload.get("data", {})
        properties = data.get("properties", {})
        created_time = data.get(
            "created_time",
            datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        )
        last_edited_time = data.get(
            "last_edited_time",
            datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        )
        return Gasto.from_notion_properties(
            page_id,
            properties,
            created_time,
            last_edited_time,
        )

    async def _resolve_ciudad(self, gasto: Gasto) -> Gasto:
        """Resolve ciudad name from ciudad_page_id.

        Args:
            gasto: The gasto with ciudad_page_id to resolve.

        Returns:
            The gasto with ciudad field populated.
        """
        if gasto.ciudad_page_id is None:
            return gasto

        resolver = self._get_city_resolver()
        ciudad_map = await resolver.resolve_ciudad_names([gasto.ciudad_page_id])
        gasto.ciudad = ciudad_map.get(gasto.ciudad_page_id)
        return gasto

    async def _create(self, model: Gasto) -> None:
        # Resolve ciudad name before creating
        model = await self._resolve_ciudad(model)
        await self.database_client.create_gasto(model)

    async def _update(self, model: Gasto) -> bool:
        # Resolve ciudad name before updating
        model = await self._resolve_ciudad(model)
        return await self.database_client.update_gasto(model)

    async def _delete(self, page_id: str) -> bool:
        return await self.database_client.delete_gasto(page_id)
