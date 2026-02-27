from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from notion_hook.models.notion_db import Ciudad
from notion_hook.workflows.local_db_sync import LocalDatabaseSyncWorkflow


class CiudadesSyncWorkflow(LocalDatabaseSyncWorkflow[Ciudad]):
    """Sync Ciudades pages into local SQLite."""

    name = "ciudades-sync"
    description = "Sync Ciudades to local SQLite database"

    async def _get_existing(self, page_id: str) -> Ciudad | None:
        return await self.database_client.get_ciudad(page_id)

    def _parse(self, page_id: str, payload: dict[str, Any]) -> Ciudad:
        data = payload.get("data", {})
        page = {
            "id": page_id,
            "properties": data.get("properties", {}),
            "created_time": data.get(
                "created_time",
                datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            ),
            "last_edited_time": data.get(
                "last_edited_time",
                datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            ),
        }
        return Ciudad.from_notion_page(page)

    async def _create(self, model: Ciudad) -> None:
        await self.database_client.create_ciudad(model)

    async def _update(self, model: Ciudad) -> bool:
        return await self.database_client.update_ciudad(model)

    async def _delete(self, page_id: str) -> bool:
        return await self.database_client.delete_ciudad(page_id)
