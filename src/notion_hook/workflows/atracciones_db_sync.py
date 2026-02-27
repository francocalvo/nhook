from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from notion_hook.models.notion_db import Atraccion
from notion_hook.workflows.local_db_sync import LocalDatabaseSyncWorkflow


class AtraccionesDbSyncWorkflow(LocalDatabaseSyncWorkflow[Atraccion]):
    """Sync Atracciones pages into local SQLite."""

    name = "atracciones-sync"
    description = "Sync Atracciones to local SQLite database"

    async def _get_existing(self, page_id: str) -> Atraccion | None:
        return await self.database_client.get_atraccion(page_id)

    def _parse(self, page_id: str, payload: dict[str, Any]) -> Atraccion:
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
        return Atraccion.from_notion_page(page)

    async def _create(self, model: Atraccion) -> None:
        await self.database_client.create_atraccion(model)

    async def _update(self, model: Atraccion) -> bool:
        return await self.database_client.update_atraccion(model)

    async def _delete(self, page_id: str) -> bool:
        return await self.database_client.delete_atraccion(page_id)
