from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from notion_hook.models.notion_db import Pasaje
from notion_hook.workflows.local_db_sync import LocalDatabaseSyncWorkflow


class PasajesDbSyncWorkflow(LocalDatabaseSyncWorkflow[Pasaje]):
    """Sync Pasajes pages into local SQLite."""

    name = "pasajes-sync"
    description = "Sync Pasajes to local SQLite database"

    async def _get_existing(self, page_id: str) -> Pasaje | None:
        return await self.database_client.get_pasaje(page_id)

    def _parse(self, page_id: str, payload: dict[str, Any]) -> Pasaje:
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
        return Pasaje.from_notion_page(page)

    async def _create(self, model: Pasaje) -> None:
        await self.database_client.create_pasaje(model)

    async def _update(self, model: Pasaje) -> bool:
        return await self.database_client.update_pasaje(model)

    async def _delete(self, page_id: str) -> bool:
        return await self.database_client.delete_pasaje(page_id)
