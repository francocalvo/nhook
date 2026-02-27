from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from notion_hook.models.notion_db import Cronograma
from notion_hook.workflows.local_db_sync import LocalDatabaseSyncWorkflow


class CronogramaDbSyncWorkflow(LocalDatabaseSyncWorkflow[Cronograma]):
    """Sync Cronograma pages into local SQLite."""

    name = "cronograma-sync"
    description = "Sync Cronograma to local SQLite database"

    async def _get_existing(self, page_id: str) -> Cronograma | None:
        return await self.database_client.get_cronograma(page_id)

    def _parse(self, page_id: str, payload: dict[str, Any]) -> Cronograma:
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
        return Cronograma.from_notion_page(page)

    async def _create(self, model: Cronograma) -> None:
        await self.database_client.create_cronograma(model)

    async def _update(self, model: Cronograma) -> bool:
        return await self.database_client.update_cronograma(model)

    async def _delete(self, page_id: str) -> bool:
        return await self.database_client.delete_cronograma(page_id)
