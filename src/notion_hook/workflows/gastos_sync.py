from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from notion_hook.models.gastos import Gasto
from notion_hook.workflows.local_db_sync import LocalDatabaseSyncWorkflow


class GastosSyncWorkflow(LocalDatabaseSyncWorkflow[Gasto]):
    """Workflow to sync Gastos entries from Notion to local database."""

    name = "gastos-sync"
    description = "Sync Gastos to local SQLite database"

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

    async def _create(self, model: Gasto) -> None:
        await self.database_client.create_gasto(model)

    async def _update(self, model: Gasto) -> bool:
        return await self.database_client.update_gasto(model)

    async def _delete(self, page_id: str) -> bool:
        return await self.database_client.delete_gasto(page_id)
