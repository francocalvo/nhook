from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from notion_hook.core.database import DatabaseClient, DatabaseError
from notion_hook.core.logging import get_logger
from notion_hook.models.gastos import Gasto
from notion_hook.workflows.base import BaseWorkflow

if TYPE_CHECKING:
    from notion_hook.models.webhook import WorkflowContext

logger = get_logger("workflows.gastos_sync")


class GastosSyncWorkflow(BaseWorkflow):
    """Workflow to sync Gastos entries from Notion to local database."""

    name = "gastos-sync"
    description = "Sync Gastos to local SQLite database"

    def __init__(self, notion_client: Any, database_client: DatabaseClient) -> None:
        """Initialize the gastos sync workflow.

        Args:
            notion_client: The Notion API client instance.
            database_client: The database client instance.
        """
        super().__init__(notion_client)
        self.database_client = database_client

    def matches(self, context: WorkflowContext) -> bool:
        """Check if this workflow should handle the webhook.

        Args:
            context: The webhook context.

        Returns:
            True if workflow name matches 'gastos-sync'.
        """
        return context.workflow_name == self.name

    async def execute(self, context: WorkflowContext) -> dict[str, Any]:
        """Execute the gastos sync workflow.

        Detects operation type (CREATE/UPDATE/DELETE) and performs
        corresponding database operation.

        Args:
            context: The webhook context.

        Returns:
            A dictionary with execution results.

        Raises:
            WorkflowError: If workflow execution fails.
        """
        page_id = context.page_id
        payload = context.payload

        operation = await self._detect_operation(page_id, payload)
        logger.info(f"Detected operation: {operation} for page {page_id}")

        try:
            if operation == "delete":
                result = await self._handle_delete(page_id)
            else:
                gasto = await self._parse_gasto(page_id, payload)
                if operation == "create":
                    result = await self._handle_create(gasto)
                else:
                    result = await self._handle_update(gasto)

            return result

        except DatabaseError as e:
            await self.database_client.log_failure(
                page_id=page_id,
                operation=operation,
                error_message=str(e),
                retry_count=self.database_client.settings.max_retries,
            )
            logger.error(f"Database error for {operation} on page {page_id}: {e}")
            raise

    async def _detect_operation(self, page_id: str, payload: dict[str, Any]) -> str:
        """Detect operation type from payload.

        Args:
            page_id: The Notion page ID.
            payload: The webhook payload.

        Returns:
            Operation type: 'create', 'update', or 'delete'.
        """
        data = payload.get("data", {})
        properties = data.get("properties", {})

        if not properties:
            return "delete"

        existing_gasto = await self.database_client.get_gasto(page_id)
        if existing_gasto is None:
            return "create"
        return "update"

    async def _parse_gasto(self, page_id: str, payload: dict[str, Any]) -> Gasto:
        """Parse gasto data from webhook payload.

        Args:
            page_id: The Notion page ID.
            payload: The webhook payload.

        Returns:
            Gasto instance.
        """
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

        gasto = Gasto.from_notion_properties(
            page_id, properties, created_time, last_edited_time
        )
        return gasto

    async def _handle_create(self, gasto: Gasto) -> dict[str, Any]:
        """Handle CREATE operation.

        Args:
            gasto: The gasto to create.

        Returns:
            Result dictionary.
        """
        logger.info(f"Creating gasto: {gasto.page_id}")
        await self.database_client.create_gasto(gasto)
        return {"operation": "create", "page_id": gasto.page_id, "success": True}

    async def _handle_update(self, gasto: Gasto) -> dict[str, Any]:
        """Handle UPDATE operation.

        Args:
            gasto: The gasto to update.

        Returns:
            Result dictionary.
        """
        logger.info(f"Updating gasto: {gasto.page_id}")
        updated = await self.database_client.update_gasto(gasto)
        return {
            "operation": "update",
            "page_id": gasto.page_id,
            "success": True,
            "updated": updated,
        }

    async def _handle_delete(self, page_id: str) -> dict[str, Any]:
        """Handle DELETE operation.

        Args:
            page_id: The page ID to delete.

        Returns:
            Result dictionary.
        """
        logger.info(f"Deleting gasto: {page_id}")
        deleted = await self.database_client.delete_gasto(page_id)
        return {
            "operation": "delete",
            "page_id": page_id,
            "success": True,
            "deleted": deleted,
        }
