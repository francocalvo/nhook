from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from notion_hook.core.database import DatabaseClient, DatabaseError
from notion_hook.core.exceptions import WorkflowError
from notion_hook.core.logging import get_logger
from notion_hook.workflows.base import BaseWorkflow

if TYPE_CHECKING:
    from notion_hook.models.webhook import WorkflowContext

logger = get_logger("workflows.local_db_sync")


class LocalDatabaseSyncWorkflow[T](BaseWorkflow):
    """Shared webhook workflow for local SQLite mirror sync."""

    def __init__(self, notion_client: Any, database_client: DatabaseClient) -> None:
        super().__init__(notion_client)
        self.database_client = database_client

    def matches(self, context: WorkflowContext) -> bool:
        return context.workflow_name == self.name

    async def execute(self, context: WorkflowContext) -> dict[str, Any]:
        page_id = context.page_id
        payload = context.payload

        operation = await self._detect_operation(page_id, payload)
        logger.info(f"[{self.name}] detected {operation} for page {page_id}")

        try:
            if operation == "delete":
                deleted = await self._delete(page_id)
                return {
                    "operation": "delete",
                    "page_id": page_id,
                    "success": True,
                    "deleted": deleted,
                }

            model = self._parse(page_id, payload)
            if operation == "create":
                await self._create(model)
                return {
                    "operation": "create",
                    "page_id": page_id,
                    "success": True,
                }

            updated = await self._update(model)
            return {
                "operation": "update",
                "page_id": page_id,
                "success": True,
                "updated": updated,
            }
        except DatabaseError as e:
            await self._log_failure(page_id, operation, str(e))
            raise
        except Exception as e:
            logger.error(
                f"[{self.name}] sync failed for page {page_id}: {e}",
                exc_info=True,
            )
            raise WorkflowError(f"{self.name} sync failed: {e}") from e

    async def _detect_operation(self, page_id: str, payload: dict[str, Any]) -> str:
        data = payload.get("data", {})
        properties = data.get("properties", {})

        if data.get("archived") is True or data.get("in_trash") is True:
            return "delete"

        if not properties:
            return "delete"

        existing = await self._get_existing(page_id)
        return "create" if existing is None else "update"

    async def _log_failure(self, page_id: str, operation: str, message: str) -> None:
        try:
            await self.database_client.log_failure(
                page_id=page_id,
                operation=operation,
                error_message=message,
                retry_count=self.database_client.settings.max_retries,
            )
        except DatabaseError as log_error:
            logger.warning(
                f"[{self.name}] failed to write fail_log for {page_id}: {log_error}"
            )

    @abstractmethod
    async def _get_existing(self, page_id: str) -> T | None:
        """Fetch existing local record by page id."""

    @abstractmethod
    def _parse(self, page_id: str, payload: dict[str, Any]) -> T:
        """Parse webhook payload into a domain model."""

    @abstractmethod
    async def _create(self, model: T) -> None:
        """Insert a new local record."""

    @abstractmethod
    async def _update(self, model: T) -> bool:
        """Update existing local record."""

    @abstractmethod
    async def _delete(self, page_id: str) -> bool:
        """Delete local record."""
