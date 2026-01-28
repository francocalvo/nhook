from __future__ import annotations

from typing import TYPE_CHECKING, Any

from notion_hook.core.exceptions import WorkflowError
from notion_hook.core.logging import get_logger
from notion_hook.workflows.base import BaseWorkflow

if TYPE_CHECKING:
    from notion_hook.models.webhook import WorkflowContext

logger = get_logger("workflows.pasajes_sync")


class PasajesSyncWorkflow(BaseWorkflow):
    """Workflow to sync Cronograma relation when departure changes in Pasajes.

    When a Pasajes entry's departure property changes:
    - If departure is empty: clear the Cronograma relation
    - If departure is set: find matching Cronograma entry by date
    """

    name = "pasajes-cronograma"
    description = "Sync Cronograma relation based on departure changes"
    date_property_name = "Departure"  # NEW

    def matches(self, context: WorkflowContext) -> bool:
        """Match if the workflow name matches this workflow.

        Args:
            context: The webhook context.

        Returns:
            True if this workflow should handle the webhook.
        """
        return context.workflow_name == self.name

    async def execute(self, context: WorkflowContext) -> dict[str, Any]:
        """Execute the Pasajes sync workflow.

        Args:
            context: The webhook context with page ID and date value.

        Returns:
            Dictionary with updated_relations list.

        Raises:
            WorkflowError: If sync fails.
        """
        page_id = context.page_id
        date_value = context.date_value  # Changed from departure_value

        logger.info(f"Executing Pasajes sync for page {page_id}")

        if date_value is None:
            logger.info(
                f"departure cleared for {page_id}, removing Cronograma relations"
            )
            logger.debug(f"Calling update_pasajes_cronograma_relation({page_id}, [])")
            await self.notion_client.update_pasajes_cronograma_relation(page_id, [])
            logger.info(f"Successfully cleared Cronograma relations for {page_id}")
            return {"updated_relations": []}

        departure_date = date_value.start
        logger.debug(f"Departure date: {departure_date.isoformat()}")

        try:
            logger.info(
                f"Querying Cronograma database for date: {departure_date.isoformat()}"
            )
            cronograma_entries = await self.notion_client.find_cronograma_by_dates(
                [departure_date]
            )
            logger.debug(f"Raw Cronograma entries: {cronograma_entries}")

            cronograma_ids = [entry["id"] for entry in cronograma_entries]
            logger.info(
                f"Found {len(cronograma_ids)} Cronograma entries to link: "
                f"{cronograma_ids}"
            )

            logger.debug(
                f"Calling update_pasajes_cronograma_relation("
                f"{page_id}, {cronograma_ids})"
            )
            await self.notion_client.update_pasajes_cronograma_relation(
                page_id, cronograma_ids
            )
            logger.info(
                f"Successfully linked {len(cronograma_ids)} Cronograma entries "
                f"to page {page_id}"
            )

            return {"updated_relations": cronograma_ids}

        except Exception as e:
            logger.error(
                f"Failed to sync Cronograma for {page_id}: {e}",
                exc_info=True,
            )
            raise WorkflowError(f"Pasajes sync failed: {e}") from e
