from __future__ import annotations

from typing import TYPE_CHECKING, Any

from notion_hook.core.exceptions import WorkflowError
from notion_hook.core.logging import get_logger
from notion_hook.workflows.base import BaseWorkflow

if TYPE_CHECKING:
    from notion_hook.models.webhook import WorkflowContext

logger = get_logger("workflows.atracciones_sync")


class AtraccionesSyncWorkflow(BaseWorkflow):
    """Workflow to sync Cronograma relation when Fecha changes in Atracciones.

    When an Atracciones entry's Fecha property changes:
    - If Fecha is empty: clear the Cronograma relation
    - If Fecha is set: find matching Cronograma entry by date

    Note: Datetime values are normalized to date at parse time,
    so only the date portion is used for matching.
    """

    name = "atracciones-cronograma"
    description = "Sync Cronograma relation based on Fecha changes"
    date_property_name = "Fecha"

    def matches(self, context: WorkflowContext) -> bool:
        """Match if the workflow name matches this workflow.

        Args:
            context: The webhook context.

        Returns:
            True if this workflow should handle the webhook.
        """
        return context.workflow_name == self.name

    async def execute(self, context: WorkflowContext) -> dict[str, Any]:
        """Execute the Atracciones sync workflow.

        Args:
            context: The webhook context with page ID and date value.

        Returns:
            Dictionary with updated_relations list.

        Raises:
            WorkflowError: If sync fails.
        """
        page_id = context.page_id
        # Alias for domain-specific clarity
        fecha_value = context.date_value

        logger.info(f"Executing Atracciones sync for page {page_id}")

        if fecha_value is None:
            if not context.date_property_present:
                logger.info(
                    f"No Fecha property in webhook payload for {page_id}; skipping sync"
                )
                return {"updated_relations": []}
            logger.info(f"Fecha cleared for {page_id}, removing Cronograma relations")
            logger.debug(
                f"Calling update_atracciones_cronograma_relation({page_id}, [])"
            )
            await self.notion_client.update_atracciones_cronograma_relation(page_id, [])
            logger.info(f"Successfully cleared Cronograma relations for {page_id}")
            return {"updated_relations": []}

        fecha_date = fecha_value.start
        logger.debug(f"Fecha date: {fecha_date.isoformat()}")

        try:
            logger.info(
                f"Querying Cronograma database for date: {fecha_date.isoformat()}"
            )
            cronograma_entries = await self.notion_client.find_cronograma_by_dates(
                [fecha_date]
            )
            logger.debug(f"Raw Cronograma entries: {cronograma_entries}")

            cronograma_ids = [entry["id"] for entry in cronograma_entries]

            if not cronograma_ids:
                logger.info(f"No Cronograma entries found for {page_id}")
                # Clear relation to ensure consistency
                await self.notion_client.update_atracciones_cronograma_relation(
                    page_id, []
                )
                return {"updated_relations": []}

            logger.info(
                f"Found {len(cronograma_ids)} Cronograma entries to link: "
                f"{cronograma_ids}"
            )

            logger.debug(
                f"Calling update_atracciones_cronograma_relation("
                f"{page_id}, {cronograma_ids})"
            )
            await self.notion_client.update_atracciones_cronograma_relation(
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
            raise WorkflowError(f"Atracciones sync failed: {e}") from e
