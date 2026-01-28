from __future__ import annotations

from datetime import date, timedelta
from typing import TYPE_CHECKING, Any

from notion_hook.core.exceptions import WorkflowError
from notion_hook.core.logging import get_logger
from notion_hook.workflows.base import BaseWorkflow

if TYPE_CHECKING:
    from notion_hook.models.webhook import WorkflowContext

logger = get_logger("workflows.cronograma_sync")


class CronogramaSyncWorkflow(BaseWorkflow):
    """Workflow to sync Cronograma relation when Date changes in Gastos.

    When a Gastos entry's Date property changes:
    - If Date is empty: clear the Cronograma relation
    - If Date is a single date: find matching Cronograma entry
    - If Date is a range: find all Cronograma entries in the range
    """

    name = "gastos-cronograma"
    description = "Sync Cronograma relation based on Date changes"

    def matches(self, context: WorkflowContext) -> bool:
        """Match if the workflow name matches this workflow.

        Args:
            context: The webhook context.

        Returns:
            True if this workflow should handle the webhook.
        """
        return context.workflow_name == self.name

    async def execute(self, context: WorkflowContext) -> dict[str, Any]:
        """Execute the Cronograma sync workflow.

        Args:
            context: The webhook context with page ID and date value.

        Returns:
            Dictionary with updated_relations list.

        Raises:
            WorkflowError: If sync fails.
        """
        page_id = context.page_id
        date_value = context.date_value

        logger.info(f"Executing Cronograma sync for page {page_id}")

        if date_value is None:
            logger.info(f"Date cleared for {page_id}, removing Cronograma relations")
            logger.debug(f"Calling update_gastos_cronograma_relation({page_id}, [])")
            await self.notion_client.update_gastos_cronograma_relation(page_id, [])
            logger.info(f"Successfully cleared Cronograma relations for {page_id}")
            return {"updated_relations": []}

        dates = self._expand_date_range(date_value.start, date_value.end)
        logger.debug(f"Expanded dates: {[d.isoformat() for d in dates]}")

        try:
            logger.info(
                f"Querying Cronograma database for dates: "
                f"{[d.isoformat() for d in dates]}"
            )
            cronograma_entries = await self.notion_client.find_cronograma_by_dates(
                dates
            )
            logger.debug(f"Raw Cronograma entries: {cronograma_entries}")

            cronograma_ids = [entry["id"] for entry in cronograma_entries]
            logger.info(
                f"Found {len(cronograma_ids)} Cronograma entries to link: "
                f"{cronograma_ids}"
            )

            logger.debug(
                f"Calling update_gastos_cronograma_relation("
                f"{page_id}, {cronograma_ids})"
            )
            await self.notion_client.update_gastos_cronograma_relation(
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
            raise WorkflowError(f"Cronograma sync failed: {e}") from e

    def _expand_date_range(self, start: date, end: date | None) -> list[date]:
        """Expand a date range to individual dates.

        Args:
            start: Start date.
            end: Optional end date. If None, returns just the start date.

        Returns:
            List of all dates in the range (inclusive).
        """
        if end is None:
            return [start]

        dates = []
        current = start
        while current <= end:
            dates.append(current)
            current += timedelta(days=1)

        return dates
