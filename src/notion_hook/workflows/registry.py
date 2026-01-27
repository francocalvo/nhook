from __future__ import annotations

from typing import TYPE_CHECKING

from notion_hook.core.exceptions import WorkflowNotFoundError
from notion_hook.core.logging import get_logger

if TYPE_CHECKING:
    from notion_hook.clients.notion import NotionClient
    from notion_hook.models.webhook import WorkflowContext
    from notion_hook.workflows.base import BaseWorkflow

logger = get_logger("workflows.registry")


class WorkflowRegistry:
    """Registry for workflow discovery and dispatch.

    Manages registered workflows and matches incoming webhooks
    to the appropriate workflow for execution.
    """

    def __init__(self, notion_client: NotionClient) -> None:
        """Initialize the registry with a Notion client.

        Args:
            notion_client: The Notion API client for workflows to use.
        """
        self.notion_client = notion_client
        self._workflows: list[BaseWorkflow] = []

    def register(self, workflow_class: type[BaseWorkflow]) -> None:
        """Register a workflow class.

        Args:
            workflow_class: The workflow class to register.
        """
        workflow = workflow_class(self.notion_client)
        self._workflows.append(workflow)
        logger.info(f"Registered workflow: {workflow.name}")

    def get_workflow(self, context: WorkflowContext) -> BaseWorkflow:
        """Find a workflow that matches the given context.

        Args:
            context: The webhook context to match against.

        Returns:
            The first matching workflow.

        Raises:
            WorkflowNotFoundError: If no workflow matches.
        """
        for workflow in self._workflows:
            if workflow.matches(context):
                logger.debug(f"Matched workflow: {workflow.name}")
                return workflow

        raise WorkflowNotFoundError(f"No workflow found for page {context.page_id}")

    @property
    def workflows(self) -> list[BaseWorkflow]:
        """Get all registered workflows."""
        return self._workflows.copy()
