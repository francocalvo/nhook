from __future__ import annotations

import inspect
from typing import TYPE_CHECKING

from notion_hook.core.exceptions import WorkflowNotFoundError
from notion_hook.core.logging import get_logger

if TYPE_CHECKING:
    from notion_hook.clients.notion import NotionClient
    from notion_hook.core.database import DatabaseClient
    from notion_hook.models.webhook import WorkflowContext
    from notion_hook.workflows.base import BaseWorkflow

logger = get_logger("workflows.registry")


class WorkflowRegistry:
    """Registry for workflow discovery and dispatch.

    Manages registered workflows and matches incoming webhooks
    to appropriate workflow for execution.
    """

    def __init__(
        self,
        notion_client: NotionClient,
        database_client: DatabaseClient | None = None,
    ) -> None:
        """Initialize registry with clients.

        Args:
            notion_client: The Notion API client for workflows to use.
            database_client: Optional database client for workflows to use.
        """
        self.notion_client = notion_client
        self.database_client = database_client
        self._workflows: list[BaseWorkflow] = []

    def register(self, workflow_class: type[BaseWorkflow]) -> None:
        """Register a workflow class.

        Args:
            workflow_class: The workflow class to register.
        """
        init_signature = inspect.signature(workflow_class.__init__)
        params = list(init_signature.parameters.values())
        non_self_params = [p for p in params if p.name != "self"]

        positional_count = sum(
            1
            for p in non_self_params
            if p.kind
            in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        )
        accepts_varargs = any(
            p.kind == inspect.Parameter.VAR_POSITIONAL for p in non_self_params
        )
        accepts_db = (
            "database_client" in init_signature.parameters
            or accepts_varargs
            or positional_count >= 2
        )

        if accepts_db:
            workflow = workflow_class(self.notion_client, self.database_client)
        else:
            workflow = workflow_class(self.notion_client)
        self._workflows.append(workflow)
        logger.info(f"Registered workflow: {workflow.name}")

    def get_workflow(self, context: WorkflowContext) -> BaseWorkflow:
        """Find a workflow that matches given context.

        Args:
            context: The webhook context to match against.

        Returns:
            The first matching workflow.

        Raises:
            WorkflowNotFoundError: If no workflow matches.
        """
        if context.workflow_name:
            for workflow in self._workflows:
                if workflow.name == context.workflow_name:
                    logger.debug(f"Matched workflow by name: {workflow.name}")
                    return workflow
            raise WorkflowNotFoundError(
                f"No workflow found with name '{context.workflow_name}'"
            )

        for workflow in self._workflows:
            if workflow.matches(context):
                logger.debug(f"Matched workflow: {workflow.name}")
                return workflow

        raise WorkflowNotFoundError(f"No workflow found for page {context.page_id}")

    def get_date_property_name(self, workflow_name: str) -> str | None:
        """Get the date property name for a workflow.

        Args:
            workflow_name: The workflow name to look up.

        Returns:
            The date property name, or None if workflow not found or
            has no date property.
        """
        for workflow in self._workflows:
            if workflow.name == workflow_name:
                return workflow.date_property_name
        return None

    @property
    def workflows(self) -> list[BaseWorkflow]:
        """Get all registered workflows."""
        return self._workflows.copy()
