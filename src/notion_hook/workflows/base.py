from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from notion_hook.clients.notion import NotionClient
    from notion_hook.models.webhook import WorkflowContext


class BaseWorkflow(ABC):
    """Abstract base class for webhook workflows.

    Each workflow handles a specific type of webhook event and performs
    the necessary operations (e.g., syncing relations, updating properties).
    """

    name: str = "base"
    description: str = "Base workflow"

    def __init__(self, notion_client: NotionClient) -> None:
        """Initialize workflow with Notion client.

        Args:
            notion_client: The Notion API client instance.
        """
        self.notion_client = notion_client

    @abstractmethod
    def matches(self, context: WorkflowContext) -> bool:
        """Check if this workflow should handle the given webhook context.

        Args:
            context: The webhook context with page ID and payload.

        Returns:
            True if this workflow should handle the webhook.
        """

    @abstractmethod
    async def execute(self, context: WorkflowContext) -> dict[str, Any]:
        """Execute the workflow logic.

        Args:
            context: The webhook context with page ID and payload.

        Returns:
            A dictionary with execution results.

        Raises:
            WorkflowError: If the workflow execution fails.
        """
