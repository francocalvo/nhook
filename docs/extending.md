# Extending NHook

## Adding a New Workflow

Workflows are the core business logic units. Each workflow handles a specific type of webhook event.

### Step 1: Create the Workflow Class

Create a new file in `src/notion_hook/workflows/`:

```python
# src/notion_hook/workflows/my_workflow.py
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from notion_hook.core.exceptions import WorkflowError
from notion_hook.core.logging import get_logger
from notion_hook.workflows.base import BaseWorkflow

if TYPE_CHECKING:
    from notion_hook.models.webhook import WorkflowContext

logger = get_logger("workflows.my_workflow")


class MyWorkflow(BaseWorkflow):
    """Description of what this workflow does."""

    name = "my_workflow"
    description = "Handles X when Y changes"

    def matches(self, context: WorkflowContext) -> bool:
        """Check if this workflow should handle the webhook.

        Return True if the payload contains the properties you care about.
        """
        # Example: match when "Status" property is in payload
        return "Status" in context.payload

    async def execute(self, context: WorkflowContext) -> dict[str, Any]:
        """Execute the workflow logic.

        Args:
            context: Contains page_id, payload, and parsed date_value.

        Returns:
            Dict with results (e.g., {"updated_relations": [...]})
        """
        logger.info(f"Executing {self.name} for page {context.page_id}")

        try:
            # Your business logic here
            # Use self.notion_client for API calls

            # Example: get the page
            page = await self.notion_client.get_page(context.page_id)

            # Example: update properties
            await self.notion_client.update_page(
                context.page_id,
                {"SomeProperty": {"rich_text": [{"text": {"content": "value"}}]}}
            )

            return {"success": True}

        except Exception as e:
            logger.error(f"Workflow failed: {e}")
            raise WorkflowError(f"{self.name} failed: {e}") from e
```

### Step 2: Register the Workflow

Add your workflow to the lifespan in `app.py`:

```python
# src/notion_hook/app.py
from notion_hook.workflows.my_workflow import MyWorkflow

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # ... existing setup ...

    _workflow_registry = WorkflowRegistry(_notion_client)
    _workflow_registry.register(CronogramaSyncWorkflow)
    _workflow_registry.register(MyWorkflow)  # Add your workflow

    # ...
```

### Step 3: Write Tests

Create `tests/test_my_workflow.py`:

```python
from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock

import pytest

from notion_hook.models.webhook import WorkflowContext
from notion_hook.workflows.my_workflow import MyWorkflow


class TestMyWorkflow:
    def test_matches_with_status(self, mock_notion_client: AsyncMock) -> None:
        workflow = MyWorkflow(mock_notion_client)
        context = WorkflowContext(
            page_id="test-id",
            payload={"id": "test-id", "Status": "Done"},
        )
        assert workflow.matches(context) is True

    def test_does_not_match_without_status(self, mock_notion_client: AsyncMock) -> None:
        workflow = MyWorkflow(mock_notion_client)
        context = WorkflowContext(
            page_id="test-id",
            payload={"id": "test-id", "OtherProperty": "value"},
        )
        assert workflow.matches(context) is False

    @pytest.mark.asyncio
    async def test_execute(self, mock_notion_client: AsyncMock) -> None:
        workflow = MyWorkflow(mock_notion_client)
        context = WorkflowContext(
            page_id="test-id",
            payload={"id": "test-id", "Status": "Done"},
        )

        result = await workflow.execute(context)

        assert result["success"] is True
        mock_notion_client.update_page.assert_called_once()
```

## Adding Notion Client Methods

If you need new Notion API operations, add methods to `NotionClient`:

```python
# src/notion_hook/clients/notion.py

class NotionClient:
    # ... existing methods ...

    async def get_database(self, database_id: str) -> dict[str, Any]:
        """Get database schema."""
        response = await self.client.get(f"/databases/{database_id}")
        if response.status_code != 200:
            raise NotionClientError(
                f"Failed to get database: {response.text}",
                status_code=response.status_code,
            )
        return response.json()

    async def create_page(
        self,
        parent_id: str,
        properties: dict[str, Any],
        is_database: bool = True,
    ) -> dict[str, Any]:
        """Create a new page."""
        parent_key = "database_id" if is_database else "page_id"
        response = await self.client.post(
            "/pages",
            json={
                "parent": {parent_key: parent_id},
                "properties": properties,
            },
        )
        if response.status_code != 200:
            raise NotionClientError(
                f"Failed to create page: {response.text}",
                status_code=response.status_code,
            )
        return response.json()
```

## Adding New Webhook Properties

To handle additional properties from Notion:

### Step 1: Update Models

Add new models to `src/notion_hook/models/webhook.py`:

```python
class StatusValue(BaseModel):
    """Notion status property value."""
    name: str
    color: str | None = None


class WebhookPayload(BaseModel):
    id: str
    date: DateValue | None = Field(None, alias="Date")
    status: StatusValue | None = Field(None, alias="Status")  # Add new field

    model_config = {"populate_by_name": True}
```

### Step 2: Update WebhookContext

Add parsed values to context if needed:

```python
class WorkflowContext(BaseModel):
    page_id: str
    payload: dict[str, Any]
    date_value: DateValue | None = None
    status_value: StatusValue | None = None  # Add new field
```

### Step 3: Update Webhook Handler

Parse the new property in `api/webhooks.py`:

```python
@router.post("/notion")
async def handle_notion_webhook(...):
    # ... existing code ...

    status_value: StatusValue | None = None
    if status_data := payload.get("Status"):
        try:
            status_value = StatusValue.model_validate(status_data)
        except Exception as e:
            logger.warning(f"Failed to parse Status: {e}")

    context = WorkflowContext(
        page_id=page_id,
        payload=payload,
        date_value=date_value,
        status_value=status_value,
    )
```

## Adding Configuration Options

Add new settings to `config.py`:

```python
class Settings(BaseSettings):
    # ... existing settings ...

    # New database
    my_database_id: str = "default-id-here"

    # Feature flags
    enable_my_workflow: bool = True

    # Timeouts
    notion_timeout: float = 30.0
```

Then use in your code:

```python
settings = get_settings()
if settings.enable_my_workflow:
    registry.register(MyWorkflow)
```

## Workflow Selection Priority

Workflows are matched in registration order. The first workflow where `matches()` returns `True` is executed.

To control priority, register workflows in the desired order:

```python
# Higher priority workflows first
_workflow_registry.register(HighPriorityWorkflow)
_workflow_registry.register(CronogramaSyncWorkflow)
_workflow_registry.register(CatchAllWorkflow)  # Matches everything
```

## Best Practices

1. **Single Responsibility**: Each workflow should handle one type of sync/update
2. **Idempotent Operations**: Workflows may be called multiple times; ensure operations are safe to repeat
3. **Error Handling**: Wrap API calls in try/except and raise `WorkflowError`
4. **Logging**: Use `get_logger(__name__)` for consistent logging
5. **Testing**: Write tests for both `matches()` and `execute()` methods
6. **Type Hints**: Use type annotations on all public methods
