from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status

from notion_hook.core.auth import verify_webhook_key
from notion_hook.core.database import DatabaseError
from notion_hook.core.exceptions import (
    NotionClientError,
    WorkflowError,
    WorkflowNotFoundError,
)
from notion_hook.core.logging import get_logger
from notion_hook.core.utils import get_property_ci
from notion_hook.models.webhook import DateValue, WebhookResponse, WorkflowContext

logger = get_logger("api.webhooks")

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


def get_workflow_registry() -> Any:
    """Get the workflow registry from app state.

    This is set during app startup via lifespan.
    """
    from notion_hook.app import _workflow_registry

    if _workflow_registry is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Workflow registry not initialized",
        )
    return _workflow_registry


@router.post("/notion", response_model=WebhookResponse)
async def handle_notion_webhook(
    request: Request,
    _: Annotated[str, Depends(verify_webhook_key)],
    x_calvo_workflow: Annotated[str | None, Header()] = None,
) -> WebhookResponse:
    """Handle incoming Notion webhook.

    Validates the X-Calvo-Key header, parses the payload,
    and dispatches to the appropriate workflow.

    Args:
        request: The incoming request with JSON payload.
        _: The validated webhook key (from dependency).
        x_calvo_workflow: Optional workflow name from header.

    Returns:
        Response with execution status and results.
    """
    try:
        payload: dict[str, Any] = await request.json()
    except Exception as e:
        logger.error(f"Failed to parse webhook payload: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid JSON payload",
        ) from e

    data = payload.get("data", {})
    page_id = data.get("id")
    if not page_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing 'data.id' in payload",
        )

    logger.info(f"Received webhook for page: {page_id}")
    logger.debug(f"Webhook payload: {payload}")

    date_value: DateValue | None = None
    departure_value: DateValue | None = None
    fecha_value: DateValue | None = None
    properties = data.get("properties", {})

    if date_data := get_property_ci(properties, "Date"):
        try:
            date_value = DateValue.model_validate(
                date_data.get("date") if date_data else None
            )
        except Exception as e:
            logger.warning(f"Failed to parse Date value: {e}")

    if departure_data := get_property_ci(properties, "Departure"):
        try:
            departure_value = DateValue.model_validate(
                departure_data.get("date") if departure_data else None
            )
        except Exception as e:
            logger.warning(f"Failed to parse Departure value: {e}")

    if fecha_data := get_property_ci(properties, "Fecha"):
        try:
            fecha_value = DateValue.model_validate(
                fecha_data.get("date") if fecha_data else None
            )
        except Exception as e:
            logger.warning(f"Failed to parse Fecha value: {e}")

    context = WorkflowContext(
        page_id=page_id,
        payload=payload,
        date_value=date_value,
        departure_value=departure_value,
        fecha_value=fecha_value,
        workflow_name=x_calvo_workflow,
    )

    registry = get_workflow_registry()

    try:
        workflow = registry.get_workflow(context)
        logger.info(f"Executing workflow: {workflow.name}")
        result = await workflow.execute(context)

        return WebhookResponse(
            success=True,
            message=f"Workflow '{workflow.name}' executed successfully",
            page_id=page_id,
            updated_relations=result.get("updated_relations", []),
        )

    except WorkflowNotFoundError as e:
        logger.warning(f"No workflow found: {e}")
        return WebhookResponse(
            success=False,
            message=str(e),
            page_id=page_id,
        )

    except WorkflowError as e:
        logger.error(f"Workflow execution failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Workflow error: {e}",
        ) from e

    except DatabaseError as e:
        logger.error(f"Database error: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database error: {e}",
        ) from e

    except NotionClientError as e:
        logger.error(f"Notion API error: {e}")
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Notion API error: {e}",
        ) from e
