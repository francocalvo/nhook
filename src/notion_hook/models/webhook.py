from __future__ import annotations

from datetime import date
from typing import Any

from pydantic import BaseModel, Field


class DateValue(BaseModel):
    """Notion date property value."""

    start: date
    end: date | None = None


class WebhookPayload(BaseModel):
    """Incoming webhook payload from Notion automation.

    The Notion automation sends page ID and properties that changed.
    """

    id: str = Field(..., description="The Notion page ID")
    date: DateValue | None = Field(None, alias="Date", description="The Date property")

    model_config = {"populate_by_name": True}


class WebhookResponse(BaseModel):
    """Response model for webhook endpoint."""

    success: bool
    message: str
    page_id: str | None = None
    updated_relations: list[str] = Field(default_factory=list)


class WorkflowContext(BaseModel):
    """Context passed to workflow execution."""

    page_id: str
    payload: dict[str, Any]
    date_value: DateValue | None = None

    model_config = {"arbitrary_types_allowed": True}
