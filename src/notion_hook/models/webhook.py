from __future__ import annotations

from datetime import date
from typing import Any

from pydantic import BaseModel, Field


class DateValue(BaseModel):
    """Notion date property value (the nested date field)."""

    start: date
    end: date | None = None


class NotionDateProperty(BaseModel):
    """Notion Date property structure."""

    id: str
    type: str
    date: DateValue | None = None


class NotionAutomationSource(BaseModel):
    """Source information from Notion automation."""

    type: str
    automation_id: str
    action_id: str
    event_id: str
    attempt: int


class NotionPageData(BaseModel):
    """Page data object from Notion webhook."""

    object: str
    id: str
    created_time: str
    last_edited_time: str
    properties: dict[str, Any]


class NotionWebhookPayload(BaseModel):
    """Incoming webhook payload from Notion automation.

    Contains source metadata and page data with nested properties.
    """

    source: NotionAutomationSource
    data: NotionPageData


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
    departure_value: DateValue | None = None
    workflow_name: str | None = None

    model_config = {"arbitrary_types_allowed": True}
