from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


class DateValue(BaseModel):
    """Notion date property value (the nested date field).

    Accepts both date (YYYY-MM-DD) and datetime (YYYY-MM-DDTHH:MM:SSZ) strings.
    DateTime values are normalized to date objects for matching.
    """

    start: date
    end: date | None = None

    @field_validator("start", "end", mode="before")
    @classmethod
    def parse_date_string(cls, v: Any) -> date | None:
        """Parse date or datetime string to date object.

        Args:
            v: Value to parse (date, datetime, or string).

        Returns:
            Date object or None.

        Raises:
            ValueError: If parsing fails.
        """
        if v is None:
            return None

        if isinstance(v, datetime):
            return v.date()

        if isinstance(v, date):
            return v

        if isinstance(v, str):
            # Try parsing as datetime first (handles ISO-8601 with time)
            try:
                dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
                return dt.date()
            except ValueError:
                pass

            # Try parsing as date only
            try:
                return date.fromisoformat(v)
            except ValueError:
                raise ValueError(f"Unable to parse date string: {v}")

        raise ValueError(f"Invalid date value type: {type(v)}")


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
    date_value: DateValue | None = None  # Single date field used by all workflows
    date_property_present: bool = False
    workflow_name: str | None = None

    model_config = {"arbitrary_types_allowed": True}
