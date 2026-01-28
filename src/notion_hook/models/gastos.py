from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field


class Gasto(BaseModel):
    """Represents a gastos database record."""

    page_id: str = Field(..., description="Notion page ID (sync key)")
    payment_method: str | None = Field(None, description="Payment method")
    description: str | None = Field(None, description="Description")
    amount: float | None = Field(None, description="Amount")
    date: str | None = Field(None, description="Date in ISO format")
    created_at: str = Field(..., description="Creation timestamp (ISO format)")
    updated_at: str = Field(..., description="Last update timestamp (ISO format)")

    @classmethod
    def from_notion_properties(
        cls,
        page_id: str,
        properties: dict[str, Any],
        created_time: str,
        last_edited_time: str,
    ) -> Gasto:
        """Create a Gasto from Notion properties.

        Args:
            page_id: The Notion page ID.
            properties: Notion properties dict.
            created_time: Page creation time from Notion.
            last_edited_time: Page last edited time from Notion.

        Returns:
            Gasto instance.
        """
        payment_method = None
        if pm_data := properties.get("payment_method") or properties.get(
            "Payment Method"
        ):
            payment_method = pm_data.get("select", {}).get("name")

        description = None
        if desc_data := properties.get("description") or properties.get("Description"):
            description = (
                desc_data.get("title", [{"text": {}}])[0].get("text", {}).get("content")
            )

        amount = None
        if amount_data := properties.get("amount") or properties.get("Amount"):
            amount = amount_data.get("number")

        date = None
        if date_data := properties.get("date") or properties.get("Date"):
            if date_obj := date_data.get("date"):
                date = date_obj.get("start")

        return cls(
            page_id=page_id,
            payment_method=payment_method,
            description=description,
            amount=amount,
            date=date,
            created_at=created_time,
            updated_at=last_edited_time,
        )


class FailLogEntry(BaseModel):
    """Represents a failure log entry."""

    id: int | None = Field(None, description="Auto-incremented ID")
    page_id: str = Field(..., description="Related Notion page ID")
    operation: str = Field(..., description="Operation type (create/update/delete)")
    error_message: str = Field(..., description="Error message")
    retry_count: int = Field(0, description="Number of retry attempts")
    created_at: str = Field(..., description="Creation timestamp (ISO format)")

    @classmethod
    def create(
        cls,
        page_id: str,
        operation: str,
        error_message: str,
        retry_count: int = 0,
    ) -> FailLogEntry:
        """Create a FailLogEntry.

        Args:
            page_id: The Notion page ID.
            operation: Operation type.
            error_message: Error message.
            retry_count: Retry count.

        Returns:
            FailLogEntry instance.
        """
        return cls(
            id=None,
            page_id=page_id,
            operation=operation,
            error_message=error_message,
            retry_count=retry_count,
            created_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        )
