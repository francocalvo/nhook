from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field

from notion_hook.core.utils import get_property_ci


class Gasto(BaseModel):
    """Represents a gastos database record."""

    page_id: str = Field(..., description="Notion page ID (sync key)")
    payment_method: str | None = Field(None, description="Payment method")
    description: str | None = Field(None, description="Description (from Expense)")
    category: str | None = Field(None, description="Category (comma-separated)")
    amount: float | None = Field(None, description="Amount")
    date: str | None = Field(None, description="Date in YYYY-MM-DD format")
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

        def _first_property(*names: str) -> dict[str, Any] | None:
            for name in names:
                value = get_property_ci(properties, name)
                if isinstance(value, dict):
                    return value
            return None

        def _extract_select_name(prop: dict[str, Any]) -> str | None:
            select = prop.get("select") or {}
            if isinstance(select, dict) and isinstance(select.get("name"), str):
                return select["name"]
            return None

        def _extract_multi_select(prop: dict[str, Any]) -> str | None:
            multi = prop.get("multi_select") or []
            if isinstance(multi, list):
                names_out: list[str] = []
                for item in multi:
                    if isinstance(item, dict) and isinstance(item.get("name"), str):
                        names_out.append(item["name"])
                return ", ".join(names_out) if names_out else None
            return None

        def _extract_text(prop: dict[str, Any]) -> str | None:
            parts: list[str] = []
            for key in ("title", "rich_text"):
                items = prop.get(key) or []
                if not isinstance(items, list):
                    continue
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    if isinstance(item.get("plain_text"), str):
                        parts.append(item["plain_text"])
                        continue
                    text = item.get("text") or {}
                    if isinstance(text, dict) and isinstance(text.get("content"), str):
                        parts.append(text["content"])
                if parts:
                    break
            return "".join(parts) if parts else None

        def _extract_number(prop: dict[str, Any]) -> float | None:
            number = prop.get("number")
            if isinstance(number, bool):
                return None
            if isinstance(number, (int, float)):
                return float(number)
            return None

        def _extract_date_start(prop: dict[str, Any]) -> str | None:
            date_obj = prop.get("date")
            if isinstance(date_obj, dict) and isinstance(date_obj.get("start"), str):
                start = date_obj["start"]
                if "T" in start:
                    return start.split("T", 1)[0]
                return start
            return None

        payment_method = None
        if pm_prop := _first_property("Payment Method", "payment_method"):
            payment_method = _extract_select_name(pm_prop)

        description = None
        if desc_prop := _first_property("Expense", "expense"):
            description = _extract_text(desc_prop)

        category = None
        if cat_prop := _first_property("Category", "category"):
            category = _extract_multi_select(cat_prop)

        amount = None
        if amount_prop := _first_property("Amount", "amount"):
            amount = _extract_number(amount_prop)

        date = None
        if date_prop := _first_property("Date", "date"):
            date = _extract_date_start(date_prop)

        return cls(
            page_id=page_id,
            payment_method=payment_method,
            description=description,
            category=category,
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
