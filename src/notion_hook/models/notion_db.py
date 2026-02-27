from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from notion_hook.core.utils import _extract_relation_id, get_property_ci


def _extract_text(prop: dict[str, Any] | None) -> str | None:
    if not prop or not isinstance(prop, dict):
        return None

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


def _extract_date_start(prop: dict[str, Any] | None) -> str | None:
    if not prop or not isinstance(prop, dict):
        return None

    date_obj = prop.get("date")
    if isinstance(date_obj, dict) and isinstance(date_obj.get("start"), str):
        start = date_obj["start"]
        if "T" in start:
            return start.split("T", 1)[0]
        return start

    text_value = _extract_text(prop)
    if isinstance(text_value, str) and text_value:
        return text_value
    return None


def _pick_property(properties: dict[str, Any], *names: str) -> dict[str, Any] | None:
    for name in names:
        value = get_property_ci(properties, name)
        if isinstance(value, dict):
            return value
    return None


class Ciudad(BaseModel):
    page_id: str = Field(..., description="Notion page ID")
    name: str | None = Field(None, description="City name")
    created_at: str = Field(..., description="Creation timestamp (ISO format)")
    updated_at: str = Field(..., description="Last update timestamp (ISO format)")

    @classmethod
    def from_notion_page(cls, page: dict[str, Any]) -> Ciudad:
        properties = page.get("properties", {})
        name_prop = _pick_property(properties, "Name", "Nombre", "Ciudad")
        return cls(
            page_id=page["id"],
            name=_extract_text(name_prop),
            created_at=page.get("created_time", ""),
            updated_at=page.get("last_edited_time", ""),
        )


class Cronograma(BaseModel):
    page_id: str = Field(..., description="Notion page ID")
    day: str | None = Field(None, description="Day value (YYYY-MM-DD)")
    ciudad_page_id: str | None = Field(None, description="Related ciudad page ID")
    created_at: str = Field(..., description="Creation timestamp (ISO format)")
    updated_at: str = Field(..., description="Last update timestamp (ISO format)")

    @classmethod
    def from_notion_page(cls, page: dict[str, Any]) -> Cronograma:
        properties = page.get("properties", {})
        day_prop = _pick_property(properties, "Día", "Dia", "Date")
        ciudad_prop = _pick_property(properties, "Ciudad", "Ciudades")
        return cls(
            page_id=page["id"],
            day=_extract_date_start(day_prop),
            ciudad_page_id=_extract_relation_id(ciudad_prop),
            created_at=page.get("created_time", ""),
            updated_at=page.get("last_edited_time", ""),
        )


class Pasaje(BaseModel):
    page_id: str = Field(..., description="Notion page ID")
    departure: str | None = Field(None, description="Departure date (YYYY-MM-DD)")
    cronograma_page_id: str | None = Field(
        None, description="Related cronograma page ID"
    )
    ciudad_page_id: str | None = Field(None, description="Related ciudad page ID")
    created_at: str = Field(..., description="Creation timestamp (ISO format)")
    updated_at: str = Field(..., description="Last update timestamp (ISO format)")

    @classmethod
    def from_notion_page(cls, page: dict[str, Any]) -> Pasaje:
        properties = page.get("properties", {})
        departure_prop = _pick_property(properties, "Departure", "departure")
        cronograma_prop = _pick_property(properties, "Cronograma")
        ciudad_prop = _pick_property(properties, "Ciudad", "Ciudades")
        return cls(
            page_id=page["id"],
            departure=_extract_date_start(departure_prop),
            cronograma_page_id=_extract_relation_id(cronograma_prop),
            ciudad_page_id=_extract_relation_id(ciudad_prop),
            created_at=page.get("created_time", ""),
            updated_at=page.get("last_edited_time", ""),
        )


class Atraccion(BaseModel):
    page_id: str = Field(..., description="Notion page ID")
    name: str | None = Field(None, description="Attraction name")
    fecha: str | None = Field(None, description="Attraction date (YYYY-MM-DD)")
    cronograma_page_id: str | None = Field(
        None, description="Related cronograma page ID"
    )
    ciudad_page_id: str | None = Field(None, description="Related ciudad page ID")
    created_at: str = Field(..., description="Creation timestamp (ISO format)")
    updated_at: str = Field(..., description="Last update timestamp (ISO format)")

    @classmethod
    def from_notion_page(cls, page: dict[str, Any]) -> Atraccion:
        properties = page.get("properties", {})
        name_prop = _pick_property(
            properties, "Name", "Nombre", "Atracción", "Atraccion"
        )
        fecha_prop = _pick_property(properties, "Fecha", "Date")
        cronograma_prop = _pick_property(properties, "Cronograma")
        ciudad_prop = _pick_property(properties, "Ciudad", "Ciudades")
        return cls(
            page_id=page["id"],
            name=_extract_text(name_prop),
            fecha=_extract_date_start(fecha_prop),
            cronograma_page_id=_extract_relation_id(cronograma_prop),
            ciudad_page_id=_extract_relation_id(ciudad_prop),
            created_at=page.get("created_time", ""),
            updated_at=page.get("last_edited_time", ""),
        )
