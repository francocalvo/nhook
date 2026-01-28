# AtraccionesSyncWorkflow Implementation Review

**Date**: 2026-01-28 **Feature**: AtraccionesSyncWorkflow - Sync Cronograma
relation when Fecha changes in Atracciones database

## Remaining Issues (Non-Critical)

### 1. Documentation Clarity in Workflow Docstring

**Severity**: 🟡 Important **Location**:
`src/notion_hook/workflows/atracciones_sync.py`, line 11

**Issue**: The docstring mentions "Fecha can be a datetime (with time). We need
to extract just the date portion for matching", but with the `DateValue` model
update, time is already dropped at parse time via the field_validator. The
workflow itself doesn't "ignore time" - it just uses the parsed date.

**Impact**: Minor documentation inaccuracy, but functionality is correct.

**Suggested Fix**: Update docstring to reflect current behavior:

```python
"""Workflow to sync Cronograma relation when Fecha changes in Atracciones.

When an Atracciones entry's Fecha property changes:
- If Fecha is empty: clear the Cronograma relation
- If Fecha is set: find matching Cronograma entry by date

Note: Datetime values are normalized to date at parse time,
so only the date portion is used for matching.
"""
```

---

### 3. No-Op Notion Updates

**Severity**: 🟢 Suggestion **Location**:
`src/notion_hook/workflows/atracciones_sync.py`, lines 75-85

**Issue**: The workflow always calls `update_atracciones_cronograma_relation()`,
even when `cronograma_ids` is empty (no matches found). This creates unnecessary
Notion API calls when there are no entries to link.

**Impact**:

- Extra Notion API calls (one per webhook with no matches)
- Unnecessary logging noise
- Minor performance impact

**Current Behavior**:

```python
cronograma_ids = [entry["id"] for entry in cronograma_entries]
logger.info(f"Found {len(cronograma_ids)} Cronograma entries to link: {cronograma_ids}")

# Always calls update, even if empty list
await self.notion_client.update_atracciones_cronograma_relation(
    page_id, cronograma_ids
)
```

**Suggested Fix**: Short-circuit when no matches found:

```python
cronograma_ids = [entry["id"] for entry in cronograma_entries]

if not cronograma_ids:
    logger.info(f"No Cronograma entries found for {page_id}")
    # Optionally check if relation needs clearing
    # For now, clear to ensure consistency
    await self.notion_client.update_atracciones_cronograma_relation(
        page_id, []
    )
    return {"updated_relations": []}

logger.info(f"Found {len(cronograma_ids)} Cronograma entries to link: {cronograma_ids}")
await self.notion_client.update_atracciones_cronograma_relation(
    page_id, cronograma_ids
)
```

**Alternative**: Keep as-is for consistency with existing workflows (they all
behave the same way).

---

### 4. Code Duplication in Test Fixture Helper

**Severity**: 🟢 Suggestion **Location**: `tests/conftest.py`, lines 180-248

**Issue**: `make_atracciones_webhook_payload()` duplicates ~90% of
`make_notion_webhook_payload()`. The only difference is the property name
("Fecha" vs "Date").

**Impact**: Code maintenance burden, risk of inconsistencies.

**Current Code**: Both functions have identical payload structure, source data,
page metadata.

**Suggested Fix**: Refactor into one helper function:

```python
def make_notion_webhook_payload(
    page_id: str = "test-page-id",
    date_start: str | None = "2026-03-14",
    date_end: str | None = None,
    property_name: str = "Date",  # New parameter
    extra_properties: dict[str, Any] | None = None,
) -> dict:
    """Create a realistic Notion webhook payload.

    Args:
        page_id: The Notion page ID.
        date_start: Start date string in YYYY-MM-DD format, or None.
        date_end: End date string in YYYY-MM-DD format, or None.
        property_name: Name of the date property (e.g., "Date", "Fecha", "Departure").
        extra_properties: Additional properties to include.

    Returns:
        Dictionary representing Notion's webhook payload structure.
    """
    # ... existing payload structure ...

    if extra_properties:
        payload["data"]["properties"].update(extra_properties)

    if date_start is None and date_end is None:
        payload["data"]["properties"][property_name] = {
            "id": f"{property_name.lower()}-property-id",
            "type": "date",
            "date": None,
        }
    elif date_start is not None:
        payload["data"]["properties"][property_name] = {
            "id": f"{property_name.lower()}-property-id",
            "type": "date",
            "date": {"start": date_start, "end": date_end, "time_zone": None},
        }

    return payload

# Convenience wrappers for backward compatibility
def make_atracciones_webhook_payload(
    page_id: str = "test-page-id",
    fecha_start: str | None = "2026-03-14",
    fecha_end: str | None = None,
    extra_properties: dict[str, Any] | None = None,
) -> dict:
    return make_notion_webhook_payload(
        page_id=page_id,
        date_start=fecha_start,
        date_end=fecha_end,
        property_name="Fecha",
        extra_properties=extra_properties,
    )
```
