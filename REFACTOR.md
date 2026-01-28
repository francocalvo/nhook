# Refactor Plan: Abstracted Date Value Architecture

## Problem Statement

The current design has separate date value fields in `WorkflowContext`:
- `date_value` for Gastos (uses `Date` property)
- `departure_value` for Pasajes (uses `Departure` property)
- Would need `fecha_value` for Atracciones (uses `Fecha` property)

This doesn't scale well - each new workflow with a different date property name requires:
1. A new field in `WorkflowContext`
2. New parsing logic in `webhooks.py`
3. Workflow-specific field access

## Proposed Solution

**Single `date_value` field** that gets populated based on workflow configuration.

Each workflow class declares which date property it needs (if any). The webhook handler uses this configuration to extract the appropriate date value into a single `date_value` field in `WorkflowContext`.

```
┌─────────────────────────────────────────────────────────────────┐
│                     Webhook Request                              │
│  Header: X-Calvo-Workflow: atracciones-cronograma               │
│  Body: { data: { properties: { Fecha: { date: {...} } } } }     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Webhook Handler                                │
│  1. Get workflow_name from header                               │
│  2. Look up workflow's date_property_name from registry         │
│  3. Extract date from that property → date_value                │
│  4. Create WorkflowContext(date_value=...)                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Workflow.execute(context)                      │
│  Uses context.date_value (same for all workflows)               │
└─────────────────────────────────────────────────────────────────┘
```

---

## Files to Modify

### 1. Modify: `src/notion_hook/workflows/base.py`

Add `date_property_name: str | None = None` class attribute:

```python
class BaseWorkflow(ABC):
    name: str = "base"
    description: str = "Base workflow"
    date_property_name: str | None = None  # NEW: Property name to extract date from
```

---

### 2. Modify: `src/notion_hook/workflows/cronograma_sync.py`

Add `date_property_name = "Date"`:

```python
class CronogramaSyncWorkflow(BaseWorkflow):
    name = "gastos-cronograma"
    description = "Sync Cronograma relation based on Date changes"
    date_property_name = "Date"  # NEW
```

---

### 3. Modify: `src/notion_hook/workflows/pasajes_sync.py`

- Add `date_property_name = "Departure"`
- Change `context.departure_value` → `context.date_value`

```python
class PasajesSyncWorkflow(BaseWorkflow):
    name = "pasajes-cronograma"
    description = "Sync Cronograma relation based on departure changes"
    date_property_name = "Departure"  # NEW
    
    async def execute(self, context: WorkflowContext) -> dict[str, Any]:
        # Change: departure_value → date_value
        date_value = context.date_value  # Was: context.departure_value
        ...
```

---

### 4. Modify: `src/notion_hook/workflows/atracciones_sync.py`

Update to use the new pattern:

```python
class AtraccionesSyncWorkflow(BaseWorkflow):
    name = "atracciones-cronograma"
    description = "Sync Cronograma relation based on Fecha changes"
    date_property_name = "Fecha"  # NEW
    
    async def execute(self, context: WorkflowContext) -> dict[str, Any]:
        date_value = context.date_value  # Uses unified field
        ...
```

---

### 5. Modify: `src/notion_hook/models/webhook.py`

Remove `departure_value` field (consolidate into `date_value`):

```python
class WorkflowContext(BaseModel):
    page_id: str
    payload: dict[str, Any]
    date_value: DateValue | None = None  # KEEP: Now used by all workflows
    # REMOVE: departure_value: DateValue | None = None
    workflow_name: str | None = None
```

---

### 6. Modify: `src/notion_hook/workflows/registry.py`

Add method to get workflow's date property name by workflow name:

```python
def get_date_property_name(self, workflow_name: str) -> str | None:
    """Get the date property name for a workflow.
    
    Args:
        workflow_name: The workflow name to look up.
        
    Returns:
        The date property name, or None if workflow not found or has no date property.
    """
    for workflow in self._workflows:
        if workflow.name == workflow_name:
            return workflow.date_property_name
    return None
```

---

### 7. Modify: `src/notion_hook/api/webhooks.py`

Remove separate parsing for `Date` and `Departure`. Use workflow's `date_property_name` to extract the date dynamically:

```python
@router.post("/webhooks/notion", response_model=WebhookResponse)
async def handle_notion_webhook(...):
    # ... parse payload, get page_id ...
    
    date_value: DateValue | None = None
    properties = data.get("properties", {})
    
    # NEW: Get date property name from workflow configuration
    registry = get_workflow_registry()
    if x_calvo_workflow:
        date_property_name = registry.get_date_property_name(x_calvo_workflow)
        if date_property_name:
            if date_data := get_property_ci(properties, date_property_name):
                try:
                    date_value = DateValue.model_validate(
                        date_data.get("date") if date_data else None
                    )
                except Exception as e:
                    logger.warning(f"Failed to parse {date_property_name} value: {e}")
    
    context = WorkflowContext(
        page_id=page_id,
        payload=payload,
        date_value=date_value,  # Single field for all workflows
        workflow_name=x_calvo_workflow,
    )
    # ... rest of handler ...
```

---

### 8. Modify: `tests/test_pasajes_sync.py`

Update all `departure_value=` to `date_value=`.

---

### 9. Modify: `tests/conftest.py`

Update any references to `departure_value`.

---

## Summary of Changes

| File | Change Type | Description |
|------|-------------|-------------|
| `workflows/base.py` | Modify | Add `date_property_name` class attribute |
| `workflows/cronograma_sync.py` | Modify | Add `date_property_name = "Date"` |
| `workflows/pasajes_sync.py` | Modify | Add `date_property_name = "Departure"`, use `context.date_value` |
| `workflows/atracciones_sync.py` | Modify | Update to use `date_property_name = "Fecha"` |
| `workflows/registry.py` | Modify | Add `get_date_property_name()` method |
| `models/webhook.py` | Modify | Remove `departure_value`, keep only `date_value` |
| `api/webhooks.py` | Modify | Dynamic date extraction based on workflow config |
| `tests/test_pasajes_sync.py` | Modify | Update `departure_value` → `date_value` |
| `tests/conftest.py` | Modify | Update fixtures |

---

## Benefits

1. **Single source of truth**: Each workflow declares its date property name
2. **Scalable**: Adding new workflows only requires setting `date_property_name`
3. **Cleaner context**: `WorkflowContext` has one `date_value` field
4. **Less duplication**: No repeated parsing logic in webhook handler
5. **Easier testing**: Consistent interface across all workflows

---

## Verification Steps

1. **Run linters**: `ruff check . && ruff format .`
2. **Run all tests**: `uv run pytest -v`
3. **Type check**: `uv run mypy src/` (if configured)
4. **Manual test**: Trigger webhooks for all three workflows (gastos, pasajes, atracciones)
