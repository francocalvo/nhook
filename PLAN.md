---
Implementation Plan: Atracciones Cronograma Sync Workflow
Overview
Create a new workflow AtraccionesSyncWorkflow that automatically fills the Cronograma relation when the Fecha field is edited in the Atracciones database. This follows the same pattern as CronogramaSyncWorkflow and PasajesSyncWorkflow.
---

Files to Create/Modify

1.  New File: src/notion_hook/workflows/atracciones_sync.py Purpose: New
    workflow class for syncing Cronograma relation based on Fecha changes.
    Structure: class AtraccionesSyncWorkflow(BaseWorkflow): name =
    "atracciones-cronograma" description = "Sync Cronograma relation based on
    Fecha changes"
        - matches(): Match when workflow_name == "atracciones-cronograma"
        - execute():
            - Extract fecha_value from context
            - If None: clear Cronograma relation
            - If set: find matching Cronograma entries by date (ignoring time portion)
            - Update the Atracciones page with the relation
    Key considerations:

- The Fecha property can be a datetime (with time). We need to extract just the
  date portion for matching against Cronograma entries (which use date-only
  format yyyy-mm-dd).
- Reuse find_cronograma_by_dates() from NotionClient
- Create a new method update_atracciones_cronograma_relation() in NotionClient

---

2. Modify: src/notion_hook/models/webhook.py Changes:

- Add fecha_value: DateValue | None = None field to WorkflowContext This follows
  the pattern of date_value and departure_value.

---

3. Modify: src/notion_hook/api/webhooks.py Changes:

- Add parsing logic for Fecha property (similar to Date and Departure)
- Pass fecha_value to WorkflowContext if fecha_data :=
  get_property_ci(properties, "Fecha"): try: fecha_value =
  DateValue.model_validate( fecha_data.get("date") if fecha_data else None )
  except Exception as e: logger.warning(f"Failed to parse Fecha value: {e}")

---

4. Modify: src/notion_hook/clients/notion.py Changes:

- Add new method update_atracciones_cronograma_relation(): async def
  update_atracciones_cronograma_relation( self, page_id: str,
  cronograma_page_ids: list[str] ) -> dict[str, Any]: """Update the Cronograma
  relation on an Atracciones page.""" relation_value = [{"id": pid} for pid in
  cronograma_page_ids] properties = { PropertyNames.CRONOGRAMA: {"relation":
  relation_value}, } logger.info(f"Updating Atracciones {page_id} with
  {len(cronograma_page_ids)} relations") return await self.update_page(page_id,
  properties)

---

5. Modify: src/notion_hook/app.py Changes:

- Import AtraccionesSyncWorkflow
- Register the workflow in the lifespan function: from
  notion_hook.workflows.atracciones_sync import AtraccionesSyncWorkflow

# ...

## \_workflow_registry.register(AtraccionesSyncWorkflow)

6. Modify: src/notion_hook/workflows/**init**.py Changes:

- Export AtraccionesSyncWorkflow if the module exports workflows

---

7. New File: tests/test_atracciones_sync.py Test cases (following the pattern
   from test_cronograma_sync.py and test_pasajes_sync.py):
1. test_matches_with_correct_workflow_name - Verify workflow matches when
   workflow_name == "atracciones-cronograma"
1. test_does_not_match_wrong_workflow_name - Verify workflow doesn't match other
   workflow names
1. test_execute_clears_relation_when_fecha_is_none - Verify relations are
   cleared when Fecha is empty
1. test_execute_finds_and_links_cronograma_entries - Verify Cronograma entries
   are found and linked
1. test_execute_handles_datetime_fecha - Verify datetime values are handled
   correctly (time portion ignored for matching)

---

8. Modify: tests/conftest.py Changes:

- Add update_atracciones_cronograma_relation mock to mock_notion_client fixture
- Optionally add helper function make_atracciones_webhook_payload() for creating
  test payloads with Fecha property

---

Data Flow Notion Automation (Fecha changed) │ ▼ Webhook POST /webhooks/notion
Header: X-Calvo-Workflow: atracciones-cronograma │ ▼ Parse payload, extract
Fecha → fecha_value │ ▼ WorkflowContext(fecha_value=DateValue(...)) │ ▼
WorkflowRegistry.get_workflow() → AtraccionesSyncWorkflow │ ▼
AtraccionesSyncWorkflow.execute() │ ├── If fecha_value is None: │ └──
update_atracciones_cronograma_relation(page_id, []) │ └── If fecha_value is set:
├── Extract date from fecha_value.start (ignore time) ├──
find_cronograma_by_dates([date]) └──
update_atracciones_cronograma_relation(page_id, cronograma_ids)

---

Edge Cases to Handle

1. Datetime with time: Fecha can be 2026-03-14T10:00:00 - extract just the date
   2026-03-14
2. Empty Fecha: Clear the Cronograma relation
3. No matching Cronograma: Link empty list (no error)
4. Multiple Cronograma matches: Pass all to Notion API (Notion handles the
   limit=1)

---

Implementation Steps

1. Add fecha_value to WorkflowContext in models/webhook.py
2. Add update_atracciones_cronograma_relation() method to clients/notion.py
3. Create AtraccionesSyncWorkflow in workflows/atracciones_sync.py
4. Update webhook handler in api/webhooks.py to parse Fecha property
5. Register workflow in app.py
6. Update **init**.py exports if needed
7. Create tests in tests/test_atracciones_sync.py
8. Update test fixtures in tests/conftest.py
9. Run linters and formatters: ruff check . && ruff format .
10. Run tests: uv run pytest
11. Verify manually (optional): Test with actual Notion webhook

---

Verification Steps

1. Unit tests pass: uv run pytest tests/test_atracciones_sync.py -v
2. All tests pass: uv run pytest
3. Linting passes: ruff check .
4. Type checking passes: uv run mypy src/ (if configured)
5. Manual verification:
   - Create/edit an Atracciones entry with a Fecha value
   - Verify the Cronograma relation is automatically populated

---
