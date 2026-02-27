# Plan: Webhook-Driven Local Sync For All Notion DBs

## Goal

Implement the same per-change sync behavior as `gastos-sync` for all relevant Notion databases, so local SQLite mirrors Notion in near real-time from webhook events.

Target DBs:

- `ciudades`
- `cronograma`
- `pasajes`
- `atracciones`
- `gastos` (already exists, keep and harden)

## Desired End State

1. Every create/update/delete webhook for these DBs can be routed to a dedicated `*-sync` workflow.
2. Local tables are up to date without requiring manual reload under normal operation.
3. Parent-child relationships remain valid despite out-of-order webhook delivery.
4. Failed syncs are logged and retried with bounded strategy.
5. Full reload (`/api/reload`, `/api/reload/all`) remains as recovery/backfill mechanism.

## Current State (As Of This Branch)

1. `gastos-sync` exists and writes local `gastos`.
2. `gastos-cronograma`, `pasajes-cronograma`, `atracciones-cronograma` update Notion relations, but do not mirror those DBs locally.
3. Local schema and batch sync methods now exist for `ciudades`, `cronograma`, `pasajes`, `atracciones`.
4. All-DB reload API/service exists and syncs all tables manually.
5. Missing piece: webhook-driven per-change local sync workflows for the other DBs.

## Architecture Decisions

### Workflow Names

Use explicit sync workflows to avoid collision with relation-sync workflows:

- `ciudades-sync`
- `cronograma-sync`
- `pasajes-sync`
- `atracciones-sync`
- `gastos-sync` (existing)

Relation-sync workflows remain separate:

- `gastos-cronograma`
- `pasajes-cronograma`
- `atracciones-cronograma`

### Routing Strategy

Use `X-Calvo-Workflow` to select exact workflow. This is already required by webhook handler and avoids heuristic matching ambiguity.

### Operation Detection

Replicate `gastos-sync` semantics in all sync workflows:

1. `delete` when `archived = true` OR `in_trash = true` OR `properties` empty.
2. Otherwise:
   - `create` if local row does not exist
   - `update` if local row exists

### Relationship Handling

Need deterministic behavior for out-of-order events (child before parent):

1. First write attempt may fail due to FK constraint.
2. Log failure (`fail_log`) with operation and message.
3. Retry policy:
   - quick retries in workflow process (existing DB retry)
   - if still failing, keep failure logged for background replay command/job
4. Add replay endpoint or command in later phase (recommended) to process `fail_log` in dependency order.

## Phased Implementation Plan

## Phase 0: Contract Freeze

Objective: remove ambiguity before coding.

Tasks:

1. Confirm Notion property mappings for each DB:
   - `ciudades`: canonical name/title field
   - `cronograma`: date/day field, city relation property
   - `pasajes`: departure date, cronograma relation, city relation
   - `atracciones`: name, fecha date, cronograma relation, city relation
2. Confirm which Notion automations emit which `X-Calvo-Workflow`.
3. Decide whether each Notion DB should have one or many automations.

Acceptance:

1. Mapping table documented in `docs/workflows.md` or `docs/reload.md`.
2. Header names and workflow names finalized.

## Phase 1: Data Access Layer Hardening

Objective: support single-record CRUD paths for all mirrored tables.

Tasks:

1. Add `get_*`, `create_*`, `update_*`, `delete_*` methods in `DatabaseClient` for:
   - `ciudades`
   - `cronograma`
   - `pasajes`
   - `atracciones`
2. Keep parity with `gastos` semantics and retry behavior.
3. Ensure SQL operations include all mapped columns.

Acceptance:

1. Unit tests per table for create/update/delete/get.
2. FK behavior validated in tests (parent required for child if FK non-null).

## Phase 2: New Sync Workflows

Objective: webhook-driven local sync for each DB.

Tasks:

1. Create workflow files:
   - `workflows/ciudades_sync.py`
   - `workflows/cronograma_sync_local.py` or `workflows/cronograma_db_sync.py` (name to avoid confusion)
   - `workflows/pasajes_sync_local.py`
   - `workflows/atracciones_sync_local.py`
2. Implement common pattern:
   - detect operation
   - parse model from webhook payload
   - execute DB operation
   - log failures
3. Register these workflows in `app.py`.

Acceptance:

1. Workflow unit tests cover create/update/delete paths.
2. Workflow tests include archived/in_trash and missing properties behavior.

## Phase 3: Shared Sync Base (Refactor For Consistency)

Objective: avoid duplication across five sync workflows.

Tasks:

1. Introduce shared abstract helper class for sync workflows:
   - operation detection
   - standardized error/fail-log handling
   - common telemetry fields
2. Migrate `gastos-sync` and new workflows to use base helper.

Acceptance:

1. No behavior regressions in existing `gastos-sync` tests.
2. New workflow code size reduced and consistent.

## Phase 4: Failure Replay Path

Objective: recover automatically from FK/order-related failures.

Tasks:

1. Add replay service for `fail_log` entries:
   - filters recent failed operations
   - reprocess in dependency order:
     - `ciudades`
     - `cronograma`
     - `pasajes`, `atracciones`
     - `gastos`
2. Expose manual endpoint:
   - `POST /api/reload/replay-failures` (or similar)
3. Optional scheduled replay every N minutes.

Acceptance:

1. Integration test:
   - child event arrives before parent
   - initial failure logged
   - replay after parent event succeeds

## Phase 5: API and Docs

Objective: make operations clear and safe.

Tasks:

1. Document all sync workflow headers and required automations.
2. Clarify manual reload vs webhook sync responsibilities.
3. Add runbook:
   - “what to do when rows are missing”
   - “how to replay failures”
   - “when to run full reload”

Acceptance:

1. `docs/README.md`, `docs/workflows.md`, `docs/reload.md`, `docs/architecture.md` updated.

## Phase 6: Observability

Objective: operational confidence.

Tasks:

1. Standard structured logs for every sync workflow:
   - workflow name
   - page_id
   - operation
   - success/failure
2. Add lightweight metrics counters in logs:
   - sync successes
   - sync failures
   - FK failures
   - replay successes
3. Add `GET /health` extension or admin endpoint with fail queue counts.

Acceptance:

1. On-call can identify failing DB/workflow from logs in < 1 minute.

## Test Plan (In Depth)

## Unit Tests

1. Parser tests per DB model:
   - happy path fields
   - missing optional properties
   - case-insensitive property names
2. Workflow operation detection tests:
   - archived/in_trash delete
   - create when absent
   - update when present
3. DB CRUD tests:
   - each table write/read/update/delete
   - FK violation behavior

## Integration Tests

1. API webhook -> workflow -> DB write (per sync workflow).
2. FK out-of-order event sequence:
   - child first fails
   - parent next succeeds
   - replay resolves child
3. Full reload + subsequent webhook updates remain consistent.

## Regression Tests

1. Existing:
   - `gastos-sync`
   - relation-sync workflows (`*-cronograma`)
   - reload endpoints
2. Search/list APIs for gastos still pass.

## Rollout Plan

## Step 1: Deploy code with new workflows disabled operationally

1. Merge code and tests.
2. Do not yet enable new Notion automations/headers.

## Step 2: Enable automations one DB at a time

1. `ciudades-sync`
2. `cronograma-sync`
3. `pasajes-sync`
4. `atracciones-sync`

After each step:

1. Watch logs for failures.
2. Validate local row count vs Notion sample.
3. Run replay if needed.

## Step 3: Run one full reload baseline

1. Trigger `POST /api/reload/all`.
2. Confirm all table counts and spot check row correctness.

## Step 4: Enable scheduled replay (optional)

1. If FK/order failures appear regularly, schedule replay endpoint/job.

## Risks and Mitigations

1. Property mapping mismatch between Notion and parser.
   - Mitigation: Phase 0 contract freeze + parser tests from real payload fixtures.
2. Out-of-order webhook delivery causing FK failures.
   - Mitigation: fail-log replay path + child-first reprocessing.
3. Duplicate/late webhook deliveries.
   - Mitigation: idempotent upsert logic, update-if-changed checks.
4. Workflow name misconfiguration in Notion automation.
   - Mitigation: explicit docs and smoke checks per workflow header.

## Acceptance Criteria For This Initiative

1. For each DB, create/update/delete in Notion is reflected locally via webhook flow.
2. Relationship integrity is preserved in SQLite.
3. If sync fails, failure is visible and replayable.
4. Full reload remains functional and non-destructive to sync workflows.
5. Test suite covers happy path, failure path, and ordering path.

## Suggested Execution Order (Concrete)

1. Phase 0
2. Phase 1
3. Phase 2
4. Phase 5 (docs draft)
5. Phase 4
6. Phase 6
7. Final rollout
