# Full Database Reload

This document describes the API used to reload all configured Notion databases into local SQLite while respecting table relationships.

## What it reloads

The full reload service fetches and syncs:

- `ciudades`
- `cronograma`
- `pasajes`
- `atracciones`
- `gastos`

Sync order for inserts/updates is parent-first:

1. `ciudades`
2. `cronograma`
3. `pasajes`
4. `atracciones`
5. `gastos`

Delete handling for missing rows is child-first to avoid FK issues.

## Endpoints

All endpoints require:

```text
X-Calvo-Key: <WEBHOOK_SECRET_KEY>
```

### POST /api/reload

Starts an all-database reload job.

Request body:

```json
{
  "mode": "full",
  "batch_size": 100,
  "delete_missing": true
}
```

Response:

```json
{
  "job_id": "uuid-v4",
  "status": "started",
  "message": "Full reload job started",
  "mode": "full",
  "batch_size": 100
}
```

### POST /api/reload/all

Alias of `POST /api/reload` that explicitly indicates “reload all DBs”.

Request/response are exactly the same.

### GET /api/reload/{job_id}

Returns status and progress for the job.

Response (example):

```json
{
  "job_id": "uuid-v4",
  "status": "in_progress",
  "progress": {
    "total": 420,
    "processed": 210,
    "created": 130,
    "updated": 70,
    "deleted": 10,
    "failed": 0
  },
  "table_progress": {
    "ciudades": { "total": 10, "processed": 10, "created": 0, "updated": 10, "deleted": 0, "failed": 0 },
    "cronograma": { "total": 90, "processed": 60, "created": 20, "updated": 40, "deleted": 0, "failed": 0 },
    "pasajes": { "total": 70, "processed": 40, "created": 25, "updated": 15, "deleted": 0, "failed": 0 },
    "atracciones": { "total": 100, "processed": 50, "created": 35, "updated": 15, "deleted": 0, "failed": 0 },
    "gastos": { "total": 150, "processed": 50, "created": 50, "updated": 0, "deleted": 10, "failed": 0 }
  },
  "started_at": "2026-02-24T14:10:00Z",
  "updated_at": "2026-02-24T14:10:12Z"
}
```

## Important behavior note

This reload service is **manual** (API-triggered), not webhook-driven.

- `gastos` has per-change local sync via `gastos-sync` webhook workflow.
- The new all-DB reload keeps local data consistent on demand.
- If you want per-change local mirroring for `ciudades/cronograma/pasajes/atracciones`, add dedicated sync workflows for those databases too.
