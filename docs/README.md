# NHook Documentation

NHook is a FastAPI webhook server that receives Notion automation webhooks and runs workflows to keep Notion relations in sync and/or mirror data to a local SQLite database.

## Quick Start

```bash
# Install dependencies
uv sync

# Copy and configure environment
cp .env.example .env
# Edit .env with your Notion API token and secret key

# Run server
uv run nhook

# Test health endpoint
curl http://localhost:8000/health
```

## Configuration

NHook is configured entirely via environment variables (see `.env.example`).

Required:

- `WEBHOOK_SECRET_KEY`: shared secret for `X-Calvo-Key` request header validation
- `NOTION_API_TOKEN`: Notion integration token
- `CRONOGRAMA_DATABASE_ID`: Notion database ID used by Cronograma relation workflows
- `GASTOS_DATABASE_ID`: Notion database ID used by Gastos reload service
- `PASAJES_DATABASE_ID`: Notion database ID used by Pasajes workflow (for Notion queries/updates)

Optional:

- `HOST` (default: `0.0.0.0`)
- `PORT` (default: `8000`)
- `DEBUG` (default: `false`)
- `DATABASE_PATH` (default: `notion_hook.db`)
- `MAX_RETRIES` (default: `3`) and `RETRY_DELAY` (default: `1.0`) for SQLite operations

## HTTP API

Endpoints:

- `GET /health` — health check
- `POST /webhooks/notion` — Notion automation webhook receiver
- `POST /api/reload` — start all-databases reload job
- `POST /api/reload/all` — alias endpoint for all-databases reload
- `GET /api/reload/{job_id}` — all-databases reload job status
- `POST /api/gastos/reload` — start a Gastos reload job
- `GET /api/gastos/reload/{job_id}` — job status

Auth:

- `X-Calvo-Key: <WEBHOOK_SECRET_KEY>` is required on all endpoints.
- `X-Calvo-Workflow: <workflow-name>` is optional but recommended for deterministically selecting a workflow.

## Workflows

Workflows are selected in this order:

1. If `X-Calvo-Workflow` is present, NHook runs the workflow with that exact name.
2. Otherwise it tries to find the first registered workflow whose `matches()` returns `True`.

Current workflow names:

- `gastos-cronograma` — sync Cronograma relation for Gastos when `Date` changes
- `pasajes-cronograma` — sync Cronograma relation for Pasajes when `Departure` changes (case-insensitive)
- `atracciones-cronograma` — sync Cronograma relation for Atracciones when `Fecha` changes (case-insensitive)
- `gastos-sync` — sync Gastos pages into local SQLite

## Documentation

- **[Workflows](./workflows.md)** - Workflow behaviors, triggers, and Notion requirements
- **[Architecture](./architecture.md)** - System design, components, and request flow
- **[Full Reload](./reload.md)** - Reload all configured Notion DBs into SQLite
- **[Gastos Feature](./gastos.md)** - Local storage + reload API (SQLite)
- **[Testing](./testing.md)** - Running tests, fixtures, and writing new tests
- **[Extending](./extending.md)** - Adding new workflows and features
- **[Deployment](./deployment.md)** - Exposing the server publicly (HTTPS) for Notion webhooks

## Features

- FastAPI-based async webhook server
- X-Calvo-Key header authentication
- Extensible workflow system
- Async Notion API client with httpx
- Full test coverage with pytest

## Current Workflows

### Cronograma Sync

Automatically syncs the `Cronograma` relation in Gastos entries when the `Date` property changes:

| Date Change | Action |
|-------------|--------|
| Set to single date | Links matching Cronograma entry |
| Set to date range | Links all Cronograma entries in range |
| Cleared | Removes all Cronograma relations |

### Pasajes Sync

Automatically syncs the `Cronograma` relation in Pasajes entries when the `departure` date property changes:

| Departure Change | Action |
|-----------------|--------|
| Set to date | Links matching Cronograma entry |
| Cleared | Removes all Cronograma relations |

### Atracciones Sync

Automatically syncs the `Cronograma` relation in Atracciones entries when the `Fecha` property changes:

| Fecha Change | Action |
|-------------|--------|
| Set to date | Links matching Cronograma entry |
| Cleared | Removes all Cronograma relations |

### Gastos Sync

Mirrors Gastos entries into a local SQLite database for querying/analytics and provides a reload API. See **[Gastos Feature](./gastos.md)**.

## Project Structure

```
src/notion_hook/
├── app.py           # FastAPI app
├── config.py        # Settings
├── api/             # HTTP endpoints
├── core/            # Auth, logging, exceptions
├── clients/         # Notion API client
├── workflows/       # Business logic
└── models/          # Pydantic models
```

## Requirements

- Python 3.12+
- Notion integration with database access
- Public HTTPS endpoint for webhooks (see [Deployment](./deployment.md))
