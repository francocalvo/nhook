# NHook Documentation

NHook is a FastAPI webhook server that auto-syncs Notion database relations based on property changes.

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

## Documentation

- **[Architecture](./architecture.md)** - System design, components, and request flow
- **[Testing](./testing.md)** - Running tests, fixtures, and writing new tests
- **[Extending](./extending.md)** - Adding new workflows and features
- **[Deployment](./deployment.md)** - Making the server publicly accessible for Notion webhooks

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
