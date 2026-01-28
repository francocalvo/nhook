# Architecture

## Overview

NHook is a FastAPI-based webhook server that receives events from Notion automations and executes workflows to sync data between Notion databases.

```
┌─────────────────┐     HTTP POST      ┌─────────────────┐
│                 │  X-Calvo-Key auth  │                 │
│  Notion         │ ────────────────── │  NHook Server   │
│  Automation     │                    │                 │
└─────────────────┘                    └────────┬────────┘
                                                │
                                                ▼
                                       ┌─────────────────┐
                                       │ Workflow        │
                                       │ Registry        │
                                       └────────┬────────┘
                                                │
                                                ▼
                                       ┌─────────────────┐
                                       │ Matched         │
                                       │ Workflow        │
                                       └────────┬────────┘
                                                │
                                                ▼
                                       ┌─────────────────┐
                                       │ Notion API      │
                                       │ Client          │
                                       └─────────────────┘
```

## Directory Structure

```
src/notion_hook/
├── app.py                 # Application factory and entry point
├── config.py              # Environment-based settings
├── api/                   # HTTP layer
│   ├── routes.py          # Router aggregation
│   ├── webhooks.py        # Webhook endpoint
│   ├── health.py          # Health check
│   └── reload.py         # Gastos reload endpoint
├── core/                  # Shared utilities
│   ├── auth.py            # Authentication
│   ├── database.py        # SQLite database client
│   ├── exceptions.py      # Custom exceptions
│   └── logging.py         # Logging configuration
├── clients/               # External service clients
│   └── notion.py          # Notion API client
├── workflows/             # Business logic
│   ├── base.py            # Abstract workflow class
│   ├── registry.py        # Workflow dispatch
│   ├── cronograma_sync.py # Cronograma sync implementation
│   ├── pasajes_sync.py    # Pasajes sync implementation
│   └── gastos_sync.py    # Gastos sync implementation
├── services/              # Business services
│   └── gastos_reload.py   # Gastos reload service
└── models/                # Data models
    ├── webhook.py         # Pydantic models for webhooks
    └── gastos.py         # Pydantic models for gastos
```

## Components

### Application Layer (`app.py`)

The FastAPI application uses a lifespan context manager to:
1. Initialize the Notion client on startup
2. Register workflows with the registry
3. Clean up resources on shutdown

```python
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Startup: create client and register workflows
    _notion_client = NotionClient(settings)
    _workflow_registry = WorkflowRegistry(_notion_client)
    _workflow_registry.register(CronogramaSyncWorkflow)

    yield

    # Shutdown: cleanup
    await _notion_client.__aexit__(...)
```

### Configuration (`config.py`)

Settings are loaded from environment variables using `pydantic-settings`:

| Variable | Description | Required |
|----------|-------------|----------|
| `WEBHOOK_SECRET_KEY` | Secret for X-Calvo-Key validation | Yes |
| `NOTION_API_TOKEN` | Notion integration token | Yes |
| `CRONOGRAMA_DATABASE_ID` | Cronograma database ID | No (has default) |
| `GASTOS_DATABASE_ID` | Gastos database ID | No (has default) |
| `PASAJES_DATABASE_ID` | Pasajes database ID | No (has default) |
| `HOST` | Server bind address | No (default: 0.0.0.0) |
| `PORT` | Server port | No (default: 8000) |
| `DEBUG` | Enable debug mode | No (default: false) |

### Authentication (`core/auth.py`)

Webhook requests are authenticated via the `X-Calvo-Key` header:

```python
async def verify_webhook_key(x_calvo_key: str | None = Header()) -> str:
    if not secrets.compare_digest(x_calvo_key, settings.webhook_secret_key):
        raise HTTPException(status_code=401)
    return x_calvo_key
```

Uses `secrets.compare_digest()` for timing-safe comparison.

### Notion Client (`clients/notion.py`)

Async HTTP client using `httpx` for Notion API operations:

- `get_page(page_id)` - Retrieve a page
- `update_page(page_id, properties)` - Update page properties
- `query_database(database_id, filter, sorts)` - Query with pagination
- `find_cronograma_by_dates(dates)` - Find Cronograma entries by date
- `update_gastos_cronograma_relation(page_id, cronograma_ids)` - Update Gastos relation
- `update_pasajes_cronograma_relation(page_id, cronograma_ids)` - Update Pasajes relation

### Workflow System

#### Base Workflow (`workflows/base.py`)

Abstract class defining the workflow interface:

```python
class BaseWorkflow(ABC):
    name: str
    description: str

    @abstractmethod
    def matches(self, context: WorkflowContext) -> bool:
        """Check if this workflow handles the webhook."""

    @abstractmethod
    async def execute(self, context: WorkflowContext) -> dict[str, Any]:
        """Execute the workflow logic."""
```

#### Registry (`workflows/registry.py`)

Manages workflow registration and dispatch:

```python
registry = WorkflowRegistry(notion_client)
registry.register(CronogramaSyncWorkflow)

# On webhook:
workflow = registry.get_workflow(context)  # Finds matching workflow
result = await workflow.execute(context)
```

#### Cronograma Sync (`workflows/cronograma_sync.py`)

The workflow that syncs Cronograma relations for Gastos entries:

1. **Matches** when payload contains `Date` property
2. **Execution logic**:
   - Date is null → Clear Cronograma relation
   - Date is single → Find matching Cronograma entry
   - Date is range → Expand to all dates, find all matching entries
3. **Updates** the Gastos page with found Cronograma page IDs

#### Pasajes Sync (`workflows/pasajes_sync.py`)

The workflow that syncs Cronograma relations for Pasajes entries:

1. **Matches** when payload contains `departure` property
2. **Execution logic**:
    - Departure is null → Clear Cronograma relation
    - Departure is set → Find matching Cronograma entry by date
3. **Updates** the Pasajes page with found Cronograma page IDs

#### Gastos Sync (`workflows/gastos_sync.py`)

The workflow that syncs Gastos entries to local SQLite database:

1. **Matches** when workflow name is `gastos-sync`
2. **Execution logic**:
    - Detects operation type (CREATE/UPDATE/DELETE)
    - Extracts properties (Expense, Category, Amount, Date, Payment Method)
    - Performs corresponding database operation with retry logic
    - Logs failures to `fail_log` table
3. **Notion Property Mapping**:
    - `Expense` (title/rich_text) → `description`
    - `Category` (multi_select) → `category` (comma-separated)
    - `Amount` (number) → `amount`
    - `Date` (date) → `date` (YYYY-MM-DD)
    - `Payment Method` (select) → `payment_method`

#### Gastos Reload Service (`services/gastos_reload.py`)

The service that provides manual reload from Notion to local database:

1. **Features**:
    - Full reload (delete all, reload from Notion)
    - Incremental reload (update changed, delete missing)
    - Batch processing for performance
    - Job tracking with progress
2. **API Endpoints**:
    - `POST /api/gastos/reload` - Start reload job
    - `GET /api/gastos/reload/{job_id}` - Get job status
3. **Use Cases**:
    - Initial deployment (seed database)
    - Recovery from missed webhooks
    - Periodic full sync
    - Data consistency verification

See **[Gastos Feature](./gastos.md)** for complete documentation.

## Request Flow

1. Notion automation triggers webhook on Date/departure change
2. Server validates `X-Calvo-Key` header
3. Webhook endpoint parses Notion's nested payload structure:
   - Page ID: `payload.data.id`
   - Date property: `payload.data.properties.Date.date` (for Gastos)
   - Departure property: `payload.data.properties.departure.date` (for Pasajes)
   - Source metadata: `payload.source.event_id`, etc.
4. Registry finds matching workflow via `matches()`
   - CronogramaSyncWorkflow for `Date` property changes
   - PasajesSyncWorkflow for `departure` property changes
5. Workflow executes and updates Notion via API client
6. Response returned to Notion

## Error Handling

Custom exceptions in `core/exceptions.py`:

- `AuthenticationError` - Invalid/missing auth header (401)
- `NotionClientError` - Notion API failures (502)
- `WorkflowError` - Workflow execution failures (500)
- `WorkflowNotFoundError` - No matching workflow (200 with success=false)
