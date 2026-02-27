# Gastos Feature

## Overview

The gastos feature provides local SQLite storage for Notion Gastos entries with full CRUD operations, retry logic, and a REST API for manual reload/sync. This enables:

- Local querying and analytics of Gastos data
- Historical tracking of changes
- Backup of Gastos data
- Debugging capabilities for webhook failures
- Manual full/incremental reload from Notion

## Architecture

```
┌─────────────────┐     Webhook       ┌─────────────────┐
│                 │  (gastos-sync)    │                 │
│  Notion         │ ────────────────── │  GastosSync     │
│  Gastos DB      │                    │  Workflow        │
└─────────────────┘                    └────────┬────────┘
                                                │
                                                ▼
                                       ┌─────────────────┐
                                       │   SQLite       │
                                       │   Database     │
                                       └─────────────────┘

┌─────────────────┐     REST API     ┌─────────────────┐
│                 │  (manual)         │                 │
│  Notion         │ ────────────────── │  GastosReload   │
│  Gastos DB      │                    │  Service        │
└─────────────────┘                    └────────┬────────┘
                                                │
                                                ▼
                                       ┌─────────────────┐
                                       │   SQLite       │
                                       │   Database     │
                                       └─────────────────┘
```

## Database Schema

### gastos Table

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `page_id` | TEXT | No | Notion page ID (sync key) |
| `payment_method` | TEXT | Yes | Payment method (select value) |
| `description` | TEXT | Yes | Description (from Expense property) |
| `category` | TEXT | Yes | Category (comma-separated multi-select) |
| `amount` | REAL | Yes | Amount |
| `date` | DATE | Yes | Date in YYYY-MM-DD format |
| `created_at` | TIMESTAMP | No | Creation timestamp |
| `updated_at` | TIMESTAMP | No | Last update timestamp |

**Indexes**:
- Primary key on `page_id` (automatic)

### fail_log Table

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INTEGER | No | Auto-incremented ID |
| `page_id` | TEXT | No | Related Notion page ID |
| `operation` | TEXT | No | Operation type (create/update/delete) |
| `error_message` | TEXT | No | Error message |
| `retry_count` | INTEGER | Yes | Number of retry attempts |
| `created_at` | TIMESTAMP | No | Creation timestamp |

**Indexes**:
- Primary key on `id` (automatic)
- Index on `page_id` for querying failures by page
- Index on `created_at` for chronological review

## Notion Property Mapping

| Notion Property | Type | Field | Notes |
|----------------|------|-------|-------|
| **Expense** | title/rich_text | `description` | Text content from title or rich_text |
| **Category** | multi_select/select | `category` | Comma-separated if multiple values |
| **Amount** | number | `amount` | Float value |
| **Date** | date | `date` | Extract start date, format as YYYY-MM-DD |
| **Payment Method** | select | `payment_method` | Select name value |
| **Persona** | multi_select/select | `persona` | Comma-separated if multiple values |

### Property Extraction Details

#### Description (Expense)
- Extracted from "Expense" property (title or rich_text type)
- Falls back to plain_text if available
- Returns `None` if property is missing or empty

#### Category (Multi-Select or Select)
- Extracted from "Category" property (multi_select preferred, select supported)
- Multiple values joined with ", " (comma + space)
- Returns `None` if property is missing or empty
- Example: `["Food", "Groceries"]` → `"Food, Groceries"`

#### Date
- Extracted from "Date" property (date type)
- Uses `date["start"]` value
- Truncates timestamp to YYYY-MM-DD format
- Returns `None` if property is missing or null

#### Payment Method
- Extracted from "Payment Method" property (select type)
- Uses `select["name"]` value
- Returns `None` if property is missing or null

## Gastos Workflows

### gastos-sync Workflow

The `gastos-sync` workflow syncs Notion Gastos entries to the local SQLite database in real-time.

### Matching

- **Trigger**: Webhook with `X-Calvo-Workflow: gastos-sync` header
- **Operation Detection**:
  - **DELETE**: When `archived=true`, `in_trash=true`, or `properties` is empty
  - **CREATE**: When page doesn't exist in local database
  - **UPDATE**: When page exists and has new data

### CREATE Operation

```python
async def _handle_create(self, gasto: Gasto) -> dict[str, Any]:
    """Handle CREATE operation."""
    await self.database_client.create_gasto(gasto)
    return {"operation": "create", "page_id": gasto.page_id, "success": True}
```

### UPDATE Operation

```python
async def _handle_update(self, gasto: Gasto) -> dict[str, Any]:
    """Handle UPDATE operation."""
    updated = await self.database_client.update_gasto(gasto)
    return {
        "operation": "update",
        "page_id": gasto.page_id,
        "success": True,
        "updated": updated,
    }
```

### DELETE Operation

```python
async def _handle_delete(self, page_id: str) -> dict[str, Any]:
    """Handle DELETE operation."""
    deleted = await self.database_client.delete_gasto(page_id)
    return {
        "operation": "delete",
        "page_id": page_id,
        "success": True,
        "deleted": deleted,
    }
```

### Error Handling

- All database operations include retry logic with exponential backoff
- Failures are logged to `fail_log` table with retry count
- Returns success/failure status to webhook caller

## REST API: Gastos CRUD

The gastos feature provides REST API endpoints for creating gastos in Notion and querying gastos from local SQLite database.

### Features

- **Full Reload**: Delete all local data, reload from Notion
- **Incremental Reload**: Update changed records, delete missing records
- **Batch Processing**: Configurable batch size for performance
- **Job Tracking**: In-memory job tracking with progress
- **Job Cleanup**: Automatic cleanup of old completed jobs

### API Endpoints

#### POST /api/gastos/reload

Trigger a full reload of Gastos from Notion.

**Authentication**: Required (X-Calvo-Key)

**Request Body**:
```json
{
  "mode": "full" | "incremental",
  "batch_size": 100,
  "delete_missing": true
}
```

**Parameters**:
- `mode`:
  - `full`: Delete all local data, reload from Notion
  - `incremental`: Only update changed records (requires comparison logic)
- `batch_size`: Number of records per database transaction (default: 100)
- `delete_missing`: Whether to delete local records not in Notion (default: true)

**Response (immediate)**:
```json
{
  "job_id": "uuid-v4",
  "status": "started",
  "message": "Reload job started",
  "mode": "full",
  "batch_size": 100
}
```

#### GET /api/gastos/reload/{job_id}

Get status of a reload job.

**Authentication**: Required (X-Calvo-Key)

**Response (in progress)**:
```json
{
  "job_id": "uuid-v4",
  "status": "in_progress",
  "progress": {
    "total": 150,
    "processed": 75,
    "created": 40,
    "updated": 30,
    "deleted": 5,
    "failed": 0
  },
  "started_at": "2026-01-27T10:00:00Z",
  "updated_at": "2026-01-27T10:05:00Z"
}
```

**Response (completed)**:
```json
{
  "job_id": "uuid-v4",
  "status": "completed",
  "progress": {
    "total": 150,
    "processed": 150,
    "created": 120,
    "updated": 25,
    "deleted": 5,
    "failed": 0
  },
  "started_at": "2026-01-27T10:00:00Z",
  "completed_at": "2026-01-27T10:10:00Z",
  "duration_seconds": 600,
  "message": "Reload completed successfully"
}
```

## REST API: Gastos CRUD

The gastos feature provides REST API endpoints for creating gastos in Notion and querying gastos from the local SQLite database.

### POST /api/gastos

Create a new gasto by writing to Notion (not SQLite).

**Authentication**: Required (X-Calvo-Key)

**Request Body**:
```json
{
  "expense": "Lunch at restaurant",
  "amount": 45.50,
  "date": "2026-01-15",
  "category": "Food",
  "payment_method": "credit_card",
  "persona": "John"
}
```

**Parameters**:
- `expense` (required): Expense description, min length 1
- `amount` (required): Amount, must be greater than 0
- `date` (optional): Date in YYYY-MM-DD format
- `category` (optional): Category(s). Can be string or list of strings
- `payment_method` (optional): Payment method
- `persona` (optional): Persona(s). Can be string or list of strings

**Response (201 Created)**:
```json
{
  "page_id": "new-page-123",
  "message": "Gasto created successfully",
  "note": "Record will be synced to SQLite via Notion automation webhooks"
}
```

**Notes**:
- The gasto is created directly in Notion
- The record will be synced to SQLite via official Notion automation webhooks
- Use the webhook system (`gastos-sync` workflow) to ensure local sync

### GET /api/gastos

List and search gastos from SQLite database.

**Authentication**: Required (X-Calvo-Key)

**Query Parameters**:
| Parameter | Type | Description |
|-----------|------|-------------|
| `q` | string | Full-text search query over description, category, persona |
| `date_from` | string | Inclusive start date (YYYY-MM-DD) |
| `date_to` | string | Inclusive end date (YYYY-MM-DD) |
| `persona` | string | Filter by exact persona value |
| `payment_method` | string | Filter by exact payment method |
| `category` | string | Filter by category (contains) |
| `amount_min` | float | Minimum amount |
| `amount_max` | float | Maximum amount |
| `sort_by` | string | Sort field (date, created_at, amount). Default: `created_at` |
| `order` | string | Sort order (asc, desc). Default: `desc` |
| `limit` | int | Maximum results (1-1000). Default: 100 |
| `offset` | int | Results to skip. Default: 0 |

**Response (200 OK)**:
```json
{
  "results": [
    {
      "page_id": "test-page-1",
      "payment_method": "credit_card",
      "description": "Groceries",
      "category": "Food",
      "amount": 120.75,
      "date": "2026-01-10",
      "persona": "Jane",
      "created_at": "2026-01-10T10:00:00Z",
      "updated_at": "2026-01-10T10:00:00Z"
    },
    {
      "page_id": "test-page-2",
      "payment_method": "cash",
      "description": "Coffee",
      "category": "Food",
      "amount": 4.50,
      "date": "2026-01-11",
      "persona": "John",
      "created_at": "2026-01-11T08:30:00Z",
      "updated_at": "2026-01-11T08:30:00Z"
    }
  ],
  "total_count": 2
}
```

**Examples**:

Search by text:
```bash
curl -H "X-Calvo-Key: your-secret-key" \
  "https://your-server.com/api/gastos?q=groceries"
```

Filter by date range:
```bash
curl -H "X-Calvo-Key: your-secret-key" \
  "https://your-server.com/api/gastos?date_from=2026-01-01&date_to=2026-01-31"
```

Filter by persona:
```bash
curl -H "X-Calvo-Key: your-secret-key" \
  "https://your-server.com/api/gastos?persona=John"
```

Filter by amount range:
```bash
curl -H "X-Calvo-Key: your-secret-key" \
  "https://your-server.com/api/gastos?amount_min=10&amount_max=100"
```

Sort and paginate:
```bash
curl -H "X-Calvo-Key: your-secret-key" \
  "https://your-server.com/api/gastos?sort_by=date&order=desc&limit=50&offset=0"
```

**Notes**:
- Uses SQLite FTS5 for full-text search when `q` is provided
- When using `q`, other filters (date_from, date_to, persona, payment_method, category, amount_min, amount_max) are ignored
- `total_count` reflects the number of results returned (not total matching records)

### GET /api/gastos/{page_id}

Get a single gasto by page_id from SQLite database.

**Authentication**: Required (X-Calvo-Key)

**Path Parameters**:
- `page_id`: The Notion page ID

**Response (200 OK)**:
```json
{
  "page_id": "test-page-123",
  "payment_method": "credit_card",
  "description": "Lunch at restaurant",
  "category": "Food",
  "amount": 45.50,
  "date": "2026-01-15",
  "persona": "John",
  "created_at": "2026-01-15T12:00:00Z",
  "updated_at": "2026-01-15T12:00:00Z"
}
```

**Response (404 Not Found)**:
```json
{
  "detail": "Gasto with page_id 'non-existent-page' not found"
}
```

**Example**:
```bash
curl -H "X-Calvo-Key: your-secret-key" \
  "https://your-server.com/api/gastos/test-page-123"
```

## Database Enhancements

### Full-Text Search (FTS)

The gastos table is now integrated with a full-text search virtual table for efficient text search.

**FTS Table**: `gastos_fts`

**Indexed Columns**: `description`, `category`, `persona`

**Search Features**:
- Prefix search (e.g., `"coff*"` matches "coffee", "coffee shop")
- Phrase search (e.g., `"\"lunch at\""` matches exact phrase)
- Boolean operators (`AND`, `OR`, `NOT`)
- Automatic triggers keep FTS table in sync

**Example Queries**:

Simple search:
```bash
GET /api/gastos?q=groceries
```

Boolean search:
```bash
GET /api/gastos?q=food AND coffee
```

Prefix search:
```bash
GET /api/gastos?q=rest*  # Matches "restaurant", "rest"
```

### Updated Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `page_id` | TEXT | No | Notion page ID (sync key) |
| `payment_method` | TEXT | Yes | Payment method (select value) |
| `description` | TEXT | Yes | Description (from Expense property) |
| `category` | TEXT | Yes | Category (comma-separated multi-select) |
| `persona` | TEXT | Yes | Persona (comma-separated multi/select) |
| `amount` | REAL | Yes | Amount |
| `date` | DATE | Yes | Date in YYYY-MM-DD format |
| `created_at` | TIMESTAMP | No | Creation timestamp |
| `updated_at` | TIMESTAMP | No | Last update timestamp |

**New Column**: `persona` - Stores persona information from Notion

**Indexes**:
- Primary key on `page_id` (automatic)
- FTS table on `description`, `category`, `persona` (for full-text search)

### Reload Modes

#### Full Reload

1. Clear all local Gastos records
2. Query all Gastos from Notion (with pagination)
3. Batch insert all records

**Use case**: Initial deployment or complete resync

#### Incremental Reload

1. Query all Gastos from Notion
2. Compare with local database
3. Update changed records
4. Delete records not in Notion (if `delete_missing=true`)

**Use case**: Periodic sync, recovery from missed webhooks

### Job Progress Tracking

Jobs track:
- `total`: Total number of records to process
- `processed`: Number of records processed
- `created`: Number of new records inserted
- `updated`: Number of existing records updated
- `deleted`: Number of records deleted
- `failed`: Number of records that failed

## Database Client

The `DatabaseClient` provides async SQLite operations with retry logic.

### Key Methods

- `initialize()`: Initialize database connection and create schema
- `close()`: Close database connection
- `get_gasto(page_id)`: Retrieve single gasto
- `create_gasto(gasto)`: Insert new gasto
- `update_gasto(gasto)`: Update existing gasto
- `delete_gasto(page_id)`: Delete gasto
- `sync_gastos_batch(gastos, update_if_changed)`: Batch sync with change detection
- `list_gastos(limit, offset)`: List gastos with pagination
- `get_all_gastos_page_ids()`: Get all page IDs
- `log_failure(...)`: Log failed operation
- `get_failures(page_id)`: Query failure log

### Retry Logic

- **Max retries**: Configurable (default: 3)
- **Delay**: Exponential backoff (default: 1s, 2s, 4s)
- **Logging**: Each retry attempt is logged
- **Failure handling**: All retries exhausted → DatabaseError

## Testing

### Test Files

- `tests/test_gastos_sync.py`: Workflow unit tests
- `tests/test_gastos_reload.py`: Service unit tests
- `tests/test_database.py`: Database client unit tests
- `tests/test_api_gastos.py`: REST API endpoint tests (new)

### Running Tests

```bash
# Run all tests
uv run pytest

# Run gastos-specific tests
uv run pytest tests/test_gastos_sync.py
uv run pytest tests/test_gastos_reload.py
uv run pytest tests/test_database.py
uv run pytest tests/test_api_gastos.py

# Run with coverage
uv run pytest --cov=src/notion_hook --cov-report=html
```

### Test Coverage

All gastos feature code has comprehensive test coverage:
- Property parsing (Expense, Category, Amount, Date, Payment Method, Persona)
- CRUD operations (create, update, delete, list)
- Retry logic and failure handling
- Full and incremental reload
- Job tracking and progress
- Multi-select category handling (single and multiple values)
- REST API endpoints (POST, GET list, GET single)
- Full-text search and filtering
- Authentication and error handling

## Configuration

### Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
The Settings model uses uppercase environment variables:

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `DATABASE_PATH` | SQLite database file path | No | `notion_hook.db` |
| `MAX_RETRIES` | Maximum retry attempts for database operations | No | `3` |
| `RETRY_DELAY` | Base delay for retry backoff (seconds) | No | `1.0` |

### Database Path

- Default: `notion_hook.db` in project root
- Can be changed via `DATABASE_PATH` environment variable
- Use `:memory:` for testing (in-memory database)

## Migration Notes

### From Previous Schema

If you have existing data with the old schema:

1. **Backup your database**: Copy `notion_hook.db` to `notion_hook.db.backup`
2. **Run the app once**: Startup will add the `category` column if missing
3. **Full reload (recommended)**: Use `POST /api/gastos/reload` to backfill categories
4. **Verify data**: Check that all records are synced correctly

### Breaking Changes

- `description` field now maps to "Expense" property (was "Description")
- `category` field added (comma-separated multi-select)
- `date` field now stores YYYY-MM-DD (time portion is discarded)

## Best Practices

### Webhook Setup

1. Create Notion automation with `gastos-sync` workflow name
2. Configure automation to trigger on all Gastos changes
3. Set `X-Calvo-Workflow: gastos-sync` header in automation

### Reload Strategy

- **Initial setup**: Use full reload to seed database
- **Recovery**: Use incremental reload to catch missed webhooks
- **Periodic sync**: Schedule incremental reload (e.g., weekly)

### Monitoring

- Monitor `fail_log` table for webhook failures
- Check reload job progress for large datasets
- Verify data consistency between Notion and local database

## Troubleshooting

### Common Issues

**Issue**: Description field is empty
- **Cause**: Notion property is named "Description" instead of "Expense"
- **Fix**: Rename Notion property to "Expense"

**Issue**: Category values not showing correctly
- **Cause**: Multi-select property has unexpected structure
- **Fix**: Check Notion API response for Category property format

**Issue**: Date format errors
- **Cause**: Date value contains time component
- **Fix**: The parser keeps only YYYY-MM-DD; verify Notion Date values

**Issue**: Database locked errors
- **Cause**: Concurrent operations on database
- **Fix**: Reduce concurrent writers or rely on reload batching

### Debugging

```python
# Check fail_log for errors
SELECT * FROM fail_log ORDER BY created_at DESC LIMIT 10;

# Verify data sync
SELECT page_id, description, category, amount, date
FROM gastos
ORDER BY updated_at DESC
LIMIT 20;

# Check job progress
# Use GET /api/gastos/reload/{job_id} endpoint
```

## Future Enhancements

- Add backup/restore functionality
- Implement data validation rules
- Add historical audit trail
- Add analytics queries
- Create admin interface for viewing data
- Support for selective reload (by date range)
- Export reload results to file
- Persistent job tracking across server restarts
