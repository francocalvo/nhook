# Gastos Feature

## Overview

The gastos feature provides local SQLite storage for Notion Gastos entries with full CRUD operations, retry logic, and a REST API for manual reload/sync. This feature enables:

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
| **Category** | multi_select | `category` | Comma-separated if multiple values |
| **Amount** | number | `amount` | Float value |
| **Date** | date | `date` | Extract start date, format as YYYY-MM-DD |
| **Payment Method** | select | `payment_method` | Select name value |

### Property Extraction Details

#### Description (Expense)
- Extracted from "Expense" property (title or rich_text type)
- Falls back to plain_text if available
- Returns `None` if property is missing or empty

#### Category (Multi-Select)
- Extracted from "Category" property (multi_select type)
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

## Workflow: gastos-sync

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

## Service: GastosReloadService

The `GastosReloadService` provides manual full/incremental reload from Notion API.

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

### Running Tests

```bash
# Run all tests
uv run pytest

# Run gastos-specific tests
uv run pytest tests/test_gastos_sync.py
uv run pytest tests/test_gastos_reload.py
uv run pytest tests/test_database.py

# Run with coverage
uv run pytest --cov=src/notion_hook --cov-report=html
```

### Test Coverage

All gastos feature code has comprehensive test coverage:
- Property parsing (Expense, Category, Amount, Date, Payment Method)
- CRUD operations (create, update, delete, list)
- Retry logic and failure handling
- Full and incremental reload
- Job tracking and progress
- Multi-select category handling (single and multiple values)

## Configuration

### Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `database_path` | SQLite database file path | No | `notion_hook.db` |
| `max_retries` | Maximum retry attempts for database operations | No | `3` |
| `retry_delay` | Base delay for retry backoff (seconds) | No | `1.0` |

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
