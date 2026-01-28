# FEATURE_3: REST API for Full Gastos Reload

## Overview
Create a REST API endpoint that triggers a full reload/sync of the Gastos table from Notion API to the local SQLite database. This provides manual control over data synchronization independent of webhooks.

## Problem Statement
The webhook-based sync has limitations:
- No way to recover from missed webhooks
- No initial data seeding mechanism
- Difficult to verify data consistency
- No manual trigger for full sync
- Relies entirely on webhook delivery
- No way to resync after database corruption

## Use Cases
- **Initial deployment**: Seed local database with all existing Gastos
- **Recovery**: Recover from webhook delivery failures
- **Verification**: Compare Notion data with local database
- **Maintenance**: Periodic full sync to catch missed events
- **Testing**: Manual trigger for development and testing
- **Resynchronization**: Fix database corruption or desync issues

## Proposed Architecture

### API Layer
New REST endpoint with the following characteristics:
- **Route**: `POST /api/gastos/reload`
- **Authentication**: Uses existing X-Calvo-Key header
- **Response**: Async task with progress tracking
- **Idempotent**: Safe to call multiple times
- **Status endpoint**: `GET /api/gastos/reload/{job_id}`

### Service Layer
New service module `services/gastos_reload.py` with:
- **GastosReloadService** class
- Handles full reload orchestration
- Manages Notion API pagination
- Handles database transaction batches
- Provides progress updates
- Logs detailed operation metrics

### Data Flow
```
REST Request
    ↓
API Endpoint
    ↓
GastosReloadService
    ↓
Notion API (query all Gastos)
    ↓
Parse and Transform
    ↓
Database Operations (batch)
    ↓
Response with Statistics
```

## API Design

### POST /api/gastos/reload
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
- `batch_size`: Number of records per database transaction
- `delete_missing`: Whether to delete local records not in Notion

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

**Response (long-running operation)**:
For full reload, returns job ID for status polling.

### GET /api/gastos/reload/{job_id}
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

**Response (failed)**:
```json
{
  "job_id": "uuid-v4",
  "status": "failed",
  "error": "Notion API timeout",
  "progress": {
    "total": 150,
    "processed": 75,
    "created": 60,
    "updated": 15,
    "deleted": 0,
    "failed": 0
  },
  "started_at": "2026-01-27T10:00:00Z",
  "failed_at": "2026-01-27T10:05:00Z"
}
```

## Service Layer Design

### GastosReloadService

#### Responsibilities
1. Query all Gastos from Notion API (handles pagination)
2. Parse and transform Notion data to Gasto models
3. Batch database operations for performance
4. Track progress and statistics
5. Handle errors and provide detailed logs
6. Support different sync modes (full/incremental)

#### Key Methods
- `start_reload(mode, batch_size, delete_missing)`: Start a reload job
- `execute_full_reload(job_id)`: Execute full reload (delete all, reload)
- `execute_incremental_reload(job_id)`: Execute incremental reload (smart merge)
- `query_notion_gastos()`: Fetch all Gastos from Notion with pagination
- `process_batch(batch, job_id)`: Process a batch of records
- `delete_missing_records(notion_page_ids, job_id)`: Delete local records not in Notion
- `update_progress(job_id, stats)`: Update job progress
- `get_job_status(job_id)`: Get current job status

#### Job Tracking
- In-memory job tracking (single process, jobs lost on restart)
- UUID for job identification
- Progress statistics (total, processed, created, updated, deleted, failed)
- Timestamps (started, updated, completed/failed)
- Error details if failed
- Job cleanup for old completed jobs (configurable max_age_hours)

### Data Transformation

#### Notion Property Mapping
Map Notion properties to Gasto model:
- Extract page_id from Notion page ID
- Parse `payment_method` property (text type, nullable)
- Parse `description` property (text type, nullable)
- Parse `amount` property (number type, nullable)
- Parse `date` property (date type, nullable, ISO format)
- Use case-insensitive property lookup (from FEATURE_1)
- Handle missing/null properties

#### Validation
- Validate required fields (page_id)
- Validate data types (amount as number)
- Validate date formats
- Log warnings for invalid data

### Batch Processing

#### Database Batching
Process records in configurable batch sizes (default: 100):
- Begin transaction
- Process all records in batch
- Commit transaction
- Log progress

#### Performance Considerations
- Reduce database round trips
- Batch inserts/updates in single transaction
- Limit memory usage with streaming
- Handle large datasets efficiently

### Error Handling

#### Notion API Errors
- Handle rate limiting (429) with retry
- Handle timeouts with retry
- Handle server errors (5xx) with retry
- Log all errors with context

#### Database Errors
- Handle database connection errors
- Handle constraint violations
- Use existing retry mechanism from database client
- Log all errors

#### Partial Failure Handling
- Continue processing on individual record failures
- Log failed records
- Provide failure count in response
- Don't abort entire job for single record failure

## Implementation Steps

### Phase 1: Notion Client Extension
1. Add method to query all Gastos from Notion
2. Handle Notion API pagination (has_more, next_cursor)
3. Configure page_size parameter (max 100 per Notion)
4. Add logging for pagination progress
5. Add error handling for API failures

### Phase 2: Job Tracking
1. Create in-memory job store (dict keyed by job_id)
2. Define job state structure
3. Add job creation method
4. Add job update method
5. Add job query method
6. Add job cleanup (old completed jobs)

### Phase 3: Reload Service
1. Create services/gastos_reload.py module
2. Implement GastosReloadService class
3. Implement job tracking methods
4. Implement Notion query method
5. Implement data transformation logic
6. Implement batch processing
7. Implement full reload execution
8. Implement incremental reload execution
9. Implement delete missing records logic
10. Add comprehensive error handling
11. Add progress logging

### Phase 4: API Endpoints
1. Create api/reload.py module (or add to existing routes)
2. Implement POST /api/gastos/reload endpoint
3. Implement GET /api/gastos/reload/{job_id} endpoint
4. Add request validation
5. Add response formatting
6. Add error handling
7. Add rate limiting (optional)

### Phase 5: Application Integration
1. Register reload service in app lifespan
2. Pass Notion client and database client to service
3. Register API routes
4. Add documentation for endpoints

### Phase 6: Testing
1. Create test_gastos_reload.py for service tests
2. Test Notion query with pagination
3. Test data transformation
4. Test batch processing
5. Test full reload execution
6. Test incremental reload execution
7. Test delete missing records
8. Test error handling
9. Create test_api_reload.py for API tests
10. Test POST /api/gastos/reload endpoint
11. Test GET /api/gastos/reload/{job_id} endpoint
12. Test authentication
13. Test invalid requests

## Testing Strategy

### Service Tests
Unit tests for GastosReloadService:
- Query all Gastos from Notion (single page)
- Query all Gastos from Notion (multiple pages)
- Query all Gastos from Notion (empty result)
- Data transformation for all property types
- Batch processing with various sizes
- Full reload execution
- Full reload with delete_missing=true
- Full reload with delete_missing=false
- Incremental reload execution
- Delete missing records
- Progress tracking and updates
- Job status queries
- Error handling for Notion API failures
- Error handling for database failures
- Partial failure handling

### API Tests
Integration tests for endpoints:
- POST /api/gastos/reload with valid request
- POST /api/gastos/reload with invalid mode
- POST /api/gastos/reload without authentication
- GET /api/gastos/reload/{job_id} for in-progress job
- GET /api/gastos/reload/{job_id} for completed job
- GET /api/gastos/reload/{job_id} for failed job
- GET /api/gastos/reload/{job_id} for non-existent job
- Response format validation
- Rate limiting (if implemented)

### End-to-End Tests
Full flow tests with mocked Notion and database:
- Full reload from empty database
- Full reload from populated database
- Full reload with delete_missing
- Full reload with Notion errors
- Full reload with database errors
- Job status polling workflow

### Performance Tests
- Reload with 100 records
- Reload with 1,000 records
- Reload with 10,000 records
- Concurrent reload requests (should queue or reject)

## Branch Strategy
Branch name: `feature/gastos-reload-api`

Commit sequence:
1. Add query_all_gastos method to Notion client
2. Implement job tracking mechanism
3. Implement GastosReloadService with full reload
4. Implement incremental reload and delete missing
5. Implement POST /api/gastos/reload endpoint
6. Implement GET /api/gastos/reload/{job_id} endpoint
7. Update application initialization
8. Add service unit tests
9. Add API integration tests
10. Add end-to-end tests
11. Run linting and formatting

## Success Criteria
- API endpoints return correct responses
- Full reload completes successfully
- Incremental reload completes successfully
- Delete missing records works correctly
- Job tracking provides accurate progress
- Error handling prevents job crashes
- All tests pass
- Code passes linting and formatting
- Test coverage > 80% for new code
- API documentation is complete

## Migration Notes
- No existing data to migrate (reload feature only)
- No breaking changes to existing APIs
- Webhook sync continues to work independently
- Reload is optional, manual operation
- No database schema changes required

## API Documentation
Add to API documentation:
- Endpoint descriptions
- Request/response schemas
- Authentication requirements
- Error codes and messages
- Example requests and responses
- Rate limiting information (if applicable)

## Risks and Mitigations
- **Risk**: Long-running reload operation blocks server
  - **Mitigation**: Use background tasks or async execution
- **Risk**: Memory exhaustion with large datasets
  - **Mitigation**: Stream records, process in batches, limit in-memory storage
- **Risk**: Concurrent reload operations cause conflicts
  - **Mitigation**: Allow only one active reload, queue others or reject
- **Risk**: Notion API rate limiting during reload
  - **Mitigation**: Handle 429 with retry, add rate limit awareness
- **Risk**: Database locks during reload
  - **Mitigation**: Use transactions, minimize lock duration
- **Risk**: Job data lost on server restart
  - **Mitigation**: Document as expected behavior, consider persistent job tracking in future

## Future Enhancements
- Add scheduled automatic reloads (cron)
- Implement diff report showing changes
- Add webhook pause/resume during reload
- Implement incremental sync based on timestamps
- Add data validation rules with error reporting
- Create admin UI for reload monitoring
- Support for selective reload (by date range)
- Export reload results to file
- Persistent job tracking across server restarts

## Integration with Existing Features
- Uses existing Notion client
- Uses existing database client
- Uses existing authentication
- Uses case-insensitive property lookup from FEATURE_1
- Works alongside webhook sync (not replacing it)
- Shares database schema from FEATURE_2
