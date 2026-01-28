# FEATURE_2: Local SQLite Database for Gastos

## Overview
Create a local SQLite database to store all Gastos entries from Notion with full CRUD operations, retry logic for failed operations, and a fail log for debugging. Implement a new workflow `gastos-sync` to handle CREATE, UPDATE, DELETE events from webhooks.

## Problem Statement
The system is currently stateless, with all data stored only in Notion. This creates limitations:
- No local query capabilities for analytics
- No historical tracking of changes
- No backup of Gastos data
- Difficult to debug webhook failures
- No way to perform local data analysis
- Dependency on Notion API availability for all operations

## Proposed Architecture

### Three-Tier Architecture

#### Presentation Layer (API)
- Existing webhook endpoint
- New workflow registration for gastos-sync
- No new API endpoints required for this feature

#### Business Logic Layer (Workflows)
- New `GastosSyncWorkflow` class
- Handles CREATE, UPDATE, DELETE operations
- Orchestrates Notion client and database client
- Implements retry logic
- Logs failures for debugging

#### Data Layer (Database)
- SQLite database with async operations
- Two tables: gastos and fail_log
- CRUD operations with transaction support
- Retry mechanism for transient failures

### Database Schema Design

#### gastos Table
- `page_id`: TEXT PRIMARY KEY (sync key with Notion)
- `payment_method`: TEXT (nullable)
- `description`: TEXT (nullable)
- `amount`: REAL (nullable)
- `date`: TEXT (ISO format, nullable)
- `created_at`: TIMESTAMP (from Notion's page.created_time on initial INSERT)
- `updated_at`: TIMESTAMP (from Notion's page.last_edited_time or local timestamp)

Indexing strategy:
- Primary key on page_id (automatic)
- Optional: Index on date for querying by date range
- Optional: Index on created_at/updated_at for sorting

#### fail_log Table
- `id`: INTEGER PRIMARY KEY AUTOINCREMENT
- `page_id`: TEXT NOT NULL
- `operation`: TEXT NOT NULL (create/update/delete)
- `error_message`: TEXT NOT NULL
- `retry_count`: INTEGER DEFAULT 0
- `created_at`: TIMESTAMP

Indexing strategy:
- Primary key on id (automatic)
- Index on page_id for querying failures by page
- Index on created_at for chronological review

### Data Models (Pydantic)
Define Pydantic models for:
- Gasto: Represents a gastos database record
- FailLogEntry: Represents a failure log entry
- Validation and serialization logic
- Type hints for all fields

### Database Client Design

#### Async Operations
- Use aiosqlite for async SQLite operations
- Context manager pattern for connection lifecycle
- Connection pooling not needed (single process)

#### Retry Mechanism
Configurable retry logic:
- Maximum retry attempts (default: 3)
- Delay between retries (default: 1 second)
- Exponential backoff option
- Log each retry attempt
- Log final failure after all retries

#### Error Handling
- Custom DatabaseError exception
- Separate error for transaction failures
- Graceful degradation on database unavailability
- All errors logged with context

#### CRUD Operations
- `get_gasto(page_id)`: Retrieve single gasto
- `create_gasto(gasto)`: Insert new gasto
- `update_gasto(gasto)`: Update existing gasto
- `delete_gasto(page_id)`: Delete gasto
- `list_gastos(limit, offset)`: Paginated list
- `log_failure(...)`: Log failed operation
- `get_failures(page_id)`: Query failure log

### Workflow Design: gastos-sync

#### Matching Logic
- Match when workflow name header is `gastos-sync`
- Support CREATE, UPDATE, DELETE operations from Notion
- Detect operation type:
  - **DELETE**: When `old_properties` exists in webhook payload and indicates page is being deleted
  - **CREATE**: When page does not exist in local database
  - **UPDATE**: When page exists in local database and has new data

#### CREATE Operation
- Extract gasto data from webhook payload
- Create Gasto model instance
- Call database client to insert
- Retry on failure
- Log failure if all retries exhausted

#### UPDATE Operation
- Extract updated gasto data from webhook
- Check if gasto exists in local database
- Update existing record with new data
- Set updated_at timestamp
- Retry on failure
- Log failure if all retries exhausted

#### DELETE Operation
- Extract page_id from webhook
- Delete gasto from local database
- Retry on failure
- Log failure if all retries exhausted

#### Data Extraction
Parse Notion properties from webhook payload:
- Extract `payment_method`, `description`, `amount`, `date` properties
- Use case-insensitive property lookup (from FEATURE_1)
- Handle Notion number types for amount
- Convert Notion date format to ISO string
- Handle missing/null properties

## Implementation Steps

### Phase 1: Dependencies and Configuration
1. Add aiosqlite dependency using uv
2. Add database settings to config module
3. Update .env.example with database configuration
4. Configure retry parameters

### Phase 2: Data Models
1. Create models/gastos.py module
2. Define Gasto Pydantic model
3. Define FailLogEntry Pydantic model
4. Add validation and type hints

### Phase 3: Database Client
1. Create core/database.py module
2. Implement DatabaseClient class
3. Implement database initialization and schema creation
4. Implement CRUD operations
5. Implement retry logic with exponential backoff
6. Implement failure logging
7. Add comprehensive error handling
8. Add logging for all operations

### Phase 4: Gastos Sync Workflow
1. Create workflows/gastos_sync.py module
2. Implement GastosSyncWorkflow class
3. Implement matches() method for workflow selection
4. Implement execute() method with operation detection
5. Implement CREATE operation handler
6. Implement UPDATE operation handler
7. Implement DELETE operation handler
8. Implement Notion property parsing logic

### Phase 5: Application Integration
1. Update app.py lifespan to initialize database client
2. Register GastosSyncWorkflow in workflow registry
3. Pass database client to workflow constructor
4. Add database cleanup on shutdown

### Phase 6: Testing
1. Create test_database.py with database client unit tests
2. Create test_gastos_sync.py with workflow unit tests
3. Test CRUD operations
4. Test retry logic
5. Test failure logging
6. Test concurrent operations
7. Test edge cases (null values, missing fields)

## Testing Strategy

### Database Client Tests
Unit tests for all operations:
- Initialization and schema creation
- Create gasto with valid data
- Create gasto with null/missing fields
- Update existing gasto
- Update non-existent gasto
- Delete existing gasto
- Delete non-existent gasto
- List gastos with pagination
- List gastos empty result
- Log failure operations
- Query failures by page_id
- Retry logic with transient failures
- Retry logic with permanent failures

### Workflow Tests
Unit tests for gastos-sync workflow:
- Match on correct workflow name
- Don't match on incorrect workflow name
- CREATE operation with valid payload
- CREATE operation with retry success
- CREATE operation with retry failure and logging
- UPDATE operation with existing record
- UPDATE operation with non-existent record
- DELETE operation with existing record
- DELETE operation with non-existent record
- Property parsing for all field types
- Property parsing with missing fields

### Integration Tests
End-to-end tests with mocked Notion client:
- Full webhook flow for CREATE
- Full webhook flow for UPDATE
- Full webhook flow for DELETE
- Retry behavior with database errors
- Failure logging on persistent errors

### Performance Tests
- Bulk insert operations
- Query performance with 1000+ records
- Concurrent operation handling

## Branch Strategy
Branch name: `feature/gastos-local-storage`

Commit sequence:
1. Add database configuration and dependencies
2. Create data models (Gasto, FailLogEntry)
3. Implement database client with CRUD operations
4. Implement retry logic and failure logging
5. Implement gastos-sync workflow
6. Update application initialization
7. Add database client unit tests
8. Add workflow unit tests
9. Add integration tests
10. Run linting and formatting

## Success Criteria
- All database operations work correctly
- Retry mechanism functions as expected
- Failures are logged to fail_log table
- Workflow handles CREATE, UPDATE, DELETE
- All tests pass
- Code passes linting and formatting
- Test coverage > 80% for new code
- No data loss in concurrent operations

## Migration Notes
- No existing data to migrate (system starts fresh)
- Database created automatically on first run
- Existing workflows continue to work
- No breaking changes to external APIs
- Database file location configurable via settings

## Risks and Mitigations
- **Risk**: Database corruption in production
  - **Mitigation**: SQLite is robust, add periodic backup scripts
- **Risk**: Performance degradation with large datasets
  - **Mitigation**: Add indexing, implement pagination
- **Risk**: Desync between Notion and local database
  - **Mitigation**: Add full sync feature (FEATURE_3)
- **Risk**: Retry loops causing resource exhaustion
  - **Mitigation**: Hard limit on retry attempts, exponential backoff

## Future Enhancements
- Add backup/restore functionality
- Implement data validation rules
- Add historical audit trail
- Implement full sync/resync feature
- Add analytics queries
- Create admin interface for viewing data
