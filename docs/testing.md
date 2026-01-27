# Testing

## Running Tests

```bash
# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v

# Run specific test file
uv run pytest tests/test_webhooks.py

# Run specific test
uv run pytest tests/test_webhooks.py::test_webhook_handles_single_date

# Run with coverage
uv run pytest --cov=src/notion_hook --cov-report=html
```

## Test Structure

```
tests/
├── conftest.py              # Shared fixtures
├── test_auth.py             # Authentication tests
├── test_health.py           # Health endpoint test
├── test_webhooks.py         # Webhook endpoint tests
└── test_cronograma_sync.py  # Workflow unit tests
```

## Fixtures

### `settings`

Returns a `Settings` instance with test values:

```python
@pytest.fixture
def settings() -> Settings:
    return Settings(
        webhook_secret_key="test-secret-key",
        notion_api_token="secret_test_token",
        cronograma_database_id="test-cronograma-db-id",
        gastos_database_id="test-gastos-db-id",
        debug=True,
    )
```

### `mock_notion_client`

Returns an `AsyncMock` with pre-configured return values for Notion client methods:

```python
@pytest.fixture
def mock_notion_client(settings: Settings) -> AsyncMock:
    client = AsyncMock(spec=NotionClient)
    client.settings = settings
    client.find_cronograma_by_dates = AsyncMock(return_value=[])
    client.update_gastos_cronograma_relation = AsyncMock(return_value={})
    return client
```

### `test_client`

Returns a FastAPI `TestClient` with:
- Mocked workflow registry
- HTTP mocking via `respx` for Notion API calls

```python
@pytest.fixture
def test_client() -> Generator[TestClient, None, None]:
    # Sets up respx mocks for Notion API
    with respx.mock(assert_all_called=False) as respx_mock:
        respx_mock.post("https://api.notion.com/v1/databases/...").mock(...)
        respx_mock.patch(url__startswith="https://api.notion.com/v1/pages/").mock(...)

        with TestClient(app) as client:
            yield client
```

### `auth_headers`

Returns valid authentication headers:

```python
@pytest.fixture
def auth_headers() -> dict[str, str]:
    return {"X-Calvo-Key": "test-secret-key"}
```

## Writing Tests

### Testing Endpoints

Use `test_client` fixture for HTTP-level tests:

```python
def test_webhook_handles_single_date(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    response = test_client.post(
        "/webhooks/notion",
        json={"id": "test-page-id", "Date": {"start": "2026-03-14"}},
        headers=auth_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
```

### Testing Workflows

Use `mock_notion_client` fixture for unit testing workflow logic:

```python
@pytest.mark.asyncio
async def test_execute_finds_and_links_cronograma_entries(
    mock_notion_client: AsyncMock
) -> None:
    # Setup mock return values
    mock_notion_client.find_cronograma_by_dates.return_value = [
        {"id": "cronograma-1"},
        {"id": "cronograma-2"},
    ]

    # Create workflow and context
    workflow = CronogramaSyncWorkflow(mock_notion_client)
    context = WorkflowContext(
        page_id="test-page-id",
        payload={"id": "test-page-id", "Date": {"start": "2026-03-14"}},
        date_value=DateValue(start=date(2026, 3, 14)),
    )

    # Execute
    result = await workflow.execute(context)

    # Assert
    mock_notion_client.find_cronograma_by_dates.assert_called_once_with(
        [date(2026, 3, 14)]
    )
    assert result["updated_relations"] == ["cronograma-1", "cronograma-2"]
```

### Testing Authentication

Test both valid and invalid auth scenarios:

```python
def test_webhook_without_auth_fails(test_client: TestClient) -> None:
    response = test_client.post("/webhooks/notion", json={"id": "test"})
    assert response.status_code == 401
    assert "Missing X-Calvo-Key" in response.json()["detail"]

def test_webhook_with_invalid_key_fails(test_client: TestClient) -> None:
    response = test_client.post(
        "/webhooks/notion",
        json={"id": "test"},
        headers={"X-Calvo-Key": "wrong-key"},
    )
    assert response.status_code == 401
```

## Mocking External APIs

We use `respx` to mock httpx requests to the Notion API:

```python
import respx
from httpx import Response

with respx.mock() as respx_mock:
    # Mock database query
    respx_mock.post(
        "https://api.notion.com/v1/databases/db-id/query"
    ).mock(return_value=Response(
        200,
        json={"results": [{"id": "page-1"}], "has_more": False}
    ))

    # Mock page update
    respx_mock.patch(
        url__startswith="https://api.notion.com/v1/pages/"
    ).mock(return_value=Response(200, json={"id": "page-id"}))

    # Run test...
```

## Test Categories

| Category | File | Tests |
|----------|------|-------|
| Authentication | `test_auth.py` | Missing header, invalid key, valid key |
| Health | `test_health.py` | Health endpoint returns 200 |
| Webhooks | `test_webhooks.py` | Payload validation, date handling, workflow dispatch |
| Workflows | `test_cronograma_sync.py` | Matching, date expansion, execution |

## Code Quality

```bash
# Lint
uv run ruff check .

# Auto-fix lint issues
uv run ruff check --fix .

# Format
uv run ruff format .

# Check formatting without changes
uv run ruff format --check .
```
