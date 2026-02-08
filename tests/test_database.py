from __future__ import annotations

import os
import tempfile
from collections.abc import Generator

import pytest

os.environ["WEBHOOK_SECRET_KEY"] = "test-secret-key"
os.environ["NOTION_API_TOKEN"] = "secret_test_token"
os.environ["CRONOGRAMA_DATABASE_ID"] = "test-cronograma-db-id"
os.environ["GASTOS_DATABASE_ID"] = "test-gastos-db-id"
os.environ["PASAJES_DATABASE_ID"] = "test-pasajes-db-id"

from notion_hook.config import Settings, clear_settings_cache
from notion_hook.core.database import DatabaseClient
from notion_hook.models.gastos import Gasto


@pytest.fixture(autouse=True)
def clear_cache() -> Generator[None, None, None]:
    """Clear settings cache before each test."""
    clear_settings_cache()
    yield


@pytest.fixture
def settings() -> Generator[Settings, None, None]:
    """Return test settings with temporary database path."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    yield Settings(
        webhook_secret_key="test-secret-key",
        notion_api_token="secret_test_token",
        cronograma_database_id="test-cronograma-db-id",
        gastos_database_id="test-gastos-db-id",
        pasajes_database_id="test-pasajes-db-id",
        database_path=db_path,
        debug=True,
        max_retries=2,
        retry_delay=0.01,
    )
    os.unlink(db_path)


@pytest.fixture
async def db_client(settings: Settings) -> DatabaseClient:
    """Return a test database client."""
    client = DatabaseClient(settings)
    await client.initialize()
    yield client
    await client.close()


class TestDatabaseClient:
    """Tests for DatabaseClient."""

    @pytest.mark.asyncio
    async def test_initialization(self, settings: Settings) -> None:
        """Test database client initialization."""
        async with DatabaseClient(settings) as client:
            assert client._conn is not None

    @pytest.mark.asyncio
    async def test_create_gasto(self, db_client: DatabaseClient) -> None:
        """Test creating a gasto."""
        gasto = Gasto(
            page_id="test-page-1",
            payment_method="Cash",
            description="Test expense",
            category=None,
            amount=100.0,
            date="2024-01-01",
            persona=None,
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        await db_client.create_gasto(gasto)
        retrieved = await db_client.get_gasto("test-page-1")
        assert retrieved is not None
        assert retrieved.page_id == "test-page-1"
        assert retrieved.payment_method == "Cash"
        assert retrieved.description == "Test expense"
        assert retrieved.amount == 100.0

    @pytest.mark.asyncio
    async def test_create_gasto_with_nulls(self, db_client: DatabaseClient) -> None:
        """Test creating a gasto with null values."""
        gasto = Gasto(
            page_id="test-page-2",
            payment_method=None,
            description=None,
            category=None,
            amount=None,
            date=None,
            persona=None,
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        await db_client.create_gasto(gasto)
        retrieved = await db_client.get_gasto("test-page-2")
        assert retrieved is not None
        assert retrieved.payment_method is None
        assert retrieved.description is None
        assert retrieved.amount is None
        assert retrieved.date is None

    @pytest.mark.asyncio
    async def test_update_gasto(self, db_client: DatabaseClient) -> None:
        """Test updating a gasto."""
        gasto = Gasto(
            page_id="test-page-3",
            payment_method="Cash",
            description="Test",
            category=None,
            amount=100.0,
            date="2024-01-01",
            persona=None,
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        await db_client.create_gasto(gasto)

        updated = Gasto(
            page_id="test-page-3",
            payment_method="Credit Card",
            description="Updated",
            category=None,
            amount=200.0,
            date="2024-01-02",
            persona=None,
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-02T00:00:00Z",
        )
        result = await db_client.update_gasto(updated)
        assert result is True

        retrieved = await db_client.get_gasto("test-page-3")
        assert retrieved.payment_method == "Credit Card"
        assert retrieved.description == "Updated"
        assert retrieved.amount == 200.0
        assert retrieved.date == "2024-01-02"

    @pytest.mark.asyncio
    async def test_update_nonexistent_gasto(self, db_client: DatabaseClient) -> None:
        """Test updating a gasto that doesn't exist."""
        gasto = Gasto(
            page_id="nonexistent",
            payment_method="Cash",
            description="Test",
            category=None,
            amount=100.0,
            date="2024-01-01",
            persona=None,
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        result = await db_client.update_gasto(gasto)
        assert result is False

    @pytest.mark.asyncio
    async def test_delete_gasto(self, db_client: DatabaseClient) -> None:
        """Test deleting a gasto."""
        gasto = Gasto(
            page_id="test-page-4",
            payment_method="Cash",
            description="Test",
            category=None,
            amount=100.0,
            date="2024-01-01",
            persona=None,
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        await db_client.create_gasto(gasto)
        result = await db_client.delete_gasto("test-page-4")
        assert result is True

        retrieved = await db_client.get_gasto("test-page-4")
        assert retrieved is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent_gasto(self, db_client: DatabaseClient) -> None:
        """Test deleting a gasto that doesn't exist."""
        result = await db_client.delete_gasto("nonexistent")
        assert result is False

    @pytest.mark.asyncio
    async def test_list_gastos(self, db_client: DatabaseClient) -> None:
        """Test listing gastos."""
        for i in range(5):
            gasto = Gasto(
                page_id=f"test-page-{i}",
                payment_method="Cash",
                description=f"Test {i}",
                category=None,
                amount=100.0 * i,
                date="2024-01-01",
                persona=None,
                created_at=f"2024-01-0{i}T00:00:00Z",
                updated_at=f"2024-01-0{i}T00:00:00Z",
            )
            await db_client.create_gasto(gasto)

        gastos = await db_client.list_gastos(limit=3)
        assert len(gastos) == 3

        gastos = await db_client.list_gastos(limit=10, offset=2)
        assert len(gastos) == 3

        gastos = await db_client.list_gastos()
        assert len(gastos) == 5

    @pytest.mark.asyncio
    async def test_list_empty_gastos(self, db_client: DatabaseClient) -> None:
        """Test listing gastos when empty."""
        gastos = await db_client.list_gastos()
        assert len(gastos) == 0

    @pytest.mark.asyncio
    async def test_log_failure(self, db_client: DatabaseClient) -> None:
        """Test logging a failure."""
        log_id = await db_client.log_failure(
            page_id="test-page-5",
            operation="create",
            error_message="Test error",
            retry_count=1,
        )
        assert log_id is not None
        assert log_id > 0

        failures = await db_client.get_failures("test-page-5")
        assert len(failures) == 1
        assert failures[0].page_id == "test-page-5"
        assert failures[0].operation == "create"
        assert failures[0].error_message == "Test error"
        assert failures[0].retry_count == 1

    @pytest.mark.asyncio
    async def test_get_failures_empty(self, db_client: DatabaseClient) -> None:
        """Test getting failures for a page with no failures."""
        failures = await db_client.get_failures("test-page-6")
        assert len(failures) == 0

    @pytest.mark.asyncio
    async def test_get_gasto_not_found(self, db_client: DatabaseClient) -> None:
        """Test getting a gasto that doesn't exist."""
        gasto = await db_client.get_gasto("nonexistent")
        assert gasto is None
