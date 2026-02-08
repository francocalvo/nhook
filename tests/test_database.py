from __future__ import annotations

import os
import tempfile
from collections.abc import Generator

import pytest

os.environ["WEBHOOK_SECRET_KEY"] = "test-secret-key"
os.environ["NOTION_API_TOKEN"] = "secret_test_token"
os.environ["ATRACCIONES_DATABASE_ID"] = "test-atracciones-db-id"
os.environ["CIUDADES_DATABASE_ID"] = "test-ciudades-db-id"
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
        atracciones_database_id="test-atracciones-db-id",
        ciudades_database_id="test-ciudades-db-id",
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

    @pytest.mark.asyncio
    async def test_foreign_keys_pragma_enabled(self, db_client: DatabaseClient) -> None:
        """Test that foreign key constraints are enabled."""
        # Query the foreign_keys pragma to verify it's enabled
        async with db_client.conn.execute("PRAGMA foreign_keys") as cursor:
            row = await cursor.fetchone()
            assert row is not None
            # SQLite returns 1 for ON, 0 for OFF
            assert row[0] == 1

    @pytest.mark.asyncio
    async def test_foreign_key_constraint_enforcement(
        self, db_client: DatabaseClient
    ) -> None:
        """Test that foreign key constraints are enforced."""
        # Create a parent table (simulating ciudades)
        await db_client.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS test_parent (
                page_id TEXT PRIMARY KEY,
                nombre TEXT,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
        """
        )
        # Create a child table with FK constraint (simulating atracciones)
        await db_client.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS test_child (
                page_id TEXT PRIMARY KEY,
                parent_page_id TEXT REFERENCES test_parent(page_id),
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
        """
        )
        await db_client.conn.commit()

        # Insert a valid parent record
        await db_client.conn.execute(
            """
            INSERT INTO test_parent
            (page_id, nombre, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                "parent-1",
                "Parent One",
                "2024-01-01T00:00:00Z",
                "2024-01-01T00:00:00Z",
            ),
        )
        await db_client.conn.commit()

        # Insert a child with valid parent reference - should succeed
        await db_client.conn.execute(
            """
            INSERT INTO test_child
            (page_id, parent_page_id, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                "child-1",
                "parent-1",
                "2024-01-01T00:00:00Z",
                "2024-01-01T00:00:00Z",
            ),
        )
        await db_client.conn.commit()

        # Verify child was inserted
        async with db_client.conn.execute("SELECT COUNT(*) FROM test_child") as cursor:
            row = await cursor.fetchone()
            assert row[0] == 1

        # Try to insert child with invalid parent reference
        # should fail with FK constraint error
        with pytest.raises(Exception) as exc_info:
            await db_client.conn.execute(
                """
                INSERT INTO test_child
                (page_id, parent_page_id, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    "child-2",
                    "nonexistent-parent",
                    "2024-01-01T00:00:00Z",
                    "2024-01-01T00:00:00Z",
                ),
            )
            await db_client.conn.commit()

        # Verify error contains "FOREIGN KEY constraint failed"
        error_str = str(exc_info.value)
        assert "FOREIGN KEY" in error_str or "foreign key" in error_str

        # Verify child was NOT inserted
        async with db_client.conn.execute("SELECT COUNT(*) FROM test_child") as cursor:
            row = await cursor.fetchone()
            assert row[0] == 1  # Only the valid child exists

        # Cleanup test tables
        await db_client.conn.execute("DROP TABLE IF EXISTS test_child")
        await db_client.conn.execute("DROP TABLE IF EXISTS test_parent")
        await db_client.conn.commit()
