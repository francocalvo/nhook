from __future__ import annotations

import os
import tempfile
from collections.abc import Generator

import aiosqlite
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
from notion_hook.models.notion_db import Atraccion, Ciudad, Cronograma, Pasaje


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
    async def test_create_gasto_with_city_fields(
        self, db_client: DatabaseClient
    ) -> None:
        """Test creating a gasto with city fields."""
        # Create the ciudad first (required for FK constraint)
        ciudad = Ciudad(
            page_id="city-rome-123",
            name="Rome",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        await db_client.create_ciudad(ciudad)

        gasto = Gasto(
            page_id="test-page-city-1",
            payment_method="Cash",
            description="Test expense in Rome",
            category="Food",
            amount=50.0,
            date="2024-01-01",
            persona="Franco",
            ciudad_page_id="city-rome-123",
            ciudad="Rome",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        await db_client.create_gasto(gasto)
        retrieved = await db_client.get_gasto("test-page-city-1")
        assert retrieved is not None
        assert retrieved.page_id == "test-page-city-1"
        assert retrieved.ciudad_page_id == "city-rome-123"
        assert retrieved.ciudad == "Rome"

    @pytest.mark.asyncio
    async def test_gastos_schema_migration(self, settings: Settings) -> None:
        """Test that schema migration adds ciudad_page_id and ciudad columns."""
        # Create a database with old schema (without ciudad fields)
        async with aiosqlite.connect(settings.database_path) as conn:
            await conn.execute("""
                CREATE TABLE gastos (
                    page_id TEXT PRIMARY KEY,
                    payment_method TEXT,
                    description TEXT,
                    category TEXT,
                    amount REAL,
                    date DATE,
                    persona TEXT,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
            """)
            # Insert a row without ciudad fields
            await conn.execute(
                """INSERT INTO gastos (
                    page_id, payment_method, description, category, amount, date,
                    persona, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    "old-page-1",
                    "Cash",
                    "Old expense",
                    None,
                    100.0,
                    "2024-01-01",
                    None,
                    "2024-01-01T00:00:00Z",
                    "2024-01-01T00:00:00Z",
                ),
            )
            await conn.commit()

        # Initialize database client (should run migration)
        async with DatabaseClient(settings) as client:
            # Verify the old row still exists
            gasto = await client.get_gasto("old-page-1")
            assert gasto is not None
            assert gasto.page_id == "old-page-1"
            assert gasto.payment_method == "Cash"
            assert gasto.ciudad_page_id is None
            assert gasto.ciudad is None

            # Create a ciudad first (required for FK constraint)
            ciudad = Ciudad(
                page_id="city-rome-456",
                name="Rome",
                created_at="2024-01-02T00:00:00Z",
                updated_at="2024-01-02T00:00:00Z",
            )
            await client.create_ciudad(ciudad)

            # Verify we can create new rows with ciudad fields
            new_gasto = Gasto(
                page_id="new-page-1",
                payment_method="Credit Card",
                description="New expense",
                category="Food",
                amount=50.0,
                date="2024-01-02",
                persona="Franco",
                ciudad_page_id="city-rome-456",
                ciudad="Rome",
                created_at="2024-01-02T00:00:00Z",
                updated_at="2024-01-02T00:00:00Z",
            )
            await client.create_gasto(new_gasto)

            retrieved = await client.get_gasto("new-page-1")
            assert retrieved is not None
            assert retrieved.ciudad_page_id == "city-rome-456"
            assert retrieved.ciudad == "Rome"

    @pytest.mark.asyncio
    async def test_update_gasto(self, db_client: DatabaseClient) -> None:
        """Test updating a gasto."""
        # Create a ciudad first (required for FK constraint)
        ciudad = Ciudad(
            page_id="city-rome-123",
            name="Rome",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        await db_client.create_ciudad(ciudad)

        gasto = Gasto(
            page_id="test-page-3",
            payment_method="Cash",
            description="Test",
            category=None,
            amount=100.0,
            date="2024-01-01",
            persona=None,
            ciudad_page_id=None,
            ciudad=None,
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
            ciudad_page_id="city-rome-123",
            ciudad="Rome",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-02T00:00:00Z",
        )
        result = await db_client.update_gasto(updated)
        assert result is True

        retrieved = await db_client.get_gasto("test-page-3")
        assert retrieved is not None
        assert retrieved.payment_method == "Credit Card"
        assert retrieved.description == "Updated"
        assert retrieved.amount == 200.0
        assert retrieved.date == "2024-01-02"
        assert retrieved.ciudad_page_id == "city-rome-123"
        assert retrieved.ciudad == "Rome"

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
            ciudad_page_id=None,
            ciudad=None,
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
            ciudad_page_id=None,
            ciudad=None,
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
                ciudad_page_id=None,
                ciudad=None,
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
    async def test_ciudad_crud(self, db_client: DatabaseClient) -> None:
        """Test create/get/update/delete for ciudades."""
        ciudad = Ciudad(
            page_id="city-1",
            name="Buenos Aires",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        await db_client.create_ciudad(ciudad)

        saved = await db_client.get_ciudad("city-1")
        assert saved is not None
        assert saved.name == "Buenos Aires"

        updated = Ciudad(
            page_id="city-1",
            name="CABA",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-02T00:00:00Z",
        )
        assert await db_client.update_ciudad(updated) is True

        saved_after = await db_client.get_ciudad("city-1")
        assert saved_after is not None
        assert saved_after.name == "CABA"
        assert await db_client.delete_ciudad("city-1") is True
        assert await db_client.get_ciudad("city-1") is None

    @pytest.mark.asyncio
    async def test_cronograma_crud(self, db_client: DatabaseClient) -> None:
        """Test create/get/update/delete for cronograma."""
        await db_client.create_ciudad(
            Ciudad(
                page_id="city-1",
                name="Buenos Aires",
                created_at="2024-01-01T00:00:00Z",
                updated_at="2024-01-01T00:00:00Z",
            )
        )
        cronograma = Cronograma(
            page_id="cron-1",
            day="2026-03-14",
            ciudad_page_id="city-1",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        await db_client.create_cronograma(cronograma)
        saved = await db_client.get_cronograma("cron-1")
        assert saved is not None
        assert saved.day == "2026-03-14"

        cronograma.day = "2026-03-15"
        cronograma.updated_at = "2024-01-02T00:00:00Z"
        assert await db_client.update_cronograma(cronograma) is True
        saved_after = await db_client.get_cronograma("cron-1")
        assert saved_after is not None
        assert saved_after.day == "2026-03-15"
        assert await db_client.delete_cronograma("cron-1") is True

    @pytest.mark.asyncio
    async def test_pasaje_crud(self, db_client: DatabaseClient) -> None:
        """Test create/get/update/delete for pasajes."""
        await db_client.create_ciudad(
            Ciudad(
                page_id="city-1",
                name="Buenos Aires",
                created_at="2024-01-01T00:00:00Z",
                updated_at="2024-01-01T00:00:00Z",
            )
        )
        await db_client.create_cronograma(
            Cronograma(
                page_id="cron-1",
                day="2026-03-14",
                ciudad_page_id="city-1",
                created_at="2024-01-01T00:00:00Z",
                updated_at="2024-01-01T00:00:00Z",
            )
        )
        pasaje = Pasaje(
            page_id="pas-1",
            departure="2026-03-14",
            cronograma_page_id="cron-1",
            ciudad_page_id="city-1",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        await db_client.create_pasaje(pasaje)
        saved = await db_client.get_pasaje("pas-1")
        assert saved is not None
        assert saved.departure == "2026-03-14"

        pasaje.departure = "2026-03-16"
        pasaje.updated_at = "2024-01-02T00:00:00Z"
        assert await db_client.update_pasaje(pasaje) is True
        saved_after = await db_client.get_pasaje("pas-1")
        assert saved_after is not None
        assert saved_after.departure == "2026-03-16"
        assert await db_client.delete_pasaje("pas-1") is True

    @pytest.mark.asyncio
    async def test_atraccion_crud(self, db_client: DatabaseClient) -> None:
        """Test create/get/update/delete for atracciones."""
        await db_client.create_ciudad(
            Ciudad(
                page_id="city-1",
                name="Buenos Aires",
                created_at="2024-01-01T00:00:00Z",
                updated_at="2024-01-01T00:00:00Z",
            )
        )
        await db_client.create_cronograma(
            Cronograma(
                page_id="cron-1",
                day="2026-03-14",
                ciudad_page_id="city-1",
                created_at="2024-01-01T00:00:00Z",
                updated_at="2024-01-01T00:00:00Z",
            )
        )
        atraccion = Atraccion(
            page_id="atr-1",
            name="Obelisco",
            fecha="2026-03-14",
            cronograma_page_id="cron-1",
            ciudad_page_id="city-1",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        await db_client.create_atraccion(atraccion)
        saved = await db_client.get_atraccion("atr-1")
        assert saved is not None
        assert saved.name == "Obelisco"

        atraccion.name = "Teatro Colon"
        atraccion.updated_at = "2024-01-02T00:00:00Z"
        assert await db_client.update_atraccion(atraccion) is True
        saved_after = await db_client.get_atraccion("atr-1")
        assert saved_after is not None
        assert saved_after.name == "Teatro Colon"
        assert await db_client.delete_atraccion("atr-1") is True

    @pytest.mark.asyncio
    async def test_create_child_with_missing_fk_fails(
        self, db_client: DatabaseClient
    ) -> None:
        """Test FK enforcement for new CRUD methods."""
        with pytest.raises(Exception):
            await db_client.create_cronograma(
                Cronograma(
                    page_id="cron-missing",
                    day="2026-03-14",
                    ciudad_page_id="missing-city",
                    created_at="2024-01-01T00:00:00Z",
                    updated_at="2024-01-01T00:00:00Z",
                )
            )

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
        with pytest.raises(aiosqlite.IntegrityError) as exc_info:
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

    @pytest.mark.asyncio
    async def test_sync_ciudades_and_children(self, db_client: DatabaseClient) -> None:
        """Test parent-child sync methods preserve FK integrity."""
        ciudades = [
            Ciudad(
                page_id="city-1",
                name="Buenos Aires",
                created_at="2024-01-01T00:00:00Z",
                updated_at="2024-01-01T00:00:00Z",
            )
        ]
        cronograma = [
            Cronograma(
                page_id="cron-1",
                day="2026-03-14",
                ciudad_page_id="city-1",
                created_at="2024-01-01T00:00:00Z",
                updated_at="2024-01-01T00:00:00Z",
            )
        ]
        pasajes = [
            Pasaje(
                page_id="pas-1",
                departure="2026-03-14",
                cronograma_page_id="cron-1",
                ciudad_page_id="city-1",
                created_at="2024-01-01T00:00:00Z",
                updated_at="2024-01-01T00:00:00Z",
            )
        ]
        atracciones = [
            Atraccion(
                page_id="atr-1",
                name="Obelisco",
                fecha="2026-03-14",
                cronograma_page_id="cron-1",
                ciudad_page_id="city-1",
                created_at="2024-01-01T00:00:00Z",
                updated_at="2024-01-01T00:00:00Z",
            )
        ]

        assert await db_client.sync_ciudades_batch(
            ciudades, update_if_changed=False
        ) == (
            1,
            0,
            0,
            0,
        )
        assert await db_client.sync_cronograma_batch(
            cronograma, update_if_changed=False
        ) == (1, 0, 0, 0)
        assert await db_client.sync_pasajes_batch(pasajes, update_if_changed=False) == (
            1,
            0,
            0,
            0,
        )
        assert await db_client.sync_atracciones_batch(
            atracciones, update_if_changed=False
        ) == (1, 0, 0, 0)

        async with db_client.conn.execute("SELECT COUNT(*) FROM ciudades") as cursor:
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == 1
        async with db_client.conn.execute("SELECT COUNT(*) FROM cronograma") as cursor:
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == 1
        async with db_client.conn.execute("SELECT COUNT(*) FROM pasajes") as cursor:
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == 1
        async with db_client.conn.execute("SELECT COUNT(*) FROM atracciones") as cursor:
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == 1

    @pytest.mark.asyncio
    async def test_sync_child_fails_without_parent(
        self, db_client: DatabaseClient
    ) -> None:
        """Test sync reports FK failures when parent rows are missing."""
        cronograma = [
            Cronograma(
                page_id="cron-1",
                day="2026-03-14",
                ciudad_page_id="missing-city",
                created_at="2024-01-01T00:00:00Z",
                updated_at="2024-01-01T00:00:00Z",
            )
        ]
        created, updated, skipped, failed = await db_client.sync_cronograma_batch(
            cronograma, update_if_changed=False
        )
        assert created == 0
        assert updated == 0
        assert skipped == 0
        assert failed == 1

    @pytest.mark.asyncio
    async def test_clear_sync_tables_child_first(
        self, db_client: DatabaseClient
    ) -> None:
        """Test clear_sync_tables deletes rows while respecting dependencies."""
        await db_client.sync_ciudades_batch(
            [
                Ciudad(
                    page_id="city-1",
                    name="Buenos Aires",
                    created_at="2024-01-01T00:00:00Z",
                    updated_at="2024-01-01T00:00:00Z",
                )
            ],
            update_if_changed=False,
        )
        await db_client.sync_cronograma_batch(
            [
                Cronograma(
                    page_id="cron-1",
                    day="2026-03-14",
                    ciudad_page_id="city-1",
                    created_at="2024-01-01T00:00:00Z",
                    updated_at="2024-01-01T00:00:00Z",
                )
            ],
            update_if_changed=False,
        )
        await db_client.sync_pasajes_batch(
            [
                Pasaje(
                    page_id="pas-1",
                    departure="2026-03-14",
                    cronograma_page_id="cron-1",
                    ciudad_page_id="city-1",
                    created_at="2024-01-01T00:00:00Z",
                    updated_at="2024-01-01T00:00:00Z",
                )
            ],
            update_if_changed=False,
        )
        await db_client.sync_atracciones_batch(
            [
                Atraccion(
                    page_id="atr-1",
                    name="Obelisco",
                    fecha="2026-03-14",
                    cronograma_page_id="cron-1",
                    ciudad_page_id="city-1",
                    created_at="2024-01-01T00:00:00Z",
                    updated_at="2024-01-01T00:00:00Z",
                )
            ],
            update_if_changed=False,
        )

        deleted = await db_client.clear_sync_tables(include_gastos=False)
        assert deleted["atracciones"] == 1
        assert deleted["pasajes"] == 1
        assert deleted["cronograma"] == 1
        assert deleted["ciudades"] == 1

    @pytest.mark.asyncio
    async def test_gastos_foreign_key_constraint_exists(
        self, db_client: DatabaseClient
    ) -> None:
        """Test that foreign key constraint exists on gastos.ciudad_page_id."""
        async with db_client.conn.execute("PRAGMA foreign_key_list(gastos)") as cursor:
            fk_rows = await cursor.fetchall()
            # Check that FK constraint exists from ciudad_page_id to ciudades
            has_fk = any(
                fk[3] == "ciudad_page_id" and fk[2] == "ciudades" for fk in fk_rows
            )
            assert has_fk, (
                "Foreign key constraint from gastos.ciudad_page_id "
                "to ciudades.page_id should exist"
            )

    @pytest.mark.asyncio
    async def test_gastos_foreign_key_constraint_enforcement(
        self, db_client: DatabaseClient
    ) -> None:
        """Test that foreign key constraint on gastos.ciudad_page_id is enforced."""
        # Create a ciudad
        ciudad = Ciudad(
            page_id="city-rome",
            name="Rome",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        await db_client.create_ciudad(ciudad)

        # Create a gasto with valid ciudad_page_id - should succeed
        gasto_valid = Gasto(
            page_id="gasto-1",
            payment_method="Cash",
            description="Valid gasto",
            category="Food",
            amount=50.0,
            date="2024-01-01",
            persona="Franco",
            ciudad_page_id="city-rome",
            ciudad="Rome",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        await db_client.create_gasto(gasto_valid)
        retrieved = await db_client.get_gasto("gasto-1")
        assert retrieved is not None
        assert retrieved.ciudad_page_id == "city-rome"

        # Try to create a gasto with invalid ciudad_page_id - should fail
        gasto_invalid = Gasto(
            page_id="gasto-2",
            payment_method="Cash",
            description="Invalid gasto",
            category="Food",
            amount=50.0,
            date="2024-01-01",
            persona="Franco",
            ciudad_page_id="nonexistent-city",
            ciudad="Nonexistent",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        with pytest.raises(Exception) as exc_info:
            await db_client.create_gasto(gasto_invalid)
        # Verify error contains "FOREIGN KEY constraint failed"
        error_str = str(exc_info.value)
        assert "FOREIGN KEY" in error_str or "foreign key" in error_str

    @pytest.mark.asyncio
    async def test_gastos_foreign_key_on_delete_set_null(
        self, db_client: DatabaseClient
    ) -> None:
        """Test that ON DELETE SET NULL works for gastos.ciudad_page_id."""
        # Create a ciudad
        ciudad = Ciudad(
            page_id="city-rome",
            name="Rome",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        await db_client.create_ciudad(ciudad)

        # Create a gasto with ciudad reference
        gasto = Gasto(
            page_id="gasto-1",
            payment_method="Cash",
            description="Gasto in Rome",
            category="Food",
            amount=50.0,
            date="2024-01-01",
            persona="Franco",
            ciudad_page_id="city-rome",
            ciudad="Rome",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        await db_client.create_gasto(gasto)

        # Verify gasto has ciudad reference
        retrieved = await db_client.get_gasto("gasto-1")
        assert retrieved is not None
        assert retrieved.ciudad_page_id == "city-rome"
        assert retrieved.ciudad == "Rome"

        # Delete the ciudad
        await db_client.delete_ciudad("city-rome")

        # Verify gasto's ciudad_page_id is now NULL (ON DELETE SET NULL)
        retrieved_after = await db_client.get_gasto("gasto-1")
        assert retrieved_after is not None
        assert retrieved_after.ciudad_page_id is None
        # Note: ciudad field is not automatically updated,
        # it's just the FK that's set to NULL

    @pytest.mark.asyncio
    async def test_gastos_schema_migration_adds_fk_constraint(
        self, settings: Settings
    ) -> None:
        """Test that schema migration adds FK constraint to existing gastos table."""
        # Create a database with old schema (without FK constraint)
        async with aiosqlite.connect(settings.database_path) as conn:
            await conn.execute("PRAGMA foreign_keys = ON")
            # Create ciudades table
            await conn.execute("""
                CREATE TABLE ciudades (
                    page_id TEXT PRIMARY KEY,
                    name TEXT,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
            """)
            # Create gastos table without FK constraint
            await conn.execute("""
                CREATE TABLE gastos (
                    page_id TEXT PRIMARY KEY,
                    payment_method TEXT,
                    description TEXT,
                    category TEXT,
                    amount REAL,
                    date DATE,
                    persona TEXT,
                    ciudad_page_id TEXT,
                    ciudad TEXT,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
            """)
            # Insert a test row
            await conn.execute(
                """INSERT INTO gastos (
                    page_id, payment_method, description, category, amount, date,
                    persona, ciudad_page_id, ciudad, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    "old-gasto-1",
                    "Cash",
                    "Old expense",
                    "Food",
                    100.0,
                    "2024-01-01",
                    "Franco",
                    None,
                    None,
                    "2024-01-01T00:00:00Z",
                    "2024-01-01T00:00:00Z",
                ),
            )
            await conn.commit()

            # Verify FK constraint doesn't exist
            async with conn.execute("PRAGMA foreign_key_list(gastos)") as cursor:
                rows = await cursor.fetchall()
                assert len(rows) == 0

        # Initialize database client (should run migration and add FK constraint)
        async with DatabaseClient(settings) as client:
            # Verify the old row still exists
            gasto = await client.get_gasto("old-gasto-1")
            assert gasto is not None
            assert gasto.page_id == "old-gasto-1"
            assert gasto.ciudad_page_id is None

            # Verify FK constraint now exists
            async with client.conn.execute("PRAGMA foreign_key_list(gastos)") as cursor:
                fk_rows = await cursor.fetchall()
                has_fk = any(
                    fk[3] == "ciudad_page_id" and fk[2] == "ciudades" for fk in fk_rows
                )
                assert has_fk, "Migration should add FK constraint"

            # Create a ciudad and verify we can now create gastos with valid references
            ciudad = Ciudad(
                page_id="city-madrid",
                name="Madrid",
                created_at="2024-01-02T00:00:00Z",
                updated_at="2024-01-02T00:00:00Z",
            )
            await client.create_ciudad(ciudad)

            new_gasto = Gasto(
                page_id="new-gasto-1",
                payment_method="Credit Card",
                description="New expense",
                category="Transport",
                amount=50.0,
                date="2024-01-02",
                persona="Mica",
                ciudad_page_id="city-madrid",
                ciudad="Madrid",
                created_at="2024-01-02T00:00:00Z",
                updated_at="2024-01-02T00:00:00Z",
            )
            await client.create_gasto(new_gasto)

            retrieved = await client.get_gasto("new-gasto-1")
            assert retrieved is not None
            assert retrieved.ciudad_page_id == "city-madrid"
            assert retrieved.ciudad == "Madrid"

    @pytest.mark.asyncio
    async def test_gastos_migration_handles_orphan_city_references(
        self, settings: Settings
    ) -> None:
        """Test that migration gracefully handles orphan ciudad_page_id values."""
        # Create a database with old schema and orphan city references
        async with aiosqlite.connect(settings.database_path) as conn:
            await conn.execute("PRAGMA foreign_keys = ON")
            # Create ciudades table
            await conn.execute(
                """
                CREATE TABLE ciudades (
                    page_id TEXT PRIMARY KEY,
                    name TEXT,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
            """
            )
            # Create gastos table without FK constraint
            await conn.execute(
                """
                CREATE TABLE gastos (
                    page_id TEXT PRIMARY KEY,
                    payment_method TEXT,
                    description TEXT,
                    category TEXT,
                    amount REAL,
                    date DATE,
                    persona TEXT,
                    ciudad_page_id TEXT,
                    ciudad TEXT,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
            """
            )
            # Insert a gasto with orphan ciudad_page_id (non-existent city)
            await conn.execute(
                """INSERT INTO gastos (
                    page_id, payment_method, description, category, amount, date,
                    persona, ciudad_page_id, ciudad, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    "orphan-gasto-1",
                    "Cash",
                    "Orphan expense",
                    "Food",
                    100.0,
                    "2024-01-01",
                    "Franco",
                    "nonexistent-city-id",  # This city doesn't exist in ciudades table
                    "Ghost City",
                    "2024-01-01T00:00:00Z",
                    "2024-01-01T00:00:00Z",
                ),
            )
            # Insert another gasto with valid NULL ciudad_page_id
            await conn.execute(
                """INSERT INTO gastos (
                    page_id, payment_method, description, category, amount, date,
                    persona, ciudad_page_id, ciudad, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    "null-city-gasto",
                    "Credit Card",
                    "No city expense",
                    "Transport",
                    50.0,
                    "2024-01-02",
                    "Mica",
                    None,
                    None,
                    "2024-01-02T00:00:00Z",
                    "2024-01-02T00:00:00Z",
                ),
            )
            await conn.commit()

        # Initialize database client (should run migration without FK constraint errors)
        async with DatabaseClient(settings) as client:
            # Verify migration succeeded and orphan reference was nulled
            orphan_gasto = await client.get_gasto("orphan-gasto-1")
            assert orphan_gasto is not None
            assert orphan_gasto.page_id == "orphan-gasto-1"
            # The orphan ciudad_page_id should have been nulled during migration
            assert orphan_gasto.ciudad_page_id is None

            # Verify the gasto with original NULL ciudad_page_id is unchanged
            null_gasto = await client.get_gasto("null-city-gasto")
            assert null_gasto is not None
            assert null_gasto.page_id == "null-city-gasto"
            assert null_gasto.ciudad_page_id is None

            # Verify FK constraint now exists and is enforced
            async with client.conn.execute("PRAGMA foreign_key_list(gastos)") as cursor:
                fk_rows = await cursor.fetchall()
                has_fk = any(
                    fk[3] == "ciudad_page_id" and fk[2] == "ciudades" for fk in fk_rows
                )
                assert has_fk, "Migration should add FK constraint"

            # Create a ciudad and verify we can create gastos with valid references
            ciudad = Ciudad(
                page_id="city-barcelona",
                name="Barcelona",
                created_at="2024-01-03T00:00:00Z",
                updated_at="2024-01-03T00:00:00Z",
            )
            await client.create_ciudad(ciudad)

            new_gasto = Gasto(
                page_id="new-gasto-2",
                payment_method="Debit Card",
                description="New expense with valid city",
                category="Food",
                amount=75.0,
                date="2024-01-03",
                persona="Franco",
                ciudad_page_id="city-barcelona",
                ciudad="Barcelona",
                created_at="2024-01-03T00:00:00Z",
                updated_at="2024-01-03T00:00:00Z",
            )
            await client.create_gasto(new_gasto)

            retrieved = await client.get_gasto("new-gasto-2")
            assert retrieved is not None
            assert retrieved.ciudad_page_id == "city-barcelona"

    async def test_gastos_migration_handles_stale_gastos_new_table(
        self, settings: Settings
    ) -> None:
        """Test that migration handles stale gastos_new table from prior failed run."""
        # Create a database with old schema and a stale gastos_new table
        async with aiosqlite.connect(settings.database_path) as conn:
            await conn.execute("PRAGMA foreign_keys = ON")
            # Create ciudades table
            await conn.execute(
                """
                CREATE TABLE ciudades (
                    page_id TEXT PRIMARY KEY,
                    name TEXT,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
            """
            )
            # Create gastos table without FK constraint
            await conn.execute(
                """
                CREATE TABLE gastos (
                    page_id TEXT PRIMARY KEY,
                    payment_method TEXT,
                    description TEXT,
                    category TEXT,
                    amount REAL,
                    date DATE,
                    persona TEXT,
                    ciudad_page_id TEXT,
                    ciudad TEXT,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
            """
            )
            # Create a stale gastos_new table (simulating prior failed migration)
            await conn.execute(
                """
                CREATE TABLE gastos_new (
                    page_id TEXT PRIMARY KEY,
                    payment_method TEXT,
                    description TEXT,
                    category TEXT,
                    amount REAL,
                    date DATE,
                    persona TEXT,
                    ciudad_page_id TEXT,
                    ciudad TEXT,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
            """
            )
            # Insert a gasto in the old table
            await conn.execute(
                """INSERT INTO gastos (
                    page_id, payment_method, description, category, amount, date,
                    persona, ciudad_page_id, ciudad, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    "test-gasto-1",
                    "Cash",
                    "Test expense",
                    "Food",
                    100.0,
                    "2024-01-01",
                    "Franco",
                    None,
                    None,
                    "2024-01-01T00:00:00Z",
                    "2024-01-01T00:00:00Z",
                ),
            )
            await conn.commit()

        # Initialize database client (should handle stale table without errors)
        async with DatabaseClient(settings) as client:
            # Verify migration succeeded
            gasto = await client.get_gasto("test-gasto-1")
            assert gasto is not None
            assert gasto.page_id == "test-gasto-1"

            # Verify FK constraint now exists and is enforced
            async with client.conn.execute("PRAGMA foreign_key_list(gastos)") as cursor:
                fk_rows = await cursor.fetchall()
                has_fk = any(
                    fk[3] == "ciudad_page_id" and fk[2] == "ciudades" for fk in fk_rows
                )
                assert has_fk, "Migration should add FK constraint"

            # Verify the stale gastos_new table no longer exists
            async with client.conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name='gastos_new'"
            ) as cursor:
                stale_table = await cursor.fetchone()
                assert stale_table is None, "Stale gastos_new table should be dropped"

            # Create a ciudad and verify we can create gastos with valid references
            ciudad = Ciudad(
                page_id="city-madrid",
                name="Madrid",
                created_at="2024-01-02T00:00:00Z",
                updated_at="2024-01-02T00:00:00Z",
            )
            await client.create_ciudad(ciudad)

            new_gasto = Gasto(
                page_id="new-gasto-3",
                payment_method="Credit Card",
                description="New expense with valid city",
                category="Transport",
                amount=50.0,
                date="2024-01-02",
                persona="Mica",
                ciudad_page_id="city-madrid",
                ciudad="Madrid",
                created_at="2024-01-02T00:00:00Z",
                updated_at="2024-01-02T00:00:00Z",
            )
            await client.create_gasto(new_gasto)

            retrieved = await client.get_gasto("new-gasto-3")
            assert retrieved is not None
            assert retrieved.ciudad_page_id == "city-madrid"
