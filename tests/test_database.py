from __future__ import annotations

import os
import tempfile
from collections.abc import AsyncGenerator, Generator
from pathlib import Path

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
async def db_client(settings: Settings) -> AsyncGenerator[DatabaseClient, None]:
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
    async def test_get_gastos_totals_empty(self, db_client: DatabaseClient) -> None:
        """Test totals when database is empty returns zero-safe defaults."""
        total, count, min_val, max_val, avg_val = await db_client.get_gastos_totals()
        assert total == 0.0
        assert count == 0
        assert min_val == 0.0
        assert max_val == 0.0
        assert avg_val == 0.0

    @pytest.mark.asyncio
    async def test_get_gastos_totals_with_data(self, db_client: DatabaseClient) -> None:
        """Test totals with actual data."""
        # Create test gastos
        gastos = [
            Gasto(
                page_id="total-test-1",
                payment_method="credit_card",
                description="Lunch",
                category="Food",
                amount=50.0,
                date="2026-01-10",
                persona="John",
                ciudad_page_id=None,
                ciudad=None,
                created_at="2026-01-10T12:00:00Z",
                updated_at="2026-01-10T12:00:00Z",
            ),
            Gasto(
                page_id="total-test-2",
                payment_method="cash",
                description="Coffee",
                category="Food",
                amount=5.0,
                date="2026-01-11",
                persona="Jane",
                ciudad_page_id=None,
                ciudad=None,
                created_at="2026-01-11T08:00:00Z",
                updated_at="2026-01-11T08:00:00Z",
            ),
            Gasto(
                page_id="total-test-3",
                payment_method="debit_card",
                description="Dinner",
                category="Food",
                amount=45.0,
                date="2026-01-12",
                persona="John",
                ciudad_page_id=None,
                ciudad=None,
                created_at="2026-01-12T19:00:00Z",
                updated_at="2026-01-12T19:00:00Z",
            ),
        ]

        for gasto in gastos:
            await db_client.create_gasto(gasto)

        total, count, min_val, max_val, avg_val = await db_client.get_gastos_totals()
        assert total == 100.0  # 50 + 5 + 45
        assert count == 3
        assert min_val == 5.0
        assert max_val == 50.0
        assert avg_val == pytest.approx(33.33, rel=0.01)

    @pytest.mark.asyncio
    async def test_get_gastos_totals_with_filters(
        self, db_client: DatabaseClient
    ) -> None:
        """Test totals with all filter types."""
        # Create test gastos with different values
        gastos = [
            Gasto(
                page_id="filter-test-1",
                payment_method="credit_card",
                description="Restaurant lunch",
                category="Food",
                amount=60.0,
                date="2026-01-15",
                persona="John",
                ciudad_page_id=None,
                ciudad="Rome",
                created_at="2026-01-15T12:00:00Z",
                updated_at="2026-01-15T12:00:00Z",
            ),
            Gasto(
                page_id="filter-test-2",
                payment_method="cash",
                description="Coffee shop",
                category="Food, Drinks",
                amount=8.0,
                date="2026-01-16",
                persona="Jane",
                ciudad_page_id=None,
                ciudad="Paris",
                created_at="2026-01-16T09:00:00Z",
                updated_at="2026-01-16T09:00:00Z",
            ),
            Gasto(
                page_id="filter-test-3",
                payment_method="credit_card",
                description="Uber ride",
                category="Transport",
                amount=25.0,
                date="2026-01-20",
                persona="John",
                ciudad_page_id=None,
                ciudad="Rome",
                created_at="2026-01-20T14:00:00Z",
                updated_at="2026-01-20T14:00:00Z",
            ),
        ]

        for gasto in gastos:
            await db_client.create_gasto(gasto)

        # Test filtering by persona
        total, count, _, _, _ = await db_client.get_gastos_totals(persona="John")
        assert count == 2  # filter-test-1 and filter-test-3
        assert total == 85.0  # 60 + 25

        # Test filtering by payment method
        total, count, _, _, _ = await db_client.get_gastos_totals(
            payment_method="credit_card"
        )
        assert count == 2  # filter-test-1 and filter-test-3
        assert total == 85.0

        # Test filtering by category (contains)
        total, count, _, _, _ = await db_client.get_gastos_totals(category="Food")
        assert (
            count == 2
        )  # filter-test-1 and filter-test-2 (both have Food in category)

        # Test filtering by ciudad
        total, count, _, _, _ = await db_client.get_gastos_totals(ciudad="Rome")
        assert count == 2  # filter-test-1 and filter-test-3

        # Test date range filter
        total, count, _, _, _ = await db_client.get_gastos_totals(
            date_from="2026-01-15", date_to="2026-01-16"
        )
        assert count == 2  # filter-test-1 and filter-test-2

        # Test amount range filter
        total, count, _, _, _ = await db_client.get_gastos_totals(
            amount_min=10.0, amount_max=50.0
        )
        # Only filter-test-3 (25.0) is in range [10, 50]
        assert count == 1

    @pytest.mark.asyncio
    async def test_get_gastos_totals_combined_filters(
        self, db_client: DatabaseClient
    ) -> None:
        """Test that all filters combine correctly."""
        # Note: FTS (q parameter) is tested separately in
        # test_get_gastos_totals_fts_with_structured_filters
        gastos = [
            Gasto(
                page_id="combine-test-1",
                payment_method="credit_card",
                description="Restaurant dinner",
                category="Food",
                amount=80.0,
                date="2026-01-10",
                persona="John",
                ciudad_page_id=None,
                ciudad="Rome",
                created_at="2026-01-10T19:00:00Z",
                updated_at="2026-01-10T19:00:00Z",
            ),
            Gasto(
                page_id="combine-test-2",
                payment_method="credit_card",
                description="Restaurant lunch",
                category="Food",
                amount=40.0,
                date="2026-01-11",
                persona="Jane",
                ciudad_page_id=None,
                ciudad="Rome",
                created_at="2026-01-11T12:00:00Z",
                updated_at="2026-01-11T12:00:00Z",
            ),
            Gasto(
                page_id="combine-test-3",
                payment_method="cash",
                description="Restaurant breakfast",
                category="Food",
                amount=20.0,
                date="2026-01-12",
                persona="John",
                ciudad_page_id=None,
                ciudad="Paris",
                created_at="2026-01-12T08:00:00Z",
                updated_at="2026-01-12T08:00:00Z",
            ),
        ]

        for gasto in gastos:
            await db_client.create_gasto(gasto)

        # Test combining multiple filters
        total, count, _, _, _ = await db_client.get_gastos_totals(
            persona="John",
            payment_method="credit_card",
        )
        assert count == 1  # only combine-test-1 matches both
        assert total == 80.0

        # Test combining persona + ciudad + date range
        total, count, _, _, _ = await db_client.get_gastos_totals(
            persona="John",
            ciudad="Rome",
            date_from="2026-01-10",
            date_to="2026-01-10",
        )
        assert count == 1  # only combine-test-1 matches all
        assert total == 80.0

        # Test combining payment_method + category + amount range
        total, count, _, _, _ = await db_client.get_gastos_totals(
            payment_method="credit_card",
            category="Food",
            amount_min=50.0,
        )
        assert count == 1  # only combine-test-1 (80) >= 50 with credit_card and Food
        assert total == 80.0

    @pytest.mark.asyncio
    async def test_get_gastos_totals_fts_with_structured_filters(
        self, db_client: DatabaseClient
    ) -> None:
        """Test FTS (q param) combines with structured filters using AND."""
        gastos = [
            Gasto(
                page_id="fts-test-1",
                payment_method="credit_card",
                description="Restaurant dinner in Rome",
                category="Food",
                amount=80.0,
                date="2026-01-10",
                persona="John",
                ciudad_page_id=None,
                ciudad="Rome",
                created_at="2026-01-10T19:00:00Z",
                updated_at="2026-01-10T19:00:00Z",
            ),
            Gasto(
                page_id="fts-test-2",
                payment_method="credit_card",
                description="Restaurant lunch in Rome",
                category="Food",
                amount=40.0,
                date="2026-01-11",
                persona="Jane",
                ciudad_page_id=None,
                ciudad="Rome",
                created_at="2026-01-11T12:00:00Z",
                updated_at="2026-01-11T12:00:00Z",
            ),
            Gasto(
                page_id="fts-test-3",
                payment_method="cash",
                description="Restaurant breakfast in Paris",
                category="Food",
                amount=20.0,
                date="2026-01-12",
                persona="John",
                ciudad_page_id=None,
                ciudad="Paris",
                created_at="2026-01-12T08:00:00Z",
                updated_at="2026-01-12T08:00:00Z",
            ),
            Gasto(
                page_id="fts-test-4",
                payment_method="cash",
                description="Coffee shop",
                category="Drinks",
                amount=5.0,
                date="2026-01-13",
                persona="John",
                ciudad_page_id=None,
                ciudad="Rome",
                created_at="2026-01-13T10:00:00Z",
                updated_at="2026-01-13T10:00:00Z",
            ),
        ]

        for gasto in gastos:
            await db_client.create_gasto(gasto)

        # Test 1: FTS only - all "Restaurant" entries
        total, count, _, _, _ = await db_client.get_gastos_totals(q="Restaurant")
        assert count == 3  # fts-test-1, fts-test-2, fts-test-3
        assert total == 140.0  # 80 + 40 + 20

        # Test 2: FTS + persona filter (AND semantics)
        total, count, _, _, _ = await db_client.get_gastos_totals(
            q="Restaurant", persona="John"
        )
        assert count == 2  # fts-test-1 (John, Rome) and fts-test-3 (John, Paris)
        assert total == 100.0  # 80 + 20

        # Test 3: FTS + ciudad filter (AND semantics)
        total, count, _, _, _ = await db_client.get_gastos_totals(
            q="Restaurant", ciudad="Rome"
        )
        assert count == 2  # fts-test-1 and fts-test-2 (both Restaurant and Rome)
        assert total == 120.0  # 80 + 40

        # Test 4: FTS + persona + ciudad + payment_method (multiple AND)
        total, count, _, _, _ = await db_client.get_gastos_totals(
            q="Restaurant",
            persona="John",
            ciudad="Rome",
            payment_method="credit_card",
        )
        assert count == 1  # only fts-test-1 matches all criteria
        assert total == 80.0

        # Test 5: FTS + category filter
        total, count, _, _, _ = await db_client.get_gastos_totals(
            q="Restaurant", category="Food"
        )
        assert count == 3  # All Restaurant entries are Food
        assert total == 140.0

        # Test 6: FTS with no matches
        total, count, _, _, _ = await db_client.get_gastos_totals(q="Nonexistent")
        assert count == 0
        assert total == 0.0

        # Test 7: FTS + filters with no matches
        total, count, _, _, _ = await db_client.get_gastos_totals(
            q="Restaurant", ciudad="Berlin"
        )
        assert count == 0  # No Restaurant entries in Berlin
        assert total == 0.0

    @pytest.mark.asyncio
    async def test_fts_update_removes_old_terms(
        self, db_client: DatabaseClient
    ) -> None:
        """Test that updating a gasto removes old FTS terms and adds new ones."""
        # Insert a gasto with specific terms
        gasto = Gasto(
            page_id="fts-update-test",
            payment_method="credit_card",
            description="Restaurant dinner",
            category="Food",
            amount=80.0,
            date="2026-01-10",
            persona="John",
            ciudad_page_id=None,
            ciudad="Rome",
            created_at="2026-01-10T19:00:00Z",
            updated_at="2026-01-10T19:00:00Z",
        )
        await db_client.create_gasto(gasto)

        # Verify initial FTS indexing
        total, count, _, _, _ = await db_client.get_gastos_totals(q="Restaurant")
        assert count == 1
        assert total == 80.0

        # Update the gasto to change description
        gasto.description = "Coffee shop morning"
        gasto.category = "Drinks"
        gasto.persona = "Jane"
        await db_client.update_gasto(gasto)

        # Old terms should no longer match
        total, count, _, _, _ = await db_client.get_gastos_totals(q="Restaurant")
        assert count == 0
        assert total == 0.0

        total, count, _, _, _ = await db_client.get_gastos_totals(q="Food")
        assert count == 0
        assert total == 0.0

        total, count, _, _, _ = await db_client.get_gastos_totals(q="John")
        assert count == 0
        assert total == 0.0

        # New terms should match
        total, count, _, _, _ = await db_client.get_gastos_totals(q="Coffee")
        assert count == 1
        assert total == 80.0

        total, count, _, _, _ = await db_client.get_gastos_totals(q="Drinks")
        assert count == 1
        assert total == 80.0

        total, count, _, _, _ = await db_client.get_gastos_totals(q="Jane")
        assert count == 1
        assert total == 80.0

    @pytest.mark.asyncio
    async def test_fts_delete_no_stale_terms_on_rowid_reuse(
        self, db_client: DatabaseClient
    ) -> None:
        """Test that deleting a gasto removes FTS terms, even with rowid reuse."""
        # Insert a gasto with specific terms
        gasto1 = Gasto(
            page_id="fts-delete-test-1",
            payment_method="credit_card",
            description="Restaurant dinner",
            category="Food",
            amount=80.0,
            date="2026-01-10",
            persona="John",
            ciudad_page_id=None,
            ciudad="Rome",
            created_at="2026-01-10T19:00:00Z",
            updated_at="2026-01-10T19:00:00Z",
        )
        await db_client.create_gasto(gasto1)

        # Verify initial FTS indexing
        total, count, _, _, _ = await db_client.get_gastos_totals(q="Restaurant")
        assert count == 1
        assert total == 80.0

        # Delete the gasto
        await db_client.delete_gasto("fts-delete-test-1")

        # Verify FTS terms are removed
        total, count, _, _, _ = await db_client.get_gastos_totals(q="Restaurant")
        assert count == 0
        assert total == 0.0

        # Insert a new gasto (may reuse rowid in SQLite)
        gasto2 = Gasto(
            page_id="fts-delete-test-2",
            payment_method="cash",
            description="Coffee shop",
            category="Drinks",
            amount=5.0,
            date="2026-01-11",
            persona="Jane",
            ciudad_page_id=None,
            ciudad="Paris",
            created_at="2026-01-11T10:00:00Z",
            updated_at="2026-01-11T10:00:00Z",
        )
        await db_client.create_gasto(gasto2)

        # Old terms should still not match (no stale terms from rowid reuse)
        total, count, _, _, _ = await db_client.get_gastos_totals(q="Restaurant")
        assert count == 0
        assert total == 0.0

        total, count, _, _, _ = await db_client.get_gastos_totals(q="Food")
        assert count == 0
        assert total == 0.0

        # New terms should match correctly
        total, count, _, _, _ = await db_client.get_gastos_totals(q="Coffee")
        assert count == 1
        assert total == 5.0

    @pytest.mark.asyncio
    async def test_fts_backfill_legacy_rows(self, tmp_path: Path) -> None:
        """Test that FTS is rebuilt for pre-existing gastos rows on initialization."""
        # Create a database and insert gastos BEFORE FTS is set up
        db_path = tmp_path / "test_fts_backfill.db"

        # Create database without initializing FTS
        conn = await aiosqlite.connect(str(db_path))
        await conn.execute("""
            CREATE TABLE gastos (
                page_id TEXT PRIMARY KEY,
                payment_method TEXT NOT NULL,
                description TEXT NOT NULL,
                category TEXT NOT NULL,
                amount REAL NOT NULL,
                date TEXT NOT NULL,
                persona TEXT NOT NULL,
                ciudad_page_id TEXT,
                ciudad TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        # Insert legacy gastos directly (simulating pre-FTS state)
        await conn.execute("""
            INSERT INTO gastos VALUES (
                'legacy-1', 'credit_card', 'Restaurant dinner', 'Food', 80.0,
                '2026-01-10', 'John', NULL, 'Rome',
                '2026-01-10T19:00:00Z', '2026-01-10T19:00:00Z'
            )
        """)
        await conn.execute("""
            INSERT INTO gastos VALUES (
                'legacy-2', 'cash', 'Coffee shop', 'Drinks', 5.0,
                '2026-01-11', 'Jane', NULL, 'Paris',
                '2026-01-11T10:00:00Z', '2026-01-11T10:00:00Z'
            )
        """)
        await conn.commit()
        await conn.close()

        # Now initialize DatabaseClient with Settings, which should create
        # FTS and backfill
        settings = Settings(
            webhook_secret_key="test-secret-key",
            notion_api_token="secret_test_token",
            atracciones_database_id="test-atracciones-db-id",
            ciudades_database_id="test-ciudades-db-id",
            cronograma_database_id="test-cronograma-db-id",
            gastos_database_id="test-gastos-db-id",
            pasajes_database_id="test-pasajes-db-id",
            database_path=str(db_path),
            debug=True,
            max_retries=2,
            retry_delay=0.01,
        )
        client = DatabaseClient(settings)
        await client.initialize()

        # Verify FTS works for legacy rows (backfill succeeded)
        total, count, _, _, _ = await client.get_gastos_totals(q="Restaurant")
        assert count == 1
        assert total == 80.0

        total, count, _, _, _ = await client.get_gastos_totals(q="Coffee")
        assert count == 1
        assert total == 5.0

        # Verify combined filters work with backfilled data
        total, count, _, _, _ = await client.get_gastos_totals(
            q="Restaurant", persona="John"
        )
        assert count == 1
        assert total == 80.0

        await client.close()

    @pytest.mark.asyncio
    async def test_fts_rebuild_on_schema_upgrade(self, tmp_path: Path) -> None:
        """Test that FTS is rebuilt when upgrading from stale index.

        Simulates upgrading from an existing FTS table with stale terms
        caused by old buggy triggers. The DatabaseClient should detect
        the schema version mismatch and rebuild the index.
        """
        # Create a database with FTS and OLD buggy triggers
        db_path = tmp_path / "test_fts_upgrade.db"
        conn = await aiosqlite.connect(str(db_path))

        # Create gastos table
        await conn.execute("""
            CREATE TABLE gastos (
                page_id TEXT PRIMARY KEY,
                payment_method TEXT NOT NULL,
                description TEXT NOT NULL,
                category TEXT NOT NULL,
                amount REAL NOT NULL,
                date TEXT NOT NULL,
                persona TEXT NOT NULL,
                ciudad_page_id TEXT,
                ciudad TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        # Create FTS5 virtual table
        await conn.execute("""
            CREATE VIRTUAL TABLE gastos_fts USING fts5(
                description,
                category,
                persona,
                content='gastos',
                content_rowid='rowid'
            )
        """)

        # Create OLD buggy triggers (using incorrect UPDATE/DELETE pattern)
        await conn.execute("""
            CREATE TRIGGER gastos_fts_insert
            AFTER INSERT ON gastos
            BEGIN
                INSERT INTO gastos_fts(rowid, description, category, persona)
                VALUES (NEW.rowid, NEW.description, NEW.category, NEW.persona);
            END
        """)

        # OLD buggy UPDATE trigger (uses UPDATE instead of 'delete' sentinel)
        await conn.execute("""
            CREATE TRIGGER gastos_fts_update
            AFTER UPDATE ON gastos
            BEGIN
                UPDATE gastos_fts
                SET description = NEW.description,
                    category = NEW.category,
                    persona = NEW.persona
                WHERE rowid = NEW.rowid;
            END
        """)

        # OLD buggy DELETE trigger (uses DELETE instead of 'delete' sentinel)
        await conn.execute("""
            CREATE TRIGGER gastos_fts_delete
            AFTER DELETE ON gastos
            BEGIN
                DELETE FROM gastos_fts WHERE rowid = OLD.rowid;
            END
        """)

        # Set user_version to 0 (simulating pre-fix version)
        await conn.execute("PRAGMA user_version = 0")

        # Insert initial gasto
        await conn.execute("""
            INSERT INTO gastos VALUES (
                'upgrade-1', 'credit_card', 'Restaurant dinner', 'Food', 80.0,
                '2026-01-10', 'John', NULL, 'Rome',
                '2026-01-10T19:00:00Z', '2026-01-10T19:00:00Z'
            )
        """)

        # Update the gasto (this will cause stale term with buggy trigger)
        await conn.execute("""
            UPDATE gastos
            SET description = 'Coffee shop', category = 'Drinks', amount = 5.0
            WHERE page_id = 'upgrade-1'
        """)

        await conn.commit()
        await conn.close()

        # Now initialize DatabaseClient, which should detect schema version
        # mismatch and rebuild FTS index
        settings = Settings(
            webhook_secret_key="test-secret-key",
            notion_api_token="secret_test_token",
            atracciones_database_id="test-atracciones-db-id",
            ciudades_database_id="test-ciudades-db-id",
            cronograma_database_id="test-cronograma-db-id",
            gastos_database_id="test-gastos-db-id",
            pasajes_database_id="test-pasajes-db-id",
            database_path=str(db_path),
            debug=True,
            max_retries=2,
            retry_delay=0.01,
        )
        client = DatabaseClient(settings)
        await client.initialize()

        # Verify FTS was rebuilt - old stale term should NOT match
        total, count, _, _, _ = await client.get_gastos_totals(q="Restaurant")
        assert count == 0  # Stale term removed
        assert total == 0.0

        # New term should match
        total, count, _, _, _ = await client.get_gastos_totals(q="Coffee")
        assert count == 1
        assert total == 5.0

        # Verify combined filters work with rebuilt index
        total, count, _, _, _ = await client.get_gastos_totals(
            q="Coffee", persona="John"
        )
        assert count == 1
        assert total == 5.0

        # Verify FTS schema version was updated in metadata table
        conn = await aiosqlite.connect(str(db_path))
        cursor = await conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'fts_version'"
        )
        row = await cursor.fetchone()
        assert row is not None, "FTS version should be set in schema_meta"
        assert row[0] == 1  # FTS_SCHEMA_VERSION
        await conn.close()

        await client.close()

    @pytest.mark.asyncio
    async def test_get_gastos_totals_negative_values(
        self, db_client: DatabaseClient
    ) -> None:
        """Test totals with negative amounts (refunds/adjustments)."""
        gastos = [
            Gasto(
                page_id="negative-test-1",
                payment_method="credit_card",
                description="Purchase",
                category="Shopping",
                amount=100.0,
                date="2026-01-10",
                persona="John",
                ciudad_page_id=None,
                ciudad=None,
                created_at="2026-01-10T12:00:00Z",
                updated_at="2026-01-10T12:00:00Z",
            ),
            Gasto(
                page_id="negative-test-2",
                payment_method="credit_card",
                description="Refund",
                category="Shopping",
                amount=-30.0,
                date="2026-01-11",
                persona="John",
                ciudad_page_id=None,
                ciudad=None,
                created_at="2026-01-11T12:00:00Z",
                updated_at="2026-01-11T12:00:00Z",
            ),
        ]

        for gasto in gastos:
            await db_client.create_gasto(gasto)

        total, count, min_val, max_val, avg_val = await db_client.get_gastos_totals()
        assert total == 70.0  # 100 - 30
        assert count == 2
        assert min_val == -30.0
        assert max_val == 100.0
        assert avg_val == 35.0  # (100 - 30) / 2

    # ========================================
    # Summary Tests (Step 5: Single-dimension)
    # ========================================

    @pytest.mark.asyncio
    async def test_get_gastos_summary_empty(self, db_client: DatabaseClient) -> None:
        """Test summary when database is empty returns zero-safe defaults."""
        groups, grand_total, total_count = await db_client.get_gastos_summary(
            group_by=["category"]
        )
        assert groups == []
        assert grand_total == 0.0
        assert total_count == 0

    @pytest.mark.asyncio
    async def test_get_gastos_summary_no_group_by(
        self, db_client: DatabaseClient
    ) -> None:
        """Test summary without group_by returns empty groups with totals."""
        # Insert test data
        gasto = Gasto(
            page_id="summary-no-group",
            payment_method="credit_card",
            description="Test expense",
            category="Food",
            amount=100.0,
            date="2026-01-15",
            persona="John",
            ciudad_page_id=None,
            ciudad="Rome",
            created_at="2026-01-15T12:00:00Z",
            updated_at="2026-01-15T12:00:00Z",
        )
        await db_client.create_gasto(gasto)

        groups, grand_total, total_count = await db_client.get_gastos_summary(
            group_by=[]
        )
        assert groups == []
        assert grand_total == 100.0
        assert total_count == 1

    @pytest.mark.asyncio
    async def test_get_gastos_summary_by_category(
        self, db_client: DatabaseClient
    ) -> None:
        """Test summary grouped by category."""
        gastos = [
            Gasto(
                page_id="summary-cat-1",
                payment_method="credit_card",
                description="Lunch",
                category="Food",
                amount=50.0,
                date="2026-01-10",
                persona="John",
                ciudad_page_id=None,
                ciudad="Rome",
                created_at="2026-01-10T12:00:00Z",
                updated_at="2026-01-10T12:00:00Z",
            ),
            Gasto(
                page_id="summary-cat-2",
                payment_method="cash",
                description="Dinner",
                category="Food",
                amount=75.0,
                date="2026-01-11",
                persona="Jane",
                ciudad_page_id=None,
                ciudad="Rome",
                created_at="2026-01-11T19:00:00Z",
                updated_at="2026-01-11T19:00:00Z",
            ),
            Gasto(
                page_id="summary-cat-3",
                payment_method="debit_card",
                description="Uber",
                category="Transport",
                amount=25.0,
                date="2026-01-12",
                persona="John",
                ciudad_page_id=None,
                ciudad="Madrid",
                created_at="2026-01-12T14:00:00Z",
                updated_at="2026-01-12T14:00:00Z",
            ),
        ]

        for gasto in gastos:
            await db_client.create_gasto(gasto)

        groups, grand_total, total_count = await db_client.get_gastos_summary(
            group_by=["category"]
        )

        assert len(groups) == 2
        assert grand_total == 150.0  # 50 + 75 + 25
        assert total_count == 3

        # Find Food group (sorted by total DESC, so Food should be first)
        food_group = next((g for g in groups if g["key"]["category"] == "Food"), None)
        assert food_group is not None
        assert food_group["total"] == 125.0  # 50 + 75
        assert food_group["count"] == 2

        # Find Transport group
        transport_group = next(
            (g for g in groups if g["key"]["category"] == "Transport"), None
        )
        assert transport_group is not None
        assert transport_group["total"] == 25.0
        assert transport_group["count"] == 1

    @pytest.mark.asyncio
    async def test_get_gastos_summary_by_persona(
        self, db_client: DatabaseClient
    ) -> None:
        """Test summary grouped by persona."""
        gastos = [
            Gasto(
                page_id="summary-per-1",
                payment_method="credit_card",
                description="Lunch",
                category="Food",
                amount=60.0,
                date="2026-01-10",
                persona="Franco",
                ciudad_page_id=None,
                ciudad="Rome",
                created_at="2026-01-10T12:00:00Z",
                updated_at="2026-01-10T12:00:00Z",
            ),
            Gasto(
                page_id="summary-per-2",
                payment_method="cash",
                description="Dinner",
                category="Food",
                amount=40.0,
                date="2026-01-11",
                persona="Mica",
                ciudad_page_id=None,
                ciudad="Rome",
                created_at="2026-01-11T19:00:00Z",
                updated_at="2026-01-11T19:00:00Z",
            ),
        ]

        for gasto in gastos:
            await db_client.create_gasto(gasto)

        groups, grand_total, total_count = await db_client.get_gastos_summary(
            group_by=["persona"]
        )

        assert len(groups) == 2
        assert grand_total == 100.0
        assert total_count == 2

        # Check that Franco has higher total (sorted DESC by total)
        franco_group = next(
            (g for g in groups if g["key"]["persona"] == "Franco"), None
        )
        assert franco_group is not None
        assert franco_group["total"] == 60.0
        assert franco_group["count"] == 1

    @pytest.mark.asyncio
    async def test_get_gastos_summary_by_date(self, db_client: DatabaseClient) -> None:
        """Test summary grouped by date (day-level, sorted ASC)."""
        gastos = [
            Gasto(
                page_id="summary-date-1",
                payment_method="credit_card",
                description="Lunch",
                category="Food",
                amount=50.0,
                date="2026-01-15",
                persona="John",
                ciudad_page_id=None,
                ciudad="Rome",
                created_at="2026-01-15T12:00:00Z",
                updated_at="2026-01-15T12:00:00Z",
            ),
            Gasto(
                page_id="summary-date-2",
                payment_method="cash",
                description="Dinner",
                category="Food",
                amount=75.0,
                date="2026-01-16",
                persona="Jane",
                ciudad_page_id=None,
                ciudad="Rome",
                created_at="2026-01-16T19:00:00Z",
                updated_at="2026-01-16T19:00:00Z",
            ),
            Gasto(
                page_id="summary-date-3",
                payment_method="debit_card",
                description="Coffee",
                category="Food",
                amount=10.0,
                date="2026-01-14",
                persona="John",
                ciudad_page_id=None,
                ciudad="Rome",
                created_at="2026-01-14T10:00:00Z",
                updated_at="2026-01-14T10:00:00Z",
            ),
        ]

        for gasto in gastos:
            await db_client.create_gasto(gasto)

        groups, grand_total, total_count = await db_client.get_gastos_summary(
            group_by=["date"]
        )

        assert len(groups) == 3
        assert grand_total == 135.0  # 50 + 75 + 10
        assert total_count == 3

        # Verify ascending date order
        assert groups[0]["key"]["date"] == "2026-01-14"
        assert groups[0]["total"] == 10.0
        assert groups[1]["key"]["date"] == "2026-01-15"
        assert groups[1]["total"] == 50.0
        assert groups[2]["key"]["date"] == "2026-01-16"
        assert groups[2]["total"] == 75.0

    @pytest.mark.asyncio
    async def test_get_gastos_summary_by_ciudad(
        self, db_client: DatabaseClient
    ) -> None:
        """Test summary grouped by city."""
        gastos = [
            Gasto(
                page_id="summary-city-1",
                payment_method="credit_card",
                description="Lunch",
                category="Food",
                amount=100.0,
                date="2026-01-10",
                persona="John",
                ciudad_page_id=None,
                ciudad="Rome",
                created_at="2026-01-10T12:00:00Z",
                updated_at="2026-01-10T12:00:00Z",
            ),
            Gasto(
                page_id="summary-city-2",
                payment_method="cash",
                description="Dinner",
                category="Food",
                amount=80.0,
                date="2026-01-11",
                persona="Jane",
                ciudad_page_id=None,
                ciudad="Madrid",
                created_at="2026-01-11T19:00:00Z",
                updated_at="2026-01-11T19:00:00Z",
            ),
        ]

        for gasto in gastos:
            await db_client.create_gasto(gasto)

        groups, grand_total, total_count = await db_client.get_gastos_summary(
            group_by=["ciudad"]
        )

        assert len(groups) == 2
        assert grand_total == 180.0
        assert total_count == 2

        # Find Rome group (higher total, should be first due to DESC sort)
        rome_group = next((g for g in groups if g["key"]["ciudad"] == "Rome"), None)
        assert rome_group is not None
        assert rome_group["total"] == 100.0
        assert rome_group["count"] == 1

    @pytest.mark.asyncio
    async def test_get_gastos_summary_unknown_value(
        self, db_client: DatabaseClient
    ) -> None:
        """Test that missing values are grouped as 'Unknown'."""
        gastos = [
            Gasto(
                page_id="summary-unknown-1",
                payment_method="credit_card",
                description="Lunch",
                category="Food",
                amount=50.0,
                date="2026-01-10",
                persona="John",
                ciudad_page_id=None,
                ciudad="Rome",
                created_at="2026-01-10T12:00:00Z",
                updated_at="2026-01-10T12:00:00Z",
            ),
            Gasto(
                page_id="summary-unknown-2",
                payment_method="cash",
                description="Mystery expense",
                category=None,  # Missing category
                amount=30.0,
                date="2026-01-11",
                persona="Jane",
                ciudad_page_id=None,
                ciudad=None,  # Missing ciudad
                created_at="2026-01-11T14:00:00Z",
                updated_at="2026-01-11T14:00:00Z",
            ),
        ]

        for gasto in gastos:
            await db_client.create_gasto(gasto)

        # Test category grouping with Unknown
        groups, grand_total, total_count = await db_client.get_gastos_summary(
            group_by=["category"]
        )

        assert len(groups) == 2
        unknown_group = next(
            (g for g in groups if g["key"]["category"] == "Unknown"), None
        )
        assert unknown_group is not None
        assert unknown_group["total"] == 30.0
        assert unknown_group["count"] == 1

        # Test ciudad grouping with Unknown
        groups, grand_total, total_count = await db_client.get_gastos_summary(
            group_by=["ciudad"]
        )

        assert len(groups) == 2
        unknown_city = next(
            (g for g in groups if g["key"]["ciudad"] == "Unknown"), None
        )
        assert unknown_city is not None
        assert unknown_city["total"] == 30.0
        assert unknown_city["count"] == 1

    @pytest.mark.asyncio
    async def test_get_gastos_summary_with_filters(
        self, db_client: DatabaseClient
    ) -> None:
        """Test summary with all filter parameters."""
        gastos = [
            Gasto(
                page_id="summary-filter-1",
                payment_method="credit_card",
                description="Restaurant dinner in Rome",
                category="Food",
                amount=80.0,
                date="2026-01-10",
                persona="John",
                ciudad_page_id=None,
                ciudad="Rome",
                created_at="2026-01-10T19:00:00Z",
                updated_at="2026-01-10T19:00:00Z",
            ),
            Gasto(
                page_id="summary-filter-2",
                payment_method="credit_card",
                description="Restaurant lunch in Rome",
                category="Food",
                amount=40.0,
                date="2026-01-11",
                persona="Jane",
                ciudad_page_id=None,
                ciudad="Rome",
                created_at="2026-01-11T12:00:00Z",
                updated_at="2026-01-11T12:00:00Z",
            ),
            Gasto(
                page_id="summary-filter-3",
                payment_method="cash",
                description="Coffee shop",
                category="Drinks",
                amount=10.0,
                date="2026-01-12",
                persona="John",
                ciudad_page_id=None,
                ciudad="Paris",
                created_at="2026-01-12T10:00:00Z",
                updated_at="2026-01-12T10:00:00Z",
            ),
        ]

        for gasto in gastos:
            await db_client.create_gasto(gasto)

        # Test with filters
        groups, grand_total, total_count = await db_client.get_gastos_summary(
            group_by=["category"],
            q="Restaurant",
            date_from="2026-01-10",
            date_to="2026-01-11",
            persona="John",
            payment_method="credit_card",
            category="Food",
            amount_min=50.0,
            amount_max=100.0,
            ciudad="Rome",
        )

        # Only summary-filter-1 matches all criteria
        assert len(groups) == 1
        assert groups[0]["key"]["category"] == "Food"
        assert groups[0]["total"] == 80.0
        assert groups[0]["count"] == 1
        assert grand_total == 80.0
        assert total_count == 1

    @pytest.mark.asyncio
    async def test_get_gastos_summary_fts_with_filters(
        self, db_client: DatabaseClient
    ) -> None:
        """Test summary FTS (q param) combines with structured filters."""
        gastos = [
            Gasto(
                page_id="summary-fts-1",
                payment_method="credit_card",
                description="Restaurant dinner",
                category="Food",
                amount=80.0,
                date="2026-01-10",
                persona="John",
                ciudad_page_id=None,
                ciudad="Rome",
                created_at="2026-01-10T19:00:00Z",
                updated_at="2026-01-10T19:00:00Z",
            ),
            Gasto(
                page_id="summary-fts-2",
                payment_method="cash",
                description="Restaurant lunch",
                category="Food",
                amount=40.0,
                date="2026-01-11",
                persona="Jane",
                ciudad_page_id=None,
                ciudad="Paris",
                created_at="2026-01-11T12:00:00Z",
                updated_at="2026-01-11T12:00:00Z",
            ),
        ]

        for gasto in gastos:
            await db_client.create_gasto(gasto)

        # Test FTS + ciudad filter
        groups, grand_total, total_count = await db_client.get_gastos_summary(
            group_by=["category"], q="Restaurant", ciudad="Rome"
        )

        # Only summary-fts-1 matches (Restaurant + Rome)
        assert len(groups) == 1
        assert grand_total == 80.0
        assert total_count == 1

    @pytest.mark.asyncio
    async def test_get_gastos_summary_negative_values(
        self, db_client: DatabaseClient
    ) -> None:
        """Test summary with negative amounts (refunds/adjustments)."""
        gastos = [
            Gasto(
                page_id="summary-neg-1",
                payment_method="credit_card",
                description="Purchase",
                category="Shopping",
                amount=100.0,
                date="2026-01-10",
                persona="John",
                ciudad_page_id=None,
                ciudad="Rome",
                created_at="2026-01-10T12:00:00Z",
                updated_at="2026-01-10T12:00:00Z",
            ),
            Gasto(
                page_id="summary-neg-2",
                payment_method="credit_card",
                description="Refund",
                category="Shopping",
                amount=-30.0,
                date="2026-01-11",
                persona="John",
                ciudad_page_id=None,
                ciudad="Rome",
                created_at="2026-01-11T12:00:00Z",
                updated_at="2026-01-11T12:00:00Z",
            ),
        ]

        for gasto in gastos:
            await db_client.create_gasto(gasto)

        groups, grand_total, total_count = await db_client.get_gastos_summary(
            group_by=["category"]
        )

        assert len(groups) == 1
        assert groups[0]["key"]["category"] == "Shopping"
        assert groups[0]["total"] == 70.0  # 100 - 30
        assert groups[0]["count"] == 2
        assert grand_total == 70.0
        assert total_count == 2

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
            assert row is not None
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
            assert row is not None
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
                assert len(list(rows)) == 0

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
