from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock

import pytest

os.environ["WEBHOOK_SECRET_KEY"] = "test-secret-key"
os.environ["NOTION_API_TOKEN"] = "secret_test_token"
os.environ["CRONOGRAMA_DATABASE_ID"] = "test-cronograma-db-id"
os.environ["GASTOS_DATABASE_ID"] = "test-gastos-db-id"
os.environ["PASAJES_DATABASE_ID"] = "test-pasajes-db-id"

from notion_hook.config import Settings, clear_settings_cache
from notion_hook.core.database import DatabaseClient
from notion_hook.models.gastos import Gasto
from notion_hook.services.gastos_reload import (
    GastosReloadService,
    JobStatus,
    ReloadAlreadyRunningError,
    ReloadJob,
    ReloadMode,
)


@pytest.fixture(autouse=True)
def clear_cache() -> None:
    """Clear settings cache before each test."""
    clear_settings_cache()


@pytest.fixture
def settings() -> Settings:
    """Return test settings."""
    return Settings(
        webhook_secret_key="test-secret-key",
        notion_api_token="secret_test_token",
        cronograma_database_id="test-cronograma-db-id",
        gastos_database_id="test-gastos-db-id",
        pasajes_database_id="test-pasajes-db-id",
        database_path=":memory:",
        debug=True,
    )


@pytest.fixture
def mock_notion_client() -> AsyncMock:
    """Return a mocked NotionClient."""
    client = AsyncMock()
    client.settings = MagicMock()
    client.settings.gastos_database_id = "test-gastos-db-id"
    return client


@pytest.fixture
async def db_client(settings: Settings) -> DatabaseClient:
    """Return a test database client."""
    client = DatabaseClient(settings)
    await client.initialize()
    yield client
    await client.close()


@pytest.fixture
def reload_service(
    mock_notion_client: AsyncMock, db_client: DatabaseClient
) -> GastosReloadService:
    """Return a test reload service."""
    return GastosReloadService(mock_notion_client, db_client)


def make_notion_gastos_page(
    page_id: str = "test-page-1",
    payment_method: str = "Cash",
    description: str = "Test expense",
    category: str | None = None,
    amount: float = 100.0,
    date: str = "2024-01-01",
) -> dict:
    """Create a mock Notion Gastos page.

    Args:
        page_id: The Notion page ID.
        payment_method: Payment method.
        description: Description.
        category: Category (single or comma-separated).
        amount: Amount.
        date: Date string.

    Returns:
        Dictionary representing a Notion page.
    """
    properties = {
        "Payment Method": {
            "id": "pm-id",
            "type": "select",
            "select": {"id": "opt-1", "name": payment_method, "color": "default"},
        },
        "Expense": {
            "id": "desc-id",
            "type": "rich_text",
            "rich_text": [{"type": "text", "text": {"content": description}}],
        },
        "Amount": {"id": "amt-id", "type": "number", "number": amount},
        "Date": {"id": "date-id", "type": "date", "date": {"start": date}},
    }

    if category is not None:
        if ", " in category:
            categories = [
                {"id": f"opt-{i}", "name": cat}
                for i, cat in enumerate(category.split(", "))
            ]
        else:
            categories = [{"id": "opt-1", "name": category}]
        properties["Category"] = {
            "id": "cat-id",
            "type": "multi_select",
            "multi_select": categories,
        }

    return {
        "id": page_id,
        "created_time": "2024-01-01T00:00:00.000Z",
        "last_edited_time": "2024-01-01T00:00:00.000Z",
        "properties": properties,
    }


class TestReloadJob:
    """Tests for ReloadJob."""

    def test_create_job(self) -> None:
        """Test creating a reload job."""
        job = ReloadJob(
            job_id="test-job-id",
            mode=ReloadMode.FULL,
            batch_size=50,
            delete_missing=False,
        )
        assert job.job_id == "test-job-id"
        assert job.status == JobStatus.STARTED
        assert job.mode == ReloadMode.FULL
        assert job.batch_size == 50
        assert job.delete_missing is False

    def test_job_to_dict_started(self) -> None:
        """Test converting started job to dict."""
        job = ReloadJob(job_id="test-job-id", mode=ReloadMode.FULL)
        result = job.to_dict()
        assert result["job_id"] == "test-job-id"
        assert result["status"] == "started"
        assert "message" not in result

    def test_job_to_dict_completed(self) -> None:
        """Test converting completed job to dict."""
        job = ReloadJob(job_id="test-job-id", mode=ReloadMode.FULL)
        job.status = JobStatus.COMPLETED
        job.completed_at = "2024-01-01T10:00:00Z"
        job.started_at = "2024-01-01T09:50:00Z"
        job.progress.created = 10
        job.progress.updated = 5

        result = job.to_dict()
        assert result["status"] == "completed"
        assert result["message"] == "Reload completed successfully"
        assert result["completed_at"] == "2024-01-01T10:00:00Z"
        assert result["duration_seconds"] == 600.0

    def test_job_to_dict_failed(self) -> None:
        """Test converting failed job to dict."""
        job = ReloadJob(job_id="test-job-id", mode=ReloadMode.FULL)
        job.status = JobStatus.FAILED
        job.failed_at = "2024-01-01T10:00:00Z"
        job.error = "Notion API error"

        result = job.to_dict()
        assert result["status"] == "failed"
        assert result["error"] == "Notion API error"
        assert result["message"] == "Reload failed: Notion API error"


class TestGastosReloadService:
    """Tests for GastosReloadService."""

    def test_create_service(self, reload_service: GastosReloadService) -> None:
        """Test creating a reload service."""
        assert reload_service.notion_client is not None
        assert reload_service.database_client is not None
        assert reload_service.max_job_age_hours == 24.0

    @pytest.mark.asyncio
    async def test_create_job(self, reload_service: GastosReloadService) -> None:
        """Test creating a reload job."""
        job_id = await reload_service.create_job(
            mode=ReloadMode.FULL,
            batch_size=50,
            delete_missing=False,
        )
        job = reload_service.get_job(job_id)
        assert job is not None
        assert job.job_id == job_id
        assert job.mode == ReloadMode.FULL
        assert job.batch_size == 50
        assert job.delete_missing is False

    def test_get_job_not_found(self, reload_service: GastosReloadService) -> None:
        """Test getting a non-existent job."""
        job = reload_service.get_job("non-existent-job-id")
        assert job is None

    @pytest.mark.asyncio
    async def test_create_job_rejects_when_active(
        self, reload_service: GastosReloadService
    ) -> None:
        """Test creating a job while one is active."""
        _ = await reload_service.create_job(mode=ReloadMode.FULL)
        with pytest.raises(ReloadAlreadyRunningError):
            await reload_service.create_job(mode=ReloadMode.FULL)

    @pytest.mark.asyncio
    async def test_execute_full_reload_empty(
        self, reload_service: GastosReloadService, mock_notion_client: AsyncMock
    ) -> None:
        """Test executing a full reload with empty results."""
        mock_notion_client.query_all_gastos = AsyncMock(return_value=[])

        job_id = await reload_service.create_job(
            mode=ReloadMode.FULL, delete_missing=False
        )
        await reload_service._execute_job(job_id)

        job = reload_service.get_job(job_id)
        assert job is not None
        assert job.status == JobStatus.COMPLETED
        assert job.progress.total == 0
        assert job.progress.processed == 0

    @pytest.mark.asyncio
    async def test_execute_full_reload_with_records(
        self,
        reload_service: GastosReloadService,
        mock_notion_client: AsyncMock,
    ) -> None:
        """Test executing a full reload with records."""
        mock_notion_client.query_all_gastos = AsyncMock(
            return_value=[
                make_notion_gastos_page(
                    page_id="page-1",
                    payment_method="Cash",
                    description="Expense 1",
                    amount=100.0,
                    date="2024-01-01",
                ),
                make_notion_gastos_page(
                    page_id="page-2",
                    payment_method="Credit",
                    description="Expense 2",
                    amount=200.0,
                    date="2024-01-02",
                ),
            ]
        )

        job_id = await reload_service.create_job(
            mode=ReloadMode.FULL, delete_missing=False
        )
        await reload_service._execute_job(job_id)

        job = reload_service.get_job(job_id)
        assert job is not None
        assert job.status == JobStatus.COMPLETED
        assert job.progress.total == 2
        assert job.progress.created == 2
        assert job.progress.updated == 0

    @pytest.mark.asyncio
    async def test_execute_full_reload_with_existing_records(
        self,
        reload_service: GastosReloadService,
        mock_notion_client: AsyncMock,
        db_client: DatabaseClient,
    ) -> None:
        """Test executing a full reload with existing records."""
        existing_gasto = Gasto(
            page_id="page-1",
            payment_method="Cash",
            description="Old description",
            category="Food",
            amount=50.0,
            date="2024-01-01",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        await db_client.create_gasto(existing_gasto)

        mock_notion_client.query_all_gastos = AsyncMock(
            return_value=[
                make_notion_gastos_page(
                    page_id="page-1",
                    payment_method="Credit",
                    description="New description",
                    category="Groceries",
                    amount=150.0,
                    date="2024-01-01",
                ),
            ]
        )

        job_id = await reload_service.create_job(
            mode=ReloadMode.FULL, delete_missing=False
        )
        await reload_service._execute_job(job_id)

        job = reload_service.get_job(job_id)
        assert job is not None
        assert job.status == JobStatus.COMPLETED
        assert job.progress.deleted == 1
        assert job.progress.created == 1
        assert job.progress.updated == 0

        updated = await db_client.get_gasto("page-1")
        assert updated is not None
        assert updated.payment_method == "Credit"
        assert updated.description == "New description"
        assert updated.category == "Groceries"
        assert updated.amount == 150.0

    @pytest.mark.asyncio
    async def test_execute_full_reload_delete_missing(
        self,
        reload_service: GastosReloadService,
        mock_notion_client: AsyncMock,
        db_client: DatabaseClient,
    ) -> None:
        """Test executing a full reload with delete_missing enabled."""
        existing_gasto = Gasto(
            page_id="page-old",
            payment_method="Cash",
            description="Old expense",
            category="Food",
            amount=50.0,
            date="2024-01-01",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        await db_client.create_gasto(existing_gasto)

        mock_notion_client.query_all_gastos = AsyncMock(
            return_value=[
                make_notion_gastos_page(
                    page_id="page-1",
                    payment_method="Cash",
                    description="New expense",
                    amount=100.0,
                    date="2024-01-01",
                ),
            ]
        )

        job_id = await reload_service.create_job(
            mode=ReloadMode.FULL, delete_missing=True
        )
        await reload_service._execute_job(job_id)

        job = reload_service.get_job(job_id)
        assert job is not None
        assert job.status == JobStatus.COMPLETED
        assert job.progress.created == 1
        assert job.progress.deleted == 1

        old_gasto = await db_client.get_gasto("page-old")
        assert old_gasto is None

    @pytest.mark.asyncio
    async def test_execute_job_failure(
        self,
        reload_service: GastosReloadService,
        mock_notion_client: AsyncMock,
    ) -> None:
        """Test executing a job that fails."""
        mock_notion_client.query_all_gastos = AsyncMock(
            side_effect=Exception("Notion API error")
        )

        job_id = await reload_service.create_job(mode=ReloadMode.FULL)
        await reload_service._execute_job(job_id)

        job = reload_service.get_job(job_id)
        assert job is not None
        assert job.status == JobStatus.FAILED
        assert job.error == "Notion API error"

    @pytest.mark.asyncio
    async def test_cleanup_old_jobs(self, reload_service: GastosReloadService) -> None:
        """Test cleaning up old jobs."""
        from datetime import UTC, datetime, timedelta

        job1_id = await reload_service.create_job(mode=ReloadMode.FULL)
        job1 = reload_service.get_job(job1_id)
        if job1:
            job1.status = JobStatus.COMPLETED
            job1.completed_at = (datetime.now(UTC) - timedelta(hours=25)).isoformat()

        job2_id = await reload_service.create_job(mode=ReloadMode.FULL)
        job2 = reload_service.get_job(job2_id)
        if job2:
            job2.status = JobStatus.COMPLETED
            job2.completed_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()

        job3_id = await reload_service.create_job(mode=ReloadMode.FULL)

        cleaned_count = await reload_service.cleanup_old_jobs()

        assert cleaned_count == 1
        assert reload_service.get_job(job1_id) is None
        assert reload_service.get_job(job2_id) is not None
        assert reload_service.get_job(job3_id) is not None

    @pytest.mark.asyncio
    async def test_category_single_value(
        self,
        reload_service: GastosReloadService,
        mock_notion_client: AsyncMock,
    ) -> None:
        """Test reload with single category value."""
        mock_notion_client.query_all_gastos = AsyncMock(
            return_value=[
                make_notion_gastos_page(
                    page_id="page-1",
                    payment_method="Cash",
                    description="Lunch",
                    category="Food",
                    amount=15.0,
                    date="2024-01-01",
                ),
            ]
        )

        job_id = await reload_service.create_job(mode=ReloadMode.FULL)
        await reload_service._execute_job(job_id)

        job = reload_service.get_job(job_id)
        assert job is not None
        assert job.status == JobStatus.COMPLETED
        assert job.progress.created == 1

        gasto = await reload_service.database_client.get_gasto("page-1")
        assert gasto is not None
        assert gasto.category == "Food"

    @pytest.mark.asyncio
    async def test_category_multiple_values(
        self,
        reload_service: GastosReloadService,
        mock_notion_client: AsyncMock,
    ) -> None:
        """Test reload with multiple category values (comma-separated)."""
        mock_notion_client.query_all_gastos = AsyncMock(
            return_value=[
                make_notion_gastos_page(
                    page_id="page-1",
                    payment_method="Cash",
                    description="Grocery shopping",
                    category="Food, Groceries, Household",
                    amount=150.0,
                    date="2024-01-01",
                ),
            ]
        )

        job_id = await reload_service.create_job(mode=ReloadMode.FULL)
        await reload_service._execute_job(job_id)

        job = reload_service.get_job(job_id)
        assert job is not None
        assert job.status == JobStatus.COMPLETED
        assert job.progress.created == 1

        gasto = await reload_service.database_client.get_gasto("page-1")
        assert gasto is not None
        assert gasto.category == "Food, Groceries, Household"
