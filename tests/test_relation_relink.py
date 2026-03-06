from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from notion_hook.config import Settings, clear_settings_cache
from notion_hook.services.relation_relink import (
    JobStatus,
    RelationRelinkService,
    RelinkAlreadyRunningError,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clear_cache() -> None:
    clear_settings_cache()


@pytest.fixture
def settings() -> Settings:
    return Settings(
        webhook_secret_key="test-secret-key",
        notion_api_token="secret_test_token",
        atracciones_database_id="test-atracciones-db-id",
        ciudades_database_id="test-ciudades-db-id",
        cronograma_database_id="test-cronograma-db-id",
        gastos_database_id="test-gastos-db-id",
        pasajes_database_id="test-pasajes-db-id",
        database_path=":memory:",
        debug=True,
    )


# ---------------------------------------------------------------------------
# Service-level tests
# ---------------------------------------------------------------------------


def _make_cronograma_page(page_id: str, date_str: str) -> dict:
    """Build a minimal Cronograma Notion page with a title = date_str."""
    return {
        "id": page_id,
        "properties": {
            "Día": {
                "title": [{"plain_text": date_str}],
            },
        },
    }


def _make_page(
    page_id: str,
    date_property: str,
    date_start: str | None,
    relation_ids: list[str] | None = None,
) -> dict:
    """Build a minimal Notion page for gastos/pasajes/atracciones."""
    properties: dict = {}
    if date_start is not None:
        properties[date_property] = {"date": {"start": date_start}}
    else:
        properties[date_property] = {"date": None}

    if relation_ids is not None:
        properties["Cronograma"] = {
            "relation": [{"id": rid} for rid in relation_ids],
        }
    else:
        properties["Cronograma"] = {"relation": []}

    return {"id": page_id, "properties": properties}


@pytest.mark.asyncio
async def test_build_cronograma_map() -> None:
    """_build_cronograma_map should create date→[page_id] mapping."""
    notion = AsyncMock()
    notion.query_all_cronograma = AsyncMock(
        return_value=[
            _make_cronograma_page("cron-1", "2024-06-01"),
            _make_cronograma_page("cron-2", "2024-06-02"),
            _make_cronograma_page("cron-3", "2024-06-01"),  # same date
        ]
    )

    service = RelationRelinkService(notion)
    result = await service._build_cronograma_map()

    assert result == {
        "2024-06-01": ["cron-1", "cron-3"],
        "2024-06-02": ["cron-2"],
    }


@pytest.mark.asyncio
async def test_relink_skips_unchanged_pages() -> None:
    """Pages whose relation already matches should be skipped (not patched)."""
    notion = AsyncMock()
    notion.query_all_cronograma = AsyncMock(
        return_value=[_make_cronograma_page("cron-1", "2024-06-01")]
    )
    # Gasto page already has the correct relation
    notion.query_all_gastos = AsyncMock(
        return_value=[
            _make_page("g1", "Date", "2024-06-01", relation_ids=["cron-1"]),
        ]
    )
    notion.query_all_pasajes = AsyncMock(return_value=[])
    notion.query_all_atracciones = AsyncMock(return_value=[])

    service = RelationRelinkService(notion)
    job_id = await service.create_job()
    await service._execute_job(job_id)

    job = service.get_job(job_id)
    assert job is not None
    assert job.status == JobStatus.COMPLETED
    # Page was processed but not updated (skipped)
    assert job.table_progress["gastos"].processed == 1
    assert job.table_progress["gastos"].updated == 0
    # update method should NOT have been called
    notion.update_gastos_cronograma_relation.assert_not_called()


@pytest.mark.asyncio
async def test_relink_patches_changed_pages() -> None:
    """Pages whose relation differs should be patched."""
    notion = AsyncMock()
    notion.query_all_cronograma = AsyncMock(
        return_value=[_make_cronograma_page("cron-1", "2024-06-01")]
    )
    # Gasto page has wrong relation (empty)
    notion.query_all_gastos = AsyncMock(
        return_value=[
            _make_page("g1", "Date", "2024-06-01", relation_ids=[]),
        ]
    )
    notion.query_all_pasajes = AsyncMock(return_value=[])
    notion.query_all_atracciones = AsyncMock(return_value=[])
    notion.update_gastos_cronograma_relation = AsyncMock(return_value={})

    service = RelationRelinkService(notion)
    job_id = await service.create_job()
    await service._execute_job(job_id)

    job = service.get_job(job_id)
    assert job is not None
    assert job.status == JobStatus.COMPLETED
    assert job.table_progress["gastos"].updated == 1
    notion.update_gastos_cronograma_relation.assert_called_once_with("g1", ["cron-1"])


@pytest.mark.asyncio
async def test_relink_clears_relation_when_no_date() -> None:
    """Pages with no date should get an empty relation if currently non-empty."""
    notion = AsyncMock()
    notion.query_all_cronograma = AsyncMock(return_value=[])
    notion.query_all_gastos = AsyncMock(return_value=[])
    notion.query_all_pasajes = AsyncMock(
        return_value=[
            _make_page("p1", "Departure", None, relation_ids=["cron-old"]),
        ]
    )
    notion.query_all_atracciones = AsyncMock(return_value=[])
    notion.update_pasajes_cronograma_relation = AsyncMock(return_value={})

    service = RelationRelinkService(notion)
    job_id = await service.create_job()
    await service._execute_job(job_id)

    job = service.get_job(job_id)
    assert job is not None
    assert job.status == JobStatus.COMPLETED
    notion.update_pasajes_cronograma_relation.assert_called_once_with("p1", [])


@pytest.mark.asyncio
async def test_relink_processes_all_three_tables() -> None:
    """All three tables should be processed in order."""
    notion = AsyncMock()
    notion.query_all_cronograma = AsyncMock(
        return_value=[_make_cronograma_page("cron-1", "2024-06-01")]
    )
    notion.query_all_gastos = AsyncMock(
        return_value=[_make_page("g1", "Date", "2024-06-01", relation_ids=[])]
    )
    notion.query_all_pasajes = AsyncMock(
        return_value=[_make_page("p1", "Departure", "2024-06-01", relation_ids=[])]
    )
    notion.query_all_atracciones = AsyncMock(
        return_value=[_make_page("a1", "Fecha", "2024-06-01", relation_ids=[])]
    )
    notion.update_gastos_cronograma_relation = AsyncMock(return_value={})
    notion.update_pasajes_cronograma_relation = AsyncMock(return_value={})
    notion.update_atracciones_cronograma_relation = AsyncMock(return_value={})

    service = RelationRelinkService(notion)
    job_id = await service.create_job()
    await service._execute_job(job_id)

    job = service.get_job(job_id)
    assert job is not None
    assert job.status == JobStatus.COMPLETED
    assert job.progress.total == 3
    assert job.progress.updated == 3
    assert job.progress.processed == 3


@pytest.mark.asyncio
async def test_relink_gastos_expands_date_range() -> None:
    """Gastos date ranges should link all Cronograma days in the range."""
    notion = AsyncMock()
    notion.query_all_cronograma = AsyncMock(
        return_value=[
            _make_cronograma_page("cron-1", "2024-06-01"),
            _make_cronograma_page("cron-2", "2024-06-02"),
            _make_cronograma_page("cron-3", "2024-06-03"),
        ]
    )
    notion.query_all_gastos = AsyncMock(
        return_value=[
            _make_page(
                "g1",
                "Date",
                "2024-06-01",
                relation_ids=[],
            )
        ]
    )
    notion.query_all_gastos.return_value[0]["properties"]["Date"]["date"]["end"] = (
        "2024-06-03"
    )
    notion.query_all_pasajes = AsyncMock(return_value=[])
    notion.query_all_atracciones = AsyncMock(return_value=[])
    notion.update_gastos_cronograma_relation = AsyncMock(return_value={})

    service = RelationRelinkService(notion)
    job_id = await service.create_job()
    await service._execute_job(job_id)

    notion.update_gastos_cronograma_relation.assert_called_once_with(
        "g1", ["cron-1", "cron-2", "cron-3"]
    )


@pytest.mark.asyncio
async def test_duplicate_job_rejected() -> None:
    """Creating a second job while one is active should raise."""
    notion = AsyncMock()
    service = RelationRelinkService(notion)
    await service.create_job()

    with pytest.raises(RelinkAlreadyRunningError):
        await service.create_job()


@pytest.mark.asyncio
async def test_create_job_rejects_batch_size_below_minimum() -> None:
    """create_job should reject batch_size lower than 1."""
    notion = AsyncMock()
    service = RelationRelinkService(notion)

    with pytest.raises(ValueError, match="batch_size must be between 1 and 100"):
        await service.create_job(batch_size=0)


@pytest.mark.asyncio
async def test_create_job_rejects_batch_size_above_maximum() -> None:
    """create_job should reject batch_size greater than 100."""
    notion = AsyncMock()
    service = RelationRelinkService(notion)

    with pytest.raises(ValueError, match="batch_size must be between 1 and 100"):
        await service.create_job(batch_size=101)


@pytest.mark.asyncio
async def test_job_failure_captured() -> None:
    """If query_all_cronograma raises, job should be marked FAILED."""
    notion = AsyncMock()
    notion.query_all_cronograma = AsyncMock(side_effect=RuntimeError("API down"))

    service = RelationRelinkService(notion)
    job_id = await service.create_job()
    await service._execute_job(job_id)

    job = service.get_job(job_id)
    assert job is not None
    assert job.status == JobStatus.FAILED
    assert "API down" in (job.error or "")


@pytest.mark.asyncio
async def test_cleanup_old_jobs_removes_stale_terminal_jobs() -> None:
    """cleanup_old_jobs should remove old completed/failed jobs."""
    notion = AsyncMock()
    service = RelationRelinkService(notion, max_job_age_hours=1.0)

    old_job_id = await service.create_job()
    old_job = service.get_job(old_job_id)
    assert old_job is not None
    old_job.status = JobStatus.COMPLETED
    old_job.completed_at = (
        (datetime.now(UTC) - timedelta(hours=2)).isoformat().replace("+00:00", "Z")
    )

    fresh_job_id = await service.create_job()
    fresh_job = service.get_job(fresh_job_id)
    assert fresh_job is not None
    fresh_job.status = JobStatus.COMPLETED
    fresh_job.completed_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    cleaned = await service.cleanup_old_jobs()

    assert cleaned == 1
    assert service.get_job(old_job_id) is None
    assert service.get_job(fresh_job_id) is not None


@pytest.mark.asyncio
async def test_execute_job_triggers_old_job_cleanup() -> None:
    """Running a job should opportunistically clean stale terminal jobs."""
    notion = AsyncMock()
    notion.query_all_cronograma = AsyncMock(return_value=[])
    notion.query_all_gastos = AsyncMock(return_value=[])
    notion.query_all_pasajes = AsyncMock(return_value=[])
    notion.query_all_atracciones = AsyncMock(return_value=[])

    service = RelationRelinkService(notion, max_job_age_hours=1.0)

    old_job_id = await service.create_job()
    old_job = service.get_job(old_job_id)
    assert old_job is not None
    old_job.status = JobStatus.COMPLETED
    old_job.completed_at = (
        (datetime.now(UTC) - timedelta(hours=2)).isoformat().replace("+00:00", "Z")
    )

    job_id = await service.create_job()
    await service._execute_job(job_id)

    assert service.get_job(old_job_id) is None


# ---------------------------------------------------------------------------
# API-level tests
# ---------------------------------------------------------------------------


@dataclass
class DummyRelinkJob:
    job_id: str
    status: JobStatus = JobStatus.STARTED
    started_at: str = "2024-01-01T00:00:00Z"
    updated_at: str = "2024-01-01T00:00:00Z"

    def to_dict(self) -> dict[str, object]:
        base_progress = {
            "total": 0,
            "processed": 0,
            "created": 0,
            "updated": 0,
            "deleted": 0,
            "failed": 0,
        }
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "progress": base_progress,
            "table_progress": {
                "gastos": base_progress,
                "pasajes": base_progress,
                "atracciones": base_progress,
            },
            "started_at": self.started_at,
            "updated_at": self.updated_at,
        }


class DummyRelinkService:
    def __init__(self) -> None:
        self.jobs: dict[str, DummyRelinkJob] = {}

    async def create_job(self, batch_size: int = 100) -> str:
        _ = batch_size
        for job in self.jobs.values():
            if job.status in (JobStatus.STARTED, JobStatus.IN_PROGRESS):
                raise RelinkAlreadyRunningError(job.job_id)

        job_id = str(uuid4())
        self.jobs[job_id] = DummyRelinkJob(job_id=job_id)
        return job_id

    async def start_relink(self, job_id: str) -> None:
        job = self.jobs.get(job_id)
        if job is not None:
            job.status = JobStatus.IN_PROGRESS
            job.updated_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    def get_job(self, job_id: str) -> DummyRelinkJob | None:
        return self.jobs.get(job_id)


@pytest.fixture
def client_and_service(
    settings: Settings,
) -> Generator[tuple[TestClient, DummyRelinkService], None, None]:
    from notion_hook.app import app

    dummy_relink_service = DummyRelinkService()
    dummy_registry = type("DummyRegistry", (), {"workflows": {}})()
    dummy_notion = object()
    dummy_db = object()
    dummy_gastos_reload = object()
    dummy_full_reload = object()

    with (
        patch("notion_hook.config.get_settings", return_value=settings),
        patch("notion_hook.core.auth.get_settings", return_value=settings),
        patch("notion_hook.app._workflow_registry", dummy_registry),
        patch("notion_hook.app._notion_client", dummy_notion),
        patch("notion_hook.app._database_client", dummy_db),
        patch("notion_hook.app._reload_service", dummy_gastos_reload),
        patch("notion_hook.app._full_reload_service", dummy_full_reload),
        patch("notion_hook.app._relink_service", dummy_relink_service),
        TestClient(app, raise_server_exceptions=False) as client,
    ):
        yield client, dummy_relink_service


def test_post_relink_requires_key(
    client_and_service: tuple[TestClient, DummyRelinkService],
) -> None:
    client, _ = client_and_service
    resp = client.post("/api/relink", json={})
    assert resp.status_code == 401


def test_post_relink_starts_job(
    client_and_service: tuple[TestClient, DummyRelinkService],
) -> None:
    client, service = client_and_service
    resp = client.post(
        "/api/relink",
        headers={"X-Calvo-Key": "test-secret-key"},
        json={"batch_size": 50},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "started"
    assert body["batch_size"] == 50
    assert body["job_id"] in service.jobs


def test_post_relink_conflicts_when_active_job(
    client_and_service: tuple[TestClient, DummyRelinkService],
) -> None:
    client, _ = client_and_service
    first = client.post(
        "/api/relink",
        headers={"X-Calvo-Key": "test-secret-key"},
        json={},
    )
    assert first.status_code == 200

    second = client.post(
        "/api/relink",
        headers={"X-Calvo-Key": "test-secret-key"},
        json={},
    )
    assert second.status_code == 409


def test_get_relink_status_404(
    client_and_service: tuple[TestClient, DummyRelinkService],
) -> None:
    client, _ = client_and_service
    resp = client.get(
        "/api/relink/non-existent",
        headers={"X-Calvo-Key": "test-secret-key"},
    )
    assert resp.status_code == 404


def test_get_relink_status_returns_job(
    client_and_service: tuple[TestClient, DummyRelinkService],
) -> None:
    client, _ = client_and_service
    resp = client.post(
        "/api/relink",
        headers={"X-Calvo-Key": "test-secret-key"},
        json={},
    )
    job_id = resp.json()["job_id"]

    status_resp = client.get(
        f"/api/relink/{job_id}",
        headers={"X-Calvo-Key": "test-secret-key"},
    )
    assert status_resp.status_code == 200
    data = status_resp.json()
    assert data["job_id"] == job_id
