from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass
from datetime import UTC, datetime
from unittest.mock import patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from notion_hook.config import Settings, clear_settings_cache
from notion_hook.services.gastos_reload import (
    JobStatus,
    ReloadAlreadyRunningError,
    ReloadMode,
)


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


@dataclass
class DummyJob:
    job_id: str
    status: JobStatus = JobStatus.STARTED
    started_at: str = "2024-01-01T00:00:00Z"
    updated_at: str = "2024-01-01T00:00:00Z"

    def to_dict(self) -> dict[str, object]:
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "progress": {
                "total": 0,
                "processed": 0,
                "created": 0,
                "updated": 0,
                "deleted": 0,
                "failed": 0,
            },
            "started_at": self.started_at,
            "updated_at": self.updated_at,
        }


class DummyReloadService:
    def __init__(self) -> None:
        self.jobs: dict[str, DummyJob] = {}

    async def create_job(
        self,
        mode: ReloadMode = ReloadMode.FULL,
        batch_size: int = 100,
        delete_missing: bool = True,
    ) -> str:
        _ = (mode, batch_size, delete_missing)
        for job in self.jobs.values():
            if job.status in (JobStatus.STARTED, JobStatus.IN_PROGRESS):
                raise ReloadAlreadyRunningError(job.job_id)

        job_id = str(uuid4())
        self.jobs[job_id] = DummyJob(job_id=job_id)
        return job_id

    async def start_reload(self, job_id: str) -> None:
        job = self.jobs.get(job_id)
        if job is not None:
            job.status = JobStatus.IN_PROGRESS
            job.updated_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    def get_job(self, job_id: str) -> DummyJob | None:
        return self.jobs.get(job_id)


@pytest.fixture
def client_and_service(
    settings: Settings,
) -> Generator[tuple[TestClient, DummyReloadService], None, None]:
    from notion_hook.app import app

    dummy_service = DummyReloadService()
    dummy_registry = type("DummyRegistry", (), {"workflows": {}})()
    dummy_notion = object()
    dummy_db = object()

    with (
        patch("notion_hook.config.get_settings", return_value=settings),
        patch("notion_hook.core.auth.get_settings", return_value=settings),
        patch("notion_hook.app._workflow_registry", dummy_registry),
        patch("notion_hook.app._notion_client", dummy_notion),
        patch("notion_hook.app._database_client", dummy_db),
        patch("notion_hook.app._reload_service", dummy_service),
        TestClient(app, raise_server_exceptions=False) as client,
    ):
        yield client, dummy_service


def test_post_reload_requires_key(
    client_and_service: tuple[TestClient, DummyReloadService],
) -> None:
    client, _ = client_and_service
    resp = client.post("/api/gastos/reload", json={"mode": "full"})
    assert resp.status_code == 401


def test_post_reload_invalid_mode_returns_422(
    client_and_service: tuple[TestClient, DummyReloadService],
) -> None:
    client, _ = client_and_service
    resp = client.post(
        "/api/gastos/reload",
        headers={"X-Calvo-Key": "test-secret-key"},
        json={"mode": "nope"},
    )
    assert resp.status_code == 422


def test_post_reload_starts_job(
    client_and_service: tuple[TestClient, DummyReloadService],
) -> None:
    client, service = client_and_service
    resp = client.post(
        "/api/gastos/reload",
        headers={"X-Calvo-Key": "test-secret-key"},
        json={"mode": "full", "batch_size": 10, "delete_missing": True},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "started"
    assert body["mode"] == "full"
    assert body["batch_size"] == 10
    assert body["job_id"] in service.jobs


def test_post_reload_conflicts_when_active_job(
    client_and_service: tuple[TestClient, DummyReloadService],
) -> None:
    client, _ = client_and_service
    first = client.post(
        "/api/gastos/reload",
        headers={"X-Calvo-Key": "test-secret-key"},
        json={"mode": "full"},
    )
    assert first.status_code == 200

    second = client.post(
        "/api/gastos/reload",
        headers={"X-Calvo-Key": "test-secret-key"},
        json={"mode": "full"},
    )
    assert second.status_code == 409


def test_get_reload_status_404(
    client_and_service: tuple[TestClient, DummyReloadService],
) -> None:
    client, _ = client_and_service
    resp = client.get(
        "/api/gastos/reload/non-existent",
        headers={"X-Calvo-Key": "test-secret-key"},
    )
    assert resp.status_code == 404


def test_get_reload_status_returns_job(
    client_and_service: tuple[TestClient, DummyReloadService],
) -> None:
    client, _ = client_and_service
    resp = client.post(
        "/api/gastos/reload",
        headers={"X-Calvo-Key": "test-secret-key"},
        json={"mode": "full"},
    )
    job_id = resp.json()["job_id"]

    status_resp = client.get(
        f"/api/gastos/reload/{job_id}",
        headers={"X-Calvo-Key": "test-secret-key"},
    )
    assert status_resp.status_code == 200
    data = status_resp.json()
    assert data["job_id"] == job_id
