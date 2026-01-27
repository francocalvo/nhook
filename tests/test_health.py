from __future__ import annotations

from fastapi.testclient import TestClient


def test_health_check(test_client: TestClient) -> None:
    """Test health endpoint returns ok status."""
    response = test_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
