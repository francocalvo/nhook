from __future__ import annotations

from fastapi.testclient import TestClient


def test_webhook_without_auth_fails(test_client: TestClient) -> None:
    """Test webhook endpoint rejects requests without X-Calvo-Key."""
    from tests.conftest import make_notion_webhook_payload

    payload = make_notion_webhook_payload()
    response = test_client.post("/webhooks/notion", json=payload)
    assert response.status_code == 401
    assert "Missing X-Calvo-Key" in response.json()["detail"]


def test_webhook_with_invalid_key_fails(test_client: TestClient) -> None:
    """Test webhook endpoint rejects requests with invalid X-Calvo-Key."""
    from tests.conftest import make_notion_webhook_payload

    payload = make_notion_webhook_payload()
    response = test_client.post(
        "/webhooks/notion",
        json=payload,
        headers={"X-Calvo-Key": "wrong-key"},
    )
    assert response.status_code == 401
    assert "Invalid X-Calvo-Key" in response.json()["detail"]


def test_webhook_with_valid_key_succeeds(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook endpoint accepts requests with valid X-Calvo-Key."""
    from tests.conftest import make_notion_webhook_payload

    payload = make_notion_webhook_payload(date_start="2026-03-14")
    response = test_client.post("/webhooks/notion", json=payload, headers=auth_headers)
    assert response.status_code == 200
