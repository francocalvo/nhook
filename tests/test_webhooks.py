from __future__ import annotations

from fastapi.testclient import TestClient


def test_webhook_requires_page_id(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook endpoint requires 'id' in payload."""
    response = test_client.post(
        "/webhooks/notion",
        json={"Date": {"start": "2026-03-14"}},
        headers=auth_headers,
    )
    assert response.status_code == 400
    assert "Missing 'id'" in response.json()["detail"]


def test_webhook_handles_single_date(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook handles single date correctly."""
    response = test_client.post(
        "/webhooks/notion",
        json={"id": "test-page-id", "Date": {"start": "2026-03-14"}},
        headers=auth_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["page_id"] == "test-page-id"


def test_webhook_handles_date_range(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook handles date range correctly."""
    response = test_client.post(
        "/webhooks/notion",
        json={
            "id": "test-page-id",
            "Date": {"start": "2026-03-14", "end": "2026-03-16"},
        },
        headers=auth_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True


def test_webhook_handles_empty_date(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook handles empty/null date (clears relation)."""
    response = test_client.post(
        "/webhooks/notion",
        json={"id": "test-page-id", "Date": None},
        headers=auth_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True


def test_webhook_returns_no_workflow_for_unmatched_payload(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook returns failure when no workflow matches."""
    response = test_client.post(
        "/webhooks/notion",
        json={"id": "test-page-id", "SomeOtherProperty": "value"},
        headers=auth_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is False
    assert "No workflow found" in data["message"]
