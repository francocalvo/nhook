from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import make_notion_webhook_payload


def test_webhook_requires_page_id(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook endpoint requires 'data.id' in payload."""
    payload = {
        "source": {
            "type": "automation",
            "automation_id": "automation-123",
            "event_id": "event-789",
            "attempt": 1,
        },
        "data": {"object": "page"},
    }
    response = test_client.post("/webhooks/notion", json=payload, headers=auth_headers)
    assert response.status_code == 400
    assert "Missing 'data.id'" in response.json()["detail"]


def test_webhook_handles_single_date(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook handles single date correctly."""
    from tests.conftest import make_notion_webhook_payload

    payload = make_notion_webhook_payload(date_start="2026-03-14")
    response = test_client.post("/webhooks/notion", json=payload, headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["page_id"] == "test-page-id"


def test_webhook_handles_date_range(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook handles date range correctly."""
    from tests.conftest import make_notion_webhook_payload

    payload = make_notion_webhook_payload(
        date_start="2026-03-14", date_end="2026-03-16"
    )
    response = test_client.post("/webhooks/notion", json=payload, headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True


def test_webhook_handles_empty_date(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook handles empty/null date (clears relation)."""
    from tests.conftest import make_notion_webhook_payload

    payload = make_notion_webhook_payload(date_start=None, date_end=None)
    response = test_client.post("/webhooks/notion", json=payload, headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True


def test_webhook_with_pasajes_workflow(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook handles pasajes workflow."""
    headers = auth_headers.copy()
    headers["X-Calvo-Workflow"] = "pasajes-cronograma"
    payload = make_notion_webhook_payload()
    response = test_client.post("/webhooks/notion", json=payload, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True


def test_webhook_without_workflow_header_no_match(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook without workflow header returns no workflow."""
    headers = auth_headers.copy()
    del headers["X-Calvo-Workflow"]
    payload = make_notion_webhook_payload()
    response = test_client.post("/webhooks/notion", json=payload, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is False
    assert "No workflow found" in data["message"]
