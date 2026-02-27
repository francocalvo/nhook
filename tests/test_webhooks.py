from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import (
    make_atracciones_webhook_payload,
    make_notion_webhook_payload,
)


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


def test_webhook_with_ciudades_sync_workflow(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook handles ciudades local sync workflow."""
    headers = auth_headers.copy()
    headers["X-Calvo-Workflow"] = "ciudades-sync"
    payload = make_notion_webhook_payload(
        extra_properties={"Name": {"title": [{"plain_text": "Madrid"}]}}
    )
    response = test_client.post("/webhooks/notion", json=payload, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True


def test_webhook_with_cronograma_sync_workflow(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook handles cronograma local sync workflow."""
    headers = auth_headers.copy()
    headers["X-Calvo-Workflow"] = "cronograma-sync"
    payload = make_notion_webhook_payload(
        property_name="Día",
        extra_properties={
            "Día": {
                "id": "dia-property-id",
                "type": "date",
                "date": {"start": "2026-03-14", "end": None, "time_zone": None},
            }
        },
    )
    response = test_client.post("/webhooks/notion", json=payload, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True


def test_webhook_requires_workflow_header(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook requires X-Calvo-Workflow header."""
    headers = auth_headers.copy()
    del headers["X-Calvo-Workflow"]
    payload = make_notion_webhook_payload()
    response = test_client.post("/webhooks/notion", json=payload, headers=headers)
    assert response.status_code == 400
    data = response.json()
    assert "Missing required 'X-Calvo-Workflow' header" in data["detail"]


def test_webhook_unknown_workflow_name(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook with unknown workflow name returns error."""
    headers = auth_headers.copy()
    headers["X-Calvo-Workflow"] = "unknown-workflow"
    payload = make_notion_webhook_payload()
    response = test_client.post("/webhooks/notion", json=payload, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is False
    assert "No workflow found" in data["message"]


def test_webhook_case_insensitive_date_lowercase(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook handles lowercase 'date' property."""
    payload = make_notion_webhook_payload()
    payload["data"]["properties"] = {
        "date": {
            "id": "date-property-id",
            "type": "date",
            "date": {"start": "2026-03-14", "end": None, "time_zone": None},
        }
    }
    response = test_client.post("/webhooks/notion", json=payload, headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True


def test_webhook_case_insensitive_date_uppercase(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook handles uppercase 'DATE' property."""
    payload = make_notion_webhook_payload()
    payload["data"]["properties"] = {
        "DATE": {
            "id": "date-property-id",
            "type": "date",
            "date": {"start": "2026-03-14", "end": None, "time_zone": None},
        }
    }
    response = test_client.post("/webhooks/notion", json=payload, headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True


def test_webhook_case_insensitive_date_mixed_case(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook handles mixed case 'dAtE' property."""
    payload = make_notion_webhook_payload()
    payload["data"]["properties"] = {
        "dAtE": {
            "id": "date-property-id",
            "type": "date",
            "date": {"start": "2026-03-14", "end": None, "time_zone": None},
        }
    }
    response = test_client.post("/webhooks/notion", json=payload, headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True


def test_webhook_case_insensitive_departure_lowercase(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook handles lowercase 'departure' property."""
    headers = auth_headers.copy()
    headers["X-Calvo-Workflow"] = "pasajes-cronograma"
    payload = make_notion_webhook_payload(
        extra_properties={
            "departure": {
                "id": "departure-property-id",
                "type": "date",
                "date": {"start": "2026-03-15", "end": None, "time_zone": None},
            }
        }
    )
    response = test_client.post("/webhooks/notion", json=payload, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True


def test_webhook_case_insensitive_departure_uppercase(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook handles uppercase 'DEPARTURE' property."""
    headers = auth_headers.copy()
    headers["X-Calvo-Workflow"] = "pasajes-cronograma"
    payload = make_notion_webhook_payload(
        extra_properties={
            "DEPARTURE": {
                "id": "departure-property-id",
                "type": "date",
                "date": {"start": "2026-03-15", "end": None, "time_zone": None},
            }
        }
    )
    response = test_client.post("/webhooks/notion", json=payload, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True


def test_webhook_atracciones_datetime_parsing(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook correctly parses datetime strings for atracciones workflow."""
    headers = auth_headers.copy()
    headers["X-Calvo-Workflow"] = "atracciones-cronograma"
    payload = make_atracciones_webhook_payload(fecha_start="2026-03-14T10:30:00.000Z")
    response = test_client.post("/webhooks/notion", json=payload, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["page_id"] == "test-page-id"


def test_webhook_atracciones_date_parsing(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook correctly parses date-only strings for atracciones workflow."""
    headers = auth_headers.copy()
    headers["X-Calvo-Workflow"] = "atracciones-cronograma"
    payload = make_atracciones_webhook_payload(fecha_start="2026-03-14")
    response = test_client.post("/webhooks/notion", json=payload, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["page_id"] == "test-page-id"


def test_webhook_atracciones_empty_fecha(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook clears relations when Fecha is empty for atracciones."""
    headers = auth_headers.copy()
    headers["X-Calvo-Workflow"] = "atracciones-cronograma"
    payload = make_atracciones_webhook_payload(fecha_start=None)
    response = test_client.post("/webhooks/notion", json=payload, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["page_id"] == "test-page-id"
    assert data["updated_relations"] == []


def test_webhook_atracciones_case_insensitive_lowercase(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook handles lowercase 'fecha' property."""
    headers = auth_headers.copy()
    headers["X-Calvo-Workflow"] = "atracciones-cronograma"
    payload = make_atracciones_webhook_payload(fecha_start="2026-03-14T15:45:00Z")
    payload["data"]["properties"] = {
        "fecha": {
            "id": "fecha-property-id",
            "type": "date",
            "date": {"start": "2026-03-14T15:45:00Z", "end": None, "time_zone": None},
        }
    }
    response = test_client.post("/webhooks/notion", json=payload, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True


def test_webhook_atracciones_case_insensitive_uppercase(
    test_client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Test webhook handles uppercase 'FECHA' property."""
    headers = auth_headers.copy()
    headers["X-Calvo-Workflow"] = "atracciones-cronograma"
    payload = make_atracciones_webhook_payload(fecha_start="2026-03-14T15:45:00Z")
    payload["data"]["properties"] = {
        "FECHA": {
            "id": "fecha-property-id",
            "type": "date",
            "date": {"start": "2026-03-14T15:45:00Z", "end": None, "time_zone": None},
        }
    }
    response = test_client.post("/webhooks/notion", json=payload, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
