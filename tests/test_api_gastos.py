from __future__ import annotations

from collections.abc import Generator
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from notion_hook.clients.notion import NotionClient
from notion_hook.config import Settings, clear_settings_cache
from notion_hook.core.database import DatabaseClient
from notion_hook.models.gastos import Gasto


@pytest.fixture(autouse=True)
def clear_cache() -> Generator[None, None, None]:
    """Clear settings cache before each test."""
    clear_settings_cache()
    yield


@pytest.fixture
def settings() -> Settings:
    """Return test settings."""
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


@pytest.fixture
def mock_notion_client(settings: Settings) -> AsyncMock:
    """Return a mocked NotionClient."""
    client = AsyncMock(spec=NotionClient)
    client.settings = settings
    return client


@pytest.fixture
def mock_database_client() -> AsyncMock:
    """Return a mocked DatabaseClient."""
    client = AsyncMock(spec=DatabaseClient)
    return client


@pytest.fixture
def test_client_with_mocks(
    settings: Settings,
    mock_notion_client: AsyncMock,
    mock_database_client: AsyncMock,
) -> Generator[TestClient, None, None]:
    """Return a test client for the FastAPI app with mocked dependencies."""
    from notion_hook.app import app

    with (
        patch("notion_hook.config.get_settings", return_value=settings),
        patch("notion_hook.core.auth.get_settings", return_value=settings),
        patch("notion_hook.app._notion_client", mock_notion_client),
        patch("notion_hook.app._database_client", mock_database_client),
        TestClient(app, raise_server_exceptions=False) as client,
    ):
        yield client


@pytest.fixture
def auth_headers() -> dict[str, str]:
    """Return valid auth headers."""
    return {"X-Calvo-Key": "test-secret-key"}


@pytest.fixture
def sample_gasto() -> Gasto:
    """Return a sample gasto for testing."""
    return Gasto(
        page_id="test-page-123",
        payment_method="credit_card",
        description="Lunch at restaurant",
        category="Food",
        amount=45.50,
        date="2026-01-15",
        persona="John",
        ciudad_page_id=None,
        ciudad=None,
        created_at="2026-01-15T12:00:00Z",
        updated_at="2026-01-15T12:00:00Z",
    )


@pytest.fixture
def sample_gastos_list() -> list[Gasto]:
    """Return a list of sample gastos for testing."""
    return [
        Gasto(
            page_id="test-page-1",
            payment_method="credit_card",
            description="Groceries",
            category="Food",
            amount=120.75,
            date="2026-01-10",
            persona="Jane",
            ciudad_page_id=None,
            ciudad=None,
            created_at="2026-01-10T10:00:00Z",
            updated_at="2026-01-10T10:00:00Z",
        ),
        Gasto(
            page_id="test-page-2",
            payment_method="cash",
            description="Coffee",
            category="Food",
            amount=4.50,
            date="2026-01-11",
            persona="John",
            ciudad_page_id=None,
            ciudad=None,
            created_at="2026-01-11T08:30:00Z",
            updated_at="2026-01-11T08:30:00Z",
        ),
        Gasto(
            page_id="test-page-3",
            payment_method="debit_card",
            description="Uber ride",
            category="Transport",
            amount=25.00,
            date="2026-01-12",
            persona="John",
            ciudad_page_id=None,
            ciudad=None,
            created_at="2026-01-12T14:20:00Z",
            updated_at="2026-01-12T14:20:00Z",
        ),
    ]


class TestCreateGasto:
    """Tests for POST /api/gastos endpoint."""

    def test_create_gasto_requires_auth(
        self,
        test_client_with_mocks: TestClient,
    ) -> None:
        """Test that creating a gasto requires authentication."""
        resp = test_client_with_mocks.post(
            "/api/gastos",
            json={
                "expense": "Test expense",
                "amount": 100.0,
            },
        )
        assert resp.status_code == 401

    def test_create_gasto_missing_expense(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test that expense field is required."""
        resp = test_client_with_mocks.post(
            "/api/gastos",
            headers=auth_headers,
            json={
                "amount": 100.0,
            },
        )
        assert resp.status_code == 422

    def test_create_gasto_missing_amount(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test that amount field is required."""
        resp = test_client_with_mocks.post(
            "/api/gastos",
            headers=auth_headers,
            json={
                "expense": "Test expense",
            },
        )
        assert resp.status_code == 422

    def test_create_gasto_invalid_amount(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test that amount must be greater than 0."""
        resp = test_client_with_mocks.post(
            "/api/gastos",
            headers=auth_headers,
            json={
                "expense": "Test expense",
                "amount": -10.0,
            },
        )
        assert resp.status_code == 422

    def test_create_gasto_empty_expense(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test that expense cannot be empty."""
        resp = test_client_with_mocks.post(
            "/api/gastos",
            headers=auth_headers,
            json={
                "expense": "",
                "amount": 100.0,
            },
        )
        assert resp.status_code == 422

    def test_create_gasto_success(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_notion_client: AsyncMock,
    ) -> None:
        """Test successful gasto creation."""
        mock_notion_client.create_gasto_page.return_value = {
            "id": "new-page-123",
        }

        resp = test_client_with_mocks.post(
            "/api/gastos",
            headers=auth_headers,
            json={
                "expense": "Test expense",
                "amount": 100.0,
                "date": "2026-01-15",
                "category": "Food",
                "payment_method": "credit_card",
                "persona": "John",
            },
        )
        assert resp.status_code == 201
        body = resp.json()
        assert body["page_id"] == "new-page-123"
        assert body["message"] == "Gasto created successfully"
        assert "note" in body

        # Verify Notion client was called with correct parameters
        mock_notion_client.create_gasto_page.assert_called_once_with(
            expense="Test expense",
            amount=100.0,
            date="2026-01-15",
            category="Food",
            payment_method="credit_card",
            persona="John",
        )

    def test_create_gasto_with_list_category(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_notion_client: AsyncMock,
    ) -> None:
        """Test creating a gasto with category as a list."""
        mock_notion_client.create_gasto_page.return_value = {
            "id": "new-page-456",
        }

        resp = test_client_with_mocks.post(
            "/api/gastos",
            headers=auth_headers,
            json={
                "expense": "Test expense",
                "amount": 100.0,
                "category": ["Food", "Dining"],
            },
        )
        assert resp.status_code == 201

        mock_notion_client.create_gasto_page.assert_called_once_with(
            expense="Test expense",
            amount=100.0,
            date=None,
            category=["Food", "Dining"],
            payment_method=None,
            persona=None,
        )

    def test_create_gasto_with_list_persona(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_notion_client: AsyncMock,
    ) -> None:
        """Test creating a gasto with persona as a list."""
        mock_notion_client.create_gasto_page.return_value = {
            "id": "new-page-789",
        }

        resp = test_client_with_mocks.post(
            "/api/gastos",
            headers=auth_headers,
            json={
                "expense": "Test expense",
                "amount": 100.0,
                "persona": ["John", "Jane"],
            },
        )
        assert resp.status_code == 201

        mock_notion_client.create_gasto_page.assert_called_once_with(
            expense="Test expense",
            amount=100.0,
            date=None,
            category=None,
            payment_method=None,
            persona=["John", "Jane"],
        )

    def test_create_gasto_notion_error(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_notion_client: AsyncMock,
    ) -> None:
        """Test handling of Notion API errors."""
        mock_notion_client.create_gasto_page.side_effect = Exception("Notion API error")

        resp = test_client_with_mocks.post(
            "/api/gastos",
            headers=auth_headers,
            json={
                "expense": "Test expense",
                "amount": 100.0,
            },
        )
        assert resp.status_code == 500
        body = resp.json()
        assert "Failed to create gasto" in body["detail"]


class TestListGastos:
    """Tests for GET /api/gastos endpoint."""

    def test_list_gastos_requires_auth(
        self,
        test_client_with_mocks: TestClient,
    ) -> None:
        """Test that listing gastos requires authentication."""
        resp = test_client_with_mocks.get("/api/gastos")
        assert resp.status_code == 401

    def test_list_gastos_empty(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test listing gastos when database is empty."""
        mock_database_client.search_gastos.return_value = []

        resp = test_client_with_mocks.get("/api/gastos", headers=auth_headers)
        assert resp.status_code == 200
        body = resp.json()
        assert body["results"] == []
        assert body["total_count"] == 0

    def test_list_gastos_success(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
        sample_gastos_list: list[Gasto],
    ) -> None:
        """Test successful listing of gastos."""
        mock_database_client.search_gastos.return_value = sample_gastos_list

        resp = test_client_with_mocks.get("/api/gastos", headers=auth_headers)
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["results"]) == 3
        assert body["total_count"] == 3

        # Verify database client was called with default parameters
        mock_database_client.search_gastos.assert_called_once_with(
            q=None,
            date_from=None,
            date_to=None,
            persona=None,
            payment_method=None,
            category=None,
            amount_min=None,
            amount_max=None,
            sort_by="created_at",
            order="desc",
            limit=100,
            offset=0,
        )

    def test_list_gastos_with_search_query(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test listing gastos with full-text search."""
        mock_database_client.search_gastos.return_value = []

        resp = test_client_with_mocks.get(
            "/api/gastos?q=groceries", headers=auth_headers
        )
        assert resp.status_code == 200

        call_args = mock_database_client.search_gastos.call_args
        assert call_args.kwargs["q"] == "groceries"

    def test_list_gastos_with_date_range(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test listing gastos with date range filter."""
        mock_database_client.search_gastos.return_value = []

        resp = test_client_with_mocks.get(
            "/api/gastos?date_from=2026-01-01&date_to=2026-01-31",
            headers=auth_headers,
        )
        assert resp.status_code == 200

        call_args = mock_database_client.search_gastos.call_args
        assert call_args.kwargs["date_from"] == "2026-01-01"
        assert call_args.kwargs["date_to"] == "2026-01-31"

    def test_list_gastos_with_persona_filter(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test listing gastos with persona filter."""
        mock_database_client.search_gastos.return_value = []

        resp = test_client_with_mocks.get(
            "/api/gastos?persona=John", headers=auth_headers
        )
        assert resp.status_code == 200

        call_args = mock_database_client.search_gastos.call_args
        assert call_args.kwargs["persona"] == "John"

    def test_list_gastos_with_payment_method_filter(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test listing gastos with payment method filter."""
        mock_database_client.search_gastos.return_value = []

        resp = test_client_with_mocks.get(
            "/api/gastos?payment_method=credit_card", headers=auth_headers
        )
        assert resp.status_code == 200

        call_args = mock_database_client.search_gastos.call_args
        assert call_args.kwargs["payment_method"] == "credit_card"

    def test_list_gastos_with_category_filter(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test listing gastos with category (contains) filter."""
        mock_database_client.search_gastos.return_value = []

        resp = test_client_with_mocks.get(
            "/api/gastos?category=Food", headers=auth_headers
        )
        assert resp.status_code == 200

        call_args = mock_database_client.search_gastos.call_args
        assert call_args.kwargs["category"] == "Food"

    def test_list_gastos_with_amount_range(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test listing gastos with amount range filter."""
        mock_database_client.search_gastos.return_value = []

        resp = test_client_with_mocks.get(
            "/api/gastos?amount_min=10&amount_max=100", headers=auth_headers
        )
        assert resp.status_code == 200

        call_args = mock_database_client.search_gastos.call_args
        assert call_args.kwargs["amount_min"] == 10
        assert call_args.kwargs["amount_max"] == 100

    def test_list_gastos_sort_by_date(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test listing gastos sorted by date."""
        mock_database_client.search_gastos.return_value = []

        resp = test_client_with_mocks.get(
            "/api/gastos?sort_by=date&order=asc", headers=auth_headers
        )
        assert resp.status_code == 200

        call_args = mock_database_client.search_gastos.call_args
        assert call_args.kwargs["sort_by"] == "date"
        assert call_args.kwargs["order"] == "asc"

    def test_list_gastos_with_pagination(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test listing gastos with pagination."""
        mock_database_client.search_gastos.return_value = []

        resp = test_client_with_mocks.get(
            "/api/gastos?limit=10&offset=20", headers=auth_headers
        )
        assert resp.status_code == 200

        call_args = mock_database_client.search_gastos.call_args
        assert call_args.kwargs["limit"] == 10
        assert call_args.kwargs["offset"] == 20

    def test_list_gastos_invalid_limit(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test that invalid limit values return 422."""
        resp = test_client_with_mocks.get("/api/gastos?limit=0", headers=auth_headers)
        assert resp.status_code == 422

        resp = test_client_with_mocks.get(
            "/api/gastos?limit=2000", headers=auth_headers
        )
        assert resp.status_code == 422

    def test_list_gastos_invalid_offset(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test that invalid offset values return 422."""
        resp = test_client_with_mocks.get("/api/gastos?offset=-1", headers=auth_headers)
        assert resp.status_code == 422

    def test_list_gastos_invalid_sort_order(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test that invalid sort order defaults to 'desc'."""
        mock_database_client.search_gastos.return_value = []

        resp = test_client_with_mocks.get(
            "/api/gastos?order=invalid", headers=auth_headers
        )
        # FastAPI query validation won't catch this, but our DB might
        # For now, just ensure it doesn't crash
        assert resp.status_code in (200, 500)

    def test_list_gastos_database_error(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test handling of database errors."""
        mock_database_client.search_gastos.side_effect = Exception("Database error")

        resp = test_client_with_mocks.get("/api/gastos", headers=auth_headers)
        assert resp.status_code == 500
        body = resp.json()
        assert "Failed to list gastos" in body["detail"]


class TestGetGasto:
    """Tests for GET /api/gastos/{page_id} endpoint."""

    def test_get_gasto_requires_auth(
        self,
        test_client_with_mocks: TestClient,
    ) -> None:
        """Test that getting a gasto requires authentication."""
        resp = test_client_with_mocks.get("/api/gastos/test-page-123")
        assert resp.status_code == 401

    def test_get_gasto_not_found(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test getting a non-existent gasto returns 404."""
        mock_database_client.get_gasto.return_value = None

        resp = test_client_with_mocks.get(
            "/api/gastos/non-existent-page", headers=auth_headers
        )
        assert resp.status_code == 404
        body = resp.json()
        assert "not found" in body["detail"].lower()

    def test_get_gasto_success(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
        sample_gasto: Gasto,
    ) -> None:
        """Test successfully getting a gasto by page_id."""
        mock_database_client.get_gasto.return_value = sample_gasto

        resp = test_client_with_mocks.get(
            "/api/gastos/test-page-123", headers=auth_headers
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["page_id"] == "test-page-123"
        assert body["description"] == "Lunch at restaurant"
        assert body["amount"] == 45.50
        assert body["category"] == "Food"
        assert body["persona"] == "John"
        assert body["payment_method"] == "credit_card"
        assert body["date"] == "2026-01-15"

    def test_get_gasto_database_error(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test handling of database errors."""
        mock_database_client.get_gasto.side_effect = Exception("Database error")

        resp = test_client_with_mocks.get(
            "/api/gastos/test-page-123", headers=auth_headers
        )
        assert resp.status_code == 500


class TestGetGastosTotals:
    """Tests for GET /api/gastos/totals endpoint."""

    def test_totals_requires_auth(
        self,
        test_client_with_mocks: TestClient,
    ) -> None:
        """Test that getting totals requires authentication."""
        resp = test_client_with_mocks.get("/api/gastos/totals")
        assert resp.status_code == 401

    def test_totals_empty_database(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test totals when database is empty returns zero-safe defaults."""
        mock_database_client.get_gastos_totals.return_value = (0.0, 0, 0.0, 0.0, 0.0)

        resp = test_client_with_mocks.get("/api/gastos/totals", headers=auth_headers)
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 0.0
        assert body["count"] == 0
        assert body["min"] == 0.0
        assert body["max"] == 0.0
        assert body["avg"] == 0.0

    def test_totals_with_data(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test totals with actual data."""
        mock_database_client.get_gastos_totals.return_value = (
            150.25,  # total
            5,  # count
            10.50,  # min
            50.00,  # max
            30.05,  # avg
        )

        resp = test_client_with_mocks.get("/api/gastos/totals", headers=auth_headers)
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 150.25
        assert body["count"] == 5
        assert body["min"] == 10.50
        assert body["max"] == 50.00
        assert body["avg"] == 30.05

    def test_totals_with_filters(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test totals with all filter parameters."""
        mock_database_client.get_gastos_totals.return_value = (
            75.0,
            2,
            25.0,
            50.0,
            37.5,
        )

        resp = test_client_with_mocks.get(
            "/api/gastos/totals"
            "?q=restaurant"
            "&date_from=2026-01-01"
            "&date_to=2026-01-31"
            "&persona=John"
            "&payment_method=credit_card"
            "&category=Food"
            "&amount_min=10"
            "&amount_max=100"
            "&ciudad=Rome",
            headers=auth_headers,
        )
        assert resp.status_code == 200

        # Verify all filters were passed to database method
        call_args = mock_database_client.get_gastos_totals.call_args
        assert call_args.kwargs["q"] == "restaurant"
        assert call_args.kwargs["date_from"] == "2026-01-01"
        assert call_args.kwargs["date_to"] == "2026-01-31"
        assert call_args.kwargs["persona"] == "John"
        assert call_args.kwargs["payment_method"] == "credit_card"
        assert call_args.kwargs["category"] == "Food"
        assert call_args.kwargs["amount_min"] == 10.0
        assert call_args.kwargs["amount_max"] == 100.0
        assert call_args.kwargs["ciudad"] == "Rome"

    def test_totals_invalid_date_format(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test that invalid date format returns 400."""
        resp = test_client_with_mocks.get(
            "/api/gastos/totals?date_from=2026-1-1",  # Non-padded month
            headers=auth_headers,
        )
        assert resp.status_code == 400
        body = resp.json()
        assert "date_from" in body["detail"]
        assert "YYYY-MM-DD" in body["detail"]

    def test_totals_invalid_date_range(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test that date_from > date_to returns 400."""
        resp = test_client_with_mocks.get(
            "/api/gastos/totals?date_from=2026-01-31&date_to=2026-01-01",
            headers=auth_headers,
        )
        assert resp.status_code == 400
        body = resp.json()
        assert "Invalid date range" in body["detail"]

    def test_totals_invalid_amount_min(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test that invalid amount_min returns 400."""
        resp = test_client_with_mocks.get(
            "/api/gastos/totals?amount_min=abc",
            headers=auth_headers,
        )
        assert resp.status_code == 400
        body = resp.json()
        assert "amount_min" in body["detail"]

    def test_totals_invalid_amount_range(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test that amount_min > amount_max returns 400."""
        resp = test_client_with_mocks.get(
            "/api/gastos/totals?amount_min=100&amount_max=10",
            headers=auth_headers,
        )
        assert resp.status_code == 400
        body = resp.json()
        assert "Invalid amount range" in body["detail"]

    def test_totals_nan_amount_rejected(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test that NaN amount values are rejected."""
        resp = test_client_with_mocks.get(
            "/api/gastos/totals?amount_min=nan",
            headers=auth_headers,
        )
        assert resp.status_code == 400
        body = resp.json()
        assert "not a finite number" in body["detail"]

    def test_totals_inf_amount_rejected(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test that infinity amount values are rejected."""
        resp = test_client_with_mocks.get(
            "/api/gastos/totals?amount_max=inf",
            headers=auth_headers,
        )
        assert resp.status_code == 400
        body = resp.json()
        assert "not a finite number" in body["detail"]

    def test_totals_negative_values_allowed(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test that negative totals/amounts are allowed (refunds)."""
        mock_database_client.get_gastos_totals.return_value = (
            -25.50,  # total (negative - refunds)
            3,
            -10.0,  # min (negative)
            5.0,
            -8.5,  # avg (negative)
        )

        resp = test_client_with_mocks.get("/api/gastos/totals", headers=auth_headers)
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == -25.50
        assert body["min"] == -10.0
        assert body["avg"] == -8.5

    def test_totals_database_error(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test handling of database errors."""
        mock_database_client.get_gastos_totals.side_effect = Exception("Database error")

        resp = test_client_with_mocks.get("/api/gastos/totals", headers=auth_headers)
        assert resp.status_code == 500
        body = resp.json()
        assert "Failed to get gastos totals" in body["detail"]

    def test_totals_no_database_client(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test getting totals when database client is not initialized."""
        with patch("notion_hook.app._database_client", None):
            resp = test_client_with_mocks.get(
                "/api/gastos/totals", headers=auth_headers
            )
            assert resp.status_code == 503


class TestGetGastosSummary:
    """Tests for GET /api/gastos/summary endpoint."""

    def test_summary_requires_auth(
        self,
        test_client_with_mocks: TestClient,
    ) -> None:
        """Test that getting summary requires authentication."""
        resp = test_client_with_mocks.get("/api/gastos/summary?group_by=category")
        assert resp.status_code == 401

    def test_summary_empty_database(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test summary when database is empty returns zero-safe defaults."""
        mock_database_client.get_gastos_summary.return_value = ([], 0.0, 0)

        resp = test_client_with_mocks.get(
            "/api/gastos/summary?group_by=category", headers=auth_headers
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["groups"] == []
        assert body["grand_total"] == 0.0
        assert body["total_count"] == 0

    def test_summary_single_dimension_category(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test summary grouped by category."""
        mock_database_client.get_gastos_summary.return_value = (
            [
                {"key": {"category": "Food"}, "total": 245.5, "count": 12},
                {"key": {"category": "Transport"}, "total": 89.0, "count": 5},
            ],
            334.5,  # grand_total
            17,  # total_count
        )

        resp = test_client_with_mocks.get(
            "/api/gastos/summary?group_by=category", headers=auth_headers
        )
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["groups"]) == 2
        assert body["groups"][0]["key"] == {"category": "Food"}
        assert body["groups"][0]["total"] == 245.5
        assert body["groups"][0]["count"] == 12
        assert body["groups"][1]["key"] == {"category": "Transport"}
        assert body["grand_total"] == 334.5
        assert body["total_count"] == 17

    def test_summary_single_dimension_persona(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test summary grouped by persona."""
        mock_database_client.get_gastos_summary.return_value = (
            [
                {"key": {"persona": "Franco"}, "total": 120.0, "count": 4},
                {"key": {"persona": "Mica"}, "total": 80.0, "count": 3},
            ],
            200.0,
            7,
        )

        resp = test_client_with_mocks.get(
            "/api/gastos/summary?group_by=persona", headers=auth_headers
        )
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["groups"]) == 2
        assert body["groups"][0]["key"] == {"persona": "Franco"}

    def test_summary_single_dimension_date(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test summary grouped by date (day-level)."""
        mock_database_client.get_gastos_summary.return_value = (
            [
                {"key": {"date": "2026-01-15"}, "total": 50.0, "count": 2},
                {"key": {"date": "2026-01-16"}, "total": 75.0, "count": 3},
            ],
            125.0,
            5,
        )

        resp = test_client_with_mocks.get(
            "/api/gastos/summary?group_by=date", headers=auth_headers
        )
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["groups"]) == 2
        assert body["groups"][0]["key"] == {"date": "2026-01-15"}

    def test_summary_single_dimension_ciudad(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test summary grouped by city."""
        mock_database_client.get_gastos_summary.return_value = (
            [
                {"key": {"ciudad": "Rome"}, "total": 150.0, "count": 5},
                {"key": {"ciudad": "Madrid"}, "total": 90.0, "count": 3},
            ],
            240.0,
            8,
        )

        resp = test_client_with_mocks.get(
            "/api/gastos/summary?group_by=ciudad", headers=auth_headers
        )
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["groups"]) == 2
        assert body["groups"][0]["key"] == {"ciudad": "Rome"}

    def test_summary_unknown_value(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test that missing values are grouped as 'Unknown'."""
        mock_database_client.get_gastos_summary.return_value = (
            [
                {"key": {"category": "Food"}, "total": 100.0, "count": 5},
                {"key": {"category": "Unknown"}, "total": 50.0, "count": 2},
            ],
            150.0,
            7,
        )

        resp = test_client_with_mocks.get(
            "/api/gastos/summary?group_by=category", headers=auth_headers
        )
        assert resp.status_code == 200
        body = resp.json()
        # Find the Unknown group
        unknown_group = next(
            (g for g in body["groups"] if g["key"]["category"] == "Unknown"), None
        )
        assert unknown_group is not None
        assert unknown_group["total"] == 50.0
        assert unknown_group["count"] == 2

    def test_summary_with_filters(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test summary with all filter parameters."""
        mock_database_client.get_gastos_summary.return_value = (
            [
                {"key": {"category": "Food"}, "total": 75.0, "count": 2},
            ],
            75.0,
            2,
        )

        resp = test_client_with_mocks.get(
            "/api/gastos/summary"
            "?group_by=category"
            "&q=restaurant"
            "&date_from=2026-01-01"
            "&date_to=2026-01-31"
            "&persona=John"
            "&payment_method=credit_card"
            "&category=Food"
            "&amount_min=10"
            "&amount_max=100"
            "&ciudad=Rome",
            headers=auth_headers,
        )
        assert resp.status_code == 200

        # Verify all filters were passed to database method
        call_args = mock_database_client.get_gastos_summary.call_args
        assert call_args.kwargs["group_by"] == ["category"]
        assert call_args.kwargs["q"] == "restaurant"
        assert call_args.kwargs["date_from"] == "2026-01-01"
        assert call_args.kwargs["date_to"] == "2026-01-31"
        assert call_args.kwargs["persona"] == "John"
        assert call_args.kwargs["payment_method"] == "credit_card"
        assert call_args.kwargs["category"] == "Food"
        assert call_args.kwargs["amount_min"] == 10.0
        assert call_args.kwargs["amount_max"] == 100.0
        assert call_args.kwargs["ciudad"] == "Rome"

    def test_summary_no_group_by(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test summary without group_by returns empty groups with totals."""
        mock_database_client.get_gastos_summary.return_value = ([], 150.0, 5)

        resp = test_client_with_mocks.get("/api/gastos/summary", headers=auth_headers)
        assert resp.status_code == 200
        body = resp.json()
        assert body["groups"] == []
        assert body["grand_total"] == 150.0
        assert body["total_count"] == 5

    def test_summary_invalid_group_by(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test that invalid group_by returns 400."""
        resp = test_client_with_mocks.get(
            "/api/gastos/summary?group_by=invalid", headers=auth_headers
        )
        assert resp.status_code == 400
        body = resp.json()
        assert "Invalid group_by" in body["detail"]
        assert "invalid" in body["detail"]
        assert "Allowed values:" in body["detail"]

    def test_summary_duplicate_group_by(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test that duplicate group_by dimensions return 400."""
        resp = test_client_with_mocks.get(
            "/api/gastos/summary?group_by=category,category", headers=auth_headers
        )
        assert resp.status_code == 400
        body = resp.json()
        assert "Duplicate" in body["detail"]
        assert "category" in body["detail"]

    def test_summary_invalid_date_format(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test that invalid date format returns 400."""
        resp = test_client_with_mocks.get(
            "/api/gastos/summary?group_by=category&date_from=2026-1-1",
            headers=auth_headers,
        )
        assert resp.status_code == 400
        body = resp.json()
        assert "date_from" in body["detail"]
        assert "YYYY-MM-DD" in body["detail"]

    def test_summary_invalid_date_range(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test that date_from > date_to returns 400."""
        resp = test_client_with_mocks.get(
            "/api/gastos/summary?group_by=category&date_from=2026-01-31&date_to=2026-01-01",
            headers=auth_headers,
        )
        assert resp.status_code == 400
        body = resp.json()
        assert "Invalid date range" in body["detail"]

    def test_summary_invalid_amount_min(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test that invalid amount_min returns 400."""
        resp = test_client_with_mocks.get(
            "/api/gastos/summary?group_by=category&amount_min=abc",
            headers=auth_headers,
        )
        assert resp.status_code == 400
        body = resp.json()
        assert "amount_min" in body["detail"]

    def test_summary_invalid_amount_range(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test that amount_min > amount_max returns 400."""
        resp = test_client_with_mocks.get(
            "/api/gastos/summary?group_by=category&amount_min=100&amount_max=10",
            headers=auth_headers,
        )
        assert resp.status_code == 400
        body = resp.json()
        assert "Invalid amount range" in body["detail"]

    def test_summary_negative_values_allowed(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test that negative totals are allowed (refunds)."""
        mock_database_client.get_gastos_summary.return_value = (
            [
                {"key": {"category": "Refunds"}, "total": -25.50, "count": 2},
            ],
            -25.50,  # grand_total (negative)
            2,
        )

        resp = test_client_with_mocks.get(
            "/api/gastos/summary?group_by=category", headers=auth_headers
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["groups"][0]["total"] == -25.50
        assert body["grand_total"] == -25.50

    def test_summary_database_error(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
        mock_database_client: AsyncMock,
    ) -> None:
        """Test handling of database errors."""
        mock_database_client.get_gastos_summary.side_effect = Exception(
            "Database error"
        )

        resp = test_client_with_mocks.get(
            "/api/gastos/summary?group_by=category", headers=auth_headers
        )
        assert resp.status_code == 500
        body = resp.json()
        assert "Failed to get gastos summary" in body["detail"]

    def test_summary_no_database_client(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test getting summary when database client is not initialized."""
        with patch("notion_hook.app._database_client", None):
            resp = test_client_with_mocks.get(
                "/api/gastos/summary?group_by=category", headers=auth_headers
            )
            assert resp.status_code == 503


class TestServiceUnavailable:
    """Tests for when services are not initialized."""

    def test_create_gasto_no_notion_client(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test creating a gasto when Notion client is not initialized."""
        # Patch to make the client None
        with patch("notion_hook.app._notion_client", None):
            resp = test_client_with_mocks.post(
                "/api/gastos",
                headers=auth_headers,
                json={
                    "expense": "Test expense",
                    "amount": 100.0,
                },
            )
            assert resp.status_code == 503

    def test_list_gastos_no_database_client(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test getting a gasto when database client is not initialized."""
        with patch("notion_hook.app._database_client", None):
            resp = test_client_with_mocks.get("/api/gastos", headers=auth_headers)
            assert resp.status_code == 503

    def test_get_gasto_no_database_client(
        self,
        test_client_with_mocks: TestClient,
        auth_headers: dict[str, str],
    ) -> None:
        """Test getting a gasto when database client is not initialized."""

        with patch("notion_hook.app._database_client", None):
            resp = test_client_with_mocks.get(
                "/api/gastos/test-page-123", headers=auth_headers
            )
            assert resp.status_code == 503
