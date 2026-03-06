from __future__ import annotations

from notion_hook.models.gastos import Gasto


class TestGastoModel:
    """Tests for the Gasto model."""

    def test_from_notion_properties_with_ciudad_relation(self) -> None:
        """Test parsing gasto with ciudad relation from Notion properties."""
        properties = {
            "Expense": {"title": [{"plain_text": "Dinner in Rome"}]},
            "Amount": {"number": 50.0},
            "Date": {"date": {"start": "2024-01-15"}},
            "Payment Method": {"select": {"name": "Credit Card"}},
            "Category": {"multi_select": [{"name": "Food"}, {"name": "Restaurant"}]},
            "Persona": {"multi_select": [{"name": "Franco"}]},
            "Ciudad": {"relation": [{"id": "ciudad-rome-123"}]},
        }

        gasto = Gasto.from_notion_properties(
            page_id="gasto-123",
            properties=properties,
            created_time="2024-01-15T10:00:00Z",
            last_edited_time="2024-01-15T10:00:00Z",
        )

        assert gasto.page_id == "gasto-123"
        assert gasto.description == "Dinner in Rome"
        assert gasto.amount == 50.0
        assert gasto.date == "2024-01-15"
        assert gasto.payment_method == "Credit Card"
        assert gasto.category == "Food, Restaurant"
        assert gasto.persona == "Franco"
        assert gasto.ciudad_page_id == "ciudad-rome-123"
        assert gasto.ciudad is None  # Not resolved during parsing
        assert gasto.created_at == "2024-01-15T10:00:00Z"
        assert gasto.updated_at == "2024-01-15T10:00:00Z"

    def test_from_notion_properties_without_ciudad_relation(self) -> None:
        """Test parsing gasto without ciudad relation."""
        properties = {
            "Expense": {"title": [{"plain_text": "Coffee"}]},
            "Amount": {"number": 5.0},
            "Date": {"date": {"start": "2024-01-16"}},
        }

        gasto = Gasto.from_notion_properties(
            page_id="gasto-456",
            properties=properties,
            created_time="2024-01-16T10:00:00Z",
            last_edited_time="2024-01-16T10:00:00Z",
        )

        assert gasto.page_id == "gasto-456"
        assert gasto.description == "Coffee"
        assert gasto.amount == 5.0
        assert gasto.date == "2024-01-16"
        assert gasto.ciudad_page_id is None
        assert gasto.ciudad is None

    def test_from_notion_properties_with_empty_ciudad_relation(self) -> None:
        """Test parsing gasto with empty ciudad relation list."""
        properties = {
            "Expense": {"title": [{"plain_text": "Train ticket"}]},
            "Amount": {"number": 25.0},
            "Date": {"date": {"start": "2024-01-17"}},
            "Ciudad": {"relation": []},
        }

        gasto = Gasto.from_notion_properties(
            page_id="gasto-789",
            properties=properties,
            created_time="2024-01-17T10:00:00Z",
            last_edited_time="2024-01-17T10:00:00Z",
        )

        assert gasto.page_id == "gasto-789"
        assert gasto.ciudad_page_id is None
        assert gasto.ciudad is None

    def test_from_notion_properties_ciudad_case_insensitive(self) -> None:
        """Test parsing ciudad relation with case-insensitive property name."""
        properties = {
            "Expense": {"title": [{"plain_text": "Museum ticket"}]},
            "Amount": {"number": 15.0},
            "Date": {"date": {"start": "2024-01-18"}},
            "ciudad": {"relation": [{"id": "ciudad-paris-456"}]},  # lowercase
        }

        gasto = Gasto.from_notion_properties(
            page_id="gasto-abc",
            properties=properties,
            created_time="2024-01-18T10:00:00Z",
            last_edited_time="2024-01-18T10:00:00Z",
        )

        assert gasto.page_id == "gasto-abc"
        assert gasto.ciudad_page_id == "ciudad-paris-456"

    def test_from_notion_properties_with_multiple_ciudad_relations(self) -> None:
        """Test parsing gasto with multiple ciudad relations (takes first)."""
        properties = {
            "Expense": {"title": [{"plain_text": "Multi-city expense"}]},
            "Amount": {"number": 100.0},
            "Date": {"date": {"start": "2024-01-19"}},
            "Ciudad": {
                "relation": [
                    {"id": "ciudad-rome-123"},
                    {"id": "ciudad-paris-456"},
                ]
            },
        }

        gasto = Gasto.from_notion_properties(
            page_id="gasto-multi",
            properties=properties,
            created_time="2024-01-19T10:00:00Z",
            last_edited_time="2024-01-19T10:00:00Z",
        )

        # Should take the first ciudad in the relation list
        assert gasto.ciudad_page_id == "ciudad-rome-123"
        assert gasto.ciudad is None

    def test_from_notion_properties_with_all_fields(self) -> None:
        """Test parsing gasto with all fields populated."""
        properties = {
            "Expense": {"title": [{"plain_text": "Full expense"}]},
            "Amount": {"number": 75.5},
            "Date": {"date": {"start": "2024-02-01"}},
            "Payment Method": {"select": {"name": "Cash"}},
            "Category": {"multi_select": [{"name": "Transport"}, {"name": "Taxi"}]},
            "Persona": {"multi_select": [{"name": "Franco"}, {"name": "Mica"}]},
            "Ciudad": {"relation": [{"id": "ciudad-barcelona-789"}]},
        }

        gasto = Gasto.from_notion_properties(
            page_id="gasto-full",
            properties=properties,
            created_time="2024-02-01T12:00:00Z",
            last_edited_time="2024-02-01T12:30:00Z",
        )

        assert gasto.page_id == "gasto-full"
        assert gasto.description == "Full expense"
        assert gasto.amount == 75.5
        assert gasto.date == "2024-02-01"
        assert gasto.payment_method == "Cash"
        assert gasto.category == "Transport, Taxi"
        assert gasto.persona == "Franco, Mica"
        assert gasto.ciudad_page_id == "ciudad-barcelona-789"
        assert gasto.ciudad is None
        assert gasto.created_at == "2024-02-01T12:00:00Z"
        assert gasto.updated_at == "2024-02-01T12:30:00Z"
