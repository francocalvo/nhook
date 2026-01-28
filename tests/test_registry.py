from __future__ import annotations

from notion_hook.workflows.atracciones_sync import AtraccionesSyncWorkflow
from notion_hook.workflows.pasajes_sync import PasajesSyncWorkflow
from notion_hook.workflows.registry import WorkflowRegistry


class TestWorkflowRegistry:
    """Tests for WorkflowRegistry."""

    def test_get_date_property_name_known_workflow(self, mock_notion_client) -> None:
        """Test get_date_property_name returns correct property for known workflow."""
        registry = WorkflowRegistry(mock_notion_client)
        registry.register(PasajesSyncWorkflow)
        registry.register(AtraccionesSyncWorkflow)

        assert registry.get_date_property_name("pasajes-cronograma") == "Departure"
        assert registry.get_date_property_name("atracciones-cronograma") == "Fecha"

    def test_get_date_property_name_unknown_workflow(self, mock_notion_client) -> None:
        """Test get_date_property_name returns None for unknown workflow."""
        registry = WorkflowRegistry(mock_notion_client)
        registry.register(PasajesSyncWorkflow)

        assert registry.get_date_property_name("unknown-workflow") is None

    def test_get_date_property_name_empty_string(self, mock_notion_client) -> None:
        """Test get_date_property_name returns None for empty string."""
        registry = WorkflowRegistry(mock_notion_client)
        registry.register(PasajesSyncWorkflow)

        assert registry.get_date_property_name("") is None
