from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock

import pytest

from notion_hook.models.webhook import DateValue, WorkflowContext
from notion_hook.workflows.atracciones_sync import AtraccionesSyncWorkflow


class TestDateValue:
    """Tests for DateValue model with datetime support."""

    def test_parse_date_string(self) -> None:
        """Test DateValue parses date-only strings."""
        fecha_dict = {"start": "2026-03-14"}
        parsed = DateValue.model_validate(fecha_dict)
        assert parsed.start == date(2026, 3, 14)
        assert parsed.end is None

    def test_parse_datetime_string(self) -> None:
        """Test DateValue parses datetime strings and extracts date."""
        fecha_dict = {"start": "2026-03-14T10:30:00.000Z"}
        parsed = DateValue.model_validate(fecha_dict)
        # Time portion should be ignored, only date kept
        assert parsed.start == date(2026, 3, 14)
        assert parsed.end is None

    def test_parse_datetime_with_timezone(self) -> None:
        """Test DateValue handles datetime with timezone."""
        fecha_dict = {"start": "2026-03-14T10:30:00+03:00"}
        parsed = DateValue.model_validate(fecha_dict)
        assert parsed.start == date(2026, 3, 14)

    def test_parse_date_range(self) -> None:
        """Test DateValue parses date range."""
        fecha_dict = {"start": "2026-03-14", "end": "2026-03-16"}
        parsed = DateValue.model_validate(fecha_dict)
        assert parsed.start == date(2026, 3, 14)
        assert parsed.end == date(2026, 3, 16)

    def test_parse_datetime_range(self) -> None:
        """Test DateValue parses datetime range and extracts dates."""
        fecha_dict = {
            "start": "2026-03-14T10:30:00.000Z",
            "end": "2026-03-16T18:45:00.000Z",
        }
        parsed = DateValue.model_validate(fecha_dict)
        assert parsed.start == date(2026, 3, 14)
        assert parsed.end == date(2026, 3, 16)

    def test_invalid_date_string_raises_error(self) -> None:
        """Test DateValue raises error for invalid date string."""
        with pytest.raises(ValueError, match="Unable to parse date string"):
            DateValue.model_validate({"start": "not-a-date"})


class TestAtraccionesSyncWorkflow:
    """Tests for the AtraccionesSyncWorkflow."""

    def test_matches_with_correct_workflow_name(
        self, mock_notion_client: AsyncMock
    ) -> None:
        """Test workflow matches when workflow name is atracciones-cronograma."""
        workflow = AtraccionesSyncWorkflow(mock_notion_client)
        context = WorkflowContext(
            page_id="test-id",
            payload={"data": {"properties": {"Fecha": {"start": "2026-03-14"}}}},
            date_value=DateValue(start=date(2026, 3, 14)),
            workflow_name="atracciones-cronograma",
        )
        assert workflow.matches(context) is True

    def test_does_not_match_wrong_workflow_name(
        self, mock_notion_client: AsyncMock
    ) -> None:
        """Test workflow does not match when workflow name is different."""
        workflow = AtraccionesSyncWorkflow(mock_notion_client)
        context = WorkflowContext(
            page_id="test-id",
            payload={"data": {"properties": {"OtherProperty": "value"}}},
            workflow_name="pasajes-cronograma",
        )
        assert workflow.matches(context) is False

    @pytest.mark.asyncio
    async def test_execute_clears_relation_when_fecha_is_none(
        self, mock_notion_client: AsyncMock
    ) -> None:
        """Test execute clears relations when Fecha is None."""
        workflow = AtraccionesSyncWorkflow(mock_notion_client)
        context = WorkflowContext(
            page_id="test-page-id",
            payload={"id": "test-page-id"},
            date_value=None,
            workflow_name="atracciones-cronograma",
        )

        result = await workflow.execute(context)

        mock_notion_client.update_atracciones_cronograma_relation.assert_called_once_with(
            "test-page-id", []
        )
        assert result["updated_relations"] == []

    @pytest.mark.asyncio
    async def test_execute_finds_and_links_cronograma_entries(
        self, mock_notion_client: AsyncMock
    ) -> None:
        """Test execute finds Cronograma entries and links them."""
        mock_notion_client.find_cronograma_by_dates.return_value = [
            {"id": "cronograma-1"},
        ]

        workflow = AtraccionesSyncWorkflow(mock_notion_client)
        context = WorkflowContext(
            page_id="test-page-id",
            payload={
                "id": "test-page-id",
                "Fecha": {"start": "2026-03-14"},
            },
            date_value=DateValue(start=date(2026, 3, 14)),
            workflow_name="atracciones-cronograma",
        )

        result = await workflow.execute(context)

        mock_notion_client.find_cronograma_by_dates.assert_called_once_with(
            [date(2026, 3, 14)]
        )
        mock_notion_client.update_atracciones_cronograma_relation.assert_called_once_with(
            "test-page-id", ["cronograma-1"]
        )
        assert result["updated_relations"] == ["cronograma-1"]

    @pytest.mark.asyncio
    async def test_execute_handles_datetime_fecha(
        self, mock_notion_client: AsyncMock
    ) -> None:
        """Test execute handles datetime values correctly (time portion ignored)."""
        mock_notion_client.find_cronograma_by_dates.return_value = [
            {"id": "cronograma-1"},
        ]

        # Test actual parsing of datetime string through DateValue
        fecha_dict = {"start": "2026-03-14T10:30:00.000Z"}
        parsed_fecha = DateValue.model_validate(fecha_dict)

        # Verify datetime string is parsed and normalized to date
        assert parsed_fecha.start == date(2026, 3, 14)

        workflow = AtraccionesSyncWorkflow(mock_notion_client)
        context = WorkflowContext(
            page_id="test-page-id",
            payload={
                "id": "test-page-id",
                "Fecha": {"start": "2026-03-14T10:30:00.000Z"},
            },
            date_value=parsed_fecha,
            workflow_name="atracciones-cronograma",
        )

        result = await workflow.execute(context)

        # Verify that only the date portion (without time) is used for matching
        mock_notion_client.find_cronograma_by_dates.assert_called_once_with(
            [date(2026, 3, 14)]
        )
        mock_notion_client.update_atracciones_cronograma_relation.assert_called_once_with(
            "test-page-id", ["cronograma-1"]
        )
        assert result["updated_relations"] == ["cronograma-1"]

    @pytest.mark.asyncio
    async def test_execute_handles_no_matching_cronograma(
        self, mock_notion_client: AsyncMock
    ) -> None:
        """Test execute handles no matching Cronograma entries."""
        mock_notion_client.find_cronograma_by_dates.return_value = []

        workflow = AtraccionesSyncWorkflow(mock_notion_client)
        context = WorkflowContext(
            page_id="test-page-id",
            payload={
                "id": "test-page-id",
                "Fecha": {"start": "2026-03-14"},
            },
            date_value=DateValue(start=date(2026, 3, 14)),
            workflow_name="atracciones-cronograma",
        )

        result = await workflow.execute(context)

        mock_notion_client.find_cronograma_by_dates.assert_called_once_with(
            [date(2026, 3, 14)]
        )
        mock_notion_client.update_atracciones_cronograma_relation.assert_called_once_with(
            "test-page-id", []
        )
        assert result["updated_relations"] == []
