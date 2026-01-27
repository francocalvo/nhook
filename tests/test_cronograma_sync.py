from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock

import pytest

from notion_hook.models.webhook import DateValue, WorkflowContext
from notion_hook.workflows.cronograma_sync import CronogramaSyncWorkflow


class TestCronogramaSyncWorkflow:
    """Tests for the CronogramaSyncWorkflow."""

    def test_matches_with_workflow_name(self, mock_notion_client: AsyncMock) -> None:
        """Test workflow matches when workflow name is atracciones-cronograma."""
        workflow = CronogramaSyncWorkflow(mock_notion_client)
        context = WorkflowContext(
            page_id="test-id",
            payload={"data": {"properties": {"Date": {"start": "2026-03-14"}}}},
            date_value=DateValue(start=date(2026, 3, 14)),
            workflow_name="atracciones-cronograma",
        )
        assert workflow.matches(context) is True

    def test_does_not_match_with_wrong_name(
        self, mock_notion_client: AsyncMock
    ) -> None:
        """Test workflow does not match when workflow name is different."""
        workflow = CronogramaSyncWorkflow(mock_notion_client)
        context = WorkflowContext(
            page_id="test-id",
            payload={"data": {"properties": {"OtherProperty": "value"}}},
            workflow_name="pasajes-cronograma",
        )
        assert workflow.matches(context) is False

    def test_expand_single_date(self, mock_notion_client: AsyncMock) -> None:
        """Test expanding a single date returns just that date."""
        workflow = CronogramaSyncWorkflow(mock_notion_client)
        dates = workflow._expand_date_range(date(2026, 3, 14), None)
        assert dates == [date(2026, 3, 14)]

    def test_expand_date_range(self, mock_notion_client: AsyncMock) -> None:
        """Test expanding a date range returns all dates inclusive."""
        workflow = CronogramaSyncWorkflow(mock_notion_client)
        dates = workflow._expand_date_range(date(2026, 3, 14), date(2026, 3, 16))
        assert dates == [
            date(2026, 3, 14),
            date(2026, 3, 15),
            date(2026, 3, 16),
        ]

    @pytest.mark.asyncio
    async def test_execute_clears_relation_when_date_is_none(
        self, mock_notion_client: AsyncMock
    ) -> None:
        """Test execute clears relations when date is None."""
        workflow = CronogramaSyncWorkflow(mock_notion_client)
        context = WorkflowContext(
            page_id="test-page-id",
            payload={"id": "test-page-id", "Date": None},
            date_value=None,
        )

        result = await workflow.execute(context)

        mock_notion_client.update_gastos_cronograma_relation.assert_called_once_with(
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
            {"id": "cronograma-2"},
        ]

        workflow = CronogramaSyncWorkflow(mock_notion_client)
        context = WorkflowContext(
            page_id="test-page-id",
            payload={"id": "test-page-id", "Date": {"start": "2026-03-14"}},
            date_value=DateValue(start=date(2026, 3, 14)),
        )

        result = await workflow.execute(context)

        mock_notion_client.find_cronograma_by_dates.assert_called_once_with(
            [date(2026, 3, 14)]
        )
        mock_notion_client.update_gastos_cronograma_relation.assert_called_once_with(
            "test-page-id", ["cronograma-1", "cronograma-2"]
        )
        assert result["updated_relations"] == ["cronograma-1", "cronograma-2"]

    @pytest.mark.asyncio
    async def test_execute_handles_date_range(
        self, mock_notion_client: AsyncMock
    ) -> None:
        """Test execute expands date range and finds all entries."""
        mock_notion_client.find_cronograma_by_dates.return_value = [
            {"id": "cronograma-1"},
            {"id": "cronograma-2"},
            {"id": "cronograma-3"},
        ]

        workflow = CronogramaSyncWorkflow(mock_notion_client)
        context = WorkflowContext(
            page_id="test-page-id",
            payload={
                "id": "test-page-id",
                "Date": {"start": "2026-03-14", "end": "2026-03-16"},
            },
            date_value=DateValue(start=date(2026, 3, 14), end=date(2026, 3, 16)),
        )

        result = await workflow.execute(context)

        mock_notion_client.find_cronograma_by_dates.assert_called_once_with(
            [date(2026, 3, 14), date(2026, 3, 15), date(2026, 3, 16)]
        )
        assert len(result["updated_relations"]) == 3
