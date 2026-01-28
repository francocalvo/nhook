from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock

import pytest

from notion_hook.models.webhook import DateValue, WorkflowContext
from notion_hook.workflows.pasajes_sync import PasajesSyncWorkflow


class TestPasajesSyncWorkflow:
    def test_matches_with_correct_workflow_name(
        self, mock_notion_client: AsyncMock
    ) -> None:
        workflow = PasajesSyncWorkflow(mock_notion_client)
        context = WorkflowContext(
            page_id="test-id",
            payload={"data": {"properties": {"Departure": {"start": "2026-03-14"}}}},
            departure_value=DateValue(start=date(2026, 3, 14)),
            workflow_name="pasajes-cronograma",
        )
        assert workflow.matches(context) is True

    def test_matches_with_lowercase_departure(
        self, mock_notion_client: AsyncMock
    ) -> None:
        workflow = PasajesSyncWorkflow(mock_notion_client)
        context = WorkflowContext(
            page_id="test-id",
            payload={"data": {"properties": {"departure": {"start": "2026-03-14"}}}},
            departure_value=DateValue(start=date(2026, 3, 14)),
            workflow_name="pasajes-cronograma",
        )
        assert workflow.matches(context) is True

    def test_does_not_match_wrong_workflow_name(
        self, mock_notion_client: AsyncMock
    ) -> None:
        workflow = PasajesSyncWorkflow(mock_notion_client)
        context = WorkflowContext(
            page_id="test-id",
            payload={"data": {"properties": {"OtherProperty": "value"}}},
            workflow_name="gastos-cronograma",
        )
        assert workflow.matches(context) is False

    @pytest.mark.asyncio
    async def test_execute_clears_relation_when_departure_is_none(
        self, mock_notion_client: AsyncMock
    ) -> None:
        workflow = PasajesSyncWorkflow(mock_notion_client)
        context = WorkflowContext(
            page_id="test-page-id",
            payload={"id": "test-page-id"},
            departure_value=None,
            workflow_name="pasajes-cronograma",
        )

        result = await workflow.execute(context)

        mock_notion_client.update_pasajes_cronograma_relation.assert_called_once_with(
            "test-page-id", []
        )
        assert result["updated_relations"] == []

    @pytest.mark.asyncio
    async def test_execute_finds_and_links_cronograma_entries(
        self, mock_notion_client: AsyncMock
    ) -> None:
        mock_notion_client.find_cronograma_by_dates.return_value = [
            {"id": "cronograma-1"},
        ]

        workflow = PasajesSyncWorkflow(mock_notion_client)
        context = WorkflowContext(
            page_id="test-page-id",
            payload={
                "id": "test-page-id",
                "Departure": {"start": "2026-03-14"},
            },
            departure_value=DateValue(start=date(2026, 3, 14)),
            workflow_name="pasajes-cronograma",
        )

        result = await workflow.execute(context)

        mock_notion_client.find_cronograma_by_dates.assert_called_once_with(
            [date(2026, 3, 14)]
        )
        mock_notion_client.update_pasajes_cronograma_relation.assert_called_once_with(
            "test-page-id", ["cronograma-1"]
        )
        assert result["updated_relations"] == ["cronograma-1"]
