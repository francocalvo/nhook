from __future__ import annotations

from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock

import pytest

from notion_hook.core.database import DatabaseError
from notion_hook.models.webhook import WorkflowContext
from notion_hook.workflows.atracciones_db_sync import AtraccionesDbSyncWorkflow
from notion_hook.workflows.ciudades_sync import CiudadesSyncWorkflow
from notion_hook.workflows.cronograma_db_sync import CronogramaDbSyncWorkflow
from notion_hook.workflows.gastos_sync import GastosSyncWorkflow
from notion_hook.workflows.pasajes_db_sync import PasajesDbSyncWorkflow


def _payload(
    *,
    page_id: str,
    properties: dict[str, Any],
    archived: bool = False,
    in_trash: bool = False,
) -> dict[str, Any]:
    return {
        "data": {
            "id": page_id,
            "archived": archived,
            "in_trash": in_trash,
            "created_time": "2026-01-01T00:00:00.000Z",
            "last_edited_time": "2026-01-01T00:00:00.000Z",
            "properties": properties,
        }
    }


@pytest.mark.parametrize(
    (
        "workflow_factory",
        "workflow_name",
        "props",
        "getter",
        "creator",
        "updater",
        "deleter",
    ),
    [
        (
            CiudadesSyncWorkflow,
            "ciudades-sync",
            {"Name": {"title": [{"plain_text": "Madrid"}]}},
            "get_ciudad",
            "create_ciudad",
            "update_ciudad",
            "delete_ciudad",
        ),
        (
            CronogramaDbSyncWorkflow,
            "cronograma-sync",
            {"Día": {"date": {"start": "2026-03-14", "end": None}}},
            "get_cronograma",
            "create_cronograma",
            "update_cronograma",
            "delete_cronograma",
        ),
        (
            PasajesDbSyncWorkflow,
            "pasajes-sync",
            {"Departure": {"date": {"start": "2026-03-14", "end": None}}},
            "get_pasaje",
            "create_pasaje",
            "update_pasaje",
            "delete_pasaje",
        ),
        (
            AtraccionesDbSyncWorkflow,
            "atracciones-sync",
            {
                "Name": {"title": [{"plain_text": "Museo"}]},
                "Fecha": {"date": {"start": "2026-03-14", "end": None}},
            },
            "get_atraccion",
            "create_atraccion",
            "update_atraccion",
            "delete_atraccion",
        ),
        (
            GastosSyncWorkflow,
            "gastos-sync",
            {"Expense": {"title": [{"plain_text": "Lunch"}]}},
            "get_gasto",
            "create_gasto",
            "update_gasto",
            "delete_gasto",
        ),
    ],
)
@pytest.mark.asyncio
async def test_local_sync_create_update_delete_paths(
    workflow_factory: Callable[..., Any],
    workflow_name: str,
    props: dict[str, Any],
    getter: str,
    creator: str,
    updater: str,
    deleter: str,
    mock_notion_client: AsyncMock,
    mock_database_client: AsyncMock,
) -> None:
    workflow = workflow_factory(mock_notion_client, mock_database_client)
    page_id = "test-page-id"

    getattr(mock_database_client, getter).return_value = None
    create_result = await workflow.execute(
        WorkflowContext(
            page_id=page_id,
            workflow_name=workflow_name,
            payload=_payload(page_id=page_id, properties=props),
        )
    )
    assert create_result["operation"] == "create"
    getattr(mock_database_client, creator).assert_called_once()

    getattr(mock_database_client, getter).reset_mock()
    getattr(mock_database_client, creator).reset_mock()
    getattr(mock_database_client, getter).return_value = object()

    update_result = await workflow.execute(
        WorkflowContext(
            page_id=page_id,
            workflow_name=workflow_name,
            payload=_payload(page_id=page_id, properties=props),
        )
    )
    assert update_result["operation"] == "update"
    getattr(mock_database_client, updater).assert_called_once()

    delete_result = await workflow.execute(
        WorkflowContext(
            page_id=page_id,
            workflow_name=workflow_name,
            payload=_payload(page_id=page_id, properties=props, archived=True),
        )
    )
    assert delete_result["operation"] == "delete"
    getattr(mock_database_client, deleter).assert_called_once_with(page_id)


@pytest.mark.asyncio
async def test_local_sync_empty_properties_means_delete(
    mock_notion_client: AsyncMock,
    mock_database_client: AsyncMock,
) -> None:
    workflow = CiudadesSyncWorkflow(mock_notion_client, mock_database_client)

    result = await workflow.execute(
        WorkflowContext(
            page_id="city-1",
            workflow_name="ciudades-sync",
            payload=_payload(page_id="city-1", properties={}),
        )
    )

    assert result["operation"] == "delete"
    mock_database_client.delete_ciudad.assert_called_once_with("city-1")


@pytest.mark.asyncio
async def test_local_sync_logs_failures_on_database_error(
    mock_notion_client: AsyncMock,
    mock_database_client: AsyncMock,
) -> None:
    workflow = CiudadesSyncWorkflow(mock_notion_client, mock_database_client)
    mock_database_client.get_ciudad.return_value = None
    mock_database_client.create_ciudad.side_effect = DatabaseError("fk failed")

    with pytest.raises(DatabaseError):
        await workflow.execute(
            WorkflowContext(
                page_id="city-1",
                workflow_name="ciudades-sync",
                payload=_payload(
                    page_id="city-1",
                    properties={"Name": {"title": [{"plain_text": "Madrid"}]}},
                ),
            )
        )

    mock_database_client.log_failure.assert_called_once()
