from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from notion_hook.models.gastos import Gasto
from notion_hook.models.notion_db import Ciudad
from notion_hook.services.notion_reload import (
    JobStatus,
    NotionReloadService,
    ReloadMode,
)


def _page(
    page_id: str,
    properties: dict[str, object],
) -> dict[str, object]:
    return {
        "id": page_id,
        "properties": properties,
        "created_time": "2026-01-01T00:00:00Z",
        "last_edited_time": "2026-01-01T00:00:00Z",
    }


@pytest.mark.asyncio
async def test_execute_full_reload_syncs_all_tables_in_order() -> None:
    notion_client = AsyncMock()
    database_client = AsyncMock()

    notion_client.query_all_ciudades.return_value = [
        _page(
            "city-1",
            {
                "Name": {
                    "title": [{"type": "text", "text": {"content": "Buenos Aires"}}]
                }
            },
        )
    ]
    notion_client.query_all_cronograma.return_value = [
        _page(
            "cron-1",
            {
                "Día": {"title": [{"type": "text", "text": {"content": "2026-03-14"}}]},
                "Ciudad": {"relation": [{"id": "city-1"}]},
            },
        )
    ]
    notion_client.query_all_pasajes.return_value = [
        _page(
            "pas-1",
            {
                "Departure": {"date": {"start": "2026-03-14", "end": None}},
                "Cronograma": {"relation": [{"id": "cron-1"}]},
                "Ciudad": {"relation": [{"id": "city-1"}]},
            },
        )
    ]
    notion_client.query_all_atracciones.return_value = [
        _page(
            "atr-1",
            {
                "Name": {"title": [{"type": "text", "text": {"content": "Obelisco"}}]},
                "Fecha": {"date": {"start": "2026-03-14", "end": None}},
                "Cronograma": {"relation": [{"id": "cron-1"}]},
                "Ciudad": {"relation": [{"id": "city-1"}]},
            },
        )
    ]
    notion_client.query_all_gastos.return_value = [
        _page(
            "gas-1",
            {
                "Expense": {"title": [{"type": "text", "text": {"content": "Lunch"}}]},
                "Amount": {"number": 10.0},
                "Date": {"date": {"start": "2026-03-14", "end": None}},
            },
        )
    ]

    database_client.clear_sync_tables.return_value = {
        "atracciones": 0,
        "pasajes": 0,
        "cronograma": 0,
        "ciudades": 0,
        "gastos": 0,
    }

    sync_order: list[str] = []

    async def _sync_ciudades(
        rows: list[object], *, update_if_changed: bool
    ) -> tuple[int, int, int, int]:
        sync_order.append("ciudades")
        return (len(rows), 0, 0, 0)

    async def _sync_cronograma(
        rows: list[object], *, update_if_changed: bool
    ) -> tuple[int, int, int, int]:
        sync_order.append("cronograma")
        return (len(rows), 0, 0, 0)

    async def _sync_pasajes(
        rows: list[object], *, update_if_changed: bool
    ) -> tuple[int, int, int, int]:
        sync_order.append("pasajes")
        return (len(rows), 0, 0, 0)

    async def _sync_atracciones(
        rows: list[object], *, update_if_changed: bool
    ) -> tuple[int, int, int, int]:
        sync_order.append("atracciones")
        return (len(rows), 0, 0, 0)

    async def _sync_gastos(
        rows: list[object], *, update_if_changed: bool
    ) -> tuple[int, int, int, int]:
        sync_order.append("gastos")
        return (len(rows), 0, 0, 0)

    database_client.sync_ciudades_batch.side_effect = _sync_ciudades
    database_client.sync_cronograma_batch.side_effect = _sync_cronograma
    database_client.sync_pasajes_batch.side_effect = _sync_pasajes
    database_client.sync_atracciones_batch.side_effect = _sync_atracciones
    database_client.sync_gastos_batch.side_effect = _sync_gastos

    service = NotionReloadService(notion_client, database_client)
    job_id = await service.create_job(mode=ReloadMode.FULL, batch_size=10)
    await service._execute_job(job_id)

    job = service.get_job(job_id)
    assert job is not None
    assert job.status == JobStatus.COMPLETED
    assert sync_order == ["ciudades", "cronograma", "pasajes", "atracciones", "gastos"]
    assert job.progress.created == 5
    assert job.progress.failed == 0


@pytest.mark.asyncio
async def test_execute_full_reload_resolves_gasto_ciudad_name() -> None:
    notion_client = AsyncMock()
    database_client = AsyncMock()

    notion_client.query_all_ciudades.return_value = []
    notion_client.query_all_cronograma.return_value = []
    notion_client.query_all_pasajes.return_value = []
    notion_client.query_all_atracciones.return_value = []
    notion_client.query_all_gastos.return_value = [
        _page(
            "gas-1",
            {
                "Expense": {"title": [{"type": "text", "text": {"content": "Lunch"}}]},
                "Amount": {"number": 10.0},
                "Date": {"date": {"start": "2026-03-14", "end": None}},
                "Ciudad": {"relation": [{"id": "city-1"}]},
            },
        )
    ]

    database_client.clear_sync_tables.return_value = {
        "atracciones": 0,
        "pasajes": 0,
        "cronograma": 0,
        "ciudades": 0,
        "gastos": 0,
    }
    database_client.sync_ciudades_batch.return_value = (0, 0, 0, 0)
    database_client.sync_cronograma_batch.return_value = (0, 0, 0, 0)
    database_client.sync_pasajes_batch.return_value = (0, 0, 0, 0)
    database_client.sync_atracciones_batch.return_value = (0, 0, 0, 0)
    database_client.get_ciudad.return_value = Ciudad(
        page_id="city-1",
        name="Buenos Aires",
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
    )

    async def _sync_gastos(
        rows: list[Gasto], *, update_if_changed: bool
    ) -> tuple[int, int, int, int]:
        assert len(rows) == 1
        assert rows[0].ciudad_page_id == "city-1"
        assert rows[0].ciudad == "Buenos Aires"
        return (1, 0, 0, 0)

    database_client.sync_gastos_batch.side_effect = _sync_gastos

    service = NotionReloadService(notion_client, database_client)
    job_id = await service.create_job(mode=ReloadMode.FULL, batch_size=10)
    await service._execute_job(job_id)

    job = service.get_job(job_id)
    assert job is not None
    assert job.status == JobStatus.COMPLETED
    database_client.get_ciudad.assert_called_once_with("city-1")
    notion_client.get_page.assert_not_called()
