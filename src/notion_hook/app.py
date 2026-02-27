from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import uvicorn
from fastapi import FastAPI

from notion_hook.api.routes import api_router
from notion_hook.clients.notion import NotionClient
from notion_hook.config import get_settings
from notion_hook.core.database import DatabaseClient
from notion_hook.core.logging import setup_logging
from notion_hook.core.middleware import LoggingMiddleware
from notion_hook.services.gastos_reload import GastosReloadService
from notion_hook.services.notion_reload import NotionReloadService
from notion_hook.workflows.atracciones_db_sync import AtraccionesDbSyncWorkflow
from notion_hook.workflows.atracciones_sync import AtraccionesSyncWorkflow
from notion_hook.workflows.ciudades_sync import CiudadesSyncWorkflow
from notion_hook.workflows.cronograma_db_sync import CronogramaDbSyncWorkflow
from notion_hook.workflows.cronograma_sync import CronogramaSyncWorkflow
from notion_hook.workflows.gastos_sync import GastosSyncWorkflow
from notion_hook.workflows.pasajes_db_sync import PasajesDbSyncWorkflow
from notion_hook.workflows.pasajes_sync import PasajesSyncWorkflow
from notion_hook.workflows.registry import WorkflowRegistry

if TYPE_CHECKING:
    pass

_workflow_registry: WorkflowRegistry | None = None
_notion_client: NotionClient | None = None
_database_client: DatabaseClient | None = None
_reload_service: GastosReloadService | None = None
_full_reload_service: NotionReloadService | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan context manager.

    Initializes Notion client, database client, and workflow registry on startup,
    and cleans up on shutdown.
    """
    global _workflow_registry, _notion_client, _database_client, _reload_service
    global _full_reload_service

    settings = get_settings()
    logger = setup_logging(settings.debug)
    logger.info("Starting notion-hook server")
    logger.debug("DEBUG mode enabled")

    created_notion_client = False
    created_database_client = False
    created_registry = False
    created_reload_service = False
    created_full_reload_service = False

    if _notion_client is None:
        _notion_client = NotionClient(settings)
        await _notion_client.__aenter__()
        created_notion_client = True

    if _database_client is None:
        _database_client = DatabaseClient(settings)
        await _database_client.initialize()
        created_database_client = True

    if _workflow_registry is None:
        _workflow_registry = WorkflowRegistry(_notion_client, _database_client)
        _workflow_registry.register(CiudadesSyncWorkflow)
        _workflow_registry.register(CronogramaDbSyncWorkflow)
        _workflow_registry.register(PasajesDbSyncWorkflow)
        _workflow_registry.register(AtraccionesDbSyncWorkflow)
        _workflow_registry.register(CronogramaSyncWorkflow)
        _workflow_registry.register(PasajesSyncWorkflow)
        _workflow_registry.register(GastosSyncWorkflow)
        _workflow_registry.register(AtraccionesSyncWorkflow)
        created_registry = True

    if _reload_service is None:
        _reload_service = GastosReloadService(_notion_client, _database_client)
        created_reload_service = True

    if _full_reload_service is None:
        _full_reload_service = NotionReloadService(_notion_client, _database_client)
        created_full_reload_service = True

    logger.info(f"Registered {len(_workflow_registry.workflows)} workflows")

    yield

    if created_notion_client and _notion_client:
        await _notion_client.__aexit__(None, None, None)
        _notion_client = None

    if created_database_client and _database_client:
        await _database_client.close()
        _database_client = None

    if created_registry:
        _workflow_registry = None

    if created_reload_service:
        _reload_service = None

    if created_full_reload_service:
        _full_reload_service = None

    logger.info("Shutdown complete")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application.

    Returns:
        Configured FastAPI application instance.
    """
    app = FastAPI(
        title="Notion Hook",
        description="Webhook server for Notion automations",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(LoggingMiddleware)
    app.include_router(api_router)

    return app


app = create_app()


def main() -> None:
    """Entry point for the application."""
    settings = get_settings()
    uvicorn.run(
        "notion_hook.app:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
    )


if __name__ == "__main__":
    main()
