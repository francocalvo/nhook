from __future__ import annotations

from fastapi import APIRouter

from notion_hook.api.gastos import router as gastos_router
from notion_hook.api.health import router as health_router
from notion_hook.api.reload import router as reload_router
from notion_hook.api.webhooks import router as webhooks_router

api_router = APIRouter()

api_router.include_router(gastos_router)
api_router.include_router(health_router)
api_router.include_router(reload_router)
api_router.include_router(webhooks_router)
