from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException, status
from pydantic import BaseModel, Field

from notion_hook.core.auth import verify_webhook_key
from notion_hook.core.logging import get_logger
from notion_hook.services.notion_reload import (
    NotionReloadService,
    ReloadAlreadyRunningError,
    ReloadMode,
)

logger = get_logger("api.full_reload")

router = APIRouter(tags=["reload"], prefix="/api")


def get_full_reload_service() -> NotionReloadService:
    """Get the full reload service instance."""
    from notion_hook.app import _full_reload_service

    if _full_reload_service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Full reload service not initialized",
        )
    return _full_reload_service


class FullReloadRequest(BaseModel):
    """Request model for full DB reload endpoint."""

    mode: ReloadMode = Field(
        default=ReloadMode.FULL, description="Reload mode (full or incremental)"
    )
    batch_size: int = Field(
        default=100,
        ge=1,
        le=100,
        description="Number of records per batch",
    )
    delete_missing: bool = Field(
        default=True, description="Whether to delete records not in Notion"
    )


@router.post("/reload")
async def trigger_full_reload(
    body: FullReloadRequest = Body(...),
    _: Annotated[str, Depends(verify_webhook_key)] = "",
) -> dict[str, str | int]:
    """Trigger full/incremental reload for all configured Notion databases."""
    service = get_full_reload_service()
    try:
        job_id = await service.create_job(
            body.mode, body.batch_size, body.delete_missing
        )
    except ReloadAlreadyRunningError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e),
        ) from e

    await service.start_reload(job_id)
    logger.info(
        f"Triggered full reload job {job_id} with mode={body.mode.value}, "
        f"batch_size={body.batch_size}"
    )
    return {
        "job_id": job_id,
        "status": "started",
        "message": "Full reload job started",
        "mode": body.mode.value,
        "batch_size": body.batch_size,
    }


@router.post("/reload/all")
async def trigger_reload_all_databases(
    body: FullReloadRequest = Body(...),
    _: Annotated[str, Depends(verify_webhook_key)] = "",
) -> dict[str, str | int]:
    """Alias endpoint to explicitly trigger all-databases reload."""
    return await trigger_full_reload(body, _)


@router.get("/reload/{job_id}")
async def get_full_reload_status(
    job_id: str,
    _: Annotated[str, Depends(verify_webhook_key)] = "",
) -> dict[str, object]:
    """Get full DB reload status by job id."""
    service = get_full_reload_service()
    job = service.get_job(job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )
    return job.to_dict()
