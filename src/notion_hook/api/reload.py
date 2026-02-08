from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException, status
from pydantic import BaseModel, Field

from notion_hook.core.auth import verify_webhook_key
from notion_hook.core.logging import get_logger
from notion_hook.services.gastos_reload import (
    GastosReloadService,
    ReloadAlreadyRunningError,
    ReloadMode,
)

logger = get_logger("api.reload")

router = APIRouter(tags=["gastos"], prefix="/api/gastos")


def get_reload_service() -> GastosReloadService:
    """Get the reload service instance.

    Returns:
        The GastosReloadService instance.

    Raises:
        HTTPException: If service is not initialized.
    """
    from notion_hook.app import _reload_service

    if _reload_service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Reload service not initialized",
        )
    return _reload_service


class ReloadRequest(BaseModel):
    """Request model for reload endpoint."""

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
async def trigger_reload(
    body: ReloadRequest = Body(...),
    _: Annotated[str, Depends(verify_webhook_key)] = "",
) -> dict[str, str | int]:
    """Trigger a full reload of Gastos from Notion.

    Args:
        body: The reload request payload.
        _: The validated webhook key (from dependency).

    Returns:
        Job ID and status information.

    Raises:
        HTTPException: If authentication fails or invalid request.
    """
    service = get_reload_service()
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
        f"Triggered reload job {job_id} with mode={body.mode.value}, "
        f"batch_size={body.batch_size}"
    )

    return {
        "job_id": job_id,
        "status": "started",
        "message": "Reload job started",
        "mode": body.mode.value,
        "batch_size": body.batch_size,
    }


@router.get("/reload/{job_id}")
async def get_reload_status(
    job_id: str,
    _: Annotated[str, Depends(verify_webhook_key)] = "",
) -> dict[str, object]:
    """Get the status of a reload job.

    Args:
        job_id: The job ID.
        _: The validated webhook key (from dependency).

    Returns:
        Job status information.

    Raises:
        HTTPException: If authentication fails or job not found.
    """
    service = get_reload_service()
    job = service.get_job(job_id)

    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )

    return job.to_dict()
