from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException, status
from pydantic import BaseModel, Field

from notion_hook.core.auth import verify_webhook_key
from notion_hook.core.logging import get_logger
from notion_hook.services.relation_relink import (
    RelationRelinkService,
    RelinkAlreadyRunningError,
)

logger = get_logger("api.relation_relink")

router = APIRouter(tags=["relink"], prefix="/api")


def get_relink_service() -> RelationRelinkService:
    """Get the relation relink service instance."""
    from notion_hook.app import _relink_service

    if _relink_service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Relation relink service not initialized",
        )
    return _relink_service


class RelinkRequest(BaseModel):
    """Request model for relation relink endpoint."""

    batch_size: int = Field(
        default=100,
        ge=1,
        le=100,
        description="Number of records per batch",
    )


@router.post("/relink")
async def trigger_relink(
    body: RelinkRequest = Body(...),
    _: Annotated[str, Depends(verify_webhook_key)] = "",
) -> dict[str, str | int]:
    """Trigger bulk re-link of Cronograma relations across all tables."""
    service = get_relink_service()
    try:
        job_id = await service.create_job(body.batch_size)
    except RelinkAlreadyRunningError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e),
        ) from e

    await service.start_relink(job_id)
    logger.info(f"Triggered relink job {job_id} with batch_size={body.batch_size}")
    return {
        "job_id": job_id,
        "status": "started",
        "message": "Relation relink job started",
        "batch_size": body.batch_size,
    }


@router.get("/relink/{job_id}")
async def get_relink_status(
    job_id: str,
    _: Annotated[str, Depends(verify_webhook_key)] = "",
) -> dict[str, object]:
    """Get relation relink job status by job id."""
    service = get_relink_service()
    job = service.get_job(job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )
    return job.to_dict()
