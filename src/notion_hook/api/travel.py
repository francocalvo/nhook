from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from notion_hook.core.auth import verify_webhook_key
from notion_hook.core.database import DatabaseClient
from notion_hook.core.logging import get_logger
from notion_hook.models.notion_db import Atraccion, Ciudad, Cronograma, Pasaje

logger = get_logger("api.travel")

router = APIRouter(tags=["travel"], prefix="/api")


def get_database_client() -> DatabaseClient:
    """Get the database client instance."""
    from notion_hook.app import _database_client

    if _database_client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database client not initialized",
        )
    return _database_client


# --- Response models ---


class CiudadListResponse(BaseModel):
    results: list[Ciudad] = Field(..., description="List of ciudades")
    total_count: int = Field(..., description="Number of returned records")


class CronogramaListResponse(BaseModel):
    results: list[Cronograma] = Field(..., description="List of cronograma entries")
    total_count: int = Field(..., description="Number of returned records")


class PasajeListResponse(BaseModel):
    results: list[Pasaje] = Field(..., description="List of pasajes")
    total_count: int = Field(..., description="Number of returned records")


class AtraccionListResponse(BaseModel):
    results: list[Atraccion] = Field(..., description="List of atracciones")
    total_count: int = Field(..., description="Number of returned records")


# --- Ciudades ---


@router.get("/ciudades", response_model=CiudadListResponse)
async def list_ciudades(
    db: Annotated[DatabaseClient, Depends(get_database_client)],
    _: Annotated[str, Depends(verify_webhook_key)] = "",
    name: str | None = Query(None, description="Filter by name (contains)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Results to skip"),
) -> CiudadListResponse:
    """List ciudades with optional name filter."""
    try:
        results = await db.list_ciudades(name=name, limit=limit, offset=offset)
        return CiudadListResponse(results=results, total_count=len(results))
    except Exception as e:
        logger.error(f"Failed to list ciudades: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list ciudades: {e}",
        ) from e


@router.get("/ciudades/{page_id}", response_model=Ciudad)
async def get_ciudad(
    page_id: str,
    db: Annotated[DatabaseClient, Depends(get_database_client)],
    _: Annotated[str, Depends(verify_webhook_key)] = "",
) -> Ciudad:
    """Get a single ciudad by page_id."""
    ciudad = await db.get_ciudad(page_id)
    if ciudad is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Ciudad with page_id '{page_id}' not found",
        )
    return ciudad


# --- Cronograma ---


@router.get("/cronograma", response_model=CronogramaListResponse)
async def list_cronograma(
    db: Annotated[DatabaseClient, Depends(get_database_client)],
    _: Annotated[str, Depends(verify_webhook_key)] = "",
    date_from: str | None = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: str | None = Query(None, description="End date (YYYY-MM-DD)"),
    ciudad_page_id: str | None = Query(None, description="Filter by ciudad page ID"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Results to skip"),
) -> CronogramaListResponse:
    """List cronograma entries with optional filters."""
    try:
        results = await db.list_cronograma(
            date_from=date_from,
            date_to=date_to,
            ciudad_page_id=ciudad_page_id,
            limit=limit,
            offset=offset,
        )
        return CronogramaListResponse(results=results, total_count=len(results))
    except Exception as e:
        logger.error(f"Failed to list cronograma: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list cronograma: {e}",
        ) from e


@router.get("/cronograma/{page_id}", response_model=Cronograma)
async def get_cronograma(
    page_id: str,
    db: Annotated[DatabaseClient, Depends(get_database_client)],
    _: Annotated[str, Depends(verify_webhook_key)] = "",
) -> Cronograma:
    """Get a single cronograma entry by page_id."""
    cronograma = await db.get_cronograma(page_id)
    if cronograma is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cronograma with page_id '{page_id}' not found",
        )
    return cronograma


# --- Pasajes ---


@router.get("/pasajes", response_model=PasajeListResponse)
async def list_pasajes(
    db: Annotated[DatabaseClient, Depends(get_database_client)],
    _: Annotated[str, Depends(verify_webhook_key)] = "",
    date_from: str | None = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: str | None = Query(None, description="End date (YYYY-MM-DD)"),
    ciudad_page_id: str | None = Query(None, description="Filter by ciudad page ID"),
    cronograma_page_id: str | None = Query(
        None, description="Filter by cronograma page ID"
    ),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Results to skip"),
) -> PasajeListResponse:
    """List pasajes with optional filters."""
    try:
        results = await db.list_pasajes(
            date_from=date_from,
            date_to=date_to,
            ciudad_page_id=ciudad_page_id,
            cronograma_page_id=cronograma_page_id,
            limit=limit,
            offset=offset,
        )
        return PasajeListResponse(results=results, total_count=len(results))
    except Exception as e:
        logger.error(f"Failed to list pasajes: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list pasajes: {e}",
        ) from e


@router.get("/pasajes/{page_id}", response_model=Pasaje)
async def get_pasaje(
    page_id: str,
    db: Annotated[DatabaseClient, Depends(get_database_client)],
    _: Annotated[str, Depends(verify_webhook_key)] = "",
) -> Pasaje:
    """Get a single pasaje by page_id."""
    pasaje = await db.get_pasaje(page_id)
    if pasaje is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pasaje with page_id '{page_id}' not found",
        )
    return pasaje


# --- Atracciones ---


@router.get("/atracciones", response_model=AtraccionListResponse)
async def list_atracciones(
    db: Annotated[DatabaseClient, Depends(get_database_client)],
    _: Annotated[str, Depends(verify_webhook_key)] = "",
    name: str | None = Query(None, description="Filter by name (contains)"),
    date_from: str | None = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: str | None = Query(None, description="End date (YYYY-MM-DD)"),
    ciudad_page_id: str | None = Query(None, description="Filter by ciudad page ID"),
    cronograma_page_id: str | None = Query(
        None, description="Filter by cronograma page ID"
    ),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Results to skip"),
) -> AtraccionListResponse:
    """List atracciones with optional filters."""
    try:
        results = await db.list_atracciones(
            name=name,
            date_from=date_from,
            date_to=date_to,
            ciudad_page_id=ciudad_page_id,
            cronograma_page_id=cronograma_page_id,
            limit=limit,
            offset=offset,
        )
        return AtraccionListResponse(results=results, total_count=len(results))
    except Exception as e:
        logger.error(f"Failed to list atracciones: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list atracciones: {e}",
        ) from e


@router.get("/atracciones/{page_id}", response_model=Atraccion)
async def get_atraccion(
    page_id: str,
    db: Annotated[DatabaseClient, Depends(get_database_client)],
    _: Annotated[str, Depends(verify_webhook_key)] = "",
) -> Atraccion:
    """Get a single atraccion by page_id."""
    atraccion = await db.get_atraccion(page_id)
    if atraccion is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Atraccion with page_id '{page_id}' not found",
        )
    return atraccion
