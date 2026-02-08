from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from notion_hook.core.auth import verify_webhook_key
from notion_hook.core.database import DatabaseClient
from notion_hook.core.logging import get_logger

logger = get_logger("api.gastos")

router = APIRouter(tags=["gastos"], prefix="/api/gastos")


def get_database_client() -> DatabaseClient:
    """Get the database client instance.

    Returns:
        The DatabaseClient instance.

    Raises:
        HTTPException: If service is not initialized.
    """
    from notion_hook.app import _database_client

    if _database_client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database client not initialized",
        )
    return _database_client


def get_notion_client() -> Any:
    """Get the Notion client instance.

    Returns:
        The NotionClient instance.

    Raises:
        HTTPException: If service is not initialized.
    """
    from notion_hook.app import _notion_client

    if _notion_client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Notion client not initialized",
        )
    return _notion_client


class CreateGastoRequest(BaseModel):
    """Request model for creating a gasto."""

    expense: str = Field(..., description="Expense description", min_length=1)
    amount: float = Field(..., description="Amount", gt=0)
    date: str | None = Field(None, description="Date in YYYY-MM-DD format")
    category: list[str] | str | None = Field(None, description="Category(s)")
    payment_method: str | None = Field(None, description="Payment method")
    persona: list[str] | str | None = Field(None, description="Persona(s)")


class GastoResponse(BaseModel):
    """Response model for a gasto."""

    page_id: str = Field(..., description="Notion page ID")
    payment_method: str | None = None
    description: str | None = None
    category: str | None = None
    amount: float | None = None
    date: str | None = None
    persona: str | None = None
    created_at: str = Field(..., description="Creation timestamp (ISO format)")
    updated_at: str = Field(..., description="Last update timestamp (ISO format)")


class GastoListResponse(BaseModel):
    """Response model for listing gastos."""

    results: list[GastoResponse] = Field(..., description="List of gastos")
    total_count: int = Field(..., description="Total number of matching records")


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_gasto(
    request: CreateGastoRequest = Body(...),
    _: Annotated[str, Depends(verify_webhook_key)] = "",
) -> dict[str, Any]:
    """Create a new gasto by writing to Notion (not SQLite).

    The gasto will be synced to SQLite via official Notion automation webhooks.

    Args:
        request: The gasto creation request.
        _: The validated webhook key (from dependency).

    Returns:
        Created page information.

    Raises:
        HTTPException: If creation fails or authentication error.
    """
    notion_client = get_notion_client()

    try:
        result = await notion_client.create_gasto_page(
            expense=request.expense,
            amount=request.amount,
            date=request.date,
            category=request.category,
            payment_method=request.payment_method,
            persona=request.persona,
        )

        page_id = result.get("id")
        logger.info(
            f"Created Gastos page {page_id} with expense='{request.expense}', "
            f"amount={request.amount}"
        )

        return {
            "page_id": page_id,
            "message": "Gasto created successfully",
            "note": "Record will be synced to SQLite via Notion automation webhooks",
        }

    except Exception as e:
        logger.error(f"Failed to create gasto: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create gasto: {e}",
        ) from e


@router.get("", response_model=GastoListResponse)
async def list_gastos(
    db: Annotated[DatabaseClient, Depends(get_database_client)],
    _: Annotated[str, Depends(verify_webhook_key)] = "",
    q: str | None = Query(None, description="Full-text search query"),
    date_from: str | None = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: str | None = Query(None, description="End date (YYYY-MM-DD)"),
    persona: str | None = Query(None, description="Filter by persona"),
    payment_method: str | None = Query(None, description="Filter by payment method"),
    category: str | None = Query(None, description="Filter by category (contains)"),
    amount_min: float | None = Query(None, description="Minimum amount"),
    amount_max: float | None = Query(None, description="Maximum amount"),
    sort_by: str = Query(
        "created_at", description="Sort field (date, created_at, amount)"
    ),
    order: str = Query("desc", description="Sort order (asc, desc)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Results to skip"),
) -> GastoListResponse:
    """List and search gastos from SQLite database.

    Args:
        db: The database client (from dependency).
        _: The validated webhook key (from dependency).
        q: Full-text search query over description/category/persona.
        date_from: Inclusive start date (YYYY-MM-DD).
        date_to: Inclusive end date (YYYY-MM-DD).
        persona: Filter by exact persona value.
        payment_method: Filter by exact payment method.
        category: Filter by category (contains).
        amount_min: Minimum amount.
        amount_max: Maximum amount.
        sort_by: Field to sort by (date, created_at, amount).
        order: Sort order (asc, desc).
        limit: Maximum number of results.
        offset: Number of results to skip.

    Returns:
        List of matching gastos with total count.

    Raises:
        HTTPException: If query fails or authentication error.
    """
    try:
        results = await db.search_gastos(
            q=q,
            date_from=date_from,
            date_to=date_to,
            persona=persona,
            payment_method=payment_method,
            category=category,
            amount_min=amount_min,
            amount_max=amount_max,
            sort_by=sort_by,
            order=order,
            limit=limit,
            offset=offset,
        )

        # Convert to response models
        response_items = [
            GastoResponse(
                page_id=g.page_id,
                payment_method=g.payment_method,
                description=g.description,
                category=g.category,
                amount=g.amount,
                date=g.date,
                persona=g.persona,
                created_at=g.created_at,
                updated_at=g.updated_at,
            )
            for g in results
        ]

        # Get total count for pagination metadata
        # For now, we'll use the returned count as total
        # In a future enhancement, we could run a separate COUNT query
        total_count = len(response_items)

        logger.info(
            f"Listed gastos: q={q}, filters={{date_from, date_to, persona, "
            f"payment_method, category}}, count={total_count}"
        )

        return GastoListResponse(results=response_items, total_count=total_count)

    except Exception as e:
        logger.error(f"Failed to list gastos: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list gastos: {e}",
        ) from e


@router.get("/{page_id}", response_model=GastoResponse)
async def get_gasto(
    page_id: str,
    db: Annotated[DatabaseClient, Depends(get_database_client)],
    _: Annotated[str, Depends(verify_webhook_key)] = "",
) -> GastoResponse:
    """Get a single gasto by page_id from SQLite database.

    Args:
        page_id: The Notion page ID.
        db: The database client (from dependency).
        _: The validated webhook key (from dependency).

    Returns:
        The gasto details.

    Raises:
        HTTPException: If not found or query fails or authentication error.
    """
    gasto = await db.get_gasto(page_id)

    if gasto is None:
        logger.info(f"Gasto not found: {page_id}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Gasto with page_id '{page_id}' not found",
        )

    logger.info(f"Retrieved gasto: {page_id}")
    return GastoResponse(
        page_id=gasto.page_id,
        payment_method=gasto.payment_method,
        description=gasto.description,
        category=gasto.category,
        amount=gasto.amount,
        date=gasto.date,
        persona=gasto.persona,
        created_at=gasto.created_at,
        updated_at=gasto.updated_at,
    )
