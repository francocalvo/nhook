from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from notion_hook.core.auth import verify_webhook_key
from notion_hook.core.database import DatabaseClient
from notion_hook.core.logging import get_logger
from notion_hook.models.gastos_aggregates import (
    AggregateFilters,
    GastoSummaryGroup,
    GastoSummaryResponse,
    GastoTotalsResponse,
    get_aggregate_filters,
    get_group_by,
)

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


@router.get("/totals", response_model=GastoTotalsResponse)
async def get_gastos_totals(
    db: Annotated[DatabaseClient, Depends(get_database_client)],
    _: Annotated[str, Depends(verify_webhook_key)] = "",
    filters: AggregateFilters = Depends(get_aggregate_filters),
) -> GastoTotalsResponse:
    """Get aggregate totals for gastos.

    This endpoint returns aggregate metrics (total, count, min, max, avg)
    for all gastos matching the provided filters.

    Unlike the list endpoint, aggregate endpoints combine ALL filters together,
    including FTS (q) and structured filters.

    Args:
        db: The database client (from dependency).
        _: The validated webhook key (from dependency).
        filters: Validated aggregate filters (from dependency).

    Returns:
        Aggregate totals for the filtered result set.

    Raises:
        HTTPException: If query fails or authentication error.
    """
    try:
        total, count, min_val, max_val, avg_val = await db.get_gastos_totals(
            q=filters.q,
            date_from=filters.date_from,
            date_to=filters.date_to,
            persona=filters.persona,
            payment_method=filters.payment_method,
            category=filters.category,
            amount_min=filters.amount_min,
            amount_max=filters.amount_max,
            ciudad=filters.ciudad,
        )

        logger.info(
            f"Computed gastos totals: q={filters.q}, "
            f"filters={{date_from={filters.date_from}, date_to={filters.date_to}, "
            f"persona={filters.persona}, payment_method={filters.payment_method}, "
            f"category={filters.category}, ciudad={filters.ciudad}}}, "
            f"result={{total={total}, count={count}}}"
        )

        return GastoTotalsResponse(
            total=total,
            count=count,
            min=min_val,
            max=max_val,
            avg=avg_val,
        )

    except Exception as e:
        logger.error(f"Failed to get gastos totals: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get gastos totals: {e}",
        ) from e


@router.get("/summary", response_model=GastoSummaryResponse)
async def get_gastos_summary(
    db: Annotated[DatabaseClient, Depends(get_database_client)],
    _: Annotated[str, Depends(verify_webhook_key)] = "",
    filters: AggregateFilters = Depends(get_aggregate_filters),
    group_by: list[str] = Depends(get_group_by),
) -> GastoSummaryResponse:
    """Get grouped summary for gastos.

    This endpoint returns grouped aggregates (total, count) for all gastos
    matching the provided filters, grouped by the specified dimension(s).

    Unlike the list endpoint, aggregate endpoints combine ALL filters together,
    including FTS (q) and structured filters.

    **Step 5**: Single-dimension grouping only (no exploded grouping yet).
    Multi-dimension grouping and exploded category/persona will be in Step 6.

    **Grouping dimensions**:
    - `category`: Group by category (no explosion in Step 5)
    - `persona`: Group by persona (no explosion in Step 5)
    - `date`: Group by date (day-level granularity)
    - `ciudad`: Group by city

    **Sorting**:
    - Date groups: sorted ascending by date
    - Other groups: sorted descending by total

    **Missing values**: Grouped under "Unknown"

    **Note on exploded grouping** (Step 6):
    When grouping by category or persona in Step 6, multi-value fields will
    be split and a single gasto can contribute to multiple groups. This means
    the sum of group totals/counts may exceed grand_total/total_count.

    Args:
        db: The database client (from dependency).
        _: The validated webhook key (from dependency).
        filters: Validated aggregate filters (from dependency).
        group_by: Validated grouping dimensions (from dependency).

    Returns:
        Grouped summary with grand totals from the filtered base set.

    Raises:
        HTTPException: If query fails or authentication error.
    """
    try:
        groups, grand_total, total_count = await db.get_gastos_summary(
            group_by=group_by,
            q=filters.q,
            date_from=filters.date_from,
            date_to=filters.date_to,
            persona=filters.persona,
            payment_method=filters.payment_method,
            category=filters.category,
            amount_min=filters.amount_min,
            amount_max=filters.amount_max,
            ciudad=filters.ciudad,
        )

        logger.info(
            f"Computed gastos summary: q={filters.q}, "
            f"filters={{date_from={filters.date_from}, date_to={filters.date_to}, "
            f"persona={filters.persona}, payment_method={filters.payment_method}, "
            f"category={filters.category}, ciudad={filters.ciudad}}}, "
            f"group_by={group_by}, "
            f"result={{groups={len(groups)}, grand_total={grand_total}, "
            f"total_count={total_count}}}"
        )

        # Convert dict groups to GastoSummaryGroup objects
        summary_groups = [
            GastoSummaryGroup(
                key=group["key"],
                total=group["total"],
                count=group["count"],
            )
            for group in groups
        ]

        return GastoSummaryResponse(
            groups=summary_groups,
            grand_total=grand_total,
            total_count=total_count,
        )

    except Exception as e:
        logger.error(f"Failed to get gastos summary: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get gastos summary: {e}",
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
