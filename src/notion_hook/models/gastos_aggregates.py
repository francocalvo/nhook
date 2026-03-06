"""Shared models and validation for Gastos aggregate endpoints.

This module provides request validation and response models for the
/api/gastos/totals and /api/gastos/summary endpoints.

Key features:
- Shared filter validation for aggregate endpoints
- group_by parsing and validation
- Zero-safe response models
- Explicit 400 error handling for invalid inputs
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime
from typing import Annotated

from fastapi import Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

# Regex pattern for strict YYYY-MM-DD format
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Allowed group_by dimensions for summary endpoint
ALLOWED_GROUP_BY_DIMENSIONS = frozenset({"category", "persona", "date", "ciudad"})


class AggregateFilters:
    """Shared filter parameters for aggregate endpoints.

    This class provides a reusable set of filter parameters that can be
    used as FastAPI dependencies for both /totals and /summary endpoints.

    Unlike the list endpoint, aggregate endpoints combine all filters
    together rather than having FTS bypass other filters.
    """

    def __init__(
        self,
        q: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        persona: str | None = None,
        payment_method: str | None = None,
        category: str | None = None,
        amount_min: str | float | None = None,
        amount_max: str | float | None = None,
        ciudad: str | None = None,
    ) -> None:
        """Initialize aggregate filters with validation.

        Args:
            q: Full-text search query.
            date_from: Start date (YYYY-MM-DD).
            date_to: End date (YYYY-MM-DD).
            persona: Filter by persona.
            payment_method: Filter by payment method.
            category: Filter by category (contains).
            amount_min: Minimum amount (string or float).
            amount_max: Maximum amount (string or float).
            ciudad: Filter by city name.

        Raises:
            HTTPException: If any filter value is invalid.
        """
        self.q = q
        self.date_from, self._date_from_obj = self._validate_date(
            date_from, "date_from"
        )
        self.date_to, self._date_to_obj = self._validate_date(date_to, "date_to")
        self.persona = persona
        self.payment_method = payment_method
        self.category = category
        self.amount_min = self._validate_amount(amount_min, "amount_min")
        self.amount_max = self._validate_amount(amount_max, "amount_max")
        self.ciudad = ciudad

        self._validate_date_range()
        self._validate_amount_range()

    @staticmethod
    def _validate_date(
        value: str | None, field_name: str
    ) -> tuple[str | None, date | None]:
        """Validate date format (strict YYYY-MM-DD).

        Args:
            value: The date string to validate.
            field_name: Name of the field for error messages.

        Returns:
            Tuple of (original string, parsed date object) or (None, None).

        Raises:
            HTTPException: If date format is invalid.
        """
        if value is None:
            return None, None

        # Enforce strict YYYY-MM-DD format
        if not _DATE_PATTERN.match(value):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"Invalid {field_name} format. Expected YYYY-MM-DD (strict), "
                    f"got '{value}'"
                ),
            ) from None

        try:
            parsed_date = datetime.strptime(value, "%Y-%m-%d").date()
            return value, parsed_date
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"Invalid {field_name} format. Expected YYYY-MM-DD, got '{value}'"
                ),
            ) from e

    def _validate_date_range(self) -> None:
        """Validate that date_from <= date_to if both are provided.

        Raises:
            HTTPException: If date range is invalid.
        """
        if (
            self._date_from_obj is not None
            and self._date_to_obj is not None
            and self._date_from_obj > self._date_to_obj
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid date range: date_from ({self.date_from}) "
                f"cannot be after date_to ({self.date_to})",
            )

    @staticmethod
    def _validate_amount(value: str | float | None, field_name: str) -> float | None:
        """Validate and parse amount parameter.

        Args:
            value: The amount value to validate (string or float).
            field_name: Name of the field for error messages.

        Returns:
            Parsed float value or None.

        Raises:
            HTTPException: If amount cannot be parsed as a number or is non-finite.
        """
        if value is None:
            return None

        # If already a float, check finiteness
        if isinstance(value, float):
            if not math.isfinite(value):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=(
                        f"Invalid {field_name} value: '{value}' is not a finite number"
                    ),
                )
            return value

        # Try to parse string as float
        try:
            parsed = float(value)
        except (ValueError, TypeError) as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid {field_name} value: '{value}' is not a valid number",
            ) from e

        # Check if parsed value is finite
        if not math.isfinite(parsed):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"Invalid {field_name} value: '{value}' is not a finite number"
                ),
            )

        return parsed

    def _validate_amount_range(self) -> None:
        """Validate that amount_min <= amount_max if both are provided.

        Raises:
            HTTPException: If amount range is invalid.
        """
        if (
            self.amount_min is not None
            and self.amount_max is not None
            and self.amount_min > self.amount_max
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid amount range: amount_min ({self.amount_min}) "
                f"cannot be greater than amount_max ({self.amount_max})",
            )


async def get_aggregate_filters(
    q: str | None = Query(None, description="Full-text search query"),
    date_from: str | None = Query(
        None, description="Start date (YYYY-MM-DD, inclusive)"
    ),
    date_to: str | None = Query(None, description="End date (YYYY-MM-DD, inclusive)"),
    persona: str | None = Query(None, description="Filter by persona"),
    payment_method: str | None = Query(None, description="Filter by payment method"),
    category: str | None = Query(None, description="Filter by category (contains)"),
    amount_min: str | None = Query(None, description="Minimum amount"),
    amount_max: str | None = Query(None, description="Maximum amount"),
    ciudad: str | None = Query(None, description="Filter by city name"),
) -> AggregateFilters:
    """FastAPI dependency for aggregate endpoint filters.

    Args:
        q: Full-text search query.
        date_from: Start date (YYYY-MM-DD).
        date_to: End date (YYYY-MM-DD).
        persona: Filter by persona.
        payment_method: Filter by payment method.
        category: Filter by category (contains).
        amount_min: Minimum amount (string for manual validation).
        amount_max: Maximum amount (string for manual validation).
        ciudad: Filter by city name.

    Returns:
        Validated AggregateFilters instance.
    """
    return AggregateFilters(
        q=q,
        date_from=date_from,
        date_to=date_to,
        persona=persona,
        payment_method=payment_method,
        category=category,
        amount_min=amount_min,
        amount_max=amount_max,
        ciudad=ciudad,
    )


def validate_group_by(group_by: str | None) -> list[str]:
    """Validate and parse group_by parameter.

    Supports single or multiple grouping dimensions (comma-separated).
    Multi-value explosion is applied for 'category' and 'persona' dimensions.

    Args:
        group_by: Comma-separated grouping dimensions (e.g., "category,persona").

    Returns:
        List of validated group_by dimensions.

    Raises:
        HTTPException: If group_by value is invalid or duplicates are present.
    """
    if group_by is None:
        return []

    dimensions = [dim.strip() for dim in group_by.split(",") if dim.strip()]

    if not dimensions:
        return []

    # Check for duplicate dimensions
    seen = set()
    duplicates = []
    for dim in dimensions:
        if dim in seen:
            if dim not in duplicates:
                duplicates.append(dim)
        else:
            seen.add(dim)

    if duplicates:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Duplicate group_by dimension(s) not allowed: {', '.join(duplicates)}"
            ),
        )

    # Check for invalid dimensions
    invalid = [dim for dim in dimensions if dim not in ALLOWED_GROUP_BY_DIMENSIONS]

    if invalid:
        allowed_list = ", ".join(sorted(ALLOWED_GROUP_BY_DIMENSIONS))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid group_by value(s): {', '.join(invalid)}. "
            f"Allowed values: {allowed_list}",
        )

    return dimensions


async def get_group_by(
    group_by: str | None = Query(
        None,
        description="Grouping dimension(s): category, persona, date, or ciudad "
        "(comma-separated for multi-dimension, e.g., 'category,persona')",
    ),
) -> list[str]:
    """FastAPI dependency for validating group_by parameter.

    Args:
        group_by: Comma-separated grouping dimension(s). Supports single or
            multiple dimensions (e.g., 'category' or 'category,persona').

    Returns:
        List of validated group_by dimensions (may be empty if group_by is None).
    """
    return validate_group_by(group_by)


# Response Models


class GastoTotalsResponse(BaseModel):
    """Response model for GET /api/gastos/totals.

    All fields default to zero for empty result sets.
    Monetary fields (total, min, max, avg) can be negative for refunds/adjustments.
    """

    total: float = Field(
        default=0.0,
        description="Total sum of amounts for filtered gastos (can be negative)",
    )
    count: int = Field(
        default=0,
        ge=0,
        description="Number of gastos matching the filters",
    )
    min: float = Field(
        default=0.0,
        description="Minimum amount in the filtered set (0 if empty, can be negative)",
    )
    max: float = Field(
        default=0.0,
        description="Maximum amount in the filtered set (0 if empty, can be negative)",
    )
    avg: float = Field(
        default=0.0,
        description="Average amount (0 if empty, can be negative)",
    )


class GastoSummaryGroupKey(BaseModel):
    """Structured key for a summary group.

    The key contains the grouping dimension values as a dictionary.
    For multi-dimension grouping, all dimensions are present.
    Missing values are represented as "Unknown".
    """

    model_config = {"extra": "allow"}  # Allow dynamic fields for group dimensions

    # Common fields that may be present depending on group_by
    category: str | None = Field(None, description="Category value (if grouped)")
    persona: str | None = Field(None, description="Persona value (if grouped)")
    date: str | None = Field(None, description="Date value (if grouped)")
    ciudad: str | None = Field(None, description="City value (if grouped)")


class GastoSummaryGroup(BaseModel):
    """A single group in the summary response."""

    key: dict[str, str] = Field(
        ...,
        description="Grouping dimension values as a structured object",
    )
    total: float = Field(
        ...,
        description="Total sum of amounts for this group (can be negative)",
    )
    count: int = Field(
        ...,
        ge=0,
        description="Number of gastos in this group",
    )


class GastoSummaryResponse(BaseModel):
    """Response model for GET /api/gastos/summary.

    The response includes:
    - groups: List of grouped aggregates
    - grand_total: Total sum of amounts in the filtered base set (pre-explosion)
    - total_count: Number of gastos in the filtered base set (pre-explosion)

    Note: When exploded grouping is used (category or persona), the sum of
    group totals/counts may exceed grand_total/total_count because a single
    gasto can contribute to multiple groups.
    """

    groups: list[GastoSummaryGroup] = Field(
        default_factory=list,
        description="List of grouped aggregates",
    )
    grand_total: float = Field(
        default=0.0,
        description="Total sum in filtered base set (pre-explosion, can be negative)",
    )
    total_count: int = Field(
        default=0,
        ge=0,
        description="Number of gastos in filtered base set (pre-explosion)",
    )


# Type aliases for dependency injection
AggregateFiltersDep = Annotated[AggregateFilters, Depends(get_aggregate_filters)]
GroupByDep = Annotated[list[str], Depends(get_group_by)]
