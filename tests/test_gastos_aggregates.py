"""Tests for Gastos aggregate validation and response models."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from notion_hook.models.gastos_aggregates import (
    AggregateFilters,
    GastoSummaryGroup,
    GastoSummaryResponse,
    GastoTotalsResponse,
    validate_group_by,
)


class TestAggregateFilters:
    """Tests for AggregateFilters validation."""

    def test_empty_filters_valid(self) -> None:
        """Test that empty filters are valid."""
        filters = AggregateFilters()
        assert filters.q is None
        assert filters.date_from is None
        assert filters.date_to is None
        assert filters.persona is None
        assert filters.payment_method is None
        assert filters.category is None
        assert filters.amount_min is None
        assert filters.amount_max is None
        assert filters.ciudad is None

    def test_valid_date_format(self) -> None:
        """Test that valid date formats are accepted."""
        filters = AggregateFilters(date_from="2026-01-01", date_to="2026-01-31")
        assert filters.date_from == "2026-01-01"
        assert filters.date_to == "2026-01-31"

    def test_strict_date_format_non_padded_month(self) -> None:
        """Test that non-padded month (2026-1-01) is rejected."""
        with pytest.raises(HTTPException) as exc_info:
            AggregateFilters(date_from="2026-1-01")

        assert exc_info.value.status_code == 400
        assert "Invalid date_from format" in exc_info.value.detail
        assert "strict" in exc_info.value.detail.lower()

    def test_strict_date_format_non_padded_day(self) -> None:
        """Test that non-padded day (2026-01-1) is rejected."""
        with pytest.raises(HTTPException) as exc_info:
            AggregateFilters(date_to="2026-01-1")

        assert exc_info.value.status_code == 400
        assert "Invalid date_to format" in exc_info.value.detail

    def test_strict_date_format_non_padded_both(self) -> None:
        """Test that non-padded month and day (2026-1-1) is rejected."""
        with pytest.raises(HTTPException) as exc_info:
            AggregateFilters(date_from="2026-1-1")

        assert exc_info.value.status_code == 400
        assert "Invalid date_from format" in exc_info.value.detail

    def test_all_filter_fields(self) -> None:
        """Test that all filter fields can be set."""
        filters = AggregateFilters(
            q="groceries",
            date_from="2026-01-01",
            date_to="2026-01-31",
            persona="John",
            payment_method="credit_card",
            category="Food",
            amount_min=10.0,
            amount_max=100.0,
            ciudad="Rome",
        )
        assert filters.q == "groceries"
        assert filters.date_from == "2026-01-01"
        assert filters.date_to == "2026-01-31"
        assert filters.persona == "John"
        assert filters.payment_method == "credit_card"
        assert filters.category == "Food"
        assert filters.amount_min == 10.0
        assert filters.amount_max == 100.0
        assert filters.ciudad == "Rome"

    def test_invalid_date_from_format(self) -> None:
        """Test that invalid date_from format raises 400."""
        with pytest.raises(HTTPException) as exc_info:
            AggregateFilters(date_from="01-01-2026")

        assert exc_info.value.status_code == 400
        assert "Invalid date_from format" in exc_info.value.detail
        assert "Expected YYYY-MM-DD" in exc_info.value.detail

    def test_invalid_date_to_format(self) -> None:
        """Test that invalid date_to format raises 400."""
        with pytest.raises(HTTPException) as exc_info:
            AggregateFilters(date_to="2026/01/01")

        assert exc_info.value.status_code == 400
        assert "Invalid date_to format" in exc_info.value.detail
        assert "Expected YYYY-MM-DD" in exc_info.value.detail

    def test_invalid_date_format_partial(self) -> None:
        """Test that partial date formats are rejected."""
        with pytest.raises(HTTPException) as exc_info:
            AggregateFilters(date_from="2026-01")

        assert exc_info.value.status_code == 400
        assert "Invalid date_from format" in exc_info.value.detail

    def test_invalid_date_format_text(self) -> None:
        """Test that text date formats are rejected."""
        with pytest.raises(HTTPException) as exc_info:
            AggregateFilters(date_from="January 1, 2026")

        assert exc_info.value.status_code == 400

    def test_date_range_invalid(self) -> None:
        """Test that date_from > date_to raises 400."""
        with pytest.raises(HTTPException) as exc_info:
            AggregateFilters(date_from="2026-01-31", date_to="2026-01-01")

        assert exc_info.value.status_code == 400
        assert "Invalid date range" in exc_info.value.detail
        assert "cannot be after" in exc_info.value.detail

    def test_date_range_same_day_valid(self) -> None:
        """Test that same day for from and to is valid."""
        filters = AggregateFilters(date_from="2026-01-15", date_to="2026-01-15")
        assert filters.date_from == "2026-01-15"
        assert filters.date_to == "2026-01-15"

    def test_date_range_comparison_uses_parsed_dates(self) -> None:
        """Test that date range comparison uses parsed dates, not string comparison.

        This is a regression test for the bug where "2026-02-01" > "2026-11-01"
        would be True due to lexical string comparison.
        """
        # This should be valid: Feb 1 comes before Nov 1
        filters = AggregateFilters(date_from="2026-02-01", date_to="2026-11-01")
        assert filters.date_from == "2026-02-01"
        assert filters.date_to == "2026-11-01"

        # This should be invalid: Nov 1 comes after Feb 1
        with pytest.raises(HTTPException) as exc_info:
            AggregateFilters(date_from="2026-11-01", date_to="2026-02-01")

        assert exc_info.value.status_code == 400
        assert "Invalid date range" in exc_info.value.detail

    def test_amount_range_invalid(self) -> None:
        """Test that amount_min > amount_max raises 400."""
        with pytest.raises(HTTPException) as exc_info:
            AggregateFilters(amount_min=100.0, amount_max=10.0)

        assert exc_info.value.status_code == 400
        assert "Invalid amount range" in exc_info.value.detail
        assert "cannot be greater than" in exc_info.value.detail

    def test_amount_range_same_valid(self) -> None:
        """Test that same amount for min and max is valid."""
        filters = AggregateFilters(amount_min=50.0, amount_max=50.0)
        assert filters.amount_min == 50.0
        assert filters.amount_max == 50.0

    def test_negative_amounts_allowed(self) -> None:
        """Test that negative amounts are allowed (for refunds, etc.)."""
        # The validation only checks range order, not sign
        filters = AggregateFilters(amount_min=-100.0, amount_max=0.0)
        assert filters.amount_min == -100.0
        assert filters.amount_max == 0.0

    def test_only_date_from_provided(self) -> None:
        """Test that providing only date_from is valid."""
        filters = AggregateFilters(date_from="2026-01-01")
        assert filters.date_from == "2026-01-01"
        assert filters.date_to is None

    def test_only_date_to_provided(self) -> None:
        """Test that providing only date_to is valid."""
        filters = AggregateFilters(date_to="2026-12-31")
        assert filters.date_from is None
        assert filters.date_to == "2026-12-31"

    def test_only_amount_min_provided(self) -> None:
        """Test that providing only amount_min is valid."""
        filters = AggregateFilters(amount_min=10.0)
        assert filters.amount_min == 10.0
        assert filters.amount_max is None

    def test_only_amount_max_provided(self) -> None:
        """Test that providing only amount_max is valid."""
        filters = AggregateFilters(amount_max=1000.0)
        assert filters.amount_min is None
        assert filters.amount_max == 1000.0

    def test_amount_min_string_parsed(self) -> None:
        """Test that amount_min string is parsed to float."""
        filters = AggregateFilters(amount_min="100.50")
        assert filters.amount_min == 100.50
        assert isinstance(filters.amount_min, float)

    def test_amount_max_string_parsed(self) -> None:
        """Test that amount_max string is parsed to float."""
        filters = AggregateFilters(amount_max="200.75")
        assert filters.amount_max == 200.75
        assert isinstance(filters.amount_max, float)

    def test_amount_min_malformed_string_rejected(self) -> None:
        """Test that malformed amount_min string returns 400."""
        with pytest.raises(HTTPException) as exc_info:
            AggregateFilters(amount_min="abc")

        assert exc_info.value.status_code == 400
        assert "Invalid amount_min" in exc_info.value.detail
        assert "not a valid number" in exc_info.value.detail

    def test_amount_max_malformed_string_rejected(self) -> None:
        """Test that malformed amount_max string returns 400."""
        with pytest.raises(HTTPException) as exc_info:
            AggregateFilters(amount_max="not-a-number")

        assert exc_info.value.status_code == 400
        assert "Invalid amount_max" in exc_info.value.detail
        assert "not a valid number" in exc_info.value.detail

    def test_amount_empty_string_rejected(self) -> None:
        """Test that empty string for amount returns 400."""
        with pytest.raises(HTTPException) as exc_info:
            AggregateFilters(amount_min="")

        assert exc_info.value.status_code == 400
        assert "Invalid amount_min" in exc_info.value.detail

    def test_amount_min_nan_string_rejected(self) -> None:
        """Test that nan string for amount_min returns 400."""
        with pytest.raises(HTTPException) as exc_info:
            AggregateFilters(amount_min="nan")

        assert exc_info.value.status_code == 400
        assert "Invalid amount_min" in exc_info.value.detail
        assert "not a finite number" in exc_info.value.detail

    def test_amount_max_nan_string_rejected(self) -> None:
        """Test that nan string for amount_max returns 400."""
        with pytest.raises(HTTPException) as exc_info:
            AggregateFilters(amount_max="nan")

        assert exc_info.value.status_code == 400
        assert "Invalid amount_max" in exc_info.value.detail
        assert "not a finite number" in exc_info.value.detail

    def test_amount_min_inf_string_rejected(self) -> None:
        """Test that inf string for amount_min returns 400."""
        with pytest.raises(HTTPException) as exc_info:
            AggregateFilters(amount_min="inf")

        assert exc_info.value.status_code == 400
        assert "Invalid amount_min" in exc_info.value.detail
        assert "not a finite number" in exc_info.value.detail

    def test_amount_max_negative_inf_string_rejected(self) -> None:
        """Test that -inf string for amount_max returns 400."""
        with pytest.raises(HTTPException) as exc_info:
            AggregateFilters(amount_max="-inf")

        assert exc_info.value.status_code == 400
        assert "Invalid amount_max" in exc_info.value.detail
        assert "not a finite number" in exc_info.value.detail

    def test_amount_min_nan_float_rejected(self) -> None:
        """Test that nan float for amount_min returns 400."""
        with pytest.raises(HTTPException) as exc_info:
            AggregateFilters(amount_min=float("nan"))

        assert exc_info.value.status_code == 400
        assert "Invalid amount_min" in exc_info.value.detail
        assert "not a finite number" in exc_info.value.detail

    def test_amount_max_inf_float_rejected(self) -> None:
        """Test that inf float for amount_max returns 400."""
        with pytest.raises(HTTPException) as exc_info:
            AggregateFilters(amount_max=float("inf"))

        assert exc_info.value.status_code == 400
        assert "Invalid amount_max" in exc_info.value.detail
        assert "not a finite number" in exc_info.value.detail

    def test_amount_min_negative_inf_float_rejected(self) -> None:
        """Test that -inf float for amount_min returns 400."""
        with pytest.raises(HTTPException) as exc_info:
            AggregateFilters(amount_min=float("-inf"))

        assert exc_info.value.status_code == 400
        assert "Invalid amount_min" in exc_info.value.detail
        assert "not a finite number" in exc_info.value.detail


class TestValidateGroupBy:
    """Tests for group_by validation."""

    def test_none_returns_empty_list(self) -> None:
        """Test that None group_by returns empty list."""
        result = validate_group_by(None)
        assert result == []

    def test_empty_string_returns_empty_list(self) -> None:
        """Test that empty string returns empty list."""
        result = validate_group_by("")
        assert result == []

    def test_whitespace_only_returns_empty_list(self) -> None:
        """Test that whitespace-only string returns empty list."""
        result = validate_group_by("   ")
        assert result == []

    def test_single_valid_dimension(self) -> None:
        """Test single valid dimension."""
        result = validate_group_by("category")
        assert result == ["category"]

    def test_multiple_valid_dimensions(self) -> None:
        """Test multiple valid dimensions."""
        result = validate_group_by("category,persona")
        assert result == ["category", "persona"]

    def test_dimensions_with_spaces(self) -> None:
        """Test that spaces are trimmed."""
        result = validate_group_by(" category , persona , date ")
        assert result == ["category", "persona", "date"]

    def test_all_valid_dimensions(self) -> None:
        """Test all valid dimensions."""
        result = validate_group_by("category,persona,date,ciudad")
        assert result == ["category", "persona", "date", "ciudad"]

    def test_invalid_single_dimension(self) -> None:
        """Test invalid single dimension raises 400."""
        with pytest.raises(HTTPException) as exc_info:
            validate_group_by("foo")

        assert exc_info.value.status_code == 400
        assert "Invalid group_by value(s)" in exc_info.value.detail
        assert "foo" in exc_info.value.detail
        assert "Allowed values:" in exc_info.value.detail

    def test_invalid_dimension_mixed_with_valid(self) -> None:
        """Test invalid dimension mixed with valid raises 400."""
        with pytest.raises(HTTPException) as exc_info:
            validate_group_by("category,invalid,persona")

        assert exc_info.value.status_code == 400
        assert "Invalid group_by value(s)" in exc_info.value.detail
        assert "invalid" in exc_info.value.detail

    def test_multiple_invalid_dimensions(self) -> None:
        """Test multiple invalid dimensions all reported."""
        with pytest.raises(HTTPException) as exc_info:
            validate_group_by("foo,bar,baz")

        assert exc_info.value.status_code == 400
        detail = exc_info.value.detail
        assert "foo" in detail
        assert "bar" in detail
        assert "baz" in detail

    def test_duplicate_dimensions_rejected(self) -> None:
        """Test that duplicate dimensions are rejected with 400."""
        with pytest.raises(HTTPException) as exc_info:
            validate_group_by("category,category")

        assert exc_info.value.status_code == 400
        assert "Duplicate" in exc_info.value.detail
        assert "category" in exc_info.value.detail

    def test_multiple_duplicate_dimensions_rejected(self) -> None:
        """Test that multiple duplicate dimensions are all reported."""
        with pytest.raises(HTTPException) as exc_info:
            validate_group_by("category,persona,category,persona")

        assert exc_info.value.status_code == 400
        assert "Duplicate" in exc_info.value.detail
        assert "category" in exc_info.value.detail
        assert "persona" in exc_info.value.detail


class TestGastoTotalsResponse:
    """Tests for GastoTotalsResponse model."""

    def test_zero_safe_defaults(self) -> None:
        """Test that zero-safe defaults are applied."""
        response = GastoTotalsResponse()
        assert response.total == 0.0
        assert response.count == 0
        assert response.min == 0.0
        assert response.max == 0.0
        assert response.avg == 0.0

    def test_with_values(self) -> None:
        """Test response with actual values."""
        response = GastoTotalsResponse(
            total=484.50,
            count=20,
            min=3.50,
            max=85.00,
            avg=24.22,
        )
        assert response.total == 484.50
        assert response.count == 20
        assert response.min == 3.50
        assert response.max == 85.00
        assert response.avg == 24.22

    def test_serialization(self) -> None:
        """Test JSON serialization."""
        response = GastoTotalsResponse(
            total=100.0,
            count=5,
            min=10.0,
            max=30.0,
            avg=20.0,
        )
        data = response.model_dump()
        assert data["total"] == 100.0
        assert data["count"] == 5
        assert data["min"] == 10.0
        assert data["max"] == 30.0
        assert data["avg"] == 20.0

    def test_negative_monetary_values_allowed(self) -> None:
        """Test that negative monetary values are allowed (for refunds, etc.)."""
        response = GastoTotalsResponse(
            total=-50.0,
            count=3,
            min=-20.0,
            max=10.0,
            avg=-16.67,
        )
        assert response.total == -50.0
        assert response.min == -20.0
        assert response.max == 10.0
        assert response.avg == -16.67

    def test_negative_count_rejected(self) -> None:
        """Test that negative count is still rejected (ge=0 constraint)."""
        with pytest.raises(Exception):  # Pydantic ValidationError
            GastoTotalsResponse(count=-1)


class TestGastoSummaryGroup:
    """Tests for GastoSummaryGroup model."""

    def test_single_dimension_key(self) -> None:
        """Test group with single dimension key."""
        group = GastoSummaryGroup(
            key={"category": "Food"},
            total=245.5,
            count=12,
        )
        assert group.key == {"category": "Food"}
        assert group.total == 245.5
        assert group.count == 12

    def test_multi_dimension_key(self) -> None:
        """Test group with multi-dimension key."""
        group = GastoSummaryGroup(
            key={"category": "Food", "persona": "Franco"},
            total=120.0,
            count=4,
        )
        assert group.key == {"category": "Food", "persona": "Franco"}
        assert group.total == 120.0
        assert group.count == 4

    def test_unknown_value_in_key(self) -> None:
        """Test group with 'Unknown' value for missing dimension."""
        group = GastoSummaryGroup(
            key={"category": "Unknown"},
            total=50.0,
            count=3,
        )
        assert group.key["category"] == "Unknown"

    def test_serialization(self) -> None:
        """Test JSON serialization."""
        group = GastoSummaryGroup(
            key={"category": "Food", "persona": "Franco"},
            total=120.0,
            count=4,
        )
        data = group.model_dump()
        assert data["key"] == {"category": "Food", "persona": "Franco"}
        assert data["total"] == 120.0
        assert data["count"] == 4

    def test_negative_total_allowed(self) -> None:
        """Test that negative totals are allowed (for refunds, etc.)."""
        group = GastoSummaryGroup(
            key={"category": "Refunds"},
            total=-45.50,
            count=2,
        )
        assert group.total == -45.50
        assert group.count == 2

    def test_negative_count_rejected(self) -> None:
        """Test that negative count is still rejected."""
        with pytest.raises(Exception):  # Pydantic ValidationError
            GastoSummaryGroup(key={"category": "Food"}, total=100.0, count=-1)


class TestGastoSummaryResponse:
    """Tests for GastoSummaryResponse model."""

    def test_zero_safe_defaults(self) -> None:
        """Test that zero-safe defaults are applied."""
        response = GastoSummaryResponse()
        assert response.groups == []
        assert response.grand_total == 0.0
        assert response.total_count == 0

    def test_with_groups(self) -> None:
        """Test response with groups."""
        groups = [
            GastoSummaryGroup(key={"category": "Food"}, total=245.5, count=12),
            GastoSummaryGroup(key={"category": "Transport"}, total=89.0, count=5),
        ]
        response = GastoSummaryResponse(
            groups=groups,
            grand_total=484.5,
            total_count=20,
        )
        assert len(response.groups) == 2
        assert response.grand_total == 484.5
        assert response.total_count == 20

    def test_serialization(self) -> None:
        """Test JSON serialization."""
        groups = [
            GastoSummaryGroup(key={"category": "Food"}, total=100.0, count=5),
        ]
        response = GastoSummaryResponse(
            groups=groups,
            grand_total=100.0,
            total_count=5,
        )
        data = response.model_dump()
        assert len(data["groups"]) == 1
        assert data["groups"][0]["key"] == {"category": "Food"}
        assert data["grand_total"] == 100.0
        assert data["total_count"] == 5

    def test_negative_grand_total_allowed(self) -> None:
        """Test that negative grand_total is allowed (for refunds, etc.)."""
        response = GastoSummaryResponse(
            groups=[],
            grand_total=-100.0,
            total_count=5,
        )
        assert response.grand_total == -100.0

    def test_negative_total_count_rejected(self) -> None:
        """Test that negative total_count is still rejected."""
        with pytest.raises(Exception):  # Pydantic ValidationError
            GastoSummaryResponse(total_count=-1)
