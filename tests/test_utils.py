from __future__ import annotations

from notion_hook.core.utils import get_property_ci, has_property_ci


def test_get_property_ci_exact_match() -> None:
    props = {"Date": {"date": "2024-01-01"}}
    result = get_property_ci(props, "Date")
    assert result == {"date": "2024-01-01"}


def test_get_property_ci_lowercase_search() -> None:
    props = {"Date": {"date": "2024-01-01"}}
    result = get_property_ci(props, "date")
    assert result == {"date": "2024-01-01"}


def test_get_property_ci_uppercase_search() -> None:
    props = {"Date": {"date": "2024-01-01"}}
    result = get_property_ci(props, "DATE")
    assert result == {"date": "2024-01-01"}


def test_get_property_ci_mixed_case_search() -> None:
    props = {"Date": {"date": "2024-01-01"}}
    result = get_property_ci(props, "dAtE")
    assert result == {"date": "2024-01-01"}


def test_get_property_ci_property_not_exists() -> None:
    props = {"Date": {"date": "2024-01-01"}}
    result = get_property_ci(props, "departure")
    assert result is None


def test_get_property_ci_empty_dict() -> None:
    result = get_property_ci({}, "Date")
    assert result is None


def test_get_property_ci_similar_names() -> None:
    props = {"Date": {"date": "2024-01-01"}, "Date2": {"date": "2024-01-02"}}
    result = get_property_ci(props, "date")
    assert result == {"date": "2024-01-01"}
    result2 = get_property_ci(props, "date2")
    assert result2 == {"date": "2024-01-02"}


def test_has_property_ci_exact_match() -> None:
    props = {"Date": {"date": "2024-01-01"}}
    assert has_property_ci(props, "Date") is True


def test_has_property_ci_lowercase_search() -> None:
    props = {"Date": {"date": "2024-01-01"}}
    assert has_property_ci(props, "date") is True


def test_has_property_ci_uppercase_search() -> None:
    props = {"Date": {"date": "2024-01-01"}}
    assert has_property_ci(props, "DATE") is True


def test_has_property_ci_mixed_case_search() -> None:
    props = {"Date": {"date": "2024-01-01"}}
    assert has_property_ci(props, "dAtE") is True


def test_has_property_ci_not_exists() -> None:
    props = {"Date": {"date": "2024-01-01"}}
    assert has_property_ci(props, "departure") is False


def test_has_property_ci_empty_dict() -> None:
    assert has_property_ci({}, "Date") is False


def test_has_property_ci_multiple_properties() -> None:
    props = {"Date": {"date": "2024-01-01"}, "Departure": {"date": "2024-01-02"}}
    assert has_property_ci(props, "date") is True
    assert has_property_ci(props, "departure") is True
    assert has_property_ci(props, "nonexistent") is False
