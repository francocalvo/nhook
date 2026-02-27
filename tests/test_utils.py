from __future__ import annotations

from notion_hook.core.utils import (
    _extract_checkbox,
    _extract_file_url,
    _extract_relation_id,
    _extract_relation_ids,
    _extract_url,
    get_property_ci,
    has_property_ci,
)


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


# Tests for _extract_relation_ids


def test_extract_relation_ids_multiple() -> None:
    prop = {"relation": [{"id": "uuid-1"}, {"id": "uuid-2"}, {"id": "uuid-3"}]}
    result = _extract_relation_ids(prop)
    assert result == ["uuid-1", "uuid-2", "uuid-3"]


def test_extract_relation_ids_single() -> None:
    prop = {"relation": [{"id": "uuid-1"}]}
    result = _extract_relation_ids(prop)
    assert result == ["uuid-1"]


def test_extract_relation_ids_empty() -> None:
    prop = {"relation": []}
    result = _extract_relation_ids(prop)
    assert result == []


def test_extract_relation_ids_none() -> None:
    result = _extract_relation_ids(None)
    assert result == []


def test_extract_relation_ids_malformed_no_id() -> None:
    prop = {"relation": [{"name": "no-id"}]}
    result = _extract_relation_ids(prop)
    assert result == []


def test_extract_relation_ids_malformed_wrong_type() -> None:
    prop = {"relation": "not-a-list"}
    result = _extract_relation_ids(prop)
    assert result == []


def test_extract_relation_ids_malformed_items() -> None:
    prop = {"relation": ["not-a-dict", 123, None]}
    result = _extract_relation_ids(prop)
    assert result == []


def test_extract_relation_ids_no_relation_key() -> None:
    prop = {"other_key": [{"id": "uuid-1"}]}
    result = _extract_relation_ids(prop)
    assert result == []


# Tests for _extract_relation_id


def test_extract_relation_id_single() -> None:
    prop = {"relation": [{"id": "uuid-1"}]}
    result = _extract_relation_id(prop)
    assert result == "uuid-1"


def test_extract_relation_id_multiple() -> None:
    prop = {"relation": [{"id": "uuid-1"}, {"id": "uuid-2"}]}
    result = _extract_relation_id(prop)
    assert result == "uuid-1"  # Returns first


def test_extract_relation_id_empty() -> None:
    prop = {"relation": []}
    result = _extract_relation_id(prop)
    assert result is None


def test_extract_relation_id_none() -> None:
    result = _extract_relation_id(None)
    assert result is None


def test_extract_relation_id_malformed() -> None:
    prop = {"relation": [{"name": "no-id"}]}
    result = _extract_relation_id(prop)
    assert result is None


# Tests for _extract_checkbox


def test_extract_checkbox_true() -> None:
    prop = {"checkbox": True}
    result = _extract_checkbox(prop)
    assert result is True


def test_extract_checkbox_false() -> None:
    prop = {"checkbox": False}
    result = _extract_checkbox(prop)
    assert result is False


def test_extract_checkbox_none() -> None:
    result = _extract_checkbox(None)
    assert result is False


def test_extract_checkbox_empty_dict() -> None:
    result = _extract_checkbox({})
    assert result is False


def test_extract_checkbox_no_key() -> None:
    result = _extract_checkbox({"other_key": "value"})
    assert result is False


def test_extract_checkbox_string_true() -> None:
    # Should return False for non-boolean values
    result = _extract_checkbox({"checkbox": "true"})
    assert result is False


def test_extract_checkbox_number() -> None:
    # Should return False for non-boolean values
    result = _extract_checkbox({"checkbox": 1})
    assert result is False


# Tests for _extract_url


def test_extract_url_valid() -> None:
    prop = {"url": "https://example.com"}
    result = _extract_url(prop)
    assert result == "https://example.com"


def test_extract_url_empty_string() -> None:
    prop = {"url": ""}
    result = _extract_url(prop)
    assert result is None


def test_extract_url_whitespace() -> None:
    prop = {"url": "   "}
    result = _extract_url(prop)
    assert result is None


def test_extract_url_none() -> None:
    result = _extract_url(None)
    assert result is None


def test_extract_url_empty_dict() -> None:
    result = _extract_url({})
    assert result is None


def test_extract_url_no_key() -> None:
    result = _extract_url({"other_key": "value"})
    assert result is None


def test_extract_url_non_string() -> None:
    result = _extract_url({"url": 123})
    assert result is None


def test_extract_url_with_whitespace() -> None:
    prop = {"url": "  https://example.com  "}
    result = _extract_url(prop)
    assert result == "https://example.com"


# Tests for _extract_file_url


def test_extract_file_url_internal() -> None:
    prop = {"files": [{"file": {"url": "https://notion.s3.amazonaws.com/file.pdf"}}]}
    result = _extract_file_url(prop)
    assert result == "https://notion.s3.amazonaws.com/file.pdf"


def test_extract_file_url_external() -> None:
    prop = {"files": [{"external": {"url": "https://example.com/ticket.pdf"}}]}
    result = _extract_file_url(prop)
    assert result == "https://example.com/ticket.pdf"


def test_extract_file_url_multiple_files() -> None:
    prop = {
        "files": [
            {"file": {"url": "https://first.pdf"}},
            {"external": {"url": "https://second.pdf"}},
        ]
    }
    result = _extract_file_url(prop)
    assert result == "https://first.pdf"  # Returns first


def test_extract_file_url_empty() -> None:
    prop = {"files": []}
    result = _extract_file_url(prop)
    assert result is None


def test_extract_file_url_none() -> None:
    result = _extract_file_url(None)
    assert result is None


def test_extract_file_url_no_files_key() -> None:
    prop = {"other_key": [{"file": {"url": "https://..."}}]}
    result = _extract_file_url(prop)
    assert result is None


def test_extract_file_url_internal_empty_url() -> None:
    prop = {"files": [{"file": {"url": ""}}]}
    result = _extract_file_url(prop)
    assert result is None


def test_extract_file_url_internal_whitespace_url() -> None:
    prop = {"files": [{"file": {"url": "   "}}]}
    result = _extract_file_url(prop)
    assert result is None


def test_extract_file_url_external_empty_url() -> None:
    prop = {"files": [{"external": {"url": ""}}]}
    result = _extract_file_url(prop)
    assert result is None


def test_extract_file_url_no_url_in_file_obj() -> None:
    prop = {"files": [{"file": {"other_key": "value"}}]}
    result = _extract_file_url(prop)
    assert result is None


def test_extract_file_url_no_url_in_external_obj() -> None:
    prop = {"files": [{"external": {"other_key": "value"}}]}
    result = _extract_file_url(prop)
    assert result is None


def test_extract_file_url_malformed_items() -> None:
    prop = {"files": ["not-a-dict", 123, None]}
    result = _extract_file_url(prop)
    assert result is None
