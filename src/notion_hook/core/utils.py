from __future__ import annotations

from typing import Any

from notion_hook.core.logging import get_logger

logger = get_logger("core.utils")


def get_property_ci(properties: dict[str, Any], property_name: str) -> Any:
    """Get a property value using case-insensitive matching.

    Args:
        properties: Dictionary of property names to values.
        property_name: Name of the property to find (case-insensitive).

    Returns:
        The property value if found, None otherwise.

    Examples:
        >>> props = {"Date": {"date": "2024-01-01"}}
        >>> get_property_ci(props, "date")
        {'date': '2024-01-01'}
        >>> get_property_ci(props, "DATE")
        {'date': '2024-01-01'}
    """
    if not properties:
        return None

    property_name_lower = property_name.lower()
    for key, value in properties.items():
        if key.lower() == property_name_lower:
            if key != property_name:
                logger.debug(
                    f"Property '{property_name}' matched as '{key}' (case mismatch)"
                )
            return value
    return None


def has_property_ci(properties: dict[str, Any], property_name: str) -> bool:
    """Check if a property exists using case-insensitive matching.

    Args:
        properties: Dictionary of property names to values.
        property_name: Name of the property to check (case-insensitive).

    Returns:
        True if property exists, False otherwise.

    Examples:
        >>> props = {"Date": {"date": "2024-01-01"}}
        >>> has_property_ci(props, "date")
        True
        >>> has_property_ci(props, "departure")
        False
    """
    if not properties:
        return False

    property_name_lower = property_name.lower()
    for key in properties:
        if key.lower() == property_name_lower:
            if key != property_name:
                logger.debug(
                    f"Property '{property_name}' exists as '{key}' (case mismatch)"
                )
            return True
    return False


def _extract_relation_ids(prop: dict[str, Any] | None) -> list[str]:
    """Extract page IDs from a relation property.

    Args:
        prop: Notion relation property dict, e.g., {"relation": [{"id": "..."}]}

    Returns:
        List of page IDs (strings). Empty list if no relations or malformed input.

    Examples:
        >>> _extract_relation_ids({"relation": [{"id": "abc"}, {"id": "def"}]})
        ['abc', 'def']
        >>> _extract_relation_ids({"relation": []})
        []
        >>> _extract_relation_ids(None)
        []
    """
    if not prop or not isinstance(prop, dict):
        return []

    relation = prop.get("relation")
    if not isinstance(relation, list):
        return []

    ids: list[str] = []
    for item in relation:
        if isinstance(item, dict) and isinstance(item.get("id"), str):
            ids.append(item["id"])
    return ids


def _extract_relation_id(prop: dict[str, Any] | None) -> str | None:
    """Extract single page ID from a limit-1 relation.

    Args:
        prop: Notion relation property dict, e.g., {"relation": [{"id": "..."}]}

    Returns:
        First page ID as string, or None if no relations.

    Examples:
        >>> _extract_relation_id({"relation": [{"id": "abc"}]})
        'abc'
        >>> _extract_relation_id({"relation": []})
        None
        >>> _extract_relation_id(None)
        None
    """
    ids = _extract_relation_ids(prop)
    return ids[0] if ids else None


def _extract_checkbox(prop: dict[str, Any] | None) -> bool:
    """Extract boolean from checkbox property.

    Args:
        prop: Notion checkbox property dict, e.g., {"checkbox": true}

    Returns:
        Checkbox value as boolean. False for None, missing, or malformed input.

    Examples:
        >>> _extract_checkbox({"checkbox": True})
        True
        >>> _extract_checkbox({"checkbox": False})
        False
        >>> _extract_checkbox(None)
        False
    """
    if not prop or not isinstance(prop, dict):
        return False

    value = prop.get("checkbox")
    return bool(value) if isinstance(value, bool) else False


def _extract_url(prop: dict[str, Any] | None) -> str | None:
    """Extract URL string from url property.

    Args:
        prop: Notion url property dict, e.g., {"url": "https://example.com"}

    Returns:
        URL string (stripped), or None if empty or malformed.

    Examples:
        >>> _extract_url({"url": "https://example.com"})
        'https://example.com'
        >>> _extract_url({"url": ""})
        None
        >>> _extract_url(None)
        None
    """
    if not prop or not isinstance(prop, dict):
        return None

    url = prop.get("url")
    if isinstance(url, str):
        url = url.strip()
        return url if url else None
    return None


def _extract_file_url(prop: dict[str, Any] | None) -> str | None:
    """Extract first file URL from file property.

    Args:
        prop: Notion file property dict with files array, e.g.,
              {"files": [{"file": {"url": "https://..."}}]}

    Returns:
        First file URL (either internal or external), or None if no files.

    Examples:
        >>> _extract_file_url({"files": [{"file": {"url": "https://..."}}]})
        'https://...'
        >>> _extract_file_url({"files": [{"external": {"url": "https://..."}}]})
        'https://...'
        >>> _extract_file_url({"files": []})
        None
        >>> _extract_file_url(None)
        None
    """
    if not prop or not isinstance(prop, dict):
        return None

    files = prop.get("files")
    if not isinstance(files, list) or not files:
        return None

    first_file = files[0]
    if not isinstance(first_file, dict):
        return None

    # Check for internal file URL
    file_obj = first_file.get("file")
    if isinstance(file_obj, dict):
        url = file_obj.get("url")
        if isinstance(url, str):
            url = url.strip()
            return url if url else None

    # Check for external file URL
    external_obj = first_file.get("external")
    if isinstance(external_obj, dict):
        url = external_obj.get("url")
        if isinstance(url, str):
            url = url.strip()
            return url if url else None

    return None
