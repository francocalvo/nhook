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
