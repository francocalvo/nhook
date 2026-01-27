from __future__ import annotations

import secrets
from typing import Annotated

from fastapi import Header, HTTPException, status

from notion_hook.config import get_settings


async def verify_webhook_key(
    x_calvo_key: Annotated[str | None, Header()] = None,
) -> str:
    """Verify the X-Calvo-Key header matches the configured secret.

    Args:
        x_calvo_key: The webhook secret key from request header.

    Returns:
        The validated key.

    Raises:
        HTTPException: If the key is missing or invalid.
    """
    if x_calvo_key is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-Calvo-Key header",
        )

    settings = get_settings()
    if not secrets.compare_digest(x_calvo_key, settings.webhook_secret_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid X-Calvo-Key",
        )

    return x_calvo_key
