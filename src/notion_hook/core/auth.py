from __future__ import annotations

import secrets
from typing import Annotated

from fastapi import Header, HTTPException, Request, status

from notion_hook.config import get_settings
from notion_hook.core.logging import get_logger

logger = get_logger("core.auth")


async def verify_webhook_key(
    request: Request,
    x_calvo_key: Annotated[str | None, Header()] = None,
) -> str:
    """Verify the X-Calvo-Key header matches the configured secret.

    Args:
        request: The incoming request.
        x_calvo_key: The webhook secret key from request header.

    Returns:
        The validated key.

    Raises:
        HTTPException: If the key is missing or invalid.
    """
    client_ip = request.client.host if request.client else "unknown"

    if x_calvo_key is None:
        logger.warning(
            f"Authentication failed: Missing X-Calvo-Key header from {client_ip}"
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-Calvo-Key header",
        )

    settings = get_settings()
    if not secrets.compare_digest(x_calvo_key, settings.webhook_secret_key):
        logger.warning(f"Authentication failed: Invalid X-Calvo-Key from {client_ip}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid X-Calvo-Key",
        )

    logger.info(f"Authentication successful from {client_ip}")
    return x_calvo_key
