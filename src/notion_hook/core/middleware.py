from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from notion_hook.core.logging import get_logger

if TYPE_CHECKING:
    pass

logger = get_logger("core.middleware")

SENSITIVE_HEADERS = {
    "authorization",
    "x-calvo-key",
    "x-api-key",
    "cookie",
    "set-cookie",
}


def _sanitize_headers(headers: dict[str, str]) -> dict[str, str]:
    """Sanitize headers by masking sensitive values.

    Args:
        headers: Dictionary of headers.

    Returns:
        Dictionary with sensitive values masked.
    """
    sanitized = {}
    for key, value in headers.items():
        if key.lower() in SENSITIVE_HEADERS:
            sanitized[key] = "***REDACTED***"
        else:
            sanitized[key] = value
    return sanitized


class LoggingMiddleware(BaseHTTPMiddleware):
    """Middleware for logging all HTTP requests and responses."""

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ):
        """Log request and response details.

        Args:
            request: Incoming request.
            call_next: Next middleware or route handler.

        Returns:
            Response from the next handler.
        """
        client_ip = request.client.host if request.client else "unknown"

        headers = dict(request.headers)
        sanitized_headers = _sanitize_headers(headers)

        logger.info(
            f"Incoming request: {request.method} {request.url.path} from {client_ip}"
        )
        logger.debug(f"Request headers: {sanitized_headers}")

        response = await call_next(request)

        logger.info(
            f"Response: {response.status_code} for {request.method} {request.url.path}"
        )

        return response
