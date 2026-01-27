from __future__ import annotations


class NotionHookError(Exception):
    """Base exception for all notion-hook errors."""


class AuthenticationError(NotionHookError):
    """Raised when authentication fails (invalid or missing X-Calvo-Key)."""


class NotionClientError(NotionHookError):
    """Raised when Notion API requests fail."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class WorkflowError(NotionHookError):
    """Raised when a workflow execution fails."""


class WorkflowNotFoundError(NotionHookError):
    """Raised when no workflow matches the incoming webhook."""
