from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


def setup_logging(debug: bool = False) -> logging.Logger:
    """Configure and return the application logger.

    Args:
        debug: If True, set log level to DEBUG, otherwise INFO.

    Returns:
        Configured logger instance.
    """
    level = logging.DEBUG if debug else logging.INFO

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )

    logger = logging.getLogger("notion_hook")
    logger.setLevel(level)

    return logger


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the given name under the notion_hook namespace.

    Args:
        name: Name for the logger (will be prefixed with 'notion_hook.').

    Returns:
        Logger instance.
    """
    return logging.getLogger(f"notion_hook.{name}")
