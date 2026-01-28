from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import ParamSpec, TypeVar

import aiosqlite

from notion_hook.config import Settings
from notion_hook.core.exceptions import NotionHookError
from notion_hook.core.logging import get_logger
from notion_hook.models.gastos import FailLogEntry, Gasto

logger = get_logger("core.database")

P = ParamSpec("P")
T = TypeVar("T")


class DatabaseError(NotionHookError):
    """Database operation error."""


class DatabaseClient:
    """Async SQLite database client for Gastos storage."""

    def __init__(self, settings: Settings) -> None:
        """Initialize the database client.

        Args:
            settings: Application settings.
        """
        self.settings = settings
        self._conn: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> DatabaseClient:
        """Enter async context and initialize database."""
        await self.initialize()
        return self

    async def __aexit__(self, *args: object) -> None:
        """Exit async context and close database connection."""
        await self.close()

    async def initialize(self) -> None:
        """Initialize database connection and create schema."""
        async with self._lock:
            if self._conn is None:
                self._conn = await aiosqlite.connect(self.settings.database_path)
                await self._create_tables()
                logger.info(f"Database initialized: {self.settings.database_path}")

    async def close(self) -> None:
        """Close database connection."""
        async with self._lock:
            if self._conn:
                await self._conn.close()
                self._conn = None
                logger.info("Database connection closed")

    @property
    def conn(self) -> aiosqlite.Connection:
        """Get the database connection."""
        if self._conn is None:
            raise DatabaseError("Database not initialized. Call initialize() first.")
        return self._conn

    async def _create_tables(self) -> None:
        """Create database schema."""
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS gastos (
                page_id TEXT PRIMARY KEY,
                payment_method TEXT,
                description TEXT,
                amount REAL,
                date TEXT,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
        """
        )
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fail_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                page_id TEXT NOT NULL,
                operation TEXT NOT NULL,
                error_message TEXT NOT NULL,
                retry_count INTEGER DEFAULT 0,
                created_at TIMESTAMP NOT NULL
            )
        """
        )
        await self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_fail_log_page_id ON fail_log(page_id)
        """
        )
        await self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_fail_log_created_at ON fail_log(created_at)
        """
        )
        await self.conn.commit()

    async def get_gasto(self, page_id: str) -> Gasto | None:
        """Retrieve a single gasto by page_id.

        Args:
            page_id: The Notion page ID.

        Returns:
            Gasto instance if found, None otherwise.

        Raises:
            DatabaseError: If the operation fails.
        """

        async def _get() -> Gasto | None:
            async with self.conn.execute(
                "SELECT * FROM gastos WHERE page_id = ?", (page_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return Gasto(
                        page_id=row[0],
                        payment_method=row[1],
                        description=row[2],
                        amount=row[3],
                        date=row[4],
                        created_at=row[5],
                        updated_at=row[6],
                    )
            return None

        return await self._retry_operation("get", _get)

    async def _retry_operation(
        self,
        operation: str,
        func: Callable[P, Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """Execute operation with retry logic.

        Args:
            operation: Operation name for logging.
            func: The function to execute.
            *args: Positional arguments for func.
            **kwargs: Keyword arguments for func.

        Returns:
            Result of func execution.

        Raises:
            DatabaseError: If all retries fail.
        """
        last_error: Exception | None = None
        max_retries = max(1, self.settings.max_retries)
        for attempt in range(max_retries):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    delay = self.settings.retry_delay * (2**attempt)
                    msg = (
                        f"Database {operation} failed "
                        f"(attempt {attempt + 1}/{max_retries}), "
                        f"retrying in {delay}s: {e}"
                    )
                    logger.warning(msg)
                    await asyncio.sleep(delay)
                else:
                    msg = (
                        f"Database {operation} failed after {max_retries} attempts: {e}"
                    )
                    logger.error(msg)
        details = f": {last_error}" if last_error else ""
        raise DatabaseError(f"Database {operation} failed{details}") from last_error

    async def create_gasto(self, gasto: Gasto) -> None:
        """Insert a new gasto.

        Args:
            gasto: The Gasto instance to insert.

        Raises:
            DatabaseError: If the operation fails after retries.
        """

        async def _insert() -> None:
            await self.conn.execute(
                """
                INSERT INTO gastos (
                    page_id,
                    payment_method,
                    description,
                    amount,
                    date,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    gasto.page_id,
                    gasto.payment_method,
                    gasto.description,
                    gasto.amount,
                    gasto.date,
                    gasto.created_at,
                    gasto.updated_at,
                ),
            )
            await self.conn.commit()

        await self._retry_operation("create", _insert)

    async def update_gasto(self, gasto: Gasto) -> bool:
        """Update an existing gasto.

        Args:
            gasto: The Gasto instance with updated data.

        Returns:
            True if updated, False if not found.

        Raises:
            DatabaseError: If the operation fails after retries.
        """

        async def _update() -> bool:
            cursor = await self.conn.execute(
                """UPDATE gastos
                SET payment_method = ?, description = ?, amount = ?,
                date = ?, updated_at = ? WHERE page_id = ?""",
                (
                    gasto.payment_method,
                    gasto.description,
                    gasto.amount,
                    gasto.date,
                    gasto.updated_at,
                    gasto.page_id,
                ),
            )
            await self.conn.commit()
            return cursor.rowcount > 0

        return await self._retry_operation("update", _update)

    async def delete_gasto(self, page_id: str) -> bool:
        """Delete a gasto.

        Args:
            page_id: The Notion page ID.

        Returns:
            True if deleted, False if not found.

        Raises:
            DatabaseError: If the operation fails after retries.
        """

        async def _delete() -> bool:
            cursor = await self.conn.execute(
                "DELETE FROM gastos WHERE page_id = ?", (page_id,)
            )
            await self.conn.commit()
            return cursor.rowcount > 0

        return await self._retry_operation("delete", _delete)

    async def list_gastos(self, limit: int = 100, offset: int = 0) -> list[Gasto]:
        """List gastos with pagination.

        Args:
            limit: Maximum number of results.
            offset: Number of results to skip.

        Returns:
            List of Gasto instances.

        Raises:
            DatabaseError: If the operation fails after retries.
        """

        async def _list() -> list[Gasto]:
            async with self.conn.execute(
                "SELECT * FROM gastos ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ) as cursor:
                rows = await cursor.fetchall()
                return [
                    Gasto(
                        page_id=row[0],
                        payment_method=row[1],
                        description=row[2],
                        amount=row[3],
                        date=row[4],
                        created_at=row[5],
                        updated_at=row[6],
                    )
                    for row in rows
                ]

        return await self._retry_operation("list", _list)

    async def log_failure(
        self, page_id: str, operation: str, error_message: str, retry_count: int = 0
    ) -> int:
        """Log a failed operation.

        Args:
            page_id: The Notion page ID.
            operation: Operation type (create/update/delete).
            error_message: Error message.
            retry_count: Number of retry attempts.

        Returns:
            The ID of the inserted log entry.

        Raises:
            DatabaseError: If the operation fails after retries.
        """

        async def _log() -> int:
            entry = FailLogEntry.create(
                page_id=page_id,
                operation=operation,
                error_message=error_message,
                retry_count=retry_count,
            )
            cursor = await self.conn.execute(
                """INSERT INTO fail_log
                (page_id, operation, error_message, retry_count, created_at)
                VALUES (?, ?, ?, ?, ?)""",
                (
                    entry.page_id,
                    entry.operation,
                    entry.error_message,
                    entry.retry_count,
                    entry.created_at,
                ),
            )
            await self.conn.commit()
            return cursor.lastrowid

        return await self._retry_operation("log_failure", _log)

    async def get_failures(self, page_id: str) -> list[FailLogEntry]:
        """Get failure log entries for a page.

        Args:
            page_id: The Notion page ID.

        Returns:
            List of FailLogEntry instances.

        Raises:
            DatabaseError: If the operation fails after retries.
        """

        async def _get() -> list[FailLogEntry]:
            async with self.conn.execute(
                "SELECT * FROM fail_log WHERE page_id = ? ORDER BY created_at DESC",
                (page_id,),
            ) as cursor:
                rows = await cursor.fetchall()
                return [
                    FailLogEntry(
                        id=row[0],
                        page_id=row[1],
                        operation=row[2],
                        error_message=row[3],
                        retry_count=row[4],
                        created_at=row[5],
                    )
                    for row in rows
                ]

        return await self._retry_operation("get_failures", _get)
