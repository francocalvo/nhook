from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import ParamSpec, TypeVar

import aiosqlite

from notion_hook.config import Settings
from notion_hook.core.exceptions import NotionHookError
from notion_hook.core.logging import get_logger
from notion_hook.models.gastos import FailLogEntry, Gasto
from notion_hook.models.notion_db import Atraccion, Ciudad, Cronograma, Pasaje

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
                # Enable foreign key constraints (required for FK relationships)
                await self.conn.execute("PRAGMA foreign_keys = ON")
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
                category TEXT,
                amount REAL,
                date DATE,
                persona TEXT,
                ciudad_page_id TEXT,
                ciudad TEXT,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
        """
        )
        await self._ensure_gastos_schema()
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ciudades (
                page_id TEXT PRIMARY KEY,
                name TEXT,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
        """
        )
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cronograma (
                page_id TEXT PRIMARY KEY,
                day DATE,
                ciudad_page_id TEXT REFERENCES ciudades(page_id) ON DELETE SET NULL,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
        """
        )
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pasajes (
                page_id TEXT PRIMARY KEY,
                departure DATE,
                cronograma_page_id TEXT
                    REFERENCES cronograma(page_id) ON DELETE SET NULL,
                ciudad_page_id TEXT REFERENCES ciudades(page_id) ON DELETE SET NULL,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
        """
        )
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS atracciones (
                page_id TEXT PRIMARY KEY,
                name TEXT,
                fecha DATE,
                cronograma_page_id TEXT
                    REFERENCES cronograma(page_id) ON DELETE SET NULL,
                ciudad_page_id TEXT REFERENCES ciudades(page_id) ON DELETE SET NULL,
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
        await self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_cronograma_ciudad
            ON cronograma(ciudad_page_id)
        """
        )
        await self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pasajes_cronograma
            ON pasajes(cronograma_page_id)
        """
        )
        await self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pasajes_ciudad
            ON pasajes(ciudad_page_id)
        """
        )
        await self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_atracciones_cronograma
            ON atracciones(cronograma_page_id)
        """
        )
        await self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_atracciones_ciudad
            ON atracciones(ciudad_page_id)
        """
        )
        await self.conn.commit()

    async def _ensure_gastos_schema(self) -> None:
        """Ensure gastos table has expected columns."""
        async with self.conn.execute("PRAGMA table_info(gastos)") as cursor:
            rows = await cursor.fetchall()
            existing = {row[1] for row in rows}

        if "category" not in existing:
            await self.conn.execute("ALTER TABLE gastos ADD COLUMN category TEXT")
        if "persona" not in existing:
            await self.conn.execute("ALTER TABLE gastos ADD COLUMN persona TEXT")
        if "ciudad_page_id" not in existing:
            await self.conn.execute("ALTER TABLE gastos ADD COLUMN ciudad_page_id TEXT")
        if "ciudad" not in existing:
            await self.conn.execute("ALTER TABLE gastos ADD COLUMN ciudad TEXT")

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
                """
                SELECT page_id, payment_method, description, category,
                amount, date, created_at, updated_at, persona, ciudad_page_id, ciudad
                FROM gastos
                WHERE page_id = ?
                """,
                (page_id,),
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return Gasto(
                        page_id=row[0],
                        payment_method=row[1],
                        description=row[2],
                        category=row[3],
                        amount=row[4],
                        date=row[5],
                        created_at=row[6],
                        updated_at=row[7],
                        persona=row[8],
                        ciudad_page_id=row[9],
                        ciudad=row[10],
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
                    category,
                    amount,
                    date,
                    persona,
                    ciudad_page_id,
                    ciudad,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    gasto.page_id,
                    gasto.payment_method,
                    gasto.description,
                    gasto.category,
                    gasto.amount,
                    gasto.date,
                    gasto.persona,
                    gasto.ciudad_page_id,
                    gasto.ciudad,
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
                SET payment_method = ?, description = ?, category = ?, amount = ?,
                date = ?, persona = ?, ciudad_page_id = ?, ciudad = ?,
                updated_at = ? WHERE page_id = ?""",
                (
                    gasto.payment_method,
                    gasto.description,
                    gasto.category,
                    gasto.amount,
                    gasto.date,
                    gasto.persona,
                    gasto.ciudad_page_id,
                    gasto.ciudad,
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

    async def clear_gastos(self) -> int:
        """Delete all gastos.

        Returns:
            Number of rows deleted.

        Raises:
            DatabaseError: If the operation fails after retries.
        """

        async def _clear() -> int:
            async with self.conn.execute("SELECT COUNT(*) FROM gastos") as cursor:
                row = await cursor.fetchone()
                count = int(row[0]) if row else 0

            await self.conn.execute("DELETE FROM gastos")
            await self.conn.commit()
            return count

        return await self._retry_operation("clear_gastos", _clear)

    async def delete_gastos(self, page_ids: list[str]) -> int:
        """Delete gastos by page_id in batches.

        Args:
            page_ids: Page IDs to delete.

        Returns:
            Number of rows deleted.

        Raises:
            DatabaseError: If the operation fails after retries.
        """
        if not page_ids:
            return 0

        async def _delete_many() -> int:
            deleted = 0
            chunk_size = 900  # stay below SQLite variable limit

            await self.conn.execute("BEGIN")
            try:
                for i in range(0, len(page_ids), chunk_size):
                    chunk = page_ids[i : i + chunk_size]
                    placeholders = ",".join("?" for _ in chunk)
                    cursor = await self.conn.execute(
                        f"DELETE FROM gastos WHERE page_id IN ({placeholders})",
                        chunk,
                    )
                    if cursor.rowcount and cursor.rowcount > 0:
                        deleted += cursor.rowcount

                await self.conn.commit()
            except Exception:
                await self.conn.rollback()
                raise

            return deleted

        return await self._retry_operation("delete_many", _delete_many)

    async def sync_gastos_batch(
        self,
        gastos: list[Gasto],
        *,
        update_if_changed: bool,
    ) -> tuple[int, int, int, int]:
        """Insert/update a batch of gastos in a single transaction.

        Args:
            gastos: Gastos to sync.
            update_if_changed: If True, only updates when fields changed.

        Returns:
            Tuple of (created, updated, skipped, failed).

        Raises:
            DatabaseError: If the operation fails after retries.
        """
        if not gastos:
            return (0, 0, 0, 0)

        async def _sync() -> tuple[int, int, int, int]:
            page_ids = [g.page_id for g in gastos]
            placeholders = ",".join("?" for _ in page_ids)

            existing: dict[
                str,
                tuple[
                    object,
                    object,
                    object,
                    object,
                    object,
                    object,
                    object,
                    object,
                    object,
                ],
            ] = {}
            async with self.conn.execute(
                f"""
                SELECT page_id, payment_method, description,
                category, amount, date, persona, ciudad_page_id, ciudad, updated_at
                FROM gastos
                WHERE page_id IN ({placeholders})
                """,
                page_ids,
            ) as cursor:
                rows = await cursor.fetchall()
                for row in rows:
                    existing[str(row[0])] = (
                        row[1],
                        row[2],
                        row[3],
                        row[4],
                        row[5],
                        row[6],
                        row[7],
                        row[8],
                        row[9],
                    )

            created = 0
            updated = 0
            skipped = 0
            failed = 0

            await self.conn.execute("BEGIN")
            try:
                for gasto in gastos:
                    row = existing.get(gasto.page_id)
                    if row is not None:
                        if update_if_changed and (
                            row[0] == gasto.payment_method
                            and row[1] == gasto.description
                            and row[2] == gasto.category
                            and row[3] == gasto.amount
                            and row[4] == gasto.date
                            and row[5] == gasto.persona
                            and row[6] == gasto.ciudad_page_id
                            and row[7] == gasto.ciudad
                            and row[8] == gasto.updated_at
                        ):
                            skipped += 1
                            continue

                        try:
                            await self.conn.execute(
                                """UPDATE gastos
                                SET payment_method = ?, description = ?,
                                category = ?, amount = ?, date = ?,
                                persona = ?, ciudad_page_id = ?, ciudad = ?,
                                updated_at = ? WHERE page_id = ?""",
                                (
                                    gasto.payment_method,
                                    gasto.description,
                                    gasto.category,
                                    gasto.amount,
                                    gasto.date,
                                    gasto.persona,
                                    gasto.ciudad_page_id,
                                    gasto.ciudad,
                                    gasto.updated_at,
                                    gasto.page_id,
                                ),
                            )
                            updated += 1
                        except Exception as e:
                            failed += 1
                            logger.warning(
                                f"Failed to update gasto {gasto.page_id}: {e}"
                            )
                    else:
                        try:
                            await self.conn.execute(
                                """
                                INSERT INTO gastos (
                                    page_id,
                                    payment_method,
                                    description,
                                    category,
                                    amount,
                                    date,
                                    persona,
                                    ciudad_page_id,
                                    ciudad,
                                    created_at,
                                    updated_at
                                )
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    gasto.page_id,
                                    gasto.payment_method,
                                    gasto.description,
                                    gasto.category,
                                    gasto.amount,
                                    gasto.date,
                                    gasto.persona,
                                    gasto.ciudad_page_id,
                                    gasto.ciudad,
                                    gasto.created_at,
                                    gasto.updated_at,
                                ),
                            )
                            created += 1
                        except Exception as e:
                            failed += 1
                            logger.warning(
                                f"Failed to create gasto {gasto.page_id}: {e}"
                            )

                await self.conn.commit()
            except Exception:
                await self.conn.rollback()
                raise

            return (created, updated, skipped, failed)

        return await self._retry_operation("sync_batch", _sync)

    async def sync_ciudades_batch(
        self, ciudades: list[Ciudad], *, update_if_changed: bool
    ) -> tuple[int, int, int, int]:
        """Insert/update a batch of ciudades records."""
        rows = [
            (
                c.page_id,
                c.name,
                c.created_at,
                c.updated_at,
            )
            for c in ciudades
        ]
        return await self._sync_table_batch(
            table_name="ciudades",
            columns=["name", "created_at", "updated_at"],
            rows=rows,
            update_if_changed=update_if_changed,
        )

    async def sync_cronograma_batch(
        self, cronograma: list[Cronograma], *, update_if_changed: bool
    ) -> tuple[int, int, int, int]:
        """Insert/update a batch of cronograma records."""
        rows = [
            (
                c.page_id,
                c.day,
                c.ciudad_page_id,
                c.created_at,
                c.updated_at,
            )
            for c in cronograma
        ]
        return await self._sync_table_batch(
            table_name="cronograma",
            columns=["day", "ciudad_page_id", "created_at", "updated_at"],
            rows=rows,
            update_if_changed=update_if_changed,
        )

    async def sync_pasajes_batch(
        self, pasajes: list[Pasaje], *, update_if_changed: bool
    ) -> tuple[int, int, int, int]:
        """Insert/update a batch of pasajes records."""
        rows = [
            (
                p.page_id,
                p.departure,
                p.cronograma_page_id,
                p.ciudad_page_id,
                p.created_at,
                p.updated_at,
            )
            for p in pasajes
        ]
        return await self._sync_table_batch(
            table_name="pasajes",
            columns=[
                "departure",
                "cronograma_page_id",
                "ciudad_page_id",
                "created_at",
                "updated_at",
            ],
            rows=rows,
            update_if_changed=update_if_changed,
        )

    async def sync_atracciones_batch(
        self, atracciones: list[Atraccion], *, update_if_changed: bool
    ) -> tuple[int, int, int, int]:
        """Insert/update a batch of atracciones records."""
        rows = [
            (
                a.page_id,
                a.name,
                a.fecha,
                a.cronograma_page_id,
                a.ciudad_page_id,
                a.created_at,
                a.updated_at,
            )
            for a in atracciones
        ]
        return await self._sync_table_batch(
            table_name="atracciones",
            columns=[
                "name",
                "fecha",
                "cronograma_page_id",
                "ciudad_page_id",
                "created_at",
                "updated_at",
            ],
            rows=rows,
            update_if_changed=update_if_changed,
        )

    async def _sync_table_batch(
        self,
        *,
        table_name: str,
        columns: list[str],
        rows: Sequence[tuple[object, ...]],
        update_if_changed: bool,
    ) -> tuple[int, int, int, int]:
        """Insert/update a batch of records for a table."""
        if not rows:
            return (0, 0, 0, 0)

        self._validate_table_name(table_name)

        async def _sync() -> tuple[int, int, int, int]:
            page_ids = [str(row[0]) for row in rows]
            placeholders = ",".join("?" for _ in page_ids)
            select_cols = ", ".join(columns)
            existing: dict[str, tuple[object, ...]] = {}

            async with self.conn.execute(
                f"SELECT page_id, {select_cols} FROM {table_name} "
                f"WHERE page_id IN ({placeholders})",
                page_ids,
            ) as cursor:
                for row in await cursor.fetchall():
                    existing[str(row[0])] = tuple(row[1:])

            created = 0
            updated = 0
            skipped = 0
            failed = 0

            await self.conn.execute("BEGIN")
            try:
                for row in rows:
                    page_id = str(row[0])
                    payload = tuple(row[1:])
                    previous = existing.get(page_id)

                    if previous is not None:
                        if update_if_changed and previous == payload:
                            skipped += 1
                            continue

                        assignments = ", ".join(f"{col} = ?" for col in columns)
                        try:
                            await self.conn.execute(
                                f"UPDATE {table_name} SET {assignments} "
                                "WHERE page_id = ?",
                                (*payload, page_id),
                            )
                            updated += 1
                        except Exception as e:
                            failed += 1
                            logger.warning(
                                f"Failed to update {table_name} row {page_id}: {e}"
                            )
                    else:
                        placeholders_insert = ", ".join(
                            "?" for _ in range(len(columns) + 1)
                        )
                        columns_insert = ", ".join(["page_id", *columns])
                        try:
                            await self.conn.execute(
                                f"INSERT INTO {table_name} ({columns_insert}) "
                                f"VALUES ({placeholders_insert})",
                                (page_id, *payload),
                            )
                            created += 1
                        except Exception as e:
                            failed += 1
                            logger.warning(
                                f"Failed to create {table_name} row {page_id}: {e}"
                            )

                await self.conn.commit()
            except Exception:
                await self.conn.rollback()
                raise

            return (created, updated, skipped, failed)

        return await self._retry_operation(f"sync_{table_name}", _sync)

    async def clear_sync_tables(self, *, include_gastos: bool = True) -> dict[str, int]:
        """Delete all rows from sync tables in FK-safe order."""
        order = ["atracciones", "pasajes", "cronograma", "ciudades"]
        if include_gastos:
            order.append("gastos")

        async def _clear_all() -> dict[str, int]:
            result: dict[str, int] = {}
            for table in order:
                self._validate_table_name(table)
                async with self.conn.execute(f"SELECT COUNT(*) FROM {table}") as cursor:
                    row = await cursor.fetchone()
                    result[table] = int(row[0]) if row else 0
                await self.conn.execute(f"DELETE FROM {table}")
            await self.conn.commit()
            return result

        return await self._retry_operation("clear_sync_tables", _clear_all)

    async def get_all_page_ids(self, table_name: str) -> set[str]:
        """Get all page IDs from a supported table."""
        self._validate_table_name(table_name)

        async def _get_all_ids() -> set[str]:
            async with self.conn.execute(f"SELECT page_id FROM {table_name}") as cursor:
                rows = await cursor.fetchall()
                return {str(row[0]) for row in rows}

        return await self._retry_operation(f"get_all_ids_{table_name}", _get_all_ids)

    async def delete_by_page_ids(self, table_name: str, page_ids: list[str]) -> int:
        """Delete rows by page_id in batches for a supported table."""
        self._validate_table_name(table_name)
        if not page_ids:
            return 0

        async def _delete_many() -> int:
            deleted = 0
            chunk_size = 900
            await self.conn.execute("BEGIN")
            try:
                for i in range(0, len(page_ids), chunk_size):
                    chunk = page_ids[i : i + chunk_size]
                    placeholders = ",".join("?" for _ in chunk)
                    cursor = await self.conn.execute(
                        f"DELETE FROM {table_name} WHERE page_id IN ({placeholders})",
                        chunk,
                    )
                    if cursor.rowcount and cursor.rowcount > 0:
                        deleted += cursor.rowcount
                await self.conn.commit()
            except Exception:
                await self.conn.rollback()
                raise
            return deleted

        return await self._retry_operation(f"delete_many_{table_name}", _delete_many)

    async def get_ciudad(self, page_id: str) -> Ciudad | None:
        """Retrieve a single ciudad by page_id."""

        async def _get() -> Ciudad | None:
            async with self.conn.execute(
                """
                SELECT page_id, name, created_at, updated_at
                FROM ciudades
                WHERE page_id = ?
                """,
                (page_id,),
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return Ciudad(
                        page_id=str(row[0]),
                        name=row[1],
                        created_at=str(row[2]),
                        updated_at=str(row[3]),
                    )
            return None

        return await self._retry_operation("get_ciudad", _get)

    async def create_ciudad(self, ciudad: Ciudad) -> None:
        """Insert a new ciudad."""

        async def _insert() -> None:
            await self.conn.execute(
                """
                INSERT INTO ciudades (page_id, name, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    ciudad.page_id,
                    ciudad.name,
                    ciudad.created_at,
                    ciudad.updated_at,
                ),
            )
            await self.conn.commit()

        await self._retry_operation("create_ciudad", _insert)

    async def update_ciudad(self, ciudad: Ciudad) -> bool:
        """Update an existing ciudad."""

        async def _update() -> bool:
            cursor = await self.conn.execute(
                """
                UPDATE ciudades
                SET name = ?, updated_at = ?
                WHERE page_id = ?
                """,
                (ciudad.name, ciudad.updated_at, ciudad.page_id),
            )
            await self.conn.commit()
            return cursor.rowcount > 0

        return await self._retry_operation("update_ciudad", _update)

    async def delete_ciudad(self, page_id: str) -> bool:
        """Delete a ciudad by page_id."""

        async def _delete() -> bool:
            cursor = await self.conn.execute(
                "DELETE FROM ciudades WHERE page_id = ?",
                (page_id,),
            )
            await self.conn.commit()
            return cursor.rowcount > 0

        return await self._retry_operation("delete_ciudad", _delete)

    async def get_cronograma(self, page_id: str) -> Cronograma | None:
        """Retrieve a single cronograma entry by page_id."""

        async def _get() -> Cronograma | None:
            async with self.conn.execute(
                """
                SELECT page_id, day, ciudad_page_id, created_at, updated_at
                FROM cronograma
                WHERE page_id = ?
                """,
                (page_id,),
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return Cronograma(
                        page_id=str(row[0]),
                        day=row[1],
                        ciudad_page_id=row[2],
                        created_at=str(row[3]),
                        updated_at=str(row[4]),
                    )
            return None

        return await self._retry_operation("get_cronograma", _get)

    async def create_cronograma(self, cronograma: Cronograma) -> None:
        """Insert a new cronograma entry."""

        async def _insert() -> None:
            await self.conn.execute(
                """
                INSERT INTO cronograma (
                    page_id, day, ciudad_page_id, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    cronograma.page_id,
                    cronograma.day,
                    cronograma.ciudad_page_id,
                    cronograma.created_at,
                    cronograma.updated_at,
                ),
            )
            await self.conn.commit()

        await self._retry_operation("create_cronograma", _insert)

    async def update_cronograma(self, cronograma: Cronograma) -> bool:
        """Update an existing cronograma entry."""

        async def _update() -> bool:
            cursor = await self.conn.execute(
                """
                UPDATE cronograma
                SET day = ?, ciudad_page_id = ?, updated_at = ?
                WHERE page_id = ?
                """,
                (
                    cronograma.day,
                    cronograma.ciudad_page_id,
                    cronograma.updated_at,
                    cronograma.page_id,
                ),
            )
            await self.conn.commit()
            return cursor.rowcount > 0

        return await self._retry_operation("update_cronograma", _update)

    async def delete_cronograma(self, page_id: str) -> bool:
        """Delete a cronograma entry by page_id."""

        async def _delete() -> bool:
            cursor = await self.conn.execute(
                "DELETE FROM cronograma WHERE page_id = ?",
                (page_id,),
            )
            await self.conn.commit()
            return cursor.rowcount > 0

        return await self._retry_operation("delete_cronograma", _delete)

    async def get_pasaje(self, page_id: str) -> Pasaje | None:
        """Retrieve a single pasaje by page_id."""

        async def _get() -> Pasaje | None:
            async with self.conn.execute(
                """
                SELECT page_id, departure, cronograma_page_id,
                ciudad_page_id, created_at, updated_at
                FROM pasajes
                WHERE page_id = ?
                """,
                (page_id,),
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return Pasaje(
                        page_id=str(row[0]),
                        departure=row[1],
                        cronograma_page_id=row[2],
                        ciudad_page_id=row[3],
                        created_at=str(row[4]),
                        updated_at=str(row[5]),
                    )
            return None

        return await self._retry_operation("get_pasaje", _get)

    async def create_pasaje(self, pasaje: Pasaje) -> None:
        """Insert a new pasaje."""

        async def _insert() -> None:
            await self.conn.execute(
                """
                INSERT INTO pasajes (
                    page_id,
                    departure,
                    cronograma_page_id,
                    ciudad_page_id,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    pasaje.page_id,
                    pasaje.departure,
                    pasaje.cronograma_page_id,
                    pasaje.ciudad_page_id,
                    pasaje.created_at,
                    pasaje.updated_at,
                ),
            )
            await self.conn.commit()

        await self._retry_operation("create_pasaje", _insert)

    async def update_pasaje(self, pasaje: Pasaje) -> bool:
        """Update an existing pasaje."""

        async def _update() -> bool:
            cursor = await self.conn.execute(
                """
                UPDATE pasajes
                SET departure = ?, cronograma_page_id = ?,
                    ciudad_page_id = ?, updated_at = ?
                WHERE page_id = ?
                """,
                (
                    pasaje.departure,
                    pasaje.cronograma_page_id,
                    pasaje.ciudad_page_id,
                    pasaje.updated_at,
                    pasaje.page_id,
                ),
            )
            await self.conn.commit()
            return cursor.rowcount > 0

        return await self._retry_operation("update_pasaje", _update)

    async def delete_pasaje(self, page_id: str) -> bool:
        """Delete a pasaje by page_id."""

        async def _delete() -> bool:
            cursor = await self.conn.execute(
                "DELETE FROM pasajes WHERE page_id = ?",
                (page_id,),
            )
            await self.conn.commit()
            return cursor.rowcount > 0

        return await self._retry_operation("delete_pasaje", _delete)

    async def get_atraccion(self, page_id: str) -> Atraccion | None:
        """Retrieve a single atraccion by page_id."""

        async def _get() -> Atraccion | None:
            async with self.conn.execute(
                """
                SELECT page_id, name, fecha, cronograma_page_id,
                ciudad_page_id, created_at, updated_at
                FROM atracciones
                WHERE page_id = ?
                """,
                (page_id,),
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return Atraccion(
                        page_id=str(row[0]),
                        name=row[1],
                        fecha=row[2],
                        cronograma_page_id=row[3],
                        ciudad_page_id=row[4],
                        created_at=str(row[5]),
                        updated_at=str(row[6]),
                    )
            return None

        return await self._retry_operation("get_atraccion", _get)

    async def create_atraccion(self, atraccion: Atraccion) -> None:
        """Insert a new atraccion."""

        async def _insert() -> None:
            await self.conn.execute(
                """
                INSERT INTO atracciones (
                    page_id,
                    name,
                    fecha,
                    cronograma_page_id,
                    ciudad_page_id,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    atraccion.page_id,
                    atraccion.name,
                    atraccion.fecha,
                    atraccion.cronograma_page_id,
                    atraccion.ciudad_page_id,
                    atraccion.created_at,
                    atraccion.updated_at,
                ),
            )
            await self.conn.commit()

        await self._retry_operation("create_atraccion", _insert)

    async def update_atraccion(self, atraccion: Atraccion) -> bool:
        """Update an existing atraccion."""

        async def _update() -> bool:
            cursor = await self.conn.execute(
                """
                UPDATE atracciones
                SET name = ?, fecha = ?, cronograma_page_id = ?,
                    ciudad_page_id = ?, updated_at = ?
                WHERE page_id = ?
                """,
                (
                    atraccion.name,
                    atraccion.fecha,
                    atraccion.cronograma_page_id,
                    atraccion.ciudad_page_id,
                    atraccion.updated_at,
                    atraccion.page_id,
                ),
            )
            await self.conn.commit()
            return cursor.rowcount > 0

        return await self._retry_operation("update_atraccion", _update)

    async def delete_atraccion(self, page_id: str) -> bool:
        """Delete an atraccion by page_id."""

        async def _delete() -> bool:
            cursor = await self.conn.execute(
                "DELETE FROM atracciones WHERE page_id = ?",
                (page_id,),
            )
            await self.conn.commit()
            return cursor.rowcount > 0

        return await self._retry_operation("delete_atraccion", _delete)

    async def list_ciudades(
        self,
        name: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Ciudad]:
        """List ciudades with optional name filter."""

        async def _list() -> list[Ciudad]:
            where_clauses: list[str] = []
            params: list[str | int] = []

            if name:
                where_clauses.append("name LIKE ?")
                params.append(f"%{name}%")

            where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

            async with self.conn.execute(
                f"SELECT page_id, name, created_at, updated_at "
                f"FROM ciudades WHERE {where_sql} ORDER BY name ASC LIMIT ? OFFSET ?",
                (*params, limit, offset),
            ) as cursor:
                rows = await cursor.fetchall()
                return [
                    Ciudad(
                        page_id=str(row[0]),
                        name=row[1],
                        created_at=str(row[2]),
                        updated_at=str(row[3]),
                    )
                    for row in rows
                ]

        return await self._retry_operation("list_ciudades", _list)

    async def list_cronograma(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        ciudad_page_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Cronograma]:
        """List cronograma entries with optional filters."""

        async def _list() -> list[Cronograma]:
            where_clauses: list[str] = []
            params: list[str | int] = []

            if date_from:
                where_clauses.append("day >= ?")
                params.append(date_from)
            if date_to:
                where_clauses.append("day <= ?")
                params.append(date_to)
            if ciudad_page_id:
                where_clauses.append("ciudad_page_id = ?")
                params.append(ciudad_page_id)

            where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

            async with self.conn.execute(
                f"SELECT page_id, day, ciudad_page_id, created_at, updated_at "
                f"FROM cronograma WHERE {where_sql} ORDER BY day ASC LIMIT ? OFFSET ?",
                (*params, limit, offset),
            ) as cursor:
                rows = await cursor.fetchall()
                return [
                    Cronograma(
                        page_id=str(row[0]),
                        day=row[1],
                        ciudad_page_id=row[2],
                        created_at=str(row[3]),
                        updated_at=str(row[4]),
                    )
                    for row in rows
                ]

        return await self._retry_operation("list_cronograma", _list)

    async def list_pasajes(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        ciudad_page_id: str | None = None,
        cronograma_page_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Pasaje]:
        """List pasajes with optional filters."""

        async def _list() -> list[Pasaje]:
            where_clauses: list[str] = []
            params: list[str | int] = []

            if date_from:
                where_clauses.append("departure >= ?")
                params.append(date_from)
            if date_to:
                where_clauses.append("departure <= ?")
                params.append(date_to)
            if ciudad_page_id:
                where_clauses.append("ciudad_page_id = ?")
                params.append(ciudad_page_id)
            if cronograma_page_id:
                where_clauses.append("cronograma_page_id = ?")
                params.append(cronograma_page_id)

            where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

            async with self.conn.execute(
                f"SELECT page_id, departure, cronograma_page_id, ciudad_page_id, "
                f"created_at, updated_at "
                f"FROM pasajes WHERE {where_sql} "
                f"ORDER BY departure ASC LIMIT ? OFFSET ?",
                (*params, limit, offset),
            ) as cursor:
                rows = await cursor.fetchall()
                return [
                    Pasaje(
                        page_id=str(row[0]),
                        departure=row[1],
                        cronograma_page_id=row[2],
                        ciudad_page_id=row[3],
                        created_at=str(row[4]),
                        updated_at=str(row[5]),
                    )
                    for row in rows
                ]

        return await self._retry_operation("list_pasajes", _list)

    async def list_atracciones(
        self,
        name: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        ciudad_page_id: str | None = None,
        cronograma_page_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Atraccion]:
        """List atracciones with optional filters."""

        async def _list() -> list[Atraccion]:
            where_clauses: list[str] = []
            params: list[str | int] = []

            if name:
                where_clauses.append("name LIKE ?")
                params.append(f"%{name}%")
            if date_from:
                where_clauses.append("fecha >= ?")
                params.append(date_from)
            if date_to:
                where_clauses.append("fecha <= ?")
                params.append(date_to)
            if ciudad_page_id:
                where_clauses.append("ciudad_page_id = ?")
                params.append(ciudad_page_id)
            if cronograma_page_id:
                where_clauses.append("cronograma_page_id = ?")
                params.append(cronograma_page_id)

            where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

            async with self.conn.execute(
                f"SELECT page_id, name, fecha, cronograma_page_id, ciudad_page_id, "
                f"created_at, updated_at "
                f"FROM atracciones WHERE {where_sql} "
                f"ORDER BY fecha ASC LIMIT ? OFFSET ?",
                (*params, limit, offset),
            ) as cursor:
                rows = await cursor.fetchall()
                return [
                    Atraccion(
                        page_id=str(row[0]),
                        name=row[1],
                        fecha=row[2],
                        cronograma_page_id=row[3],
                        ciudad_page_id=row[4],
                        created_at=str(row[5]),
                        updated_at=str(row[6]),
                    )
                    for row in rows
                ]

        return await self._retry_operation("list_atracciones", _list)

    def _validate_table_name(self, table_name: str) -> None:
        allowed = {
            "atracciones",
            "ciudades",
            "cronograma",
            "fail_log",
            "gastos",
            "pasajes",
        }
        if table_name not in allowed:
            raise DatabaseError(f"Unsupported table name: {table_name}")

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
                """
                SELECT page_id, payment_method, description, category,
                amount, date, created_at, updated_at, persona, ciudad_page_id, ciudad
                FROM gastos
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            ) as cursor:
                rows = await cursor.fetchall()
                return [
                    Gasto(
                        page_id=row[0],
                        payment_method=row[1],
                        description=row[2],
                        category=row[3],
                        amount=row[4],
                        date=row[5],
                        created_at=row[6],
                        updated_at=row[7],
                        persona=row[8],
                        ciudad_page_id=row[9],
                        ciudad=row[10],
                    )
                    for row in rows
                ]

        return await self._retry_operation("list", _list)

    async def _build_fts_query(self, query: str) -> str:
        """Build FTS5 query string with proper escaping.

        Args:
            query: The search query string.

        Returns:
            Properly escaped FTS query.
        """
        # Simple escaping - quote terms with spaces
        parts = []
        for term in query.split():
            if " " in term:
                # Phrase search - already quoted
                parts.append(term)
            else:
                # Prefix search
                parts.append(f"{term}*")
        return " ".join(parts)

    async def search_gastos(
        self,
        q: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        persona: str | None = None,
        payment_method: str | None = None,
        category: str | None = None,
        amount_min: float | None = None,
        amount_max: float | None = None,
        sort_by: str = "created_at",
        order: str = "desc",
        limit: int = 100,
        offset: int = 0,
    ) -> list[Gasto]:
        """Search gastos with filters and full-text search.

        Args:
            q: Full-text search query (uses FTS when provided).
            date_from: Inclusive start date (YYYY-MM-DD).
            date_to: Inclusive end date (YYYY-MM-DD).
            persona: Filter by exact persona value.
            payment_method: Filter by exact payment method.
            category: Filter by category (contains).
            amount_min: Minimum amount.
            amount_max: Maximum amount.
            sort_by: Field to sort by (date, created_at, amount).
            order: Sort order (asc, desc).
            limit: Maximum number of results.
            offset: Number of results to skip.

        Returns:
            List of matching Gasto instances.

        Raises:
            DatabaseError: If the operation fails after retries.
        """

        async def _search() -> list[Gasto]:
            # Build WHERE clause and parameters
            where_clauses: list[str] = []
            params: list[str | float | None] = []
            param_index = 1

            # Use FTS for full-text search
            if q:
                fts_query = await self._build_fts_query(q)
                where_clauses.append(
                    "gastos.rowid IN "
                    "(SELECT rowid FROM gastos_fts "
                    "WHERE gastos_fts MATCH ?)"
                )
                params.append(fts_query)
            else:
                # Regular filters when not using FTS
                if date_from:
                    where_clauses.append(f"date >= ?{param_index}")
                    params.append(date_from)
                    param_index += 1
                if date_to:
                    where_clauses.append(f"date <= ?{param_index}")
                    params.append(date_to)
                    param_index += 1
                if persona:
                    where_clauses.append(f"persona = ?{param_index}")
                    params.append(persona)
                    param_index += 1
                if payment_method:
                    where_clauses.append(f"payment_method = ?{param_index}")
                    params.append(payment_method)
                    param_index += 1
                if category:
                    where_clauses.append(f"category LIKE ?{param_index}")
                    params.append(f"%{category}%")
                    param_index += 1
                if amount_min:
                    where_clauses.append(f"amount >= ?{param_index}")
                    params.append(amount_min)
                    param_index += 1
                if amount_max:
                    where_clauses.append(f"amount <= ?{param_index}")
                    params.append(amount_max)
                    param_index += 1

            # Validate sort_by
            valid_sort_fields = {"date", "created_at", "amount"}
            sort_field = sort_by if sort_by in valid_sort_fields else "created_at"
            if sort_field != sort_by:
                logger.warning(f"Invalid sort_by '{sort_by}', using 'created_at'")

            # Validate order
            order_upper = order.upper()
            order_sql = "DESC" if order_upper not in ("ASC", "DESC") else order_upper
            if order_upper != order_upper:
                logger.warning(f"Invalid order '{order}', using 'DESC'")

            # Build SQL query
            where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
            order_final = f"ORDER BY {sort_field} {order_sql}"

            async with self.conn.execute(
                f"SELECT page_id, payment_method, description, category, "
                f"amount, date, created_at, updated_at, persona, "
                f"ciudad_page_id, ciudad "
                f"FROM gastos WHERE {where_sql} {order_final} LIMIT ? OFFSET ?",
                (*params, limit, offset),
            ) as cursor:
                rows = await cursor.fetchall()
                return [
                    Gasto(
                        page_id=row[0],
                        payment_method=row[1],
                        description=row[2],
                        category=row[3],
                        amount=row[4],
                        date=row[5],
                        created_at=row[6],
                        updated_at=row[7],
                        persona=row[8],
                        ciudad_page_id=row[9],
                        ciudad=row[10],
                    )
                    for row in rows
                ]

        return await self._retry_operation("search", _search)

    async def get_all_gastos_page_ids(self) -> set[str]:
        """Get all Gastos page IDs from the database.

        Returns:
            Set of all page IDs.

        Raises:
            DatabaseError: If the operation fails after retries.
        """

        async def _get_all_ids() -> set[str]:
            async with self.conn.execute("SELECT page_id FROM gastos") as cursor:
                rows = await cursor.fetchall()
                return {row[0] for row in rows}

        return await self._retry_operation("get_all_ids", _get_all_ids)

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
            last_id = cursor.lastrowid
            return last_id if last_id is not None else 0

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
