from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, ParamSpec, TypeVar

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
        # Create ciudades first so gastos can reference it
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
            CREATE TABLE IF NOT EXISTS gastos (
                page_id TEXT PRIMARY KEY,
                payment_method TEXT,
                description TEXT,
                category TEXT,
                amount REAL,
                date DATE,
                persona TEXT,
                ciudad_page_id TEXT REFERENCES ciudades(page_id) ON DELETE SET NULL,
                ciudad TEXT,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
        """
        )
        await self._ensure_gastos_schema()
        await self._ensure_gastos_fts()
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
        """Ensure gastos table has expected columns and foreign key constraints."""
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

        # Check if FK constraint exists from ciudad_page_id to ciudades
        async with self.conn.execute("PRAGMA foreign_key_list(gastos)") as cursor:
            fk_rows = await cursor.fetchall()
            has_fk = any(
                fk[3] == "ciudad_page_id" and fk[2] == "ciudades" for fk in fk_rows
            )

        # If FK constraint doesn't exist, recreate the table
        if not has_fk:
            logger.info("Adding foreign key constraint to gastos.ciudad_page_id")
            # Clean up any stale table from prior failed migration
            await self.conn.execute("DROP TABLE IF EXISTS gastos_new")
            # Create a new table with the FK constraint
            await self.conn.execute(
                """
                CREATE TABLE gastos_new (
                    page_id TEXT PRIMARY KEY,
                    payment_method TEXT,
                    description TEXT,
                    category TEXT,
                    amount REAL,
                    date DATE,
                    persona TEXT,
                    ciudad_page_id TEXT REFERENCES ciudades(page_id) ON DELETE SET NULL,
                    ciudad TEXT,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
            """
            )
            # Copy data from old table, NULLing invalid ciudad_page_id references
            await self.conn.execute(
                """
                INSERT INTO gastos_new
                SELECT page_id, payment_method, description, category, amount, date,
                       persona,
                       CASE
                           WHEN ciudad_page_id IS NULL THEN NULL
                           WHEN EXISTS (
                               SELECT 1 FROM ciudades c
                               WHERE c.page_id = gastos.ciudad_page_id
                           ) THEN ciudad_page_id
                           ELSE NULL
                       END,
                       ciudad, created_at, updated_at
                FROM gastos
            """
            )
            # Drop old table and rename new one
            await self.conn.execute("DROP TABLE gastos")
            await self.conn.execute("ALTER TABLE gastos_new RENAME TO gastos")
            await self.conn.commit()

    async def _ensure_schema_meta_table(self) -> None:
        """Ensure schema_meta table exists for feature-specific versioning.

        This table stores schema versions for individual features (like FTS)
        without clobbering the global database user_version pragma.
        """
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_meta (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            )
        """
        )

    async def _get_schema_version(self, key: str) -> int:
        """Get schema version for a specific feature.

        Args:
            key: The feature key (e.g., 'fts_version').

        Returns:
            The schema version, or 0 if not set.
        """
        cursor = await self.conn.execute(
            "SELECT value FROM schema_meta WHERE key = ?", (key,)
        )
        row = await cursor.fetchone()
        return row[0] if row else 0

    async def _set_schema_version(self, key: str, version: int) -> None:
        """Set schema version for a specific feature.

        Args:
            key: The feature key (e.g., 'fts_version').
            version: The schema version number.
        """
        await self.conn.execute(
            """
            INSERT OR REPLACE INTO schema_meta (key, value)
            VALUES (?, ?)
        """,
            (key, version),
        )

    async def _ensure_gastos_fts(self) -> None:
        """Ensure FTS5 virtual table exists for full-text search on gastos.

        Uses schema_meta table to track FTS schema version.
        When upgrading, rebuilds the FTS index to ensure consistency.
        """
        # Ensure schema_meta table exists
        await self._ensure_schema_meta_table()

        # FTS schema version - increment when changing trigger/index semantics
        fts_schema_version = 1

        # Check current FTS schema version from metadata table
        current_version = await self._get_schema_version("fts_version")

        # Check if FTS table already exists
        cursor = await self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='gastos_fts'"
        )
        fts_exists = await cursor.fetchone()

        # Create FTS5 virtual table if it doesn't exist
        await self.conn.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS gastos_fts USING fts5(
                description,
                category,
                persona,
                content='gastos',
                content_rowid='rowid'
            )
        """
        )

        # Drop existing triggers (in case they were created with
        # incorrect implementation)
        # This ensures we always have the correct trigger definitions
        for trigger_name in [
            "gastos_fts_insert",
            "gastos_fts_update",
            "gastos_fts_delete",
        ]:
            await self.conn.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")

        # Create triggers to keep FTS in sync with gastos table
        # For external-content FTS5 tables, we must use the 'delete' sentinel pattern

        # Trigger for INSERT
        await self.conn.execute(
            """
            CREATE TRIGGER gastos_fts_insert
            AFTER INSERT ON gastos
            BEGIN
                INSERT INTO gastos_fts(rowid, description, category, persona)
                VALUES (NEW.rowid, NEW.description, NEW.category, NEW.persona);
            END
        """
        )

        # Trigger for UPDATE - use 'delete' sentinel for OLD, then insert NEW
        await self.conn.execute(
            """
            CREATE TRIGGER gastos_fts_update
            AFTER UPDATE ON gastos
            BEGIN
                INSERT INTO gastos_fts
                    (gastos_fts, rowid, description, category, persona)
                VALUES('delete', OLD.rowid, OLD.description, OLD.category, OLD.persona);
                INSERT INTO gastos_fts(rowid, description, category, persona)
                VALUES (NEW.rowid, NEW.description, NEW.category, NEW.persona);
            END
        """
        )

        # Trigger for DELETE - use 'delete' sentinel
        await self.conn.execute(
            """
            CREATE TRIGGER gastos_fts_delete
            AFTER DELETE ON gastos
            BEGIN
                INSERT INTO gastos_fts
                    (gastos_fts, rowid, description, category, persona)
                VALUES('delete', OLD.rowid, OLD.description, OLD.category, OLD.persona);
            END
        """
        )

        # Rebuild FTS index if:
        # 1. FTS table was just created (no existing data indexed)
        # 2. Schema version is outdated (upgrading from old trigger semantics)
        needs_rebuild = not fts_exists or current_version < fts_schema_version

        if needs_rebuild:
            await self.conn.execute(
                "INSERT INTO gastos_fts(gastos_fts) VALUES('rebuild')"
            )

        # Update FTS schema version in metadata table
        await self._set_schema_version("fts_version", fts_schema_version)

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

    async def get_gastos_totals(
        self,
        q: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        persona: str | None = None,
        payment_method: str | None = None,
        category: str | None = None,
        amount_min: float | None = None,
        amount_max: float | None = None,
        ciudad: str | None = None,
    ) -> tuple[float, int, float, float, float]:
        """Get aggregate totals for gastos with filters.

        Unlike search_gastos, this method combines ALL filters together
        including FTS and structured filters.

        Args:
            q: Full-text search query (combined with other filters).
            date_from: Inclusive start date (YYYY-MM-DD).
            date_to: Inclusive end date (YYYY-MM-DD).
            persona: Filter by exact persona value.
            payment_method: Filter by exact payment method.
            category: Filter by category (contains).
            amount_min: Minimum amount.
            amount_max: Maximum amount.
            ciudad: Filter by city name.

        Returns:
            Tuple of (total, count, min, max, avg).
            Returns zero-safe defaults when no rows match.

        Raises:
            DatabaseError: If the operation fails after retries.
        """

        async def _get_totals() -> tuple[float, int, float, float, float]:
            # Build WHERE clause and parameters
            where_clauses: list[str] = []
            params: list[str | float | None] = []

            # FTS filter (combined with other filters)
            if q:
                fts_query = await self._build_fts_query(q)
                where_clauses.append(
                    "gastos.rowid IN "
                    "(SELECT rowid FROM gastos_fts "
                    "WHERE gastos_fts MATCH ?)"
                )
                params.append(fts_query)

            # Date filters
            if date_from:
                where_clauses.append("date >= ?")
                params.append(date_from)
            if date_to:
                where_clauses.append("date <= ?")
                params.append(date_to)

            # Structured filters
            if persona:
                where_clauses.append("persona = ?")
                params.append(persona)
            if payment_method:
                where_clauses.append("payment_method = ?")
                params.append(payment_method)
            if category:
                where_clauses.append("category LIKE ?")
                params.append(f"%{category}%")

            # Amount range filters
            if amount_min is not None:
                where_clauses.append("amount >= ?")
                params.append(amount_min)
            if amount_max is not None:
                where_clauses.append("amount <= ?")
                params.append(amount_max)

            # City filter
            if ciudad:
                where_clauses.append("ciudad = ?")
                params.append(ciudad)

            # Build SQL query
            where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

            async with self.conn.execute(
                f"SELECT "
                f"COALESCE(SUM(amount), 0.0) as total, "
                f"COUNT(*) as count, "
                f"COALESCE(MIN(amount), 0.0) as min, "
                f"COALESCE(MAX(amount), 0.0) as max, "
                f"COALESCE(AVG(amount), 0.0) as avg "
                f"FROM gastos WHERE {where_sql}",
                params,
            ) as cursor:
                row = await cursor.fetchone()
                if row is None:
                    # Zero-safe defaults for empty result
                    return (0.0, 0, 0.0, 0.0, 0.0)

                total = float(row[0]) if row[0] is not None else 0.0
                count = int(row[1]) if row[1] is not None else 0
                min_val = float(row[2]) if row[2] is not None else 0.0
                max_val = float(row[3]) if row[3] is not None else 0.0
                avg_val = float(row[4]) if row[4] is not None else 0.0

                return (total, count, min_val, max_val, avg_val)

        return await self._retry_operation("get_totals", _get_totals)

    async def get_gastos_summary(
        self,
        group_by: list[str],
        q: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        persona: str | None = None,
        payment_method: str | None = None,
        category: str | None = None,
        amount_min: float | None = None,
        amount_max: float | None = None,
        ciudad: str | None = None,
    ) -> tuple[list[dict[str, Any]], float, int]:
        """Get grouped summary for gastos with filters.

        Supports single or multi-dimension grouping with exploded handling
        for category and persona fields.

        Args:
            group_by: List of grouping dimensions (e.g., ['category', 'persona']).
            q: Full-text search query (combined with other filters).
            date_from: Inclusive start date (YYYY-MM-DD).
            date_to: Inclusive end date (YYYY-MM-DD).
            persona: Filter by exact persona value.
            payment_method: Filter by exact payment method.
            category: Filter by category (contains).
            amount_min: Minimum amount.
            amount_max: Maximum amount.
            ciudad: Filter by city name.

        Returns:
            Tuple of (groups, grand_total, total_count).
            - groups: List of dicts with 'key', 'total', 'count'
            - grand_total: Total sum of filtered base set (pre-explosion)
            - total_count: Count of filtered base set (pre-explosion)

        Raises:
            DatabaseError: If the operation fails after retries.
        """

        async def _get_summary() -> tuple[list[dict[str, Any]], float, int]:
            # Build WHERE clause and parameters (same as totals)
            where_clauses: list[str] = []
            params: list[str | float | None] = []

            # FTS filter (combined with other filters)
            if q:
                fts_query = await self._build_fts_query(q)
                where_clauses.append(
                    "gastos.rowid IN "
                    "(SELECT rowid FROM gastos_fts "
                    "WHERE gastos_fts MATCH ?)"
                )
                params.append(fts_query)

            # Date filters
            if date_from:
                where_clauses.append("date >= ?")
                params.append(date_from)
            if date_to:
                where_clauses.append("date <= ?")
                params.append(date_to)

            # Structured filters
            if persona:
                where_clauses.append("persona = ?")
                params.append(persona)
            if payment_method:
                where_clauses.append("payment_method = ?")
                params.append(payment_method)
            if category:
                where_clauses.append("category LIKE ?")
                params.append(f"%{category}%")

            # Amount range filters
            if amount_min is not None:
                where_clauses.append("amount >= ?")
                params.append(amount_min)
            if amount_max is not None:
                where_clauses.append("amount <= ?")
                params.append(amount_max)

            # City filter
            if ciudad:
                where_clauses.append("ciudad = ?")
                params.append(ciudad)

            where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

            # Compute grand_total and total_count from filtered base set
            async with self.conn.execute(
                f"SELECT COALESCE(SUM(amount), 0.0), COUNT(*) "
                f"FROM gastos WHERE {where_sql}",
                params,
            ) as cursor:
                row = await cursor.fetchone()
                if row is None:
                    grand_total = 0.0
                    total_count = 0
                else:
                    grand_total = float(row[0]) if row[0] is not None else 0.0
                    total_count = int(row[1]) if row[1] is not None else 0

            # If no grouping requested, return empty groups
            if not group_by:
                return ([], grand_total, total_count)

            # Fetch all filtered rows for Python-side grouping
            async with self.conn.execute(
                f"SELECT category, persona, date, ciudad, amount "
                f"FROM gastos WHERE {where_sql}",
                params,
            ) as cursor:
                rows = await cursor.fetchall()

            # Group rows in Python with exploded handling
            # Key: tuple of dimension values, Value: (total, count)
            group_aggregates: dict[tuple[str, ...], tuple[float, int]] = {}

            for row in rows:
                category_val = str(row[0]) if row[0] is not None else None
                persona_val = str(row[1]) if row[1] is not None else None
                date_val = str(row[2]) if row[2] is not None else None
                ciudad_val = str(row[3]) if row[3] is not None else None
                amount_val = float(row[4]) if row[4] is not None else 0.0

                # Expand dimension values (explode category/persona if needed)
                expanded_values: dict[str, list[str]] = {}
                for dim in group_by:
                    if dim == "category":
                        if category_val:
                            # Split comma-separated values and deduplicate
                            # (preserve order using dict.fromkeys)
                            values = list(
                                dict.fromkeys(
                                    v.strip()
                                    for v in category_val.split(",")
                                    if v.strip()
                                )
                            )
                            expanded_values[dim] = values if values else ["Unknown"]
                        else:
                            expanded_values[dim] = ["Unknown"]
                    elif dim == "persona":
                        if persona_val:
                            # Split comma-separated values and deduplicate
                            # (preserve order using dict.fromkeys)
                            values = list(
                                dict.fromkeys(
                                    v.strip()
                                    for v in persona_val.split(",")
                                    if v.strip()
                                )
                            )
                            expanded_values[dim] = values if values else ["Unknown"]
                        else:
                            expanded_values[dim] = ["Unknown"]
                    elif dim == "date":
                        expanded_values[dim] = [date_val if date_val else "Unknown"]
                    elif dim == "ciudad":
                        expanded_values[dim] = [ciudad_val if ciudad_val else "Unknown"]

                # Build cross-product of all dimension values
                # For single dimension, this is just the list of values
                # For multiple dimensions, this creates all combinations
                def build_combinations(
                    dims: list[str], idx: int
                ) -> list[tuple[str, ...]]:
                    """Recursively build all combinations of dimension values."""
                    if idx >= len(dims):
                        return [()]
                    dim = dims[idx]
                    rest_combinations = build_combinations(dims, idx + 1)
                    result = []
                    for val in expanded_values[dim]:
                        for rest in rest_combinations:
                            result.append((val,) + rest)
                    return result

                combinations = build_combinations(group_by, 0)

                # Add amount to each group combination
                for combo in combinations:
                    if combo not in group_aggregates:
                        group_aggregates[combo] = (0.0, 0)
                    current_total, current_count = group_aggregates[combo]
                    group_aggregates[combo] = (
                        current_total + amount_val,
                        current_count + 1,
                    )

            # Convert aggregates to list of group dicts
            groups: list[dict[str, Any]] = []
            for combo, (total, count) in group_aggregates.items():
                # Build key dict from dimension names and values
                key_dict = {dim: combo[i] for i, dim in enumerate(group_by)}
                groups.append(
                    {
                        "key": key_dict,
                        "total": total,
                        "count": count,
                    }
                )

            # Sort groups
            # If any dimension is 'date', sort by date ascending
            # Otherwise, sort by total descending
            if "date" in group_by:
                # Find the index of 'date' in group_by
                date_idx = group_by.index("date")
                groups.sort(key=lambda g: g["key"][group_by[date_idx]])
            else:
                groups.sort(key=lambda g: g["total"], reverse=True)

            return (groups, grand_total, total_count)

        return await self._retry_operation("get_summary", _get_summary)

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
