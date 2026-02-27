from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import uuid4

from notion_hook.clients.notion import NotionClient
from notion_hook.core.database import DatabaseClient
from notion_hook.core.logging import get_logger
from notion_hook.models.gastos import Gasto
from notion_hook.models.notion_db import Atraccion, Ciudad, Cronograma, Pasaje

logger = get_logger("services.notion_reload")

TIMESTAMP_FORMAT_SUFFIX = "Z"


class ReloadMode(str, Enum):
    """Reload mode options."""

    FULL = "full"
    INCREMENTAL = "incremental"


class JobStatus(str, Enum):
    """Job status values."""

    STARTED = "started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class JobProgress:
    """Progress tracking for a reload job."""

    total: int = 0
    processed: int = 0
    created: int = 0
    updated: int = 0
    deleted: int = 0
    failed: int = 0


@dataclass
class ReloadJob:
    """Represents a reload job with its state and progress."""

    job_id: str
    status: JobStatus = JobStatus.STARTED
    mode: ReloadMode = ReloadMode.FULL
    batch_size: int = 100
    delete_missing: bool = True
    progress: JobProgress = field(default_factory=JobProgress)
    table_progress: dict[str, JobProgress] = field(default_factory=dict)
    started_at: str = field(
        default_factory=lambda: datetime.now(UTC).isoformat().replace("+00:00", "Z")
    )
    updated_at: str = field(
        default_factory=lambda: datetime.now(UTC).isoformat().replace("+00:00", "Z")
    )
    completed_at: str | None = None
    failed_at: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert job to dictionary for API responses."""
        result: dict[str, Any] = {
            "job_id": self.job_id,
            "status": self.status.value,
            "progress": _progress_to_dict(self.progress),
            "table_progress": {
                table: _progress_to_dict(progress)
                for table, progress in self.table_progress.items()
            },
            "started_at": self.started_at,
            "updated_at": self.updated_at,
        }

        if self.completed_at:
            result["completed_at"] = self.completed_at
            result["duration_seconds"] = (
                datetime.fromisoformat(self.completed_at.replace("Z", "+00:00"))
                - datetime.fromisoformat(self.started_at.replace("Z", "+00:00"))
            ).total_seconds()

        if self.failed_at:
            result["failed_at"] = self.failed_at

        if self.error:
            result["error"] = self.error

        if self.status in (JobStatus.COMPLETED, JobStatus.FAILED):
            result["message"] = (
                "Reload completed successfully"
                if self.status == JobStatus.COMPLETED
                else f"Reload failed: {self.error}"
            )

        return result


class ReloadAlreadyRunningError(RuntimeError):
    """Raised when a reload job is already active."""

    def __init__(self, active_job_id: str) -> None:
        super().__init__(f"Reload already running (job_id={active_job_id})")
        self.active_job_id = active_job_id


class NotionReloadService:
    """Service for full/incremental sync of Notion DBs to local SQLite."""

    TABLE_ORDER = ["ciudades", "cronograma", "pasajes", "atracciones", "gastos"]

    def __init__(
        self,
        notion_client: NotionClient,
        database_client: DatabaseClient,
        max_job_age_hours: float = 24.0,
    ) -> None:
        self.notion_client = notion_client
        self.database_client = database_client
        self.max_job_age_hours = max_job_age_hours
        self._jobs: dict[str, ReloadJob] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._lock = asyncio.Lock()

    async def create_job(
        self,
        mode: ReloadMode = ReloadMode.FULL,
        batch_size: int = 100,
        delete_missing: bool = True,
    ) -> str:
        """Create a new reload job."""
        async with self._lock:
            active_job_id = self._get_active_job_id_locked()
            if active_job_id is not None:
                raise ReloadAlreadyRunningError(active_job_id)

            job_id = str(uuid4())
            job = ReloadJob(
                job_id=job_id,
                mode=mode,
                batch_size=batch_size,
                delete_missing=delete_missing,
                table_progress={table: JobProgress() for table in self.TABLE_ORDER},
            )
            self._jobs[job_id] = job

        logger.info(
            f"Created full reload job {job_id} with mode={mode.value}, "
            f"batch_size={batch_size}"
        )
        return job_id

    def get_job(self, job_id: str) -> ReloadJob | None:
        """Get a job by ID."""
        return self._jobs.get(job_id)

    async def start_reload(self, job_id: str) -> None:
        """Start a reload job execution in background."""
        async with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                logger.error(f"Job {job_id} not found")
                return

            if job.status in (
                JobStatus.IN_PROGRESS,
                JobStatus.COMPLETED,
                JobStatus.FAILED,
            ):
                logger.warning(f"Job {job_id} is already {job.status.value}")
                return

            task = asyncio.create_task(
                self._execute_job(job_id),
                name=f"notion-reload-{job_id}",
            )
            self._tasks[job_id] = task
            task.add_done_callback(lambda _: self._tasks.pop(job_id, None))

    async def _execute_job(self, job_id: str) -> None:
        job = self._jobs.get(job_id)
        if job is None:
            logger.error(f"Job {job_id} not found")
            return

        try:
            job.status = JobStatus.IN_PROGRESS
            await self._touch_job(job)

            if job.mode == ReloadMode.FULL:
                await self._execute_full_reload(job)
            else:
                await self._execute_incremental_reload(job)

            job.status = JobStatus.COMPLETED
            job.completed_at = _now_iso_z()
            job.updated_at = job.completed_at
        except Exception as e:
            job.status = JobStatus.FAILED
            job.failed_at = _now_iso_z()
            job.updated_at = job.failed_at
            job.error = str(e)
            logger.error(f"Reload job {job.job_id} failed: {e}", exc_info=True)
        finally:
            await self._touch_job(job)

    async def _execute_full_reload(self, job: ReloadJob) -> None:
        pages_by_table = await self._query_all_tables()
        job.progress.total = sum(len(pages) for pages in pages_by_table.values())
        for table, pages in pages_by_table.items():
            job.table_progress[table].total = len(pages)

        deleted_by_table = await self.database_client.clear_sync_tables(
            include_gastos=True
        )
        deleted_total = 0
        for table, deleted in deleted_by_table.items():
            job.table_progress[table].deleted += deleted
            deleted_total += deleted
        job.progress.deleted += deleted_total
        await self._touch_job(job)

        await self._sync_all_tables(job, pages_by_table, update_if_changed=False)

    async def _execute_incremental_reload(self, job: ReloadJob) -> None:
        pages_by_table = await self._query_all_tables()
        job.progress.total = sum(len(pages) for pages in pages_by_table.values())
        for table, pages in pages_by_table.items():
            job.table_progress[table].total = len(pages)

        if job.delete_missing:
            await self._delete_missing_records(job, pages_by_table)

        await self._sync_all_tables(job, pages_by_table, update_if_changed=True)

    async def _query_all_tables(self) -> dict[str, list[dict[str, Any]]]:
        ciudades = await self.notion_client.query_all_ciudades()
        cronograma = await self.notion_client.query_all_cronograma()
        pasajes = await self.notion_client.query_all_pasajes()
        atracciones = await self.notion_client.query_all_atracciones()
        gastos = await self.notion_client.query_all_gastos()
        return {
            "ciudades": ciudades,
            "cronograma": cronograma,
            "pasajes": pasajes,
            "atracciones": atracciones,
            "gastos": gastos,
        }

    async def _sync_all_tables(
        self,
        job: ReloadJob,
        pages_by_table: dict[str, list[dict[str, Any]]],
        *,
        update_if_changed: bool,
    ) -> None:
        for table in self.TABLE_ORDER:
            pages = pages_by_table[table]
            parser, syncer = self._get_table_handlers(table)
            await self._sync_table(
                job,
                table=table,
                pages=pages,
                parser=parser,
                syncer=syncer,
                update_if_changed=update_if_changed,
            )

    async def _sync_table(
        self,
        job: ReloadJob,
        *,
        table: str,
        pages: list[dict[str, Any]],
        parser: Callable[[dict[str, Any]], Any],
        syncer: Callable[[list[Any], bool], Any],
        update_if_changed: bool,
    ) -> None:
        for i in range(0, len(pages), job.batch_size):
            batch = pages[i : i + job.batch_size]
            parsed: list[Any] = []
            parse_failed = 0
            for page in batch:
                try:
                    parsed.append(parser(page))
                except Exception as e:
                    parse_failed += 1
                    logger.warning(
                        f"Failed to parse {table} page {page.get('id', 'unknown')}: {e}"
                    )

            created, updated, skipped, failed = await syncer(parsed, update_if_changed)
            progress = job.table_progress[table]
            progress.created += created
            progress.updated += updated
            progress.processed += created + updated + skipped
            progress.failed += failed + parse_failed

            job.progress.created += created
            job.progress.updated += updated
            job.progress.processed += created + updated + skipped
            job.progress.failed += failed + parse_failed
            await self._touch_job(job)

    async def _delete_missing_records(
        self, job: ReloadJob, pages_by_table: dict[str, list[dict[str, Any]]]
    ) -> None:
        # Child-first order for predictable FK behavior.
        for table in ["atracciones", "pasajes", "cronograma", "ciudades", "gastos"]:
            notion_ids = {page["id"] for page in pages_by_table[table]}
            local_ids = await self.database_client.get_all_page_ids(table)
            missing = [page_id for page_id in local_ids if page_id not in notion_ids]
            if not missing:
                continue

            try:
                deleted = await self.database_client.delete_by_page_ids(table, missing)
                job.table_progress[table].deleted += deleted
                job.progress.deleted += deleted
            except Exception as e:
                job.table_progress[table].failed += len(missing)
                job.progress.failed += len(missing)
                logger.warning(f"Failed to delete missing {table} records: {e}")

            await self._touch_job(job)

    def _get_table_handlers(
        self, table: str
    ) -> tuple[
        Callable[[dict[str, Any]], Any],
        Callable[[list[Any], bool], Any],
    ]:
        if table == "ciudades":
            return (
                Ciudad.from_notion_page,
                lambda rows,
                update_if_changed: self.database_client.sync_ciudades_batch(
                    rows, update_if_changed=update_if_changed
                ),
            )
        if table == "cronograma":
            return (
                Cronograma.from_notion_page,
                lambda rows,
                update_if_changed: self.database_client.sync_cronograma_batch(
                    rows, update_if_changed=update_if_changed
                ),
            )
        if table == "pasajes":
            return (
                Pasaje.from_notion_page,
                lambda rows, update_if_changed: self.database_client.sync_pasajes_batch(
                    rows, update_if_changed=update_if_changed
                ),
            )
        if table == "atracciones":
            return (
                Atraccion.from_notion_page,
                lambda rows,
                update_if_changed: self.database_client.sync_atracciones_batch(
                    rows, update_if_changed=update_if_changed
                ),
            )
        if table == "gastos":
            return (
                lambda page: Gasto.from_notion_properties(
                    page["id"],
                    page.get("properties", {}),
                    page.get("created_time", ""),
                    page.get("last_edited_time", ""),
                ),
                lambda rows, update_if_changed: self.database_client.sync_gastos_batch(
                    rows, update_if_changed=update_if_changed
                ),
            )
        raise ValueError(f"Unsupported table handler: {table}")

    async def _touch_job(self, job: ReloadJob) -> None:
        job.updated_at = _now_iso_z()

    async def cleanup_old_jobs(self) -> int:
        """Clean up completed jobs older than max_job_age_hours."""
        if not self._jobs:
            return 0

        cutoff = datetime.now(UTC).timestamp() - (self.max_job_age_hours * 3600)
        cleaned = 0

        async with self._lock:
            for job_id, job in list(self._jobs.items()):
                if job.status not in (JobStatus.COMPLETED, JobStatus.FAILED):
                    continue

                if job.completed_at:
                    completed_ts = datetime.fromisoformat(
                        job.completed_at.replace(TIMESTAMP_FORMAT_SUFFIX, "+00:00")
                    ).timestamp()
                    if completed_ts < cutoff:
                        del self._jobs[job_id]
                        cleaned += 1
                elif job.failed_at:
                    failed_ts = datetime.fromisoformat(
                        job.failed_at.replace(TIMESTAMP_FORMAT_SUFFIX, "+00:00")
                    ).timestamp()
                    if failed_ts < cutoff:
                        del self._jobs[job_id]
                        cleaned += 1

        return cleaned

    def _get_active_job_id_locked(self) -> str | None:
        for job_id, job in self._jobs.items():
            if job.status in (JobStatus.STARTED, JobStatus.IN_PROGRESS):
                return job_id
        return None


def _progress_to_dict(progress: JobProgress) -> dict[str, int]:
    return {
        "total": progress.total,
        "processed": progress.processed,
        "created": progress.created,
        "updated": progress.updated,
        "deleted": progress.deleted,
        "failed": progress.failed,
    }


def _now_iso_z() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", TIMESTAMP_FORMAT_SUFFIX)
