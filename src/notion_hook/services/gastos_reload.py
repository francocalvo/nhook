from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import uuid4

from notion_hook.clients.notion import NotionClient
from notion_hook.core.database import DatabaseClient
from notion_hook.core.logging import get_logger
from notion_hook.models.gastos import Gasto

logger = get_logger("services.gastos_reload")

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
            "progress": {
                "total": self.progress.total,
                "processed": self.progress.processed,
                "created": self.progress.created,
                "updated": self.progress.updated,
                "deleted": self.progress.deleted,
                "failed": self.progress.failed,
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


class GastosReloadService:
    """Service for full reload/sync of Gastos from Notion to local database."""

    def __init__(
        self,
        notion_client: NotionClient,
        database_client: DatabaseClient,
        max_job_age_hours: float = 24.0,
    ) -> None:
        """Initialize the GastosReloadService.

        Args:
            notion_client: The Notion API client.
            database_client: The database client.
            max_job_age_hours: Maximum age in hours before jobs are cleaned up.
        """
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
        """Create a new reload job.

        Args:
            mode: Reload mode (full or incremental).
            batch_size: Number of records per batch.
            delete_missing: Whether to delete records not in Notion.

        Returns:
            The job ID.

        Raises:
            ReloadAlreadyRunningError: If an active reload is already running.
        """
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
            )
            self._jobs[job_id] = job
        logger.info(
            f"Created reload job {job_id} with mode={mode.value}, "
            f"batch_size={batch_size}"
        )
        return job_id

    def get_job(self, job_id: str) -> ReloadJob | None:
        """Get a job by ID.

        Args:
            job_id: The job ID.

        Returns:
            The job if found, None otherwise.
        """
        return self._jobs.get(job_id)

    async def start_reload(
        self,
        job_id: str,
    ) -> None:
        """Start a reload job execution in background.

        Args:
            job_id: The job ID to execute.
        """
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

            logger.info(f"Starting reload job {job_id} execution")
            task = asyncio.create_task(
                self._execute_job(job_id),
                name=f"gastos-reload-{job_id}",
            )
            self._tasks[job_id] = task
            task.add_done_callback(lambda _: self._tasks.pop(job_id, None))

    async def _execute_job(self, job_id: str) -> None:
        """Execute a reload job.

        Args:
            job_id: The job ID to execute.
        """
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

            logger.info(
                f"Reload job {job.job_id} completed: "
                f"created={job.progress.created}, "
                f"updated={job.progress.updated}, "
                f"deleted={job.progress.deleted}, "
                f"failed={job.progress.failed}"
            )
        except Exception as e:
            job.status = JobStatus.FAILED
            job.failed_at = _now_iso_z()
            job.updated_at = job.failed_at
            job.error = str(e)
            logger.error(f"Reload job {job.job_id} failed: {e}", exc_info=True)
        finally:
            await self._touch_job(job)

    async def _execute_full_reload(self, job: ReloadJob) -> None:
        """Execute a full reload of all Gastos.

        Args:
            job: The reload job.
        """
        logger.info(f"Starting full reload for job {job.job_id}")

        notion_pages = await self.notion_client.query_all_gastos()
        job.progress.total = len(notion_pages)
        await self._touch_job(job)

        deleted = await self.database_client.clear_gastos()
        job.progress.deleted += deleted
        await self._touch_job(job)

        for i in range(0, len(notion_pages), job.batch_size):
            batch = notion_pages[i : i + job.batch_size]
            await self._process_batch(job, batch, update_if_changed=False)

    async def _execute_incremental_reload(self, job: ReloadJob) -> None:
        """Execute an incremental reload of changed Gastos.

        Args:
            job: The reload job.
        """
        logger.info(f"Starting incremental reload for job {job.job_id}")

        notion_pages = await self.notion_client.query_all_gastos()
        job.progress.total = len(notion_pages)
        await self._touch_job(job)

        if job.delete_missing:
            await self._delete_missing_records(job, notion_pages)

        for i in range(0, len(notion_pages), job.batch_size):
            batch = notion_pages[i : i + job.batch_size]
            await self._process_batch(job, batch, update_if_changed=True)

    async def _delete_missing_records(
        self, job: ReloadJob, notion_pages: list[dict[str, Any]]
    ) -> None:
        """Delete local records not present in Notion.

        Args:
            job: The reload job.
            notion_pages: List of all Notion pages.
        """
        notion_page_ids = {page["id"] for page in notion_pages}

        local_page_ids = await self.database_client.get_all_gastos_page_ids()
        missing = [
            page_id for page_id in local_page_ids if page_id not in notion_page_ids
        ]
        if not missing:
            return

        try:
            deleted = await self.database_client.delete_gastos(missing)
            job.progress.deleted += deleted
        except Exception as e:
            job.progress.failed += len(missing)
            logger.warning(f"Failed to delete missing records: {e}")

        await self._touch_job(job)

    async def _process_batch(
        self,
        job: ReloadJob,
        batch: list[dict[str, Any]],
        *,
        update_if_changed: bool,
    ) -> None:
        """Process a batch of Notion pages.

        Args:
            job: The reload job.
            batch: List of Notion pages to process.
            update_if_changed: If True, only update when fields changed.
        """
        gastos: list[Gasto] = []
        parse_failed = 0
        for page in batch:
            try:
                page_id = page["id"]
                properties = page.get("properties", {})
                created_time = page.get("created_time", "")
                last_edited_time = page.get("last_edited_time", "")

                gasto = Gasto.from_notion_properties(
                    page_id, properties, created_time, last_edited_time
                )
                gastos.append(gasto)
            except Exception as e:
                parse_failed += 1
                logger.warning(
                    f"Failed to process page {page.get('id', 'unknown')}: {e}"
                )

        (
            created,
            updated,
            skipped,
            failed,
        ) = await self.database_client.sync_gastos_batch(
            gastos,
            update_if_changed=update_if_changed,
        )

        job.progress.created += created
        job.progress.updated += updated
        job.progress.processed += created + updated + skipped
        job.progress.failed += failed + parse_failed

        await self._touch_job(job)

    async def _touch_job(self, job: ReloadJob) -> None:
        """Touch job timestamp.

        Args:
            job: The job to update.
        """
        job.updated_at = _now_iso_z()

    async def cleanup_old_jobs(self) -> int:
        """Clean up completed jobs older than max_job_age_hours.

        Returns:
            Number of jobs cleaned up.
        """
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

        if cleaned > 0:
            logger.info(f"Cleaned up {cleaned} old reload jobs")

        return cleaned

    def _get_active_job_id_locked(self) -> str | None:
        for job_id, job in self._jobs.items():
            if job.status in (JobStatus.STARTED, JobStatus.IN_PROGRESS):
                return job_id
        return None


def _now_iso_z() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", TIMESTAMP_FORMAT_SUFFIX)
