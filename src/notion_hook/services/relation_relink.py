from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import uuid4

from notion_hook.clients.notion import NotionClient, PropertyNames
from notion_hook.core.logging import get_logger
from notion_hook.core.utils import _extract_relation_ids, get_property_ci
from notion_hook.models.webhook import DateValue
from notion_hook.services.notion_reload import (
    TIMESTAMP_FORMAT_SUFFIX,
    JobProgress,
    JobStatus,
)

logger = get_logger("services.relation_relink")


@dataclass
class RelinkJob:
    """Represents a relation relink job with its state and progress."""

    job_id: str
    status: JobStatus = JobStatus.STARTED
    batch_size: int = 100
    progress: JobProgress = field(default_factory=JobProgress)
    table_progress: dict[str, JobProgress] = field(default_factory=dict)
    started_at: str = field(default_factory=lambda: _now_iso_z())
    updated_at: str = field(default_factory=lambda: _now_iso_z())
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
                "Relink completed successfully"
                if self.status == JobStatus.COMPLETED
                else f"Relink failed: {self.error}"
            )

        return result


class RelinkAlreadyRunningError(RuntimeError):
    """Raised when a relink job is already active."""

    def __init__(self, active_job_id: str) -> None:
        super().__init__(f"Relink already running (job_id={active_job_id})")
        self.active_job_id = active_job_id


# Table name → (date property name, update method name on NotionClient)
TABLE_CONFIG: dict[str, tuple[str, str]] = {
    "gastos": ("Date", "update_gastos_cronograma_relation"),
    "pasajes": ("Departure", "update_pasajes_cronograma_relation"),
    "atracciones": ("Fecha", "update_atracciones_cronograma_relation"),
}


class RelationRelinkService:
    """Service for bulk re-linking Cronograma relations across all tables."""

    TABLE_ORDER = ["gastos", "pasajes", "atracciones"]
    MIN_BATCH_SIZE = 1
    MAX_BATCH_SIZE = 100

    def __init__(
        self,
        notion_client: NotionClient,
        max_job_age_hours: float = 24.0,
    ) -> None:
        self.notion_client = notion_client
        self.max_job_age_hours = max_job_age_hours
        self._jobs: dict[str, RelinkJob] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._lock = asyncio.Lock()

    async def create_job(self, batch_size: int = 100) -> str:
        """Create a new relink job."""
        if not (self.MIN_BATCH_SIZE <= batch_size <= self.MAX_BATCH_SIZE):
            raise ValueError(
                "batch_size must be between "
                f"{self.MIN_BATCH_SIZE} and {self.MAX_BATCH_SIZE}"
            )

        async with self._lock:
            active_job_id = self._get_active_job_id_locked()
            if active_job_id is not None:
                raise RelinkAlreadyRunningError(active_job_id)

            job_id = str(uuid4())
            job = RelinkJob(
                job_id=job_id,
                batch_size=batch_size,
                table_progress={table: JobProgress() for table in self.TABLE_ORDER},
            )
            self._jobs[job_id] = job

        logger.info(f"Created relink job {job_id} with batch_size={batch_size}")
        return job_id

    def get_job(self, job_id: str) -> RelinkJob | None:
        """Get a job by ID."""
        return self._jobs.get(job_id)

    async def start_relink(self, job_id: str) -> None:
        """Start a relink job execution in background."""
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
                name=f"relation-relink-{job_id}",
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
            job.updated_at = _now_iso_z()

            cronograma_map = await self._build_cronograma_map()
            logger.info(f"Built cronograma map with {len(cronograma_map)} date entries")

            for table in self.TABLE_ORDER:
                await self._relink_table(job, table, cronograma_map)

            job.status = JobStatus.COMPLETED
            job.completed_at = _now_iso_z()
            job.updated_at = job.completed_at
        except Exception as e:
            job.status = JobStatus.FAILED
            job.failed_at = _now_iso_z()
            job.updated_at = job.failed_at
            job.error = str(e)
            logger.error(f"Relink job {job.job_id} failed: {e}", exc_info=True)
        finally:
            job.updated_at = _now_iso_z()
            try:
                cleaned = await self.cleanup_old_jobs()
                if cleaned:
                    logger.info(f"Cleaned {cleaned} old relink jobs")
            except Exception as e:
                logger.warning(f"Failed cleaning old relink jobs: {e}")

    async def _build_cronograma_map(self) -> dict[str, list[str]]:
        """Fetch all Cronograma pages and build a date → page_ids mapping."""
        pages = await self.notion_client.query_all_cronograma()
        date_map: dict[str, list[str]] = {}

        for page in pages:
            page_id = page["id"]
            properties = page.get("properties", {})
            title_prop = get_property_ci(properties, PropertyNames.CRONOGRAMA_DAY)
            if not title_prop:
                continue

            title_parts = title_prop.get("title", [])
            if not title_parts:
                continue

            date_str = "".join(
                part.get("plain_text", "") for part in title_parts
            ).strip()
            if not date_str:
                continue

            date_map.setdefault(date_str, []).append(page_id)

        return date_map

    async def _relink_table(
        self,
        job: RelinkJob,
        table: str,
        cronograma_map: dict[str, list[str]],
    ) -> None:
        """Re-link all pages in a single table."""
        date_property_name, update_method_name = TABLE_CONFIG[table]
        update_method = getattr(self.notion_client, update_method_name)

        query_method = getattr(self.notion_client, f"query_all_{table}")
        pages = await query_method()

        table_progress = job.table_progress[table]
        table_progress.total = len(pages)
        job.progress.total += len(pages)
        job.updated_at = _now_iso_z()

        for i in range(0, len(pages), job.batch_size):
            batch = pages[i : i + job.batch_size]
            for page in batch:
                try:
                    changed = await self._relink_page(
                        page, date_property_name, cronograma_map, update_method
                    )
                    table_progress.processed += 1
                    job.progress.processed += 1
                    if changed:
                        table_progress.updated += 1
                        job.progress.updated += 1
                except Exception as e:
                    table_progress.processed += 1
                    table_progress.failed += 1
                    job.progress.processed += 1
                    job.progress.failed += 1
                    logger.warning(
                        f"Failed to relink {table} page "
                        f"{page.get('id', 'unknown')}: {e}"
                    )

            job.updated_at = _now_iso_z()

    async def _relink_page(
        self,
        page: dict[str, Any],
        date_property_name: str,
        cronograma_map: dict[str, list[str]],
        update_method: Any,
    ) -> bool:
        """Re-link a single page. Returns True if the relation was changed."""
        page_id = page["id"]
        properties = page.get("properties", {})

        # Extract date from page
        computed_ids: list[str] = []
        date_prop = get_property_ci(properties, date_property_name)
        if date_prop and date_prop.get("date"):
            try:
                date_value = DateValue.model_validate(date_prop["date"])
                computed_ids = self._compute_cronograma_ids(
                    date_value=date_value,
                    date_property_name=date_property_name,
                    cronograma_map=cronograma_map,
                )
            except Exception as e:
                logger.debug(f"Failed to parse date for page {page_id}: {e}")

        # Extract current relation IDs
        cronograma_prop = get_property_ci(properties, PropertyNames.CRONOGRAMA)
        current_ids = _extract_relation_ids(cronograma_prop)

        # Compare (order-insensitive)
        if sorted(computed_ids) == sorted(current_ids):
            return False

        await update_method(page_id, computed_ids)
        return True

    def _compute_cronograma_ids(
        self,
        *,
        date_value: DateValue,
        date_property_name: str,
        cronograma_map: dict[str, list[str]],
    ) -> list[str]:
        # Keep parity with webhook behavior: only Gastos "Date" supports ranges.
        if date_property_name == "Date" and date_value.end is not None:
            current: date = date_value.start
            end = date_value.end
            date_keys: list[str] = []
            while current <= end:
                date_keys.append(current.isoformat())
                current += timedelta(days=1)
        else:
            date_keys = [date_value.start.isoformat()]

        result: list[str] = []
        for date_key in date_keys:
            for page_id in cronograma_map.get(date_key, []):
                if page_id not in result:
                    result.append(page_id)
        return result

    def _get_active_job_id_locked(self) -> str | None:
        for job_id, job in self._jobs.items():
            if job.status in (JobStatus.STARTED, JobStatus.IN_PROGRESS):
                return job_id
        return None

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
