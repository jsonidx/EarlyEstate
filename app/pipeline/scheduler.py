"""
Scheduler — enqueues pipeline jobs into the Postgres job table.

Uses APScheduler with an AsyncIOScheduler. Job records are inserted into
the `job` table; the worker process polls and executes them.

Cadences (from PRD):
  - Insolvency portal: every 30 minutes (configurable)
  - Bank portals (Sparkasse, LBS): every 24 hours
  - Retention purge: daily
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import AsyncSessionLocal
from app.models.job import Job

logger = structlog.get_logger(__name__)


async def enqueue_job(
    db: AsyncSession,
    job_type: str,
    source_key: str | None = None,
    payload: dict | None = None,
) -> Job:
    """Insert a new PENDING job into the job table."""
    job = Job(
        job_type=job_type,
        source_key=source_key,
        payload=payload or {},
        scheduled_at=datetime.now(timezone.utc),
    )
    db.add(job)
    await db.flush()
    logger.info("scheduler.enqueued", job_type=job_type, source_key=source_key, job_id=str(job.id))
    return job


async def _enqueue_insolvency_jobs() -> None:
    """Enqueue insolvency discover jobs for all states × current time window."""
    from app.adapters.insolvency import InsolvencyAdapter
    windows = InsolvencyAdapter.build_discover_windows(
        lookback_hours=1,
        window_minutes=settings.insolvency_poll_minutes,
    )
    async with AsyncSessionLocal() as db:
        for window in windows:
            await enqueue_job(
                db,
                job_type="DISCOVER",
                source_key="insolvency_portal",
                payload={
                    "state": window.state,
                    "date_from": str(window.date_from),
                    "date_to": str(window.date_to),
                },
            )
        await db.commit()
    logger.info("scheduler.insolvency_jobs_enqueued", count=len(windows))


async def _enqueue_bank_portal_jobs() -> None:
    """Enqueue daily discover jobs for Sparkasse and LBS."""
    async with AsyncSessionLocal() as db:
        for source_key in ["sparkasse_immobilien", "lbs_immobilien"]:
            await enqueue_job(
                db,
                job_type="DISCOVER",
                source_key=source_key,
                payload={"page": 1},
            )
        await db.commit()
    logger.info("scheduler.bank_portal_jobs_enqueued")


async def _enqueue_retention_purge() -> None:
    """Enqueue daily retention purge job."""
    async with AsyncSessionLocal() as db:
        await enqueue_job(db, job_type="PURGE", source_key="insolvency_portal")
        await db.commit()
    logger.info("scheduler.retention_purge_enqueued")


def build_scheduler() -> AsyncIOScheduler:
    """Build and configure the APScheduler instance."""
    scheduler = AsyncIOScheduler(timezone="Europe/Berlin")

    scheduler.add_job(
        _enqueue_insolvency_jobs,
        trigger="interval",
        minutes=settings.insolvency_poll_minutes,
        id="insolvency_discover",
        replace_existing=True,
        max_instances=1,
    )

    scheduler.add_job(
        _enqueue_bank_portal_jobs,
        trigger="interval",
        hours=settings.bank_portal_poll_hours,
        id="bank_portals_discover",
        replace_existing=True,
        max_instances=1,
    )

    scheduler.add_job(
        _enqueue_retention_purge,
        trigger="cron",
        hour=2,
        minute=0,
        id="retention_purge",
        replace_existing=True,
        max_instances=1,
    )

    return scheduler
