"""Admin endpoints — trigger jobs, view job queue, force runs."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.job import Job
from app.pipeline.scheduler import enqueue_job

router = APIRouter()


class JobOut(BaseModel):
    id: str
    job_type: str
    source_key: str | None
    status: str
    attempts: int
    scheduled_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    error_message: str | None


class TriggerJobRequest(BaseModel):
    job_type: str
    source_key: str | None = None
    payload: dict = {}


@router.get("/jobs", response_model=list[JobOut])
async def list_jobs(
    status: str | None = None,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Job).order_by(Job.created_at.desc()).limit(limit)
    if status:
        stmt = stmt.where(Job.status == status)

    result = await db.execute(stmt)
    jobs = result.scalars().all()

    return [
        JobOut(
            id=str(j.id),
            job_type=j.job_type,
            source_key=j.source_key,
            status=j.status,
            attempts=j.attempts,
            scheduled_at=j.scheduled_at,
            started_at=j.started_at,
            completed_at=j.completed_at,
            error_message=j.error_message,
        )
        for j in jobs
    ]


@router.post("/jobs/trigger", response_model=JobOut)
async def trigger_job(body: TriggerJobRequest, db: AsyncSession = Depends(get_db)):
    allowed_types = {"DISCOVER", "FETCH", "PARSE", "ENRICH", "MATCH", "ALERT", "PURGE"}
    if body.job_type not in allowed_types:
        raise HTTPException(status_code=400, detail=f"job_type must be one of {allowed_types}")

    job = await enqueue_job(db, body.job_type, body.source_key, body.payload)
    await db.commit()

    return JobOut(
        id=str(job.id),
        job_type=job.job_type,
        source_key=job.source_key,
        status=job.status,
        attempts=job.attempts,
        scheduled_at=job.scheduled_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        error_message=job.error_message,
    )


@router.post("/purge/run")
async def run_purge(db: AsyncSession = Depends(get_db)):
    """Manually trigger InsBekV retention purge."""
    from app.pipeline.auditor import run_retention_purge
    stats = await run_retention_purge(db)
    return {"status": "ok", "stats": stats}
