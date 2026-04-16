"""FastAPI application entry point."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import admin, events, leads, matches, sources

logger = structlog.get_logger(__name__)

_scheduler = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler
    # Only start the APScheduler in-process when explicitly enabled.
    # In production (Railway + GitHub Actions) the scheduler is not needed —
    # GitHub Actions crons handle all pipeline runs.
    if os.getenv("ENABLE_SCHEDULER", "").lower() in ("1", "true", "yes"):
        from app.pipeline.scheduler import build_scheduler
        _scheduler = build_scheduler()
        _scheduler.start()
        logger.info("api.startup", scheduler="started")
    else:
        logger.info("api.startup", scheduler="disabled")
    yield
    if _scheduler:
        _scheduler.shutdown(wait=False)
    logger.info("api.shutdown")


app = FastAPI(
    title="EarlyEstate",
    description="Early-stage real estate distress screener for Germany",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(sources.router, prefix="/sources", tags=["sources"])
app.include_router(events.router, prefix="/events", tags=["events"])
app.include_router(leads.router, prefix="/leads", tags=["leads"])
app.include_router(matches.router, prefix="/matches", tags=["matches"])
app.include_router(admin.router, prefix="/admin", tags=["admin"])


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
