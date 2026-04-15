"""FastAPI application entry point."""

from __future__ import annotations

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import admin, events, leads, matches, sources
from app.pipeline.scheduler import build_scheduler

logger = structlog.get_logger(__name__)

app = FastAPI(
    title="EarlyEstate",
    description="Early-stage real estate distress screener for Germany",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
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

_scheduler = None


@app.on_event("startup")
async def startup() -> None:
    global _scheduler
    _scheduler = build_scheduler()
    _scheduler.start()
    logger.info("api.startup", scheduler="started")


@app.on_event("shutdown")
async def shutdown() -> None:
    if _scheduler:
        _scheduler.shutdown(wait=False)
    logger.info("api.shutdown")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
