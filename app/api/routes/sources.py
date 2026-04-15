"""Source management endpoints."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.source import Source

router = APIRouter()


class SourceOut(BaseModel):
    id: str
    source_key: str
    source_type: str
    base_url: str | None
    enabled: bool
    robots_policy: str

    model_config = {"from_attributes": True}


class SourceUpdate(BaseModel):
    enabled: bool | None = None
    robots_policy: str | None = None


@router.get("/", response_model=list[SourceOut])
async def list_sources(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Source).order_by(Source.source_key))
    return [
        SourceOut(
            id=str(s.id),
            source_key=s.source_key,
            source_type=s.source_type,
            base_url=s.base_url,
            enabled=s.enabled,
            robots_policy=s.robots_policy,
        )
        for s in result.scalars().all()
    ]


@router.patch("/{source_key}", response_model=SourceOut)
async def update_source(
    source_key: str, body: SourceUpdate, db: AsyncSession = Depends(get_db)
):
    result = await db.execute(select(Source).where(Source.source_key == source_key))
    source = result.scalar_one_or_none()
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    if body.enabled is not None:
        source.enabled = body.enabled
    if body.robots_policy is not None:
        source.robots_policy = body.robots_policy

    return SourceOut(
        id=str(source.id),
        source_key=source.source_key,
        source_type=source.source_type,
        base_url=source.base_url,
        enabled=source.enabled,
        robots_policy=source.robots_policy,
    )
