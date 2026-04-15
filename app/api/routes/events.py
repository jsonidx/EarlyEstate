"""Events endpoints — insolvency publications and signals."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.event import Event

router = APIRouter()


class EventOut(BaseModel):
    id: str
    source_id: str
    event_type: str
    event_time: datetime | None
    external_id: str | None
    party_id: str | None
    asset_lead_id: str | None
    payload: dict
    confidence: float


@router.get("/", response_model=list[EventOut])
async def list_events(
    event_type: str | None = None,
    limit: int = Query(50, le=500),
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Event).order_by(Event.event_time.desc()).limit(limit).offset(offset)
    if event_type:
        stmt = stmt.where(Event.event_type == event_type)

    result = await db.execute(stmt)
    events = result.scalars().all()

    return [
        EventOut(
            id=str(e.id),
            source_id=str(e.source_id),
            event_type=e.event_type,
            event_time=e.event_time,
            external_id=e.external_id,
            party_id=str(e.party_id) if e.party_id else None,
            asset_lead_id=str(e.asset_lead_id) if e.asset_lead_id else None,
            payload=e.payload,
            confidence=float(e.confidence),
        )
        for e in events
    ]
