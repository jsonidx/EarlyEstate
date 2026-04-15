"""Match candidates endpoints — scored party ↔ lead links."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.match_candidate import MatchCandidate

router = APIRouter()


class MatchOut(BaseModel):
    id: str
    party_id: str
    asset_lead_id: str
    score_total: float
    score_breakdown: dict
    status: str
    created_at: datetime


class MatchStatusUpdate(BaseModel):
    status: str  # CONFIRMED | REJECTED


@router.get("/", response_model=list[MatchOut])
async def list_matches(
    status: str = "OPEN",
    min_score: float = 0.0,
    limit: int = Query(50, le=500),
    db: AsyncSession = Depends(get_db),
):
    stmt = (
        select(MatchCandidate)
        .where(
            MatchCandidate.status == status,
            MatchCandidate.score_total >= min_score,
        )
        .order_by(MatchCandidate.score_total.desc())
        .limit(limit)
    )
    result = await db.execute(stmt)
    matches = result.scalars().all()

    return [
        MatchOut(
            id=str(m.id),
            party_id=str(m.party_id),
            asset_lead_id=str(m.asset_lead_id),
            score_total=float(m.score_total),
            score_breakdown=m.score_breakdown,
            status=m.status,
            created_at=m.created_at,
        )
        for m in matches
    ]


@router.patch("/{match_id}", response_model=MatchOut)
async def update_match_status(
    match_id: str, body: MatchStatusUpdate, db: AsyncSession = Depends(get_db)
):
    import uuid as _uuid
    match = await db.get(MatchCandidate, _uuid.UUID(match_id))
    if not match:
        raise HTTPException(status_code=404, detail="Match not found")

    if body.status not in ("CONFIRMED", "REJECTED", "OPEN"):
        raise HTTPException(status_code=400, detail="Invalid status")

    match.status = body.status
    return MatchOut(
        id=str(match.id),
        party_id=str(match.party_id),
        asset_lead_id=str(match.asset_lead_id),
        score_total=float(match.score_total),
        score_breakdown=match.score_breakdown,
        status=match.status,
        created_at=match.created_at,
    )
