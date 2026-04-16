"""Match candidates endpoints — scored party ↔ lead links."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.asset_lead import AssetLead
from app.models.event import Event
from app.models.match_candidate import MatchCandidate
from app.models.party import Party

router = APIRouter()


class MatchOut(BaseModel):
    id: str
    party_id: str
    asset_lead_id: str
    score_total: float
    score_breakdown: dict
    status: str
    created_at: datetime


class MatchDetail(BaseModel):
    """Full enriched match — all context needed for manual review."""
    id: str
    score_total: float
    score_breakdown: dict[str, Any]
    status: str
    created_at: datetime
    party: dict[str, Any]
    insolvency_event: dict[str, Any]
    asset_lead: dict[str, Any]
    next_actions: list[str]


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


@router.get("/{match_id}", response_model=MatchDetail)
async def get_match_detail(match_id: str, db: AsyncSession = Depends(get_db)):
    """
    Full enriched match detail — party info, insolvency event, asset lead, score breakdown.
    Designed for the manual review UI.
    """
    import uuid as _uuid
    try:
        mc = await db.get(MatchCandidate, _uuid.UUID(match_id))
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid match_id UUID")
    if not mc:
        raise HTTPException(status_code=404, detail="Match not found")

    party = await db.get(Party, mc.party_id)
    lead = await db.get(AssetLead, mc.asset_lead_id)
    if not party or not lead:
        raise HTTPException(status_code=404, detail="Party or lead no longer exists")

    # Most recent insolvency event for this party
    stmt = (
        select(Event)
        .where(Event.party_id == mc.party_id, Event.event_type == "INSOLVENCY_PUBLICATION")
        .order_by(Event.event_time.desc())
        .limit(1)
    )
    result = await db.execute(stmt)
    event = result.scalar_one_or_none()

    return MatchDetail(
        id=str(mc.id),
        score_total=float(mc.score_total),
        score_breakdown=mc.score_breakdown,
        status=mc.status,
        created_at=mc.created_at,
        party={
            "id": str(party.id),
            "canonical_name": party.canonical_name,
            "name_raw": party.name_raw,
            "party_type": party.party_type,
            "legal_form": party.legal_form,
            "register_id": party.register_id,
            "register_court": party.register_court,
        },
        insolvency_event={
            "event_id": str(event.id) if event else None,
            "event_time": event.event_time.isoformat() if event and event.event_time else None,
            "case_number": event.payload.get("case_number") if event else None,
            "court": event.payload.get("court") if event else None,
            "state": event.payload.get("state") if event else None,
            "publication_subject": event.payload.get("publication_subject") if event else None,
            "seat_city": event.payload.get("seat_city") if event else None,
        },
        asset_lead={
            "id": str(lead.id),
            "title": lead.title,
            "address_raw": lead.address_raw,
            "postal_code": lead.postal_code,
            "city": lead.city,
            "object_type": lead.object_type,
            "asking_price_eur": float(lead.asking_price_eur) if lead.asking_price_eur else None,
            "verkehrswert_eur": float(lead.verkehrswert_eur) if lead.verkehrswert_eur else None,
            "auction_date": lead.auction_date.isoformat() if lead.auction_date else None,
            "court": lead.court,
            "details_url": lead.details_url,
            "auction_signal_terms": lead.auction_signal_terms or [],
        },
        next_actions=[
            "Check insolvency court file at Insolvenzgericht",
            "Verify property ownership via Grundbuch (requires berechtigtes Interesse)",
            "Inspect property / contact agent if details_url is available",
            "Review ZVG term publication if auction_date is set",
        ],
    )


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

    previous_status = match.status
    match.status = body.status

    # Push to onOffice CRM when a match is confirmed for the first time
    if body.status == "CONFIRMED" and previous_status != "CONFIRMED":
        # Load the full payload from the most recent alert for this match
        from app.models.alert import Alert
        from app.alerts.onoffice import push_confirmed_match
        alert_stmt = (
            select(Alert)
            .where(Alert.match_candidate_id == match.id)
            .order_by(Alert.created_at.desc())
            .limit(1)
        )
        result = await db.execute(alert_stmt)
        alert = result.scalar_one_or_none()
        if alert and alert.payload:
            await push_confirmed_match(alert.payload)

    return MatchOut(
        id=str(match.id),
        party_id=str(match.party_id),
        asset_lead_id=str(match.asset_lead_id),
        score_total=float(match.score_total),
        score_breakdown=match.score_breakdown,
        status=match.status,
        created_at=match.created_at,
    )
