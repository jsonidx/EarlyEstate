"""Asset leads endpoints — bank portal listings with auction cues."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.asset_lead import AssetLead

router = APIRouter()


class AssetLeadOut(BaseModel):
    id: str
    source_id: str
    listing_id: str | None
    title: str | None
    address_raw: str | None
    postal_code: str | None
    city: str | None
    object_type: str | None
    asking_price_eur: float | None
    verkehrswert_eur: float | None
    auction_date: datetime | None
    court: str | None
    details_url: str | None
    auction_signal_terms: list | None
    created_at: datetime


@router.get("/", response_model=list[AssetLeadOut])
async def list_leads(
    city: str | None = None,
    has_auction: bool | None = None,
    limit: int = Query(50, le=500),
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
):
    stmt = select(AssetLead).order_by(AssetLead.created_at.desc()).limit(limit).offset(offset)

    if city:
        stmt = stmt.where(AssetLead.city.ilike(f"%{city}%"))
    if has_auction is True:
        stmt = stmt.where(AssetLead.auction_signal_terms.isnot(None))

    result = await db.execute(stmt)
    leads = result.scalars().all()

    return [
        AssetLeadOut(
            id=str(lead.id),
            source_id=str(lead.source_id),
            listing_id=lead.listing_id,
            title=lead.title,
            address_raw=lead.address_raw,
            postal_code=lead.postal_code,
            city=lead.city,
            object_type=lead.object_type,
            asking_price_eur=float(lead.asking_price_eur) if lead.asking_price_eur else None,
            verkehrswert_eur=float(lead.verkehrswert_eur) if lead.verkehrswert_eur else None,
            auction_date=lead.auction_date,
            court=lead.court,
            details_url=lead.details_url,
            auction_signal_terms=lead.auction_signal_terms,
            created_at=lead.created_at,
        )
        for lead in leads
    ]
