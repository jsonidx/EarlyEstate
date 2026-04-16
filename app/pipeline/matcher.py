"""
Matching + scoring pipeline step.

Builds MatchCandidate records linking Party (insolvency debtor) to AssetLead
(bank/auction listing).

Scoring rubric (0–100):
  - name_similarity:         0–40  (RapidFuzz token_sort_ratio)
  - geo_distance:            0–30  (inverse distance score)
  - auction_signal_terms:    0–20  (number of cue terms found)
  - register_id_match:       0–10  (exact register match if available)

Tiers:
  High   ≥ 80  — strong linkage
  Medium 50–79 — partial, compelling
  Low    20–49 — weak, needs manual review
"""

from __future__ import annotations

import hashlib
import math
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import structlog
from rapidfuzz import fuzz
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.asset_lead import AssetLead
from app.models.event import Event
from app.models.match_candidate import MatchCandidate
from app.models.party import Party, PartyAddress
from app.pipeline.entity_resolution import normalize_name

logger = structlog.get_logger(__name__)

# Score thresholds
HIGH_THRESHOLD = 80.0
MEDIUM_THRESHOLD = 50.0
LOW_THRESHOLD = 20.0

# Max distance for geo scoring (metres)
MAX_GEO_DISTANCE_M = 50_000  # 50 km — beyond this, geo score = 0


@dataclass
class ScoreBreakdown:
    name_similarity: float      # 0–40
    geo_distance_m: float | None
    geo_score: float            # 0–30
    auction_signal_score: float  # 0–20
    register_id_match: float    # 0–10
    court_jurisdiction_score: float  # 0–15 (bonus)
    source_trust_weight: float  # multiplier 0.5–1.0
    total: float
    bucket: str                 # HIGH | MEDIUM | LOW | BELOW_THRESHOLD


def score_match(
    party: Party,
    asset_lead: AssetLead,
    party_address: PartyAddress | None,
    insolvency_court: str | None = None,
    source_trust: float = 1.0,
) -> ScoreBreakdown:
    """
    Compute match score between a party and an asset lead.
    Returns a ScoreBreakdown with individual feature weights.
    """
    # 1. Name similarity (0–40)
    lead_city = (asset_lead.city or "").lower()
    party_norm = normalize_name(party.canonical_name)
    lead_city_norm = normalize_name(lead_city)
    name_sim_raw = fuzz.token_sort_ratio(party_norm, lead_city_norm)
    name_score = (name_sim_raw / 100.0) * 40.0

    # For asset leads, compare party name against listing title too
    lead_title_norm = normalize_name(asset_lead.title or "")
    title_sim = fuzz.partial_ratio(party_norm, lead_title_norm)
    name_score = max(name_score, (title_sim / 100.0) * 30.0)
    name_score = min(name_score, 40.0)

    # 2. Geo distance score (0–30)
    # Try PostGIS distance first; fall through to PLZ/city if unavailable.
    geo_distance_m: float | None = None
    geo_score = 0.0
    if party_address and party_address.geom is not None and asset_lead.geom is not None:
        try:
            geo_distance_m = _haversine_distance(party_address, asset_lead)
            if geo_distance_m <= MAX_GEO_DISTANCE_M:
                # Inverse linear: 0m → 30 pts, 50km → 0 pts
                geo_score = 30.0 * (1.0 - geo_distance_m / MAX_GEO_DISTANCE_M)
        except Exception:
            pass  # fall through to PLZ/city fallback below

    # PLZ / city fallback — always applies when PostGIS score is still 0
    if geo_score == 0.0 and party_address:
        # PLZ exact match (strongest fallback)
        if party_address.postal_code and asset_lead.postal_code:
            if party_address.postal_code == asset_lead.postal_code:
                geo_score = 20.0
        # City name match (party_address.city is always set; postal_code may be None)
        if geo_score == 0.0 and party_address.city and asset_lead.city:
            if party_address.city.lower() == (asset_lead.city or "").lower():
                geo_score = 15.0

    # 3. Auction signal terms (0–20)
    cues = asset_lead.auction_signal_terms or []
    auction_score = min(len(cues) * 5.0, 20.0)

    # 4. Register ID match (0–10)
    register_score = 0.0
    if party.register_id and asset_lead.payload.get("register_id"):
        if party.register_id == asset_lead.payload["register_id"]:
            register_score = 10.0

    # 5. Court jurisdiction cross-match (0–15 bonus)
    # Insolvency court city matching lead city → strong geo signal even without address.
    # German court names: "Amtsgericht München", "Insolvenzgericht Berlin", etc.
    # We extract the last word (the city) and compare to asset_lead.city.
    court_score = 0.0
    if insolvency_court and asset_lead.city:
        court_city = _extract_court_city(insolvency_court)
        if court_city and court_city.lower() == (asset_lead.city or "").lower():
            court_score = 15.0
        elif court_city and asset_lead.city:
            # Fuzzy fallback — "München" vs "Munich" or compound city names
            sim = fuzz.token_sort_ratio(court_city.lower(), asset_lead.city.lower())
            if sim >= 85:
                court_score = 10.0

    # Apply source trust weight (0.5–1.0)
    raw_total = name_score + geo_score + auction_score + register_score + court_score
    total = raw_total * source_trust

    bucket = _score_bucket(total)

    return ScoreBreakdown(
        name_similarity=round(name_score, 2),
        geo_distance_m=geo_distance_m,
        geo_score=round(geo_score, 2),
        auction_signal_score=round(auction_score, 2),
        register_id_match=round(register_score, 2),
        court_jurisdiction_score=round(court_score, 2),
        source_trust_weight=source_trust,
        total=round(total, 2),
        bucket=bucket,
    )


def _score_bucket(total: float) -> str:
    if total >= HIGH_THRESHOLD:
        return "HIGH"
    if total >= MEDIUM_THRESHOLD:
        return "MEDIUM"
    if total >= LOW_THRESHOLD:
        return "LOW"
    return "BELOW_THRESHOLD"


def _extract_court_city(court_name: str) -> str | None:
    """
    Extract the city portion from a German court name.

    Examples:
      "Amtsgericht München"     → "München"
      "Insolvenzgericht Berlin" → "Berlin"
      "AG Hamburg"              → "Hamburg"
      "München"                 → "München"  (already just city)
    """
    if not court_name:
        return None
    parts = court_name.strip().split()
    if len(parts) == 0:
        return None
    # Skip known prefix words: Amtsgericht, Insolvenzgericht, AG, IG
    skip_prefixes = {"amtsgericht", "insolvenzgericht", "ag", "ig", "landgericht", "lg"}
    for part in parts:
        if part.lower() not in skip_prefixes:
            return part
    return parts[-1]


def _haversine_distance(party_addr: PartyAddress, lead: AssetLead) -> float:
    """
    Approximate distance in metres between party address and asset lead.
    Requires geom to be a WKT POINT or geoalchemy2 WKBElement.
    Falls back to city-level if geometry is unavailable.
    """
    # This is a placeholder — real implementation reads lat/lon from geom.
    # With geoalchemy2 + PostGIS, use ST_Distance or ST_DWithin in SQL instead.
    raise NotImplementedError("Use PostGIS ST_Distance for production; implement in query layer.")


async def run_matching_for_party(
    db: AsyncSession,
    party_id: uuid.UUID,
    preloaded_leads: list[AssetLead] | None = None,
) -> list[MatchCandidate]:
    """
    Run matching for a single party against all asset leads.
    Persists MatchCandidate records for scores ≥ LOW_THRESHOLD.

    Pass preloaded_leads to avoid a per-party SELECT when matching many parties
    in a loop — load the leads once externally and pass them in.

    Uses 4 queries per party:
    1. Fetch party
    2. Fetch party address
    3. Fetch all existing candidates for this party (bulk)
    4. Fetch most recent insolvency event (for court city)
    Then does in-memory upsert — no per-candidate SELECT.
    """
    party = await db.get(Party, party_id)
    if not party:
        logger.warning("matcher.party_not_found", party_id=str(party_id))
        return []

    # Load party address
    stmt = select(PartyAddress).where(PartyAddress.party_id == party_id).limit(1)
    result = await db.execute(stmt)
    party_address = result.scalar_one_or_none()

    # Load all asset leads — use preloaded set when available (bulk-run optimisation)
    if preloaded_leads is not None:
        leads = preloaded_leads
    else:
        stmt = select(AssetLead)
        result = await db.execute(stmt)
        leads = result.scalars().all()

    # Preload all existing candidates for this party (one query, not N)
    stmt = select(MatchCandidate).where(MatchCandidate.party_id == party_id)
    result = await db.execute(stmt)
    existing_by_lead: dict[uuid.UUID, MatchCandidate] = {
        mc.asset_lead_id: mc for mc in result.scalars().all()
    }

    # Load most recent insolvency event court name for this party (one query)
    insolvency_court: str | None = None
    stmt = (
        select(Event)
        .where(Event.party_id == party_id, Event.event_type == "INSOLVENCY_PUBLICATION")
        .order_by(Event.event_time.desc())
        .limit(1)
    )
    result = await db.execute(stmt)
    event = result.scalar_one_or_none()
    if event:
        insolvency_court = event.payload.get("court")

    candidates = []
    now = datetime.now(timezone.utc)
    for lead in leads:
        breakdown = score_match(party, lead, party_address, insolvency_court=insolvency_court)
        if breakdown.total < LOW_THRESHOLD:
            continue

        existing = existing_by_lead.get(lead.id)
        if existing:
            existing.score_total = breakdown.total
            existing.score_breakdown = _breakdown_to_dict(breakdown)
            existing.updated_at = now
            candidates.append(existing)
        else:
            mc = MatchCandidate(
                party_id=party_id,
                asset_lead_id=lead.id,
                score_total=breakdown.total,
                score_breakdown=_breakdown_to_dict(breakdown),
            )
            db.add(mc)
            candidates.append(mc)

    await db.flush()
    logger.info(
        "matcher.done",
        party_id=str(party_id),
        candidates=len(candidates),
    )
    return candidates


def build_dedup_key(
    party_id: uuid.UUID,
    asset_lead_id: uuid.UUID,
    score_bucket: str,
    event_time: datetime,
) -> str:
    """
    Stable dedup key for alerts.
    sha256(party_id|asset_lead_id|scoring_bucket|floor(event_time to 24h))
    """
    day_floor = event_time.strftime("%Y-%m-%d")
    raw = f"{party_id}|{asset_lead_id}|{score_bucket}|{day_floor}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _breakdown_to_dict(b: ScoreBreakdown) -> dict[str, Any]:
    return {
        "name_similarity": b.name_similarity,
        "geo_distance_m": b.geo_distance_m,
        "geo_score": b.geo_score,
        "auction_signal_score": b.auction_signal_score,
        "register_id_match": b.register_id_match,
        "court_jurisdiction_score": b.court_jurisdiction_score,
        "source_trust_weight": b.source_trust_weight,
        "total": b.total,
        "bucket": b.bucket,
    }
