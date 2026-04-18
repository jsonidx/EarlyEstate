"""
Matching + scoring pipeline step.

Builds MatchCandidate records linking Party (insolvency debtor) to AssetLead
(bank/auction listing).

Scoring rubric (0–100):
  - address_score:          0–40  (street fuzzy match within same PLZ)
  - geo_score:              0–15  (PLZ exact=15, city=10, distance fallback=5)
  - court_jurisdiction:     0–20  (insolvency court city vs property city)
  - timing_score:           0–10  (insolvency date → auction date window)
  - register_id_match:      0–10  (exact Handelsregister match)
  - name_similarity:        0– 5  (fallback only — titles rarely name the debtor)

Tiers:
  High   ≥ 75  — strong linkage (was 80 — recalibrated for new weight model)
  Medium 50–74 — partial, worth reviewing
  Low    20–49 — weak, digest only
"""

from __future__ import annotations

import hashlib
import re
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
HIGH_THRESHOLD = 75.0
MEDIUM_THRESHOLD = 50.0
LOW_THRESHOLD = 20.0

# Classic German foreclosure window after insolvency opening: 6–18 months
TIMING_WINDOW_MIN_DAYS = 180
TIMING_WINDOW_MAX_DAYS = 540


@dataclass
class ScoreBreakdown:
    address_score: float        # 0–40
    geo_score: float            # 0–15
    court_jurisdiction_score: float  # 0–20
    timing_score: float         # 0–10
    register_id_match: float    # 0–10
    name_similarity: float      # 0–5
    geo_distance_m: float | None
    source_trust_weight: float
    total: float
    bucket: str                 # HIGH | MEDIUM | LOW | BELOW_THRESHOLD


def score_match(
    party: Party,
    asset_lead: AssetLead,
    party_address: PartyAddress | None,
    insolvency_court: str | None = None,
    insolvency_date: datetime | None = None,
    source_trust: float = 1.0,
) -> ScoreBreakdown:
    """Compute match score between a party (insolvency debtor) and an asset lead."""

    # 1. Address match (0–40) — primary signal
    address_score, geo_distance_m = _score_address(party_address, asset_lead)

    # 2. Geo score (0–15) — discrete fallback when no street data
    geo_score = _score_geo(party_address, asset_lead, geo_distance_m)

    # 3. Court jurisdiction (0–20)
    court_score = _score_court(insolvency_court, asset_lead)

    # 4. Timing (0–10)
    timing_score = _score_timing(insolvency_date, asset_lead.auction_date)

    # 5. Register ID exact match (0–10)
    register_score = 0.0
    if party.register_id and asset_lead.payload.get("register_id"):
        if party.register_id == asset_lead.payload["register_id"]:
            register_score = 10.0

    # 6. Name similarity (0–5) — last resort fallback
    name_score = _score_name(party, asset_lead)

    raw_total = (
        address_score + geo_score + court_score +
        timing_score + register_score + name_score
    )
    total = raw_total * source_trust
    bucket = _score_bucket(total)

    return ScoreBreakdown(
        address_score=round(address_score, 2),
        geo_score=round(geo_score, 2),
        court_jurisdiction_score=round(court_score, 2),
        timing_score=round(timing_score, 2),
        register_id_match=round(register_score, 2),
        name_similarity=round(name_score, 2),
        geo_distance_m=geo_distance_m,
        source_trust_weight=source_trust,
        total=round(total, 2),
        bucket=bucket,
    )


# ── Individual scorers ────────────────────────────────────────────────────────

def _score_address(
    party_address: PartyAddress | None,
    lead: AssetLead,
) -> tuple[float, float | None]:
    """
    Street-level address match within the same PLZ.
    Returns (score 0–40, geo_distance_m or None).

    Requires both sides to have a street name and the same PLZ.
    Falls back gracefully to 0 when either side lacks street data.
    """
    geo_distance_m = None

    if not party_address:
        return 0.0, None

    # Need PLZ match as a precondition for street comparison
    party_plz = party_address.postal_code
    lead_plz = lead.postal_code
    plz_match = bool(party_plz and lead_plz and party_plz == lead_plz)

    # Extract lead street from address_raw if not separately stored
    lead_street = _extract_street(lead.address_raw or "")
    party_street = _normalize_street(party_address.street or "")

    if not party_street or not lead_street:
        return 0.0, None

    street_sim = fuzz.token_sort_ratio(party_street, lead_street)

    if plz_match and street_sim >= 85:
        return 40.0, 0.0   # near-certain — same PLZ, same street
    if plz_match and street_sim >= 70:
        return 28.0, None
    if street_sim >= 90:
        # Identical street name, different PLZ — could be same city, different district
        return 20.0, None

    return 0.0, None


def _score_geo(
    party_address: PartyAddress | None,
    lead: AssetLead,
    geo_distance_m: float | None,
) -> float:
    """
    Discrete geo score (0–15). Used as complement to address score,
    or as primary geo signal when street data is absent.
    """
    if not party_address:
        return 0.0

    # PLZ exact match
    if party_address.postal_code and lead.postal_code:
        if party_address.postal_code == lead.postal_code:
            return 15.0

    # City name match
    if party_address.city and lead.city:
        if party_address.city.lower() == (lead.city or "").lower():
            return 10.0
        # Fuzzy city — handles umlauts, compound names
        if fuzz.token_sort_ratio(party_address.city.lower(), lead.city.lower()) >= 88:
            return 8.0

    # Distance fallback — only when PostGIS geometry is available
    if geo_distance_m is not None:
        if geo_distance_m <= 5_000:
            return 5.0
        if geo_distance_m <= 15_000:
            return 3.0

    return 0.0


def _score_court(insolvency_court: str | None, lead: AssetLead) -> float:
    """Court jurisdiction cross-match (0–20)."""
    if not insolvency_court or not lead.city:
        return 0.0
    court_city = _extract_court_city(insolvency_court)
    if not court_city:
        return 0.0
    if court_city.lower() == (lead.city or "").lower():
        return 20.0
    if fuzz.token_sort_ratio(court_city.lower(), lead.city.lower()) >= 85:
        return 14.0
    # Also check against lead court name if present
    if lead.court:
        lead_court_city = _extract_court_city(lead.court)
        if lead_court_city and court_city.lower() == lead_court_city.lower():
            return 20.0
    return 0.0


def _score_timing(
    insolvency_date: datetime | None,
    auction_date: datetime | None,
) -> float:
    """
    Timing signal (0–10).
    Classic foreclosure window after insolvency opening: 6–18 months.
    """
    if not insolvency_date or not auction_date:
        return 0.0
    days = (auction_date - insolvency_date).days
    if TIMING_WINDOW_MIN_DAYS <= days <= TIMING_WINDOW_MAX_DAYS:
        return 10.0
    if 90 <= days <= 720:
        return 5.0
    return 0.0


def _score_name(party: Party, lead: AssetLead) -> float:
    """Name similarity fallback (0–5). Fires rarely — titles almost never name the debtor."""
    party_norm = normalize_name(party.canonical_name)
    title_norm = normalize_name(lead.title or "")
    sim = fuzz.partial_ratio(party_norm, title_norm)
    return min((sim / 100.0) * 5.0, 5.0)


# ── Helpers ───────────────────────────────────────────────────────────────────

_STREET_SUFFIXES = re.compile(
    r'\b(stra(ss|ß)e|str\.?|weg|gasse|allee|platz|ring|damm|chaussee|ufer|pfad)\b',
    re.IGNORECASE,
)
_HOUSE_NUMBER = re.compile(r'\s+\d+[a-zA-Z]?\s*$')


def _normalize_street(s: str) -> str:
    """Normalise a German street name for fuzzy comparison."""
    s = s.lower().strip()
    s = _STREET_SUFFIXES.sub("str", s)
    s = _HOUSE_NUMBER.sub("", s)
    s = re.sub(r'[^\w\s]', '', s)
    return s.strip()


def _extract_street(address_raw: str) -> str:
    """
    Extract and normalise the street portion from a raw address string.
    Strips PLZ, city, and house numbers.
    """
    if not address_raw:
        return ""
    # Remove PLZ (5 digits) and everything after
    cleaned = re.sub(r'\b\d{5}\b.*', '', address_raw)
    return _normalize_street(cleaned)


def _extract_court_city(court_name: str) -> str | None:
    """Extract city from German court name: 'Amtsgericht München' → 'München'."""
    if not court_name:
        return None
    skip = {"amtsgericht", "insolvenzgericht", "ag", "ig", "landgericht", "lg"}
    for part in court_name.strip().split():
        if part.lower() not in skip:
            return part
    return court_name.strip().split()[-1] if court_name.strip() else None


def _score_bucket(total: float) -> str:
    if total >= HIGH_THRESHOLD:
        return "HIGH"
    if total >= MEDIUM_THRESHOLD:
        return "MEDIUM"
    if total >= LOW_THRESHOLD:
        return "LOW"
    return "BELOW_THRESHOLD"


# ── Main matching runner ──────────────────────────────────────────────────────

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
    """
    party = await db.get(Party, party_id)
    if not party:
        logger.warning("matcher.party_not_found", party_id=str(party_id))
        return []

    stmt = select(PartyAddress).where(PartyAddress.party_id == party_id).limit(1)
    result = await db.execute(stmt)
    party_address = result.scalar_one_or_none()

    if preloaded_leads is not None:
        leads = preloaded_leads
    else:
        stmt = select(AssetLead)
        result = await db.execute(stmt)
        leads = result.scalars().all()

    # Preload existing candidates for this party (one query)
    stmt = select(MatchCandidate).where(MatchCandidate.party_id == party_id)
    result = await db.execute(stmt)
    existing_by_lead: dict[uuid.UUID, MatchCandidate] = {
        mc.asset_lead_id: mc for mc in result.scalars().all()
    }

    # Load most recent insolvency event for court + timing signals
    insolvency_court: str | None = None
    insolvency_date: datetime | None = None
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
        insolvency_date = event.event_time

    candidates = []
    now = datetime.now(timezone.utc)

    for lead in leads:
        breakdown = score_match(
            party, lead, party_address,
            insolvency_court=insolvency_court,
            insolvency_date=insolvency_date,
        )
        if breakdown.total < LOW_THRESHOLD:
            continue

        existing = existing_by_lead.get(lead.id)
        if existing:
            existing.score_total = breakdown.total
            existing.score_breakdown = breakdown_to_dict(breakdown)
            existing.updated_at = now
            candidates.append(existing)
        else:
            mc = MatchCandidate(
                party_id=party_id,
                asset_lead_id=lead.id,
                score_total=breakdown.total,
                score_breakdown=breakdown_to_dict(breakdown),
            )
            db.add(mc)
            candidates.append(mc)

    await db.flush()
    logger.info("matcher.done", party_id=str(party_id), candidates=len(candidates))
    return candidates


async def reverse_match_lead(
    db: AsyncSession,
    lead: AssetLead,
) -> list[MatchCandidate]:
    """
    Phase 3 — Reverse lookup: given a newly ingested asset lead, find insolvent
    parties that may own this property by querying North Data for companies
    registered at the lead's address, then cross-checking against the party table.

    Returns any new MatchCandidate records created.
    """
    if not lead.address_raw:
        return []

    from app.pipeline.enrichment import enrich_from_north_data_by_address
    from app.models.party import Party

    # Extract street + PLZ for the North Data query
    plz = lead.postal_code or ""
    city = lead.city or ""
    lead_street = _extract_street(lead.address_raw)
    if not lead_street:
        return []

    company_names = await enrich_from_north_data_by_address(
        street=lead_street, plz=plz, city=city
    )
    if not company_names:
        return []

    # Fuzzy-match returned company names against our party table
    stmt = select(Party).where(Party.party_type == "COMPANY")
    result = await db.execute(stmt)
    all_parties = result.scalars().all()

    matched_party_ids: list[uuid.UUID] = []
    for company_name in company_names:
        name_norm = normalize_name(company_name)
        for party in all_parties:
            sim = fuzz.token_sort_ratio(name_norm, normalize_name(party.canonical_name))
            if sim >= 85:
                matched_party_ids.append(party.id)
                logger.info(
                    "matcher.reverse_lookup.hit",
                    company_name=company_name,
                    party_id=str(party.id),
                    similarity=sim,
                )
                break

    if not matched_party_ids:
        return []

    # Run full match scoring for each matched party
    candidates = []
    for party_id in matched_party_ids:
        new_candidates = await run_matching_for_party(
            db, party_id, preloaded_leads=[lead]
        )
        candidates.extend(new_candidates)

    return candidates


# ── Serialisation ─────────────────────────────────────────────────────────────

def build_dedup_key(
    party_id: uuid.UUID,
    asset_lead_id: uuid.UUID,
    score_bucket: str,
    event_time: datetime,
) -> str:
    day_floor = event_time.strftime("%Y-%m-%d")
    raw = f"{party_id}|{asset_lead_id}|{score_bucket}|{day_floor}"
    return hashlib.sha256(raw.encode()).hexdigest()


def breakdown_to_dict(b: ScoreBreakdown) -> dict[str, Any]:
    return {
        "address_score": b.address_score,
        "geo_score": b.geo_score,
        "court_jurisdiction_score": b.court_jurisdiction_score,
        "timing_score": b.timing_score,
        "register_id_match": b.register_id_match,
        "name_similarity": b.name_similarity,
        "geo_distance_m": b.geo_distance_m,
        "source_trust_weight": b.source_trust_weight,
        "total": b.total,
        "bucket": b.bucket,
    }
