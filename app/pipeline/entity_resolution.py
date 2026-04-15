"""
Entity Resolution (ER) — dedup and unify Party records.

Strategy:
1. Normalize the incoming name (strip legal forms, whitespace, punctuation).
2. Fuzzy-match against existing canonical_name values using RapidFuzz.
3. If score ≥ threshold → merge into existing party (add alias).
4. Otherwise → create new party.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

import structlog
from rapidfuzz import fuzz, process
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from unidecode import unidecode

from app.models.party import Party, PartyAlias

logger = structlog.get_logger(__name__)

# Legal form suffixes to strip during normalization
LEGAL_FORMS = [
    r"\bGmbH\s*&?\s*Co\.?\s*KG\b",
    r"\bGmbH\s*&?\s*Co\b",
    r"\bAG\s*&?\s*Co\.?\s*KG\b",
    r"\bGmbH\b",
    r"\bAG\b",
    r"\bKG\b",
    r"\bOHG\b",
    r"\bUG\b",
    r"\bPartGmbB\b",
    r"\be\.?\s*V\.?\b",
    r"\be\.?\s*G\.?\b",
    r"\bmbH\b",
    r"\bSE\b",
]

MATCH_THRESHOLD = 88  # RapidFuzz score threshold for same-entity decision


@dataclass
class ERInput:
    name_raw: str
    party_type: str  # COMPANY | PERSON | UNKNOWN
    register_id: str | None = None
    register_court: str | None = None
    source_id: str | None = None  # UUID string


@dataclass
class ERResult:
    party_id: str  # UUID string
    canonical_name: str
    is_new: bool
    match_score: float | None


class PartyCache:
    """
    In-memory party cache for bulk ingestion runs.

    Loads all parties once from the DB, then serves ER lookups in memory.
    New parties created during the run are added to the cache immediately,
    so subsequent items benefit from them without additional DB round-trips.

    Usage:
        cache = await PartyCache.load(db)
        # ... then pass to resolve_party for each item
    """

    def __init__(self) -> None:
        # party_type → list of (canonical_name, Party)
        self._by_type: dict[str, list[tuple[str, "Party"]]] = {}

    @classmethod
    async def load(cls, db: AsyncSession) -> "PartyCache":
        cache = cls()
        stmt = select(Party)
        result = await db.execute(stmt)
        for party in result.scalars().all():
            cache._add(party)
        return cache

    def _add(self, party: "Party") -> None:
        bucket = self._by_type.setdefault(party.party_type, [])
        bucket.append((party.canonical_name, party))

    def get_candidates(self, party_type: str) -> list[tuple[str, "Party"]]:
        return self._by_type.get(party_type, [])

    def add_new(self, party: "Party") -> None:
        """Register a newly created party so future items in this run can match it."""
        self._add(party)


def normalize_name(raw: str) -> str:
    """
    Normalize a company/person name for matching:
    - Transliterate to ASCII (handles umlauts etc.)
    - Strip legal form suffixes
    - Remove punctuation, collapse whitespace, lowercase
    """
    # Transliterate umlauts: ä→ae, ö→oe, ü→ue, ß→ss (German-aware)
    name = raw
    name = name.replace("ä", "ae").replace("Ä", "ae")
    name = name.replace("ö", "oe").replace("Ö", "oe")
    name = name.replace("ü", "ue").replace("Ü", "ue")
    name = name.replace("ß", "ss")

    # Strip legal forms
    for pattern in LEGAL_FORMS:
        name = re.sub(pattern, "", name, flags=re.IGNORECASE)

    # Remove punctuation except hyphens inside words
    name = re.sub(r"[^\w\s-]", " ", name)
    name = re.sub(r"\s+", " ", name).strip().lower()
    return name


def extract_legal_form(raw: str) -> str | None:
    """Extract the legal form suffix from a company name."""
    for pattern in LEGAL_FORMS:
        m = re.search(pattern, raw, re.IGNORECASE)
        if m:
            return m.group(0).strip()
    return None


async def resolve_party(
    db: AsyncSession,
    er_input: ERInput,
    cache: PartyCache | None = None,
) -> ERResult:
    """
    Find or create a Party for the given input.

    If `cache` is provided, party lookups are done in-memory (no DB round-trip).
    The caller must call cache.add_new(party) after creation if using a cache.
    """
    name_norm = normalize_name(er_input.name_raw)

    # 1. Exact register_id match (highest confidence)
    if er_input.register_id:
        stmt = select(Party).where(Party.register_id == er_input.register_id)
        result = await db.execute(stmt)
        existing = result.scalar_one_or_none()
        if existing:
            await _add_alias_if_new(db, existing, er_input)
            logger.info("er.register_match", party_id=str(existing.id), name=er_input.name_raw)
            return ERResult(
                party_id=str(existing.id),
                canonical_name=existing.canonical_name,
                is_new=False,
                match_score=100.0,
            )

    # 2. Fuzzy match — use in-memory cache if available, else query DB
    if cache is not None:
        candidates = cache.get_candidates(er_input.party_type)
        existing_parties = [p for _, p in candidates]
        canonical_names = [name for name, _ in candidates]
    else:
        stmt = select(Party).where(Party.party_type == er_input.party_type)
        result = await db.execute(stmt)
        existing_parties = result.scalars().all()
        canonical_names = [p.canonical_name for p in existing_parties]

    if canonical_names:
        match = process.extractOne(
            name_norm,
            canonical_names,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=MATCH_THRESHOLD,
        )

        if match:
            matched_name, score, idx = match
            party = existing_parties[idx]
            await _add_alias_if_new(db, party, er_input)
            logger.info(
                "er.fuzzy_match",
                party_id=str(party.id),
                score=score,
                input=er_input.name_raw,
                matched=matched_name,
            )
            return ERResult(
                party_id=str(party.id),
                canonical_name=party.canonical_name,
                is_new=False,
                match_score=float(score),
            )

    # 3. Create new party
    party = Party(
        party_type=er_input.party_type,
        canonical_name=name_norm,
        name_raw=er_input.name_raw,
        legal_form=extract_legal_form(er_input.name_raw),
        register_id=er_input.register_id,
        register_court=er_input.register_court,
    )
    db.add(party)
    await db.flush()  # Get ID without committing

    # Register in cache immediately so subsequent items in same run can match it
    if cache is not None:
        cache.add_new(party)

    logger.info("er.new_party", party_id=str(party.id), name=er_input.name_raw)
    return ERResult(
        party_id=str(party.id),
        canonical_name=party.canonical_name,
        is_new=True,
        match_score=None,
    )


async def _add_alias_if_new(db: AsyncSession, party: Party, er_input: ERInput) -> None:
    """Add a new alias if this raw name variant hasn't been seen before."""
    alias_norm = normalize_name(er_input.name_raw)
    stmt = select(PartyAlias).where(
        PartyAlias.party_id == party.id,
        PartyAlias.alias_norm == alias_norm,
    )
    result = await db.execute(stmt)
    if result.scalar_one_or_none() is None:
        alias = PartyAlias(
            party_id=party.id,
            alias=er_input.name_raw,
            alias_norm=alias_norm,
        )
        db.add(alias)
