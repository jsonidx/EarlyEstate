"""Unit tests for the matching + scoring pipeline."""

from __future__ import annotations

import uuid
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from app.pipeline.matcher import (
    HIGH_THRESHOLD,
    LOW_THRESHOLD,
    MEDIUM_THRESHOLD,
    ScoreBreakdown,
    _score_bucket,
    build_dedup_key,
    score_match,
)


def make_party(canonical_name: str = "test gmbh", register_id: str | None = None):
    p = MagicMock()
    p.id = uuid.uuid4()
    p.canonical_name = canonical_name
    p.party_type = "COMPANY"
    p.register_id = register_id
    return p


def make_lead(
    city: str = "münchen",
    title: str = "",
    auction_terms: list | None = None,
    postal_code: str | None = None,
):
    lead = MagicMock()
    lead.id = uuid.uuid4()
    lead.city = city
    lead.title = title
    lead.asking_price_eur = None
    lead.verkehrswert_eur = None
    lead.auction_signal_terms = auction_terms or []
    lead.postal_code = postal_code
    lead.payload = {}
    lead.geom = None
    return lead


def make_address(city: str = "münchen", postal_code: str | None = None, geom=None):
    addr = MagicMock()
    addr.city = city
    addr.postal_code = postal_code
    addr.geom = geom
    return addr


# ── Score bucket tests ────────────────────────────────────────────────────────

def test_score_bucket_high():
    assert _score_bucket(80.0) == "HIGH"
    assert _score_bucket(100.0) == "HIGH"


def test_score_bucket_medium():
    assert _score_bucket(50.0) == "MEDIUM"
    assert _score_bucket(79.0) == "MEDIUM"


def test_score_bucket_low():
    assert _score_bucket(20.0) == "LOW"
    assert _score_bucket(49.0) == "LOW"


def test_score_bucket_below():
    assert _score_bucket(0.0) == "BELOW_THRESHOLD"
    assert _score_bucket(19.9) == "BELOW_THRESHOLD"


# ── Scoring tests ─────────────────────────────────────────────────────────────

def test_score_no_match_returns_low():
    party = make_party("xyz company abc")
    lead = make_lead(city="berlin", title="villa", auction_terms=[])
    breakdown = score_match(party, lead, None)
    assert breakdown.total < HIGH_THRESHOLD


def test_auction_terms_boost_score():
    party = make_party("testfirma gmbh")
    lead_no_cues = make_lead(city="münchen", auction_terms=[])
    lead_with_cues = make_lead(
        city="münchen",
        auction_terms=["zwangsversteigerung", "verkehrswert", "amtsgericht", "zv termin"],
    )
    b_no = score_match(party, lead_no_cues, None)
    b_with = score_match(party, lead_with_cues, None)
    assert b_with.auction_signal_score > b_no.auction_signal_score
    assert b_with.total > b_no.total


def test_max_auction_signal_score():
    party = make_party("test")
    # 4+ cues = max 20 pts
    lead = make_lead(
        city="x",
        auction_terms=["a", "b", "c", "d", "e"],  # 5 cues × 5 = 25 → capped at 20
    )
    b = score_match(party, lead, None)
    assert b.auction_signal_score == 20.0


def test_postal_code_match_boosts_geo():
    party = make_party("muster gmbh")
    addr = make_address(city="münchen", postal_code="80331")
    lead = make_lead(city="münchen", postal_code="80331", auction_terms=["verkehrswert"])
    lead.postal_code = "80331"

    b = score_match(party, lead, addr)
    assert b.geo_score > 0.0


def test_register_id_exact_match():
    party = make_party("firma gmbh", register_id="HRB 12345")
    lead = make_lead(city="berlin")
    lead.payload = {"register_id": "HRB 12345"}

    b = score_match(party, lead, None)
    assert b.register_id_match == 10.0


def test_register_id_no_match():
    party = make_party("firma gmbh", register_id="HRB 12345")
    lead = make_lead(city="berlin")
    lead.payload = {"register_id": "HRB 99999"}

    b = score_match(party, lead, None)
    assert b.register_id_match == 0.0


def test_source_trust_weight_scales_total():
    party = make_party("muster immobilien")
    lead = make_lead(city="frankfurt", auction_terms=["zwangsversteigerung"])
    b_full = score_match(party, lead, None, source_trust=1.0)
    b_half = score_match(party, lead, None, source_trust=0.5)
    assert b_half.total == pytest.approx(b_full.total * 0.5, rel=0.01)


# ── Dedup key tests ───────────────────────────────────────────────────────────

def test_dedup_key_stable():
    party_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
    lead_id = uuid.UUID("87654321-4321-8765-4321-876543218765")
    dt = datetime(2026, 4, 15, 14, 30, 0)

    k1 = build_dedup_key(party_id, lead_id, "HIGH", dt)
    k2 = build_dedup_key(party_id, lead_id, "HIGH", dt)
    assert k1 == k2


def test_dedup_key_different_day():
    party_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
    lead_id = uuid.UUID("87654321-4321-8765-4321-876543218765")
    dt1 = datetime(2026, 4, 15, 14, 30, 0)
    dt2 = datetime(2026, 4, 16, 14, 30, 0)

    k1 = build_dedup_key(party_id, lead_id, "HIGH", dt1)
    k2 = build_dedup_key(party_id, lead_id, "HIGH", dt2)
    assert k1 != k2


def test_dedup_key_different_bucket():
    party_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
    lead_id = uuid.UUID("87654321-4321-8765-4321-876543218765")
    dt = datetime(2026, 4, 15, 14, 30, 0)

    k_high = build_dedup_key(party_id, lead_id, "HIGH", dt)
    k_medium = build_dedup_key(party_id, lead_id, "MEDIUM", dt)
    assert k_high != k_medium
