"""Unit tests for the insolvency adapter (no network required)."""

from __future__ import annotations

import pytest

from app.adapters.insolvency import InsolvencyAdapter, InsolvencyDetail


@pytest.fixture
def adapter():
    return InsolvencyAdapter(headless=True)


def test_normalize_case_number(adapter):
    assert adapter._normalize_case_number("1 IN 123/24") == "1in123/24"
    assert adapter._normalize_case_number(" 12 ABC / 2024 ") == "12abc/2024"


def test_normalize_name(adapter):
    assert adapter._normalize_name("  Mustermann  GmbH  ") == "mustermann gmbh"


def test_parse_german_date(adapter):
    dt = adapter._parse_german_date("15.04.2026")
    assert dt is not None
    assert dt.year == 2026
    assert dt.month == 4
    assert dt.day == 15


def test_parse_german_date_with_time(adapter):
    dt = adapter._parse_german_date("15.04.2026 09:30")
    assert dt is not None
    assert dt.hour == 9
    assert dt.minute == 30


def test_parse_german_date_invalid():
    result = InsolvencyAdapter._parse_german_date("not a date")
    assert result is None


def test_infer_state_from_court():
    assert InsolvencyAdapter._infer_state_from_court("Amtsgericht München") == "Bayern"
    assert InsolvencyAdapter._infer_state_from_court("Amtsgericht Berlin-Mitte") == "Berlin"
    assert InsolvencyAdapter._infer_state_from_court("Amtsgericht Köln") == "Nordrhein-Westfalen"
    assert InsolvencyAdapter._infer_state_from_court("Amtsgericht Unbekannt") is None


def test_fingerprint_stable(adapter):
    detail = InsolvencyDetail(
        case_number="1 IN 123/24",
        debtor_name="Test GmbH",
        court="Amtsgericht München",
        publication_subject="Eröffnung des Insolvenzverfahrens",
        seat_city="München",
        publication_date="15.04.2026",
        source_url="https://neu.insolvenzbekanntmachungen.de",
        raw_html_snippet="<tr>...</tr>",
    )
    fp1 = adapter.fingerprint(detail)
    fp2 = adapter.fingerprint(detail)
    assert fp1 == fp2
    assert len(fp1) == 64  # SHA-256 hex


def test_fingerprint_changes_on_content(adapter):
    detail1 = InsolvencyDetail(
        case_number="1 IN 123/24",
        debtor_name="Test GmbH",
        court="Amtsgericht München",
        publication_subject="Eröffnung",
        seat_city="München",
        publication_date="15.04.2026",
        source_url="https://neu.insolvenzbekanntmachungen.de",
        raw_html_snippet="",
    )
    detail2 = InsolvencyDetail(
        case_number="1 IN 123/24",
        debtor_name="Test GmbH",
        court="Amtsgericht München",
        publication_subject="Aufhebung",  # Different subject
        seat_city="München",
        publication_date="15.04.2026",
        source_url="https://neu.insolvenzbekanntmachungen.de",
        raw_html_snippet="",
    )
    assert adapter.fingerprint(detail1) != adapter.fingerprint(detail2)


@pytest.mark.asyncio
async def test_parse_returns_correct_fields(adapter):
    detail = InsolvencyDetail(
        case_number="12 IN 456/2025",
        debtor_name="Mustermann Immobilien GmbH",
        court="Amtsgericht Frankfurt am Main",
        publication_subject="Eröffnung des Insolvenzverfahrens",
        seat_city="Frankfurt",
        publication_date="01.03.2026",
        source_url="https://neu.insolvenzbekanntmachungen.de",
        raw_html_snippet="",
    )
    parsed = await adapter.parse(detail)

    assert parsed.debtor_name == "Mustermann Immobilien GmbH"
    assert parsed.court == "Amtsgericht Frankfurt am Main"
    assert parsed.publication_date is not None
    assert parsed.publication_date.year == 2026
    assert parsed.external_id  # Should be set


def test_compliance_meta(adapter):
    meta = adapter.compliance_meta()
    assert meta.robots_respected is True
    assert meta.store_raw_payload == "metadata_only"
    assert meta.personal_data_level == "medium"
    assert meta.rate_limit_rps == 0.5
    assert len(meta.notes) > 0


def test_build_discover_windows():
    windows = InsolvencyAdapter.build_discover_windows(
        states=["Bayern", "Berlin"],
        lookback_hours=1,
        window_minutes=30,
    )
    # 2 states × 2 windows (1 hour / 30 min) = 4
    assert len(windows) == 4
    assert all(w.state in ("Bayern", "Berlin") for w in windows)
