"""Unit tests for entity resolution normalization."""

from __future__ import annotations

import pytest

from app.pipeline.entity_resolution import extract_legal_form, normalize_name


@pytest.mark.parametrize("raw,expected_contains", [
    ("Mustermann GmbH", "mustermann"),
    ("Test AG & Co. KG", "test"),
    ("XYZ GmbH & Co. KG", "xyz"),
    ("Müller Immobilien GmbH", "mueller immobilien"),
    ("Schulz & Partner OHG", "schulz partner"),
])
def test_normalize_name_strips_legal_forms(raw, expected_contains):
    result = normalize_name(raw)
    assert expected_contains in result


def test_normalize_name_handles_umlauts():
    result = normalize_name("Müller Öl Übergabe")
    assert "mueller" in result
    assert "oel" in result
    assert "uebergabe" in result


def test_normalize_name_collapses_whitespace():
    result = normalize_name("  Test   GmbH  ")
    assert "  " not in result
    assert result == result.strip()


def test_normalize_name_lowercase():
    result = normalize_name("MUSTERFIRMA AG")
    assert result == result.lower()


@pytest.mark.parametrize("raw,expected_form", [
    ("Test GmbH", "GmbH"),
    ("ABC AG", "AG"),
    ("XYZ GmbH & Co. KG", "GmbH & Co. KG"),
    ("Test KG", "KG"),
    ("No Legal Form", None),
])
def test_extract_legal_form(raw, expected_form):
    result = extract_legal_form(raw)
    if expected_form is None:
        assert result is None
    else:
        assert result is not None
        assert expected_form.lower() in result.lower()
