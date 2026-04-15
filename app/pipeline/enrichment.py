"""
Enrichment pipeline step.

Providers:
- North Data API: company/person data, relationships, publications.
- BORIS/BRW: land value proxies (Bodenrichtwert) via state OGC APIs.
- Sprengnetter AVM: optional commercial valuation API.

All enrichment is optional and controlled by feature flags / API key availability.
Results are stored with full provenance (provider, timestamp, source link).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx
import structlog

from app.config import settings

logger = structlog.get_logger(__name__)

NORTH_DATA_BASE = "https://www.northdata.de/_api"


# ── North Data ────────────────────────────────────────────────────────────────

@dataclass
class NorthDataCompany:
    name: str
    register_id: str | None
    register_court: str | None
    address: str | None
    status: str | None  # active / dissolved / insolvent
    publications: list[dict[str, Any]]


async def enrich_from_north_data(company_name: str) -> NorthDataCompany | None:
    """
    Look up a company in North Data for enrichment context.
    Requires NORTH_DATA_API_KEY.
    """
    if not settings.north_data_api_key:
        return None

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resp = await client.get(
                f"{NORTH_DATA_BASE}/company/v1/company",
                params={"name": company_name, "status": "any"},
                headers={"X-Api-Key": settings.north_data_api_key},
            )
            resp.raise_for_status()
            data = resp.json()

            company = data.get("company") or (data.get("companies") or [{}])[0]
            if not company:
                return None

            register = company.get("register", {}) or {}
            address_parts = company.get("address", {}) or {}
            address_str = ", ".join(
                filter(None, [
                    address_parts.get("street"),
                    address_parts.get("city"),
                    address_parts.get("postalCode"),
                ])
            )

            return NorthDataCompany(
                name=company.get("name", company_name),
                register_id=register.get("id"),
                register_court=register.get("courtName"),
                address=address_str or None,
                status=company.get("status"),
                publications=company.get("publications", []),
            )

        except httpx.HTTPError as exc:
            logger.error("enrichment.north_data.error", name=company_name, error=str(exc))
            return None


# ── BORIS / BRW ───────────────────────────────────────────────────────────────

@dataclass
class BORISValuation:
    provider: str
    state: str
    value_eur_per_m2: float
    reference_year: int
    zone_id: str | None
    license: str


async def fetch_boris_brw(lat: float, lon: float, state: str = "BB") -> BORISValuation | None:
    """
    Fetch Bodenrichtwert (land value) from a state BORIS/BRW OGC API.

    Currently implements Brandenburg (BB) which provides a public OGC API
    under "Datenlizenz Deutschland – Namensnennung – Version 2.0".

    Other states have different endpoints — extend via adapter registry as needed.
    """
    fetchers = {
        "BB": _fetch_boris_brandenburg,
    }
    fetcher = fetchers.get(state.upper())
    if not fetcher:
        logger.info("enrichment.boris.no_adapter", state=state)
        return None

    return await fetcher(lat, lon)


async def _fetch_boris_brandenburg(lat: float, lon: float) -> BORISValuation | None:
    """Brandenburg BORIS OGC API Features endpoint."""
    url = "https://ogc-api.geobasis-bb.de/ogc/v1/collections/boris_brw/items"
    params = {
        "f": "json",
        "limit": 1,
        "bbox": f"{lon-0.001},{lat-0.001},{lon+0.001},{lat+0.001}",
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            features = data.get("features", [])
            if not features:
                return None

            props = features[0].get("properties", {})
            brw = props.get("brw") or props.get("BRW")
            if not brw:
                return None

            return BORISValuation(
                provider="BORIS_BB",
                state="BB",
                value_eur_per_m2=float(brw),
                reference_year=int(props.get("stichtag", datetime.now().year)[:4]),
                zone_id=props.get("brwzoneid") or props.get("id"),
                license="Datenlizenz Deutschland – Namensnennung – Version 2.0",
            )
        except (httpx.HTTPError, ValueError, KeyError) as exc:
            logger.error("enrichment.boris_bb.error", error=str(exc))
            return None


# ── Sprengnetter AVM ──────────────────────────────────────────────────────────

@dataclass
class SprengnetterValuation:
    value_point_eur: float
    value_low_eur: float
    value_high_eur: float
    object_type: str
    source_url: str | None


async def fetch_sprengnetter_avm(
    address: str, object_type: str, area_m2: float | None = None
) -> SprengnetterValuation | None:
    """
    Fetch AVM valuation from Sprengnetter API.
    Requires SPRENGNETTER_API_KEY and a valid commercial contract.
    """
    if not settings.sprengnetter_api_key:
        return None

    # Sprengnetter API details are contract-dependent.
    # Placeholder — implement once API credentials and endpoint are confirmed.
    logger.info("enrichment.sprengnetter.not_configured")
    return None
