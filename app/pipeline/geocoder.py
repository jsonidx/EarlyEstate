"""
Geocoding pipeline step.

Provider abstraction: Google Maps API or Nominatim (OSM).

Policy rules (from PRD):
- Nominatim: bulk discouraged; strict caching required; prefer self-hosted.
- Google: caching restrictions; place_id may be stored indefinitely; honor refresh policy.
- All results are cached in party_address / asset_lead (geom column).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Literal

import httpx
import structlog

from app.config import settings

logger = structlog.get_logger(__name__)

# In-memory cache for geocoding results within a process lifetime
_GEOCODE_CACHE: dict[str, "GeocodeResult"] = {}


@dataclass
class GeocodeResult:
    lat: float
    lon: float
    confidence: float  # 0.0–1.0
    provider: Literal["GOOGLE", "NOMINATIM", "MANUAL"]
    display_name: str | None = None
    place_id: str | None = None  # Google place_id (may be stored indefinitely)


async def geocode_address(address: str, country: str = "DE") -> GeocodeResult | None:
    """
    Geocode an address using the configured provider.
    Results are cached in-process. DB-level caching is handled by the caller.
    """
    cache_key = f"{address.lower().strip()}|{country}"
    if cache_key in _GEOCODE_CACHE:
        return _GEOCODE_CACHE[cache_key]

    result: GeocodeResult | None = None

    if settings.geocoding_provider == "google" and settings.google_maps_api_key:
        result = await _geocode_google(address, country)
    else:
        result = await _geocode_nominatim(address, country)

    if result:
        _GEOCODE_CACHE[cache_key] = result

    return result


async def _geocode_nominatim(address: str, country: str = "DE") -> GeocodeResult | None:
    """
    Geocode via Nominatim (OSM).
    Rate limit: max 1 req/sec (Nominatim policy). Prefer self-hosted for bulk.
    """
    await asyncio.sleep(1.1)  # Respect 1 req/sec policy

    url = f"{settings.nominatim_base_url}/search"
    params = {
        "q": address,
        "countrycodes": country.lower(),
        "format": "json",
        "limit": 1,
        "addressdetails": 1,
    }

    async with httpx.AsyncClient(
        headers={"User-Agent": settings.nominatim_user_agent},
        timeout=10.0,
    ) as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            if not data:
                logger.warning("geocoder.nominatim.no_result", address=address)
                return None

            hit = data[0]
            importance = float(hit.get("importance", 0.5))

            return GeocodeResult(
                lat=float(hit["lat"]),
                lon=float(hit["lon"]),
                confidence=min(importance, 1.0),
                provider="NOMINATIM",
                display_name=hit.get("display_name"),
            )

        except (httpx.HTTPError, KeyError, ValueError) as exc:
            logger.error("geocoder.nominatim.error", address=address, error=str(exc))
            return None


async def _geocode_google(address: str, country: str = "DE") -> GeocodeResult | None:
    """
    Geocode via Google Maps Geocoding API.
    Policy: place_id may be stored indefinitely; other fields follow caching restrictions.
    """
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": f"{address}, {country}",
        "key": settings.google_maps_api_key,
        "region": country.lower(),
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") != "OK" or not data.get("results"):
                logger.warning(
                    "geocoder.google.no_result",
                    address=address,
                    status=data.get("status"),
                )
                return None

            result = data["results"][0]
            location = result["geometry"]["location"]

            # Confidence heuristic from location_type
            location_type = result["geometry"].get("location_type", "APPROXIMATE")
            confidence_map = {
                "ROOFTOP": 1.0,
                "RANGE_INTERPOLATED": 0.85,
                "GEOMETRIC_CENTER": 0.7,
                "APPROXIMATE": 0.5,
            }
            confidence = confidence_map.get(location_type, 0.5)

            return GeocodeResult(
                lat=float(location["lat"]),
                lon=float(location["lng"]),
                confidence=confidence,
                provider="GOOGLE",
                display_name=result.get("formatted_address"),
                place_id=result.get("place_id"),  # Store indefinitely per Google policy
            )

        except (httpx.HTTPError, KeyError, ValueError) as exc:
            logger.error("geocoder.google.error", address=address, error=str(exc))
            return None


def result_to_wkt(result: GeocodeResult) -> str:
    """Convert GeocodeResult to WKT POINT for PostGIS insertion."""
    return f"POINT({result.lon} {result.lat})"
