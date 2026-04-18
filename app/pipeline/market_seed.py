"""
Seed plz_market_stats from Immowelt regular sale listings.

Scrapes N pages of standard Kauf (purchase) listings per property type,
extracts PLZ + price + m² from search cards, aggregates medians in memory,
then upserts into plz_market_stats. No AssetLead records are created.
"""

from __future__ import annotations

import asyncio
import re
import statistics
from collections import defaultdict
from dataclasses import dataclass

import httpx
import structlog

logger = structlog.get_logger(__name__)

BASE_URL = "https://www.immowelt.de"
PAGES_PER_TYPE = 20       # ~20–30 cards/page → up to 600 listings per type
RATE_LIMIT_SECS = 2.0
MIN_SAMPLES = 5           # min per PLZ+type to include in seed

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    "DNT": "1",
}

_TYPE_MAP = {
    "haeuser": "house",
    "wohnungen": "apartment",
    "grundstuecke": "land",
}


@dataclass
class _ListingData:
    plz: str
    property_type: str
    price_per_m2: float


def _parse_cards(html: str, property_type: str) -> list[_ListingData]:
    """Extract (PLZ, price/m²) from one Immowelt search results page."""
    results: list[_ListingData] = []
    card_boundary = 'data-testid="serp-core-classified-card-testid"'

    for segment in html.split(card_boundary)[1:]:
        plz_m = re.search(r'\b(\d{5})\b', segment)
        if not plz_m:
            continue
        plz = plz_m.group(1)

        title_m = re.search(r'title="([^"]+)"', segment)
        if not title_m:
            continue
        title = title_m.group(1)

        price_m = re.search(r'([\d.]+(?:,\d+)?)\s*€', title)
        if not price_m:
            continue
        try:
            price = float(price_m.group(1).replace(".", "").replace(",", "."))
        except ValueError:
            continue

        # m² from title first, then raw card HTML
        area_m = re.search(r'(\d+(?:[.,]\d+)?)\s*m²', title) or \
                 re.search(r'(\d+(?:[.,]\d+)?)\s*m[²2]', segment)
        if not area_m:
            continue
        try:
            area = float(area_m.group(1).replace(",", "."))
        except ValueError:
            continue

        if area < 20 or price < 10_000:
            continue
        ppm2 = price / area
        if not (200 <= ppm2 <= 15_000):
            continue

        results.append(_ListingData(plz=plz, property_type=property_type, price_per_m2=ppm2))

    return results


async def scrape_market_listings(pages_per_type: int = PAGES_PER_TYPE) -> list[_ListingData]:
    """Scrape regular Kauf listings from Immowelt across all property types."""
    all_listings: list[_ListingData] = []

    async with httpx.AsyncClient(headers=_HEADERS, timeout=30.0, follow_redirects=True) as client:
        for slug, ptype in _TYPE_MAP.items():
            for page in range(1, pages_per_type + 1):
                url = f"{BASE_URL}/liste/deutschland/{slug}/kaufen?cp={page}"
                try:
                    resp = await client.get(url)
                    if resp.status_code in (403, 429, 451):
                        logger.warning("market_seed.blocked",
                                       slug=slug, page=page, status=resp.status_code)
                        break
                    resp.raise_for_status()
                    found = _parse_cards(resp.text, ptype)
                    all_listings.extend(found)
                    logger.info("market_seed.page", slug=slug, page=page, found=len(found))
                    if page < pages_per_type:
                        await asyncio.sleep(RATE_LIMIT_SECS)
                except httpx.HTTPError as exc:
                    logger.error("market_seed.error", slug=slug, page=page, error=str(exc))
                    break

    return all_listings


def aggregate_to_stats(listings: list[_ListingData]) -> list[dict]:
    """Compute median/p25/p75 per (PLZ, property_type) from scraped listings."""
    grouped: dict[tuple[str, str], list[float]] = defaultdict(list)
    for item in listings:
        grouped[(item.plz, item.property_type)].append(item.price_per_m2)

    rows = []
    for (plz, ptype), prices in grouped.items():
        if len(prices) < MIN_SAMPLES:
            continue
        prices.sort()
        n = len(prices)
        rows.append({
            "plz": plz,
            "property_type": ptype,
            "median": statistics.median(prices),
            "p25": prices[max(0, int(n * 0.25))],
            "p75": prices[min(n - 1, int(n * 0.75))],
            "sample_size": n,
        })
    return rows
