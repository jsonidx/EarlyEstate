"""
Sparkasse / S-Immobilien portal adapter.

Strategy: HTTP fetch of listing pages → keyword classification for auction cues.

Auction indicator terms (from PRD):
  "Zwangsversteigerung", "ZV Termin", "Amtsgericht", "Verkehrswert",
  "Zuschlagswert", "Mitbieten"

We only ingest listings where at least one auction cue term is detected.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from app.adapters.base import ComplianceMeta, DiscoverItem, ScrapeAdapter

logger = structlog.get_logger(__name__)

# Auction cue terms to detect in listing pages
AUCTION_CUES = [
    "zwangsversteigerung",
    "zv termin",
    "zv-termin",
    "amtsgericht",
    "verkehrswert",
    "zuschlagswert",
    "mitbieten",
    "versteigerungstermin",
    "bieterverfahren",
]

# Sparkasse Immobilien search endpoint (national aggregator)
BASE_URL = "https://www.sparkasse-immobilien.de"
SEARCH_URL = f"{BASE_URL}/immobilien/suche"


@dataclass
class SparkasseDiscoverParams:
    region: str | None = None       # Optional PLZ or city filter
    page: int = 1
    page_size: int = 50


@dataclass
class SparkasseDetail:
    listing_id: str
    title: str
    address_raw: str
    postal_code: str | None
    city: str | None
    object_type: str | None
    asking_price_eur: float | None
    verkehrswert_eur: float | None
    auction_date: datetime | None
    court: str | None
    details_url: str
    auction_signal_terms: list[str]
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class SparkasseParsed:
    listing_id: str
    title: str
    address_raw: str
    postal_code: str | None
    city: str | None
    object_type: str | None
    asking_price_eur: float | None
    verkehrswert_eur: float | None
    auction_date: datetime | None
    court: str | None
    details_url: str
    auction_signal_terms: list[str]
    is_auction_listing: bool


class SparkasseAdapter(ScrapeAdapter[SparkasseDiscoverParams, SparkasseDetail, SparkasseParsed]):
    """
    HTTP-based adapter for Sparkasse Immobilien listings.

    Allowlist strategy: only process pages from known Sparkasse domains.
    Change detection: fingerprint the extracted structured fields.
    """

    source_key = "sparkasse_immobilien"

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; EarlyEstate/0.1; research use)",
                    "Accept-Language": "de-DE,de;q=0.9",
                },
                timeout=30.0,
                follow_redirects=True,
            )
        return self._client

    # ── Discover ──────────────────────────────────────────────────────────────

    async def discover(self, params: SparkasseDiscoverParams) -> list[DiscoverItem]:
        items: list[DiscoverItem] = []
        client = await self._get_client()

        search_params: dict[str, Any] = {
            "seite": params.page,
            "treffer": params.page_size,
        }
        if params.region:
            search_params["ort"] = params.region

        try:
            resp = await client.get(SEARCH_URL, params=search_params)
            resp.raise_for_status()
            items = self._extract_listing_links(resp.text)
            logger.info("sparkasse.discover", count=len(items), page=params.page)
        except httpx.HTTPError as exc:
            logger.error("sparkasse.discover.error", error=str(exc))

        return items

    def _extract_listing_links(self, html: str) -> list[DiscoverItem]:
        """Extract listing URLs and IDs from search results page."""
        items = []
        # Match listing detail links (pattern: /immobilien/<id> or similar)
        pattern = re.compile(
            r'href="(/immobilien/(?:expose|detail|listing)/([^"?]+))"',
            re.IGNORECASE,
        )
        seen: set[str] = set()
        for match in pattern.finditer(html):
            path, listing_id = match.groups()
            if listing_id in seen:
                continue
            seen.add(listing_id)
            items.append(
                DiscoverItem(
                    external_id=listing_id,
                    url=f"{BASE_URL}{path}",
                    hint={"source": "sparkasse"},
                )
            )
        return items

    # ── Fetch detail ──────────────────────────────────────────────────────────

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    async def fetch_detail(
        self, external_id: str, url: str, hint: dict[str, Any] | None = None
    ) -> SparkasseDetail:
        client = await self._get_client()
        resp = await client.get(url)
        resp.raise_for_status()
        return self._parse_html(external_id, url, resp.text)

    def _parse_html(self, listing_id: str, url: str, html: str) -> SparkasseDetail:
        """Extract structured fields from a Sparkasse listing page."""
        html_lower = html.lower()

        # Detect auction cue terms
        found_cues = [term for term in AUCTION_CUES if term in html_lower]

        title = self._extract_meta(html, "og:title") or self._extract_h1(html) or ""
        address_raw = self._extract_address(html) or ""
        postal_code = self._extract_postal_code(address_raw)
        city = self._extract_city(address_raw, html)
        asking_price = self._extract_price(html, ["kaufpreis", "preis"])
        verkehrswert = self._extract_price(html, ["verkehrswert"])
        auction_date = self._extract_auction_date(html)
        court = self._extract_court(html)
        object_type = self._extract_object_type(html)

        return SparkasseDetail(
            listing_id=listing_id,
            title=title,
            address_raw=address_raw,
            postal_code=postal_code,
            city=city,
            object_type=object_type,
            asking_price_eur=asking_price,
            verkehrswert_eur=verkehrswert,
            auction_date=auction_date,
            court=court,
            details_url=url,
            auction_signal_terms=found_cues,
        )

    # ── Parse ─────────────────────────────────────────────────────────────────

    async def parse(self, detail: SparkasseDetail) -> SparkasseParsed:
        return SparkasseParsed(
            listing_id=detail.listing_id,
            title=detail.title,
            address_raw=detail.address_raw,
            postal_code=detail.postal_code,
            city=detail.city,
            object_type=detail.object_type,
            asking_price_eur=detail.asking_price_eur,
            verkehrswert_eur=detail.verkehrswert_eur,
            auction_date=detail.auction_date,
            court=detail.court,
            details_url=detail.details_url,
            auction_signal_terms=detail.auction_signal_terms,
            is_auction_listing=len(detail.auction_signal_terms) > 0,
        )

    # ── Fingerprint ───────────────────────────────────────────────────────────

    def fingerprint(self, detail: SparkasseDetail, parsed: SparkasseParsed | None = None) -> str:
        key = "|".join([
            detail.listing_id,
            detail.address_raw,
            str(detail.asking_price_eur),
            str(detail.verkehrswert_eur),
            str(detail.auction_date),
        ])
        return self.sha256_hex(key)

    # ── Compliance ────────────────────────────────────────────────────────────

    def compliance_meta(self) -> ComplianceMeta:
        return ComplianceMeta(
            robots_respected=True,
            tos_reviewed=False,
            store_raw_payload="metadata_only",
            personal_data_level="low",
            rate_limit_rps=0.2,  # 1 request per 5 seconds — be conservative
            notes=[
                "Only ingest listings with at least one auction cue term.",
                "Do not store full HTML; extract and store structured fields only.",
                "Allowlist strategy: only crawl known Sparkasse domains.",
            ],
        )

    # ── Field extractors ──────────────────────────────────────────────────────

    @staticmethod
    def _extract_meta(html: str, prop: str) -> str | None:
        m = re.search(rf'<meta[^>]+property="{re.escape(prop)}"[^>]+content="([^"]+)"', html, re.I)
        return m.group(1).strip() if m else None

    @staticmethod
    def _extract_h1(html: str) -> str | None:
        m = re.search(r"<h1[^>]*>([^<]+)</h1>", html, re.I)
        return m.group(1).strip() if m else None

    @staticmethod
    def _extract_address(html: str) -> str | None:
        patterns = [
            r'class="[^"]*(?:address|adresse|location|ort)[^"]*"[^>]*>([^<]{5,100})<',
            r'<span[^>]*itemprop="streetAddress"[^>]*>([^<]+)</span>',
        ]
        for pat in patterns:
            m = re.search(pat, html, re.I)
            if m:
                return m.group(1).strip()
        return None

    @staticmethod
    def _extract_postal_code(address: str | None) -> str | None:
        if not address:
            return None
        m = re.search(r"\b(\d{5})\b", address)
        return m.group(1) if m else None

    @staticmethod
    def _extract_city(address: str | None, html: str) -> str | None:
        if address:
            m = re.search(r"\d{5}\s+([A-ZÄÖÜa-zäöüß][^,\n]{2,40})", address)
            if m:
                return m.group(1).strip()
        return None

    @staticmethod
    def _extract_price(html: str, keywords: list[str]) -> float | None:
        for kw in keywords:
            pattern = rf"{kw}[^€\d]*(\d[\d.,]+)\s*€"
            m = re.search(pattern, html, re.IGNORECASE)
            if m:
                raw = m.group(1).replace(".", "").replace(",", ".")
                try:
                    return float(raw)
                except ValueError:
                    continue
        return None

    @staticmethod
    def _extract_auction_date(html: str) -> datetime | None:
        patterns = [
            r"(?:zv.?termin|versteigerungstermin)[^\d]*(\d{2}\.\d{2}\.\d{4})\s*(?:um\s*)?(\d{2}:\d{2})?",
        ]
        for pat in patterns:
            m = re.search(pat, html, re.IGNORECASE)
            if m:
                date_str = m.group(1)
                time_str = m.group(2) or "00:00"
                try:
                    return datetime.strptime(f"{date_str} {time_str}", "%d.%m.%Y %H:%M")
                except ValueError:
                    pass
        return None

    @staticmethod
    def _extract_court(html: str) -> str | None:
        m = re.search(r"amtsgericht\s+([A-ZÄÖÜa-zäöüß][a-zäöüßA-ZÄÖÜ\s-]{2,40})", html, re.I)
        return m.group(0).strip() if m else None

    @staticmethod
    def _extract_object_type(html: str) -> str | None:
        types = {
            "einfamilienhaus": "house",
            "doppelhaus": "house",
            "reihenhaus": "house",
            "eigentumswohnung": "condo",
            "wohnung": "condo",
            "grundstück": "land",
            "gewerbeobjekt": "commercial",
            "gewerbe": "commercial",
        }
        html_lower = html.lower()
        for key, val in types.items():
            if key in html_lower:
                return val
        return "other"
