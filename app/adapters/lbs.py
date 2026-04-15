"""
LBS (Landesbausparkassen) portal adapter.

LBS exposés explicitly declare ZV Termin, Amtsgericht, Verkehrswert.
Structured fields make this a high-signal source when auction cues are present.
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
from app.adapters.sparkasse import AUCTION_CUES  # Reuse same cue list

logger = structlog.get_logger(__name__)

BASE_URL = "https://www.lbs.de"
SEARCH_URL = f"{BASE_URL}/immobilien/suche"


@dataclass
class LBSDiscoverParams:
    region: str | None = None
    page: int = 1


@dataclass
class LBSDetail:
    listing_id: str
    title: str
    address_raw: str
    postal_code: str | None
    city: str | None
    object_type: str | None
    verkehrswert_eur: float | None  # LBS shows Verkehrswert as the "price"
    auction_date: datetime | None
    auction_time: str | None
    court: str | None
    details_url: str
    auction_signal_terms: list[str]
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class LBSParsed:
    listing_id: str
    title: str
    address_raw: str
    postal_code: str | None
    city: str | None
    object_type: str | None
    verkehrswert_eur: float | None
    auction_date: datetime | None
    court: str | None
    details_url: str
    auction_signal_terms: list[str]
    is_auction_listing: bool


class LBSAdapter(ScrapeAdapter[LBSDiscoverParams, LBSDetail, LBSParsed]):
    """
    HTTP-based adapter for LBS Immobilien listings.
    LBS exposés are highly structured for ZV listings — Verkehrswert = displayed price.
    """

    source_key = "lbs_immobilien"

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

    async def discover(self, params: LBSDiscoverParams) -> list[DiscoverItem]:
        items: list[DiscoverItem] = []
        client = await self._get_client()
        search_params: dict[str, Any] = {"page": params.page}
        if params.region:
            search_params["ort"] = params.region

        try:
            resp = await client.get(SEARCH_URL, params=search_params)
            resp.raise_for_status()
            items = self._extract_listing_links(resp.text)
            logger.info("lbs.discover", count=len(items), page=params.page)
        except httpx.HTTPError as exc:
            logger.error("lbs.discover.error", error=str(exc))

        return items

    def _extract_listing_links(self, html: str) -> list[DiscoverItem]:
        items = []
        pattern = re.compile(
            r'href="(/immobilien/(?:expose|detail)/([^"?/]+))"', re.IGNORECASE
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
                    hint={"source": "lbs"},
                )
            )
        return items

    # ── Fetch detail ──────────────────────────────────────────────────────────

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    async def fetch_detail(
        self, external_id: str, url: str, hint: dict[str, Any] | None = None
    ) -> LBSDetail:
        client = await self._get_client()
        resp = await client.get(url)
        resp.raise_for_status()
        return self._parse_html(external_id, url, resp.text)

    def _parse_html(self, listing_id: str, url: str, html: str) -> LBSDetail:
        html_lower = html.lower()
        found_cues = [term for term in AUCTION_CUES if term in html_lower]

        title = self._extract_h1(html) or ""
        address_raw = self._extract_address(html) or ""
        postal_code = self._extract_postal_code(address_raw)
        city = self._extract_city(address_raw)
        verkehrswert = self._extract_verkehrswert(html)
        auction_date, auction_time = self._extract_auction_datetime(html)
        court = self._extract_court(html)
        object_type = self._extract_object_type(html)

        return LBSDetail(
            listing_id=listing_id,
            title=title,
            address_raw=address_raw,
            postal_code=postal_code,
            city=city,
            object_type=object_type,
            verkehrswert_eur=verkehrswert,
            auction_date=auction_date,
            auction_time=auction_time,
            court=court,
            details_url=url,
            auction_signal_terms=found_cues,
        )

    # ── Parse ─────────────────────────────────────────────────────────────────

    async def parse(self, detail: LBSDetail) -> LBSParsed:
        return LBSParsed(
            listing_id=detail.listing_id,
            title=detail.title,
            address_raw=detail.address_raw,
            postal_code=detail.postal_code,
            city=detail.city,
            object_type=detail.object_type,
            verkehrswert_eur=detail.verkehrswert_eur,
            auction_date=detail.auction_date,
            court=detail.court,
            details_url=detail.details_url,
            auction_signal_terms=detail.auction_signal_terms,
            is_auction_listing=len(detail.auction_signal_terms) > 0,
        )

    # ── Fingerprint ───────────────────────────────────────────────────────────

    def fingerprint(self, detail: LBSDetail, parsed: LBSParsed | None = None) -> str:
        key = "|".join([
            detail.listing_id,
            detail.address_raw,
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
            rate_limit_rps=0.2,
            notes=[
                "Ingest only listings with ZV/auction cue terms.",
                "Verkehrswert is displayed as 'price' in LBS ZV listings — store correctly.",
                "Daily crawl cadence; change detection via fingerprint.",
            ],
        )

    # ── Field extractors ──────────────────────────────────────────────────────

    @staticmethod
    def _extract_h1(html: str) -> str | None:
        m = re.search(r"<h1[^>]*>([^<]+)</h1>", html, re.I)
        return m.group(1).strip() if m else None

    @staticmethod
    def _extract_address(html: str) -> str | None:
        patterns = [
            r'itemprop="streetAddress"[^>]*>([^<]+)<',
            r'class="[^"]*(?:address|adresse|location)[^"]*"[^>]*>([^<]{5,120})<',
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
    def _extract_city(address: str | None) -> str | None:
        if not address:
            return None
        m = re.search(r"\d{5}\s+([A-ZÄÖÜa-zäöüß][^,\n]{2,40})", address)
        return m.group(1).strip() if m else None

    @staticmethod
    def _extract_verkehrswert(html: str) -> float | None:
        m = re.search(r"verkehrswert[^€\d]*(\d[\d.,]+)\s*€", html, re.IGNORECASE)
        if m:
            raw = m.group(1).replace(".", "").replace(",", ".")
            try:
                return float(raw)
            except ValueError:
                pass
        return None

    @staticmethod
    def _extract_auction_datetime(html: str) -> tuple[datetime | None, str | None]:
        m = re.search(
            r"(?:zv.?termin|versteigerungstermin)[^\d]*(\d{2}\.\d{2}\.\d{4})"
            r"(?:[^\d]*(\d{2}:\d{2}))?",
            html,
            re.IGNORECASE,
        )
        if m:
            date_str = m.group(1)
            time_str = m.group(2)
            full_str = f"{date_str} {time_str or '00:00'}"
            try:
                dt = datetime.strptime(full_str, "%d.%m.%Y %H:%M")
                return dt, time_str
            except ValueError:
                pass
        return None, None

    @staticmethod
    def _extract_court(html: str) -> str | None:
        m = re.search(r"amtsgericht\s+([A-ZÄÖÜa-zäöüß][a-zäöüßA-ZÄÖÜ\s-]{2,40})", html, re.I)
        return m.group(0).strip() if m else None

    @staticmethod
    def _extract_object_type(html: str) -> str | None:
        types = {
            "einfamilienhaus": "house", "doppelhaus": "house", "reihenhaus": "house",
            "eigentumswohnung": "condo", "wohnung": "condo",
            "grundstück": "land", "gewerbeobjekt": "commercial",
        }
        html_lower = html.lower()
        for key, val in types.items():
            if key in html_lower:
                return val
        return "other"
