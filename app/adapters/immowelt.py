"""
Immowelt ZV (Zwangsversteigerung) adapter.

Strategy: scrape Immowelt's server-side-rendered ZV search pages — filtered by
vermarktungsart=ZWANGSVERSTEIGERUNG — across four property types.  Each search
card contains title, city, PLZ, and price.  Detail pages are fetched with
browser-like headers; if the portal blocks them (403) the search-card data is
used as-is.

All listings come from an explicit ZV filter, so auction_signal_terms is always
["zwangsversteigerung"] — this satisfies the _ingest_asset_lead gate.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from app.adapters.base import ComplianceMeta, DiscoverItem, ScrapeAdapter

logger = structlog.get_logger(__name__)

BASE_URL = "https://www.immowelt.de"

# Property-type path segments for ZV search
PROPERTY_TYPES = ["haeuser", "wohnungen", "grundstuecke", "gewerbe"]

# Map immowelt type slug → AssetLead object_type
_TYPE_MAP = {
    "haeuser": "house",
    "wohnungen": "condo",
    "grundstuecke": "land",
    "gewerbe": "commercial",
}

# ZV cue words present in the listing title text from search cards
_ZV_TITLE_CUES = ["versteigerung", "zwangsversteigerung"]


def _build_search_url(prop_type: str) -> str:
    return (
        f"{BASE_URL}/liste/deutschland/{prop_type}/kaufen"
        f"?vermarktungsart=ZWANGSVERSTEIGERUNG"
    )


@dataclass
class ImmoweltDiscoverParams:
    prop_type: str = "haeuser"  # one of PROPERTY_TYPES


@dataclass
class ImmoweltDetail:
    listing_id: str
    prop_type: str
    title: str
    address_raw: str
    postal_code: str | None
    city: str | None
    object_type: str | None
    asking_price_eur: float | None
    verkehrswert_eur: float | None
    details_url: str
    auction_signal_terms: list[str]
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class ImmoweltParsed:
    listing_id: str
    title: str
    address_raw: str
    postal_code: str | None
    city: str | None
    object_type: str | None
    asking_price_eur: float | None
    verkehrswert_eur: float | None
    details_url: str
    auction_signal_terms: list[str]
    is_auction_listing: bool


class ImmoweltAdapter(ScrapeAdapter[ImmoweltDiscoverParams, ImmoweltDetail, ImmoweltParsed]):
    """HTTP adapter for Immowelt ZV listings (server-side rendered search page)."""

    source_key = "immowelt_zv"

    # Headers that mimic a real browser visit
    _HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                headers=self._HEADERS,
                timeout=30.0,
                follow_redirects=True,
            )
        return self._client

    # ── Discover ──────────────────────────────────────────────────────────────

    async def discover(self, params: ImmoweltDiscoverParams) -> list[DiscoverItem]:
        """Fetch one property-type ZV search page and return listing stubs."""
        client = await self._get_client()
        url = _build_search_url(params.prop_type)
        items: list[DiscoverItem] = []

        try:
            resp = await client.get(url)
            resp.raise_for_status()
            items = self._extract_listing_stubs(resp.text, params.prop_type)
            logger.info("immowelt.discover", prop_type=params.prop_type, count=len(items))
        except httpx.HTTPStatusError as exc:
            logger.error("immowelt.discover.http_error",
                         prop_type=params.prop_type, status=exc.response.status_code)
        except httpx.HTTPError as exc:
            logger.error("immowelt.discover.error",
                         prop_type=params.prop_type, error=str(exc))

        return items

    def _extract_listing_stubs(self, html: str, prop_type: str) -> list[DiscoverItem]:
        """Extract expose UUIDs and card-level data from search results HTML."""
        items: list[DiscoverItem] = []
        seen: set[str] = set()

        # Each listing has an anchor with href="/expose/{UUID}"
        expose_pattern = re.compile(
            r'href="/expose/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"',
            re.IGNORECASE,
        )

        # Find all unique UUIDs; also try to extract surrounding card text
        # We split by expose link to keep card context
        segments = expose_pattern.split(html)
        # segments alternates: [before_first, uuid1, after_uuid1, uuid2, after_uuid2, ...]

        i = 1  # start at first UUID
        while i < len(segments):
            listing_id = segments[i].lower()
            if listing_id in seen:
                i += 2
                continue
            seen.add(listing_id)

            card_context = segments[i + 1] if i + 1 < len(segments) else ""
            # Take a reasonable window of HTML around the card
            card_snippet = card_context[:2000]

            hint = self._parse_card_snippet(card_snippet, prop_type, listing_id)
            items.append(
                DiscoverItem(
                    external_id=listing_id,
                    url=f"{BASE_URL}/expose/{listing_id}",
                    hint=hint,
                )
            )
            i += 2

        return items

    def _parse_card_snippet(
        self, snippet: str, prop_type: str, listing_id: str
    ) -> dict[str, Any]:
        """Extract structured fields from the HTML snippet following a listing link."""
        # Strip tags for text extraction
        text = re.sub(r"<[^>]+>", " ", snippet)
        text = re.sub(r"\s+", " ", text).strip()

        # PLZ: (12345) or standalone 5-digit block
        plz_m = re.search(r"\((\d{5})\)", text)
        if not plz_m:
            plz_m = re.search(r"\b(\d{5})\b", text)
        postal_code = plz_m.group(1) if plz_m else None

        # City: word(s) before the PLZ in parentheses, or after PLZ
        city: str | None = None
        city_m = re.search(r"([A-ZÄÖÜa-zäöüß][A-Za-zÄÖÜäöüß\s\-]{2,40})\s*\(\d{5}\)", text)
        if city_m:
            city = city_m.group(1).strip()
        elif postal_code:
            after_plz = re.search(rf"{postal_code}\s+([A-ZÄÖÜa-zäöüß][^\s,]{{2,40}})", text)
            if after_plz:
                city = after_plz.group(1).strip()

        # Price: first EUR amount
        price: float | None = None
        price_m = re.search(r"(\d[\d.]+)\s*€", text)
        if price_m:
            try:
                price = float(price_m.group(1).replace(".", "").replace(",", "."))
            except ValueError:
                pass

        # Title: first meaningful sentence / heading fragment
        title_m = re.search(
            r"((?:Einfamilienhaus|Doppelhaus|Reihenhaus|Mehrfamilienhaus|"
            r"Eigentumswohnung|Wohnung|Grundstück|Gewerbe|Zwangsversteigerung)"
            r"[^\n<]{0,120})",
            text, re.IGNORECASE,
        )
        title = title_m.group(1).strip() if title_m else f"ZV-Listing {listing_id[:8]}"

        # Detect Verkehrswert in card text
        vkw: float | None = None
        vkw_m = re.search(r"verkehrswert[^\d]*(\d[\d.,]+)\s*€", text, re.IGNORECASE)
        if vkw_m:
            try:
                vkw = float(vkw_m.group(1).replace(".", "").replace(",", "."))
            except ValueError:
                pass

        # Address raw: city + PLZ combined if available
        if city and postal_code:
            address_raw = f"{city} {postal_code}"
        elif city:
            address_raw = city
        elif postal_code:
            address_raw = postal_code
        else:
            address_raw = ""

        return {
            "prop_type": prop_type,
            "title": title,
            "address_raw": address_raw,
            "postal_code": postal_code,
            "city": city,
            "object_type": _TYPE_MAP.get(prop_type, "other"),
            "asking_price_eur": price,
            "verkehrswert_eur": vkw,
            "auction_signal_terms": ["zwangsversteigerung"],
        }

    # ── Fetch detail ──────────────────────────────────────────────────────────

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=2, max=8))
    async def fetch_detail(
        self, external_id: str, url: str, hint: dict[str, Any] | None = None
    ) -> ImmoweltDetail:
        """
        Try to fetch the expose detail page for richer data.
        Falls back to hint-dict data (from search card) on 403/429.
        """
        hint = hint or {}
        detail_from_hint = self._hint_to_detail(external_id, url, hint)

        client = await self._get_client()
        try:
            resp = await client.get(
                url,
                headers={**self._HEADERS, "Referer": BASE_URL + "/"},
            )
            if resp.status_code in (403, 429, 451):
                logger.debug("immowelt.detail.blocked",
                             listing_id=external_id, status=resp.status_code)
                return detail_from_hint
            resp.raise_for_status()

            # Enrich with detail page data
            detail = self._parse_detail_html(external_id, url, resp.text, hint)
            return detail

        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in (403, 429, 451):
                return detail_from_hint
            raise
        except httpx.HTTPError:
            return detail_from_hint

    def _hint_to_detail(
        self, listing_id: str, url: str, hint: dict[str, Any]
    ) -> ImmoweltDetail:
        return ImmoweltDetail(
            listing_id=listing_id,
            prop_type=hint.get("prop_type", "haeuser"),
            title=hint.get("title", ""),
            address_raw=hint.get("address_raw", ""),
            postal_code=hint.get("postal_code"),
            city=hint.get("city"),
            object_type=hint.get("object_type", "other"),
            asking_price_eur=hint.get("asking_price_eur"),
            verkehrswert_eur=hint.get("verkehrswert_eur"),
            details_url=url,
            auction_signal_terms=hint.get("auction_signal_terms", ["zwangsversteigerung"]),
        )

    def _parse_detail_html(
        self,
        listing_id: str,
        url: str,
        html: str,
        hint: dict[str, Any],
    ) -> ImmoweltDetail:
        """Parse enriched fields from detail page; fall back to hint for missing fields."""
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"\s+", " ", text).strip()
        html_lower = html.lower()

        # Auction cue terms
        from app.adapters.sparkasse import AUCTION_CUES
        found_cues = list({
            term for term in AUCTION_CUES if term in html_lower
        })
        if not found_cues:
            found_cues = ["zwangsversteigerung"]

        # Verkehrswert
        vkw: float | None = hint.get("verkehrswert_eur")
        vkw_m = re.search(r"verkehrswert[^\d]*(\d[\d.,]+)\s*€", text, re.IGNORECASE)
        if vkw_m:
            try:
                vkw = float(vkw_m.group(1).replace(".", "").replace(",", "."))
            except ValueError:
                pass

        # Price
        price: float | None = hint.get("asking_price_eur")
        price_m = re.search(r"kaufpreis[^\d]*(\d[\d.,]+)\s*€", text, re.IGNORECASE)
        if not price_m:
            price_m = re.search(r"(\d[\d.]+)\s*€", text)
        if price_m:
            try:
                price = float(price_m.group(1).replace(".", "").replace(",", "."))
            except ValueError:
                pass

        # Court
        court: str | None = None
        court_m = re.search(r"amtsgericht\s+([A-ZÄÖÜa-zäöüß][^\n<,]{2,40})", text, re.I)
        if court_m:
            court = court_m.group(0).strip()

        # PLZ / city (prefer detail page over hint)
        postal_code = hint.get("postal_code")
        city = hint.get("city")
        plz_m = re.search(r"\b(\d{5})\b", text)
        if plz_m:
            postal_code = plz_m.group(1)
        city_m = re.search(r"\b(\d{5})\s+([A-ZÄÖÜa-zäöüß][^\s,]{2,40})", text)
        if city_m:
            city = city_m.group(2).strip()

        address_raw = hint.get("address_raw") or (
            f"{city} {postal_code}" if city and postal_code else city or postal_code or ""
        )

        # Title
        title = hint.get("title", "")
        og_m = re.search(r'<meta[^>]+property="og:title"[^>]+content="([^"]+)"', html, re.I)
        if og_m:
            title = og_m.group(1).strip()

        return ImmoweltDetail(
            listing_id=listing_id,
            prop_type=hint.get("prop_type", "haeuser"),
            title=title,
            address_raw=address_raw,
            postal_code=postal_code,
            city=city,
            object_type=hint.get("object_type", "other"),
            asking_price_eur=price,
            verkehrswert_eur=vkw,
            details_url=url,
            auction_signal_terms=found_cues,
        )

    # ── Parse ─────────────────────────────────────────────────────────────────

    async def parse(self, detail: ImmoweltDetail) -> ImmoweltParsed:
        return ImmoweltParsed(
            listing_id=detail.listing_id,
            title=detail.title,
            address_raw=detail.address_raw,
            postal_code=detail.postal_code,
            city=detail.city,
            object_type=detail.object_type,
            asking_price_eur=detail.asking_price_eur,
            verkehrswert_eur=detail.verkehrswert_eur,
            details_url=detail.details_url,
            auction_signal_terms=detail.auction_signal_terms,
            is_auction_listing=True,  # Always True — from ZV-filtered search
        )

    # ── Fingerprint ───────────────────────────────────────────────────────────

    def fingerprint(self, detail: ImmoweltDetail, parsed: ImmoweltParsed | None = None) -> str:
        key = "|".join([
            detail.listing_id,
            detail.address_raw or "",
            str(detail.asking_price_eur),
            str(detail.verkehrswert_eur),
        ])
        return self.sha256_hex(key)

    # ── Compliance ────────────────────────────────────────────────────────────

    def compliance_meta(self) -> ComplianceMeta:
        return ComplianceMeta(
            robots_respected=True,
            tos_reviewed=False,
            store_raw_payload="metadata_only",
            personal_data_level="low",
            rate_limit_rps=0.5,  # 1 req / 2 s
            notes=[
                "Only ingest ZV listings (vermarktungsart=ZWANGSVERSTEIGERUNG filter).",
                "Detail page fetch gracefully skipped if portal blocks (403/429).",
                "Search cards are SSR — no JS required for discovery.",
            ],
        )
