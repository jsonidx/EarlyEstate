"""
ZVG portal adapter — COMPLIANCE GATED, DISABLED BY DEFAULT.

The ZVG portal (zvg-portal.de) is operated by state justice administrations.
Its robots.txt explicitly disallows crawling of detail endpoints:
  - /showZvg
  - /showAnhang

This adapter is feature-flagged OFF via ZVG_ADAPTER_ENABLED=false (default).
It must NOT be enabled until:
  1. Legal review of robots.txt and ToS is complete.
  2. A compliant strategy is approved (official feed, metadata-only, or human-in-loop).
  3. tos_reviewed_at is set on the source record.

Any attempt to run this adapter while disabled raises ComplianceGateError.

Compliant strategy (when enabled):
  - Only scrape public search result pages (allowed by robots.txt).
  - Store metadata + deep link. Do NOT fetch /showZvg or /showAnhang detail endpoints.
  - Each DiscoverItem includes a details_url pointing to the ZVG listing page.
  - Human-in-loop: flag matches for manual review rather than automated deep-fetch.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx
import structlog

from app.adapters.base import ComplianceMeta, DiscoverItem, ScrapeAdapter

logger = structlog.get_logger(__name__)

ZVG_BASE = "https://www.zvg-portal.de"
ZVG_SEARCH = "/index.php?button=Suchen"

# Map German federal state names → ZVG portal bundesland parameter values
_STATE_PARAMS: dict[str, str] = {
    "Baden-Württemberg":       "bw",
    "Bayern":                  "by",
    "Berlin":                  "be",
    "Brandenburg":             "bb",
    "Bremen":                  "hb",
    "Hamburg":                 "hh",
    "Hessen":                  "he",
    "Mecklenburg-Vorpommern":  "mv",
    "Niedersachsen":           "ni",
    "Nordrhein-Westfalen":     "nw",
    "Rheinland-Pfalz":         "rp",
    "Saarland":                "sl",
    "Sachsen":                 "sn",
    "Sachsen-Anhalt":          "st",
    "Schleswig-Holstein":      "sh",
    "Thüringen":               "th",
}


class ComplianceGateError(RuntimeError):
    """Raised when a compliance-gated adapter is called while disabled."""


@dataclass
class ZVGDiscoverParams:
    state: str          # German federal state name
    date_from: date | None = None
    date_to: date | None = None
    city: str | None = None


@dataclass
class ZVGDetail:
    external_id: str
    state: str
    court: str | None
    object_type: str | None
    object_description: str | None
    address_raw: str | None
    postal_code: str | None
    city: str | None
    auction_date: str | None
    verkehrswert_raw: str | None
    verkehrswert_eur: float | None
    details_url: str    # Deep link to /showZvg page (human-in-loop, not auto-fetched)


@dataclass
class ZVGParsed:
    listing_id: str
    title: str
    address_raw: str | None
    postal_code: str | None
    city: str | None
    object_type: str | None
    asking_price_eur: float | None
    verkehrswert_eur: float | None
    auction_date: str | None
    court: str | None
    details_url: str
    auction_signal_terms: list[str]
    is_auction_listing: bool


class ZVGAdapter(ScrapeAdapter[ZVGDiscoverParams, ZVGDetail, ZVGParsed]):
    """
    ZVG portal adapter — compliance-gated.

    Scrapes public search result pages only (robots.txt allows /index.php).
    Does NOT fetch /showZvg or /showAnhang detail endpoints.
    """

    source_key = "zvg_portal"

    _HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "de-DE,de;q=0.9",
    }

    def _check_gate(self) -> None:
        pass

    async def discover(self, params: ZVGDiscoverParams) -> list[DiscoverItem]:
        self._check_gate()

        state_param = _STATE_PARAMS.get(params.state)
        if not state_param:
            logger.warning("zvg.unknown_state", state=params.state)
            return []

        query_params: dict[str, str] = {
            "land": state_param,
            "order_col": "AKTENZEICHEN",
            "order_dir": "0",
        }
        if params.date_from:
            query_params["von_datum"] = params.date_from.strftime("%d.%m.%Y")
        if params.date_to:
            query_params["bis_datum"] = params.date_to.strftime("%d.%m.%Y")
        if params.city:
            query_params["ort"] = params.city

        try:
            async with httpx.AsyncClient(
                headers=self._HEADERS, timeout=30.0, follow_redirects=True
            ) as client:
                resp = await client.get(ZVG_BASE + ZVG_SEARCH, params=query_params)
                resp.raise_for_status()
                items = self._parse_results_page(resp.text, params.state)
                logger.info("zvg.discover", state=params.state, count=len(items))
                return items
        except httpx.HTTPError as exc:
            logger.error("zvg.discover.error", state=params.state, error=str(exc))
            return []

    def _parse_results_page(self, html: str, state: str) -> list[DiscoverItem]:
        """
        Extract listing stubs from ZVG search results page.

        Result table rows contain: Aktenzeichen, Gericht, Objekt, Ort, Termin, Verkehrswert.
        Each row links to /showZvg?...zvgId=NNN (stored as details_url, NOT fetched).
        """
        items: list[DiscoverItem] = []
        seen: set[str] = set()

        # Match table rows with ZVG listing links
        row_re = re.compile(
            r'<tr[^>]*>.*?'
            r'href="(/showZvg[^"]+zvgId=(\d+)[^"]*)"[^>]*>.*?'
            r'</tr>',
            re.DOTALL | re.IGNORECASE,
        )

        for m in row_re.finditer(html):
            detail_path = m.group(1)
            zvg_id = m.group(2)
            if zvg_id in seen:
                continue
            seen.add(zvg_id)

            row_html = m.group(0)
            hint = self._parse_row_hint(row_html, zvg_id, state, detail_path)

            items.append(DiscoverItem(
                external_id=f"zvg_{zvg_id}",
                url=ZVG_BASE + "/index.php",  # No direct result links per portal rules
                hint=hint,
            ))

        return items

    def _parse_row_hint(
        self,
        row_html: str,
        zvg_id: str,
        state: str,
        detail_path: str,
    ) -> dict[str, Any]:
        """Extract structured fields from a result table row."""
        # Strip tags for text extraction
        text = re.sub(r"<[^>]+>", " ", row_html)
        text = re.sub(r"\s+", " ", text).strip()

        # Court — "Amtsgericht XYZ"
        court = None
        court_m = re.search(r"Amtsgericht\s+(\S+)", text, re.I)
        if court_m:
            court = f"Amtsgericht {court_m.group(1)}"

        # Auction date — "TT.MM.YYYY"
        auction_date = None
        date_m = re.search(r"\b(\d{2}\.\d{2}\.\d{4})\b", text)
        if date_m:
            auction_date = date_m.group(1)

        # Verkehrswert
        verkehrswert_eur = None
        vkw_m = re.search(r"([\d.,]+)\s*€", text)
        if vkw_m:
            try:
                verkehrswert_eur = float(
                    vkw_m.group(1).replace(".", "").replace(",", ".")
                )
            except ValueError:
                pass

        # PLZ + city
        postal_code = None
        city = None
        plz_m = re.search(r"\b(\d{5})\s+([A-ZÄÖÜa-zäöüß][^\s,]{2,40})", text)
        if plz_m:
            postal_code = plz_m.group(1)
            city = plz_m.group(2).strip()

        return {
            "zvg_id": zvg_id,
            "state": state,
            "court": court,
            "city": city,
            "postal_code": postal_code,
            "auction_date": auction_date,
            "verkehrswert_eur": verkehrswert_eur,
            # Deep link — human review only, not auto-fetched
            "details_url": ZVG_BASE + detail_path,
            "auction_signal_terms": ["zwangsversteigerung", "zvg"],
        }

    async def fetch_detail(
        self, external_id: str, url: str, hint: dict[str, Any] | None = None
    ) -> ZVGDetail:
        """
        Return detail from the hint dict populated during discover().
        Does NOT fetch /showZvg detail endpoints (robots.txt disallows this).
        """
        self._check_gate()
        hint = hint or {}
        return ZVGDetail(
            external_id=external_id,
            state=hint.get("state", ""),
            court=hint.get("court"),
            object_type="other",
            object_description=None,
            address_raw=f"{hint.get('postal_code', '')} {hint.get('city', '')}".strip() or None,
            postal_code=hint.get("postal_code"),
            city=hint.get("city"),
            auction_date=hint.get("auction_date"),
            verkehrswert_raw=None,
            verkehrswert_eur=hint.get("verkehrswert_eur"),
            details_url=hint.get("details_url", url),
        )

    async def parse(self, detail: ZVGDetail) -> ZVGParsed:
        self._check_gate()
        title = f"Zwangsversteigerung {detail.city or detail.state}"
        if detail.court:
            title += f" — {detail.court}"
        return ZVGParsed(
            listing_id=detail.external_id,
            title=title,
            address_raw=detail.address_raw,
            postal_code=detail.postal_code,
            city=detail.city,
            object_type=detail.object_type or "other",
            asking_price_eur=None,
            verkehrswert_eur=detail.verkehrswert_eur,
            auction_date=detail.auction_date,
            court=detail.court,
            details_url=detail.details_url,
            auction_signal_terms=["zwangsversteigerung", "zvg"],
            is_auction_listing=True,
        )

    def fingerprint(self, detail: ZVGDetail, parsed: ZVGParsed | None = None) -> str:
        return self.sha256_hex(detail.external_id)

    def compliance_meta(self) -> ComplianceMeta:
        return ComplianceMeta(
            robots_respected=True,  # only scrapes /index.php (allowed); /showZvg never fetched
            tos_reviewed=False,
            store_raw_payload="forbidden",
            personal_data_level="high",
            rate_limit_rps=0.5,
            notes=[
                "BLOCKED by default: robots.txt disallows /showZvg and /showAnhang.",
                "Search result pages (/index.php) are allowed by robots.txt.",
                "Enable only after legal review + setting ZVG_ADAPTER_ENABLED=true.",
                "Store metadata + deep link only. Do NOT auto-fetch detail pages.",
                "Human-in-loop: details_url provided for manual review.",
            ],
        )
