"""
Insolvency portal adapter (insolvenzbekanntmachungen.de).

Key constraints from the PRD:
- JSF (.jsf) endpoints require session cookies — use Playwright.
- Search result pages expire; direct linking is not permitted.
- 1,000 hit display cap → shard by state × date window.
- Store only: case number, debtor name, court, publication subject, seat city.
- Respect InsBekV § 3 retention: deletion jobs run after 6 months.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import structlog
from playwright.async_api import Page, async_playwright

from app.adapters.base import ComplianceMeta, DiscoverItem, ScrapeAdapter

logger = structlog.get_logger(__name__)

BASE_URL = "https://neu.insolvenzbekanntmachungen.de"
SEARCH_PATH = "/ap/ext/0/result.jsf"

# German federal states for sharding
FEDERAL_STATES = [
    "Baden-Württemberg", "Bayern", "Berlin", "Brandenburg", "Bremen",
    "Hamburg", "Hessen", "Mecklenburg-Vorpommern", "Niedersachsen",
    "Nordrhein-Westfalen", "Rheinland-Pfalz", "Saarland", "Sachsen",
    "Sachsen-Anhalt", "Schleswig-Holstein", "Thüringen",
]


@dataclass
class InsolvencyDiscoverParams:
    state: str
    date_from: date
    date_to: date


@dataclass
class InsolvencyDetail:
    case_number: str        # Aktenzeichen
    debtor_name: str
    court: str              # Insolvenzgericht
    publication_subject: str
    seat_city: str | None
    publication_date: str
    source_url: str
    raw_html_snippet: str   # Minimal — just the result row HTML


@dataclass
class InsolvencyParsed:
    case_number: str
    case_number_norm: str
    debtor_name: str
    debtor_name_norm: str
    court: str
    state: str | None
    publication_subject: str
    seat_city: str | None
    publication_date: datetime | None
    external_id: str  # stable: sha256(case_number_norm + court_norm)


class InsolvencyAdapter(ScrapeAdapter[InsolvencyDiscoverParams, InsolvencyDetail, InsolvencyParsed]):
    """
    Playwright-based scraper for insolvenzbekanntmachungen.de.

    Discovery strategy: iterate over states × 30-min date windows.
    The scheduler calls discover() once per trigger; it shards internally.
    """

    source_key = "insolvency_portal"

    def __init__(self, headless: bool = True, timeout_ms: int = 30_000):
        self.headless = headless
        self.timeout_ms = timeout_ms

    # ── Discover ──────────────────────────────────────────────────────────────

    async def discover(self, params: InsolvencyDiscoverParams) -> list[DiscoverItem]:
        """
        Submit the search form for a given state + date window and
        return one DiscoverItem per result row found.
        """
        items: list[DiscoverItem] = []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=self.headless)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (compatible; EarlyEstate/0.1; research use)",
                locale="de-DE",
            )
            page = await context.new_page()
            page.set_default_timeout(self.timeout_ms)

            try:
                await self._navigate_to_search(page)
                await self._fill_search_form(page, params)
                rows = await self._extract_result_rows(page)
                items = self._rows_to_discover_items(rows, params)
                logger.info(
                    "insolvency.discover",
                    state=params.state,
                    date_from=str(params.date_from),
                    date_to=str(params.date_to),
                    count=len(items),
                )
            except Exception as exc:
                logger.error("insolvency.discover.error", error=str(exc), state=params.state)
            finally:
                await browser.close()

        return items

    async def _navigate_to_search(self, page: Page) -> None:
        await page.goto(BASE_URL, wait_until="networkidle")
        # Accept cookies/session if a consent banner appears
        try:
            await page.click("button:has-text('Akzeptieren')", timeout=3_000)
        except Exception:
            pass

    async def _fill_search_form(self, page: Page, params: InsolvencyDiscoverParams) -> None:
        # Navigate to the search form
        await page.goto(f"{BASE_URL}/ap/ext/0/index.jsf", wait_until="networkidle")

        # Select federal state
        state_select = page.locator("select[name*='bundesland'], select[id*='bundesland']")
        if await state_select.count() > 0:
            await state_select.select_option(label=params.state)

        # Fill date range (dd.mm.yyyy format for German portal)
        date_from_str = params.date_from.strftime("%d.%m.%Y")
        date_to_str = params.date_to.strftime("%d.%m.%Y")

        date_from_input = page.locator("input[name*='datumVon'], input[id*='datumVon']")
        if await date_from_input.count() > 0:
            await date_from_input.fill(date_from_str)

        date_to_input = page.locator("input[name*='datumBis'], input[id*='datumBis']")
        if await date_to_input.count() > 0:
            await date_to_input.fill(date_to_str)

        # Submit
        submit_btn = page.locator("input[type='submit'], button[type='submit']")
        if await submit_btn.count() > 0:
            await submit_btn.first.click()
            await page.wait_for_load_state("networkidle")

    async def _extract_result_rows(self, page: Page) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []

        # Result table rows — adapt selector to actual portal structure
        result_rows = page.locator("table.result tr, tr.resultRow, tbody tr")
        count = await result_rows.count()

        for i in range(count):
            row = result_rows.nth(i)
            cells = row.locator("td")
            cell_count = await cells.count()
            if cell_count < 4:
                continue

            row_data: dict[str, str] = {}
            texts = []
            for j in range(cell_count):
                texts.append((await cells.nth(j).inner_text()).strip())

            # Column order from portal: date, court, case_number, debtor, subject
            if len(texts) >= 5:
                row_data = {
                    "publication_date": texts[0],
                    "court": texts[1],
                    "case_number": texts[2],
                    "debtor_name": texts[3],
                    "publication_subject": texts[4],
                    "seat_city": texts[5] if len(texts) > 5 else "",
                }
            elif len(texts) >= 4:
                row_data = {
                    "publication_date": texts[0],
                    "court": texts[1],
                    "case_number": texts[2],
                    "debtor_name": texts[3],
                    "publication_subject": "",
                    "seat_city": "",
                }

            if row_data.get("case_number"):
                row_data["raw_html"] = await row.inner_html()
                rows.append(row_data)

        return rows

    def _rows_to_discover_items(
        self, rows: list[dict[str, str]], params: InsolvencyDiscoverParams
    ) -> list[DiscoverItem]:
        items = []
        for row in rows:
            case_norm = self._normalize_case_number(row.get("case_number", ""))
            court_norm = row.get("court", "").lower().strip()
            ext_id = self.sha256_hex(f"{case_norm}|{court_norm}")[:32]

            items.append(
                DiscoverItem(
                    external_id=ext_id,
                    url=BASE_URL,  # Results are not directly linkable per portal rules
                    hint={
                        "state": params.state,
                        "case_number": row.get("case_number", ""),
                        "court": row.get("court", ""),
                        "debtor_name": row.get("debtor_name", ""),
                        "publication_subject": row.get("publication_subject", ""),
                        "seat_city": row.get("seat_city", ""),
                        "publication_date": row.get("publication_date", ""),
                        "raw_html": row.get("raw_html", ""),
                    },
                )
            )
        return items

    # ── Fetch detail ──────────────────────────────────────────────────────────

    async def fetch_detail(
        self, external_id: str, url: str, hint: dict[str, Any] | None = None
    ) -> InsolvencyDetail:
        """
        Detail comes from the hint dict populated during discover().
        The portal does not allow direct linking to result pages,
        so we store the normalized metadata, not raw HTML.
        """
        hint = hint or {}
        return InsolvencyDetail(
            case_number=hint.get("case_number", ""),
            debtor_name=hint.get("debtor_name", ""),
            court=hint.get("court", ""),
            publication_subject=hint.get("publication_subject", ""),
            seat_city=hint.get("seat_city") or None,
            publication_date=hint.get("publication_date", ""),
            source_url=url,
            raw_html_snippet=hint.get("raw_html", "")[:2000],  # Truncate — metadata only
        )

    # ── Parse ─────────────────────────────────────────────────────────────────

    async def parse(self, detail: InsolvencyDetail) -> InsolvencyParsed:
        case_norm = self._normalize_case_number(detail.case_number)
        court_norm = detail.court.lower().strip()
        debtor_norm = self._normalize_name(detail.debtor_name)
        pub_dt = self._parse_german_date(detail.publication_date)
        ext_id = self.sha256_hex(f"{case_norm}|{court_norm}")[:32]

        # Derive state from court name heuristic
        state = self._infer_state_from_court(detail.court)

        return InsolvencyParsed(
            case_number=detail.case_number,
            case_number_norm=case_norm,
            debtor_name=detail.debtor_name,
            debtor_name_norm=debtor_norm,
            court=detail.court,
            state=state,
            publication_subject=detail.publication_subject,
            seat_city=detail.seat_city,
            publication_date=pub_dt,
            external_id=ext_id,
        )

    # ── Fingerprint ───────────────────────────────────────────────────────────

    def fingerprint(self, detail: InsolvencyDetail, parsed: InsolvencyParsed | None = None) -> str:
        key = f"{detail.case_number}|{detail.court}|{detail.publication_subject}|{detail.publication_date}"
        return self.sha256_hex(key)

    # ── Compliance ────────────────────────────────────────────────────────────

    def compliance_meta(self) -> ComplianceMeta:
        return ComplianceMeta(
            robots_respected=True,
            tos_reviewed=False,  # Must be reviewed before production use
            store_raw_payload="metadata_only",
            personal_data_level="medium",
            rate_limit_rps=0.5,  # 1 request per 2 seconds
            notes=[
                "InsBekV § 3: delete personal data ≤6 months after insolvency procedure ends.",
                "Portal rules: do not store result list URLs; search results are session-bound.",
                "GDPR Art.6(1)(e): purpose-limited to risk screening / professional investor.",
                "Two-week rule applies to pure consumer procedures post-30.06.2021.",
            ],
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _normalize_case_number(raw: str) -> str:
        """Normalize Aktenzeichen: remove whitespace, lowercase."""
        return re.sub(r"\s+", "", raw).lower()

    @staticmethod
    def _normalize_name(name: str) -> str:
        """Basic normalization: strip, collapse whitespace, lowercase."""
        return re.sub(r"\s+", " ", name).strip().lower()

    @staticmethod
    def _parse_german_date(date_str: str) -> datetime | None:
        """Parse dd.mm.yyyy or dd.mm.yyyy HH:MM format."""
        for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y"):
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def _infer_state_from_court(court: str) -> str | None:
        """Best-effort mapping from court name to German state."""
        court_lower = court.lower()
        mapping = {
            "münchen": "Bayern", "nürnberg": "Bayern", "augsburg": "Bayern",
            "berlin": "Berlin",
            "hamburg": "Hamburg",
            "bremen": "Bremen",
            "stuttgart": "Baden-Württemberg", "karlsruhe": "Baden-Württemberg",
            "düsseldorf": "Nordrhein-Westfalen", "köln": "Nordrhein-Westfalen",
            "dortmund": "Nordrhein-Westfalen", "essen": "Nordrhein-Westfalen",
            "frankfurt": "Hessen", "kassel": "Hessen",
            "dresden": "Sachsen", "leipzig": "Sachsen",
            "hannover": "Niedersachsen",
            "magdeburg": "Sachsen-Anhalt",
            "erfurt": "Thüringen",
            "rostock": "Mecklenburg-Vorpommern",
            "potsdam": "Brandenburg",
            "saarbrücken": "Saarland",
            "mainz": "Rheinland-Pfalz",
            "kiel": "Schleswig-Holstein",
        }
        for key, state in mapping.items():
            if key in court_lower:
                return state
        return None

    @classmethod
    def build_discover_windows(
        cls,
        states: list[str] | None = None,
        lookback_hours: int = 1,
        window_minutes: int = 30,
    ) -> list[InsolvencyDiscoverParams]:
        """
        Generate (state, date_window) parameter pairs for the scheduler.
        Shards time into window_minutes intervals to stay under the 1,000-hit cap.
        """
        states = states or FEDERAL_STATES
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        windows = []
        t = now - timedelta(hours=lookback_hours)
        while t < now:
            t_end = min(t + timedelta(minutes=window_minutes), now)
            for state in states:
                windows.append(
                    InsolvencyDiscoverParams(
                        state=state,
                        date_from=t.date(),
                        date_to=t_end.date(),
                    )
                )
            t = t_end
        return windows
