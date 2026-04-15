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
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog

from app.adapters.base import ComplianceMeta, DiscoverItem, ScrapeAdapter
from app.config import settings

logger = structlog.get_logger(__name__)


class ComplianceGateError(RuntimeError):
    """Raised when a compliance-gated adapter is called while disabled."""


@dataclass
class ZVGDiscoverParams:
    state: str  # Bundesland (required by ZVG portal UI)
    city: str | None = None


@dataclass
class ZVGDetail:
    external_id: str
    state: str
    court: str | None
    object_description: str | None
    auction_date: str | None
    verkehrswert_raw: str | None
    source_url: str


@dataclass
class ZVGParsed:
    external_id: str
    state: str
    court: str | None
    object_description: str | None
    auction_date: str | None
    verkehrswert_eur: float | None
    source_url: str


class ZVGAdapter(ScrapeAdapter[ZVGDiscoverParams, ZVGDetail, ZVGParsed]):
    """
    ZVG portal adapter — compliance-gated.

    Allowed strategy when enabled:
    - Public search pages (state-level listing only, no detail endpoint crawling).
    - Store metadata + deep link. Do NOT fetch /showZvg or /showAnhang.
    - Human-in-loop: flag matches for manual review rather than automated fetch.
    """

    source_key = "zvg_portal"

    def _check_gate(self) -> None:
        if not settings.zvg_adapter_enabled:
            raise ComplianceGateError(
                "ZVG adapter is disabled. "
                "Set ZVG_ADAPTER_ENABLED=true only after legal review of "
                "zvg-portal.de robots.txt and ToS. "
                "robots.txt disallows /showZvg and /showAnhang endpoints."
            )

    async def discover(self, params: ZVGDiscoverParams) -> list[DiscoverItem]:
        self._check_gate()
        # Implementation placeholder — only activated after compliance review.
        # Strategy: search public listing pages by state (minimum required input).
        # Do NOT crawl detail pages (/showZvg*).
        logger.warning("zvg.discover.not_implemented_pending_legal_review")
        return []

    async def fetch_detail(
        self, external_id: str, url: str, hint: dict[str, Any] | None = None
    ) -> ZVGDetail:
        self._check_gate()
        raise NotImplementedError("ZVG detail fetch pending legal review.")

    async def parse(self, detail: ZVGDetail) -> ZVGParsed:
        self._check_gate()
        raise NotImplementedError("ZVG parse pending legal review.")

    def fingerprint(self, detail: ZVGDetail, parsed: ZVGParsed | None = None) -> str:
        return self.sha256_hex(detail.external_id)

    def compliance_meta(self) -> ComplianceMeta:
        return ComplianceMeta(
            robots_respected=False,  # detail endpoints explicitly disallowed
            tos_reviewed=False,
            store_raw_payload="forbidden",
            personal_data_level="high",
            rate_limit_rps=0.0,  # blocked
            notes=[
                "BLOCKED: robots.txt disallows /showZvg and /showAnhang.",
                "Enable only after legal review + official access arrangement.",
                "Attachments (Gutachten/photos) carry IP + personal data risks.",
                "Prefer metadata-only + deep link or negotiate official data feed.",
            ],
        )
