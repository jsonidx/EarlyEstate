"""
Abstract scraping adapter interface.

All source integrations implement this contract so the pipeline runner can
call discover → fetch_detail → parse uniformly, regardless of the underlying
source technology (Playwright, HTTP, API, etc.).
"""

from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Generic, Literal, TypeVar

TDiscover = TypeVar("TDiscover")
TDetail = TypeVar("TDetail")
TParsed = TypeVar("TParsed")


@dataclass
class DiscoverItem:
    external_id: str
    url: str
    hint: dict[str, Any] = field(default_factory=dict)


@dataclass
class ComplianceMeta:
    robots_respected: bool
    tos_reviewed: bool
    store_raw_payload: Literal["allowed", "metadata_only", "forbidden"]
    personal_data_level: Literal["low", "medium", "high"]
    rate_limit_rps: float
    notes: list[str] = field(default_factory=list)


class ScrapeAdapter(ABC, Generic[TDiscover, TDetail, TParsed]):
    """
    Base adapter. Subclasses implement each source.

    Compliance metadata is checked by the scheduler before every run:
    - If source is not enabled in DB, skip.
    - If store_raw_payload == "forbidden", never persist raw HTML.
    - rate_limit_rps is enforced by the worker via asyncio.sleep.
    """

    source_key: str  # Must be unique, matches Source.source_key in DB

    @abstractmethod
    async def discover(self, params: TDiscover) -> list[DiscoverItem]:
        """Return new item IDs/URLs to fetch. Must be idempotent."""

    @abstractmethod
    async def fetch_detail(
        self, external_id: str, url: str, hint: dict[str, Any] | None = None
    ) -> TDetail:
        """Retrieve raw payload for a single item."""

    @abstractmethod
    async def parse(self, detail: TDetail) -> TParsed:
        """Normalize raw payload into domain objects."""

    @abstractmethod
    def fingerprint(self, detail: TDetail, parsed: TParsed | None = None) -> str:
        """Stable content fingerprint for idempotency (stored in raw_document)."""

    @abstractmethod
    def compliance_meta(self) -> ComplianceMeta:
        """Compliance constraints used by scheduler and policy engine."""

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def sha256_hex(data: str | bytes) -> str:
        if isinstance(data, str):
            data = data.encode()
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def url_hash(url: str) -> bytes:
        return hashlib.sha256(url.encode()).digest()
