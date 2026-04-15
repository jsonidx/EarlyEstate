"""Base alert dispatcher interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class AlertChannel(ABC):
    """Abstract base for alert delivery channels."""

    @abstractmethod
    async def send(self, recipient: str, payload: dict[str, Any]) -> bool:
        """Send alert. Returns True on success, False on failure."""


def format_score_badge(score: float) -> str:
    if score >= 80:
        return "🔴 HIGH"
    if score >= 50:
        return "🟡 MEDIUM"
    return "🟢 LOW"


def format_alert_text(payload: dict[str, Any]) -> str:
    """Render a human-readable alert summary from the payload dict."""
    party = payload.get("party", {})
    event = payload.get("insolvency_event", {})
    lead = payload.get("asset_lead", {})
    score = payload.get("score_total", 0)
    features = payload.get("features", {})

    lines = [
        f"{format_score_badge(score)} Match — Score {score:.0f}/100",
        "",
        f"Party: {party.get('canonical_name', '?')} ({party.get('party_type', '?')})",
        f"Insolvency: {event.get('case_number', '?')} @ {event.get('court', '?')}",
        f"Published: {event.get('event_time', '?')}",
        f"Subject: {event.get('publication_subject', '?')}",
        "",
        f"Lead: {lead.get('title', '?')}",
        f"Address: {lead.get('address_raw', '?')}",
        f"Type: {lead.get('object_type', '?')}",
    ]

    if lead.get("verkehrswert_eur"):
        lines.append(f"Verkehrswert: €{lead['verkehrswert_eur']:,.0f}")
    if lead.get("auction_date"):
        lines.append(f"ZV Termin: {lead['auction_date']}")
    if lead.get("court"):
        lines.append(f"Court: {lead['court']}")
    if lead.get("auction_signal_terms"):
        lines.append(f"Cues: {', '.join(lead['auction_signal_terms'])}")

    lines += [
        "",
        f"Score breakdown: name={features.get('name_similarity', 0):.0f} "
        f"geo={features.get('geo_score', 0):.0f} "
        f"auction={features.get('auction_signal_score', 0):.0f} "
        f"register={features.get('register_id_match', 0):.0f}",
    ]

    if lead.get("details_url"):
        lines.append(f"URL: {lead['details_url']}")

    return "\n".join(lines)
