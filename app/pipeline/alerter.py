"""
Alert pipeline step.

Selects pending match candidates, applies delivery rules (INSTANT vs DIGEST),
deduplicates via dedup_key, and dispatches to configured channels.

Scoring tiers determine delivery:
  HIGH   → INSTANT (immediate delivery)
  MEDIUM → INSTANT if party/city subscription, else DIGEST
  LOW    → DIGEST only
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.alert import Alert
from app.models.asset_lead import AssetLead
from app.models.event import Event
from app.models.match_candidate import MatchCandidate
from app.models.party import Party
from app.models.source import Source
from app.pipeline.matcher import HIGH_THRESHOLD, MEDIUM_THRESHOLD, build_dedup_key

logger = structlog.get_logger(__name__)


async def process_pending_alerts(db: AsyncSession) -> int:
    """
    Main alerter entry point called by the worker.
    Finds OPEN match candidates above threshold, creates + dispatches Alert records.
    Returns number of alerts sent.
    """
    stmt = (
        select(MatchCandidate)
        .where(
            MatchCandidate.status == "OPEN",
            MatchCandidate.score_total >= 20.0,  # LOW threshold
        )
        .order_by(MatchCandidate.score_total.desc())
        .limit(100)
    )
    result = await db.execute(stmt)
    candidates = result.scalars().all()

    sent = 0
    for mc in candidates:
        try:
            dispatched = await _dispatch_match_candidate(db, mc)
            sent += dispatched
        except Exception as exc:
            logger.error("alerter.dispatch_error", match_id=str(mc.id), error=str(exc))

    return sent


async def _dispatch_match_candidate(db: AsyncSession, mc: MatchCandidate) -> int:
    """Build and dispatch alerts for a single match candidate."""
    party = await db.get(Party, mc.party_id)
    lead = await db.get(AssetLead, mc.asset_lead_id)
    if not party or not lead:
        return 0

    bucket = mc.score_breakdown.get("bucket", "LOW")
    delivery_rule = "INSTANT" if mc.score_total >= MEDIUM_THRESHOLD else "DIGEST"

    # Find the most recent event for this party
    stmt = (
        select(Event)
        .where(Event.party_id == mc.party_id)
        .order_by(Event.event_time.desc())
        .limit(1)
    )
    result = await db.execute(stmt)
    event = result.scalar_one_or_none()
    event_time = event.event_time if event else datetime.now(timezone.utc)

    dedup_key = build_dedup_key(mc.party_id, mc.asset_lead_id, bucket, event_time)

    # Build alert payload (explainability fields from PRD)
    payload = _build_alert_payload(mc, party, lead, event)

    sent = 0

    # Telegram
    if settings.telegram_bot_token and settings.telegram_chat_id:
        sent += await _create_and_send_alert(
            db, mc, "TELEGRAM", settings.telegram_chat_id, dedup_key, payload, delivery_rule
        )

    # Email
    if settings.smtp_user:
        sent += await _create_and_send_alert(
            db, mc, "EMAIL", settings.smtp_user, dedup_key, payload, delivery_rule
        )

    # Webhook
    if settings.webhook_url:
        sent += await _create_and_send_alert(
            db, mc, "WEBHOOK", settings.webhook_url, dedup_key, payload, delivery_rule
        )

    return sent


async def _create_and_send_alert(
    db: AsyncSession,
    mc: MatchCandidate,
    channel: str,
    recipient: str,
    dedup_key: str,
    payload: dict[str, Any],
    delivery_rule: str,
) -> int:
    """Create an Alert record if not duplicate, then dispatch it."""
    # Dedup check
    stmt = select(Alert).where(
        Alert.channel == channel,
        Alert.recipient == recipient,
        Alert.dedup_key == dedup_key,
    )
    result = await db.execute(stmt)
    existing = result.scalar_one_or_none()

    if existing and existing.status == "SENT":
        return 0  # Already delivered

    if not existing:
        alert = Alert(
            match_candidate_id=mc.id,
            channel=channel,
            recipient=recipient,
            dedup_key=dedup_key,
            payload=payload,
            delivery_rule=delivery_rule,
        )
        db.add(alert)
        await db.flush()
    else:
        alert = existing

    if delivery_rule == "DIGEST":
        return 0  # Digest is batched separately

    # Dispatch
    from app.alerts import dispatch_alert
    success = await dispatch_alert(alert)

    if success:
        alert.status = "SENT"
        alert.sent_at = datetime.now(timezone.utc)
    else:
        alert.status = "FAILED"

    return 1 if success else 0


def _build_alert_payload(
    mc: MatchCandidate,
    party: Party,
    lead: AssetLead,
    event: Event | None,
) -> dict[str, Any]:
    """Build the explainable alert payload (PRD spec)."""
    return {
        "party": {
            "id": str(party.id),
            "canonical_name": party.canonical_name,
            "name_raw": party.name_raw,
            "party_type": party.party_type,
            "legal_form": party.legal_form,
            "register_id": party.register_id,
        },
        "insolvency_event": {
            "event_id": str(event.id) if event else None,
            "event_type": event.event_type if event else None,
            "case_number": event.payload.get("case_number") if event else None,
            "court": event.payload.get("court") if event else None,
            "publication_subject": event.payload.get("publication_subject") if event else None,
            "event_time": event.event_time.isoformat() if event and event.event_time else None,
        },
        "asset_lead": {
            "id": str(lead.id),
            "title": lead.title,
            "address_raw": lead.address_raw,
            "postal_code": lead.postal_code,
            "city": lead.city,
            "object_type": lead.object_type,
            "asking_price_eur": float(lead.asking_price_eur) if lead.asking_price_eur else None,
            "verkehrswert_eur": float(lead.verkehrswert_eur) if lead.verkehrswert_eur else None,
            "auction_date": lead.auction_date.isoformat() if lead.auction_date else None,
            "court": lead.court,
            "details_url": lead.details_url,
            "auction_signal_terms": lead.auction_signal_terms or [],
        },
        "features": mc.score_breakdown,
        "score_total": float(mc.score_total),
        "next_actions": [
            "Check insolvency court file at Insolvenzgericht",
            "Verify property ownership via Grundbuch (requires berechtigtes Interesse)",
            "Inspect property / contact agent if details_url is available",
            "Review ZVG term publication if auction_date is set",
        ],
    }
