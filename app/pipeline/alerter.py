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
        .limit(200)
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

    brw = await _fetch_bodenrichtwert(db, lead)

    # Build alert payload (explainability fields from PRD)
    payload = _build_alert_payload(mc, party, lead, event, bodenrichtwert_eur_m2=brw)

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


async def send_digest_alerts(db: AsyncSession, top_n: int = 10) -> int:
    """Dispatch both Telegram and email digests. Returns total alerts marked SENT."""
    sent = 0
    sent += await _send_telegram_digest(db, top_n)
    sent += await _send_email_digest(db, top_n)
    return sent


async def _send_telegram_digest(db: AsyncSession, top_n: int = 10) -> int:
    """
    Batch dispatch: group DIGEST-status alerts into a single Telegram message.

    Selects the top N pending DIGEST alerts (by match score), renders one
    grouped summary message, marks all as SENT, and returns the count sent.
    Only fires on Telegram — digest summaries are not emailed individually.
    """
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        logger.warning("digest.no_telegram_config")
        return 0

    stmt = (
        select(Alert)
        .where(
            Alert.delivery_rule == "DIGEST",
            Alert.status == "PENDING",
            Alert.channel == "TELEGRAM",
        )
        .order_by(Alert.created_at.desc())
        .limit(top_n)
    )
    result = await db.execute(stmt)
    alerts: list[Alert] = result.scalars().all()

    if not alerts:
        logger.info("digest.nothing_to_send")
        return 0

    text = _format_digest_message(alerts)

    try:
        from telegram import Bot

        bot = Bot(token=settings.telegram_bot_token)
        if len(text) > 4000:
            text = text[:4000] + "\n…"
        await bot.send_message(
            chat_id=settings.telegram_chat_id,
            text=f"<pre>{text}</pre>",
            parse_mode="HTML",
        )
        logger.info("digest.sent", count=len(alerts))
    except Exception as exc:
        logger.error("digest.send_error", error=str(exc))
        return 0

    now = datetime.now(timezone.utc)
    for alert in alerts:
        alert.status = "SENT"
        alert.sent_at = now

    return len(alerts)


def _format_digest_message(alerts: list[Alert]) -> str:
    """Render top-N digest alerts as a single grouped Telegram message."""
    from app.alerts.base import format_score_badge

    lines = [
        f"EarlyEstate Daily Digest — {len(alerts)} match(es)",
        "=" * 40,
        "",
    ]
    for i, alert in enumerate(alerts, 1):
        p = alert.payload
        party = p.get("party", {})
        lead = p.get("asset_lead", {})
        event = p.get("insolvency_event", {})
        score = p.get("score_total", 0)
        features = p.get("features", {})

        lines.append(f"#{i} {format_score_badge(score)} Score {score:.0f}/100")
        lines.append(f"  Party : {party.get('canonical_name', '?')} ({party.get('party_type', '?')})")
        lines.append(f"  Court : {event.get('court', '?')}")
        lines.append(f"  Lead  : {lead.get('title', '?')}")
        city_plz = " ".join(filter(None, [lead.get('postal_code'), lead.get('city')]))
        if city_plz:
            lines.append(f"  Loc   : {city_plz}")
        if lead.get("verkehrswert_eur"):
            lines.append(f"  Wert  : €{lead['verkehrswert_eur']:,.0f}")
        if lead.get("auction_date"):
            lines.append(f"  Termin: {lead['auction_date']}")
        breakdown = (
            f"name={features.get('name_similarity', 0):.0f} "
            f"geo={features.get('geo_score', 0):.0f} "
            f"auction={features.get('auction_signal_score', 0):.0f}"
        )
        lines.append(f"  Score : {breakdown}")
        if lead.get("details_url"):
            lines.append(f"  URL   : {lead['details_url']}")
        lines.append("")

    return "\n".join(lines)


async def _send_email_digest(db: AsyncSession, top_n: int = 10) -> int:
    """
    Send a grouped HTML email digest for the top N pending DIGEST email alerts.
    Marks delivered alerts as SENT. Returns count sent.
    """
    if not (settings.smtp_user and settings.smtp_password):
        return 0

    stmt = (
        select(Alert)
        .where(
            Alert.delivery_rule == "DIGEST",
            Alert.status == "PENDING",
            Alert.channel == "EMAIL",
        )
        .order_by(Alert.created_at.desc())
        .limit(top_n)
    )
    result = await db.execute(stmt)
    alerts: list[Alert] = result.scalars().all()

    if not alerts:
        return 0

    subject = f"[EarlyEstate] Daily Digest — {len(alerts)} match(es)"
    html_body = _render_digest_html(alerts)
    plain_body = _format_digest_message(alerts)

    try:
        import aiosmtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = settings.alert_from_email
        msg["To"] = settings.smtp_user
        msg.attach(MIMEText(plain_body, "plain", "utf-8"))
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        await aiosmtplib.send(
            msg,
            hostname=settings.smtp_host,
            port=settings.smtp_port,
            username=settings.smtp_user,
            password=settings.smtp_password,
            start_tls=True,
        )
        logger.info("digest.email_sent", count=len(alerts))
    except Exception as exc:
        logger.error("digest.email_error", error=str(exc))
        return 0

    now = datetime.now(timezone.utc)
    for alert in alerts:
        alert.status = "SENT"
        alert.sent_at = now

    return len(alerts)


def _render_digest_html(alerts: list[Alert]) -> str:
    """Render top-N digest alerts as a single HTML email."""
    from app.alerts.base import format_score_badge

    rows = []
    for i, alert in enumerate(alerts, 1):
        p = alert.payload
        party = p.get("party", {})
        lead = p.get("asset_lead", {})
        event = p.get("insolvency_event", {})
        score = p.get("score_total", 0)
        features = p.get("features", {})
        badge = format_score_badge(score)
        url = lead.get("details_url", "")
        brw = lead.get("bodenrichtwert_eur_m2")

        rows.append(f"""
  <tr style="border-top:2px solid #333">
    <td style="padding:8px 4px;vertical-align:top">{i}</td>
    <td style="padding:8px 4px">
      <b>{badge} Score {score:.0f}/100</b><br>
      <b>Partei:</b> {party.get('canonical_name','?')} ({party.get('party_type','?')})<br>
      <b>Gericht:</b> {event.get('court','?')}<br>
      <b>Aktenzeichen:</b> {event.get('case_number','?')}<br>
      <b>ZV-Objekt:</b> {lead.get('title','?')}<br>
      <b>Ort:</b> {lead.get('postal_code','')} {lead.get('city','?')}<br>
      {f"<b>Verkehrswert:</b> €{lead['verkehrswert_eur']:,.0f}<br>" if lead.get('verkehrswert_eur') else ''}
      {f"<b>Bodenrichtwert:</b> €{brw:,.0f}/m² (BORIS)<br>" if brw else ''}
      {f"<b>ZV-Termin:</b> {lead['auction_date']}<br>" if lead.get('auction_date') else ''}
      <b>Score:</b> name={features.get('name_similarity',0):.0f}
        geo={features.get('geo_score',0):.0f}
        auction={features.get('auction_signal_score',0):.0f}
        court={features.get('court_jurisdiction_score',0):.0f}<br>
      {f'<a href="{url}">{url}</a>' if url else '—'}
    </td>
  </tr>""")

    rows_html = "\n".join(rows)
    return f"""<!DOCTYPE html>
<html>
<body style="font-family:monospace;max-width:800px;margin:0 auto;padding:20px">
  <h2>EarlyEstate Daily Digest — {len(alerts)} match(es)</h2>
  <table border="1" cellpadding="4" cellspacing="0" style="width:100%">
    <thead><tr><th>#</th><th>Match</th></tr></thead>
    <tbody>{rows_html}</tbody>
  </table>
  <hr>
  <small>EarlyEstate — Distress Screener | Retention: 6 months (InsBekV §3)</small>
</body>
</html>"""


async def _fetch_bodenrichtwert(db: AsyncSession, lead: AssetLead) -> float | None:
    """Load the most recent BORIS BRW valuation for this lead, if available."""
    from app.models.asset_lead import Valuation
    stmt = (
        select(Valuation)
        .where(Valuation.asset_lead_id == lead.id)
        .order_by(Valuation.created_at.desc())
        .limit(1)
    )
    result = await db.execute(stmt)
    val = result.scalar_one_or_none()
    if val and val.meta.get("type") == "bodenrichtwert":
        return val.meta.get("value_eur_per_m2")
    return None


def _build_alert_payload(
    mc: MatchCandidate,
    party: Party,
    lead: AssetLead,
    event: Event | None,
    bodenrichtwert_eur_m2: float | None = None,
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
            "bodenrichtwert_eur_m2": bodenrichtwert_eur_m2,
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
