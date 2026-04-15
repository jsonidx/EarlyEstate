"""Email alert channel (aiosmtplib)."""

from __future__ import annotations

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import aiosmtplib
import structlog

from app.alerts.base import AlertChannel, format_alert_text, format_score_badge
from app.config import settings

logger = structlog.get_logger(__name__)


class EmailChannel(AlertChannel):
    """Send alerts via SMTP using aiosmtplib."""

    async def send(self, recipient: str, payload: dict[str, Any]) -> bool:
        if not (settings.smtp_user and settings.smtp_password):
            logger.warning("email.no_credentials")
            return False

        try:
            score = payload.get("score_total", 0)
            party_name = payload.get("party", {}).get("canonical_name", "Unknown")
            lead_city = payload.get("asset_lead", {}).get("city", "Unknown city")

            subject = (
                f"[EarlyEstate] {format_score_badge(score)} — "
                f"{party_name} / {lead_city}"
            )

            plain_text = format_alert_text(payload)
            html_body = _render_html(payload)

            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = settings.alert_from_email
            msg["To"] = recipient

            msg.attach(MIMEText(plain_text, "plain", "utf-8"))
            msg.attach(MIMEText(html_body, "html", "utf-8"))

            await aiosmtplib.send(
                msg,
                hostname=settings.smtp_host,
                port=settings.smtp_port,
                username=settings.smtp_user,
                password=settings.smtp_password,
                start_tls=True,
            )

            logger.info("email.sent", recipient=recipient)
            return True

        except Exception as exc:
            logger.error("email.send_error", recipient=recipient, error=str(exc))
            return False


def _render_html(payload: dict[str, Any]) -> str:
    """Render a simple HTML email body."""
    party = payload.get("party", {})
    event = payload.get("insolvency_event", {})
    lead = payload.get("asset_lead", {})
    score = payload.get("score_total", 0)
    features = payload.get("features", {})
    badge = format_score_badge(score)
    next_actions = payload.get("next_actions", [])

    actions_html = "".join(f"<li>{a}</li>" for a in next_actions)

    return f"""
<!DOCTYPE html>
<html>
<body style="font-family: monospace; max-width: 700px; margin: 0 auto; padding: 20px;">
  <h2>{badge} — Score {score:.0f}/100</h2>

  <h3>Party (Debtor)</h3>
  <table border="1" cellpadding="4" cellspacing="0">
    <tr><td><b>Name</b></td><td>{party.get('canonical_name', '?')}</td></tr>
    <tr><td><b>Type</b></td><td>{party.get('party_type', '?')}</td></tr>
    <tr><td><b>Register</b></td><td>{party.get('register_id', '—')}</td></tr>
  </table>

  <h3>Insolvency Event</h3>
  <table border="1" cellpadding="4" cellspacing="0">
    <tr><td><b>Case No.</b></td><td>{event.get('case_number', '?')}</td></tr>
    <tr><td><b>Court</b></td><td>{event.get('court', '?')}</td></tr>
    <tr><td><b>Published</b></td><td>{event.get('event_time', '?')}</td></tr>
    <tr><td><b>Subject</b></td><td>{event.get('publication_subject', '?')}</td></tr>
  </table>

  <h3>Asset Lead</h3>
  <table border="1" cellpadding="4" cellspacing="0">
    <tr><td><b>Title</b></td><td>{lead.get('title', '?')}</td></tr>
    <tr><td><b>Address</b></td><td>{lead.get('address_raw', '?')}</td></tr>
    <tr><td><b>Type</b></td><td>{lead.get('object_type', '?')}</td></tr>
    <tr><td><b>Verkehrswert</b></td><td>€{lead.get('verkehrswert_eur') or '—'}</td></tr>
    <tr><td><b>ZV Termin</b></td><td>{lead.get('auction_date') or '—'}</td></tr>
    <tr><td><b>Court</b></td><td>{lead.get('court') or '—'}</td></tr>
    <tr><td><b>Cues</b></td><td>{', '.join(lead.get('auction_signal_terms') or [])}</td></tr>
    <tr><td><b>URL</b></td><td><a href="{lead.get('details_url', '#')}">{lead.get('details_url', '—')}</a></td></tr>
  </table>

  <h3>Score Breakdown</h3>
  <table border="1" cellpadding="4" cellspacing="0">
    <tr><td>Name similarity</td><td>{features.get('name_similarity', 0):.1f}/40</td></tr>
    <tr><td>Geo score</td><td>{features.get('geo_score', 0):.1f}/30</td></tr>
    <tr><td>Auction signals</td><td>{features.get('auction_signal_score', 0):.1f}/20</td></tr>
    <tr><td>Register match</td><td>{features.get('register_id_match', 0):.1f}/10</td></tr>
    <tr><td><b>Total</b></td><td><b>{score:.1f}/100</b></td></tr>
  </table>

  <h3>Next Actions</h3>
  <ul>{actions_html}</ul>

  <hr>
  <small>EarlyEstate — Distress Screener | Retention: 6 months (InsBekV §3)</small>
</body>
</html>
"""
