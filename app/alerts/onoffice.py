"""
onOffice CRM integration.

Pushes confirmed EarlyEstate matches to onOffice as Agentslog (activity note)
entries. Each confirmed match creates a timestamped note visible in the agent's
dashboard with full party, event, and lead details.

Authentication: onOffice REST API v2 with HMAC-SHA256 per-action signing.
  hmac = base64(HMAC_SHA256(api_secret, timestamp + api_token + action_id + resource_type))

Requires:
  ONOFFICE_API_KEY    — API token from onOffice account settings
  ONOFFICE_API_SECRET — API secret from onOffice account settings
"""

from __future__ import annotations

import base64
import hashlib
import hmac as _hmac
import time
from typing import Any

import httpx
import structlog

from app.config import settings

logger = structlog.get_logger(__name__)

ONOFFICE_API_URL = "https://api.onoffice.de/api/stable/api.php"

_ACTION_CREATE = "urn:onoffice-de-ns:smart:2.5:smartml:action:create"
_RESOURCE_AGENTSLOG = "agentslog"


def _compute_hmac(timestamp: int, action_id: str, resource_type: str) -> str:
    """
    Compute the per-action HMAC signature for onOffice REST API v2.
    Message = timestamp + api_token + action_id + resource_type
    """
    message = f"{timestamp}{settings.onoffice_api_key}{action_id}{resource_type}"
    raw = _hmac.new(
        settings.onoffice_api_secret.encode(),
        message.encode(),
        hashlib.sha256,
    ).digest()
    return base64.b64encode(raw).decode()


def _format_note(payload: dict[str, Any]) -> str:
    """Render the match details as a plain-text activity note."""
    party = payload.get("party", {})
    lead = payload.get("asset_lead", {})
    event = payload.get("insolvency_event", {})
    score = payload.get("score_total", 0)
    features = payload.get("features", {})

    lines = [
        f"EarlyEstate Match BESTÄTIGT — Score {score:.0f}/100",
        "",
        f"Partei:        {party.get('canonical_name', '?')} ({party.get('party_type', '?')})",
        f"Register:      {party.get('register_id') or '—'}",
        f"Rechtsstatus:  Insolvenz",
        "",
        f"Aktenzeichen:  {event.get('case_number', '?')}",
        f"Gericht:       {event.get('court', '?')}",
        f"Veröffentl.:   {event.get('event_time', '?')}",
        "",
        f"ZV-Objekt:     {lead.get('title', '?')}",
        f"Adresse:       {lead.get('address_raw', '?')}",
        f"Ort:           {lead.get('postal_code', '')} {lead.get('city', '?')}",
        f"Typ:           {lead.get('object_type', '?')}",
        f"Verkehrswert:  €{lead.get('verkehrswert_eur') or '—'}",
    ]
    if lead.get("bodenrichtwert_eur_m2"):
        lines.append(f"Bodenrichtwert:€{lead['bodenrichtwert_eur_m2']:,.0f}/m²  (BORIS)")
    lines += [
        f"ZV-Termin:     {lead.get('auction_date') or '—'}",
        f"Gericht (ZV):  {lead.get('court') or '—'}",
        "",
        f"Score-Details: name={features.get('name_similarity', 0):.0f} "
        f"geo={features.get('geo_score', 0):.0f} "
        f"auction={features.get('auction_signal_score', 0):.0f} "
        f"court={features.get('court_jurisdiction_score', 0):.0f} "
        f"register={features.get('register_id_match', 0):.0f}",
        "",
        f"URL:           {lead.get('details_url') or '—'}",
    ]
    return "\n".join(lines)


async def push_confirmed_match(payload: dict[str, Any]) -> bool:
    """
    Push a confirmed match to onOffice as an Agentslog activity note.

    Returns True on success, False if the API key is not configured or the
    request fails. Errors are logged but never raised — caller is unaffected.
    """
    if not (settings.onoffice_api_key and settings.onoffice_api_secret):
        logger.debug("onoffice.not_configured")
        return False

    timestamp = int(time.time())
    hmac_value = _compute_hmac(timestamp, _ACTION_CREATE, _RESOURCE_AGENTSLOG)
    note = _format_note(payload)

    body = {
        "token": settings.onoffice_api_key,
        "request": {
            "actions": [
                {
                    "actionid": _ACTION_CREATE,
                    "resourceid": "",
                    "identifier": "",
                    "resourcetype": _RESOURCE_AGENTSLOG,
                    "hmac": hmac_value,
                    "hmac_version": "2",
                    "cacheable": False,
                    "parameters": {
                        "actiontype": "Notiz",
                        "note": note,
                        "created": time.strftime("%Y-%m-%d"),
                        "timestamp": timestamp,
                    },
                }
            ]
        },
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resp = await client.post(ONOFFICE_API_URL, json=body)
            resp.raise_for_status()
            data = resp.json()

            results = data.get("response", {}).get("results", [{}])
            error_code = results[0].get("status", {}).get("errorcode", -1) if results else -1

            if error_code == 0:
                party_name = payload.get("party", {}).get("canonical_name", "?")
                logger.info("onoffice.activity_created", party=party_name)
                return True
            else:
                logger.warning("onoffice.api_error", errorcode=error_code, response=data)
                return False

        except Exception as exc:
            logger.error("onoffice.push_error", error=str(exc))
            return False
