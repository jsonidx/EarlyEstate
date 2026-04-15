"""Webhook alert channel — HMAC-signed JSON POST."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any

import httpx
import structlog

from app.alerts.base import AlertChannel
from app.config import settings

logger = structlog.get_logger(__name__)


class WebhookChannel(AlertChannel):
    """
    POST alert payload as JSON to a configured webhook URL.
    Signs the request with HMAC-SHA256 if WEBHOOK_SECRET is configured.

    Signature header: X-EarlyEstate-Signature: sha256=<hex>
    Timestamp header:  X-EarlyEstate-Timestamp: <unix_ts>
    """

    async def send(self, recipient: str, payload: dict[str, Any]) -> bool:
        url = settings.webhook_url or recipient
        if not url:
            logger.warning("webhook.no_url")
            return False

        try:
            body = json.dumps(payload, default=str).encode()
            timestamp = str(int(time.time()))
            headers = {
                "Content-Type": "application/json",
                "X-EarlyEstate-Timestamp": timestamp,
                "User-Agent": "EarlyEstate-Alerter/0.1",
            }

            if settings.webhook_secret:
                sig = _sign(settings.webhook_secret, timestamp, body)
                headers["X-EarlyEstate-Signature"] = f"sha256={sig}"

            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(url, content=body, headers=headers)
                resp.raise_for_status()

            logger.info("webhook.sent", url=url, status=resp.status_code)
            return True

        except Exception as exc:
            logger.error("webhook.send_error", url=url, error=str(exc))
            return False


def _sign(secret: str, timestamp: str, body: bytes) -> str:
    """HMAC-SHA256 signature: sha256(timestamp + "." + body)."""
    message = timestamp.encode() + b"." + body
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()
