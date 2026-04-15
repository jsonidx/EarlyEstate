from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from app.models.alert import Alert


async def dispatch_alert(alert: "Alert") -> bool:
    """Route an Alert record to the correct channel handler."""
    from app.alerts.email import EmailChannel
    from app.alerts.telegram import TelegramChannel
    from app.alerts.webhook import WebhookChannel

    channels = {
        "TELEGRAM": TelegramChannel(),
        "EMAIL": EmailChannel(),
        "WEBHOOK": WebhookChannel(),
    }
    handler = channels.get(alert.channel)
    if not handler:
        return False

    return await handler.send(alert.recipient, alert.payload)
