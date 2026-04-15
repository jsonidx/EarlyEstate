"""Telegram alert channel."""

from __future__ import annotations

from typing import Any

import structlog

from app.alerts.base import AlertChannel, format_alert_text
from app.config import settings

logger = structlog.get_logger(__name__)


class TelegramChannel(AlertChannel):
    """Send alerts via Telegram Bot API."""

    async def send(self, recipient: str, payload: dict[str, Any]) -> bool:
        if not settings.telegram_bot_token:
            logger.warning("telegram.no_token")
            return False

        try:
            from telegram import Bot

            bot = Bot(token=settings.telegram_bot_token)
            text = format_alert_text(payload)

            # Telegram max message length = 4096 chars
            if len(text) > 4000:
                text = text[:4000] + "\n…"

            await bot.send_message(
                chat_id=recipient,
                text=f"<pre>{text}</pre>",
                parse_mode="HTML",
            )
            logger.info("telegram.sent", chat_id=recipient)
            return True

        except Exception as exc:
            logger.error("telegram.send_error", chat_id=recipient, error=str(exc))
            return False
