"""
Value screening — standalone property value signals independent of insolvency matching.

Two tiers:
  Tier 1: verkehrswert_discount — asking price < 60% of court-assessed value
  Tier 2: sqm_bargain — price/m² Wohnfläche < 75% of local PLZ median

Both tiers can fire independently. When both fire: confidence = HIGH.
Only fires on NEW listings (dedup handled upstream by listing_id idempotency).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from app.models.asset_lead import AssetLead
    from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)

# Tier 1: asking must be below this fraction of Verkehrswert to trigger.
# ZVG auctions typically start at 50-70% by law; 0.60 catches genuinely exceptional deals.
VERKEHRSWERT_DISCOUNT_THRESHOLD = 0.60

# Tier 2: price/m² must be below this fraction of the PLZ median to trigger.
SQM_BARGAIN_THRESHOLD = 0.75

# Minimum sample size in plz_market_stats before Tier 2 is trusted.
MIN_SAMPLE_SIZE = 20

# Minimum plausible living area — filters out typos / land-only listings.
MIN_LIVING_AREA_M2 = 20.0


@dataclass
class ValueSignal:
    confidence: str           # "HIGH" | "MEDIUM"
    verkehrswert_discount: bool
    sqm_bargain: bool
    discount_pct: float | None        # e.g. 35.0 means 35% below Verkehrswert
    price_per_m2: float | None
    median_price_per_m2: float | None
    sqm_discount_pct: float | None    # e.g. 28.0 means 28% below local median
    plz_sample_size: int | None


def parse_wohnflaeche(text: str) -> float | None:
    """Extract Wohnfläche in m² from listing text. Returns None if not found."""
    if not text:
        return None

    patterns = [
        # "84 m² Wohnfläche" / "84m² Wohnfläche"
        r'(\d{1,4}(?:[.,]\d{1,2})?)\s*m²?\s*(?:Wohn|Nutz|Wohnfläche)',
        # "Wohnfläche: 84" / "Wohnfläche ca. 84"
        r'Wohnfläche\s*(?:ca\.?)?\s*[:≈]?\s*(\d{1,4}(?:[.,]\d{1,2})?)',
        # "84 m²" anywhere (last resort — may match land area)
        r'(\d{2,4}(?:[.,]\d{1,2})?)\s*m²',
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            try:
                value = float(m.group(1).replace(",", "."))
                if MIN_LIVING_AREA_M2 <= value <= 2000:
                    return value
            except ValueError:
                continue
    return None


async def evaluate_value_signals(
    db: "AsyncSession",
    lead: "AssetLead",
) -> ValueSignal | None:
    """
    Evaluate both value tiers for a freshly ingested lead.
    Returns a ValueSignal if at least one tier fires, else None.
    """
    if not lead.asking_price_eur or lead.asking_price_eur <= 0:
        return None

    asking = float(lead.asking_price_eur)
    verkehrswert_discount = False
    discount_pct: float | None = None

    # Tier 1 — Verkehrswert discount
    if lead.verkehrswert_eur and float(lead.verkehrswert_eur) > 0:
        vw = float(lead.verkehrswert_eur)
        ratio = asking / vw
        if ratio < VERKEHRSWERT_DISCOUNT_THRESHOLD:
            verkehrswert_discount = True
            discount_pct = round((1 - ratio) * 100, 1)

    # Tier 2 — €/m² vs PLZ median
    sqm_bargain = False
    price_per_m2: float | None = None
    median_price_per_m2: float | None = None
    sqm_discount_pct: float | None = None
    plz_sample_size: int | None = None

    area = float(lead.living_area_m2) if lead.living_area_m2 else None
    if area and area >= MIN_LIVING_AREA_M2 and lead.postal_code:
        price_per_m2 = round(asking / area, 2)
        stats = await _load_plz_stats(db, lead.postal_code, lead.object_type or "other")
        if stats and stats.sample_size >= MIN_SAMPLE_SIZE:
            median = float(stats.median_price_per_m2)
            median_price_per_m2 = median
            plz_sample_size = stats.sample_size
            if price_per_m2 < median * SQM_BARGAIN_THRESHOLD:
                sqm_bargain = True
                sqm_discount_pct = round((1 - price_per_m2 / median) * 100, 1)

    if not verkehrswert_discount and not sqm_bargain:
        return None

    confidence = "HIGH" if (verkehrswert_discount and sqm_bargain) else "MEDIUM"

    logger.info(
        "value_screening.signal",
        lead_id=str(lead.id),
        confidence=confidence,
        verkehrswert_discount=verkehrswert_discount,
        sqm_bargain=sqm_bargain,
        discount_pct=discount_pct,
        sqm_discount_pct=sqm_discount_pct,
    )

    return ValueSignal(
        confidence=confidence,
        verkehrswert_discount=verkehrswert_discount,
        sqm_bargain=sqm_bargain,
        discount_pct=discount_pct,
        price_per_m2=price_per_m2,
        median_price_per_m2=median_price_per_m2,
        sqm_discount_pct=sqm_discount_pct,
        plz_sample_size=plz_sample_size,
    )


async def _load_plz_stats(db: "AsyncSession", plz: str, object_type: str):
    """Load PLZ market stats for this PLZ + normalised property type."""
    from sqlalchemy import select
    from app.models.asset_lead import PlzMarketStats

    # Normalise our object_type to stats table property_type
    prop_type = _normalise_property_type(object_type)

    stmt = select(PlzMarketStats).where(
        PlzMarketStats.plz == plz,
        PlzMarketStats.property_type == prop_type,
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


def _normalise_property_type(object_type: str) -> str:
    mapping = {
        "condo": "apartment",
        "house": "house",
        "land": "land",
        "commercial": "commercial",
    }
    return mapping.get(object_type, "other")


async def send_value_alert(lead: "AssetLead", signal: ValueSignal) -> None:
    """Send a standalone Telegram alert for a below-market property."""
    from app.config import settings

    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        return

    text = _format_value_alert(lead, signal)
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
        logger.info("value_alert.sent", lead_id=str(lead.id), confidence=signal.confidence)
    except Exception as exc:
        logger.error("value_alert.send_error", lead_id=str(lead.id), error=str(exc))


def _format_value_alert(lead: "AssetLead", signal: ValueSignal) -> str:
    badge = "🔴 HIGH VALUE" if signal.confidence == "HIGH" else "🟡 BELOW MARKET"
    lines = [
        f"{badge} — Standalone Property Signal",
        "",
        f"Title   : {lead.title or '?'}",
        f"Address : {lead.address_raw or '?'}",
        f"Type    : {lead.object_type or '?'}",
        f"Asking  : €{float(lead.asking_price_eur):,.0f}" if lead.asking_price_eur else "Asking  : ?",
    ]

    if lead.living_area_m2:
        lines.append(f"Area    : {float(lead.living_area_m2):,.0f} m² Wohnfläche")

    if signal.verkehrswert_discount and lead.verkehrswert_eur:
        lines.append(
            f"Verkehrswert: €{float(lead.verkehrswert_eur):,.0f}"
            f"  ({signal.discount_pct:.1f}% BELOW court value)"
        )

    if signal.sqm_bargain and signal.price_per_m2 and signal.median_price_per_m2:
        lines.append(
            f"€/m²    : €{signal.price_per_m2:,.0f}"
            f"  vs local median €{signal.median_price_per_m2:,.0f}"
            f"  ({signal.sqm_discount_pct:.1f}% below, n={signal.plz_sample_size})"
        )

    lines += [
        "",
        f"Signals : {'verkehrswert_discount ' if signal.verkehrswert_discount else ''}"
                  f"{'sqm_bargain' if signal.sqm_bargain else ''}".strip(),
        f"Confidence: {signal.confidence}",
    ]

    if lead.auction_date:
        lines.append(f"ZV Termin: {lead.auction_date}")
    if lead.court:
        lines.append(f"Court   : {lead.court}")
    if lead.details_url:
        lines.append(f"URL     : {lead.details_url}")

    return "\n".join(lines)
