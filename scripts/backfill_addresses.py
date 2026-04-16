"""
Party address backfill.

Finds COMPANY parties with no PartyAddress and geocodes them using
seat_city from their most recent insolvency event.

Run via GitHub Actions (manual) or locally:
    DATABASE_URL=... GEOCODING_PROVIDER=nominatim python scripts/backfill_addresses.py

Options:
    --limit N     Max parties to backfill per run (default: 100, Nominatim rate)
    --dry-run     Print what would be geocoded without writing to DB
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import structlog

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ],
)

logger = structlog.get_logger()


async def backfill(limit: int, dry_run: bool) -> None:
    from sqlalchemy import select, text
    from app.database import AsyncSessionLocal
    from app.models.event import Event
    from app.models.party import Party, PartyAddress
    from app.pipeline.geocoder import geocode_address, result_to_wkt

    async with AsyncSessionLocal() as db:
        # Find COMPANY parties with no PartyAddress
        stmt = text("""
            SELECT p.id, p.canonical_name
            FROM party p
            LEFT JOIN party_address pa ON pa.party_id = p.id
            WHERE p.party_type = 'COMPANY'
              AND pa.id IS NULL
            LIMIT :limit
        """)
        result = await db.execute(stmt, {"limit": limit})
        parties = result.fetchall()

    logger.info("backfill.found", count=len(parties), limit=limit, dry_run=dry_run)

    geocoded = 0
    skipped = 0
    errors = 0

    for party_id, canonical_name in parties:
        # Look up seat_city from most recent insolvency event
        async with AsyncSessionLocal() as db:
            evt_stmt = (
                select(Event)
                .where(
                    Event.party_id == party_id,
                    Event.event_type == "INSOLVENCY_PUBLICATION",
                )
                .order_by(Event.event_time.desc())
                .limit(1)
            )
            result = await db.execute(evt_stmt)
            event = result.scalar_one_or_none()

        if not event:
            skipped += 1
            continue

        seat_city = event.payload.get("seat_city")
        if not seat_city:
            skipped += 1
            continue

        logger.info("backfill.geocoding", party=canonical_name, city=seat_city)

        if dry_run:
            geocoded += 1
            continue

        try:
            geo = await geocode_address(f"{seat_city}, Germany")
            if not geo:
                logger.warning("backfill.no_geocode", party=canonical_name, city=seat_city)
                skipped += 1
                continue

            async with AsyncSessionLocal() as db:
                async with db.begin():
                    addr = PartyAddress(
                        party_id=party_id,
                        address_raw=seat_city,
                        city=seat_city,
                        country="DE",
                        geom=result_to_wkt(geo),
                        geocode_provider=geo.provider,
                        geocode_confidence=geo.confidence,
                        google_place_id=geo.place_id,
                    )
                    db.add(addr)

            geocoded += 1
            logger.info("backfill.geocoded", party=canonical_name, city=seat_city,
                        lat=geo.lat, lon=geo.lon)

        except Exception as exc:
            logger.error("backfill.error", party=canonical_name, error=str(exc))
            errors += 1

    logger.info("backfill.done", geocoded=geocoded, skipped=skipped, errors=errors)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill party addresses from seat_city")
    parser.add_argument("--limit", type=int, default=100,
                        help="Max parties to process (Nominatim: ≤1 req/s → ~100/run)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be geocoded without writing")
    args = parser.parse_args()
    asyncio.run(backfill(limit=args.limit, dry_run=args.dry_run))
