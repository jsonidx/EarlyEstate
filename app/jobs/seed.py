"""Seed script: inserts source records into DB. Run with: python -m app.jobs.seed"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import select

from app.database import AsyncSessionLocal
from app.models.source import Source

SOURCES = [
    {
        "source_key": "insolvency_portal",
        "source_type": "INSOLVENCY_PORTAL",
        "base_url": "https://neu.insolvenzbekanntmachungen.de",
        "enabled": True,
        "robots_policy": "ALLOWLIST",
    },
    {
        "source_key": "immowelt_zv",
        "source_type": "BANK_PORTAL",
        "base_url": "https://www.immowelt.de",
        "enabled": True,
        "robots_policy": "ALLOWLIST",
    },
    {
        "source_key": "sparkasse_immobilien",
        "source_type": "BANK_PORTAL",
        "base_url": "https://www.sparkasse-immobilien.de",
        "enabled": False,  # URL unresolvable — placeholder
        "robots_policy": "ALLOWLIST",
    },
    {
        "source_key": "lbs_immobilien",
        "source_type": "BANK_PORTAL",
        "base_url": "https://www.lbs.de",
        "enabled": False,  # JS-rendered SPA — placeholder
        "robots_policy": "ALLOWLIST",
    },
    {
        "source_key": "zvg_portal",
        "source_type": "ZVG_PORTAL",
        "base_url": "https://www.zvg-portal.de",
        "enabled": False,  # Compliance gate — disabled until legal review
        "robots_policy": "BLOCKLIST",
    },
]


async def seed() -> None:
    async with AsyncSessionLocal() as db:
        for s in SOURCES:
            stmt = select(Source).where(Source.source_key == s["source_key"])
            result = await db.execute(stmt)
            existing = result.scalar_one_or_none()

            if existing:
                print(f"  skip (exists): {s['source_key']}")
                continue

            source = Source(**s)
            db.add(source)
            print(f"  seeded: {s['source_key']}")

        await db.commit()
    print("Seed complete.")


if __name__ == "__main__":
    asyncio.run(seed())
