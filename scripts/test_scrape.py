"""
Smoke test: run the insolvency adapter for Saarland, today.
"""
import asyncio
import sys
from datetime import date

sys.path.insert(0, ".")

from app.adapters.insolvency import InsolvencyAdapter, InsolvencyDiscoverParams


async def main():
    adapter = InsolvencyAdapter(headless=True)
    params = InsolvencyDiscoverParams(
        state="Saarland",
        date_from=date(2026, 4, 14),
        date_to=date(2026, 4, 14),
    )
    print(f"Scraping Saarland 2026-04-14...")
    items = await adapter.discover(params)
    print(f"Found {len(items)} items")
    for item in items[:5]:
        print(f"  {item.external_id[:8]} | {item.hint.get('case_number')} | {item.hint.get('debtor_name')} | {item.hint.get('court')}")


if __name__ == "__main__":
    asyncio.run(main())
