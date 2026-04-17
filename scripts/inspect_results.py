"""
Diagnostic script: submit the search form and inspect the results table.
Uses a small state + 1-day window to stay under the 1000-hit cap.
"""
import asyncio
from playwright.async_api import async_playwright

BASE_URL = "https://neu.insolvenzbekanntmachungen.de"


async def main():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (compatible; EarlyEstate/0.1; research use)",
            locale="de-DE",
        )
        page = await context.new_page()
        page.set_default_timeout(30_000)

        await page.goto(f"{BASE_URL}/ap/suche.jsf", wait_until="networkidle")

        # Use Saarland (small state, value=11) with 1-day window
        await page.select_option("#frm_suche\\:lsom_bundesland\\:lsom", value="11")
        await page.fill("#frm_suche\\:ldi_datumVon\\:datumHtml5", "2026-04-14")
        await page.fill("#frm_suche\\:ldi_datumBis\\:datumHtml5", "2026-04-14")

        print("Submitting search form (Saarland, 2026-04-14)...")
        await page.click("#frm_suche\\:cbt_suchen")
        await page.wait_for_load_state("networkidle")

        print(f"Result URL: {page.url}")
        print(f"Title: {await page.title()}")

        # Try all possible result table selectors
        print("\n=== Selector counts ===")
        for sel in ["table tbody tr", "tbody tr", "table tr",
                    ".ergebnis tr", ".ergebnisliste tr",
                    "tr.ergebnis", ".listItem", ".result-row",
                    "ul li", ".publication-row"]:
            c = await page.locator(sel).count()
            if c > 0:
                print(f"  *** FOUND: {sel!r}: {c}")
            else:
                print(f"       miss: {sel!r}: {c}")

        # Check for pagination or result count text
        print("\n=== Page text snippets ===")
        content = await page.content()
        for keyword in ["Treffer", "Ergebnis", "keine", "Veröffentlichung",
                        "Insolvenz", "tbody", "table", "otx_"]:
            idx = content.find(keyword)
            if idx != -1:
                snippet = content[max(0, idx-30):idx+100].replace('\n', ' ')
                print(f"  [{keyword}]: ...{snippet}...")

        # Dump 8000 chars around the result area
        print("\n=== Full result section ===")
        # Find the Ergebnisliste section
        start = content.find("Ergebnisliste")
        if start != -1:
            print(content[start:start+8000])
        else:
            print("'Ergebnisliste' not found — dumping 3000 chars from body:")
            body_start = content.find("<body")
            print(content[body_start:body_start+3000] if body_start != -1 else content[:3000])

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
