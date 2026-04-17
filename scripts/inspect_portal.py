"""
Diagnostic script: inspect the insolvency portal's search form structure.
Dumps form element names/IDs to stdout so we can fix the scraper selectors.

Usage:
  python scripts/inspect_portal.py
"""
import asyncio
import json
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

        print("=== Navigating to main page ===")
        await page.goto(BASE_URL, wait_until="networkidle")
        print(f"Title: {await page.title()}")

        print("\n=== Navigating to search form ===")
        await page.goto(f"{BASE_URL}/ap/suche.jsf", wait_until="networkidle")
        print(f"Title: {await page.title()}")
        print(f"URL: {page.url}")

        # Dump all form inputs
        print("\n=== Form inputs ===")
        inputs = await page.query_selector_all("input, select, textarea, button")
        for el in inputs:
            tag = await el.evaluate("e => e.tagName.toLowerCase()")
            name = await el.get_attribute("name") or ""
            id_ = await el.get_attribute("id") or ""
            type_ = await el.get_attribute("type") or ""
            value = await el.get_attribute("value") or ""
            print(f"  <{tag}> name={name!r:40} id={id_!r:40} type={type_!r:10} value={value[:30]!r}")

        # Dump select options for Bundesland
        print("\n=== Select options (first 3 selects) ===")
        selects = await page.query_selector_all("select")
        for i, sel in enumerate(selects[:3]):
            name = await sel.get_attribute("name") or await sel.get_attribute("id") or f"select_{i}"
            options = await sel.query_selector_all("option")
            print(f"\n  Select [{name}]:")
            for opt in options[:20]:
                val = await opt.get_attribute("value") or ""
                text = (await opt.inner_text()).strip()
                print(f"    value={val!r:30} text={text!r}")

        # Dump first 3000 chars of form HTML
        print("\n=== Form HTML (first 3000 chars) ===")
        form = await page.query_selector("form")
        if form:
            html = await form.inner_html()
            print(html[:3000])
        else:
            print("No <form> found — dumping full body:")
            body = await page.content()
            print(body[:3000])

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
