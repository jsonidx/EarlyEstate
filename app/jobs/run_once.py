"""
Single-pass pipeline runner — called by GitHub Actions.

Usage:
  python -m app.jobs.run_once --source insolvency_portal
  python -m app.jobs.run_once --source sparkasse_immobilien
  python -m app.jobs.run_once --source lbs_immobilien
  python -m app.jobs.run_once --job-type alert
  python -m app.jobs.run_once --job-type purge
  python -m app.jobs.run_once --migrate          # run DB migrations then exit
  python -m app.jobs.run_once --seed             # seed source records then exit

Exits 0 on success, 1 on error. Structured JSON logs to stdout.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

# Ensure project root on path when run as script
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import structlog

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    logger_factory=structlog.PrintLoggerFactory(),
)

logger = structlog.get_logger(__name__)


def run_migrations() -> None:
    """Run alembic upgrade head synchronously."""
    import subprocess
    result = subprocess.run(
        ["alembic", "upgrade", "head"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.error("migrate.failed", stderr=result.stderr)
        sys.exit(1)
    logger.info("migrate.ok", stdout=result.stdout.strip())


async def run_seed() -> None:
    from app.jobs.seed import seed
    await seed()


async def run_insolvency(lookback_hours: int = 2) -> dict:
    """
    Discover + ingest insolvency publications for the last N hours.

    Optimised for GitHub Actions:
    - One Playwright browser shared across all 16 states (avoids 16× launch overhead).
    - One DB session per state (avoids per-item connection setup cost).
    - Geocode cache warms up across states (Berlin benefits from Bayern's cached cities).
    """
    from app.adapters.insolvency import InsolvencyAdapter, FEDERAL_STATES
    from app.database import AsyncSessionLocal
    from app.models.job import Job
    from playwright.async_api import async_playwright
    import uuid

    windows = InsolvencyAdapter.build_discover_windows(
        states=FEDERAL_STATES,
        lookback_hours=lookback_hours,
        window_minutes=60,
    )

    total_discovered = 0
    total_ingested = 0
    errors = 0

    # Single browser for all 16 state searches
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (compatible; EarlyEstate/0.1; research use)",
            locale="de-DE",
        )
        adapter = InsolvencyAdapter(headless=True)

        for window in windows:
            logger.info(
                "insolvency.window",
                state=window.state,
                date_from=str(window.date_from),
                date_to=str(window.date_to),
            )
            try:
                # Reuse existing browser context — pass it into the adapter
                page = await context.new_page()
                page.set_default_timeout(30_000)
                try:
                    await adapter._fill_search_form(page, window)
                    too_many, rows = await adapter._extract_result_rows(page)
                    if too_many:
                        logger.warning(
                            "insolvency.too_many_results",
                            state=window.state,
                            date_from=str(window.date_from),
                            date_to=str(window.date_to),
                        )
                    items = adapter._rows_to_discover_items(rows, window)
                finally:
                    await page.close()

                total_discovered += len(items)
                logger.info(
                    "insolvency.discover",
                    state=window.state,
                    date_from=str(window.date_from),
                    date_to=str(window.date_to),
                    count=len(items),
                )

                # Single DB session per state — avoids per-item connection setup overhead.
                # Each item gets its own SAVEPOINT so a bad item doesn't roll back
                # the whole state's work.
                if items:
                    async with AsyncSessionLocal() as db:
                        async with db.begin():
                            for item in items:
                                try:
                                    async with db.begin_nested():
                                        fetch_job = Job(
                                            id=uuid.uuid4(),
                                            job_type="FETCH",
                                            source_key="insolvency_portal",
                                            payload={
                                                "external_id": item.external_id,
                                                "url": item.url,
                                                "hint": item.hint,
                                            },
                                        )
                                        result = await _handle_fetch_inline(fetch_job, db)
                                        if result.get("status") == "fetched":
                                            total_ingested += 1
                                except Exception as exc:
                                    logger.error("insolvency.item_error", error=str(exc))
                                    errors += 1

            except Exception as exc:
                logger.error("insolvency.window_error", state=window.state, error=str(exc))
                errors += 1

        await browser.close()

    return {
        "source": "insolvency_portal",
        "discovered": total_discovered,
        "ingested": total_ingested,
        "errors": errors,
    }


async def run_bank_portal(source_key: str, pages: int = 5) -> dict:
    """Discover + ingest bank portal listings."""
    from app.adapters import ADAPTER_REGISTRY
    from app.database import AsyncSessionLocal
    from app.models.job import Job
    import uuid

    if source_key not in ADAPTER_REGISTRY:
        logger.error("run_bank.unknown_source", source_key=source_key)
        return {"error": f"unknown source: {source_key}"}

    adapter = ADAPTER_REGISTRY[source_key]()
    total_discovered = 0
    total_ingested = 0
    errors = 0

    for page in range(1, pages + 1):
        try:
            if source_key == "sparkasse_immobilien":
                from app.adapters.sparkasse import SparkasseDiscoverParams
                params = SparkasseDiscoverParams(page=page)
            else:
                from app.adapters.lbs import LBSDiscoverParams
                params = LBSDiscoverParams(page=page)

            items = await adapter.discover(params)
            if not items:
                break
            total_discovered += len(items)

            for item in items:
                try:
                    detail = await adapter.fetch_detail(item.external_id, item.url, item.hint)
                    parsed = await adapter.parse(detail)

                    if not parsed.is_auction_listing:
                        continue

                    async with AsyncSessionLocal() as db:
                        async with db.begin():
                            result = await _ingest_parsed_lead(db, source_key, parsed)
                            if result.get("status") == "ingested":
                                total_ingested += 1

                except Exception as exc:
                    logger.error("bank.item_error", source=source_key, error=str(exc))
                    errors += 1

        except Exception as exc:
            logger.error("bank.page_error", source=source_key, page=page, error=str(exc))
            errors += 1
            break

    return {
        "source": source_key,
        "discovered": total_discovered,
        "ingested": total_ingested,
        "errors": errors,
    }


async def run_match_and_alert() -> dict:
    """Run matching for all unmatched parties, then send alerts."""
    from app.database import AsyncSessionLocal
    from app.models.party import Party
    from app.pipeline.alerter import process_pending_alerts
    from app.pipeline.matcher import run_matching_for_party
    from sqlalchemy import select

    candidates_total = 0
    alerts_sent = 0

    async with AsyncSessionLocal() as db:
        async with db.begin():
            stmt = select(Party).limit(500)
            result = await db.execute(stmt)
            parties = result.scalars().all()

            for party in parties:
                try:
                    candidates = await run_matching_for_party(db, party.id)
                    candidates_total += len(candidates)
                except Exception as exc:
                    logger.error("match.party_error", party_id=str(party.id), error=str(exc))

    async with AsyncSessionLocal() as db:
        async with db.begin():
            alerts_sent = await process_pending_alerts(db)

    return {"candidates": candidates_total, "alerts_sent": alerts_sent}


async def run_purge() -> dict:
    from app.database import AsyncSessionLocal
    from app.pipeline.auditor import run_retention_purge

    async with AsyncSessionLocal() as db:
        async with db.begin():
            stats = await run_retention_purge(db)
    return stats


# ── Inline fetch handler (avoids job table round-trip in Actions) ─────────────

async def _handle_fetch_inline(job, db):
    """Inline version of runner._handle_fetch — skips job table."""
    from app.adapters import ADAPTER_REGISTRY
    from app.jobs.runner import _ingest_insolvency, _ingest_asset_lead
    import hashlib

    source_key = job.source_key
    payload = job.payload
    external_id = payload["external_id"]
    url = payload["url"]
    hint = payload.get("hint", {})

    adapter = ADAPTER_REGISTRY[source_key]()

    meta = adapter.compliance_meta()
    if meta.rate_limit_rps > 0:
        await asyncio.sleep(1.0 / meta.rate_limit_rps)

    detail = await adapter.fetch_detail(external_id, url, hint)
    parsed = await adapter.parse(detail)
    fingerprint = adapter.fingerprint(detail, parsed)

    from app.models.raw_document import RawDocument
    from app.models.source import Source
    from sqlalchemy import select

    src_stmt = select(Source).where(Source.source_key == source_key)
    src = (await db.execute(src_stmt)).scalar_one_or_none()
    if not src:
        return {"status": "error", "reason": "source not found"}

    dup_stmt = select(RawDocument).where(
        RawDocument.source_id == src.id,
        RawDocument.fingerprint == fingerprint,
    )
    if (await db.execute(dup_stmt)).scalar_one_or_none():
        return {"status": "duplicate"}

    raw_doc = RawDocument(
        source_id=src.id,
        external_id=external_id,
        url=url,
        url_hash=hashlib.sha256(url.encode()).digest(),
        fingerprint=fingerprint,
        parse_status="PENDING",
    )
    db.add(raw_doc)
    await db.flush()

    import dataclasses
    parsed_dict = dataclasses.asdict(parsed) if dataclasses.is_dataclass(parsed) else {}

    if source_key == "insolvency_portal":
        return await _ingest_insolvency(db, parsed_dict, str(raw_doc.id))
    else:
        return await _ingest_asset_lead(db, source_key, parsed_dict, str(raw_doc.id))


async def _ingest_parsed_lead(db, source_key: str, parsed) -> dict:
    """Ingest a parsed bank portal lead directly (no job table)."""
    import dataclasses
    from app.jobs.runner import _ingest_asset_lead
    parsed_dict = dataclasses.asdict(parsed) if dataclasses.is_dataclass(parsed) else {}
    return await _ingest_asset_lead(db, source_key, parsed_dict, None)


# ── CLI ───────────────────────────────────────────────────────────────────────

async def main(args: argparse.Namespace) -> int:
    start = time.monotonic()

    if args.migrate:
        run_migrations()
        return 0

    if args.seed:
        await run_seed()
        return 0

    result: dict = {}

    if args.source == "insolvency_portal":
        result = await run_insolvency(lookback_hours=args.lookback_hours)

    elif args.source in ("sparkasse_immobilien", "lbs_immobilien"):
        result = await run_bank_portal(args.source, pages=args.pages)

    elif args.job_type == "alert" or args.source == "match_alert":
        result = await run_match_and_alert()

    elif args.job_type == "purge":
        result = await run_purge()

    else:
        logger.error("run_once.unknown_args", source=args.source, job_type=args.job_type)
        return 1

    duration_ms = int((time.monotonic() - start) * 1000)
    logger.info("run_once.done", duration_ms=duration_ms, **result)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EarlyEstate single-pass pipeline runner")
    parser.add_argument("--source", default=None,
                        choices=["insolvency_portal", "sparkasse_immobilien",
                                 "lbs_immobilien", "match_alert"],
                        help="Source to scrape")
    parser.add_argument("--job-type", default=None,
                        choices=["alert", "purge"],
                        help="Pipeline job type to run")
    parser.add_argument("--migrate", action="store_true",
                        help="Run DB migrations and exit")
    parser.add_argument("--seed", action="store_true",
                        help="Seed source records and exit")
    parser.add_argument("--lookback-hours", type=int, default=2,
                        help="How many hours back to scan (insolvency)")
    parser.add_argument("--pages", type=int, default=5,
                        help="How many listing pages to scrape (bank portals)")

    args = parser.parse_args()
    sys.exit(asyncio.run(main(args)))
