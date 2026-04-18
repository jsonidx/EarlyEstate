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
    from app.models.source import Source
    from app.pipeline.entity_resolution import PartyCache
    from playwright.async_api import async_playwright
    from sqlalchemy import select
    import uuid

    # Pre-fetch source ID and load party cache ONCE for the entire run.
    # This eliminates a DB round-trip per item for both lookups.
    insolvency_src_id: str | None = None
    party_cache: PartyCache | None = None
    async with AsyncSessionLocal() as db:
        src_stmt = select(Source).where(Source.source_key == "insolvency_portal")
        src = (await db.execute(src_stmt)).scalar_one_or_none()
        if src:
            insolvency_src_id = str(src.id)
        party_cache = await PartyCache.load(db)

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
                        # Portal returned partial results (cap hit). Retry with a
                        # narrower window — today-only — to recover the most recent
                        # publications. The earlier-date results are accepted as-is.
                        from datetime import date as _date
                        today = _date.today()
                        if window.date_from < today:
                            from app.adapters.insolvency import InsolvencyDiscoverParams
                            narrow = InsolvencyDiscoverParams(
                                state=window.state,
                                date_from=today,
                                date_to=today,
                            )
                            try:
                                await adapter._fill_search_form(page, narrow)
                                _, narrow_rows = await adapter._extract_result_rows(page)
                                # Merge — deduplicate by external_id
                                seen_ids = {adapter.sha256_hex(
                                    f"{adapter._normalize_case_number(r['case_number'])}|"
                                    f"{r['court'].lower().strip()}"
                                )[:32] for r in rows}
                                for r in narrow_rows:
                                    eid = adapter.sha256_hex(
                                        f"{adapter._normalize_case_number(r['case_number'])}|"
                                        f"{r['court'].lower().strip()}"
                                    )[:32]
                                    if eid not in seen_ids:
                                        rows.append(r)
                                        seen_ids.add(eid)
                                logger.info(
                                    "insolvency.too_many_retry_ok",
                                    state=window.state,
                                    total_rows=len(rows),
                                )
                            except Exception as retry_exc:
                                logger.warning(
                                    "insolvency.too_many_retry_failed",
                                    state=window.state,
                                    error=str(retry_exc),
                                )
                        logger.warning(
                            "insolvency.too_many_results",
                            state=window.state,
                            date_from=str(window.date_from),
                            date_to=str(window.date_to),
                            partial_rows=len(rows),
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
                                        result = await _handle_fetch_inline(
                                            fetch_job, db,
                                            src_id=insolvency_src_id,
                                            party_cache=party_cache,
                                        )
                                        if result.get("status") == "ingested":
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

    if source_key not in ADAPTER_REGISTRY:
        logger.error("run_bank.unknown_source", source_key=source_key)
        return {"error": f"unknown source: {source_key}"}

    if source_key == "immowelt_zv":
        return await _run_immowelt()

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


async def _run_immowelt() -> dict:
    """
    Scrape Immowelt ZV listings across all four property types.
    Each property type = one search page request (~20 listings).
    """
    from app.adapters.immowelt import ImmoweltAdapter, PROPERTY_TYPES, ImmoweltDiscoverParams
    from app.database import AsyncSessionLocal

    adapter = ImmoweltAdapter()
    total_discovered = 0
    total_ingested = 0
    errors = 0
    source_key = "immowelt_zv"

    for prop_type in PROPERTY_TYPES:
        params = ImmoweltDiscoverParams(prop_type=prop_type)
        try:
            items = await adapter.discover(params)
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
                    logger.error("immowelt.item_error", prop_type=prop_type, error=str(exc))
                    errors += 1

            logger.info("immowelt.prop_type_done",
                        prop_type=prop_type,
                        discovered=len(items),
                        ingested=total_ingested)

        except Exception as exc:
            logger.error("immowelt.prop_type_error", prop_type=prop_type, error=str(exc))
            errors += 1

    return {
        "source": source_key,
        "discovered": total_discovered,
        "ingested": total_ingested,
        "errors": errors,
    }


async def wipe_match_candidates() -> dict:
    """Delete all match_candidate rows so a rescore starts from a clean slate."""
    from app.database import AsyncSessionLocal
    from sqlalchemy import text

    async with AsyncSessionLocal() as db:
        async with db.begin():
            result = await db.execute(text("DELETE FROM match_candidate"))
            deleted = result.rowcount
    return {"deleted_match_candidates": deleted}


async def run_match_and_alert(lookback_hours: int = 0) -> dict:
    """
    Run matching for parties, then send alerts.

    lookback_hours > 0 (incremental): per-party sessions, only recent parties.
    lookback_hours == 0 (full rescore): bulk mode — 4 queries total, in-memory scoring.
    """
    from datetime import datetime, timedelta, timezone

    from app.database import AsyncSessionLocal
    from app.models.asset_lead import AssetLead
    from app.models.party import Party
    from app.pipeline.alerter import process_pending_alerts
    from sqlalchemy import select

    alerts_sent = 0

    async with AsyncSessionLocal() as db:
        stmt = select(AssetLead)
        result = await db.execute(stmt)
        all_leads = list(result.scalars().all())

    if lookback_hours > 0:
        candidates_total = await _run_incremental_match(all_leads, lookback_hours)
    else:
        candidates_total = await _run_bulk_match(all_leads)

    async with AsyncSessionLocal() as db:
        async with db.begin():
            alerts_sent = await process_pending_alerts(db)

    return {"candidates": candidates_total, "alerts_sent": alerts_sent}


async def _run_incremental_match(all_leads, lookback_hours: int) -> int:
    """Per-party sessions for incremental runs (small number of new parties)."""
    from datetime import datetime, timedelta, timezone

    from app.database import AsyncSessionLocal
    from app.models.party import Party
    from app.pipeline.matcher import run_matching_for_party
    from sqlalchemy import select
    import uuid as _uuid

    async with AsyncSessionLocal() as db:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        result = await db.execute(
            select(Party.id).where(Party.created_at >= cutoff)
        )
        party_ids: list[_uuid.UUID] = [row[0] for row in result.fetchall()]

    logger.info("match.scope", lookback_hours=lookback_hours, parties=len(party_ids),
                leads=len(all_leads))

    candidates_total = 0
    for party_id in party_ids:
        try:
            async with AsyncSessionLocal() as db:
                async with db.begin():
                    candidates = await run_matching_for_party(
                        db, party_id, preloaded_leads=all_leads
                    )
                    candidates_total += len(candidates)
        except Exception as exc:
            logger.error("match.party_error", party_id=str(party_id), error=str(exc))

    return candidates_total


async def _run_bulk_match(all_leads) -> int:
    """
    Full rescore in bulk — 4 DB queries total, all scoring in memory.

    Instead of 4 queries × N parties (36K+ round-trips for 9K parties),
    loads all parties, addresses, and events in one query each, then scores
    all party×lead combinations in-process and bulk-inserts results.
    """
    from datetime import datetime, timezone

    from app.database import AsyncSessionLocal
    from app.models.event import Event
    from app.models.match_candidate import MatchCandidate
    from app.models.party import Party, PartyAddress
    from app.pipeline.matcher import LOW_THRESHOLD, score_match, breakdown_to_dict as _breakdown_to_dict
    from sqlalchemy import select, text

    async with AsyncSessionLocal() as db:
        # 1. All parties
        parties = {
            p.id: p for p in (await db.execute(select(Party))).scalars().all()
        }
        # 2. First address per party
        addresses: dict = {}
        for pa in (await db.execute(select(PartyAddress))).scalars().all():
            if pa.party_id not in addresses:
                addresses[pa.party_id] = pa
        # 3. Latest insolvency event per party
        events: dict = {}
        for ev in (await db.execute(
            select(Event).where(Event.event_type == "INSOLVENCY_PUBLICATION")
        )).scalars().all():
            existing = events.get(ev.party_id)
            if existing is None or (ev.event_time and (
                existing.event_time is None or ev.event_time > existing.event_time
            )):
                events[ev.party_id] = ev

    logger.info(
        "match.scope", lookback_hours=0,
        parties=len(parties), leads=len(all_leads), events=len(events),
    )

    # Score all pairs in memory
    rows = []
    now = datetime.now(timezone.utc)
    for party in parties.values():
        pa = addresses.get(party.id)
        ev = events.get(party.id)
        court = ev.payload.get("court") if ev else None
        ins_date = ev.event_time if ev else None

        for lead in all_leads:
            bd = score_match(party, lead, pa,
                             insolvency_court=court, insolvency_date=ins_date)
            if bd.total >= LOW_THRESHOLD:
                rows.append({
                    "party_id": str(party.id),
                    "asset_lead_id": str(lead.id),
                    "score_total": bd.total,
                    "score_breakdown": _breakdown_to_dict(bd),
                })

    logger.info("match.scored", above_threshold=len(rows))

    if not rows:
        return 0

    # Bulk insert in batches of 500
    BATCH = 500
    async with AsyncSessionLocal() as db:
        for i in range(0, len(rows), BATCH):
            chunk = rows[i:i + BATCH]
            async with db.begin():
                db.add_all([
                    MatchCandidate(
                        party_id=r["party_id"],
                        asset_lead_id=r["asset_lead_id"],
                        score_total=r["score_total"],
                        score_breakdown=r["score_breakdown"],
                    )
                    for r in chunk
                ])

    return len(rows)


async def run_digest() -> dict:
    """Send the daily digest batch — top 10 pending DIGEST alerts as one Telegram message."""
    from app.database import AsyncSessionLocal
    from app.pipeline.alerter import send_digest_alerts

    async with AsyncSessionLocal() as db:
        async with db.begin():
            sent = await send_digest_alerts(db)
    return {"digest_sent": sent}


async def run_market_stats() -> dict:
    """
    Rebuild plz_market_stats from current asset_lead data.

    Uses PostgreSQL PERCENTILE_CONT for median/p25/p75.
    Requires sample_size >= 20 per PLZ+type before writing (cold-start guard).
    Rows with fewer than 5 samples are removed to avoid stale stats after purges.
    """
    from app.database import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        async with db.begin():
            from sqlalchemy import text

            # Remove stale rows where sample_size dropped too low after purges
            await db.execute(text("""
                DELETE FROM plz_market_stats WHERE sample_size < 5
            """))

            # Rebuild from current asset_lead data
            await db.execute(text("""
                INSERT INTO plz_market_stats
                    (plz, property_type, median_price_per_m2, p25_price_per_m2,
                     p75_price_per_m2, sample_size, last_updated)
                SELECT
                    plz,
                    CASE object_type
                        WHEN 'condo'      THEN 'apartment'
                        WHEN 'house'      THEN 'house'
                        WHEN 'land'       THEN 'land'
                        WHEN 'commercial' THEN 'commercial'
                        ELSE 'other'
                    END AS property_type,
                    PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY price_per_m2) AS median,
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price_per_m2) AS p25,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price_per_m2) AS p75,
                    COUNT(*) AS sample_size,
                    NOW() AS last_updated
                FROM (
                    SELECT
                        postal_code AS plz,
                        object_type,
                        asking_price_eur / living_area_m2 AS price_per_m2
                    FROM asset_lead
                    WHERE living_area_m2 IS NOT NULL
                      AND living_area_m2 >= 20
                      AND asking_price_eur > 10000
                      AND asking_price_eur / living_area_m2 BETWEEN 200 AND 15000
                      AND postal_code IS NOT NULL
                ) sub
                GROUP BY plz, property_type
                HAVING COUNT(*) >= 20
                ON CONFLICT (plz, property_type)
                DO UPDATE SET
                    median_price_per_m2 = EXCLUDED.median_price_per_m2,
                    p25_price_per_m2    = EXCLUDED.p25_price_per_m2,
                    p75_price_per_m2    = EXCLUDED.p75_price_per_m2,
                    sample_size         = EXCLUDED.sample_size,
                    last_updated        = NOW()
            """))

            result = await db.execute(text("SELECT COUNT(*) FROM plz_market_stats"))
            total_rows = result.scalar()

    return {"plz_market_stats_rows": total_rows}


async def run_seed_market_stats(pages_per_type: int = 20) -> dict:
    """
    Seed plz_market_stats by scraping regular Immowelt Kauf listings.

    Runs once to bootstrap PLZ medians before the self-populating pipeline
    has accumulated enough asset_lead rows per PLZ.
    """
    from app.database import AsyncSessionLocal
    from app.pipeline.market_seed import aggregate_to_stats, scrape_market_listings
    from sqlalchemy import text

    listings = await scrape_market_listings(pages_per_type=pages_per_type)
    rows = aggregate_to_stats(listings)
    logger.info("market_seed.aggregated", listings=len(listings), plz_rows=len(rows))

    async with AsyncSessionLocal() as db:
        async with db.begin():
            for row in rows:
                await db.execute(text("""
                    INSERT INTO plz_market_stats
                        (plz, property_type, median_price_per_m2, p25_price_per_m2,
                         p75_price_per_m2, sample_size, last_updated)
                    VALUES (:plz, :property_type, :median, :p25, :p75, :sample_size, NOW())
                    ON CONFLICT (plz, property_type)
                    DO UPDATE SET
                        median_price_per_m2 = EXCLUDED.median_price_per_m2,
                        p25_price_per_m2    = EXCLUDED.p25_price_per_m2,
                        p75_price_per_m2    = EXCLUDED.p75_price_per_m2,
                        sample_size         = EXCLUDED.sample_size,
                        last_updated        = NOW()
                """), row)

    return {"listings_scraped": len(listings), "plz_rows_upserted": len(rows)}


async def run_purge() -> dict:
    from app.database import AsyncSessionLocal
    from app.pipeline.auditor import expire_stale_matches, run_retention_purge

    async with AsyncSessionLocal() as db:
        async with db.begin():
            stats = await run_retention_purge(db)
            expired = await expire_stale_matches(db)
            stats["expired_matches"] = expired
    return stats


# ── Inline fetch handler (avoids job table round-trip in Actions) ─────────────

async def _handle_fetch_inline(job, db, src_id: str | None = None, party_cache=None):
    """
    Inline version of runner._handle_fetch — skips job table.

    src_id: pre-fetched Source.id string (avoids per-item DB lookup).
    party_cache: PartyCache instance for in-memory ER (avoids per-item table scan).
    """
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
    import uuid as _uuid

    # Use pre-fetched src_id if available, otherwise look it up
    if src_id is None:
        src_stmt = select(Source).where(Source.source_key == source_key)
        src = (await db.execute(src_stmt)).scalar_one_or_none()
        if not src:
            return {"status": "error", "reason": "source not found"}
        src_id = str(src.id)
    src_uuid = _uuid.UUID(src_id)

    dup_stmt = select(RawDocument).where(
        RawDocument.source_id == src_uuid,
        RawDocument.fingerprint == fingerprint,
    )
    if (await db.execute(dup_stmt)).scalar_one_or_none():
        return {"status": "duplicate"}

    raw_doc = RawDocument(
        source_id=src_uuid,
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
        return await _ingest_insolvency(db, parsed_dict, str(raw_doc.id),
                                        src_id=src_id, party_cache=party_cache,
                                        skip_enqueue=True)
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

    elif args.source in ("immowelt_zv", "sparkasse_immobilien", "lbs_immobilien"):
        result = await run_bank_portal(args.source, pages=args.pages)

    elif args.job_type == "alert" or args.source == "match_alert":
        if args.wipe_matches:
            wipe_result = await wipe_match_candidates()
            logger.info("run_once.wiped_matches", **wipe_result)
        result = await run_match_and_alert(lookback_hours=args.lookback_hours)

    elif args.job_type == "digest":
        result = await run_digest()

    elif args.job_type == "market_stats":
        result = await run_market_stats()

    elif args.job_type == "seed_market_stats":
        result = await run_seed_market_stats(pages_per_type=args.pages)

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
                        choices=["insolvency_portal", "immowelt_zv",
                                 "sparkasse_immobilien", "lbs_immobilien", "match_alert"],
                        help="Source to scrape")
    parser.add_argument("--job-type", default=None,
                        choices=["alert", "digest", "purge", "market_stats", "seed_market_stats"],
                        help="Pipeline job type to run")
    parser.add_argument("--migrate", action="store_true",
                        help="Run DB migrations and exit")
    parser.add_argument("--seed", action="store_true",
                        help="Seed source records and exit")
    parser.add_argument("--lookback-hours", type=int, default=2,
                        help="How many hours back to scan (insolvency)")
    parser.add_argument("--pages", type=int, default=5,
                        help="How many listing pages to scrape (bank portals)")
    parser.add_argument("--wipe-matches", action="store_true",
                        help="Delete all match_candidate rows before rescoring")

    args = parser.parse_args()
    sys.exit(asyncio.run(main(args)))
