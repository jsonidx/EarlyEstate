"""
Postgres-based job queue runner.

Polls the `job` table for PENDING jobs, claims them (status → RUNNING),
executes the handler, and marks DONE or FAILED.

Job types → handlers:
  DISCOVER  → adapter.discover()
  FETCH     → adapter.fetch_detail()
  PARSE     → ingest pipeline (ER → geocode → enrich → match)
  MATCH     → matcher.run_matching_for_party()
  ALERT     → alerter.process_pending_alerts()
  PURGE     → auditor.run_retention_purge()
"""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import structlog
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.adapters import ADAPTER_REGISTRY
from app.database import AsyncSessionLocal
from app.models.job import Job

logger = structlog.get_logger(__name__)

POLL_INTERVAL_SECONDS = 5
MAX_CONCURRENT_JOBS = 4


async def claim_next_job(db: AsyncSession) -> Job | None:
    """
    Atomically claim the next PENDING job using SELECT FOR UPDATE SKIP LOCKED.
    Returns None if no jobs are available.
    """
    stmt = (
        select(Job)
        .where(
            Job.status == "PENDING",
            Job.scheduled_at <= datetime.now(timezone.utc),
            Job.attempts < Job.max_attempts,
        )
        .order_by(Job.scheduled_at)
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()

    if job:
        job.status = "RUNNING"
        job.started_at = datetime.now(timezone.utc)
        job.attempts += 1
        await db.flush()

    return job


async def execute_job(job: Job, db: AsyncSession) -> dict[str, Any]:
    """Dispatch a job to its handler. Returns result dict."""
    handlers = {
        "DISCOVER": _handle_discover,
        "FETCH": _handle_fetch,
        "PARSE": _handle_parse,
        "MATCH": _handle_match,
        "ALERT": _handle_alert,
        "PURGE": _handle_purge,
    }

    handler = handlers.get(job.job_type)
    if not handler:
        raise ValueError(f"Unknown job_type: {job.job_type}")

    return await handler(job, db)


async def _handle_discover(job: Job, db: AsyncSession) -> dict[str, Any]:
    source_key = job.source_key
    if not source_key or source_key not in ADAPTER_REGISTRY:
        raise ValueError(f"Unknown source_key for DISCOVER: {source_key}")

    adapter_cls = ADAPTER_REGISTRY[source_key]
    adapter = adapter_cls()

    compliance = adapter.compliance_meta()
    if not compliance.robots_respected:
        raise ValueError(f"Compliance gate: robots not respected for {source_key}")

    # Build params from job payload
    params = _build_discover_params(source_key, job.payload)
    items = await adapter.discover(params)

    # Enqueue FETCH jobs for each discovered item
    from app.pipeline.scheduler import enqueue_job
    enqueued = 0
    for item in items:
        await enqueue_job(
            db,
            job_type="FETCH",
            source_key=source_key,
            payload={
                "external_id": item.external_id,
                "url": item.url,
                "hint": item.hint,
            },
        )
        enqueued += 1

    return {"discovered": len(items), "enqueued": enqueued}


async def _handle_fetch(job: Job, db: AsyncSession) -> dict[str, Any]:
    source_key = job.source_key
    if not source_key or source_key not in ADAPTER_REGISTRY:
        raise ValueError(f"Unknown source_key for FETCH: {source_key}")

    adapter_cls = ADAPTER_REGISTRY[source_key]
    adapter = adapter_cls()

    payload = job.payload
    external_id = payload["external_id"]
    url = payload["url"]
    hint = payload.get("hint", {})

    # Rate limiting
    meta = adapter.compliance_meta()
    if meta.rate_limit_rps > 0:
        await asyncio.sleep(1.0 / meta.rate_limit_rps)

    detail = await adapter.fetch_detail(external_id, url, hint)
    parsed = await adapter.parse(detail)
    fingerprint = adapter.fingerprint(detail, parsed)

    # Check if already ingested (idempotency)
    from app.models.raw_document import RawDocument
    from app.models.source import Source
    from sqlalchemy import select as sa_select

    src_stmt = sa_select(Source).where(Source.source_key == source_key)
    src_result = await db.execute(src_stmt)
    source = src_result.scalar_one_or_none()

    if not source:
        raise ValueError(f"Source not found in DB: {source_key}")

    dup_stmt = sa_select(RawDocument).where(
        RawDocument.source_id == source.id,
        RawDocument.fingerprint == fingerprint,
    )
    dup_result = await db.execute(dup_stmt)
    existing_doc = dup_result.scalar_one_or_none()

    if existing_doc:
        return {"status": "duplicate", "fingerprint": fingerprint}

    # Store raw_document (metadata only per compliance)
    import hashlib
    raw_doc = RawDocument(
        source_id=source.id,
        external_id=external_id,
        url=url,
        url_hash=hashlib.sha256(url.encode()).digest(),
        http_status=200,
        fingerprint=fingerprint,
        parse_status="PENDING",
        storage_ref=None,  # Not storing raw HTML
    )
    db.add(raw_doc)
    await db.flush()

    # Enqueue PARSE job
    from app.pipeline.scheduler import enqueue_job
    await enqueue_job(
        db,
        job_type="PARSE",
        source_key=source_key,
        payload={
            "raw_document_id": str(raw_doc.id),
            "parsed_data": _serialize_parsed(parsed),
            "source_key": source_key,
        },
    )

    return {"status": "fetched", "raw_document_id": str(raw_doc.id)}


async def _handle_parse(job: Job, db: AsyncSession) -> dict[str, Any]:
    """
    Full ingest pipeline: ER → geocode → enrich → store event + party + asset_lead.
    Then enqueue MATCH job.
    """
    payload = job.payload
    source_key = payload.get("source_key", "")
    parsed_data = payload.get("parsed_data", {})
    raw_doc_id = payload.get("raw_document_id")

    if source_key == "insolvency_portal":
        return await _ingest_insolvency(db, parsed_data, raw_doc_id)
    elif source_key in ("sparkasse_immobilien", "lbs_immobilien"):
        return await _ingest_asset_lead(db, source_key, parsed_data, raw_doc_id)

    return {"status": "skipped", "reason": f"no parser for {source_key}"}


async def _ingest_insolvency(
    db: AsyncSession,
    data: dict,
    raw_doc_id: str | None,
    src_id: str | None = None,
    party_cache: "Any | None" = None,
    skip_enqueue: bool = False,
) -> dict[str, Any]:
    from app.models.event import Event
    from app.models.source import Source
    from app.pipeline.entity_resolution import ERInput, resolve_party
    from app.pipeline.geocoder import geocode_address, result_to_wkt
    from app.models.party import PartyAddress
    from datetime import datetime

    # Allow callers to pass a cached src_id to avoid a DB lookup per item
    if src_id is None:
        src_stmt = select(Source).where(Source.source_key == "insolvency_portal")
        src = (await db.execute(src_stmt)).scalar_one_or_none()
        if not src:
            return {"status": "error", "reason": "source not found"}
        src_id = str(src.id)
        src_uuid = src.id
    else:
        import uuid as _uuid
        src_uuid = _uuid.UUID(src_id)

    # Entity resolution — pass in-memory cache if available
    er_input = ERInput(
        name_raw=data.get("debtor_name", "Unknown"),
        party_type=_guess_party_type(data.get("debtor_name", "")),
        source_id=src_id,
    )
    er_result = await resolve_party(db, er_input, cache=party_cache)

    # Geocode debtor seat — only for companies, not natural persons.
    # Natural persons in insolvency don't typically hold commercial real estate,
    # and geocoding every consumer case would exhaust the Nominatim rate limit.
    seat_city = data.get("seat_city")
    party_type = er_input.party_type
    if seat_city and er_result.is_new and party_type == "COMPANY":
        geo = await geocode_address(f"{seat_city}, Germany")
        if geo:
            addr = PartyAddress(
                party_id=uuid.UUID(er_result.party_id),
                address_raw=seat_city,
                city=seat_city,
                country="DE",
                geom=result_to_wkt(geo),
                geocode_provider=geo.provider,
                geocode_confidence=geo.confidence,
                google_place_id=geo.place_id,
            )
            db.add(addr)

    # Create event
    pub_date = data.get("publication_date")
    event = Event(
        source_id=src_uuid,
        event_type="INSOLVENCY_PUBLICATION",
        event_time=_parse_iso(pub_date),
        external_id=data.get("external_id"),
        party_id=uuid.UUID(er_result.party_id),
        raw_document_id=uuid.UUID(raw_doc_id) if raw_doc_id else None,
        payload={
            "case_number": data.get("case_number"),
            "case_number_norm": data.get("case_number_norm"),
            "court": data.get("court"),
            "state": data.get("state"),
            "publication_subject": data.get("publication_subject"),
            "seat_city": seat_city,
            "register_info": data.get("register_info"),
        },
        confidence=0.9,
    )
    db.add(event)
    await db.flush()

    # Enqueue MATCH job — skip during bulk GitHub Actions runs (match_alert step handles it)
    if not skip_enqueue:
        from app.pipeline.scheduler import enqueue_job
        await enqueue_job(
            db,
            job_type="MATCH",
            source_key="insolvency_portal",
            payload={"party_id": er_result.party_id},
        )

    return {
        "status": "ingested",
        "party_id": er_result.party_id,
        "is_new_party": er_result.is_new,
        "event_id": str(event.id),
    }


async def _ingest_asset_lead(
    db: AsyncSession, source_key: str, data: dict, raw_doc_id: str | None
) -> dict[str, Any]:
    from app.models.asset_lead import AssetLead
    from app.models.source import Source
    from app.pipeline.geocoder import geocode_address, result_to_wkt

    src_stmt = select(Source).where(Source.source_key == source_key)
    src = (await db.execute(src_stmt)).scalar_one_or_none()
    if not src:
        return {"status": "error", "reason": "source not found"}

    # Only ingest if auction signals present
    cues = data.get("auction_signal_terms", [])
    if not cues:
        return {"status": "skipped", "reason": "no auction signal terms"}

    address_raw = data.get("address_raw", "")
    city = data.get("city", "")
    geom_wkt = None

    if address_raw:
        geo = await geocode_address(f"{address_raw}, Germany")
        if geo:
            geom_wkt = result_to_wkt(geo)

    lead = AssetLead(
        source_id=src.id,
        listing_id=data.get("listing_id"),
        title=data.get("title"),
        address_raw=address_raw,
        postal_code=data.get("postal_code"),
        city=city,
        geom=geom_wkt,
        object_type=data.get("object_type"),
        asking_price_eur=data.get("asking_price_eur"),
        verkehrswert_eur=data.get("verkehrswert_eur"),
        auction_date=_parse_iso(data.get("auction_date")),
        court=data.get("court"),
        details_url=data.get("details_url"),
        auction_signal_terms=cues,
        payload=data,
    )
    db.add(lead)
    await db.flush()
    return {"status": "ingested", "asset_lead_id": str(lead.id)}


async def _handle_match(job: Job, db: AsyncSession) -> dict[str, Any]:
    party_id_str = job.payload.get("party_id")
    if not party_id_str:
        raise ValueError("MATCH job missing party_id")

    from app.pipeline.matcher import run_matching_for_party
    candidates = await run_matching_for_party(db, uuid.UUID(party_id_str))

    # Enqueue ALERT for candidates above threshold
    if candidates:
        from app.pipeline.scheduler import enqueue_job
        await enqueue_job(db, job_type="ALERT", payload={"triggered_by": party_id_str})

    return {"candidates": len(candidates)}


async def _handle_alert(job: Job, db: AsyncSession) -> dict[str, Any]:
    from app.pipeline.alerter import process_pending_alerts
    sent = await process_pending_alerts(db)
    return {"sent": sent}


async def _handle_purge(job: Job, db: AsyncSession) -> dict[str, Any]:
    from app.pipeline.auditor import run_retention_purge
    stats = await run_retention_purge(db)
    return stats


# ── Worker loop ───────────────────────────────────────────────────────────────

async def run_worker_loop() -> None:
    """Main worker loop — polls job table and processes jobs."""
    logger.info("worker.started", poll_interval=POLL_INTERVAL_SECONDS)

    while True:
        try:
            async with AsyncSessionLocal() as db:
                async with db.begin():
                    job = await claim_next_job(db)

                    if not job:
                        await asyncio.sleep(POLL_INTERVAL_SECONDS)
                        continue

                    start = time.monotonic()
                    logger.info("worker.executing", job_id=str(job.id), type=job.job_type)

                    try:
                        result = await execute_job(job, db)
                        job.status = "DONE"
                        job.result = result
                        job.completed_at = datetime.now(timezone.utc)
                        duration_ms = int((time.monotonic() - start) * 1000)
                        logger.info(
                            "worker.done",
                            job_id=str(job.id),
                            type=job.job_type,
                            duration_ms=duration_ms,
                            result=result,
                        )

                    except Exception as exc:
                        job.status = "FAILED"
                        job.error_message = str(exc)
                        job.completed_at = datetime.now(timezone.utc)
                        logger.error(
                            "worker.job_failed",
                            job_id=str(job.id),
                            type=job.job_type,
                            error=str(exc),
                        )

        except Exception as exc:
            logger.error("worker.loop_error", error=str(exc))
            await asyncio.sleep(POLL_INTERVAL_SECONDS)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _build_discover_params(source_key: str, payload: dict) -> Any:
    if source_key == "insolvency_portal":
        from app.adapters.insolvency import InsolvencyDiscoverParams
        from datetime import date
        return InsolvencyDiscoverParams(
            state=payload.get("state", "Bayern"),
            date_from=date.fromisoformat(payload["date_from"]),
            date_to=date.fromisoformat(payload["date_to"]),
        )
    elif source_key == "sparkasse_immobilien":
        from app.adapters.sparkasse import SparkasseDiscoverParams
        return SparkasseDiscoverParams(
            region=payload.get("region"),
            page=payload.get("page", 1),
        )
    elif source_key == "lbs_immobilien":
        from app.adapters.lbs import LBSDiscoverParams
        return LBSDiscoverParams(
            region=payload.get("region"),
            page=payload.get("page", 1),
        )
    raise ValueError(f"No discover params builder for {source_key}")


def _serialize_parsed(parsed: Any) -> dict:
    """Convert a parsed dataclass to a dict for job payload storage."""
    import dataclasses
    if dataclasses.is_dataclass(parsed):
        return dataclasses.asdict(parsed)
    return {}


def _guess_party_type(name: str) -> str:
    """Heuristic: if name contains legal form suffix → COMPANY, else UNKNOWN."""
    company_indicators = ["gmbh", " ag ", " ag)", " kg ", "ohg", " ug ", " ug)", "gmbh &", "s.a.", "s.p.a.", "e.k.", " se ", " se)"]
    name_lower = name.lower()
    for indicator in company_indicators:
        if indicator in name_lower:
            return "COMPANY"
    return "UNKNOWN"


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except (ValueError, TypeError):
        return None
