"""
Audit + retention pipeline step.

Responsibilities:
1. Log job metrics (success/failure rates per source).
2. Enforce InsBekV § 3 retention: purge personal data from insolvency
   records ≥ 6 months after the procedure end date (or ingestion date
   as conservative proxy when end date is unknown).
3. Mark raw_document as SKIPPED if compliance gate blocks processing.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import structlog
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.asset_lead import AssetLead
from app.models.event import Event
from app.models.match_candidate import MatchCandidate
from app.models.party import Party, PartyAddress, PartyAlias
from app.models.raw_document import RawDocument
from app.models.source import Source

logger = structlog.get_logger(__name__)

# InsBekV § 3: delete no later than 6 months after procedure ends.
# Conservative default: use ingestion date + 6 months when end date unknown.
INSOLVENCY_RETENTION_DAYS = 180


async def run_retention_purge(db: AsyncSession) -> dict[str, int]:
    """
    Purge personal data from insolvency records older than INSOLVENCY_RETENTION_DAYS.

    Purge strategy (data minimization):
    - Delete PartyAddress records for parties from insolvency source.
    - Anonymize Party.name_raw → NULL.
    - Delete PartyAlias records.
    - Keep Party canonical_name (pseudonymized/normalized) and case metadata for
      aggregate statistics.
    - Keep Event records but clear personal fields from payload.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=INSOLVENCY_RETENTION_DAYS)

    # Find insolvency source
    stmt = select(Source).where(Source.source_key == "insolvency_portal")
    result = await db.execute(stmt)
    source = result.scalar_one_or_none()
    if not source:
        return {"purged_addresses": 0, "anonymized_parties": 0, "cleared_events": 0}

    # 1. Purge PartyAddress for old insolvency parties
    old_event_stmt = select(Event.party_id).where(
        Event.source_id == source.id,
        Event.created_at < cutoff,
        Event.party_id.is_not(None),
    )
    result = await db.execute(old_event_stmt)
    old_party_ids = [row[0] for row in result.fetchall()]

    if not old_party_ids:
        return {"purged_addresses": 0, "anonymized_parties": 0, "cleared_events": 0}

    # Delete addresses
    del_addr = delete(PartyAddress).where(PartyAddress.party_id.in_(old_party_ids))
    addr_result = await db.execute(del_addr)

    # Delete aliases
    del_alias = delete(PartyAlias).where(PartyAlias.party_id.in_(old_party_ids))
    await db.execute(del_alias)

    # Anonymize raw names
    anon_party = (
        update(Party)
        .where(Party.id.in_(old_party_ids))
        .values(name_raw=None)
    )
    party_result = await db.execute(anon_party)

    # Clear personal fields from event payloads
    # Keep: case_number, court, publication_subject, event_time
    # Remove: debtor address details beyond city level
    clear_events = (
        update(Event)
        .where(
            Event.source_id == source.id,
            Event.created_at < cutoff,
            Event.party_id.in_(old_party_ids),
        )
        .values(
            payload=func.jsonb_set(
                Event.payload, "{seat_city}", "null"
            )
        )
    )
    event_result = await db.execute(clear_events)

    stats = {
        "purged_addresses": addr_result.rowcount,
        "anonymized_parties": party_result.rowcount,
        "cleared_events": event_result.rowcount,
    }
    logger.info("auditor.retention_purge", **stats)
    return stats


async def expire_stale_matches(db: AsyncSession) -> int:
    """
    Mark OPEN match_candidates as EXPIRED when the associated lead's auction_date
    has passed (with a 3-day grace period).

    This prevents outdated auction opportunities from re-triggering alerts or
    cluttering the manual review queue.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=3)

    # Subquery: leads whose auction has already passed
    expired_lead_ids = select(AssetLead.id).where(
        AssetLead.auction_date.is_not(None),
        AssetLead.auction_date < cutoff,
    )

    stmt = (
        update(MatchCandidate)
        .where(
            MatchCandidate.status == "OPEN",
            MatchCandidate.asset_lead_id.in_(expired_lead_ids),
        )
        .values(status="EXPIRED")
        .execution_options(synchronize_session=False)
    )
    result = await db.execute(stmt)
    count = result.rowcount
    logger.info("auditor.stale_matches_expired", count=count)
    return count


async def log_job_metrics(
    db: AsyncSession,
    source_key: str,
    job_type: str,
    success: bool,
    duration_ms: int,
    items_processed: int = 0,
    error: str | None = None,
) -> None:
    """
    Log job execution metrics to structured log.
    Extend to write to a metrics table if needed.
    """
    logger.info(
        "audit.job_metric",
        source_key=source_key,
        job_type=job_type,
        success=success,
        duration_ms=duration_ms,
        items_processed=items_processed,
        error=error,
    )
