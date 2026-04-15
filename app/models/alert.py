import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class Alert(Base):
    """Alerts emitted from match candidates, delivered via Telegram/Email/Webhook."""

    __tablename__ = "alert"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    match_candidate_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("match_candidate.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    channel: Mapped[str] = mapped_column(Text, nullable=False)
    # TELEGRAM | EMAIL | WEBHOOK
    recipient: Mapped[str] = mapped_column(Text, nullable=False)
    # chat_id / email address / endpoint key
    dedup_key: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    # sha256(party_id|asset_lead_id|scoring_bucket|floor(event_time to 24h))
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    delivery_rule: Mapped[str] = mapped_column(Text, nullable=False, default="INSTANT")
    # INSTANT | DIGEST
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="PENDING", index=True)
    # PENDING | SENT | FAILED | SKIPPED
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
