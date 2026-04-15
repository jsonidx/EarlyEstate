import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Numeric, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class MatchCandidate(Base):
    """Scored candidate links between a party (debtor) and an asset lead."""

    __tablename__ = "match_candidate"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    party_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("party.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    asset_lead_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("asset_lead.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    score_total: Mapped[float] = mapped_column(Numeric(5, 2), nullable=False)
    score_breakdown: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    # {name_similarity, address_similarity, geo_distance_m, auction_signal_terms_found,
    #  register_id_match, source_trust_weight}
    status: Mapped[str] = mapped_column(Text, nullable=False, default="OPEN", index=True)
    # OPEN | CONFIRMED | REJECTED
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
