import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Numeric, Text, func, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base

try:
    from geoalchemy2 import Geometry
    _GEO = True
except ImportError:
    _GEO = False


class Party(Base):
    """Debtors — companies or persons identified in distress signals."""

    __tablename__ = "party"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    party_type: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    # COMPANY | PERSON | UNKNOWN
    canonical_name: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    name_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    legal_form: Mapped[str | None] = mapped_column(Text, nullable=True)
    register_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    # HRB/HRA/VR/GnR + number
    register_court: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class PartyAlias(Base):
    """Alternate names for a party (raw name variants from different sources)."""

    __tablename__ = "party_alias"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    party_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("party.id", ondelete="CASCADE"), nullable=False, index=True
    )
    alias: Mapped[str] = mapped_column(Text, nullable=False)
    alias_norm: Mapped[str] = mapped_column(Text, nullable=False)
    source_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("source.id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class PartyAddress(Base):
    """Geocoded addresses linked to a party."""

    __tablename__ = "party_address"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    party_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("party.id", ondelete="CASCADE"), nullable=False, index=True
    )
    address_raw: Mapped[str] = mapped_column(Text, nullable=False)
    street: Mapped[str | None] = mapped_column(Text, nullable=True)
    house_no: Mapped[str | None] = mapped_column(Text, nullable=True)
    postal_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    city: Mapped[str | None] = mapped_column(Text, nullable=True)
    state: Mapped[str | None] = mapped_column(Text, nullable=True)
    country: Mapped[str] = mapped_column(Text, nullable=False, default="DE")
    geom = mapped_column(
        Geometry("POINT", srid=4326) if _GEO else Text,
        nullable=True,
    )
    geocode_provider: Mapped[str | None] = mapped_column(Text, nullable=True)
    # GOOGLE | NOMINATIM | MANUAL
    geocode_confidence: Mapped[float | None] = mapped_column(Numeric(5, 2), nullable=True)
    google_place_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
