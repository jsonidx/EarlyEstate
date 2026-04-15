import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Numeric, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base

try:
    from geoalchemy2 import Geometry
    _GEO = True
except ImportError:
    _GEO = False


class AssetLead(Base):
    """Real estate leads from bank portals and auction listings."""

    __tablename__ = "asset_lead"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    source_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("source.id"), nullable=False, index=True
    )
    listing_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    address_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    postal_code: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    city: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    geom = mapped_column(
        Geometry("POINT", srid=4326) if _GEO else Text,
        nullable=True,
    )
    object_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    # house | condo | land | commercial | other
    asking_price_eur: Mapped[float | None] = mapped_column(Numeric, nullable=True)
    verkehrswert_eur: Mapped[float | None] = mapped_column(Numeric, nullable=True)
    auction_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    court: Mapped[str | None] = mapped_column(Text, nullable=True)
    details_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    auction_signal_terms: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    # List of matched auction cue terms
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class CadastralParcel(Base):
    """ALKIS cadastral parcels normalized to PostGIS (without owner data in MVP)."""

    __tablename__ = "cadastral_parcel"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str | None] = mapped_column(Text, nullable=True)
    parcel_id: Mapped[str] = mapped_column(Text, nullable=False)
    geom = mapped_column(
        Geometry("MULTIPOLYGON", srid=4326) if _GEO else Text,
        nullable=False,
    )
    area_m2: Mapped[float | None] = mapped_column(Numeric, nullable=True)
    usage_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Valuation(Base):
    """Valuations from BORIS BRW, Sprengnetter AVM, or other providers."""

    __tablename__ = "valuation"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    asset_lead_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("asset_lead.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    # BORIS | SPRENGNETTER | OTHER
    value_point_eur: Mapped[float | None] = mapped_column(Numeric, nullable=True)
    value_low_eur: Mapped[float | None] = mapped_column(Numeric, nullable=True)
    value_high_eur: Mapped[float | None] = mapped_column(Numeric, nullable=True)
    currency: Mapped[str] = mapped_column(Text, nullable=False, default="EUR")
    meta: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
