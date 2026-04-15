import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, LargeBinary, String, Text, func, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class RawDocument(Base):
    """Raw fetch artifacts — HTML snapshots, response metadata."""

    __tablename__ = "raw_document"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    source_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("source.id"), nullable=False
    )
    retrieved_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    external_id: Mapped[str | None] = mapped_column(String, nullable=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    url_hash: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    content_type: Mapped[str | None] = mapped_column(String, nullable=True)
    storage_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Pointer to object store; NULL if metadata-only
    content_sha256: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    fingerprint: Mapped[str] = mapped_column(String, nullable=False)
    parse_status: Mapped[str] = mapped_column(String, nullable=False, default="PENDING")
    # PENDING | PARSED | FAILED | SKIPPED
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        # Enforce idempotency per source + fingerprint
        {"schema": None},
    )
