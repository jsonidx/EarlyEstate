"""Initial schema — all tables, PostGIS extensions, indexes, constraints.

Revision ID: 001
Revises:
Create Date: 2026-04-15
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── Extensions ────────────────────────────────────────────────────────────
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    op.execute("CREATE EXTENSION IF NOT EXISTS citext")

    # ── source ────────────────────────────────────────────────────────────────
    op.create_table(
        "source",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("source_key", sa.Text(), nullable=False),
        sa.Column("source_type", sa.Text(), nullable=False),
        sa.Column("base_url", sa.Text(), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("robots_policy", sa.Text(), nullable=False, server_default="'UNKNOWN'"),
        sa.Column("tos_reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.UniqueConstraint("source_key"),
    )

    # ── raw_document ──────────────────────────────────────────────────────────
    op.create_table(
        "raw_document",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("source_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("retrieved_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("external_id", sa.Text(), nullable=True),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("url_hash", sa.LargeBinary(), nullable=False),
        sa.Column("http_status", sa.Integer(), nullable=True),
        sa.Column("content_type", sa.Text(), nullable=True),
        sa.Column("storage_ref", sa.Text(), nullable=True),
        sa.Column("content_sha256", sa.LargeBinary(), nullable=True),
        sa.Column("fingerprint", sa.Text(), nullable=False),
        sa.Column("parse_status", sa.Text(), nullable=False, server_default="'PENDING'"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["source_id"], ["source.id"]),
        sa.UniqueConstraint("source_id", "fingerprint"),
    )
    op.create_index("raw_document_source_id_idx", "raw_document", ["source_id"])
    op.create_index("raw_document_parse_status_idx", "raw_document", ["parse_status"])

    # ── party ─────────────────────────────────────────────────────────────────
    op.create_table(
        "party",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("party_type", sa.Text(), nullable=False),
        sa.Column("canonical_name", sa.Text(), nullable=False),
        sa.Column("name_raw", sa.Text(), nullable=True),
        sa.Column("legal_form", sa.Text(), nullable=True),
        sa.Column("register_id", sa.Text(), nullable=True),
        sa.Column("register_court", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
    )
    op.create_index("party_party_type_idx", "party", ["party_type"])
    op.create_index("party_canonical_name_idx", "party", ["canonical_name"])
    op.create_index("party_register_id_idx", "party", ["register_id"])

    # ── party_alias ───────────────────────────────────────────────────────────
    op.create_table(
        "party_alias",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("party_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("alias", sa.Text(), nullable=False),
        sa.Column("alias_norm", sa.Text(), nullable=False),
        sa.Column("source_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["party_id"], ["party.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["source_id"], ["source.id"]),
        sa.UniqueConstraint("party_id", "alias_norm"),
    )
    op.create_index("party_alias_party_id_idx", "party_alias", ["party_id"])

    # ── party_address ─────────────────────────────────────────────────────────
    op.create_table(
        "party_address",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("party_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("address_raw", sa.Text(), nullable=False),
        sa.Column("street", sa.Text(), nullable=True),
        sa.Column("house_no", sa.Text(), nullable=True),
        sa.Column("postal_code", sa.Text(), nullable=True),
        sa.Column("city", sa.Text(), nullable=True),
        sa.Column("state", sa.Text(), nullable=True),
        sa.Column("country", sa.Text(), nullable=False, server_default="'DE'"),
        sa.Column("geom", sa.Text(), nullable=True),  # WKT; PostGIS casts to geometry
        sa.Column("geocode_provider", sa.Text(), nullable=True),
        sa.Column("geocode_confidence", sa.Numeric(5, 2), nullable=True),
        sa.Column("google_place_id", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["party_id"], ["party.id"], ondelete="CASCADE"),
    )
    op.create_index("party_address_party_id_idx", "party_address", ["party_id"])
    # Real PostGIS geometry index — applied after geom is geometry type
    op.execute("""
        ALTER TABLE party_address
        ALTER COLUMN geom TYPE geometry(Point, 4326)
        USING ST_GeomFromText(geom, 4326)
    """)
    op.execute(
        "CREATE INDEX party_address_geom_gix ON party_address USING gist (geom)"
    )

    # ── asset_lead ────────────────────────────────────────────────────────────
    op.create_table(
        "asset_lead",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("source_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("listing_id", sa.Text(), nullable=True),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("address_raw", sa.Text(), nullable=True),
        sa.Column("postal_code", sa.Text(), nullable=True),
        sa.Column("city", sa.Text(), nullable=True),
        sa.Column("geom", sa.Text(), nullable=True),
        sa.Column("object_type", sa.Text(), nullable=True),
        sa.Column("asking_price_eur", sa.Numeric(), nullable=True),
        sa.Column("verkehrswert_eur", sa.Numeric(), nullable=True),
        sa.Column("auction_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("court", sa.Text(), nullable=True),
        sa.Column("details_url", sa.Text(), nullable=True),
        sa.Column("auction_signal_terms", postgresql.JSONB(), nullable=True),
        sa.Column("payload", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["source_id"], ["source.id"]),
    )
    op.create_index("asset_lead_source_id_idx", "asset_lead", ["source_id"])
    op.create_index("asset_lead_city_idx", "asset_lead", ["city"])
    op.create_index("asset_lead_postal_code_idx", "asset_lead", ["postal_code"])
    op.execute("""
        ALTER TABLE asset_lead
        ALTER COLUMN geom TYPE geometry(Point, 4326)
        USING ST_GeomFromText(geom, 4326)
    """)
    op.execute("CREATE INDEX asset_lead_geom_gix ON asset_lead USING gist (geom)")

    # ── event ─────────────────────────────────────────────────────────────────
    op.create_table(
        "event",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("source_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("external_id", sa.Text(), nullable=True),
        sa.Column("raw_document_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("party_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("asset_lead_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("payload", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("confidence", sa.Numeric(5, 2), nullable=False, server_default="0.0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["source_id"], ["source.id"]),
        sa.ForeignKeyConstraint(["raw_document_id"], ["raw_document.id"]),
        sa.ForeignKeyConstraint(["party_id"], ["party.id"]),
        sa.ForeignKeyConstraint(["asset_lead_id"], ["asset_lead.id"]),
    )
    op.create_index("event_source_id_idx", "event", ["source_id"])
    op.create_index("event_event_type_idx", "event", ["event_type"])
    op.create_index("event_event_time_idx", "event", ["event_time"])
    op.create_index("event_party_id_idx", "event", ["party_id"])
    op.create_index("event_asset_lead_id_idx", "event", ["asset_lead_id"])
    op.create_index("event_external_id_idx", "event", ["external_id"])

    # ── cadastral_parcel ──────────────────────────────────────────────────────
    op.create_table(
        "cadastral_parcel",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=True),
        sa.Column("parcel_id", sa.Text(), nullable=False),
        sa.Column("geom", sa.Text(), nullable=False),
        sa.Column("area_m2", sa.Numeric(), nullable=True),
        sa.Column("usage_code", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("provider", "parcel_id"),
    )
    op.execute("""
        ALTER TABLE cadastral_parcel
        ALTER COLUMN geom TYPE geometry(MultiPolygon, 4326)
        USING ST_GeomFromText(geom, 4326)
    """)
    op.execute(
        "CREATE INDEX cadastral_parcel_geom_gix ON cadastral_parcel USING gist (geom)"
    )

    # ── valuation ─────────────────────────────────────────────────────────────
    op.create_table(
        "valuation",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("asset_lead_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("value_point_eur", sa.Numeric(), nullable=True),
        sa.Column("value_low_eur", sa.Numeric(), nullable=True),
        sa.Column("value_high_eur", sa.Numeric(), nullable=True),
        sa.Column("currency", sa.Text(), nullable=False, server_default="'EUR'"),
        sa.Column("meta", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["asset_lead_id"], ["asset_lead.id"], ondelete="CASCADE"),
    )
    op.create_index("valuation_asset_lead_id_idx", "valuation", ["asset_lead_id"])

    # ── match_candidate ───────────────────────────────────────────────────────
    op.create_table(
        "match_candidate",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("party_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("asset_lead_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("score_total", sa.Numeric(5, 2), nullable=False),
        sa.Column("score_breakdown", postgresql.JSONB(), nullable=False,
                  server_default=sa.text("'{}'::jsonb")),
        sa.Column("status", sa.Text(), nullable=False, server_default="'OPEN'"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["party_id"], ["party.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["asset_lead_id"], ["asset_lead.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("party_id", "asset_lead_id"),
    )
    op.create_index("match_candidate_party_id_idx", "match_candidate", ["party_id"])
    op.create_index("match_candidate_asset_lead_id_idx", "match_candidate", ["asset_lead_id"])
    op.create_index("match_candidate_status_idx", "match_candidate", ["status"])
    op.create_index("match_candidate_score_idx", "match_candidate", ["score_total"])

    # ── alert ─────────────────────────────────────────────────────────────────
    op.create_table(
        "alert",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("match_candidate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("channel", sa.Text(), nullable=False),
        sa.Column("recipient", sa.Text(), nullable=False),
        sa.Column("dedup_key", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("delivery_rule", sa.Text(), nullable=False, server_default="'INSTANT'"),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default="'PENDING'"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["match_candidate_id"], ["match_candidate.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("channel", "recipient", "dedup_key"),
    )
    op.create_index("alert_match_candidate_id_idx", "alert", ["match_candidate_id"])
    op.create_index("alert_status_idx", "alert", ["status"])
    op.create_index("alert_dedup_key_idx", "alert", ["dedup_key"])

    # ── job ───────────────────────────────────────────────────────────────────
    op.create_table(
        "job",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("job_type", sa.Text(), nullable=False),
        sa.Column("source_key", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default="'PENDING'"),
        sa.Column("payload", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("result", postgresql.JSONB(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
    )
    op.create_index("job_status_scheduled_idx", "job", ["status", "scheduled_at"],
                    postgresql_where=sa.text("status IN ('PENDING', 'FAILED')"))
    op.create_index("job_source_key_idx", "job", ["source_key"])
    op.create_index("job_job_type_idx", "job", ["job_type"])


def downgrade() -> None:
    op.drop_table("job")
    op.drop_table("alert")
    op.drop_table("match_candidate")
    op.drop_table("valuation")
    op.drop_table("cadastral_parcel")
    op.drop_table("event")
    op.drop_table("asset_lead")
    op.drop_table("party_address")
    op.drop_table("party_alias")
    op.drop_table("party")
    op.drop_table("raw_document")
    op.drop_table("source")
    op.execute("DROP EXTENSION IF EXISTS citext")
    op.execute("DROP EXTENSION IF EXISTS pgcrypto")
    op.execute("DROP EXTENSION IF EXISTS postgis")
