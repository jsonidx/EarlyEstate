"""Add living_area_m2 to asset_lead and create plz_market_stats table.

Revision ID: 002
Revises: 001
Create Date: 2026-04-17
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision: str = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "asset_lead",
        sa.Column("living_area_m2", sa.Numeric(10, 2), nullable=True),
    )

    op.create_table(
        "plz_market_stats",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("plz", sa.CHAR(5), nullable=False),
        sa.Column("property_type", sa.Text(), nullable=False),
        sa.Column("median_price_per_m2", sa.Numeric(10, 2), nullable=False),
        sa.Column("p25_price_per_m2", sa.Numeric(10, 2), nullable=True),
        sa.Column("p75_price_per_m2", sa.Numeric(10, 2), nullable=True),
        sa.Column("sample_size", sa.Integer(), nullable=False),
        sa.Column(
            "last_updated",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("plz", "property_type"),
    )
    op.create_index("plz_market_stats_plz_idx", "plz_market_stats", ["plz"])


def downgrade() -> None:
    op.drop_index("plz_market_stats_plz_idx", "plz_market_stats")
    op.drop_table("plz_market_stats")
    op.drop_column("asset_lead", "living_area_m2")
