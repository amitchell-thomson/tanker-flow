"""add market_series (Part B control set + spread target)

Revision ID: b7c2e9f1a3d4
Revises: a1f2c3d4e5b6
Create Date: 2026-09-05

Non-EIA market/control series for the Part B spread model: TTF (the EU leg of
the HH-TTF target), EUR/USD, Brent, EU gas storage, degree days. Same
(series_id, period) key shape as eia_series so one assembler joins both onto
the signal_daily daily grid.
"""

from alembic import op
import sqlalchemy as sa

revision = "b7c2e9f1a3d4"
down_revision = "a1f2c3d4e5b6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "market_series",
        sa.Column("series_id", sa.Text(), nullable=False),
        sa.Column("period", sa.Date(), nullable=False),
        sa.Column("value", sa.Float(precision=53), nullable=True),
        sa.Column("unit", sa.Text(), nullable=False),
        sa.Column("frequency", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column(
            "fetched_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("series_id", "period"),
    )


def downgrade() -> None:
    op.drop_table("market_series")
