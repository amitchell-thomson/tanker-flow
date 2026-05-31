"""add tier_promotions log

Append-only log of priority_watchlist tier promotions, so the TUI can show all
recent promotions (persisted) rather than only those observed since it started.
Two writers: pipeline/scoring.py (via='scoring', the periodic re-rank) and
ingestion/aisstream.py (via='inline', the instant in-zone promotion on a live
fix). vessel_name + zone are denormalised at write time so the panel never shows
'?' and can state where the vessel was seen.

Revision ID: f4b1c7e0a9d2
Revises: e2a4c8d1f0b3
Create Date: 2026-05-31

"""
from typing import Sequence, Union

from alembic import op


revision: str = "f4b1c7e0a9d2"
down_revision: Union[str, Sequence[str], None] = "e2a4c8d1f0b3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE tier_promotions (
            id            BIGSERIAL    PRIMARY KEY,
            promoted_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
            mmsi          BIGINT       NOT NULL,
            vessel_name   TEXT,
            old_tier      SMALLINT,
            new_tier      SMALLINT     NOT NULL,
            via           TEXT         NOT NULL CHECK (via IN ('scoring', 'inline')),
            reason        TEXT,
            zone          TEXT
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_tier_promotions_promoted_at "
        "ON tier_promotions (promoted_at DESC)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE tier_promotions")
