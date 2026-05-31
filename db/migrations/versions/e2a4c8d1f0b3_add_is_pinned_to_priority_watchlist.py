"""add is_pinned to priority_watchlist

Open-leg pin: scoring.py sets is_pinned=TRUE for vessels with a recent open
laden leg (a laden `departed` with no later `zone_entry`). aisstream.py then
forces those MMSIs into the persistent subscription block regardless of tier,
so the new MMSI-filter scheme re-acquires the vessel on its European approach
instead of losing it to tier-decay (the phantom-open-leg failure, M1). See
docs/review-2026-05-31-pre-signal-audit.md.

Revision ID: e2a4c8d1f0b3
Revises: d1f3a7c0b2e9
Create Date: 2026-05-31

"""

from typing import Sequence, Union

from alembic import op


revision: str = "e2a4c8d1f0b3"
down_revision: Union[str, Sequence[str], None] = "d1f3a7c0b2e9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE priority_watchlist "
        "ADD COLUMN is_pinned BOOLEAN NOT NULL DEFAULT FALSE"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE priority_watchlist DROP COLUMN is_pinned")
