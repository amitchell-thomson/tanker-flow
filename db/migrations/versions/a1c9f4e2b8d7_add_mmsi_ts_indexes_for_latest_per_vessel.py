"""add (mmsi, ts DESC) indexes for latest-per-vessel lookups

Speeds up the viz /api/vessels LATERAL query (and laden.py / density track
order) from a full-hypertable seq-scan + on-disk merge sort (seconds, ~130 MB
spill) to per-vessel index seeks. IF NOT EXISTS so it is a no-op where the
indexes were already created by hand.

Revision ID: a1c9f4e2b8d7
Revises: 19867dc74649
Create Date: 2026-05-30 12:00:00.000000

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a1c9f4e2b8d7"
down_revision: Union[str, Sequence[str], None] = "19867dc74649"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX IF NOT EXISTS ais_fixes_mmsi_fix_ts_idx "
        "ON ais_fixes (mmsi, fix_ts DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS vessel_state_mmsi_state_ts_idx "
        "ON vessel_state (mmsi, state_ts DESC)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS vessel_state_mmsi_state_ts_idx")
    op.execute("DROP INDEX IF EXISTS ais_fixes_mmsi_fix_ts_idx")
