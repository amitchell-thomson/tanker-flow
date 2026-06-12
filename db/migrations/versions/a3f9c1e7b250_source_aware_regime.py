"""source-aware regime + port_events.source (historical backfill phase 1)

Adds `port_events.source` (event provenance) and redefines the generated
`port_events.regime` column to be **source-aware**: it tags *fidelity*, not just
calendar. NOAA backfill events ('noaa-ais') tag regime 'noaa', GFW direct rows
('gfw_voyages'/'gfw_events') tag 'gfw', and live state-machine events fall through
to the existing time split ('bbox' before the 2026-05-30 09:27 UTC cutover,
'mmsi_filter' after). Without this, NOAA backfill events — the cleanest source —
would land in 'bbox' (pre-cutover by time) and signal.py would lump them with the
throttled live block. See ingestion/historical/PLAN.md §3.4 and analysis/SIGNALS.md
§0.5. Also widens signal_daily.regime's CHECK to admit 'noaa'/'gfw'.

A generated column's expression cannot be ALTERed in place, so regime is dropped
and re-added (it is STORED, so it recomputes for every existing row — all of which
get source='state_machine' → the unchanged time split).

Revision ID: a3f9c1e7b250
Revises: c5e8a1f3d927
Create Date: 2026-06-12

"""
from typing import Sequence, Union

from alembic import op


revision: str = "a3f9c1e7b250"
down_revision: Union[str, Sequence[str], None] = "c5e8a1f3d927"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Event provenance. Existing rows are all live state-machine events.
    op.execute(
        """
        ALTER TABLE port_events
            ADD COLUMN source TEXT NOT NULL DEFAULT 'state_machine'
            CHECK (source IN ('state_machine','noaa-ais','gfw_voyages','gfw_events'))
        """
    )
    # 2. Redefine regime source-aware (drop + re-add: generated exprs can't ALTER).
    op.execute("ALTER TABLE port_events DROP COLUMN regime")
    op.execute(
        """
        ALTER TABLE port_events
            ADD COLUMN regime TEXT GENERATED ALWAYS AS (
                CASE
                    WHEN source = 'noaa-ais' THEN 'noaa'
                    WHEN source IN ('gfw_voyages','gfw_events') THEN 'gfw'
                    WHEN event_time < TIMESTAMPTZ '2026-05-30 09:27:00+00' THEN 'bbox'
                    ELSE 'mmsi_filter'
                END) STORED
        """
    )
    # 3. signal_daily.regime must admit the two new fidelity tags.
    op.execute("ALTER TABLE signal_daily DROP CONSTRAINT signal_daily_regime_check")
    op.execute(
        """
        ALTER TABLE signal_daily
            ADD CONSTRAINT signal_daily_regime_check
            CHECK (regime IN ('noaa','gfw','bbox','mmsi_filter','all'))
        """
    )


def downgrade() -> None:
    # Reverse order. Any 'noaa'/'gfw' signal_daily rows must be cleared first, and
    # any non-state_machine port_events rows dropped, or the old CHECKs reject them.
    op.execute("DELETE FROM signal_daily WHERE regime IN ('noaa','gfw')")
    op.execute("ALTER TABLE signal_daily DROP CONSTRAINT signal_daily_regime_check")
    op.execute(
        """
        ALTER TABLE signal_daily
            ADD CONSTRAINT signal_daily_regime_check
            CHECK (regime IN ('bbox','mmsi_filter','all'))
        """
    )
    op.execute("DELETE FROM port_events WHERE source <> 'state_machine'")
    op.execute("ALTER TABLE port_events DROP COLUMN regime")
    op.execute(
        """
        ALTER TABLE port_events
            ADD COLUMN regime TEXT GENERATED ALWAYS AS (
                CASE WHEN event_time < TIMESTAMPTZ '2026-05-30 09:27:00+00'
                     THEN 'bbox' ELSE 'mmsi_filter' END) STORED
        """
    )
    op.execute("ALTER TABLE port_events DROP COLUMN source")
