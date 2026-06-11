"""add discovery_candidates (Phase-1 bbox unknown-tanker capture)

Under server-side MMSI filtering we only see vessels we subscribe to (by MMSI) or
hear on the bbox catch-all's terminal geofence. The catch-all previously dropped
any MMSI not on the in-scope-LNG allow-list, so an UNKNOWN tanker sitting at an
LNG terminal — almost certainly a carrier we never registered — was silently
discarded. Phase 1 captures those instead: the bbox connection now also subscribes
to ShipStaticData (carries the AIS ship type), and an unlisted MMSI with a tanker
type (80-89) is upserted here with its latest position. This is the measurement
substrate for the count ("how many tankers at LNG berths are we missing?") and the
foundation for the Phase-2 auto-add (registry insert + VF masterdata lookup). The
is-it-at-an-LNG-berth refinement is done offline (PostGIS against berth polygons),
so the table holds raw geofence sightings, not berth-confirmed candidates.

Revision ID: c5e8a1f3d927
Revises: b3f9d2a17c64
Create Date: 2026-06-11

"""

from typing import Sequence, Union

from alembic import op


revision: str = "c5e8a1f3d927"
down_revision: Union[str, Sequence[str], None] = "b3f9d2a17c64"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE discovery_candidates (
            mmsi        BIGINT       PRIMARY KEY,
            ais_type    SMALLINT,            -- AIS numeric type (80-89 = tanker)
            ship_name   TEXT,
            imo         BIGINT,
            lat         DOUBLE PRECISION,    -- latest position (for berth refine)
            lon         DOUBLE PRECISION,
            sog         DOUBLE PRECISION,    -- speed over ground (sitting ⇒ ~0)
            nav_status  SMALLINT,            -- AIS nav status (5 = moored, 1 = anchored)
            first_seen  TIMESTAMPTZ  NOT NULL,
            last_seen   TIMESTAMPTZ  NOT NULL,
            n_msgs      INTEGER      NOT NULL DEFAULT 1
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_discovery_candidates_last_seen "
        "ON discovery_candidates (last_seen DESC)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS discovery_candidates")
