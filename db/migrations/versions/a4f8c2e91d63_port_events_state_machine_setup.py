"""port_events state machine setup

Adds:
- 'approach' as a valid terminal_zones.zone_type (macro envelope containing
  anchorage + channel + berth, drawn in QGIS).
- terminal_id, lat, lon, cold_start columns on port_events for finer
  granularity, downstream great-circle distance, and cold-start annotation.
- fsru_host_mmsi on terminals to declare the MMSI permanently moored at each
  FSRU import terminal (avoids inference from AIS, which is fragile during
  FSRU relocations).
- Seeds the four FSRU host mappings inferred from current ais_fixes density;
  remaining FSRU terminals are left NULL until enrichment confirms the host.

Revision ID: a4f8c2e91d63
Revises: 3f8a2c1d9e47
Create Date: 2026-05-28

"""

from typing import Sequence, Union

from alembic import op


revision: str = "a4f8c2e91d63"
down_revision: Union[str, Sequence[str], None] = "3f8a2c1d9e47"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# (terminal_name, fsru_host_mmsi) — confirmed via ais_fixes density inside
# terminal berth polygons. Other FSRU terminals (Klaipeda, Mukran, Piombino,
# Ravenna, Alexandroupolis, Krk, Lubmin II retired) are left NULL until the
# resident FSRU is observed and enriched.
FSRU_HOST_ASSIGNMENTS = [
    ("Eemshaven FSRU", 205157000),  # EEMSHAVEN LNG
    ("Brunsbuttel FSRU", 563071100),  # HOEGH GANNET
    ("Wilhelmshaven 1 FSRU", 257344000),  # HOEGH ESPERANZA
    ("Wilhelmshaven 2 FSRU", 205423000),  # EXCELSIOR
]


def upgrade() -> None:
    # terminal_zones.zone_type: add 'approach'.
    # The live constraint is named `port_zones_zone_type_check` (legacy from
    # before the table rename); we drop by name rather than relying on default.
    op.execute("ALTER TABLE terminal_zones DROP CONSTRAINT port_zones_zone_type_check")
    op.execute("""
        ALTER TABLE terminal_zones
        ADD CONSTRAINT terminal_zones_zone_type_check
        CHECK (zone_type IN ('berth', 'anchorage', 'approach'))
    """)

    # port_events extensions.
    op.execute("""
        ALTER TABLE port_events
            ADD COLUMN terminal_id INTEGER REFERENCES terminals(terminal_id),
            ADD COLUMN lat         REAL,
            ADD COLUMN lon         REAL,
            ADD COLUMN cold_start  BOOLEAN NOT NULL DEFAULT FALSE
    """)

    # Useful for the retroactive-reattribution pass and for the downstream
    # "open visits" query (moored with no later departed for the same mmsi).
    op.execute("CREATE INDEX ON port_events (terminal_id, event_time DESC)")

    # FSRU host declaration on terminals.
    op.execute("ALTER TABLE terminals ADD COLUMN fsru_host_mmsi BIGINT")
    for terminal_name, host_mmsi in FSRU_HOST_ASSIGNMENTS:
        op.execute(
            "UPDATE terminals SET fsru_host_mmsi = "
            f"{host_mmsi} WHERE terminal_name = '{terminal_name}'"
        )


def downgrade() -> None:
    op.execute("ALTER TABLE terminals DROP COLUMN fsru_host_mmsi")

    op.execute("DROP INDEX IF EXISTS port_events_terminal_id_event_time_idx")
    op.execute("""
        ALTER TABLE port_events
            DROP COLUMN cold_start,
            DROP COLUMN lon,
            DROP COLUMN lat,
            DROP COLUMN terminal_id
    """)

    op.execute(
        "ALTER TABLE terminal_zones DROP CONSTRAINT terminal_zones_zone_type_check"
    )
    op.execute("""
        ALTER TABLE terminal_zones
        ADD CONSTRAINT port_zones_zone_type_check
        CHECK (zone_type IN ('berth', 'anchorage'))
    """)
