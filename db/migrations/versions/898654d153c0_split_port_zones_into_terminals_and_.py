"""split port_zones into terminals and port_zones

Revision ID: 898654d153c0
Revises: ff684d03b7ff
Create Date: 2026-05-26 10:16:07.132418

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = '898654d153c0'
down_revision: Union[str, Sequence[str], None] = 'ff684d03b7ff'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("DROP TABLE IF EXISTS port_zones CASCADE")
    op.execute("""
        CREATE TABLE terminals (
            terminal_id     SERIAL PRIMARY KEY,
            terminal_name   VARCHAR(100) NOT NULL UNIQUE,
            country         CHAR(2)      NOT NULL,
            flow_direction  VARCHAR(10)  NOT NULL
                              CHECK (flow_direction IN ('export','import')),
            in_signal_scope BOOLEAN      NOT NULL DEFAULT TRUE,
            is_fsru         BOOLEAN      NOT NULL DEFAULT FALSE,
            notes           TEXT
        )
    """)
    op.execute("""
        CREATE TABLE port_zones (
            id              SERIAL PRIMARY KEY,
            terminal_id     INTEGER      NOT NULL REFERENCES terminals(terminal_id),
            zone_type       VARCHAR(20)  NOT NULL
                              CHECK (zone_type IN ('berth','anchorage')),
            sub_zone        SMALLINT     NOT NULL DEFAULT 0,
            is_provisional  BOOLEAN      NOT NULL DEFAULT TRUE,
            source          VARCHAR(30),
            notes           TEXT,
            geom            geometry(MultiPolygon, 4326) NOT NULL,

            UNIQUE (terminal_id, zone_type, sub_zone)
        )
    """)
    op.execute("CREATE INDEX idx_port_zones_geom     ON port_zones USING GIST (geom)")
    op.execute("CREATE INDEX idx_port_zones_terminal ON port_zones (terminal_id)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS port_zones CASCADE")
    op.execute("DROP TABLE IF EXISTS terminals CASCADE")
