"""update port_zones schema

Revision ID: ff684d03b7ff
Revises: e0018ac8f802
Create Date: 2026-05-26 10:07:12.495776

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'ff684d03b7ff'
down_revision: Union[str, Sequence[str], None] = 'e0018ac8f802'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("DROP TABLE IF EXISTS port_zones CASCADE")
    op.execute("""
        CREATE TABLE port_zones (
            id              SERIAL PRIMARY KEY,
            terminal_name   VARCHAR(100) NOT NULL,
            zone_type       VARCHAR(20)  NOT NULL
                              CHECK (zone_type IN ('berth', 'anchorage')),
            sub_zone        SMALLINT     NOT NULL DEFAULT 0,
            country         CHAR(2)      NOT NULL,
            flow_direction  VARCHAR(10)  NOT NULL
                              CHECK (flow_direction IN ('export', 'import')),
            is_provisional  BOOLEAN      NOT NULL DEFAULT TRUE,
            source          VARCHAR(30),
            notes           TEXT,
            created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
            geom            geometry(MultiPolygon, 4326) NOT NULL,

            CONSTRAINT uq_terminal_zone
                UNIQUE (terminal_name, zone_type, sub_zone)
        )
    """)
    op.execute("CREATE INDEX idx_port_zones_geom ON port_zones USING GIST (geom)")
    op.execute("CREATE INDEX idx_port_zones_lookup ON port_zones (zone_type, flow_direction)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS port_zones CASCADE")
