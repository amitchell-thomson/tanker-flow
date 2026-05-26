"""add port_zones table

Revision ID: e0018ac8f802
Revises: c7f2e9a1b4d3
Create Date: 2026-05-26 09:11:58.316805

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'e0018ac8f802'
down_revision: Union[str, Sequence[str], None] = 'c7f2e9a1b4d3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE port_zones (
            id              SERIAL PRIMARY KEY,
            terminal_name   VARCHAR(100) NOT NULL,
            zone_type       VARCHAR(20)  NOT NULL CHECK (zone_type IN ('berth', 'anchorage')),
            country         CHAR(5)      NOT NULL,
            flow_direction  VARCHAR(10)  NOT NULL CHECK (flow_direction IN ('export', 'import')),
            notes           TEXT,
            geom            geometry(Polygon, 4326) NOT NULL
        )
    """)
    op.execute("CREATE INDEX idx_port_zones_geom ON port_zones USING GIST (geom)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS port_zones")
