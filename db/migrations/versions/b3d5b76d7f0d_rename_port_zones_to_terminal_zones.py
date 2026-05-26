"""rename port_zones to terminal_zones

Revision ID: b3d5b76d7f0d
Revises: 898654d153c0
Create Date: 2026-05-26 10:17:12.513230

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'b3d5b76d7f0d'
down_revision: Union[str, Sequence[str], None] = '898654d153c0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE port_zones RENAME TO terminal_zones")
    op.execute("ALTER INDEX idx_port_zones_geom     RENAME TO idx_terminal_zones_geom")
    op.execute("ALTER INDEX idx_port_zones_terminal RENAME TO idx_terminal_zones_terminal")


def downgrade() -> None:
    op.execute("ALTER TABLE terminal_zones RENAME TO port_zones")
    op.execute("ALTER INDEX idx_terminal_zones_geom     RENAME TO idx_port_zones_geom")
    op.execute("ALTER INDEX idx_terminal_zones_terminal RENAME TO idx_port_zones_terminal")
