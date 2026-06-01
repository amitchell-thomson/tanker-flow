"""add cog to ais_fixes

Stores course-over-ground (degrees) on every raw position fix. AISstream sends
Cog in every PositionReport but it was previously dropped at parse. Nullable —
the AIS "course not available" sentinel (360) is stored as NULL, and the
vesselfinder reconciliation path has no COG. Backfill is impossible (historical
fixes never captured it), so older rows stay NULL.

Intended downstream use: leg-direction / basin-diversion detection and
inbound/outbound disambiguation in the signal layer.

Revision ID: a7c3e9f1b206
Revises: f4b1c7e0a9d2
Create Date: 2026-06-01

"""
from typing import Sequence, Union

from alembic import op


revision: str = "a7c3e9f1b206"
down_revision: Union[str, Sequence[str], None] = "f4b1c7e0a9d2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE ais_fixes ADD COLUMN cog REAL")


def downgrade() -> None:
    op.execute("ALTER TABLE ais_fixes DROP COLUMN cog")
