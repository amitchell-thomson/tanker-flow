"""add generated regime column to port_events

Tags every port_events row with the ingestion regime its event_time falls in:
'bbox' (the old AISstream bbox+throttle subscription) or 'mmsi_filter'
(server-side MMSI filtering), split at the 2026-05-30 09:27 UTC cutover.
GENERATED/STORED so it can never drift from event_time. The cutover literal
mirrors config.REGIME_CUTOVER. See docs/review-2026-05-31-pre-signal-audit.md §0.

Revision ID: d1f3a7c0b2e9
Revises: a1c9f4e2b8d7
Create Date: 2026-05-31

"""

from typing import Sequence, Union

from alembic import op


revision: str = "d1f3a7c0b2e9"
down_revision: Union[str, Sequence[str], None] = "a1c9f4e2b8d7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE port_events ADD COLUMN regime TEXT "
        "GENERATED ALWAYS AS ("
        "CASE WHEN event_time < TIMESTAMPTZ '2026-05-30 09:27:00+00' "
        "THEN 'bbox' ELSE 'mmsi_filter' END) STORED"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE port_events DROP COLUMN regime")
