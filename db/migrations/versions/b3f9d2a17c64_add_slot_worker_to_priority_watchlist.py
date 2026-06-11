"""add slot_worker to priority_watchlist

Stage-3 sharding (multi-worker ingester behind a second egress IP). Two workers
each own a disjoint mmsi-modulo half of the fleet and both write
priority_watchlist.in_slot. The old mark_slot_assignments did a GLOBAL
`SET in_slot = FALSE` then set its own rows, so a second writer would clobber the
first's slot assignments. slot_worker records which WORKER_ID holds each slot;
each worker now clears/sets only its own partition. NULL = unslotted. No-op for a
single worker (WORKER_COUNT=1), where the column is simply 0 for every in-slot row.

Revision ID: b3f9d2a17c64
Revises: a2e7c4f9b531
Create Date: 2026-06-11

"""

from typing import Sequence, Union

from alembic import op


revision: str = "b3f9d2a17c64"
down_revision: Union[str, Sequence[str], None] = "a2e7c4f9b531"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE priority_watchlist ADD COLUMN slot_worker SMALLINT")


def downgrade() -> None:
    op.execute("ALTER TABLE priority_watchlist DROP COLUMN slot_worker")
