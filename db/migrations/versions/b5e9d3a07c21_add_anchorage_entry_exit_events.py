"""add anchorage_entry and anchorage_exit to port_events.valid_event_type

These are raw polygon-crossing events (no dwell, no SOG filter). They bracket
every visit to the anchorage polygon and let downstream signals measure queue
time as `anchorage_exit.event_time - anchorage_entry.event_time` without
inheriting the ~30 min dwell bias of the existing `anchored` marker.

Revision ID: b5e9d3a07c21
Revises: a4f8c2e91d63
Create Date: 2026-05-28

"""
from typing import Sequence, Union

from alembic import op


revision: str = "b5e9d3a07c21"
down_revision: Union[str, Sequence[str], None] = "a4f8c2e91d63"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


NEW_EVENT_TYPES = (
    "zone_entry",
    "anchorage_entry",
    "anchored",
    "anchorage_exit",
    "moored",
    "departed",
    "zone_exit",
)
OLD_EVENT_TYPES = (
    "zone_entry",
    "anchored",
    "moored",
    "departed",
    "zone_exit",
)


def _rebuild_constraint(values: tuple[str, ...]) -> None:
    literals = ", ".join(f"'{v}'" for v in values)
    op.execute("ALTER TABLE port_events DROP CONSTRAINT valid_event_type")
    op.execute(
        f"ALTER TABLE port_events ADD CONSTRAINT valid_event_type "
        f"CHECK (event_type IN ({literals}))"
    )


def upgrade() -> None:
    _rebuild_constraint(NEW_EVENT_TYPES)


def downgrade() -> None:
    # port_events is recomputed idempotently by `make port-events`; the
    # downgrade just narrows the constraint. Existing rows of the new types
    # would block this — TRUNCATE first if you need to downgrade in place.
    _rebuild_constraint(OLD_EVENT_TYPES)
