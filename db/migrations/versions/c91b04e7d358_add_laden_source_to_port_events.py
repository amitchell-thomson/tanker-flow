"""add laden_source to port_events

Records which strategy decided each event's laden_flag — 'draught' (primary,
from vessel_state.draught vs vessel_registry.design_draught) or
'flow_direction' (fallback, from terminals.flow_direction + the event's
side-of-moored). NULL when neither could decide.

Revision ID: c91b04e7d358
Revises: b5e9d3a07c21
Create Date: 2026-05-28

"""
from typing import Sequence, Union

from alembic import op


revision: str = "c91b04e7d358"
down_revision: Union[str, Sequence[str], None] = "b5e9d3a07c21"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE port_events ADD COLUMN laden_source TEXT "
        "CHECK (laden_source IN ('draught', 'flow_direction'))"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE port_events DROP COLUMN laden_source")
