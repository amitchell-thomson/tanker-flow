"""add priority_watchlist.last_scan_window_at

Revision ID: 19867dc74649
Revises: be88d7401362
Create Date: 2026-05-30 09:22:09.753658

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '19867dc74649'
down_revision: Union[str, Sequence[str], None] = 'be88d7401362'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "priority_watchlist",
        sa.Column("last_scan_window_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_priority_watchlist_tier_scan_window",
        "priority_watchlist",
        ["tier", sa.text("last_scan_window_at ASC NULLS FIRST")],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_priority_watchlist_tier_scan_window", table_name="priority_watchlist"
    )
    op.drop_column("priority_watchlist", "last_scan_window_at")
