"""add priority_watchlist and terminals.unlocode

Revision ID: be88d7401362
Revises: ccf6db6be4e6
Create Date: 2026-05-29 21:50:35.461588

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'be88d7401362'
down_revision: Union[str, Sequence[str], None] = 'ccf6db6be4e6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("terminals", sa.Column("unlocode", sa.Text(), nullable=True))

    op.create_table(
        "priority_watchlist",
        sa.Column("mmsi", sa.BigInteger(), nullable=False),
        sa.Column("tier", sa.SmallInteger(), nullable=False),
        sa.Column("score", sa.REAL(), nullable=False),
        sa.Column("score_reason", sa.Text(), nullable=True),
        sa.Column("last_fix_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_zone_fix_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("parsed_dest_terminal_id", sa.Integer(), nullable=True),
        sa.Column("parsed_eta", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "in_slot", sa.Boolean(), nullable=False, server_default=sa.text("FALSE")
        ),
        sa.Column("slot_kind", sa.Text(), nullable=True),
        sa.Column(
            "computed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("mmsi"),
        sa.ForeignKeyConstraint(["mmsi"], ["vessel_registry.mmsi"]),
        sa.ForeignKeyConstraint(
            ["parsed_dest_terminal_id"], ["terminals.terminal_id"]
        ),
    )
    op.create_index(
        "ix_priority_watchlist_tier_last_fix",
        "priority_watchlist",
        ["tier", sa.text("last_fix_ts DESC")],
    )
    op.create_index(
        "ix_priority_watchlist_slot_kind_last_fix",
        "priority_watchlist",
        ["slot_kind", "last_fix_ts"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_priority_watchlist_slot_kind_last_fix",
        table_name="priority_watchlist",
    )
    op.drop_index(
        "ix_priority_watchlist_tier_last_fix",
        table_name="priority_watchlist",
    )
    op.drop_table("priority_watchlist")
    op.drop_column("terminals", "unlocode")
