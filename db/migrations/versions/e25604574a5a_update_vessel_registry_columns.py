"""update vessel registry columns

Revision ID: e25604574a5a
Revises:
Create Date: 2026-05-24 09:03:55.241663

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e25604574a5a"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "vessel_registry", sa.Column("vf_vessel_type", sa.Text(), nullable=True)
    )
    op.add_column(
        "vessel_registry", sa.Column("year_built", sa.SmallInteger(), nullable=True)
    )
    op.add_column("vessel_registry", sa.Column("builder", sa.Text(), nullable=True))
    op.add_column("vessel_registry", sa.Column("owner", sa.Text(), nullable=True))
    op.add_column("vessel_registry", sa.Column("manager", sa.Text(), nullable=True))
    op.add_column("vessel_registry", sa.Column("length_m", sa.Float(), nullable=True))
    op.add_column("vessel_registry", sa.Column("beam_m", sa.Float(), nullable=True))
    op.add_column(
        "vessel_registry", sa.Column("gross_tonnage", sa.Integer(), nullable=True)
    )
    op.add_column(
        "vessel_registry", sa.Column("net_tonnage", sa.Integer(), nullable=True)
    )
    op.add_column("vessel_registry", sa.Column("teu", sa.Integer(), nullable=True))
    op.add_column(
        "vessel_registry", sa.Column("crude_capacity", sa.Integer(), nullable=True)
    )
    op.add_column(
        "vessel_registry", sa.Column("gas_capacity_m3", sa.Integer(), nullable=True)
    )
    op.add_column(
        "vessel_registry", sa.Column("is_lng_carrier", sa.Boolean(), nullable=True)
    )
    op.add_column("vessel_registry", sa.Column("is_fsru", sa.Boolean(), nullable=True))
    op.add_column(
        "vessel_registry",
        sa.Column("excluded", sa.Boolean(), nullable=False, server_default="false"),
    )
    op.add_column(
        "vessel_registry", sa.Column("exclusion_reason", sa.Text(), nullable=True)
    )
    op.add_column(
        "vessel_registry", sa.Column("vf_enrichment_status", sa.Text(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("vessel_registry", "vf_enrichment_status")
    op.drop_column("vessel_registry", "exclusion_reason")
    op.drop_column("vessel_registry", "excluded")
    op.drop_column("vessel_registry", "is_fsru")
    op.drop_column("vessel_registry", "is_lng_carrier")
    op.drop_column("vessel_registry", "gas_capacity_m3")
    op.drop_column("vessel_registry", "crude_capacity")
    op.drop_column("vessel_registry", "teu")
    op.drop_column("vessel_registry", "net_tonnage")
    op.drop_column("vessel_registry", "gross_tonnage")
    op.drop_column("vessel_registry", "beam_m")
    op.drop_column("vessel_registry", "length_m")
    op.drop_column("vessel_registry", "manager")
    op.drop_column("vessel_registry", "owner")
    op.drop_column("vessel_registry", "builder")
    op.drop_column("vessel_registry", "year_built")
    op.drop_column("vessel_registry", "vf_vessel_type")
