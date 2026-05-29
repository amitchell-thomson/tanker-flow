"""drop ingestion_heartbeat table

Revision ID: ccf6db6be4e6
Revises: 1b16cfbf2851
Create Date: 2026-05-29 16:56:04.607843

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ccf6db6be4e6'
down_revision: Union[str, Sequence[str], None] = '1b16cfbf2851'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_table("ingestion_heartbeat")


def downgrade() -> None:
    op.create_table(
        "ingestion_heartbeat",
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column(
            "last_heartbeat",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("source"),
    )
