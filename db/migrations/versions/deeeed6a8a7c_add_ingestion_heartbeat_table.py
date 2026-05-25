"""add ingestion_heartbeat table

Revision ID: deeeed6a8a7c
Revises: e25604574a5a
Create Date: 2026-05-25 14:21:24.098055

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "deeeed6a8a7c"
down_revision: Union[str, Sequence[str], None] = "e25604574a5a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
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


def downgrade() -> None:
    op.drop_table("ingestion_heartbeat")
