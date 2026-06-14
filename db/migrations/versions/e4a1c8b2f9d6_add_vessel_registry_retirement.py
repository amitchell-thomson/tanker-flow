"""add vessel_registry retirement columns

Revision ID: e4a1c8b2f9d6
Revises: d7c4a2e9f813
Create Date: 2026-06-14 10:30:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "e4a1c8b2f9d6"
down_revision: Union[str, Sequence[str], None] = "d7c4a2e9f813"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE vessel_registry
            ADD COLUMN retired_at       TIMESTAMPTZ,
            ADD COLUMN retirement_basis TEXT;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE vessel_registry
            DROP COLUMN IF EXISTS retirement_basis,
            DROP COLUMN IF EXISTS retired_at;
        """
    )
