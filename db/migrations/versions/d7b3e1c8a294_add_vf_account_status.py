"""add vf_account_status

Periodic snapshot of the VesselFinder account balance from the free /status
endpoint (CREDITS + EXPIRATION_DATE). The rescue worker writes a row each run;
the TUI reads the latest for a live "credits remaining / expires" readout, and
consecutive rows give the real burn rate (and auto-reconcile the per-position
billing assumption against logged spend).

Revision ID: d7b3e1c8a294
Revises: c4a9f2e7d518
Create Date: 2026-06-01

"""
from typing import Sequence, Union

from alembic import op


revision: str = "d7b3e1c8a294"
down_revision: Union[str, Sequence[str], None] = "c4a9f2e7d518"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE vf_account_status (
            checked_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            credits         INTEGER     NOT NULL,
            expiration_date TIMESTAMPTZ
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_vf_account_status_checked_at "
        "ON vf_account_status (checked_at DESC)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE vf_account_status")
