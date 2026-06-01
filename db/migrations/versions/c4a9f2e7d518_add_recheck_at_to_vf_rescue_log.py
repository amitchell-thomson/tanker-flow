"""add recheck_at to vf_rescue_log

Per-vessel variable cooldown for ingestion/vf_rescue.py. A vessel caught still
moving in its approach is re-polled sooner (so we capture the actual entry/moored
event) than one caught settled at berth/anchor. The worker writes recheck_at on
every log row; the cooldown check uses the latest row's recheck_at.

Revision ID: c4a9f2e7d518
Revises: b8e1a3f6c290
Create Date: 2026-06-01

"""
from typing import Sequence, Union

from alembic import op


revision: str = "c4a9f2e7d518"
down_revision: Union[str, Sequence[str], None] = "b8e1a3f6c290"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE vf_rescue_log ADD COLUMN recheck_at TIMESTAMPTZ")


def downgrade() -> None:
    op.execute("ALTER TABLE vf_rescue_log DROP COLUMN recheck_at")
