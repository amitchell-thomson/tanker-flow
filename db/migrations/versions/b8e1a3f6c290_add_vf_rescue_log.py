"""add vf_rescue_log

Append-only audit trail and credit-budget ledger for ingestion/vf_rescue.py, the
VesselFinder live-positions rescue worker. Each row is one VF lookup attempt for
a high-value vessel that went AIS-silent. today's SUM(credits) is the restart-safe
daily-spend ledger; a per-mmsi recency check is the cooldown. requested_imos /
returned_rows reconcile the per-returned-row billing assumption against the VF
dashboard on the first live run.

Revision ID: b8e1a3f6c290
Revises: a7c3e9f1b206
Create Date: 2026-06-01

"""
from typing import Sequence, Union

from alembic import op


revision: str = "b8e1a3f6c290"
down_revision: Union[str, Sequence[str], None] = "a7c3e9f1b206"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE vf_rescue_log (
            id             BIGSERIAL   PRIMARY KEY,
            requested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            mmsi           BIGINT      NOT NULL,
            imo            BIGINT,
            vessel_name    TEXT,
            rescue_class   TEXT        NOT NULL,
            sat            BOOLEAN     NOT NULL DEFAULT FALSE,
            src            TEXT,
            result         TEXT        NOT NULL CHECK (result IN (
                               'rescued','no_position','rejected_stale',
                               'rejected_teleport','error','dry_run')),
            credits        SMALLINT    NOT NULL DEFAULT 0,
            requested_imos SMALLINT,
            returned_rows  SMALLINT,
            fix_ts         TIMESTAMPTZ,
            detail         TEXT
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_vf_rescue_log_requested_at "
        "ON vf_rescue_log (requested_at DESC)"
    )
    op.execute(
        "CREATE INDEX ix_vf_rescue_log_mmsi_requested "
        "ON vf_rescue_log (mmsi, requested_at DESC)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE vf_rescue_log")
