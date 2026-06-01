"""add signal_daily

Tidy/long daily panel of market signals, written by pipeline/signal.py
(TRUNCATE + rebuild, like port_events). One row per
(signal_key, bucket_date, zone_scope, regime, basis). The headline #1/#2
"laden ton-miles in transit" lives here as a signal_key value rather than a
dedicated table. `regime` is segmented per analysis/SIGNALS.md §0.5 (never
aggregate a model across the 2026-05-30 seam); `basis` reserves a slot for the
future leakage-free point-in-time series ('knowable') alongside today's
hindsight-clean 'physical' reconstruction.

Revision ID: f1a8c3d5e7b9
Revises: d7b3e1c8a294
Create Date: 2026-06-01

"""
from typing import Sequence, Union

from alembic import op


revision: str = "f1a8c3d5e7b9"
down_revision: Union[str, Sequence[str], None] = "d7b3e1c8a294"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE signal_daily (
            id           BIGSERIAL        PRIMARY KEY,
            signal_key   TEXT             NOT NULL,
            bucket_date  DATE             NOT NULL,
            zone_scope   TEXT             NOT NULL,
            regime       TEXT             NOT NULL
                                          CHECK (regime IN ('bbox','mmsi_filter','all')),
            value        DOUBLE PRECISION NOT NULL,
            n_legs       INTEGER,
            basis        TEXT             NOT NULL DEFAULT 'physical'
                                          CHECK (basis IN ('physical','knowable')),
            computed_at  TIMESTAMPTZ      NOT NULL DEFAULT now(),
            UNIQUE (signal_key, bucket_date, zone_scope, regime, basis)
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_signal_daily_key_date ON signal_daily (signal_key, bucket_date)"
    )
    op.execute("CREATE INDEX ix_signal_daily_date ON signal_daily (bucket_date)")


def downgrade() -> None:
    op.execute("DROP TABLE signal_daily")
