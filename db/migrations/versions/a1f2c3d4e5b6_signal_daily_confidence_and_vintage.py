"""signal_daily confidence components + live-vintage log

Adds the decomposed data-quality columns to signal_daily (value_dispersion,
open_fraction, estimated_fraction — SIGNALS.md §0·8) and the append-only
signal_daily_live_vintage "as-printed" log backing the knowable-basis
self-validation (§0·7·1·4).

Revision ID: a1f2c3d4e5b6
Revises: e4a1c8b2f9d6
Create Date: 2026-06-16

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "a1f2c3d4e5b6"
down_revision: Union[str, Sequence[str], None] = "e4a1c8b2f9d6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE signal_daily
            ADD COLUMN value_dispersion   DOUBLE PRECISION,
            ADD COLUMN open_fraction      DOUBLE PRECISION,
            ADD COLUMN estimated_fraction DOUBLE PRECISION;

        CREATE TABLE signal_daily_live_vintage (
            id           BIGSERIAL        PRIMARY KEY,
            signal_key   TEXT             NOT NULL,
            bucket_date  DATE             NOT NULL,
            zone_scope   TEXT             NOT NULL,
            regime       TEXT             NOT NULL,
            basis        TEXT             NOT NULL,
            value        DOUBLE PRECISION NOT NULL,
            n_legs       INTEGER,
            printed_at   TIMESTAMPTZ      NOT NULL DEFAULT now()
        );
        CREATE INDEX ix_sdlv_key_date ON signal_daily_live_vintage (signal_key, bucket_date);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE signal_daily_live_vintage;
        ALTER TABLE signal_daily
            DROP COLUMN value_dispersion,
            DROP COLUMN open_fraction,
            DROP COLUMN estimated_fraction;
        """
    )
