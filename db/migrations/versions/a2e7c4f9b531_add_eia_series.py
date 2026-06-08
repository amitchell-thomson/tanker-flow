"""add eia_series

Exogenous EIA ground-truth + fundamentals (data/eia.py). Tidy/long table keyed
by (series_id, period). NOT derived from our pipeline — upserted, never rebuilt
by `make signals`. EIA revises recent periods, so the loader upserts ON CONFLICT
and `fetched_at` records the last pull. Phase 1 stores monthly US LNG exports
(the capture-rate ground truth, the long pole in docs/park-checkups.md #13);
Phase 2 adds weekly storage + Henry Hub spot as one registry entry each.

Revision ID: a2e7c4f9b531
Revises: b8d2f4a6c103
Create Date: 2026-06-08

"""

from typing import Sequence, Union

from alembic import op


revision: str = "a2e7c4f9b531"
down_revision: Union[str, Sequence[str], None] = "b8d2f4a6c103"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE eia_series (
            series_id   TEXT             NOT NULL,
            period      DATE             NOT NULL,
            value       DOUBLE PRECISION,
            unit        TEXT             NOT NULL,
            frequency   TEXT             NOT NULL,
            fetched_at  TIMESTAMPTZ      NOT NULL DEFAULT now(),
            PRIMARY KEY (series_id, period)
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE eia_series")
