"""add fixes_per_minute and fixes_per_hour continuous aggregates

Revision ID: c7f2e9a1b4d3
Revises: deeeed6a8a7c
Create Date: 2026-05-25 16:00:00.000000

"""

from typing import Sequence, Union

from alembic import op

revision: str = "c7f2e9a1b4d3"
down_revision: Union[str, Sequence[str], None] = "deeeed6a8a7c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE MATERIALIZED VIEW fixes_per_minute
        WITH (timescaledb.continuous) AS
        SELECT time_bucket('1 minute', fix_ts) AS bucket, COUNT(*) AS cnt
        FROM ais_fixes
        GROUP BY bucket
        WITH NO DATA
    """)
    op.execute("""
        SELECT add_continuous_aggregate_policy('fixes_per_minute',
            start_offset => INTERVAL '2 days',
            end_offset   => INTERVAL '1 minute',
            schedule_interval => INTERVAL '1 minute')
    """)

    op.execute("""
        CREATE MATERIALIZED VIEW fixes_per_hour
        WITH (timescaledb.continuous) AS
        SELECT time_bucket('1 hour', fix_ts) AS bucket, COUNT(*) AS cnt
        FROM ais_fixes
        GROUP BY bucket
        WITH NO DATA
    """)
    op.execute("""
        SELECT add_continuous_aggregate_policy('fixes_per_hour',
            start_offset => INTERVAL '365 days',
            end_offset   => INTERVAL '1 hour',
            schedule_interval => INTERVAL '1 hour')
    """)

    # Historical backfill cannot run inside a transaction (TimescaleDB restriction).
    # The refresh policies above will populate the CAGGs on their first run.
    # To backfill immediately, run manually in psql:
    #   CALL refresh_continuous_aggregate('fixes_per_minute', NULL, now() - INTERVAL '1 minute');
    #   CALL refresh_continuous_aggregate('fixes_per_hour',   NULL, now() - INTERVAL '1 hour');


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS fixes_per_hour CASCADE")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS fixes_per_minute CASCADE")
