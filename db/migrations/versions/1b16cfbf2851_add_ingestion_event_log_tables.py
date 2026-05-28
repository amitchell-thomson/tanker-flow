"""add ingestion event log tables

Revision ID: 1b16cfbf2851
Revises: c91b04e7d358
Create Date: 2026-05-28 22:45:06.532965

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "1b16cfbf2851"
down_revision: Union[str, Sequence[str], None] = "c91b04e7d358"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE ingestion_events (
            event_ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
            source          TEXT        NOT NULL,
            event_type      TEXT        NOT NULL,
            detail          JSONB
        );
        SELECT create_hypertable('ingestion_events', 'event_ts');
        SELECT set_chunk_time_interval('ingestion_events', INTERVAL '7 days');
        CREATE INDEX ON ingestion_events (source, event_ts DESC);
        CREATE INDEX ON ingestion_events (source, event_type, event_ts DESC);

        CREATE TABLE ingestion_stats_minute (
            bucket                      TIMESTAMPTZ NOT NULL,
            source                      TEXT        NOT NULL,
            fix_count                   INTEGER     NOT NULL,
            distinct_mmsi               INTEGER     NOT NULL,
            mean_lag_s                  REAL,
            p95_lag_s                   REAL,
            max_raw_q                   INTEGER,
            seconds_since_last_message  INTEGER,
            current_connection_age_s    INTEGER,
            PRIMARY KEY (source, bucket)
        );
        SELECT create_hypertable('ingestion_stats_minute', 'bucket');
        SELECT set_chunk_time_interval('ingestion_stats_minute', INTERVAL '7 days');

        CREATE TABLE ingestion_zone_minute (
            bucket          TIMESTAMPTZ NOT NULL,
            source          TEXT        NOT NULL,
            zone            TEXT        NOT NULL,
            fix_count       INTEGER     NOT NULL,
            PRIMARY KEY (source, bucket, zone)
        );
        SELECT create_hypertable('ingestion_zone_minute', 'bucket');
        SELECT set_chunk_time_interval('ingestion_zone_minute', INTERVAL '7 days');
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE IF EXISTS ingestion_zone_minute;
        DROP TABLE IF EXISTS ingestion_stats_minute;
        DROP TABLE IF EXISTS ingestion_events;
        """
    )
