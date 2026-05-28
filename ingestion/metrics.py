"""DB-backed ingestion observability: lifecycle events + per-minute stats.

Two tables are written by the running ingester:

- `ingestion_events`: append-only timeline of connect/subscribe/planned_reconnect/
  disconnect/error events. One row per occurrence. Queryable to answer "did the
  process reconnect when we expected?" without needing access to file logs.

- `ingestion_stats_minute` + `ingestion_zone_minute`: one row per (source, bucket)
  with totals + lag + connection-state, plus a per-zone breakdown. Populated by
  the in-process `MinuteAggregator` as messages arrive; flushed when the wall
  clock crosses a minute boundary or on disconnect.

The aggregator runs entirely in-process — no DB hit per message — and writes a
single batch per minute. Designed so the file-log story keeps working for crash
cases while this gives queryable history.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

import asyncpg

from config import ZONES

logger = logging.getLogger(__name__)


def classify_zone(lat: float, lon: float) -> str | None:
    """Return the geographic zone name containing (lat, lon), or None.

    First match wins. The wmed bbox overlaps the iberian one in parts of
    eastern Spain; tuple order in config.ZONES decides precedence.
    """
    for name, lat_min, lat_max, lon_min, lon_max in ZONES:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return name
    return None


async def record_event(
    pool: asyncpg.Pool,
    source: str,
    event_type: str,
    detail: dict | None = None,
) -> None:
    """Append a lifecycle event. Swallows DB errors — observability must never
    take down the ingester."""
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO ingestion_events (source, event_type, detail)
                VALUES ($1, $2, $3::jsonb)
                """,
                source,
                event_type,
                json.dumps(detail) if detail is not None else None,
            )
    except Exception as e:
        logger.warning(f"record_event({event_type}) failed: {e}")


def _minute_bucket(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)


@dataclass
class MinuteAggregator:
    """Tracks per-minute counters in-process; flushes one stats row + N zone rows
    per minute boundary.

    Usage:
      - parser thread calls observe_fix(mmsi, fix_ts, lag_s, zone) for every fix
      - flusher task calls maybe_flush(pool) every flush tick; it writes prior
        minute's row when wall clock crosses a boundary
      - on disconnect, force_flush(pool) writes whatever is pending
    """

    source: str
    bucket: datetime | None = None
    fix_count: int = 0
    mmsi_set: set[int] = field(default_factory=set)
    lag_samples: list[float] = field(default_factory=list)
    zone_counts: dict[str, int] = field(default_factory=dict)
    max_raw_q: int = 0
    last_message_wall_ts: datetime | None = None
    connection_started_at: datetime | None = None

    def reset(self, new_bucket: datetime) -> None:
        self.bucket = new_bucket
        self.fix_count = 0
        self.mmsi_set = set()
        self.lag_samples = []
        self.zone_counts = {}
        self.max_raw_q = 0

    def observe_fix(
        self, mmsi: int, fix_ts: datetime, lag_s: float, zone: str | None
    ) -> None:
        if self.bucket is None:
            self.bucket = _minute_bucket(fix_ts)
        self.fix_count += 1
        self.mmsi_set.add(mmsi)
        self.lag_samples.append(lag_s)
        if zone is not None:
            self.zone_counts[zone] = self.zone_counts.get(zone, 0) + 1
        self.last_message_wall_ts = datetime.now(timezone.utc)

    def observe_q_depth(self, depth: int) -> None:
        if depth > self.max_raw_q:
            self.max_raw_q = depth

    def note_connection_start(self) -> None:
        self.connection_started_at = datetime.now(timezone.utc)

    async def maybe_flush(self, pool: asyncpg.Pool) -> None:
        """If wall clock has crossed into a new minute, write out the current bucket."""
        if self.bucket is None:
            return
        now_bucket = _minute_bucket(datetime.now(timezone.utc))
        if now_bucket > self.bucket:
            await self._write(pool, self.bucket)
            self.reset(now_bucket)

    async def force_flush(self, pool: asyncpg.Pool) -> None:
        """Write whatever is pending without rolling over (used on disconnect)."""
        if self.bucket is not None and self.fix_count > 0:
            await self._write(pool, self.bucket)
            self.reset(_minute_bucket(datetime.now(timezone.utc)))

    async def _write(self, pool: asyncpg.Pool, bucket: datetime) -> None:
        if self.fix_count == 0:
            return
        now = datetime.now(timezone.utc)
        sorted_lags = sorted(self.lag_samples)
        mean_lag = sum(sorted_lags) / len(sorted_lags)
        p95_idx = max(0, int(0.95 * (len(sorted_lags) - 1)))
        p95_lag = sorted_lags[p95_idx]
        secs_since_last_msg = (
            int((now - self.last_message_wall_ts).total_seconds())
            if self.last_message_wall_ts is not None
            else None
        )
        conn_age = (
            int((now - self.connection_started_at).total_seconds())
            if self.connection_started_at is not None
            else None
        )
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO ingestion_stats_minute
                        (bucket, source, fix_count, distinct_mmsi,
                         mean_lag_s, p95_lag_s, max_raw_q,
                         seconds_since_last_message, current_connection_age_s)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (source, bucket) DO UPDATE SET
                        fix_count = ingestion_stats_minute.fix_count + EXCLUDED.fix_count,
                        distinct_mmsi = GREATEST(ingestion_stats_minute.distinct_mmsi, EXCLUDED.distinct_mmsi),
                        mean_lag_s = EXCLUDED.mean_lag_s,
                        p95_lag_s = EXCLUDED.p95_lag_s,
                        max_raw_q = GREATEST(COALESCE(ingestion_stats_minute.max_raw_q, 0), COALESCE(EXCLUDED.max_raw_q, 0)),
                        seconds_since_last_message = EXCLUDED.seconds_since_last_message,
                        current_connection_age_s   = EXCLUDED.current_connection_age_s
                    """,
                    bucket,
                    self.source,
                    self.fix_count,
                    len(self.mmsi_set),
                    mean_lag,
                    p95_lag,
                    self.max_raw_q,
                    secs_since_last_msg,
                    conn_age,
                )
                if self.zone_counts:
                    await conn.executemany(
                        """
                        INSERT INTO ingestion_zone_minute
                            (bucket, source, zone, fix_count)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (source, bucket, zone) DO UPDATE SET
                            fix_count = ingestion_zone_minute.fix_count + EXCLUDED.fix_count
                        """,
                        [
                            (bucket, self.source, zone, cnt)
                            for zone, cnt in self.zone_counts.items()
                        ],
                    )
        except Exception as e:
            logger.warning(f"Stats flush for bucket {bucket} failed: {e}")
