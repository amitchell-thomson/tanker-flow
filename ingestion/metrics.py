"""DB-backed ingestion observability: lifecycle events + per-minute stats.

Two tables are written by the running ingester:

- `ingestion_events`: append-only timeline of connect/subscribe/planned_reconnect/
  disconnect/error events. One row per occurrence. Queryable to answer "did the
  process reconnect when we expected?" without needing access to file logs.

- `ingestion_stats_minute` + `ingestion_zone_minute`: one row per (source, bucket)
  with totals + lag + connection-state, plus a per-zone breakdown. Populated by
  the in-process `MinuteAggregator`: writes the in-progress minute incrementally
  (every flush tick, gated by LIVE_FLUSH_INTERVAL_S so we don't hammer the DB)
  and finalises the row at minute rollover.

To keep multi-writer correctness (rotation/reconnect both produce new
aggregator instances mid-minute), the aggregator emits *deltas* of fix_count
and per-zone counts since its last write; the DB's additive `ON CONFLICT DO
UPDATE` then sums them. Snapshot-style stats (lag, queue depth, connection
age) just take the latest value.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import asyncpg

from config import ZONES

logger = logging.getLogger(__name__)

# Minimum gap between mid-minute live writes. Rollover writes are always immediate.
LIVE_FLUSH_INTERVAL_S = 2.0


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
    """Tracks per-minute counters in-process and writes the current minute's
    row to ingestion_stats_minute live (every LIVE_FLUSH_INTERVAL_S) plus an
    immediate final write at minute rollover.

    Delta accounting: each write emits only (current - last_flushed) for the
    additive columns (fix_count, per-zone counts). Snapshot columns
    (lag, queue depth, connection age) just take the current value.
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
    # Watermarks for delta accounting.
    _flushed_fix_count: int = 0
    _flushed_zone_counts: dict[str, int] = field(default_factory=dict)
    _last_live_flush_mono: float = 0.0

    def reset(self, new_bucket: datetime) -> None:
        self.bucket = new_bucket
        self.fix_count = 0
        self.mmsi_set = set()
        self.lag_samples = []
        self.zone_counts = {}
        self.max_raw_q = 0
        self._flushed_fix_count = 0
        self._flushed_zone_counts = {}

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
        """Write the current bucket — incrementally during the minute (throttled
        to LIVE_FLUSH_INTERVAL_S between writes) and immediately at rollover."""
        if self.bucket is None:
            return
        now_bucket = _minute_bucket(datetime.now(timezone.utc))
        if now_bucket > self.bucket:
            # Rollover: emit final delta for the just-completed bucket, then reset.
            await self._write_delta(pool, self.bucket)
            self.reset(now_bucket)
            self._last_live_flush_mono = time.monotonic()
            return
        # Mid-minute: throttle to avoid hammering the DB on every 0.5s tick.
        if time.monotonic() - self._last_live_flush_mono >= LIVE_FLUSH_INTERVAL_S:
            await self._write_delta(pool, self.bucket)
            self._last_live_flush_mono = time.monotonic()

    async def force_flush(self, pool: asyncpg.Pool) -> None:
        """Write whatever is pending without rolling over (used on disconnect)."""
        if self.bucket is not None:
            await self._write_delta(pool, self.bucket)
            self.reset(_minute_bucket(datetime.now(timezone.utc)))

    async def _write_delta(self, pool: asyncpg.Pool, bucket: datetime) -> None:
        fix_delta = self.fix_count - self._flushed_fix_count
        if fix_delta == 0:
            return

        zone_deltas = {
            zone: cnt - self._flushed_zone_counts.get(zone, 0)
            for zone, cnt in self.zone_counts.items()
        }
        zone_deltas = {z: d for z, d in zone_deltas.items() if d > 0}

        sorted_lags = sorted(self.lag_samples)
        mean_lag = sum(sorted_lags) / len(sorted_lags) if sorted_lags else None
        p95_lag = (
            sorted_lags[max(0, int(0.95 * (len(sorted_lags) - 1)))]
            if sorted_lags
            else None
        )
        now = datetime.now(timezone.utc)
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
                    fix_delta,
                    len(self.mmsi_set),
                    mean_lag,
                    p95_lag,
                    self.max_raw_q,
                    secs_since_last_msg,
                    conn_age,
                )
                if zone_deltas:
                    await conn.executemany(
                        """
                        INSERT INTO ingestion_zone_minute
                            (bucket, source, zone, fix_count)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (source, bucket, zone) DO UPDATE SET
                            fix_count = ingestion_zone_minute.fix_count + EXCLUDED.fix_count
                        """,
                        [
                            (bucket, self.source, zone, delta)
                            for zone, delta in zone_deltas.items()
                        ],
                    )
        except Exception as e:
            logger.warning(f"Stats flush for bucket {bucket} failed: {e}")
            return  # leave watermarks unchanged so the delta is retried next tick

        self._flushed_fix_count = self.fix_count
        self._flushed_zone_counts = dict(self.zone_counts)
