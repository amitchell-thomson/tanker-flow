"""Port-events state machine runner.

Recomputes the `port_events` table from `ais_fixes` + `terminal_zones`. Idempotent:
TRUNCATEs the table and rebuilds from row 1. Streaming spatial join keeps memory
bounded over the full hypertable.

Usage: `uv run python -m pipeline.port_events` (or `make port-events`).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

import asyncpg
from rich.logging import RichHandler

from config import settings

from .laden import Side, build_draught_lookup, infer_laden
from .state_machine import (
    Event,
    Fix,
    make_nearest_berth,
    validate_sequence,
    walk,
)


logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])
logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# SQL
# ----------------------------------------------------------------------

TRUNCATE_SQL = "TRUNCATE port_events RESTART IDENTITY"

# In-scope vessels: LNG carriers + FSRUs (the VesselFinder taxonomy classifies
# FSRUs as 'Offshore Support Vessel', so is_lng_carrier=FALSE for them — they
# must be admitted via is_fsru explicitly).
IN_SCOPE_MMSIS_SQL = """
SELECT mmsi, is_fsru, design_draught
FROM vessel_registry
WHERE is_lng_carrier = TRUE OR is_fsru = TRUE
"""

# Berth centroids (lat,lon) per terminal — used by the nearest-berth tiebreaker
# when two terminals' anchorages overlap. Uses every berth sub_zone, not just
# the primary one.
BERTH_CENTROIDS_SQL = """
SELECT terminal_id, ST_Y(ST_Centroid(geom)) AS lat, ST_X(ST_Centroid(geom)) AS lon
FROM terminal_zones
WHERE zone_type = 'berth'
"""

TERMINAL_METADATA_SQL = (
    "SELECT terminal_id, zone, flow_direction FROM terminals WHERE zone IS NOT NULL"
)

FSRU_HOSTS_SQL = """
SELECT terminal_id, fsru_host_mmsi
FROM terminals
WHERE is_fsru = TRUE AND fsru_host_mmsi IS NOT NULL
"""

DRAUGHTS_SQL = """
SELECT mmsi, state_ts, draught
FROM vessel_state
WHERE mmsi = ANY($1)
ORDER BY mmsi, state_ts
"""

# Single streaming cursor: every in-scope fix with its candidate zones attached,
# ordered by (mmsi, fix_ts). LEFT JOIN preserves open-ocean fixes (empty arrays)
# so the state machine can detect envelope exit naturally. Three parallel
# arrays (terminal_id, zone_type, sub_zone) avoid the awkward asyncpg
# representation of array-of-array.
SPATIAL_JOIN_SQL = """
SELECT
    f.mmsi,
    f.fix_ts,
    f.lat,
    f.lon,
    f.sog,
    f.nav_status,
    COALESCE(
        array_agg(tz.terminal_id ORDER BY
            CASE tz.zone_type WHEN 'berth' THEN 0 WHEN 'anchorage' THEN 1 ELSE 2 END,
            tz.terminal_id, tz.sub_zone
        ) FILTER (WHERE tz.terminal_id IS NOT NULL),
        '{}'::int[]
    ) AS terminal_ids,
    COALESCE(
        array_agg(tz.zone_type::text ORDER BY
            CASE tz.zone_type WHEN 'berth' THEN 0 WHEN 'anchorage' THEN 1 ELSE 2 END,
            tz.terminal_id, tz.sub_zone
        ) FILTER (WHERE tz.zone_type IS NOT NULL),
        '{}'::text[]
    ) AS zone_types,
    COALESCE(
        array_agg(tz.sub_zone::int ORDER BY
            CASE tz.zone_type WHEN 'berth' THEN 0 WHEN 'anchorage' THEN 1 ELSE 2 END,
            tz.terminal_id, tz.sub_zone
        ) FILTER (WHERE tz.sub_zone IS NOT NULL),
        '{}'::int[]
    ) AS sub_zones
FROM ais_fixes f
LEFT JOIN terminal_zones tz
    ON ST_Within(ST_SetSRID(ST_Point(f.lon, f.lat), 4326), tz.geom)
WHERE f.mmsi = ANY($1)
  AND f.lat IS NOT NULL
  AND f.lon IS NOT NULL
GROUP BY f.mmsi, f.fix_ts, f.lat, f.lon, f.sog, f.nav_status
ORDER BY f.mmsi, f.fix_ts
"""

INSERT_SQL = """
INSERT INTO port_events
    (mmsi, event_type, zone, terminal_id, event_time, lat, lon,
     laden_flag, laden_source, cold_start)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
"""


# ----------------------------------------------------------------------
# Orchestration
# ----------------------------------------------------------------------


async def run(pool: asyncpg.Pool) -> None:
    t_start = time.monotonic()
    # Wall-clock reference for end-of-stream stale-envelope closing. Computed
    # once at run start so the cutoff is consistent across all vessels in this
    # batch — otherwise a vessel processed later would be evaluated against a
    # slightly later `now`, biasing nothing in practice but introducing
    # rebuild-time-dependent non-determinism.
    now = datetime.now(UTC)

    async with pool.acquire() as conn:
        await conn.execute(TRUNCATE_SQL)
        in_scope = await conn.fetch(IN_SCOPE_MMSIS_SQL)
        berths = await conn.fetch(BERTH_CENTROIDS_SQL)
        terminal_rows = await conn.fetch(TERMINAL_METADATA_SQL)
        terminal_zone_map = {r["terminal_id"]: r["zone"] for r in terminal_rows}
        terminal_flow_map: dict[int, str] = {
            r["terminal_id"]: r["flow_direction"] for r in terminal_rows
        }
        fsru_hosts = {
            r["fsru_host_mmsi"]: r["terminal_id"]
            for r in await conn.fetch(FSRU_HOSTS_SQL)
        }

    if not in_scope:
        logger.warning("No in-scope LNG carriers in vessel_registry; nothing to do.")
        return

    fsru_mmsis = {r["mmsi"] for r in in_scope if r["is_fsru"]}
    walker_mmsis = [r["mmsi"] for r in in_scope if not r["is_fsru"]]
    design_draught = {r["mmsi"]: r["design_draught"] for r in in_scope}

    centroids: dict[int, list[tuple[float, float]]] = defaultdict(list)
    for r in berths:
        centroids[r["terminal_id"]].append((r["lat"], r["lon"]))
    if not centroids:
        logger.warning(
            "No berth polygons found in terminal_zones; nearest-berth "
            "tiebreaker is unavailable — overlapping terminals will resolve "
            "by terminal_id order."
        )
    nearest_berth = make_nearest_berth(dict(centroids))

    # Pre-fetch all draught records so we can do bisect lookups in memory.
    async with pool.acquire() as conn:
        draught_rows = await conn.fetch(DRAUGHTS_SQL, [r["mmsi"] for r in in_scope])
    draught_lookup = build_draught_lookup(
        [(r["mmsi"], r["state_ts"], r["draught"]) for r in draught_rows]
    )

    # ----- FSRU short-circuit: one moored event per declared host -----
    fsru_events_inserted = await _emit_fsru_moored(
        pool, fsru_mmsis, fsru_hosts, terminal_zone_map
    )

    # ----- Walk every other in-scope MMSI through the state machine -----
    summary = {
        "regular_vessels": 0,
        "fsru_vessels_emitted": fsru_events_inserted,
        "events_by_kind": defaultdict(int),
        "open_visits": 0,
        "cold_start_events": 0,
        "vessels_with_zero_events": 0,
    }

    # Stream the spatial join in a single cursor and split at MMSI boundaries.
    async with pool.acquire() as conn:
        async with conn.transaction():
            cur = await conn.cursor(SPATIAL_JOIN_SQL, walker_mmsis)
            current_mmsi: int | None = None
            buf: list[Fix] = []
            while True:
                rows = await cur.fetch(2000)
                if not rows:
                    break
                for row in rows:
                    if row["mmsi"] != current_mmsi:
                        if current_mmsi is not None:
                            await _process_vessel(
                                pool,
                                current_mmsi,
                                buf,
                                nearest_berth,
                                terminal_zone_map,
                                terminal_flow_map,
                                design_draught,
                                draught_lookup,
                                summary,
                                now,
                            )
                        current_mmsi = row["mmsi"]
                        buf = []
                    buf.append(_row_to_fix(row))
            if current_mmsi is not None:
                await _process_vessel(
                    pool,
                    current_mmsi,
                    buf,
                    nearest_berth,
                    terminal_zone_map,
                    terminal_flow_map,
                    design_draught,
                    draught_lookup,
                    summary,
                    now,
                )

    _log_summary(summary, time.monotonic() - t_start)


def _row_to_fix(row: asyncpg.Record) -> Fix:
    terminal_ids: list[int] = row["terminal_ids"] or []
    zone_types: list[str] = row["zone_types"] or []
    sub_zones: list[int] = row["sub_zones"] or []
    zones = tuple(zip(terminal_ids, zone_types, sub_zones, strict=True))
    return Fix(
        fix_ts=row["fix_ts"],
        lat=row["lat"],
        lon=row["lon"],
        sog=row["sog"],
        nav_status=row["nav_status"],
        zones=zones,
    )


def _classify_envelope_sides(events: list[Event]) -> list[Side]:
    """For each event, return its position relative to the envelope's moored:
    'pre' (before moored), 'moored' (the moored itself), 'post' (after moored),
    or 'no_moored' (envelope contains no moored).

    An envelope = events between successive zone_entry events (or from the
    start / to the end of the list). Each envelope has 0 or 1 moored events.
    """
    sides: list[Side] = ["no_moored"] * len(events)
    envelope_start = 0
    for i, ev in enumerate(events):
        is_last = i == len(events) - 1
        envelope_ends_here = ev.event_type == "zone_exit" or is_last
        next_starts_new = (
            i + 1 < len(events) and events[i + 1].event_type == "zone_entry"
        )
        if envelope_ends_here or next_starts_new:
            close_idx = i if envelope_ends_here else i
            moored_idx = next(
                (
                    j
                    for j in range(envelope_start, close_idx + 1)
                    if events[j].event_type == "moored"
                ),
                None,
            )
            if moored_idx is None:
                for j in range(envelope_start, close_idx + 1):
                    sides[j] = "no_moored"
            else:
                for j in range(envelope_start, close_idx + 1):
                    if j < moored_idx:
                        sides[j] = "pre"
                    elif j == moored_idx:
                        sides[j] = "moored"
                    else:
                        sides[j] = "post"
            envelope_start = i + 1
    return sides


async def _process_vessel(
    pool: asyncpg.Pool,
    mmsi: int,
    fixes: list[Fix],
    nearest_berth,
    terminal_zone_map: dict[int, str],
    terminal_flow_map: dict[int, str],
    design_draught: dict[int, float | None],
    draught_lookup,
    summary: dict[str, Any],
    now: datetime,
) -> None:
    summary["regular_vessels"] += 1
    if not fixes:
        summary["vessels_with_zero_events"] += 1
        return

    events = walk(iter(fixes), nearest_berth, now=now)
    if not events:
        summary["vessels_with_zero_events"] += 1
        return

    try:
        validate_sequence(events)
    except AssertionError as e:
        logger.error("MMSI %s: event sequence invalid: %s", mmsi, e)
        raise

    sides = _classify_envelope_sides(events)
    rows = []
    has_moored = False
    has_departed = False
    for ev, side in zip(events, sides, strict=True):
        zone = terminal_zone_map.get(ev.terminal_id)
        if zone is None:
            # Terminal exists but has no zone assignment (e.g., out-of-scope
            # export terminals from the migration). Skip — port_events.zone
            # is NOT NULL with a CHECK constraint.
            logger.warning(
                "MMSI %s: skipping event for terminal_id=%s (no zone assigned)",
                mmsi,
                ev.terminal_id,
            )
            continue
        laden, laden_source = infer_laden(
            mmsi,
            ev.event_time,
            side,
            terminal_flow_map.get(ev.terminal_id),
            design_draught.get(mmsi),
            draught_lookup,
        )
        rows.append(
            (
                mmsi,
                ev.event_type,
                zone,
                ev.terminal_id,
                ev.event_time,
                ev.lat,
                ev.lon,
                laden,
                laden_source,
                ev.cold_start,
            )
        )
        summary["events_by_kind"][(zone, ev.event_type)] += 1
        if ev.cold_start:
            summary["cold_start_events"] += 1
        if ev.event_type == "moored":
            has_moored = True
        elif ev.event_type == "departed":
            has_departed = True

    if has_moored and not has_departed:
        summary["open_visits"] += 1

    if rows:
        async with pool.acquire() as conn:
            await conn.executemany(INSERT_SQL, rows)


async def _emit_fsru_moored(
    pool: asyncpg.Pool,
    fsru_mmsis: set[int],
    fsru_hosts: dict[int, int],
    terminal_zone_map: dict[int, str],
) -> int:
    """Emit one synthetic moored event per declared FSRU at its host terminal.

    The event_time is the first fix observed for that MMSI. FSRUs without a
    declared host (terminals.fsru_host_mmsi IS NULL) are logged and skipped —
    the resident vessel is unknown.
    """
    if not fsru_mmsis:
        return 0

    inserted = 0
    rows = []
    async with pool.acquire() as conn:
        for mmsi in sorted(fsru_mmsis):
            terminal_id = fsru_hosts.get(mmsi)
            if terminal_id is None:
                logger.warning(
                    "FSRU MMSI %s has no declared host terminal "
                    "(terminals.fsru_host_mmsi); skipping.",
                    mmsi,
                )
                continue
            zone = terminal_zone_map.get(terminal_id)
            if zone is None:
                logger.warning(
                    "FSRU MMSI %s host terminal_id=%s has no zone; skipping.",
                    mmsi,
                    terminal_id,
                )
                continue
            first = await conn.fetchrow(
                """
                SELECT fix_ts, lat, lon
                FROM ais_fixes
                WHERE mmsi = $1 AND lat IS NOT NULL AND lon IS NOT NULL
                ORDER BY fix_ts
                LIMIT 1
                """,
                mmsi,
            )
            if first is None:
                logger.info("FSRU MMSI %s has no fixes; skipping.", mmsi)
                continue
            rows.append(
                (
                    mmsi,
                    "moored",
                    zone,
                    terminal_id,
                    first["fix_ts"],
                    first["lat"],
                    first["lon"],
                    None,  # FSRUs: laden_flag NULL (they don't ballast in/out)
                    None,  # laden_source NULL (no inference attempted)
                    True,  # cold_start = TRUE — they've been moored since before data
                )
            )
        if rows:
            await conn.executemany(INSERT_SQL, rows)
            inserted = len(rows)
    return inserted


def _log_summary(summary: dict[str, Any], wall_seconds: float) -> None:
    logger.info("=" * 60)
    logger.info(
        "port_events recompute complete (%.1fs wall, %d events)",
        wall_seconds,
        sum(summary["events_by_kind"].values()) + summary["fsru_vessels_emitted"],
    )
    logger.info(
        "  regular vessels: %d   (zero-event: %d)",
        summary["regular_vessels"],
        summary["vessels_with_zero_events"],
    )
    logger.info("  fsru moored emits: %d", summary["fsru_vessels_emitted"])
    logger.info("  cold-start events: %d", summary["cold_start_events"])
    logger.info("  open visits (moored, no departed): %d", summary["open_visits"])
    if summary["events_by_kind"]:
        logger.info("  events by (zone, type):")
        for (zone, et), n in sorted(summary["events_by_kind"].items()):
            logger.info("    %-12s %-12s %d", zone, et, n)
    logger.info("=" * 60)


async def main() -> None:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=4)
    try:
        await run(pool)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
