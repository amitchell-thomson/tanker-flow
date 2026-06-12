"""NOAA ⋈ GFW reconciliation — the de-duplication that keeps US legs single.

This is the single highest-priority signal-correctness step of the historical
backfill (PLAN §3.7). Run it AFTER `gfw_events.py` loads and after every
`make port-events` rebuild, and BEFORE `make signals`.

────────────────────────────────────────────────────────────────────────────
The double-count it prevents
────────────────────────────────────────────────────────────────────────────
A US→EU laden voyage has its US *departure* witnessed twice:

  • by NOAA  — the real fix stream → state machine → a `departed` at the US export
               terminal, laden inferred from DRAUGHT (the authoritative version);
  • by GFW   — `gfw_events.py` also sees the US port visit and emits its own
               `departed` (laden from flow_direction).

If both survive into `port_events`, legs.py pairs the leg TWICE — NOAA-departed →
GFW-EU-arrival AND GFW-departed → GFW-EU-arrival — and `gas_in_transit_volume`
DOUBLES for every US-origin laden leg. The US berth visit doubles the same way
(`gas_loading_us`).

The two sources are complementary halves, not redundant copies: NOAA owns the US
*departure* endpoint (real draught), GFW owns the EU *arrival* endpoint (no free
raw AIS there). So the rule is:

  • keep NOAA's US events; suppress the GFW US events that NOAA already covers.
  • a US→EU leg then pairs NOAA-departed(US) → GFW-zone_entry(EU): one clean,
    fully-observed leg.

We do NOT blanket-delete GFW at US terminals — NOAA is ~77 % capture, and GFW
fills the gaps (pre-2016, NOAA gap days, the ~23 % NOAA missed). Only GFW US
visits that *match* a NOAA visit are dropped; unmatched GFW US visits survive and
add coverage.

────────────────────────────────────────────────────────────────────────────
The match
────────────────────────────────────────────────────────────────────────────
A GFW US visit ≙ a NOAA US visit when they share (mmsi, terminal_id) and their
`moored` times fall within MATCH_TOLERANCE_HOURS. A vessel does not re-visit the
same US terminal within ~2 days, so a 48 h window is unambiguous yet absorbs the
offset between GFW's anchorage-entry `start` and NOAA's berth `moored`. On a
match we delete that GFW visit's whole event set — its `zone_entry` and `moored`
(at the visit start) and its paired `departed` (at the visit end) — so no orphan
GFW endpoint is left to mis-pair.

Only export (US Gulf / US Atlantic) terminals are considered: NOAA never sees EU
import terminals, so a GFW EU visit can never be a duplicate.

Idempotent: deletes only; re-running after the rows are gone is a no-op, and it is
safe to re-run after each `make port-events` (NOAA events are regenerated, the
match is purely by mmsi/terminal/time).

Usage:
    uv run python -m ingestion.historical.reconcile
    uv run python -m ingestion.historical.reconcile --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import logging

import asyncpg

import config

logger = logging.getLogger("reconcile")

# A GFW US `moored` within this many hours of a NOAA `moored` for the same vessel
# and terminal is the same visit. Wide enough to bridge GFW anchorage-entry vs
# NOAA berth-moored timing, far below the gap between successive US visits.
MATCH_TOLERANCE_HOURS = 48

# Identify each GFW US-export visit (its moored) that matches a NOAA visit, and
# pull both sources' *departed* laden for the QC cross-check. PLAN §3.7's check is
# on the departure — off a US export terminal both NOAA's draught-laden and GFW's
# flow_direction-laden should read TRUE (loaded); a disagreement flags a bad
# draught read or a mis-typed terminal. (The mooring/arrival laden is NOT compared:
# there the two methods legitimately differ — a vessel can arrive with heel.)
MATCHED_VISITS_SQL = f"""
WITH gfw_visit AS (
    SELECT m.mmsi, m.terminal_id, m.event_time AS moored_ts,
           (SELECT d.event_time FROM port_events d
            WHERE d.source = 'gfw_events' AND d.event_type = 'departed'
              AND d.mmsi = m.mmsi AND d.terminal_id = m.terminal_id
              AND d.event_time >= m.event_time
            ORDER BY d.event_time LIMIT 1) AS departed_ts,
           (SELECT d.laden_flag FROM port_events d
            WHERE d.source = 'gfw_events' AND d.event_type = 'departed'
              AND d.mmsi = m.mmsi AND d.terminal_id = m.terminal_id
              AND d.event_time >= m.event_time
            ORDER BY d.event_time LIMIT 1) AS gfw_dep_laden
    FROM port_events m
    JOIN terminals t ON t.terminal_id = m.terminal_id
    WHERE m.source = 'gfw_events' AND m.event_type = 'moored'
      AND t.flow_direction = 'export'          -- US export = the NOAA-covered side
)
SELECT g.mmsi, g.terminal_id, g.moored_ts, g.departed_ts,
       g.gfw_dep_laden,
       (SELECT nd.laden_flag FROM port_events nd
        WHERE nd.source = 'noaa-ais' AND nd.event_type = 'departed'
          AND nd.mmsi = g.mmsi AND nd.terminal_id = g.terminal_id
        ORDER BY abs(extract(epoch FROM
            (nd.event_time - COALESCE(g.departed_ts, g.moored_ts))))
        LIMIT 1) AS noaa_dep_laden
FROM gfw_visit g
WHERE EXISTS (
    SELECT 1 FROM port_events n
    WHERE n.source = 'noaa-ais' AND n.event_type = 'moored'
      AND n.mmsi = g.mmsi AND n.terminal_id = g.terminal_id
      AND abs(extract(epoch FROM (n.event_time - g.moored_ts)))
          <= {MATCH_TOLERANCE_HOURS} * 3600
)
"""

# Delete each matched GFW visit's whole event set: zone_entry + moored (at the
# visit start) and the paired departed (at/after the start, through the visit end
# + 1h slack). Leaves NOAA's superior US events as the sole copy.
DELETE_SQL = f"""
WITH gfw_visit AS (
    SELECT m.id AS moored_id, m.mmsi, m.terminal_id, m.event_time AS moored_ts,
           (SELECT min(d.event_time) FROM port_events d
            WHERE d.source = 'gfw_events' AND d.event_type = 'departed'
              AND d.mmsi = m.mmsi AND d.terminal_id = m.terminal_id
              AND d.event_time >= m.event_time) AS departed_ts
    FROM port_events m
    JOIN terminals t ON t.terminal_id = m.terminal_id
    WHERE m.source = 'gfw_events' AND m.event_type = 'moored'
      AND t.flow_direction = 'export'
),
matched AS (
    SELECT g.* FROM gfw_visit g
    WHERE EXISTS (
        SELECT 1 FROM port_events n
        WHERE n.source = 'noaa-ais' AND n.event_type = 'moored'
          AND n.mmsi = g.mmsi AND n.terminal_id = g.terminal_id
          AND abs(extract(epoch FROM (n.event_time - g.moored_ts)))
              <= {MATCH_TOLERANCE_HOURS} * 3600
    )
)
DELETE FROM port_events p
USING matched g
WHERE p.source = 'gfw_events'
  AND p.mmsi = g.mmsi
  AND p.terminal_id = g.terminal_id
  AND p.event_time >= g.moored_ts - interval '1 hour'
  AND p.event_time <= COALESCE(g.departed_ts, g.moored_ts) + interval '1 hour'
"""


async def run(pool: asyncpg.Pool, *, dry_run: bool = False) -> None:
    async with pool.acquire() as conn:
        matches = await conn.fetch(MATCHED_VISITS_SQL)
        n = len(matches)
        # Free QC cross-check (PLAN §3.7): where both sources saw the US departure,
        # NOAA's draught-laden and GFW's flow_direction-laden should agree (both
        # TRUE off an export terminal). Disagreement flags a bad draught read.
        disagree = sum(
            1 for r in matches
            if r["gfw_dep_laden"] is not None and r["noaa_dep_laden"] is not None
            and r["gfw_dep_laden"] != r["noaa_dep_laden"]
        )
        logger.info(
            "%d GFW US-export visits match a NOAA visit (≤%dh); "
            "laden QC: %d/%d disagree.",
            n, MATCH_TOLERANCE_HOURS, disagree, n,
        )
        if dry_run:
            logger.info("[dry-run] would delete the %d matched GFW visits' events "
                        "(zone_entry + moored + departed each).", n)
            return
        deleted = await conn.execute(DELETE_SQL)
    logger.info("Deleted %s. GFW now contributes only the endpoints NOAA cannot "
                "see; US legs pair once.", deleted)


async def main() -> None:
    ap = argparse.ArgumentParser(
        description="Suppress GFW US-terminal events NOAA already covers (PLAN §3.7)."
    )
    ap.add_argument("--dry-run", action="store_true",
                    help="report matches + laden QC, delete nothing")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    pool = await asyncpg.create_pool(config.settings.database_url, min_size=1, max_size=4)
    try:
        await run(pool, dry_run=args.dry_run)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
