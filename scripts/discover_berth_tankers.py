"""Phase-2 auto-add: register unknown tankers caught sitting in an LNG berth.

Under the closed-loop MMSI filter the only unknown vessels we ever hear are the
ones the Stage-3c bbox catch-all picks up on a terminal geofence (see
ingestion/aisstream.py:_capture_discovery_candidate). Those land in
`discovery_candidates`. The vast majority are product/chemical tankers loitering
*near* a terminal — type-80 tankers, but not in an LNG berth. This worker applies
the deterministic gate: a tanker whose latest position is *inside an LNG berth
polygon* (terminal_zones.zone_type='berth') is almost certainly an LNG carrier we
never registered. It is VF-enriched, and registered into vessel_registry **iff VF
confirms MASTERDATA.TYPE = 'LNG Tanker'** — the type-check is the final gate, so a
non-LNG tanker that clipped a berth is recorded and dropped, not added.

Cost / self-limiting:
  - VF VESSELS is keyed by IMO and billed per returned record (3 credits). A real
    LNG carrier always broadcasts a valid IMO from day one, so imo=0 candidates
    (sub-IMO coasters) are never LNG and are skipped without a call.
  - Each candidate is VF-checked at most once: the result (registered/not_lng/
    no_master) is written back to discovery_candidates.resolved_at as a negative
    cache, so a non-LNG tanker that sits in a berth for days costs one credit, not
    one per run.
  - Spend is logged to vf_rescue_log (rescue_class='berth_discovery') so it
    reconciles against the shared rescue/discovery glide budget. There is no
    budget *gate* — the request is "ALWAYS enrich an unknown tanker in an LNG
    berth" — but BERTH_DISCOVERY_MAX_PER_RUN brakes a one-off backlog so a
    mis-drawn berth polygon can't blow the reserve in a single run.

Run via `make discover-berths` (`make discover-berths-dry` to preview) and as a
periodic background task inside aisstream.py (settings.run_berth_discovery).
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncpg  # noqa: E402
import httpx  # noqa: E402
from rich.logging import RichHandler  # noqa: E402

from config import settings  # noqa: E402
from ingestion.vf_rescue import update_account_status  # noqa: E402
from scripts.import_igu_fleet import (  # noqa: E402
    RATE_LIMIT_DELAY,
    fetch_vessel,
    insert_snapshot_fix,
    insert_vessel_state,
    upsert_registry,
)

logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])
logger = logging.getLogger(__name__)

# A VF VESSELS hit returns one record = 3 credits (master+AIS), whether or not the
# vessel turns out to be an LNG carrier. A 404/empty (vessel unknown to VF) is free.
VF_RECORD_CREDITS = 3
# One-off-backlog brake (NOT a steady-state throttle — the negative cache means a
# settled system checks ~0-2 new candidates per run). Protects the credit reserve
# if a berth polygon is mis-drawn and suddenly contains a crowd of through-traffic.
BERTH_DISCOVERY_MAX_PER_RUN = 25

# Unknown tankers whose latest fix is inside an LNG berth polygon, not yet
# VF-checked, not already registered, with a usable IMO. DISTINCT ON dedupes a
# point that lands in two overlapping berth sub_zones.
BERTH_CANDIDATE_SQL = """
SELECT DISTINCT ON (c.mmsi)
       c.mmsi, c.imo, c.ship_name, t.terminal_name
FROM discovery_candidates c
JOIN terminal_zones tz
  ON tz.zone_type = 'berth'
 AND ST_Within(ST_SetSRID(ST_Point(c.lon, c.lat), 4326), tz.geom)
JOIN terminals t ON t.terminal_id = tz.terminal_id
WHERE c.resolved_at IS NULL
  AND c.imo IS NOT NULL AND c.imo <> 0
  AND c.lat IS NOT NULL AND c.lon IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM vessel_registry vr WHERE vr.mmsi = c.mmsi)
ORDER BY c.mmsi, c.last_seen DESC
"""

MARK_RESOLVED_SQL = """
UPDATE discovery_candidates
SET resolved_at = now(), outcome = $2, vf_type = $3
WHERE mmsi = $1
"""

LOG_BERTH_SQL = """
INSERT INTO vf_rescue_log (
    mmsi, imo, vessel_name, rescue_class, src, result, credits,
    requested_imos, returned_rows
)
VALUES ($1, $2, $3, 'berth_discovery', 'TER', $4, $5, 1, $6)
"""


def classify_vf_result(result: dict | None) -> tuple[bool, str | None, int]:
    """Pure decision on a VF VESSELS result: (is_lng, vf_type, credits_billed).

    A returned record is billed VF_RECORD_CREDITS regardless of type; a miss
    (None/empty) is free. is_lng is the registration gate — VF's MASTERDATA.TYPE
    must be exactly 'LNG Tanker' (mirrors import_igu_fleet.upsert_registry).
    """
    if not result:
        return (False, None, 0)
    vf_type = (result.get("MASTERDATA") or {}).get("TYPE")
    return (vf_type == "LNG Tanker", vf_type, VF_RECORD_CREDITS)


async def load_berth_candidates(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch(BERTH_CANDIDATE_SQL)


async def run(dry_run: bool) -> None:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=3)
    try:
        candidates = await load_berth_candidates(pool)
        if not candidates:
            logger.info("No unknown tankers in an LNG berth to resolve")
            return

        logger.info(
            f"{len(candidates)} unknown tanker(s) in an LNG berth: "
            + ", ".join(
                f"{r['ship_name'] or '?'} (IMO {r['imo']}) @ {r['terminal_name']}"
                for r in candidates[:10]
            )
            + (" ..." if len(candidates) > 10 else "")
        )

        if dry_run:
            est = min(len(candidates), BERTH_DISCOVERY_MAX_PER_RUN) * VF_RECORD_CREDITS
            logger.info(
                f"[dry-run] would VF-check up to "
                f"{min(len(candidates), BERTH_DISCOVERY_MAX_PER_RUN)} "
                f"(≤{est} credits if all return a record); no spend"
            )
            return

        batch = candidates[:BERTH_DISCOVERY_MAX_PER_RUN]
        if len(candidates) > BERTH_DISCOVERY_MAX_PER_RUN:
            logger.warning(
                f"Capping at {BERTH_DISCOVERY_MAX_PER_RUN}/run "
                f"({len(candidates) - BERTH_DISCOVERY_MAX_PER_RUN} deferred to next run)"
            )

        registered = not_lng = no_master = errors = 0
        async with httpx.AsyncClient(timeout=15.0) as client:
            for rec in batch:
                mmsi, imo, name = rec["mmsi"], rec["imo"], rec["ship_name"]
                try:
                    result = await fetch_vessel(client, imo)
                except Exception as e:
                    logger.warning(f"IMO={imo} ({name}): VF request failed ({e})")
                    async with pool.acquire() as conn:
                        await conn.execute(MARK_RESOLVED_SQL, mmsi, "error", None)
                    errors += 1
                    await asyncio.sleep(RATE_LIMIT_DELAY)
                    continue

                is_lng, vf_type, credits = classify_vf_result(result)
                did_register = False
                if is_lng:
                    ais = result.get("AIS") or {}
                    master = result.get("MASTERDATA") or {}
                    if ais.get("MMSI"):
                        async with pool.acquire() as conn, conn.transaction():
                            await upsert_registry(conn, ais, master)
                            await insert_snapshot_fix(conn, ais)
                            await insert_vessel_state(conn, ais)
                        did_register = True
                    else:
                        logger.warning(
                            f"IMO={imo} ({name}): VF says LNG Tanker but no AIS MMSI; "
                            "cannot register"
                        )

                if did_register:
                    outcome, result_label = "registered", "rescued"
                    registered += 1
                    logger.info(
                        f"REGISTERED MMSI={ais.get('MMSI')} IMO={imo}: "
                        f"{master.get('NAME') or name or '?'} (LNG Tanker)"
                    )
                elif credits:  # a record came back, just not a registerable LNG carrier
                    outcome, result_label = "not_lng", "not_lng"
                    not_lng += 1
                    logger.info(
                        f"NOT LNG MMSI={mmsi} IMO={imo} ({name}): VF type={vf_type}"
                    )
                else:  # 404/empty — VF doesn't know this IMO
                    outcome, result_label = "no_master", None
                    no_master += 1
                    logger.info(f"NO MASTER MMSI={mmsi} IMO={imo} ({name}): VF miss")

                async with pool.acquire() as conn:
                    await conn.execute(MARK_RESOLVED_SQL, mmsi, outcome, vf_type)
                    if credits:  # free misses aren't a spend, don't log to the ledger
                        await conn.execute(
                            LOG_BERTH_SQL, mmsi, imo, name, result_label, credits, 1
                        )
                await asyncio.sleep(RATE_LIMIT_DELAY)

            # Refresh the free balance snapshot so the next rescue/discovery pass
            # sees this spend in its glide accounting.
            if registered or not_lng:
                await update_account_status(pool, client)

        logger.info(
            f"Berth discovery done: registered={registered} not_lng={not_lng} "
            f"no_master={no_master} errors={errors}"
        )
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Register unknown tankers caught in an LNG berth (Phase-2 auto-add)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List candidates + est cost, no VF spend",
    )
    args = parser.parse_args()
    asyncio.run(run(args.dry_run))


if __name__ == "__main__":
    main()
