"""One-shot bulk import of the IGU 2025 LNG fleet into vessel_registry.

The IGU CSV gives us IMOs; vessel_registry is keyed by MMSI; AISstream's
FiltersShipMMSI takes MMSI. The VF VESSELS endpoint (`/vessels?imo=...
&extradata=master`) returns both — IMO -> MMSI mapping plus master data plus
a current AIS snapshot (position, dest, ETA) — for 3 credits per IMO.

For each unknown-IMO from db/seed/lng_fleet_igu_2025.csv we:

1. Query VF VESSELS, sleep 1s for rate-limiting.
2. UPSERT vessel_registry by MMSI: fill IMO + master data + is_lng_carrier/
   is_fsru flags. If MMSI is new, INSERT a fresh row; if it already exists
   (we've seen the vessel via aisstream before), UPDATE missing fields only.
3. Insert the AIS snapshot into ais_fixes (source='vesselfinder') so the
   tier-scoring layer has a starting position for every vessel.
4. Insert dest/ETA/draught into vessel_state.

Usage:
    uv run python scripts/import_igu_fleet.py            # process all unknown IMOs
    uv run python scripts/import_igu_fleet.py --limit 10 # test on first 10
    uv run python scripts/import_igu_fleet.py --dry-run  # show what would happen
"""

import argparse
import asyncio
import csv
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncpg
import httpx
from rich.logging import RichHandler

from config import settings  # noqa: E402

VF_VESSELS_URL = "https://api.vesselfinder.com/vessels"
RATE_LIMIT_DELAY = 1.0

logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])
logger = logging.getLogger(__name__)


def load_igu_capacities(csv_path: Path) -> dict[int, int]:
    """IGU CSV → {imo: capacity_cm}. Used as a fallback when VF MASTERDATA.GAS
    is NULL (common for 2025-2026 newbuilds not yet in VF's masterdata table)."""
    out: dict[int, int] = {}
    with csv_path.open() as f:
        for r in csv.DictReader(f):
            if not r["imo"] or not r["capacity_cm"]:
                continue
            try:
                out[int(r["imo"])] = int(r["capacity_cm"])
            except ValueError:
                continue
    return out


async def load_unknown_imos(pool: asyncpg.Pool, csv_path: Path) -> list[int]:
    with csv_path.open() as f:
        igu_imos = {int(r["imo"]) for r in csv.DictReader(f) if r["imo"]}
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT imo FROM vessel_registry WHERE imo IS NOT NULL AND imo != 0"
        )
    known = {r["imo"] for r in rows}
    return sorted(igu_imos - known)


async def fetch_vessel(client: httpx.AsyncClient, imo: int) -> dict | None:
    """Call VF VESSELS endpoint for one IMO. Returns parsed dict or None on miss."""
    resp = await client.get(
        VF_VESSELS_URL,
        params={"userkey": settings.vf_api_key, "imo": imo, "extradata": "master"},
    )
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    body = resp.json()
    return body[0] if body else None


def parse_vf_timestamp(ts: str | None) -> datetime | None:
    """VF AIS TIMESTAMP arrives as '2026-05-21 09:42:29 UTC'."""
    if not ts:
        return None
    try:
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


async def upsert_registry(
    conn: asyncpg.pool.PoolConnectionProxy,
    ais: dict,
    master: dict,
    igu_capacity: int | None = None,
) -> None:
    """UPSERT vessel_registry by MMSI. Don't overwrite columns that have
    live data already; only fill what's NULL or set authoritative fields.

    `igu_capacity` is a fallback used only when VF MASTERDATA.GAS is NULL.
    Many 2025-2026 newbuilds are missing GAS in VF's table but have it in
    the IGU report.
    """
    vtype = master.get("TYPE")
    is_lng = vtype == "LNG Tanker"
    is_fsru = vtype == "Offshore Support Vessel"
    gas_capacity_m3 = master.get("GAS") or igu_capacity
    await conn.execute(
        """
        INSERT INTO vessel_registry (
            mmsi, imo, vessel_name, call_sign, vessel_type, flag,
            vf_vessel_type, year_built, builder, owner, manager,
            length_m, beam_m, gross_tonnage, net_tonnage, dwt,
            design_draught, teu, crude_capacity, gas_capacity_m3,
            is_lng_carrier, is_fsru,
            enriched_at, vf_enrichment_status, updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6,
            $7, $8, $9, $10, $11,
            $12, $13, $14, $15, $16,
            $17, $18, $19, $20,
            $21, $22,
            now(), 'ok', now()
        )
        ON CONFLICT (mmsi) DO UPDATE SET
            imo               = COALESCE(vessel_registry.imo, EXCLUDED.imo),
            vessel_name       = COALESCE(vessel_registry.vessel_name, EXCLUDED.vessel_name),
            call_sign         = COALESCE(vessel_registry.call_sign, EXCLUDED.call_sign),
            vessel_type       = COALESCE(vessel_registry.vessel_type, EXCLUDED.vessel_type),
            flag              = EXCLUDED.flag,
            vf_vessel_type    = EXCLUDED.vf_vessel_type,
            year_built        = EXCLUDED.year_built,
            builder           = EXCLUDED.builder,
            owner             = EXCLUDED.owner,
            manager           = EXCLUDED.manager,
            length_m          = EXCLUDED.length_m,
            beam_m            = EXCLUDED.beam_m,
            gross_tonnage     = EXCLUDED.gross_tonnage,
            net_tonnage       = EXCLUDED.net_tonnage,
            dwt               = EXCLUDED.dwt,
            design_draught    = EXCLUDED.design_draught,
            teu               = EXCLUDED.teu,
            crude_capacity    = EXCLUDED.crude_capacity,
            gas_capacity_m3   = EXCLUDED.gas_capacity_m3,
            is_lng_carrier    = EXCLUDED.is_lng_carrier,
            is_fsru           = EXCLUDED.is_fsru,
            enriched_at       = now(),
            vf_enrichment_status = 'ok',
            updated_at        = now()
        """,
        ais["MMSI"],
        ais.get("IMO") or master["IMO"],
        master.get("NAME") or ais.get("NAME"),
        ais.get("CALLSIGN"),
        ais.get("TYPE"),
        master.get("FLAG"),
        vtype,
        master.get("BUILT"),
        master.get("BUILDER"),
        master.get("OWNER"),
        master.get("MANAGER"),
        master.get("LENGTH"),
        master.get("BEAM"),
        master.get("GT"),
        master.get("NT"),
        master.get("DWT"),
        master.get("MAXDRAUGHT"),
        master.get("TEU"),
        master.get("CRUDE"),
        gas_capacity_m3,
        is_lng,
        is_fsru,
    )


async def insert_snapshot_fix(
    conn: asyncpg.pool.PoolConnectionProxy, ais: dict
) -> bool:
    """Insert the VF AIS snapshot into ais_fixes for tier-scoring kickstart.
    Returns True if a row was actually written."""
    ts = parse_vf_timestamp(ais.get("TIMESTAMP"))
    lat = ais.get("LATITUDE")
    lon = ais.get("LONGITUDE")
    if not (ts and lat is not None and lon is not None):
        return False
    result = await conn.execute(
        """
        INSERT INTO ais_fixes (server_ts, fix_ts, mmsi, lat, lon, nav_status, sog, source)
        VALUES (now(), $1, $2, $3, $4, $5, $6, 'vesselfinder')
        ON CONFLICT (fix_ts, mmsi) DO NOTHING
        """,
        ts, ais["MMSI"], lat, lon, ais.get("NAVSTAT"), ais.get("SPEED"),
    )
    return result.endswith(" 1")


async def insert_vessel_state(
    conn: asyncpg.pool.PoolConnectionProxy, ais: dict
) -> bool:
    """Insert dest/ETA/draught into vessel_state. ETA is stored as JSONB."""
    ts = parse_vf_timestamp(ais.get("TIMESTAMP"))
    if not ts:
        return False
    dest = ais.get("DESTINATION")
    eta = ais.get("ETA")
    draught = ais.get("DRAUGHT")
    if not any([dest, eta, draught]):
        return False
    await conn.execute(
        """
        INSERT INTO vessel_state (server_ts, state_ts, mmsi, draught, dest, eta, source)
        VALUES (now(), $1, $2, $3, $4, $5, 'vesselfinder')
        ON CONFLICT DO NOTHING
        """,
        ts, ais["MMSI"], draught, dest,
        json.dumps({"raw": eta}) if eta else None,
    )
    return True


async def process_one(
    pool: asyncpg.Pool,
    client: httpx.AsyncClient,
    imo: int,
    igu_capacities: dict[int, int],
) -> str:
    """Returns one of: 'ok', 'not_found', 'error', 'skip_no_mmsi', 'skip_dup'."""
    try:
        result = await fetch_vessel(client, imo)
    except httpx.HTTPStatusError as e:
        logger.warning(f"IMO={imo}: HTTP {e.response.status_code}")
        return "error"
    except Exception as e:
        logger.warning(f"IMO={imo}: request failed ({e})")
        return "error"

    if result is None:
        logger.info(f"IMO={imo}: not found in VesselFinder")
        return "not_found"

    ais = result.get("AIS") or {}
    master = result.get("MASTERDATA") or {}
    mmsi = ais.get("MMSI")
    if not mmsi:
        logger.warning(f"IMO={imo}: VF returned no MMSI")
        return "skip_no_mmsi"

    async with pool.acquire() as conn:
        async with conn.transaction():
            await upsert_registry(
                conn, ais, master, igu_capacity=igu_capacities.get(imo)
            )
            wrote_fix = await insert_snapshot_fix(conn, ais)
            wrote_state = await insert_vessel_state(conn, ais)

    name = master.get("NAME") or ais.get("NAME") or "?"
    vtype = master.get("TYPE") or "?"
    extras = []
    if wrote_fix:
        extras.append("fix")
    if wrote_state:
        extras.append("state")
    logger.info(
        f"IMO={imo} MMSI={mmsi}: {name} ({vtype})"
        + (f" [+{'+'.join(extras)}]" if extras else "")
    )
    return "ok"


async def run(csv_path: Path, limit: int | None, dry_run: bool) -> None:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=3)
    try:
        unknowns = await load_unknown_imos(pool, csv_path)
        igu_capacities = load_igu_capacities(csv_path)
        logger.info(
            f"{len(unknowns)} unknown IMOs to resolve "
            f"({len(igu_capacities)} have IGU-listed gas capacity as fallback)"
        )
        if limit is not None:
            unknowns = unknowns[:limit]
            logger.info(f"Limited to first {len(unknowns)}")

        if dry_run:
            logger.info("DRY RUN — no API calls, no DB writes")
            logger.info(f"Would call VF for: {unknowns[:5]} … (and {len(unknowns)-5} more)")
            return

        counts: dict[str, int] = {}
        async with httpx.AsyncClient(timeout=15.0) as client:
            for i, imo in enumerate(unknowns, 1):
                logger.info(f"[{i}/{len(unknowns)}] IMO={imo}")
                status = await process_one(pool, client, imo, igu_capacities)
                counts[status] = counts.get(status, 0) + 1
                await asyncio.sleep(RATE_LIMIT_DELAY)
        logger.info(f"Done. Outcomes: {counts}")
    finally:
        await pool.close()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--csv", type=Path, default=Path("db/seed/lng_fleet_igu_2025.csv"))
    p.add_argument("--limit", type=int)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    try:
        asyncio.run(run(args.csv, args.limit, args.dry_run))
    except KeyboardInterrupt:
        logger.info("Stopped.")


if __name__ == "__main__":
    main()
