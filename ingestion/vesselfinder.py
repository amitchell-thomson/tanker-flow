import argparse
import asyncio
import json
import logging

import asyncpg
import httpx
from rich.logging import RichHandler

from config import settings

from .models import VesselFinderMasterdata, VesselFinderResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler()],
)
logger = logging.getLogger(__name__)

VF_API_BASE = "https://api.vesselfinder.com/masterdata"
RATE_LIMIT_DELAY = 1.0  # seconds between requests


PENDING_ALL = """
    SELECT mmsi, imo FROM vessel_registry
    WHERE imo IS NOT NULL AND imo != 0
      AND (vf_enrichment_status IS NULL OR vf_enrichment_status = 'error')
    ORDER BY mmsi
"""

PENDING_TERMINAL_ONLY = """
    SELECT DISTINCT vr.mmsi, vr.imo
    FROM vessel_registry vr
    JOIN ais_fixes af ON af.mmsi = vr.mmsi
    JOIN terminal_zones tz
      ON ST_Within(ST_SetSRID(ST_Point(af.lon, af.lat), 4326), tz.geom)
    WHERE vr.imo IS NOT NULL AND vr.imo != 0
      AND (vr.vf_enrichment_status IS NULL OR vr.vf_enrichment_status = 'error')
    ORDER BY vr.mmsi
"""


async def fetch_pending(pool: asyncpg.Pool, terminal_only: bool = False) -> list[asyncpg.Record]:
    query = PENDING_TERMINAL_ONLY if terminal_only else PENDING_ALL
    async with pool.acquire() as conn:
        return await conn.fetch(query)


async def update_registry(
    conn: asyncpg.pool.PoolConnectionProxy,
    mmsi: int,
    data: VesselFinderMasterdata,
    status: str,
) -> None:
    await conn.execute(
        """
        UPDATE vessel_registry SET
            flag              = $1,
            vf_vessel_type    = $2,
            year_built        = $3,
            builder           = $4,
            owner             = $5,
            manager           = $6,
            length_m          = $7,
            beam_m            = $8,
            gross_tonnage     = $9,
            net_tonnage       = $10,
            dwt               = $11,
            design_draught    = $12,
            teu               = $13,
            crude_capacity    = $14,
            gas_capacity_m3   = $15,
            is_lng_carrier    = COALESCE($2 = 'LNG Tanker', FALSE),
            -- 'Offshore Support Vessel' is a broad VF bucket (AHTS, PSV, FSRU, ...).
            -- Safe here only because aisstream filters to AIS type 80-89 (tankers) upstream,
            -- so the only OSVs reaching enrichment are vessels self-reporting as tankers — in
            -- practice, FSRUs. Revisit if TANKER_TYPES in aisstream.py is ever widened.
            is_fsru           = COALESCE($2 = 'Offshore Support Vessel', FALSE),
            enriched_at       = now(),
            vf_enrichment_status = $16,
            updated_at        = now()
        WHERE mmsi = $17
        """,
        data.FLAG,
        data.TYPE,
        data.BUILT,
        data.BUILDER,
        data.OWNER,
        data.MANAGER,
        data.LENGTH,
        data.BEAM,
        data.GT,
        data.NT,
        data.DWT,
        data.MAXDRAUGHT,
        data.TEU,
        data.CRUDE,
        data.GAS,
        status,
        mmsi,
    )


async def mark_status(
    conn: asyncpg.pool.PoolConnectionProxy,
    mmsi: int,
    status: str,
) -> None:
    await conn.execute(
        "UPDATE vessel_registry SET vf_enrichment_status = $1, updated_at = now() WHERE mmsi = $2",
        status,
        mmsi,
    )


async def _fetch_raw(client: httpx.AsyncClient, imo: int) -> httpx.Response:
    return await client.get(
        VF_API_BASE,
        params={"userkey": settings.vf_api_key, "imo": imo},
    )


async def fetch_masterdata(
    client: httpx.AsyncClient,
    imo: int,
) -> VesselFinderMasterdata | None:
    response = await _fetch_raw(client, imo)

    if response.status_code == 404:
        return None

    response.raise_for_status()

    items = response.json()
    if not items:
        return None

    return VesselFinderResponse.model_validate(items[0]).MASTERDATA


async def enrich_vessel(
    pool: asyncpg.Pool,
    client: httpx.AsyncClient,
    mmsi: int,
    imo: int,
) -> None:
    """Fetch + persist masterdata for one vessel, then sleep RATE_LIMIT_DELAY.

    The trailing sleep belongs here so every caller (batch and dynamic worker) honours the
    VesselFinder rate limit without having to remember to sleep themselves.
    """
    try:
        try:
            data = await fetch_masterdata(client, imo)
        except httpx.HTTPStatusError as e:
            logger.warning(
                f"MMSI={mmsi} IMO={imo}: HTTP {e.response.status_code} — marking error"
            )
            async with pool.acquire() as conn:
                await mark_status(conn, mmsi, "error")
            return
        except Exception as e:
            logger.warning(f"MMSI={mmsi} IMO={imo}: request failed ({e}) — marking error")
            async with pool.acquire() as conn:
                await mark_status(conn, mmsi, "error")
            return

        if data is None:
            logger.info(f"MMSI={mmsi} IMO={imo}: not found in VesselFinder")
            async with pool.acquire() as conn:
                await mark_status(conn, mmsi, "not_found")
            return

        async with pool.acquire() as conn:
            await update_registry(conn, mmsi, data, "ok")
        logger.info(f"MMSI={mmsi} IMO={imo}: enriched ({data.NAME}, {data.TYPE})")
    finally:
        await asyncio.sleep(RATE_LIMIT_DELAY)


async def enrich(limit: int | None = None, terminal_only: bool = False) -> None:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=3)
    logger.info("DB pool created")

    pending = await fetch_pending(pool, terminal_only=terminal_only)
    logger.info(f"{len(pending)} vessels pending enrichment")

    if not pending:
        await pool.close()
        return

    if limit is not None:
        pending = pending[:limit]
        logger.info(f"Limiting to {limit} vessels")

    async with httpx.AsyncClient(timeout=10.0) as client:
        for i, row in enumerate(pending, 1):
            mmsi, imo = row["mmsi"], row["imo"]
            logger.info(f"[{i}/{len(pending)}] MMSI={mmsi} IMO={imo}")
            await enrich_vessel(pool, client, mmsi, imo)

    await pool.close()
    logger.info("Enrichment complete")


async def probe(imo: int) -> None:
    """Fetch a single IMO, print raw response and parsed fields. No DB writes."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await _fetch_raw(client, imo)

    print(f"\n--- Raw response (status {response.status_code}) ---")
    try:
        print(json.dumps(response.json(), indent=2))
    except Exception:
        print(response.text)

    if not response.is_success:
        return

    items = response.json()
    if not items:
        print("\nEmpty response — IMO not found in VesselFinder.")
        return

    data = VesselFinderResponse.model_validate(items[0]).MASTERDATA
    print("\n--- Parsed model ---")
    print(data.model_dump_json(indent=2))


def main():
    parser = argparse.ArgumentParser(description="VesselFinder enrichment")
    parser.add_argument(
        "--probe",
        type=int,
        metavar="IMO",
        help="Fetch a single IMO and print the response without writing to the DB",
    )
    parser.add_argument(
        "--limit",
        type=int,
        metavar="N",
        help="Enrich at most N vessels (useful for testing)",
    )
    parser.add_argument(
        "--terminal-only",
        action="store_true",
        help="Only enrich vessels with fixes inside a terminal zone polygon",
    )
    args = parser.parse_args()

    try:
        if args.probe:
            asyncio.run(probe(args.probe))
        else:
            asyncio.run(enrich(limit=args.limit, terminal_only=args.terminal_only))
    except KeyboardInterrupt:
        logger.info("Stopped.")


if __name__ == "__main__":
    main()
