import asyncio
import logging
from dataclasses import dataclass, field

import asyncpg
import httpx
from shapely import wkb
from shapely.geometry import Point
from shapely.strtree import STRtree

from .vesselfinder import enrich_vessel

logger = logging.getLogger(__name__)


@dataclass
class ZoneIndex:
    """In-process spatial index over terminal_zones, replacing per-message ST_Within calls.

    Why: the previous DB-round-trip-per-PositionReport was the dominant cost on the ingestion
    hot path, causing TCP backpressure on the AISstream socket and (we suspect) provoking
    server-side pruning of our vessel subscription. See investigation in the README.
    """

    tree: STRtree
    polys: list

    @classmethod
    async def load(cls, pool: asyncpg.Pool) -> "ZoneIndex":
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT ST_AsBinary(geom) AS wkb FROM terminal_zones")
        polys = [wkb.loads(bytes(r["wkb"])) for r in rows]
        return cls(tree=STRtree(polys), polys=polys)

    def contains(self, lon: float, lat: float) -> bool:
        p = Point(lon, lat)
        for idx in self.tree.query(p):
            if self.polys[idx].contains(p):
                return True
        return False


@dataclass
class EnrichmentState:
    """Tracks which MMSIs are already enriched, queued for enrichment, and the work queue itself."""

    known_mmsis: set[int] = field(default_factory=set)
    queued_mmsis: set[int] = field(default_factory=set)
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)

    def maybe_queue(
        self,
        zone_index: ZoneIndex,
        mmsi: int,
        lon: float,
        lat: float,
    ) -> None:
        """Queue an MMSI for enrichment if unseen and its fix falls inside a terminal zone."""
        if mmsi in self.known_mmsis or mmsi in self.queued_mmsis:
            return
        if zone_index.contains(lon, lat):
            self.queue.put_nowait(mmsi)
            self.queued_mmsis.add(mmsi)
            logger.debug(f"Queued enrichment: MMSI={mmsi} inside terminal zone")


async def load_known_mmsis(pool: asyncpg.Pool) -> set[int]:
    """All MMSIs that already have a terminal enrichment status — skip re-queueing these.

    Includes vf_enrichment_status='error' rows: a failed enrichment lands in known_mmsis here
    and stays there for the session, so the dynamic path won't hammer VF with retries on every
    new fix. Errored vessels are retried only via the batch path (`vesselfinder --terminal-only`).
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT mmsi FROM vessel_registry WHERE vf_enrichment_status IS NOT NULL"
        )
    return {row["mmsi"] for row in rows}


async def enrichment_worker(pool: asyncpg.Pool, state: EnrichmentState) -> None:
    """Drains the enrichment queue, calling VesselFinder once per MMSI subject to rate limit."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            mmsi = await state.queue.get()
            try:
                async with pool.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT imo FROM vessel_registry WHERE mmsi = $1", mmsi
                    )
                    if not row:
                        # ShipStaticData not yet received; allow re-queue once IMO is known
                        state.queued_mmsis.discard(mmsi)
                        imo = None
                    elif not row["imo"]:
                        # IMO is NULL or 0 — sub-IMO vessel (typically < 300 GT). Won't match
                        # in VesselFinder; mark terminal so we never re-queue this MMSI.
                        await conn.execute(
                            "UPDATE vessel_registry SET vf_enrichment_status = 'no_imo', "
                            "updated_at = now() WHERE mmsi = $1",
                            mmsi,
                        )
                        state.known_mmsis.add(mmsi)
                        imo = None
                    else:
                        imo = row["imo"]
                if imo:
                    await enrich_vessel(pool, client, mmsi, imo)
                    state.known_mmsis.add(mmsi)
            except Exception as e:
                logger.warning(f"Dynamic enrichment error MMSI={mmsi}: {e}")
                state.queued_mmsis.discard(mmsi)
            finally:
                state.queue.task_done()
