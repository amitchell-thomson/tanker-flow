import asyncio
import logging
from dataclasses import dataclass, field

import asyncpg
import httpx

from .vesselfinder import enrich_vessel

logger = logging.getLogger(__name__)


@dataclass
class EnrichmentState:
    """Tracks which MMSIs are already enriched, queued for enrichment, and the
    work queue itself.

    Originally this drove a passive discovery path: `maybe_queue()` watched
    every fix for an unknown MMSI inside a terminal_zones polygon and queued
    a VesselFinder lookup. With the move to server-side MMSI filtering on
    `ingestion/aisstream.py`, unknown MMSIs no longer flow through us at
    all, so the watch hook is gone — the worker remains so the existing
    batch path (`ingestion/vesselfinder.py --terminal-only`) and any future
    feed (e.g. a daily LNG-fleet refresh) can keep dropping MMSIs onto the
    queue.
    """

    known_mmsis: set[int] = field(default_factory=set)
    queued_mmsis: set[int] = field(default_factory=set)
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)


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
