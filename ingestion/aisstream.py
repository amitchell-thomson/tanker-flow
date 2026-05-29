# ingestion/aisstream.py
import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import asyncpg
import websockets
from rich.logging import RichHandler

from config import settings

from pipeline import scoring

from .dynamic_enrichment import EnrichmentState, enrichment_worker, load_known_mmsis
from .metrics import MinuteAggregator, classify_zone, record_event
from .models import AISMessage, PositionReport, ShipStaticData

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler()],
)
logger = logging.getLogger(__name__)


# MMSI-filtered subscriptions are sparse — at ~50 MMSIs with variable broadcast
# cadence + terrestrial-AIS coverage gaps, going a couple of minutes without any
# of the 50 reporting is normal on a healthy connection. The watchdog should
# only fire if the connection is genuinely dead.
SILENCE_THRESHOLD_SECONDS = 300
# At ~150 fixes/min total across 3 connections (≈ <1 fix/s per connection), 1k
# is several minutes of headroom for any transient parser stall — plenty for the
# MMSI-filtered firehose, which is sparse by design.
RAW_QUEUE_MAXSIZE = 1000
FLUSH_INTERVAL_SECONDS = 0.5

# Plan: subscribe to specific LNG-carrier + FSRU MMSIs across N parallel WebSockets
# (server-side MMSI filter), instead of pulling all vessels in 7 wide bboxes and
# discarding 95% client-side. This sidesteps AISstream's per-account throttle —
# the previous bbox+rotation design was throttled to ~25-50% per-LNG-carrier
# visibility; MMSI filtering achieves ~100% on the priority list. See README.
NUM_CONNECTIONS = 3
MMSI_CAP_PER_CONNECTION = 50         # AISstream's documented limit

# Slot allocation: chunks 0 and 1 take the persistent block; chunk 2 is the
# scan-rotation connection that cycles through tier-4/5 vessels.
PERSISTENT_CONNECTIONS = 2
PERSISTENT_SLOTS = PERSISTENT_CONNECTIONS * MMSI_CAP_PER_CONNECTION   # 100
SCAN_CHUNK_INDEX = NUM_CONNECTIONS - 1
SCAN_SLOTS = MMSI_CAP_PER_CONNECTION                                  # 50

# Reconnect every hour. Each reconnect:
#   1. Triggers a fresh scoring run (see scoring_loop) just beforehand
#   2. Re-queries priority_watchlist for the current top-150
#   3. Closes + reopens the WebSocket with the new MMSI chunk
# The 1h cadence is what makes scan rotation work — chunk 2 swaps its 50
# vessels each cycle, cycling through ~650 tier-4/5 candidates over ~13h.
RECONNECT_INTERVAL_SECONDS = 3600

# Use full-globe bbox; the MMSI filter does the actual constraining.
GLOBAL_BBOX = [[[-85.0, -180.0], [85.0, 180.0]]]

TANKER_TYPES = set(range(80, 90))


@dataclass
class IngestionState:
    """Per-connection: MMSI filter set + counters + buffers."""

    non_tanker_mmsis: set[int] = field(default_factory=set)
    fix_inserts: int = 0
    registry_upserts: int = 0
    state_inserts: int = 0

    fix_buf: list[tuple] = field(default_factory=list)
    registry_buf: list[tuple] = field(default_factory=list)
    state_buf: list[tuple] = field(default_factory=list)


def build_subscribe_payload(api_key: str, mmsis: list[int]) -> dict:
    return {
        "APIKey": api_key,
        "BoundingBoxes": GLOBAL_BBOX,
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
        "FiltersShipMMSI": [str(m) for m in mmsis],
    }


async def load_persistent_mmsis(pool: asyncpg.Pool) -> list[int]:
    """Top PERSISTENT_SLOTS vessels from priority_watchlist (tier 1-3),
    ordered by (tier ASC, score DESC). Falls back to the cold-start query
    on the same shape if priority_watchlist is empty (e.g. first boot before
    the first scoring pass)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT mmsi FROM priority_watchlist
            WHERE tier <= 3
            ORDER BY tier ASC, score DESC
            LIMIT $1
            """,
            PERSISTENT_SLOTS,
        )
        if rows:
            return [r["mmsi"] for r in rows]
        # Cold start: priority_watchlist not yet populated. Fall back to the
        # is_lng_carrier OR is_fsru list ordered by recency so the ingester
        # still has something useful to subscribe to.
        logger.warning("priority_watchlist empty — using cold-start fallback")
        rows = await conn.fetch(
            """
            SELECT v.mmsi
            FROM vessel_registry v
            LEFT JOIN LATERAL (
                SELECT MAX(fix_ts) AS last_fix
                FROM ais_fixes a
                WHERE a.mmsi = v.mmsi AND a.fix_ts > now() - INTERVAL '90 days'
            ) f ON TRUE
            WHERE (v.is_lng_carrier OR v.is_fsru) AND NOT v.excluded
            ORDER BY f.last_fix DESC NULLS LAST, v.mmsi
            LIMIT $1
            """,
            PERSISTENT_SLOTS,
        )
    return [r["mmsi"] for r in rows]


async def load_scan_mmsis(pool: asyncpg.Pool) -> list[int]:
    """Next SCAN_SLOTS vessels from priority_watchlist (tier 4-5), ordered by
    (tier ASC, score ASC) so the stalest vessels rotate in first. With
    ~650 tier-4/5 candidates and 50 slots per 1h window, full coverage of the
    pool takes ~13h."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT mmsi FROM priority_watchlist
            WHERE tier >= 4
            ORDER BY tier ASC, score ASC
            LIMIT $1
            """,
            SCAN_SLOTS,
        )
    return [r["mmsi"] for r in rows]


async def mark_slot_assignments(
    pool: asyncpg.Pool, persistent: list[int], scan: list[int]
) -> None:
    """Write back which MMSIs won slots this cycle. Pure observability — the
    TUI reads in_slot/slot_kind to render the tier breakdown panel."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "UPDATE priority_watchlist SET in_slot = FALSE, slot_kind = NULL"
            )
            if persistent:
                await conn.execute(
                    "UPDATE priority_watchlist SET in_slot = TRUE, slot_kind = 'persistent' "
                    "WHERE mmsi = ANY($1::BIGINT[])",
                    persistent,
                )
            if scan:
                await conn.execute(
                    "UPDATE priority_watchlist SET in_slot = TRUE, slot_kind = 'scan' "
                    "WHERE mmsi = ANY($1::BIGINT[])",
                    scan,
                )


def chunk_persistent(mmsis: list[int], num_chunks: int) -> list[list[int]]:
    """Interleave persistent MMSIs across `num_chunks` chunks (the persistent
    connections). The input is already in tier-priority order; interleaving
    spreads activity evenly so no single connection is starved.
    """
    chunks: list[list[int]] = [[] for _ in range(num_chunks)]
    for i, m in enumerate(mmsis):
        chunks[i % num_chunks].append(m)
    return chunks


def parse_message(
    raw: str | bytes,
    ingest_state: IngestionState,
    minute_agg: MinuteAggregator,
) -> None:
    """Pure-CPU: parse, filter, append to buffers, observe stats. No I/O.

    No dynamic enrichment here — the MMSI filter means unknown MMSIs never
    flow through us in the first place. The `non_tanker_mmsis` defensive
    filter is kept as a cheap guard.
    """
    try:
        data = json.loads(raw)
        msg = AISMessage.model_validate(data).root
    except Exception as e:
        logger.warning(f"Discarding invalid message: {e}")
        return

    mmsi = msg.MetaData.MMSI

    if isinstance(msg, PositionReport):
        if mmsi in ingest_state.non_tanker_mmsis:
            return

        ingest_state.fix_buf.append(
            (
                msg.MetaData.time_utc,
                mmsi,
                msg.Message.Latitude,
                msg.Message.Longitude,
                msg.Message.NavigationalStatus,
                msg.Message.Sog,
                "aisstream",
            )
        )
        zone = classify_zone(msg.Message.Latitude, msg.Message.Longitude)
        lag_s = (datetime.now(timezone.utc) - msg.MetaData.time_utc).total_seconds()
        minute_agg.observe_fix(mmsi, msg.MetaData.time_utc, lag_s, zone)

    elif isinstance(msg, ShipStaticData):
        vessel_type = msg.Message.Type

        if vessel_type is not None and vessel_type not in TANKER_TYPES:
            ingest_state.non_tanker_mmsis.add(mmsi)
            return

        if vessel_type in TANKER_TYPES:
            ingest_state.non_tanker_mmsis.discard(mmsi)

        ingest_state.registry_buf.append(
            (
                mmsi,
                msg.Message.ImoNumber,
                msg.MetaData.ShipName,
                msg.Message.CallSign,
                msg.Message.Type,
            )
        )
        ingest_state.state_buf.append(
            (
                msg.MetaData.time_utc,
                mmsi,
                msg.Message.MaximumStaticDraught,
                msg.Message.Destination,
                json.dumps(msg.Message.Eta) if msg.Message.Eta is not None else None,
                "aisstream",
            )
        )


async def flush_buffers(pool: asyncpg.Pool, ingest_state: IngestionState) -> None:
    """Swap-and-write the in-memory buffers in one batched round-trip per table."""
    fix_batch = ingest_state.fix_buf
    registry_batch = ingest_state.registry_buf
    state_batch = ingest_state.state_buf
    if not fix_batch and not registry_batch and not state_batch:
        return
    ingest_state.fix_buf = []
    ingest_state.registry_buf = []
    ingest_state.state_buf = []

    async with pool.acquire() as conn:
        if fix_batch:
            try:
                await conn.executemany(
                    """
                    INSERT INTO ais_fixes
                        (fix_ts, mmsi, lat, lon, nav_status, sog, source)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (fix_ts, mmsi) DO NOTHING
                    """,
                    fix_batch,
                )
                ingest_state.fix_inserts += len(fix_batch)
            except Exception as e:
                logger.warning(f"Batch fix insert failed ({len(fix_batch)} rows): {e}")

        if registry_batch:
            try:
                await conn.executemany(
                    """
                    INSERT INTO vessel_registry
                        (mmsi, imo, vessel_name, call_sign, vessel_type)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (mmsi) DO UPDATE SET
                        vessel_name = EXCLUDED.vessel_name,
                        call_sign = EXCLUDED.call_sign,
                        vessel_type = EXCLUDED.vessel_type
                    """,
                    registry_batch,
                )
                ingest_state.registry_upserts += len(registry_batch)
            except Exception as e:
                logger.warning(
                    f"Batch registry upsert failed ({len(registry_batch)} rows): {e}"
                )

        if state_batch:
            try:
                await conn.executemany(
                    """
                    INSERT INTO vessel_state
                        (state_ts, mmsi, draught, dest, eta, source)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (state_ts, mmsi) DO NOTHING
                    """,
                    state_batch,
                )
                ingest_state.state_inserts += len(state_batch)
            except Exception as e:
                logger.warning(f"Batch state insert failed ({len(state_batch)} rows): {e}")


async def connect_and_drain(
    url: str,
    pool: asyncpg.Pool,
    ingest_state: IngestionState,
    mmsis: list[int],
    source_name: str,
) -> None:
    """One MMSI-filtered WebSocket lifecycle: subscribe + drain until disconnect.

    Tasks: drain_socket, parser, flusher, watchdog, planned_reconnect.
    No rotation — the MMSI filter is fixed for this connection's lifetime; a
    fresh chunk is loaded from vessel_registry on each reconnect. Liveness is
    derived downstream from `ingestion_stats_minute` per source (see viz/tui.py)
    rather than from a separate heartbeat table.
    """
    minute_agg = MinuteAggregator(source=source_name)
    payload = build_subscribe_payload(settings.aisstream_api_key, mmsis)

    logger.info(f"[{source_name}] Connecting to aisstream.io ({len(mmsis)} MMSIs)...")
    await record_event(
        pool, source_name, "connect", {"mmsi_count": len(mmsis)}
    )
    async with websockets.connect(url, ping_timeout=None) as ws:
        await ws.send(json.dumps(payload))
        logger.info(f"[{source_name}] Subscribed.")
        await record_event(
            pool, source_name, "subscribed", {"mmsi_count": len(mmsis)}
        )
        minute_agg.note_connection_start()

        last_message_time = time.monotonic()
        raw_q: asyncio.Queue = asyncio.Queue(maxsize=RAW_QUEUE_MAXSIZE)
        last_logged_fixes = ingest_state.fix_inserts

        async def drain_socket():
            nonlocal last_message_time
            async for raw in ws:
                last_message_time = time.monotonic()
                await raw_q.put(raw)

        async def parser():
            while True:
                raw = await raw_q.get()
                try:
                    parse_message(raw, ingest_state, minute_agg)
                finally:
                    raw_q.task_done()

        async def flusher():
            nonlocal last_logged_fixes
            while True:
                await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
                minute_agg.observe_q_depth(raw_q.qsize())
                try:
                    await flush_buffers(pool, ingest_state)
                except Exception as e:
                    logger.warning(f"Flush failed: {e}")
                try:
                    await minute_agg.maybe_flush(pool)
                except Exception as e:
                    logger.warning(f"Minute-stats flush failed: {e}")
                if ingest_state.fix_inserts // 1000 > last_logged_fixes // 1000:
                    logger.info(
                        f"[{source_name}] fixes={ingest_state.fix_inserts}, "
                        f"registry={ingest_state.registry_upserts}, "
                        f"state={ingest_state.state_inserts}, "
                        f"raw_q={raw_q.qsize()}"
                    )
                    last_logged_fixes = ingest_state.fix_inserts

        async def watchdog():
            while True:
                await asyncio.sleep(15)
                silence = time.monotonic() - last_message_time
                if silence > SILENCE_THRESHOLD_SECONDS:
                    logger.warning(
                        f"[{source_name}] No messages for {silence:.0f}s — triggering reconnect"
                    )
                    await record_event(
                        pool, source_name, "watchdog_reconnect",
                        {"silence_s": int(silence)},
                    )
                    await ws.close()

        async def planned_reconnect():
            """Force a fresh WS after RECONNECT_INTERVAL_SECONDS so the outer
            loop re-queries vessel_registry and picks up new MMSIs."""
            await asyncio.sleep(RECONNECT_INTERVAL_SECONDS)
            logger.info(
                f"[{source_name}] Planned reconnect after "
                f"{RECONNECT_INTERVAL_SECONDS}s — closing ws to refresh watchlist"
            )
            await record_event(
                pool, source_name, "planned_reconnect",
                {"interval_s": RECONNECT_INTERVAL_SECONDS},
            )
            await ws.close()

        tasks = [
            asyncio.create_task(drain_socket()),
            asyncio.create_task(parser()),
            asyncio.create_task(flusher()),
            asyncio.create_task(watchdog()),
            asyncio.create_task(planned_reconnect()),
        ]
        try:
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            try:
                await flush_buffers(pool, ingest_state)
            except Exception as e:
                logger.warning(f"Final flush on disconnect failed: {e}")
            try:
                await minute_agg.force_flush(pool)
            except Exception as e:
                logger.warning(f"Minute-stats final flush failed: {e}")
            await record_event(pool, source_name, "disconnect")


async def ingest():
    pool = await asyncpg.create_pool(settings.database_url, min_size=2, max_size=8)
    logger.info("DB pool created")

    enrich_state = EnrichmentState(known_mmsis=await load_known_mmsis(pool))
    logger.info(
        f"Pre-loaded {len(enrich_state.known_mmsis)} known MMSIs from vessel_registry"
    )

    url = "wss://stream.aisstream.io/v0/stream"

    # enrichment_worker drains the queue feeding VesselFinder lookups. Under
    # MMSI-only mode no new MMSIs get queued, but keeping the worker running
    # lets the existing batch path (`make enrich`) still feed it indirectly.
    enrichment_task = asyncio.create_task(enrichment_worker(pool, enrich_state))

    # Run scoring once before opening any sockets so the first reconnect has
    # a fresh priority_watchlist. Then re-run on the same 1h cadence as the
    # planned reconnects so that promoted vessels get persistent slots on the
    # very next cycle. If the first run fails (e.g. priority_watchlist not yet
    # migrated), log + continue — load_persistent_mmsis has a cold-start
    # fallback that keeps the ingester useful.
    try:
        await scoring.compute_and_upsert(pool)
    except Exception as e:
        logger.warning(f"Initial scoring run failed: {e}")

    async def scoring_loop():
        while True:
            await asyncio.sleep(RECONNECT_INTERVAL_SECONDS)
            try:
                await scoring.compute_and_upsert(pool)
            except Exception as e:
                logger.warning(f"Scoring run failed: {e}")

    scoring_task = asyncio.create_task(scoring_loop())

    async def connection_loop(source_name: str, chunk_index: int):
        """Reconnect loop owning one MMSI-filtered subscription. On each
        (re)connect:

        - chunk_index < SCAN_CHUNK_INDEX → persistent block (interleaved half
          of the top PERSISTENT_SLOTS by tier/score from priority_watchlist)
        - chunk_index == SCAN_CHUNK_INDEX → scan rotation (next SCAN_SLOTS
          oldest tier-4/5 candidates, rotating each 1h reconnect)
        """
        while True:
            try:
                if chunk_index == SCAN_CHUNK_INDEX:
                    my_mmsis = await load_scan_mmsis(pool)
                    persistent_mmsis = []  # set by other connections' loops, written below for observability only when we're a persistent conn
                else:
                    persistent_mmsis = await load_persistent_mmsis(pool)
                    chunks = chunk_persistent(persistent_mmsis, PERSISTENT_CONNECTIONS)
                    my_mmsis = chunks[chunk_index]
                    # Persistent conn 0 is the only one that writes the slot
                    # assignments — coordinated single-writer, no races.
                    if chunk_index == 0:
                        scan_mmsis = await load_scan_mmsis(pool)
                        try:
                            await mark_slot_assignments(
                                pool, persistent_mmsis, scan_mmsis
                            )
                        except Exception as e:
                            logger.warning(f"mark_slot_assignments failed: {e}")

                if not my_mmsis:
                    logger.warning(
                        f"[{source_name}] empty MMSI chunk; sleeping 60s"
                    )
                    await asyncio.sleep(60)
                    continue
                ingest_state = IngestionState()
                await connect_and_drain(
                    url, pool, ingest_state, my_mmsis, source_name
                )
            except websockets.ConnectionClosed as e:
                logger.warning(
                    f"[{source_name}] Websocket closed: {e}. Reconnecting in 30s"
                )
                await record_event(
                    pool, source_name, "error",
                    {"kind": "ConnectionClosed", "msg": str(e)},
                )
                await asyncio.sleep(30)
            except Exception as e:
                logger.warning(
                    f"[{source_name}] Unexpected error: {e}. Reconnecting in 60s"
                )
                await record_event(
                    pool, source_name, "error",
                    {"kind": type(e).__name__, "msg": str(e)},
                )
                await asyncio.sleep(60)

    try:
        await asyncio.gather(
            *[
                connection_loop(f"aisstream-mmsi-{i + 1}", i)
                for i in range(NUM_CONNECTIONS)
            ]
        )
    finally:
        for t in (enrichment_task, scoring_task):
            t.cancel()
        await asyncio.gather(enrichment_task, scoring_task, return_exceptions=True)
        await pool.close()


def main():
    asyncio.run(ingest())


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Ingestion Stopped.")
