# ingestions/aisstream.py
import asyncio
import json
import logging
import time
from dataclasses import dataclass, field

import asyncpg
import websockets
from rich.logging import RichHandler

from config import AIS_BOUNDING_BOXES, settings

from datetime import datetime, timezone

from .dynamic_enrichment import (
    EnrichmentState,
    ZoneIndex,
    enrichment_worker,
    load_known_mmsis,
)
from .metrics import MinuteAggregator, classify_zone, record_event
from .models import AISMessage, PositionReport, ShipStaticData

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler()],
)
logger = logging.getLogger(__name__)


SILENCE_THRESHOLD_SECONDS = 45
RAW_QUEUE_MAXSIZE = 10000
FLUSH_INTERVAL_SECONDS = 0.5
RECONNECT_INTERVAL_SECONDS = 1800


TANKER_TYPES = set(range(80, 90))


@dataclass
class IngestionState:
    """MMSI filter set + counters mutated by the parser/flusher."""

    non_tanker_mmsis: set[int] = field(default_factory=set)
    fix_inserts: int = 0
    registry_upserts: int = 0
    state_inserts: int = 0

    # In-memory buffers populated by parser, drained by flusher.
    fix_buf: list[tuple] = field(default_factory=list)
    registry_buf: list[tuple] = field(default_factory=list)
    state_buf: list[tuple] = field(default_factory=list)


def build_subscribe_payload(api_key: str):
    return {
        "APIKey": api_key,
        "BoundingBoxes": AIS_BOUNDING_BOXES,
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }


async def _try_heartbeat(pool: asyncpg.Pool, status: str) -> None:
    try:
        await upsert_heartbeat(pool, status)
    except Exception as e:
        logger.warning(f"Heartbeat write failed ({status}): {e}")


async def upsert_heartbeat(pool: asyncpg.Pool, status: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO ingestion_heartbeat (source, status, last_heartbeat)
            VALUES ('aisstream', $1, now())
            ON CONFLICT (source) DO UPDATE SET
                status = EXCLUDED.status,
                last_heartbeat = EXCLUDED.last_heartbeat
            """,
            status,
        )


def parse_message(
    raw: str | bytes,
    ingest_state: IngestionState,
    enrich_state: EnrichmentState,
    zone_index: ZoneIndex,
    minute_agg: MinuteAggregator,
) -> None:
    """Pure-CPU: parse, filter, append to buffers, maybe queue for enrichment. No I/O."""
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

        enrich_state.maybe_queue(
            zone_index, mmsi, msg.Message.Longitude, msg.Message.Latitude
        )

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

        # Stats: zone, lag (wall-now − vessel broadcast ts), MMSI uniqueness.
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
    payload: dict,
    pool: asyncpg.Pool,
    ingest_state: IngestionState,
    enrich_state: EnrichmentState,
    zone_index: ZoneIndex,
    minute_agg: MinuteAggregator,
) -> None:
    """One websocket lifecycle: subscribe, fan out drain/parse/flush/heartbeat/watchdog tasks."""
    logger.info("Connecting to aisstream.io...")
    await upsert_heartbeat(pool, "connecting")
    await record_event(pool, "aisstream", "connect")
    async with websockets.connect(url, ping_timeout=None) as ws:
        await ws.send(json.dumps(payload))
        logger.info("Subscribed. Receiving messages...")
        await upsert_heartbeat(pool, "connected")
        await record_event(pool, "aisstream", "subscribed")
        minute_agg.note_connection_start()

        last_message_time = time.monotonic()
        raw_q: asyncio.Queue = asyncio.Queue(maxsize=RAW_QUEUE_MAXSIZE)
        last_logged_fixes = ingest_state.fix_inserts

        async def drain_socket():
            """Pure: pull raw frames off the WS and onto the bounded queue. No DB, no parsing."""
            nonlocal last_message_time
            async for raw in ws:
                last_message_time = time.monotonic()
                await raw_q.put(raw)

        async def parser():
            """CPU-only consumer: parse, filter, accumulate into ingest_state buffers."""
            while True:
                raw = await raw_q.get()
                try:
                    parse_message(
                        raw, ingest_state, enrich_state, zone_index, minute_agg
                    )
                finally:
                    raw_q.task_done()

        async def flusher():
            """Periodically flush in-memory buffers to DB, plus an early flush at threshold."""
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
                        f"fixes={ingest_state.fix_inserts}, "
                        f"registry={ingest_state.registry_upserts}, "
                        f"state={ingest_state.state_inserts}, "
                        f"raw_q={raw_q.qsize()}"
                    )
                    last_logged_fixes = ingest_state.fix_inserts

        async def watchdog():
            """Force reconnect if no message received for SILENCE_THRESHOLD_SECONDS."""
            while True:
                await asyncio.sleep(15)
                silence = time.monotonic() - last_message_time
                if silence > SILENCE_THRESHOLD_SECONDS:
                    logger.warning(
                        f"No messages for {silence:.0f}s — triggering reconnect"
                    )
                    await record_event(
                        pool, "aisstream", "watchdog_reconnect",
                        {"silence_s": int(silence)},
                    )
                    await ws.close()

        async def heartbeat_loop():
            """Periodically refresh the DB heartbeat while connected."""
            while True:
                await asyncio.sleep(10)
                try:
                    await upsert_heartbeat(pool, "connected")
                except Exception as e:
                    logger.warning(f"Heartbeat write failed: {e}")

        async def planned_reconnect():
            """Force a fresh WS after RECONNECT_INTERVAL_SECONDS.

            Why: AISstream appears to silently degrade per-connection vessel coverage over
            time (geometric decay from ~3000/min to ~1000/min over ~25 min on a stable
            connection with no client-side backpressure). A fresh connection resets whatever
            server-side state drives that. See investigation in the README.
            """
            await asyncio.sleep(RECONNECT_INTERVAL_SECONDS)
            logger.info(
                f"Planned reconnect after {RECONNECT_INTERVAL_SECONDS}s — closing ws"
            )
            await record_event(
                pool, "aisstream", "planned_reconnect",
                {"interval_s": RECONNECT_INTERVAL_SECONDS},
            )
            await ws.close()

        tasks = [
            asyncio.create_task(drain_socket()),
            asyncio.create_task(parser()),
            asyncio.create_task(flusher()),
            asyncio.create_task(watchdog()),
            asyncio.create_task(heartbeat_loop()),
            asyncio.create_task(planned_reconnect()),
        ]
        try:
            # If drain_socket exits (ws closed), unwind the rest.
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            # Final flush so we don't lose what's already in the buffers on reconnect.
            try:
                await flush_buffers(pool, ingest_state)
            except Exception as e:
                logger.warning(f"Final flush on disconnect failed: {e}")
            try:
                await minute_agg.force_flush(pool)
            except Exception as e:
                logger.warning(f"Minute-stats final flush failed: {e}")
            await record_event(pool, "aisstream", "disconnect")


async def ingest():
    pool = await asyncpg.create_pool(settings.database_url, min_size=2, max_size=5)
    logger.info("DB pool created")

    enrich_state = EnrichmentState(known_mmsis=await load_known_mmsis(pool))
    logger.info(
        f"Pre-loaded {len(enrich_state.known_mmsis)} known MMSIs from vessel_registry"
    )

    zone_index = await ZoneIndex.load(pool)
    logger.info(f"Loaded {len(zone_index.polys)} terminal zone polygons into STRtree")

    url = "wss://stream.aisstream.io/v0/stream"
    payload = build_subscribe_payload(settings.aisstream_api_key)
    ingest_state = IngestionState()
    minute_agg = MinuteAggregator(source="aisstream")

    enrichment_task = asyncio.create_task(enrichment_worker(pool, enrich_state))

    try:
        while True:
            try:
                await connect_and_drain(
                    url, payload, pool, ingest_state, enrich_state, zone_index,
                    minute_agg,
                )
            except websockets.ConnectionClosed as e:
                logger.warning(f"Websocket closed: {e}. Reconnecting in 30s")
                await _try_heartbeat(pool, "reconnecting")
                await record_event(
                    pool, "aisstream", "error",
                    {"kind": "ConnectionClosed", "msg": str(e)},
                )
                await asyncio.sleep(30)
            except Exception as e:
                logger.warning(f"Unexpected error: {e}. Reconnecting in 60s")
                await _try_heartbeat(pool, "reconnecting")
                await record_event(
                    pool, "aisstream", "error",
                    {"kind": type(e).__name__, "msg": str(e)},
                )
                await asyncio.sleep(60)
    finally:
        enrichment_task.cancel()
        await asyncio.gather(enrichment_task, return_exceptions=True)
        await pool.close()


def main():
    asyncio.run(ingest())


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Ingestion Stopped.")
