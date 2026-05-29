# ingestions/aisstream.py
import asyncio
import json
import logging
import time
from dataclasses import dataclass, field

import asyncpg
import websockets
from rich.logging import RichHandler

from config import (
    MAIN_ZONES,
    SECONDARY_ZONES,
    bboxes_for_zones,
    settings,
)

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
# Subscription rotation: AISstream's throttle is keyed on the active subscription,
# and each new subscription message resets the bucket on the new bbox set. So we
# cycle between MAIN_ZONES (sustained ~3750/min) and SECONDARY_ZONES (fresh spike
# of ~370/min) on a single WebSocket. Effective rate: ~3160/min covering all 7
# zones, vs ~600/min if we stayed pinned to a 7-bbox subscription. See README.
MAIN_WINDOW_S = 300
SECONDARY_WINDOW_S = 60

MAIN_SOURCE = "aisstream-main"
SECONDARY_SOURCE = "aisstream-secondary"


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


def build_subscribe_payload(api_key: str, bboxes: list):
    return {
        "APIKey": api_key,
        "BoundingBoxes": bboxes,
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
    pool: asyncpg.Pool,
    ingest_state: IngestionState,
    enrich_state: EnrichmentState,
    zone_index: ZoneIndex,
) -> None:
    """One websocket lifecycle with subscription rotation.

    A single WebSocket alternates between MAIN_ZONES and SECONDARY_ZONES via
    in-place subscription updates. Each swap resets the throttle bucket on the
    new bbox set, so we harvest fresh spikes indefinitely while a single
    persistent connection covers all 7 zones.
    """
    main_bboxes = bboxes_for_zones(MAIN_ZONES)
    secondary_bboxes = bboxes_for_zones(SECONDARY_ZONES)

    # Mutable rotation state. The parser/flusher always references rot["agg"]
    # so messages get tagged to whichever subscription is currently active.
    rot = {
        "on_main": True,
        "source": MAIN_SOURCE,
        "agg": MinuteAggregator(source=MAIN_SOURCE),
    }

    logger.info("Connecting to aisstream.io...")
    await upsert_heartbeat(pool, "connecting")
    await record_event(pool, MAIN_SOURCE, "connect")
    async with websockets.connect(url, ping_timeout=None) as ws:
        await ws.send(json.dumps(build_subscribe_payload(settings.aisstream_api_key, main_bboxes)))
        logger.info(f"Subscribed: {MAIN_ZONES}")
        await upsert_heartbeat(pool, "connected")
        await record_event(
            pool, MAIN_SOURCE, "subscribed",
            {"zones": MAIN_ZONES, "window_s": MAIN_WINDOW_S, "initial": True},
        )
        rot["agg"].note_connection_start()

        last_message_time = time.monotonic()
        raw_q: asyncio.Queue = asyncio.Queue(maxsize=RAW_QUEUE_MAXSIZE)
        last_logged_fixes = ingest_state.fix_inserts

        async def drain_socket():
            """Pure: pull raw frames off the WS and onto the bounded queue."""
            nonlocal last_message_time
            async for raw in ws:
                last_message_time = time.monotonic()
                await raw_q.put(raw)

        async def parser():
            """CPU-only: parse, filter, append to buffers, route stats to the
            currently-active aggregator."""
            while True:
                raw = await raw_q.get()
                try:
                    parse_message(
                        raw, ingest_state, enrich_state, zone_index, rot["agg"]
                    )
                finally:
                    raw_q.task_done()

        async def flusher():
            """Periodically flush in-memory buffers + active aggregator's minute stats."""
            nonlocal last_logged_fixes
            while True:
                await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
                rot["agg"].observe_q_depth(raw_q.qsize())
                try:
                    await flush_buffers(pool, ingest_state)
                except Exception as e:
                    logger.warning(f"Flush failed: {e}")
                try:
                    await rot["agg"].maybe_flush(pool)
                except Exception as e:
                    logger.warning(f"Minute-stats flush failed: {e}")
                if ingest_state.fix_inserts // 1000 > last_logged_fixes // 1000:
                    logger.info(
                        f"[{rot['source']}] fixes={ingest_state.fix_inserts}, "
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
                        pool, rot["source"], "watchdog_reconnect",
                        {"silence_s": int(silence)},
                    )
                    await ws.close()

        async def heartbeat_loop():
            while True:
                await asyncio.sleep(10)
                try:
                    await upsert_heartbeat(pool, "connected")
                except Exception as e:
                    logger.warning(f"Heartbeat write failed: {e}")

        async def rotation_loop():
            """Cycle the subscription between main and secondary."""
            while True:
                window_s = MAIN_WINDOW_S if rot["on_main"] else SECONDARY_WINDOW_S
                await asyncio.sleep(window_s)

                # Flush the outgoing aggregator so its partial minute lands
                # tagged with the right source before we swap.
                try:
                    await rot["agg"].force_flush(pool)
                except Exception as e:
                    logger.warning(f"Rotation flush failed: {e}")

                # Swap subscription.
                rot["on_main"] = not rot["on_main"]
                if rot["on_main"]:
                    rot["source"] = MAIN_SOURCE
                    new_bboxes = main_bboxes
                    zones = MAIN_ZONES
                    new_window = MAIN_WINDOW_S
                else:
                    rot["source"] = SECONDARY_SOURCE
                    new_bboxes = secondary_bboxes
                    zones = SECONDARY_ZONES
                    new_window = SECONDARY_WINDOW_S

                rot["agg"] = MinuteAggregator(source=rot["source"])
                rot["agg"].note_connection_start()

                try:
                    await ws.send(
                        json.dumps(
                            build_subscribe_payload(settings.aisstream_api_key, new_bboxes)
                        )
                    )
                except Exception as e:
                    logger.warning(f"Subscription swap send failed: {e}")
                    return
                await record_event(
                    pool, rot["source"], "subscribed",
                    {"zones": zones, "window_s": new_window},
                )
                logger.info(
                    f"Rotated → {rot['source']} ({len(new_bboxes)} bboxes, {new_window}s)"
                )

        tasks = [
            asyncio.create_task(drain_socket()),
            asyncio.create_task(parser()),
            asyncio.create_task(flusher()),
            asyncio.create_task(watchdog()),
            asyncio.create_task(heartbeat_loop()),
            asyncio.create_task(rotation_loop()),
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
                await rot["agg"].force_flush(pool)
            except Exception as e:
                logger.warning(f"Minute-stats final flush failed: {e}")
            await record_event(pool, rot["source"], "disconnect")


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

    enrichment_task = asyncio.create_task(enrichment_worker(pool, enrich_state))

    ingest_state = IngestionState()

    try:
        while True:
            try:
                await connect_and_drain(
                    url, pool, ingest_state, enrich_state, zone_index,
                )
            except websockets.ConnectionClosed as e:
                logger.warning(f"Websocket closed: {e}. Reconnecting in 30s")
                await _try_heartbeat(pool, "reconnecting")
                await record_event(
                    pool, MAIN_SOURCE, "error",
                    {"kind": "ConnectionClosed", "msg": str(e)},
                )
                await asyncio.sleep(30)
            except Exception as e:
                logger.warning(f"Unexpected error: {e}. Reconnecting in 60s")
                await _try_heartbeat(pool, "reconnecting")
                await record_event(
                    pool, MAIN_SOURCE, "error",
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
