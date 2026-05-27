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

from .dynamic_enrichment import (
    EnrichmentState,
    enrichment_worker,
    load_known_mmsis,
)
from .models import AISMessage, PositionReport, ShipStaticData

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler()],
)
logger = logging.getLogger(__name__)


SILENCE_THRESHOLD_SECONDS = 45


TANKER_TYPES = set(range(80, 90))


@dataclass
class IngestionState:
    """MMSI filter set + counters mutated by handle_message."""

    non_tanker_mmsis: set[int] = field(default_factory=set)
    fix_inserts: int = 0
    registry_upserts: int = 0
    state_inserts: int = 0


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


async def insert_fix(conn: asyncpg.pool.PoolConnectionProxy, msg: PositionReport):
    """Extract PositionReport fields and insert a single fix"""
    await conn.execute(
        """
        INSERT INTO ais_fixes
            (fix_ts, mmsi, lat, lon, nav_status, sog, source)
        VALUES
            ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (fix_ts, mmsi) DO NOTHING
        """,
        msg.MetaData.time_utc,
        msg.MetaData.MMSI,
        msg.Message.Latitude,
        msg.Message.Longitude,
        msg.Message.NavigationalStatus,
        msg.Message.Sog,
        "aisstream",
    )


async def upsert_registry(conn: asyncpg.pool.PoolConnectionProxy, msg: ShipStaticData):
    """Extract static ShipStaticData + MetaData fields and add new ships/ upsert existing ones in the vessel registry"""
    await conn.execute(
        """
        INSERT INTO vessel_registry
            (mmsi, imo, vessel_name, call_sign, vessel_type)
        VALUES
            ($1, $2, $3, $4, $5)
        ON CONFLICT (mmsi) DO UPDATE SET
            vessel_name = EXCLUDED.vessel_name,
            call_sign = EXCLUDED.call_sign,
            vessel_type = EXCLUDED.vessel_type
        """,
        msg.MetaData.MMSI,
        msg.Message.ImoNumber,
        msg.MetaData.ShipName,
        msg.Message.CallSign,
        msg.Message.Type,
    )


async def insert_state(conn: asyncpg.pool.PoolConnectionProxy, msg: ShipStaticData):
    """Extract voyage-specific ShipsStaticData fields and insert a vessel state record"""
    await conn.execute(
        """
        INSERT INTO vessel_state
            (state_ts, mmsi, draught, dest, eta, source)
        VALUES 
            ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (state_ts, mmsi) DO NOTHING
        """,
        msg.MetaData.time_utc,
        msg.MetaData.MMSI,
        msg.Message.MaximumStaticDraught,
        msg.Message.Destination,
        json.dumps(msg.Message.Eta) if msg.Message.Eta is not None else None,
        "aisstream",
    )


async def handle_message(
    raw: str | bytes,
    pool: asyncpg.Pool,
    ingest_state: IngestionState,
    enrich_state: EnrichmentState,
):
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

        async with pool.acquire() as conn:
            await enrich_state.maybe_queue(
                conn, mmsi, msg.Message.Longitude, msg.Message.Latitude
            )

            try:
                await insert_fix(conn, msg)
                ingest_state.fix_inserts += 1
                if ingest_state.fix_inserts % 1000 == 0:
                    logger.info(
                        f"fixes={ingest_state.fix_inserts}, registry={ingest_state.registry_upserts}, state={ingest_state.state_inserts}"
                    )
                logger.debug(f"Inserted fix: MMSI={mmsi}, Name={msg.MetaData.ShipName}")

            except Exception as e:
                logger.warning(f"Failed to insert fix MMSI={mmsi}: {e}")

    elif isinstance(msg, ShipStaticData):
        vessel_type = msg.Message.Type

        if vessel_type is not None and vessel_type not in TANKER_TYPES:
            ingest_state.non_tanker_mmsis.add(mmsi)
            return

        if vessel_type in TANKER_TYPES:
            ingest_state.non_tanker_mmsis.discard(mmsi)

        async with pool.acquire() as conn:
            try:
                await upsert_registry(conn, msg)
                ingest_state.registry_upserts += 1
                logger.debug(
                    f"Upserted ship: MMSI={mmsi}, Name={msg.MetaData.ShipName}"
                )

            except Exception as e:
                logger.warning(f"Failed to upsert registry MMSI={mmsi}: {e}")
                return

            try:
                await insert_state(conn, msg)
                ingest_state.state_inserts += 1
                logger.debug(
                    f"Inserted state: MMSI={mmsi}, Name={msg.MetaData.ShipName}"
                )

            except Exception as e:
                logger.warning(f"Failed to insert state MMSI={mmsi}: {e}")


async def connect_and_drain(
    url: str,
    payload: dict,
    pool: asyncpg.Pool,
    ingest_state: IngestionState,
    enrich_state: EnrichmentState,
) -> None:
    """One websocket lifecycle: subscribe, run watchdog + heartbeat, drain messages until disconnect."""
    logger.info("Connecting to aisstream.io...")
    await upsert_heartbeat(pool, "connecting")
    async with websockets.connect(url, ping_timeout=None) as ws:
        await ws.send(json.dumps(payload))
        logger.info("Subscribed. Receiving messages...")
        await upsert_heartbeat(pool, "connected")

        last_message_time = time.monotonic()

        async def watchdog():
            """Force reconnect if no message received for SILENCE_THRESHOLD_SECONDS."""
            while True:
                await asyncio.sleep(15)
                silence = time.monotonic() - last_message_time
                if silence > SILENCE_THRESHOLD_SECONDS:
                    logger.warning(
                        f"No messages for {silence:.0f}s — triggering reconnect"
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

        watchdog_task = asyncio.create_task(watchdog())
        heartbeat_task = asyncio.create_task(heartbeat_loop())
        try:
            async for raw_message in ws:
                last_message_time = time.monotonic()
                await handle_message(raw_message, pool, ingest_state, enrich_state)
        finally:
            watchdog_task.cancel()
            heartbeat_task.cancel()
            await asyncio.gather(
                watchdog_task, heartbeat_task, return_exceptions=True
            )


async def ingest():
    pool = await asyncpg.create_pool(settings.database_url, min_size=2, max_size=5)
    logger.info("DB pool created")

    enrich_state = EnrichmentState(known_mmsis=await load_known_mmsis(pool))
    logger.info(
        f"Pre-loaded {len(enrich_state.known_mmsis)} known MMSIs from vessel_registry"
    )

    url = "wss://stream.aisstream.io/v0/stream"
    payload = build_subscribe_payload(settings.aisstream_api_key)
    ingest_state = IngestionState()

    enrichment_task = asyncio.create_task(enrichment_worker(pool, enrich_state))

    try:
        while True:
            try:
                await connect_and_drain(url, payload, pool, ingest_state, enrich_state)
            except websockets.ConnectionClosed as e:
                logger.warning(f"Websocket closed: {e}. Reconnecting in 30s")
                await _try_heartbeat(pool, "reconnecting")
                await asyncio.sleep(30)
            except Exception as e:
                logger.warning(f"Unexpected error: {e}. Reconnecting in 60s")
                await _try_heartbeat(pool, "reconnecting")
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
