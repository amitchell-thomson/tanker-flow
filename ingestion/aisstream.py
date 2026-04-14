# ingestions/aisstream.py
import asyncio
import json
import logging

import asyncpg
import websockets
from rich.logging import RichHandler

from config import settings

from .models import AISMessage, PositionReport, ShipStaticData

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler()],
)
logger = logging.getLogger(__name__)


BOUNDING_BOXES = [
    [[25.0, -98.0], [31.0, -88.0]],
    [[51.0, -5.0], [58.0, 9.0]],
]

TANKER_TYPES = set(range(80, 90))


def build_subscribe_payload(api_key: str):
    return {
        "APIKey": api_key,
        "BoundingBoxes": BOUNDING_BOXES,
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }


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
    tanker_mmsis: set[int],
    non_tanker_mmsis: set[int],
):
    try:
        data = json.loads(raw)
        msg = AISMessage.model_validate(data).root

    except Exception as e:
        logger.warning(f"Discarding invalid message: {e}")
        return

    mmsi = msg.MetaData.MMSI

    if isinstance(msg, PositionReport):
        if mmsi in non_tanker_mmsis:
            return

        async with pool.acquire() as conn:
            try:
                await insert_fix(conn, msg)
                logger.debug(f"Inserted fix: MMSI={mmsi}, Name={msg.MetaData.ShipName}")

            except Exception as e:
                logger.warning(f"Failed to insert fix MMSI={mmsi}: {e}")

    elif isinstance(msg, ShipStaticData):
        vessel_type = msg.Message.Type

        if (
            vessel_type is not None
            and vessel_type != 0
            and vessel_type not in TANKER_TYPES
        ):
            non_tanker_mmsis.add(mmsi)
            return

        if vessel_type in TANKER_TYPES:
            tanker_mmsis.add(mmsi)
            non_tanker_mmsis.discard(mmsi)

        async with pool.acquire() as conn:
            try:
                await upsert_registry(conn, msg)
                logger.debug(
                    f"Upserted ship: MMSI={mmsi}, Name={msg.MetaData.ShipName}"
                )

            except Exception as e:
                logger.warning(f"Failed to upsert registry MMSI={mmsi}: {e}")
                return

            try:
                await insert_state(conn, msg)
                logger.debug(
                    f"Inserted state: MMSI={mmsi}, Name={msg.MetaData.ShipName}"
                )

            except Exception as e:
                logger.warning(f"Failed to insert state MMSI={mmsi}: {e}")


async def ingest():
    pool = await asyncpg.create_pool(settings.database_url, min_size=2, max_size=5)
    logger.info("DB pool created")

    url = "wss://stream.aisstream.io/v0/stream"
    payload = build_subscribe_payload(settings.aisstream_api_key)

    tanker_mmsis = set()
    non_tanker_mmsis = set()
    try:
        while True:
            try:
                logger.info("Connecting to aisstream.io...")
                async with websockets.connect(url) as ws:
                    await ws.send(json.dumps(payload))
                    logger.info("Subscribed. Receiving messages...")

                    async for raw_message in ws:
                        await handle_message(
                            raw_message, pool, tanker_mmsis, non_tanker_mmsis
                        )

            except websockets.ConnectionClosed as e:
                logger.warning(f"Websocket closed: {e}. Reconnecting in 30s")
                await asyncio.sleep(30)

            except Exception as e:
                logger.warning(f"Unexpected error: {e}. Reconnecting in 60s")
                await asyncio.sleep(60)
    finally:
        await pool.close()


def main():
    asyncio.run(ingest())


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Ingestion Stopped.")
