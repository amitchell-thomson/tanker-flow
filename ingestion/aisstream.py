# ingestions/aisstream.py
import asyncio
import json
import logging
from datetime import UTC, datetime

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


def build_subscribe_payload(api_key: str):
    return {
        "APIKey": api_key,
        "BoundingBoxes": BOUNDING_BOXES,
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }


async def insert_fix(conn: asyncpg.pool.PoolConnectionProxy, msg: PositionReport):
    """Extract PositionReport fields and insert a single fix"""
    ...


async def upsert_registry():
    """Extract static ShipStaticData + MetaData fields and add new ships/ upsert existing ones in the vessel registry"""
    ...


async def insert_state():
    """Extract voyage-specific ShipsStaticData fields and insert a vessel state record"""
    ...


async def handle_message(raw: str | bytes, pool: asyncpg.Pool):
    try:
        data = json.loads(raw)
        msg = AISMessage.model_validate(data).root

    except Exception as e:
        logger.warning(f"Discarding invalid message: {e}")
        return

    async with pool.acquire() as conn:
        if isinstance(msg, PositionReport):
            await insert_fix(conn, msg)
            logger.debug(
                f"Inserted fix: MMSI={msg.MetaData.MMSI}, Name={msg.MetaData.ShipName}"
            )

        elif isinstance(msg, ShipStaticData):
            await upsert_registry()
            await insert_state()


async def ingest():
    pool = await asyncpg.create_pool(settings.database_url, min_size=2, max_size=5)
    logger.info("DB pool created")

    url = "wss://stream.aisstream.io/v0/stream"
    payload = build_subscribe_payload(settings.aisstream_api_key)

    while True:
        try:
            logger.info("Connecting to aisstream.io...")
            async with websockets.connect(url) as ws:
                await ws.send(json.dumps(payload))
                logger.info("Subscribed. Receiving messages...")

                async for raw_message in ws:
                    await handle_message(raw_message, pool)

                    # print_json(
                    #     raw_message.decode("utf-8")
                    #     if isinstance(raw_message, bytes)
                    #    else raw_message
                    # )

        except websockets.ConnectionClosed as e:
            logger.warning(f"Websocket closed: {e}. Reconnecting in 30s")
            await asyncio.sleep(30)

        except Exception as e:
            logger.warning(f"Unexpected error: {e}. Reconnecting in 60s")
            await asyncio.sleep(60)


def main():
    asyncio.run(ingest())


if __name__ == "__main__":
    main()
