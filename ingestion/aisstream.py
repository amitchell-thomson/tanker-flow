# ingestions/aisstream.py
import asyncio
import json
import logging
from datetime import UTC, datetime

import asyncpg
import websockets
from rich.logging import RichHandler

from config import settings

from .models import AISMessage

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


async def insert_fix(conn: asyncpg.pool.PoolConnectionProxy, msg: AISMessage):
    """Extract fields and insert a single fix"""
    await conn.execute(
        """
        INSERT INTO ais_fixes
            (server_ts, fix_ts, mmsi, lat, lon, nav_status, sog, draught, dest, eta)
        VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        """,
        datetime.now(tz=UTC),
        msg.MetaData.time_utc,
        msg.MetaData.MMSI,
        msg.Message.PositionReport.Latitude,
        msg.Message.PositionReport.Longitude,
        msg.Message.PositionReport.NavigationalStatus,
        msg.Message.PositionReport.Sog,
        msg.Message.ShipStaticData.MaximumStaticDraught,
        msg.Message.ShipStaticData.Destination,
        msg.Message.ShipStaticData.Eta,
    )


async def handle_message(raw: str | bytes, pool: asyncpg.Pool):
    try:
        data = json.loads(raw)
        msg = AISMessage.model_validate(data)

    except Exception as e:
        logger.warning(f"Discarding invalid message: {e}")
        return

    async with pool.acquire() as conn:
        await insert_fix(conn, msg)
        logger.debug(
            f"Inserted fix: MMSI={msg.MetaData.MMSI}, Name={msg.MetaData.ShipName}"
        )


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
