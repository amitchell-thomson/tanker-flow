# ingestions/aisstream.py
import asyncio
import json
import logging

import asyncpg
import websockets
from rich import print, print_json

from config import settings

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


async def ingest():
    pool = await asyncpg.create_pool(settings.database_url, min_size=2, max_size=5)

    url = "wss://stream.aisstream.io/v0/stream"
    payload = build_subscribe_payload(settings.aisstream_api_key)

    while True:
        try:
            async with websockets.connect(url) as ws:
                await ws.send(json.dumps(payload))

                async for raw_message in ws:
                    print_json(
                        raw_message.decode("utf-8")
                        if isinstance(raw_message, bytes)
                        else raw_message
                    )

        except websockets.ConnectionClosed as e:
            print(e)
            await asyncio.sleep(30)

        except Exception as e:
            print(e)
            await asyncio.sleep(60)


def main():
    asyncio.run(ingest())


if __name__ == "__main__":
    main()
