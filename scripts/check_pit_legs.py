"""Throwaway check: does point_in_time=True actually change an as-of replay?

Compares the leg status distribution at a historical as_of under the default
(unbounded, as-of-unsafe) loader vs the new bounded one. Not a test — a probe.
"""

import asyncio
import sys
from collections import Counter
from datetime import UTC, datetime

import asyncpg

from config import settings
from pipeline.legs import compute_legs


async def main(as_of: datetime) -> None:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=2)
    try:
        for pit in (False, True):
            t0 = asyncio.get_event_loop().time()
            legs = await compute_legs(pool, as_of, point_in_time=pit)
            dt = asyncio.get_event_loop().time() - t0
            counts = Counter(lg.status for lg in legs)
            open_legs = [lg for lg in legs if lg.status.startswith("open")]
            future = sum(
                1 for lg in legs if lg.arrived_ts is not None and lg.arrived_ts > as_of
            )
            print(f"\npoint_in_time={pit}  ({dt:.1f}s)  as_of={as_of:%Y-%m-%d}")
            print(f"  total legs           {len(legs)}")
            for status, n in sorted(counts.items(), key=lambda kv: -kv[1]):
                print(f"    {status:20s} {n}")
            print(f"  open legs            {len(open_legs)}")
            print(f"  legs closed by a FUTURE arrival (leak): {future}")
            declared = sum(1 for lg in legs if lg.dest_region is not None)
            print(f"  legs with a declared dest_region:       {declared}")
    finally:
        await pool.close()


if __name__ == "__main__":
    stamp = sys.argv[1] if len(sys.argv) > 1 else "2020-06-01"
    asyncio.run(main(datetime.fromisoformat(stamp).replace(tzinfo=UTC)))
