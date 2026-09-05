"""Throwaway: is the A1 forecast invariant to leg enrichment?

Enrichment (last-fix + declaration) only picks among open_* sub-statuses. A1 reads
open-vs-closed and is_eu_arrival only. If the forecasts match exactly, the replay
can drop both expensive per-as-of queries.
"""
import asyncio
from datetime import UTC, datetime
import asyncpg
from analysis.a1 import forecast_window, realised_arrivals, W1, W2
from config import settings
from pipeline.legs import compute_legs
from pipeline.signal import TERMINAL_METADATA_SQL, build_lane_filter

async def main():
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=2)
    async with pool.acquire() as c:
        lane = build_lane_filter(await c.fetch(TERMINAL_METADATA_SQL))
    print(f"{'as_of':12s} {'enrich=T':>10s} {'enrich=F':>10s} {'match':>6s} {'legs':>6s}")
    for stamp in ("2019-06-03", "2021-06-07", "2023-01-02", "2025-01-06"):
        as_of = datetime.fromisoformat(stamp).replace(tzinfo=UTC)
        a = await compute_legs(pool, as_of, point_in_time=True, enrich=True)
        b = await compute_legs(pool, as_of, point_in_time=True, enrich=False)
        fa = forecast_window(a, as_of, lane, u0=W1[0], u1=W1[1])
        fb = forecast_window(b, as_of, lane, u0=W1[0], u1=W1[1])
        ta = realised_arrivals(a, as_of, lane, u0=W2[0], u1=W2[1])
        tb = realised_arrivals(b, as_of, lane, u0=W2[0], u1=W2[1])
        ok = (abs(fa.expected - fb.expected) < 1e-12
              and fa.n_legs == fb.n_legs and ta == tb)
        print(f"{stamp:12s} {fa.expected:10.6f} {fb.expected:10.6f} "
              f"{'OK' if ok else 'DIFF':>6s} {len(a):6d}")
    await pool.close()

asyncio.run(main())
