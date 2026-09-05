"""Throwaway probe: the assembled A1 per-leg forecast on real data.

Prints, per as-of date: the open at-sea population, the W1/W2 Poisson-binomial
mean and sd, how many legs fell beyond curve support, and the realised truth
(EU arrivals actually observed in each window, from the full-hindsight leg set).

Sanity only — not the scored replay (step 6). Truth here is read from a
hindsight leg load, which is legitimate for checking the forecast is in the right
ballpark but is NOT the scored comparison.
"""

import asyncio
from datetime import UTC, datetime, timedelta

import asyncpg

from analysis.a1 import forecast_window, is_eu_arrival
from config import settings
from pipeline.legs import compute_legs
from pipeline.signal import TERMINAL_METADATA_SQL, build_lane_filter

AS_OFS = [
    "2018-01-01", "2019-06-03", "2020-06-01", "2021-06-07",
    "2022-03-07", "2023-01-02", "2024-06-03", "2025-01-06", "2025-06-02",
]


def realised(all_legs, lane, as_of, u0, u1):
    """EU arrivals actually observed in (as_of+u0, as_of+u1] — hindsight truth."""
    lo, hi = as_of + timedelta(days=u0), as_of + timedelta(days=u1)
    return sum(
        1
        for lg in all_legs
        if is_eu_arrival(lg, lane)
        and lg.laden is True
        and lane.is_export(lg.origin_zone)
        and lg.arrived_ts is not None
        and lo < lg.arrived_ts <= hi
    )


async def main() -> None:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            lane = build_lane_filter(await conn.fetch(TERMINAL_METADATA_SQL))
        # Full-hindsight set, for the truth column only.
        truth_legs = await compute_legs(
            pool, datetime(2026, 8, 11, tzinfo=UTC), point_in_time=True
        )

        print(f"{'as_of':12s} {'open':>5s} {'byd':>4s} "
              f"{'W1 mean':>8s} {'W1 sd':>6s} {'W1 obs':>7s}   "
              f"{'W2 mean':>8s} {'W2 sd':>6s} {'W2 obs':>7s}   tiers")
        for stamp in AS_OFS:
            as_of = datetime.fromisoformat(stamp).replace(tzinfo=UTC)
            legs = await compute_legs(pool, as_of, point_in_time=True)
            curves: dict = {}
            f1 = forecast_window(legs, as_of, lane, u0=0.0, u1=7.0, curves=curves)
            f2 = forecast_window(legs, as_of, lane, u0=7.0, u1=14.0, curves=curves)
            o1 = realised(truth_legs, lane, as_of, 0.0, 7.0)
            o2 = realised(truth_legs, lane, as_of, 7.0, 14.0)
            tiers = ",".join(sorted({t.split("/")[-1] for t in f1.tiers}))
            print(f"{stamp:12s} {f1.n_legs:5d} {f1.n_beyond_support:4d} "
                  f"{f1.expected:8.2f} {f1.variance**0.5:6.2f} {o1:7d}   "
                  f"{f2.expected:8.2f} {f2.variance**0.5:6.2f} {o2:7d}   {tiers}")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
