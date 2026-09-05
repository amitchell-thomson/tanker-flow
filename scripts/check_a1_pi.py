"""Throwaway probe: build the A1 pi(a) curve at several as-of dates on real data.

Checks the estimator behaves as designed across the panel — which ladder rung
fires when, how pi drifts, and that the burn-in period is marked UNSUPPORTED
rather than silently returning a thin curve.
"""

import asyncio
from datetime import UTC, datetime

import asyncpg

from analysis.a1 import build_eu_rate_curve
from config import settings
from pipeline.legs import compute_legs
from pipeline.signal import TERMINAL_METADATA_SQL, build_lane_filter

AS_OFS = [
    "2016-07-01", "2017-01-02", "2018-01-01", "2020-06-01",
    "2022-03-07", "2023-01-02", "2025-01-06", "2026-08-10",
]
PROBE_AGES = [0.0, 5.0, 10.0, 14.0, 18.0, 21.0, 30.0]


async def main() -> None:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            lane = build_lane_filter(await conn.fetch(TERMINAL_METADATA_SQL))

        hdr = "  ".join(f"pi({a:g}d)" for a in PROBE_AGES)
        print(f"{'as_of':12s} {'n':>6s}  {hdr}   tier")
        for stamp in AS_OFS:
            as_of = datetime.fromisoformat(stamp).replace(tzinfo=UTC)
            legs = await compute_legs(pool, as_of, point_in_time=True)
            c = build_eu_rate_curve(
                legs, as_of, lane, origin_zone="usgulf", regime="noaa"
            )
            cells = "  ".join(f"{c.at(a):7.3f}" for a in PROBE_AGES)
            print(f"{stamp:12s} {c.n_legs:6d}  {cells}   {c.tier}"
                  f"  (trusted<={c.trusted_max_d:.1f}d)")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
