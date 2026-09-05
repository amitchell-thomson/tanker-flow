"""Throwaway probe: validate the A1 truth series and nulls on real data.

Three checks:

1. **Exactly-once.** Summing the W1 truth over a complete weekly grid must equal a
   direct count of qualifying legs whose arrival falls in the grid span. This is the
   invariant that says the truth series neither drops nor double-counts an arrival —
   it holds because every US->EU leg takes >= 7.04 d, so an arrival inside
   `(as_of, as_of+7d]` always has its departure at or before `as_of`.
2. **W1 vs W2 consistency.** The W2 series is the W1 series shifted one week, up to
   the departure conditioning (which genuinely differs: 1,200 of 3,200 legs arrive
   within 14 d, so some W2 arrivals come from legs not yet departed at `as_of`).
3. **Cross-check vs `od_flow_count`.** That signal is dated by *departure* and
   includes ballast, so it bounds rather than equals the truth total.
"""

import asyncio
from collections import Counter
from datetime import UTC, datetime, timedelta

import asyncpg

from analysis.a1 import (
    W1,
    W2,
    climatology_null,
    is_eu_arrival,
    persistence_null,
    realised_arrivals,
)
from config import settings
from pipeline.legs import compute_legs
from pipeline.signal import TERMINAL_METADATA_SQL, build_lane_filter

GRID_START = datetime(2018, 1, 1, tzinfo=UTC)
GRID_END = datetime(2026, 1, 1, tzinfo=UTC)


def mondays(start, end):
    d = start
    while d < end:
        yield d
        d += timedelta(days=7)


async def main() -> None:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            lane = build_lane_filter(await conn.fetch(TERMINAL_METADATA_SQL))
            od = await conn.fetch("""
                SELECT sum(value)::int AS n
                FROM signal_daily
                WHERE signal_key='od_flow_count' AND basis='physical'
                  AND regime <> 'all'
                  AND (zone_scope LIKE 'usgulf->%' OR zone_scope LIKE 'usatlantic->%')
                  AND split_part(zone_scope,'->',2) IN
                      ('nweurope','baltic','iberian','wmed','emed')
                  AND bucket_date >= $1::date AND bucket_date < $2::date
            """, GRID_START.date(), GRID_END.date())
        legs = await compute_legs(
            pool, datetime(2026, 8, 11, tzinfo=UTC), point_in_time=True
        )
    finally:
        await pool.close()

    grid = list(mondays(GRID_START, GRID_END))
    print(f"grid: {len(grid)} weekly as-ofs, {GRID_START:%Y-%m-%d} -> {GRID_END:%Y-%m-%d}")

    # --- 1. exactly-once -----------------------------------------------------
    series_w1 = [realised_arrivals(legs, a, lane, u0=W1[0], u1=W1[1]) for a in grid]
    total_w1 = sum(series_w1)
    span_lo, span_hi = grid[0], grid[-1] + timedelta(days=7)
    direct = sum(
        1
        for lg in legs
        if lg.laden is True
        and lane.is_export(lg.origin_zone)
        and is_eu_arrival(lg, lane)
        and lg.arrived_ts is not None
        and span_lo < lg.arrived_ts <= span_hi
    )
    print(f"\n1. exactly-once:  sum(W1 series) = {total_w1}   "
          f"direct count in span = {direct}   "
          f"{'OK' if total_w1 == direct else 'MISMATCH'}")

    # --- 2. W1 vs W2 ---------------------------------------------------------
    series_w2 = [realised_arrivals(legs, a, lane, u0=W2[0], u1=W2[1]) for a in grid]
    shifted = sum(1 for i in range(len(grid) - 1) if series_w2[i] != series_w1[i + 1])
    print(f"2. W2[i] vs W1[i+1]: {shifted} of {len(grid)-1} weeks differ "
          f"(expected > 0 — W2 excludes legs not yet departed at as_of)")
    print(f"   sum(W2) = {sum(series_w2)}  vs  sum(W1) = {total_w1}")

    # --- 3. od_flow_count bound ---------------------------------------------
    print(f"3. od_flow_count (US->EU lanes, by departure, incl. ballast) = "
          f"{od[0]['n']}  vs truth {total_w1}")

    # --- series shape + nulls ------------------------------------------------
    by_year = Counter()
    for a, v in zip(grid, series_w1):
        by_year[a.year] += v
    print("\nW1 truth by year:")
    for y in sorted(by_year):
        n_wk = sum(1 for a in grid if a.year == y)
        print(f"  {y}  total={by_year[y]:5d}  mean/wk={by_year[y]/n_wk:5.2f}")

    print("\nsample (truth vs nulls):")
    print(f"{'as_of':12s} {'W1':>4s} {'W2':>4s} {'persist':>8s} {'clim4':>7s}")
    for a in grid[::52][:9]:
        i = grid.index(a)
        print(f"{a:%Y-%m-%d}   {series_w1[i]:4d} {series_w2[i]:4d} "
              f"{persistence_null(legs, a, lane):8.1f} "
              f"{climatology_null(legs, a, lane):7.2f}")


if __name__ == "__main__":
    asyncio.run(main())
