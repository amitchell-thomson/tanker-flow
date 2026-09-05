"""Throwaway probe: pi conditional on the leg still being OPEN at age a.

Two design questions D-002 left implicit, both decided by this measurement:

1. Does `same_zone` (45.6% of laden export-origin legs — berth shifts / re-entries,
   which legs.py and signal.py already treat as non-voyages) belong in pi's
   denominator? It matters only if such legs are still open when A1 looks. If they
   flush out within hours, the open population A1 actually forecasts is already
   clean of them and pi should be measured on that population.

2. Expanding vs rolling window: pi by year drifts 0.10 -> 0.49 -> 0.41. Report the
   conditional pi per era to see whether the drift survives conditioning.

Descriptive only. No scoring.
"""

import asyncio
from collections import defaultdict
from datetime import UTC, datetime, timedelta

import asyncpg

from config import settings
from pipeline.legs import MAX_LEG_PAIR_DAYS, compute_legs
from pipeline.signal import TERMINAL_METADATA_SQL, build_lane_filter

AGES_D = [0.0, 0.25, 0.5, 1.0, 2.0, 3.0, 5.0, 7.0, 10.0,
          12.0, 14.0, 16.0, 18.0, 21.0, 25.0, 30.0, 40.0, 60.0]


async def main() -> None:
    as_of = datetime(2026, 8, 11, tzinfo=UTC)
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            lane = build_lane_filter(await conn.fetch(TERMINAL_METADATA_SQL))
        legs = await compute_legs(pool, as_of, point_in_time=True)
    finally:
        await pool.close()

    # Matured only: departed >= MAX_LEG_PAIR_DAYS before as_of, so the outcome
    # can no longer change by construction (pair_legs refuses later arrivals).
    cutoff = as_of - timedelta(days=MAX_LEG_PAIR_DAYS)
    pop = [
        lg for lg in legs
        if lg.laden is True
        and lane.is_export(lg.origin_zone)
        and lg.departed_ts <= cutoff
    ]
    print(f"matured laden export-origin legs: {len(pop)}\n")

    def age_h(lg):
        if lg.arrived_ts is None:
            return None  # never closed -> open forever
        return (lg.arrived_ts - lg.departed_ts).total_seconds() / 3600.0

    def is_eu(lg):
        return lg.status == "closed" and lane.is_import(lg.dest_zone)

    # How fast does same_zone flush out?
    sz = sorted(h for lg in pop if lg.status == "same_zone" and (h := age_h(lg)))
    if sz:
        def q(p):
            return sz[min(len(sz) - 1, int(p * len(sz)))]
        print(f"same_zone leg duration (n={len(sz)}), hours:")
        print(f"  p50 {q(.5):7.2f}   p75 {q(.75):7.2f}   p90 {q(.90):7.2f}"
              f"   p95 {q(.95):7.2f}   p99 {q(.99):7.2f}   max {sz[-1]:7.2f}")

    print("\npi | still open at age a  (whole matured sample):")
    print(f"{'age_d':>7s} {'n_open':>8s} {'eu':>6s} {'pi':>7s} {'sz_left':>8s}")
    for a in AGES_D:
        open_at = [
            lg for lg in pop
            if (h := age_h(lg)) is None or h > a * 24.0
        ]
        if not open_at:
            continue
        eu = sum(1 for lg in open_at if is_eu(lg))
        szl = sum(1 for lg in open_at if lg.status == "same_zone")
        print(f"{a:7.2f} {len(open_at):8d} {eu:6d} {eu/len(open_at):7.3f} "
              f"{szl:8d}")

    print("\npi | open at age 1d, by departure year (drift check):")
    by_year = defaultdict(list)
    for lg in pop:
        h = age_h(lg)
        if h is None or h > 24.0:
            by_year[lg.departed_ts.year].append(lg)
    for y in sorted(by_year):
        ls = by_year[y]
        eu = sum(1 for lg in ls if is_eu(lg))
        print(f"  {y}  n={len(ls):5d}  pi={eu/len(ls):.3f}")

    print("\npi | open at age 1d, by origin zone x era:")
    eras = [(2016, 2019), (2020, 2021), (2022, 2023), (2024, 2026)]
    for z in sorted({lg.origin_zone for lg in pop}):
        cells = []
        for lo, hi in eras:
            ls = [
                lg for lg in by_year_flat(by_year)
                if lg.origin_zone == z and lo <= lg.departed_ts.year <= hi
            ]
            if not ls:
                cells.append(f"{lo}-{hi}: -")
                continue
            eu = sum(1 for lg in ls if is_eu(lg))
            cells.append(f"{lo}-{hi}: {eu/len(ls):.3f} (n={len(ls)})")
        print(f"  {z:12s} " + "  ".join(cells))


def by_year_flat(by_year):
    return [lg for ls in by_year.values() for lg in ls]


if __name__ == "__main__":
    asyncio.run(main())
