"""Throwaway probe: the exact predictive distribution on real data.

Prints the W1 forecast as a distribution (mean, sd, 50/80 % central intervals) at
several as-of dates, alongside the realised count from a hindsight leg load, plus
the normal-approximation interval the exact PMF replaces — to show whether the
exactness earns its keep at these sample sizes.

Sanity only. Not the scored replay (step 6); truth here is hindsight.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from statistics import NormalDist

import asyncpg

from analysis.a1 import forecast_window, is_eu_arrival
from config import settings
from pipeline.legs import compute_legs
from pipeline.signal import TERMINAL_METADATA_SQL, build_lane_filter

AS_OFS = ["2018-01-01", "2020-06-01", "2022-03-07", "2023-01-02", "2025-01-06"]


def realised(all_legs, lane, as_of, u0, u1):
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


def normal_interval(mean, sd, coverage):
    if sd <= 0:
        return round(mean), round(mean)
    z = NormalDist().inv_cdf(1.0 - (1.0 - coverage) / 2.0)
    return round(mean - z * sd), round(mean + z * sd)


async def main() -> None:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            lane = build_lane_filter(await conn.fetch(TERMINAL_METADATA_SQL))
        truth_legs = await compute_legs(
            pool, datetime(2026, 8, 11, tzinfo=UTC), point_in_time=True
        )

        print(f"{'as_of':12s} {'nnz':>4s} {'mean':>6s} {'sd':>5s} "
              f"{'exact50':>9s} {'exact80':>9s} {'normal80':>9s} "
              f"{'obs':>4s} {'crps':>6s} {'in80':>5s}")
        for stamp in AS_OFS:
            as_of = datetime.fromisoformat(stamp).replace(tzinfo=UTC)
            legs = await compute_legs(pool, as_of, point_in_time=True)
            fs = forecast_window(legs, as_of, lane, u0=0.0, u1=7.0)
            d = fs.predictive()
            obs = realised(truth_legs, lane, as_of, 0.0, 7.0)
            lo50, hi50 = d.interval(0.50)
            lo80, hi80 = d.interval(0.80)
            nlo, nhi = normal_interval(d.mean, d.variance**0.5, 0.80)
            nnz = sum(1 for p in fs.probabilities if p > 0)
            inside = "yes" if lo80 <= obs <= hi80 else "NO"
            print(f"{stamp:12s} {nnz:4d} {d.mean:6.2f} {d.variance**0.5:5.2f} "
                  f"{f'[{lo50},{hi50}]':>9s} {f'[{lo80},{hi80}]':>9s} "
                  f"{f'[{nlo},{nhi}]':>9s} {obs:4d} {d.crps(obs):6.2f} {inside:>5s}")

        # Exactness check: PMF sums to 1 and its moments match the closed forms.
        print("\nPMF integrity at the last date: "
              f"sum={sum(d.pmf):.12f}  "
              f"mean-vs-sum_p={abs(d.mean - fs.expected):.2e}  "
              f"var-vs-sum_pq={abs(d.variance - fs.variance):.2e}")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
