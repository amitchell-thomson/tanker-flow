"""Cross-check the daily TTF series against the World Bank Pink Sheet.

Read-only. The daily `ttf_front_month` series is a *rolled continuous* futures
contract: every month it switches from the expiring contract to the next one,
and that switch moves the printed level by the calendar spread rather than by
any change in the price of gas. Those jumps are artefacts. They matter if the
spread model reads *levels*; they matter much less if it reads *changes*.

The World Bank Pink Sheet's "Natural gas, Europe" is an independent monthly
TTF benchmark that is *not* built by rolling our contract, and it is published
already in $/MMBtu. So regressing our converted daily series (resampled to
monthly) on theirs tests two things at once:

  1. the EUR/MWh -> $/MMBtu conversion (`/3.412 x EUR/USD`, MODELS.md 2), and
  2. whether the roll leaves a systematic level distortion.

A slope near 1, an intercept near 0 and a high R^2 mean the roll is benign and
the conversion is right. A slope away from 1 or a drifting residual is the
signal to switch to a roll-adjusted or cash series before modelling levels.

Usage:  uv run python -m scripts.check_ttf_roll
"""

from __future__ import annotations

import asyncio
import statistics

import asyncpg

from config import settings

# Monthly mean of the daily converted series, joined to the Pink Sheet month.
# The daily side is averaged over trading days only (no forward-fill): a
# calendar-day fill would weight Mondays triple and bias the monthly mean.
QUERY = """
WITH daily AS (
    SELECT t.period,
           t.value / 3.412 * f.value AS usd_mmbtu
    FROM market_series t
    JOIN market_series f
      ON f.series_id = 'eurusd' AND f.period = t.period
    WHERE t.series_id = 'ttf_front_month'
      AND t.value IS NOT NULL AND f.value IS NOT NULL
),
ours AS (
    SELECT date_trunc('month', period)::date AS month,
           avg(usd_mmbtu) AS ours,
           count(*)        AS n_days
    FROM daily GROUP BY 1
),
theirs AS (
    SELECT period AS month, value AS theirs
    FROM market_series
    WHERE series_id = 'ttf_eu_monthly' AND value IS NOT NULL
)
SELECT o.month, o.ours, t.theirs, o.n_days
FROM ours o JOIN theirs t USING (month)
WHERE o.n_days >= 15          -- drop part-months at either end of the daily series
ORDER BY o.month
"""


def ols(xs: list[float], ys: list[float]) -> tuple[float, float, float]:
    """Least-squares slope, intercept and R^2 of y on x."""
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    slope = sxy / sxx
    intercept = my - slope * mx
    ss_res = sum((y - (intercept + slope * x)) ** 2 for x, y in zip(xs, ys))
    ss_tot = sum((y - my) ** 2 for y in ys)
    return slope, intercept, 1 - ss_res / ss_tot


async def main() -> None:
    conn = await asyncpg.connect(settings.database_url)
    try:
        rows = await conn.fetch(QUERY)
    finally:
        await conn.close()

    if not rows:
        print("No overlapping months — is `make market-full` done?")
        return

    ours = [float(r["ours"]) for r in rows]
    theirs = [float(r["theirs"]) for r in rows]
    slope, intercept, r2 = ols(theirs, ours)

    # Relative error per month, so the 2022 spike does not dominate the summary.
    rel = [(o - t) / t for o, t in zip(ours, theirs)]
    abs_rel = sorted(abs(v) for v in rel)

    print(f"\nOverlap: {len(rows)} months, {rows[0]['month']} -> {rows[-1]['month']}")
    print("\nOLS  ours = a + b * worldbank")
    print(f"  slope      {slope:8.4f}   (1.0 = no systematic roll/scale distortion)")
    print(f"  intercept  {intercept:8.4f}   ($/MMBtu)")
    print(f"  R^2        {r2:8.4f}")

    print("\nRelative difference (ours vs World Bank)")
    print(f"  median     {statistics.median(rel):+8.3%}")
    print(f"  mean       {statistics.fmean(rel):+8.3%}   (bias: roll would show here)")
    print(f"  median |.| {statistics.median(abs_rel):8.3%}")
    print(f"  p90   |.|  {abs_rel[int(0.9 * (len(abs_rel) - 1))]:8.3%}")

    worst = sorted(zip(rows, rel), key=lambda p: -abs(p[1]))[:5]
    print("\nLargest disagreements")
    print(f"  {'month':<12}{'ours':>9}{'worldbank':>11}{'diff':>9}")
    for r, d in worst:
        print(f"  {str(r['month']):<12}{r['ours']:>9.2f}{r['theirs']:>11.2f}{d:>+9.1%}")


if __name__ == "__main__":
    asyncio.run(main())
