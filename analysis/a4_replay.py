"""A4 replay harness — walk-forward Kalman nowcast, scored (D-022).

Read-only. Same weekly grid, target definition, nulls and acceptance shape as A2, so
the two are directly comparable. D-017's rule is applied: the scored span ends inside
the NOAA regime's own departure history, not at the panel data max.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import asyncpg

from analysis.a4 import (
    MIN_TRAIN_WEEKS,
    GaussianPredictive,
    climatology,
    fit_local_level,
    fit_local_trend,
    persistence,
)
from config import settings

logger = logging.getLogger(__name__)

GRID_START = datetime(2018, 1, 1, tzinfo=UTC)

LOADINGS_SQL = """
SELECT event_time
FROM port_events
WHERE event_type = 'departed'
  AND laden_flag IS TRUE
  AND zone IN ('usgulf', 'usatlantic')
  AND regime = $1
ORDER BY event_time
"""


@dataclass(frozen=True)
class ReplayRow:
    as_of: datetime
    horizon: str
    model: str
    truth: float
    forecast: float
    sd: float
    crps: float
    pit: float
    lo50: float
    hi50: float
    lo80: float
    hi80: float
    persistence: float
    climatology: float
    n_train: int
    alpha: float | None  # equivalent EWMA smoothing (local level only)
    snr: float | None

    @property
    def year(self) -> int:
        return self.as_of.year

    @property
    def in_50(self) -> bool:
        return self.lo50 <= self.truth <= self.hi50

    @property
    def in_80(self) -> bool:
        return self.lo80 <= self.truth <= self.hi80


def weekly_series(times: list[datetime], grid: list[datetime]) -> list[float]:
    """`y_t` = loadings in `(t-7d, t]` — A2's `lag1`, and the persistence null."""
    out = []
    for t in grid:
        lo = t - timedelta(days=7)
        out.append(float(sum(1 for x in times if lo < x <= t)))
    return out


def replay(grid, y, *, models=("level", "trend")) -> list[ReplayRow]:
    """At each as-of: fit on history `<= t`, forecast `y_{t+1}` and `y_{t+2}`.

    No purge is needed beyond the natural one: the filter is fitted on `y[:i+1]`,
    all of which is observed by `grid[i]`, and the targets are strictly later.
    """
    rows: list[ReplayRow] = []
    for i in range(len(grid)):
        hist = y[: i + 1]
        if len(hist) < MIN_TRAIN_WEEKS:
            continue
        pers, clim = persistence(hist), climatology(hist)
        fits = {}
        if "level" in models:
            fits["level"] = fit_local_level(hist)
        if "trend" in models:
            fits["trend"] = fit_local_trend(hist)
        for h, name in ((1, "W1"), (2, "W2")):
            if i + h >= len(y):
                continue
            truth = y[i + h]
            for model, fit in fits.items():
                mean, var = fit.forecast(h)
                d = GaussianPredictive(mean=mean, var=var)
                lo50, hi50 = d.interval(0.50)
                lo80, hi80 = d.interval(0.80)
                rows.append(
                    ReplayRow(
                        as_of=grid[i], horizon=name, model=model, truth=truth,
                        forecast=mean, sd=d.sd, crps=d.crps(truth),
                        pit=d.pit(truth), lo50=lo50, hi50=hi50,
                        lo80=lo80, hi80=hi80, persistence=pers,
                        climatology=clim, n_train=len(hist),
                        alpha=getattr(fit, "ewma_alpha", None),
                        snr=getattr(fit, "snr", None),
                    )
                )
    return rows


# ----------------------------------------------------------------------
# Scoring (same shape as A1/A2)
# ----------------------------------------------------------------------


def _mae(pairs):
    v = [abs(a - b) for a, b in pairs]
    return sum(v) / len(v) if v else float("nan")


def _rmse(pairs):
    v = [(a - b) ** 2 for a, b in pairs]
    return (sum(v) / len(v)) ** 0.5 if v else float("nan")


@dataclass(frozen=True)
class Scorecard:
    label: str
    n: int
    mean_truth: float
    mean_forecast: float
    mae: float
    rmse: float
    crps: float
    persist_mae: float
    clim_mae: float
    cov50: float
    cov80: float

    @property
    def bias(self) -> float:
        return self.mean_forecast - self.mean_truth

    @property
    def beats_persistence(self) -> bool:
        return self.mae < self.persist_mae

    @property
    def beats_climatology(self) -> bool:
        return self.mae < self.clim_mae


def score(rows: list[ReplayRow], label: str) -> Scorecard:
    if not rows:
        return Scorecard(label, 0, *[float("nan")] * 9)
    return Scorecard(
        label=label, n=len(rows),
        mean_truth=sum(r.truth for r in rows) / len(rows),
        mean_forecast=sum(r.forecast for r in rows) / len(rows),
        mae=_mae([(r.forecast, r.truth) for r in rows]),
        rmse=_rmse([(r.forecast, r.truth) for r in rows]),
        crps=sum(r.crps for r in rows) / len(rows),
        persist_mae=_mae([(r.persistence, r.truth) for r in rows]),
        clim_mae=_mae([(r.climatology, r.truth) for r in rows]),
        cov50=sum(r.in_50 for r in rows) / len(rows),
        cov80=sum(r.in_80 for r in rows) / len(rows),
    )


def pit_histogram(rows, bins=10):
    hist = [0] * bins
    for r in rows:
        hist[min(bins - 1, int(r.pit * bins))] += 1
    return hist


HDR = (
    f"{'slice':24s} {'n':>4s} {'truth':>6s} {'fcst':>6s} {'bias':>6s} "
    f"{'MAE':>7s} {'RMSE':>6s} {'CRPS':>6s} {'persist':>8s} {'clim':>6s} "
    f"{'cov50':>6s} {'cov80':>6s}"
)


def fmt(sc: Scorecard) -> str:
    if sc.n == 0:
        return f"{sc.label:24s} {0:>4d}  (no scored weeks)"
    flag = ""
    if sc.beats_persistence and sc.beats_climatology:
        flag = "  <= beats both"
    elif sc.beats_persistence or sc.beats_climatology:
        flag = "  <= beats one"
    return (
        f"{sc.label:24s} {sc.n:4d} {sc.mean_truth:6.2f} {sc.mean_forecast:6.2f} "
        f"{sc.bias:+6.2f} {sc.mae:7.3f} {sc.rmse:6.2f} {sc.crps:6.3f} "
        f"{sc.persist_mae:8.3f} {sc.clim_mae:6.3f} "
        f"{sc.cov50:6.1%} {sc.cov80:6.1%}{flag}"
    )


A2_W1_MAE = 3.961  # D-018, same target and grid


def report(rows: list[ReplayRow], primary: str) -> None:
    print(f"\n{'=' * 122}\nA4 REPLAY SCORECARD — weekly US loadings (Kalman)")
    print(f"{'=' * 122}")
    print(f"scored observations: {len(rows)}   "
          f"span: {min(r.as_of for r in rows):%Y-%m-%d} -> "
          f"{max(r.as_of for r in rows):%Y-%m-%d}")

    for horizon in ("W1", "W2"):
        hr = [r for r in rows if r.horizon == horizon]
        if not hr:
            continue
        print(f"\n--- {horizon} " + "-" * 110)
        print(HDR)
        for model in sorted({r.model for r in hr}):
            print(fmt(score([r for r in hr if r.model == model], f"{model}")))

        pr = [r for r in hr if r.model == primary]
        if not pr:
            continue
        print(f"\n  by year ({primary}):")
        print("  " + HDR)
        for yy in sorted({r.year for r in pr}):
            print("  " + fmt(score([r for r in pr if r.year == yy], f"{yy}")))
        print("\n  regime-episode split (MODELS.md §0·3 #1):")
        print("  " + HDR)
        print("  " + fmt(score([r for r in pr if r.year in (2021, 2022)],
                               "2021-22 (crisis)")))
        print("  " + fmt(score([r for r in pr if r.year not in (2021, 2022)],
                               "all other years")))

        hist = pit_histogram(pr)
        total = sum(hist) or 1
        print("\n  PIT histogram (continuous) — uniform => calibrated:")
        for i, c in enumerate(hist):
            print(f"    [{i / 10:.1f},{(i + 1) / 10:.1f})  {c:4d}  "
                  f"{'#' * round(40 * c / total * len(hist))}")

    # --- the interpretable output ---------------------------------------
    pr1 = [r for r in rows if r.horizon == "W1" and r.model == "level"]
    if pr1:
        alphas = [r.alpha for r in pr1 if r.alpha is not None]
        snrs = [r.snr for r in pr1 if r.snr is not None]
        if alphas:
            print(f"\n{'=' * 122}\nFITTED SMOOTHING (local level, W1)\n{'=' * 122}")
            print(f"  equivalent EWMA alpha: mean {sum(alphas)/len(alphas):.3f}, "
                  f"median {sorted(alphas)[len(alphas)//2]:.3f}  "
                  f"(1.0 = persistence, ->0 = heavy smoothing)")
            print(f"  signal-to-noise q/r  : median {sorted(snrs)[len(snrs)//2]:.4f}")
            eff = 2.0 / max(sum(alphas) / len(alphas), 1e-9) - 1.0
            print(f"  => effective window  : ~{eff:.1f} weeks "
                  f"(the 4-week SMA null is the comparison)")

    # --- verdict ---------------------------------------------------------
    print(f"\n{'=' * 122}\nPRE-REGISTERED ACCEPTANCE (D-022)\n{'=' * 122}")
    sc = score(pr1, primary)
    if sc.n == 0:
        print("  no scored W1 rows — cannot evaluate")
        return
    a = sc.beats_persistence and sc.beats_climatology
    b = 0.70 <= sc.cov80 <= 0.90
    print(f"  (a) W1 MAE beats BOTH nulls  : A4 {sc.mae:.3f} vs "
          f"persist {sc.persist_mae:.3f}, clim {sc.clim_mae:.3f}  -> "
          f"{'PASS' if a else 'FAIL'}")
    print(f"  (b) 80% coverage in [70,90]% : {sc.cov80:.1%}  -> "
          f"{'PASS' if b else 'FAIL'}")
    print(f"\n  VERDICT: {'A4 WORKS' if a and b else 'A4 does NOT meet the bar'}")
    print(f"\n  vs A2 (same target/grid, D-018): A4 {sc.mae:.3f} vs "
          f"A2 {A2_W1_MAE:.3f}  -> "
          f"{'A4 better' if sc.mae < A2_W1_MAE else 'A2 better'}")


async def main() -> None:
    ap = argparse.ArgumentParser(description="A4 Kalman nowcast replay")
    ap.add_argument("--regime", default="noaa")
    ap.add_argument("--primary", default="level")
    ap.add_argument("--json", action="store_true", help="Also write paper/results/a4.json")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(LOADINGS_SQL, args.regime)
    finally:
        await pool.close()
    times = [r["event_time"] for r in rows]

    # D-017: end inside this regime's own history, not at the panel data max.
    regime_end = max(times)
    grid, d = [], GRID_START
    while d <= regime_end:
        grid.append(d)
        d += timedelta(days=7)
    # Need y_{t+2} observed for W2, so the last two grid points cannot be scored.
    logger.info("loadings=%d  regime_end=%s  grid=%d weekly as-ofs %s -> %s",
                len(times), f"{regime_end:%Y-%m-%d}", len(grid),
                f"{grid[0]:%Y-%m-%d}", f"{grid[-1]:%Y-%m-%d}")

    y = weekly_series(times, grid)
    logger.info("weekly series: mean %.2f, min %.0f, max %.0f",
                sum(y) / len(y), min(y), max(y))
    rows = replay(grid, y)
    report(rows, args.primary)
    if args.json:
        from analysis.results_io import dump

        pr = [r for r in rows if r.model == args.primary]
        w1 = [r for r in pr if r.horizon == "W1" and r.alpha is not None]
        dump(
            "a4",
            {
                "regime": args.regime,
                "primary_model": args.primary,
                "scorecards": {
                    h: score([r for r in pr if r.horizon == h], h) for h in ("W1", "W2")
                },
                "alpha_mean": sum(r.alpha for r in w1) / max(1, len(w1)),
                "effective_window_weeks": (2 / (sum(r.alpha for r in w1) / max(1, len(w1))) - 1)
                if w1 else None,
            },
        )


if __name__ == "__main__":
    asyncio.run(main())
