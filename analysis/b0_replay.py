"""B0 replay — walk-forward scoring of the Part B null, plus the FWL scan (D-028).

Read-only. Loads `model_panel`, samples it to the weekly Monday grid from
2018-01-01, scores the M0/M1/M2 ladder at h = 1 and h = 4 weeks, and then runs
the FWL partial-effect scan over the pre-registered tanker signals.

Nothing here decides anything: every threshold, sign and horizon comes from
D-028, fixed before the first fit.

Usage:
  uv run python -m analysis.b0_replay
  uv run python -m analysis.b0_replay --horizons 1 2 4 8
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import date

import asyncpg
import numpy as np

from analysis.b0 import MIN_TRAIN_WEEKS, run_ladder, score
from analysis.fwl import partial_effect
from config import settings
from data.model_panel import load_wide

GRID_START = "2018-01-01"  # a Monday; the Part A grid (D-008/D-028)
# Last date on which the tanker signals are carried by the decade backfill.
# NOAA (US) ends 2025-12-31 and GFW (EU) 2026-02-21; past that only the live
# feed contributes and gas_in_transit_eu falls ~90 % for measurement reasons.
COVERAGE_HORIZON = date(2025, 12, 31)
TARGET = "spread_hh_ttf"

# D-028's control block. The price legs are excluded — they construct the target.
CONTROLS: tuple[str, ...] = (
    "hdd_us",
    "cdd_us",
    "hdd_nwe",
    "cdd_nwe",
    "us_storage_bcf",
    "eu_storage_pct",
    "brent",
)
WINTER_MONTHS = (11, 12, 1, 2, 3)

# The pre-registered signs (D-028). 0 = no prior registered, judged two-sided.
SIGNAL_PRIORS: dict[str, int] = {
    "gas_in_transit_eu": +1,
    "gas_discharging_eu": +1,
    "gas_loading_us": +1,
    "gas_ballast_to_us": +1,
    "load_queue_h": -1,
    "laden_voyage_age_d": -1,
    "net_export_pressure": +1,
    "gas_in_transit_unknown": 0,
}

SIGN_LABEL = {1: "+", -1: "-", 0: "?"}


@dataclass(frozen=True)
class Weekly:
    """The weekly modelling frame, NaN-free on every column used."""

    dates: np.ndarray
    years: np.ndarray
    level: np.ndarray
    controls: np.ndarray
    control_names: tuple[str, ...]
    signals: dict[str, np.ndarray]


async def load_weekly(pool: asyncpg.Pool, *, end: date | None = None) -> Weekly:
    """Load the weekly modelling frame.

    `end` truncates the panel. The default runs to the data max, but the
    backfill sources stop long before it — NOAA at 2025-12-31, GFW at
    2026-02-21 — after which the tanker signals are carried by the thin live
    feed alone and step down by ~90 %. `end=COVERAGE_HORIZON` restricts the
    sample to the regime where the signals are measured consistently (D-033).
    """
    frame = await load_wide(pool, start=np.datetime64(GRID_START).astype("O"))
    if end is not None:
        frame = frame.loc[[d for d in frame.index if d <= end]]
    needed = [TARGET, *CONTROLS, *SIGNAL_PRIORS]
    frame = frame[needed].copy()

    # Weekly Mondays. Resampling to 'W-MON' would *label* weeks by Monday while
    # aggregating the preceding week, mixing future days into the label; taking
    # the actual Monday row keeps every value strictly as-of that date.
    frame.index = frame.index.map(lambda d: d)
    mondays = [d for d in frame.index if d.weekday() == 0]
    frame = frame.loc[mondays]

    before = len(frame)
    frame = frame.dropna()
    dropped = before - len(frame)
    if dropped:
        print(f"[load] dropped {dropped}/{before} weeks with an incomplete row")

    dates = np.array(frame.index)
    winter = np.array([1.0 if d.month in WINTER_MONTHS else 0.0 for d in frame.index])
    controls = np.column_stack([frame[c].to_numpy(float) for c in CONTROLS] + [winter])

    return Weekly(
        dates=dates,
        years=np.array([d.year for d in frame.index]),
        level=frame[TARGET].to_numpy(float),
        controls=controls,
        control_names=(*CONTROLS, "winter"),
        signals={k: frame[k].to_numpy(float) for k in SIGNAL_PRIORS},
    )


def report_ladder(weekly: Weekly, horizon: int) -> None:
    out = run_ladder(
        weekly.level, weekly.controls, weekly.control_names, horizon=horizon
    )
    scores = {name: score(name, preds) for name, preds in out.items()}
    m0 = scores["M0"]

    print(
        f"\n{'=' * 78}\nLADDER — h = {horizon} week(s), target = {horizon}-week change in spread"
    )
    print(
        f"{'model':<8}{'n':>6}{'MAE':>9}{'RMSE':>9}{'80% cov':>10}{'skill vs M0':>13}"
    )
    print("-" * 78)
    for name in ("M0", "M1", "M2"):
        s = scores[name]
        skill = "—" if name == "M0" else f"{s.skill_vs(m0):+.1%}"
        print(
            f"{name:<8}{s.n:>6}{s.mae:>9.3f}{s.rmse:>9.3f}"
            f"{s.coverage_80:>9.1%}{skill:>13}"
        )

    # Per-year MAE — D-028 §Metrics: a win carried by 2022 alone is not a win.
    print(f"\n{'per-year MAE':<14}", end="")
    years = sorted({int(weekly.years[p.index]) for p in out["M0"]})
    for y in years:
        print(f"{y:>8}", end="")
    print()
    for name in ("M0", "M1", "M2"):
        print(f"{name:<14}", end="")
        for y in years:
            errs = [abs(p.error) for p in out[name] if int(weekly.years[p.index]) == y]
            print(f"{np.mean(errs):>8.2f}" if errs else f"{'—':>8}", end="")
        print()


def report_fwl(weekly: Weekly, horizon: int) -> None:
    """FWL scan: each signal's effect net of persistence, weather, storage, oil.

    The AR term joins the control block here so the partial effect is measured
    net of the spread's own persistence, exactly as in M2.
    """
    n = len(weekly.level)
    idx = np.arange(n - horizon)
    y = weekly.level[idx + horizon]
    controls = np.column_stack([weekly.controls[idx], weekly.level[idx]])

    print(
        f"\n{'=' * 78}\nFWL PARTIAL EFFECTS — h = {horizon} week(s), net of AR(1)+controls"
    )
    print(
        f"{'signal':<24}{'prior':>6}{'beta':>12}{'HAC t':>8}"
        f"{'yr cons':>9}{'pR2':>7}  verdict"
    )
    print("-" * 78)
    for name, prior in SIGNAL_PRIORS.items():
        eff = partial_effect(
            name,
            y,
            weekly.signals[name][idx],
            controls,
            weekly.years[idx],
            horizon=horizon,
            expected_sign=prior,
        )
        print(
            f"{name:<24}{SIGN_LABEL[prior]:>6}{eff.beta:>12.3e}{eff.t_stat:>8.2f}"
            f"{eff.year_consistency:>8.0%}{eff.partial_r2:>7.3f}  {eff.verdict()}"
        )


async def run(horizons: list[int], *, end: date | None = None) -> None:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=3)
    try:
        weekly = await load_weekly(pool, end=end)
    finally:
        await pool.close()

    print(
        f"\nWeekly grid: {len(weekly.level)} complete weeks, "
        f"{weekly.dates[0]} -> {weekly.dates[-1]}"
    )
    print(f"Controls: {', '.join(weekly.control_names)}")
    print(f"Min training window: {MIN_TRAIN_WEEKS} weeks")

    for h in horizons:
        report_ladder(weekly, h)
        report_fwl(weekly, h)


def main() -> None:
    parser = argparse.ArgumentParser(description="Part B null + FWL scan (D-028)")
    parser.add_argument(
        "--end",
        choices=["data-max", "coverage"],
        default="data-max",
        help="Sample end: 'data-max' (default, D-029 as published) or 'coverage' "
        "(truncate at the backfill horizon 2025-12-31 — the D-033 robustness run)",
    )
    parser.add_argument(
        "--horizons",
        nargs="+",
        type=int,
        default=[1, 4],
        help="Forecast horizons in weeks (D-028 pre-registers 1 and 4)",
    )
    args = parser.parse_args()
    end = COVERAGE_HORIZON if args.end == "coverage" else None
    asyncio.run(run(args.horizons, end=end))


if __name__ == "__main__":
    main()
