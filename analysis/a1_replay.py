"""A1 replay harness — walk the weekly grid, forecast, score (MODELS.md Part A).

Build step 6. Every modelling choice was fixed before this ran: the target (D-001),
the estimator (D-007/D-009), the scored span (D-008), the truth and nulls (D-011),
and the acceptance bar (D-003). This module only *measures*; it contains no
tunable and must never acquire one.

Design notes
------------
**Enrichment is skipped.** `compute_legs(enrich=...)` only selects among the
`open_*` sub-statuses; A1 reads open-vs-closed and `is_eu_arrival`, both of which
are enrichment-independent. Verified identical to 1e-12 on four as-of dates, so the
replay drops the per-as-of last-fix LATERAL and declaration query — ~7 s -> ~0.03 s
each.

**Events are hoisted.** `pair_legs` is pure, so the whole decade of leg events is
fetched once, sorted by time, and sliced by bisect per as-of. The DB is touched
three times total, not 418 times.

**A week is scored only if the regime had something at sea.** Weeks where the
forecast population is empty are not a test of anything (A1 is not being asked a
question), and after a backfill's last departure they would otherwise pad every
metric with trivially-correct zeros. This is a structural criterion decided in
advance — "was there anything to forecast?" — not a performance filter (D-012).

**CRPS comparability.** A null is a point forecast, and the CRPS of a deterministic
forecast equals its absolute error. So the nulls' MAE columns are directly
comparable to A1's CRPS — and it is the harder comparison for A1, since a point
forecast is neither rewarded nor punished for its (absent) uncertainty.
"""

from __future__ import annotations

import argparse
import asyncio
import bisect
import logging
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import asyncpg

from analysis.a1 import (
    W1,
    W2,
    ArrivalCurve,
    climatology_null,
    forecast_window,
    persistence_null,
    realised_arrivals,
)
from config import settings
from pipeline.legs import (
    FALLBACK_DEST_REGION,
    LEG_EVENTS_SQL,
    WEIGHTS_SQL,
    LegEvent,
    pair_legs,
)
from pipeline.signal import TERMINAL_METADATA_SQL, LaneFilter, build_lane_filter

logger = logging.getLogger(__name__)

# Grid start per D-008 (2016-17 is burn-in: pi is UNSUPPORTED there).
GRID_START = datetime(2018, 1, 1, tzinfo=UTC)
# Fixed seed for the randomised discrete PIT, recorded so the histogram reproduces.
PIT_SEED = 20260810

HORIZONS = {"W1": W1, "W2": W2}


@dataclass(frozen=True)
class ReplayRow:
    """One (as_of, horizon, regime) scored observation."""

    as_of: datetime
    horizon: str
    regime: str
    truth: int
    forecast: float
    crps: float
    pit: float
    lo50: int
    hi50: int
    lo80: int
    hi80: int
    persistence: float
    climatology: float
    n_open: int
    n_beyond_support: int
    supported: bool

    @property
    def year(self) -> int:
        return self.as_of.year

    @property
    def in_50(self) -> bool:
        return self.lo50 <= self.truth <= self.hi50

    @property
    def in_80(self) -> bool:
        return self.lo80 <= self.truth <= self.hi80


def mondays(start: datetime, end: datetime) -> list[datetime]:
    out, d = [], start
    while d <= end:
        out.append(d)
        d += timedelta(days=7)
    return out


def replay(
    events: list[LegEvent],
    weights: dict[int, tuple[int | None, int | None]],
    truth_legs: list,
    lane: LaneFilter,
    grid: list[datetime],
    regimes: list[str],
) -> list[ReplayRow]:
    """Walk the grid and score. Pure given the loaded inputs.

    `truth_legs` is the hindsight leg set (D-011); `events` is the same underlying
    event stream, sliced per as-of to rebuild the point-in-time view.
    """
    rng = random.Random(PIT_SEED)
    times = [e.event_time for e in events]  # events pre-sorted by caller
    rows: list[ReplayRow] = []

    for as_of in grid:
        cut = bisect.bisect_right(times, as_of)
        legs = pair_legs(
            events[:cut],
            as_of,
            weights=weights,
            fallback_region=FALLBACK_DEST_REGION,
        )
        for regime in regimes:
            curves: dict[tuple[str, str], ArrivalCurve] = {}
            for name, (u0, u1) in HORIZONS.items():
                fs = forecast_window(
                    legs, as_of, lane, u0=u0, u1=u1, curves=curves, regime=regime
                )
                if fs.n_legs == 0:
                    continue  # nothing at sea for this regime — not a question
                d = fs.predictive()
                truth = realised_arrivals(
                    truth_legs, as_of, lane, u0=u0, u1=u1, regime=regime
                )
                lo50, hi50 = d.interval(0.50)
                lo80, hi80 = d.interval(0.80)
                rows.append(
                    ReplayRow(
                        as_of=as_of,
                        horizon=name,
                        regime=regime,
                        truth=truth,
                        forecast=d.mean,
                        crps=d.crps(truth),
                        pit=d.pit(truth, rng.random()),
                        lo50=lo50,
                        hi50=hi50,
                        lo80=lo80,
                        hi80=hi80,
                        persistence=persistence_null(
                            truth_legs, as_of, lane, regime=regime
                        ),
                        climatology=climatology_null(
                            truth_legs, as_of, lane, regime=regime
                        ),
                        n_open=fs.n_legs,
                        n_beyond_support=fs.n_beyond_support,
                        supported=not any(
                            t.startswith("UNSUPPORTED") for t in fs.tiers
                        ),
                    )
                )
    return rows


# ----------------------------------------------------------------------
# Scoring
# ----------------------------------------------------------------------


def _mae(pairs) -> float:
    vals = [abs(a - b) for a, b in pairs]
    return sum(vals) / len(vals) if vals else float("nan")


def _rmse(pairs) -> float:
    vals = [(a - b) ** 2 for a, b in pairs]
    return (sum(vals) / len(vals)) ** 0.5 if vals else float("nan")


@dataclass(frozen=True)
class Scorecard:
    label: str
    n: int
    mean_truth: float
    mean_forecast: float
    a1_mae: float
    a1_rmse: float
    a1_crps: float
    persist_mae: float
    clim_mae: float
    cov50: float
    cov80: float

    @property
    def bias(self) -> float:
        """Mean forecast minus mean truth — the staleness signature if negative
        in growth years (pi lags the market by >= MATURITY_DAYS + the window)."""
        return self.mean_forecast - self.mean_truth

    @property
    def beats_persistence(self) -> bool:
        return self.a1_mae < self.persist_mae

    @property
    def beats_climatology(self) -> bool:
        return self.a1_mae < self.clim_mae


def score(rows: list[ReplayRow], label: str) -> Scorecard:
    if not rows:
        return Scorecard(label, 0, *[float("nan")] * 9)
    return Scorecard(
        label=label,
        n=len(rows),
        mean_truth=sum(r.truth for r in rows) / len(rows),
        mean_forecast=sum(r.forecast for r in rows) / len(rows),
        a1_mae=_mae([(r.forecast, r.truth) for r in rows]),
        a1_rmse=_rmse([(r.forecast, r.truth) for r in rows]),
        a1_crps=sum(r.crps for r in rows) / len(rows),
        persist_mae=_mae([(r.persistence, r.truth) for r in rows]),
        clim_mae=_mae([(r.climatology, r.truth) for r in rows]),
        cov50=sum(r.in_50 for r in rows) / len(rows),
        cov80=sum(r.in_80 for r in rows) / len(rows),
    )


def pit_histogram(rows: list[ReplayRow], bins: int = 10) -> list[int]:
    hist = [0] * bins
    for r in rows:
        hist[min(bins - 1, int(r.pit * bins))] += 1
    return hist


# ----------------------------------------------------------------------
# Reporting
# ----------------------------------------------------------------------

HDR = (
    f"{'slice':28s} {'n':>4s} {'truth':>6s} {'fcst':>6s} {'bias':>6s} "
    f"{'A1 MAE':>7s} {'RMSE':>6s} {'CRPS':>6s} {'persist':>8s} {'clim':>6s} "
    f"{'cov50':>6s} {'cov80':>6s}"
)


def fmt(sc: Scorecard) -> str:
    if sc.n == 0:
        return f"{sc.label:28s} {0:>4d}  (no scored weeks)"
    flag = ""
    if sc.beats_persistence and sc.beats_climatology:
        flag = "  <= beats both"
    elif sc.beats_persistence or sc.beats_climatology:
        flag = "  <= beats one"
    return (
        f"{sc.label:28s} {sc.n:4d} {sc.mean_truth:6.2f} {sc.mean_forecast:6.2f} "
        f"{sc.bias:+6.2f} {sc.a1_mae:7.3f} "
        f"{sc.a1_rmse:6.2f} {sc.a1_crps:6.3f} {sc.persist_mae:8.3f} "
        f"{sc.clim_mae:6.3f} {sc.cov50:6.1%} {sc.cov80:6.1%}{flag}"
    )


def report(rows: list[ReplayRow], primary_regime: str) -> None:
    print(f"\n{'=' * 124}\nA1 REPLAY SCORECARD")
    print(f"{'=' * 124}")
    print(f"scored observations: {len(rows)}   "
          f"regimes: {sorted({r.regime for r in rows})}   "
          f"span: {min(r.as_of for r in rows):%Y-%m-%d} -> "
          f"{max(r.as_of for r in rows):%Y-%m-%d}")
    unsupported = [r for r in rows if not r.supported]
    beyond = sum(r.n_beyond_support for r in rows)
    print(f"unsupported-curve rows: {len(unsupported)} (excluded from headline)   "
          f"legs beyond curve support: {beyond}")

    rows = [r for r in rows if r.supported]

    for horizon in ("W1", "W2"):
        hr = [r for r in rows if r.horizon == horizon]
        if not hr:
            continue
        print(f"\n--- {horizon} " + "-" * 112)
        print(HDR)
        for regime in sorted({r.regime for r in hr}):
            rr = [r for r in hr if r.regime == regime]
            print(fmt(score(rr, f"regime={regime}")))

        pr = [r for r in hr if r.regime == primary_regime]
        if not pr:
            continue
        print(f"\n  by year ({primary_regime}):")
        print("  " + HDR)
        for y in sorted({r.year for r in pr}):
            print("  " + fmt(score([r for r in pr if r.year == y], f"{y}")))

        print(f"\n  regime-episode split ({primary_regime}, MODELS.md §0·3 #1):")
        print("  " + HDR)
        crisis = [r for r in pr if r.year in (2021, 2022)]
        rest = [r for r in pr if r.year not in (2021, 2022)]
        print("  " + fmt(score(crisis, "2021-22 (crisis)")))
        print("  " + fmt(score(rest, "all other years")))

        hist = pit_histogram(pr)
        total = sum(hist) or 1
        print(f"\n  PIT histogram ({primary_regime}, seed={PIT_SEED}) — "
              f"uniform => calibrated:")
        for i, c in enumerate(hist):
            bar = "#" * round(40 * c / total * len(hist))
            print(f"    [{i / 10:.1f},{(i + 1) / 10:.1f})  {c:4d}  {bar}")

    print(f"\n{'=' * 124}\nPRE-REGISTERED ACCEPTANCE (D-003)\n{'=' * 124}")
    w1 = [r for r in rows if r.horizon == "W1" and r.regime == primary_regime]
    sc = score(w1, primary_regime)
    if sc.n == 0:
        print("  no scored W1 rows for the primary regime — cannot evaluate")
        return
    a = sc.beats_persistence and sc.beats_climatology
    b = 0.70 <= sc.cov80 <= 0.90
    print(f"  (a) W1 MAE beats BOTH nulls  : A1 {sc.a1_mae:.3f} vs "
          f"persist {sc.persist_mae:.3f}, clim {sc.clim_mae:.3f}  -> "
          f"{'PASS' if a else 'FAIL'}")
    print(f"  (b) 80% coverage in [70,90]% : {sc.cov80:.1%}  -> "
          f"{'PASS' if b else 'FAIL'}")
    print(f"\n  VERDICT: {'A1 WORKS' if a and b else 'A1 does NOT meet the bar'}")
    if a and not b:
        print("  (point baseline stands; the calibration gap is A3's opening — D-003)")


# ----------------------------------------------------------------------
# Loading + CLI
# ----------------------------------------------------------------------


async def load(pool: asyncpg.Pool):
    """Three queries, once — not per as-of."""
    async with pool.acquire() as conn:
        lane = build_lane_filter(await conn.fetch(TERMINAL_METADATA_SQL))
        ev_rows = await conn.fetch(LEG_EVENTS_SQL)
        w_rows = await conn.fetch(WEIGHTS_SQL)
    events = [
        LegEvent(
            mmsi=r["mmsi"],
            event_type=r["event_type"],
            event_time=r["event_time"],
            zone=r["zone"],
            terminal_id=r["terminal_id"],
            lat=r["lat"],
            lon=r["lon"],
            laden_flag=r["laden_flag"],
            source=r["source"],
        )
        for r in ev_rows
    ]
    events.sort(key=lambda e: e.event_time)
    weights = {r["mmsi"]: (r["dwt"], r["gas_capacity_m3"]) for r in w_rows}
    return lane, events, weights


async def main() -> None:
    ap = argparse.ArgumentParser(description="A1 arrival-count replay")
    ap.add_argument("--start", default=GRID_START.date().isoformat())
    ap.add_argument("--regimes", default="noaa,gfw")
    ap.add_argument("--primary", default="noaa")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=2)
    try:
        lane, events, weights = await load(pool)
        data_max = events[-1].event_time
        # Last as-of whose W2 window is fully observed.
        grid_end = data_max - timedelta(days=W2[1])
        start = datetime.fromisoformat(args.start).replace(tzinfo=UTC)
        grid = mondays(start, grid_end)
        logger.info(
            "events=%d  data_max=%s  grid=%d weekly as-ofs %s -> %s",
            len(events), f"{data_max:%Y-%m-%d}", len(grid),
            f"{grid[0]:%Y-%m-%d}", f"{grid[-1]:%Y-%m-%d}",
        )
        truth_legs = pair_legs(
            events, data_max + timedelta(days=1),
            weights=weights, fallback_region=FALLBACK_DEST_REGION,
        )
        logger.info("hindsight legs for truth: %d", len(truth_legs))

        rows = replay(
            events, weights, truth_legs, lane, grid, args.regimes.split(",")
        )
        report(rows, args.primary)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
