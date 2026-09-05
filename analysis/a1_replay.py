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


def build_horizons(n_weeks: int) -> dict[str, tuple[float, float]]:
    """Consecutive one-week duration windows `W1..Wn`, `Wk = ((k-1)*7, k*7]`.

    `n_weeks=2` reproduces the D-013 default exactly, so extending the horizon
    grid for D-025's H1 test cannot silently change the original scorecard.
    """
    return {f"W{k}": ((k - 1) * 7.0, k * 7.0) for k in range(1, n_weeks + 1)}


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
    horizons: dict[str, tuple[float, float]] | None = None,
) -> list[ReplayRow]:
    """Walk the grid and score. Pure given the loaded inputs.

    `truth_legs` is the hindsight leg set (D-011); `events` is the same underlying
    event stream, sliced per as-of to rebuild the point-in-time view.
    """
    horizons = HORIZONS if horizons is None else horizons
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
            for name, (u0, u1) in horizons.items():
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
    print(
        f"scored observations: {len(rows)}   "
        f"regimes: {sorted({r.regime for r in rows})}   "
        f"span: {min(r.as_of for r in rows):%Y-%m-%d} -> "
        f"{max(r.as_of for r in rows):%Y-%m-%d}"
    )
    unsupported = [r for r in rows if not r.supported]
    beyond = sum(r.n_beyond_support for r in rows)
    print(
        f"unsupported-curve rows: {len(unsupported)} (excluded from headline)   "
        f"legs beyond curve support: {beyond}"
    )

    rows = [r for r in rows if r.supported]

    horizon_names = sorted({r.horizon for r in rows}, key=lambda h: int(h[1:]))
    for horizon in horizon_names:
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
        print(
            f"\n  PIT histogram ({primary_regime}, seed={PIT_SEED}) — "
            f"uniform => calibrated:"
        )
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
    print(
        f"  (a) W1 MAE beats BOTH nulls  : A1 {sc.a1_mae:.3f} vs "
        f"persist {sc.persist_mae:.3f}, clim {sc.clim_mae:.3f}  -> "
        f"{'PASS' if a else 'FAIL'}"
    )
    print(
        f"  (b) 80% coverage in [70,90]% : {sc.cov80:.1%}  -> {'PASS' if b else 'FAIL'}"
    )
    print(f"\n  VERDICT: {'A1 WORKS' if a and b else 'A1 does NOT meet the bar'}")
    if a and not b:
        print("  (point baseline stands; the calibration gap is A3's opening — D-003)")


def report_h1(rows: list[ReplayRow], primary_regime: str) -> None:
    """Grade D-025's H1 horizon hypothesis — pre-specified 2026-08-12.

    H1's claim: the pipeline signal's lead is *mechanical*. A US->EU voyage takes
    14-18 d, so cargo at sea today determines EU arrivals two to three weeks out,
    while the naive nulls (which never extrapolate) should decay as `h` grows.
    Skill should therefore RISE with `h` and peak around h = 2-3.

    The three pre-specified predictions, graded verbatim:
      P1  skill over the nulls increasing across h = 1..4, peaking at h ~ 2-3
      P3  the nulls degrade monotonically in h
          -- if P3 fails the premise is wrong and H1 is abandoned, not patched
    (P2 concerns US loadings on the A2/A4 harness and is not scored here.)
    """
    rows = [r for r in rows if r.supported and r.regime == primary_regime]
    names = sorted({r.horizon for r in rows}, key=lambda h: int(h[1:]))
    cards = {h: score([r for r in rows if r.horizon == h], h) for h in names}

    print(f"\n{'=' * 124}\nD-025 H1 — HORIZON HYPOTHESIS (pre-specified 2026-08-12)")
    print(f"{'=' * 124}")
    print(f"  regime={primary_regime}\n")
    print(
        f"  {'h':<5}{'n':>6}{'truth':>8}{'A1 MAE':>9}{'persist':>9}{'clim':>8}"
        f"{'skill vs best null':>21}{'cov80':>8}"
    )
    print("  " + "-" * 74)
    skills: list[float] = []
    for h in names:
        sc = cards[h]
        best_null = min(sc.persist_mae, sc.clim_mae)
        skill = (best_null - sc.a1_mae) / best_null if best_null else float("nan")
        skills.append(skill)
        print(
            f"  {h:<5}{sc.n:>6}{sc.mean_truth:>8.2f}{sc.a1_mae:>9.3f}"
            f"{sc.persist_mae:>9.3f}{sc.clim_mae:>8.3f}{skill:>20.1%}{sc.cov80:>8.1%}"
        )

    # Comparability guard. A1's target is CONDITIONAL on legs already at sea
    # (D-001); both nulls are the last fully-elapsed UNCONDITIONAL weekly count
    # and carry no horizon at all. Those coincide while the window still holds
    # most of the at-sea stock (W1/W2: the minimum observed US->EU laden voyage
    # is 7.04 d), but past the voyage-time tail the conditional truth empties
    # toward zero while the nulls keep predicting the full weekly rate. "Skill"
    # then measures the target changing meaning, not a better forecast, so it is
    # flagged rather than reported as a win.
    base_truth = cards[names[0]].mean_truth
    incomparable = [
        h for h in names if base_truth > 0 and cards[h].mean_truth < 0.5 * base_truth
    ]
    if incomparable:
        last = cards[incomparable[-1]]
        print(
            f"\n  !! NOT COMPARABLE at {', '.join(incomparable)}: mean truth falls "
            f"from {base_truth:.2f} (W1) to {last.mean_truth:.2f},"
            f"\n     while the nulls stay unconditional ({last.persist_mae:.2f} MAE). "
            f"A1's target is conditional on"
            f"\n     legs already at sea; past the voyage tail it empties toward zero "
            f"and the nulls are"
            f"\n     answering a different question. The skill column is an ARTEFACT "
            f"there, not a win."
        )

    # P3: the premise. If the nulls do not decay, D-025 says abandon, not patch.
    persist = [cards[h].persist_mae for h in names]
    clim = [cards[h].clim_mae for h in names]
    p3_persist = all(b >= a for a, b in zip(persist, persist[1:]))
    p3_clim = all(b >= a for a, b in zip(clim, clim[1:]))
    p3 = p3_persist and p3_clim

    peak_idx = max(range(len(skills)), key=lambda i: skills[i])
    peak = names[peak_idx]
    rising = (
        all(b >= a for a, b in zip(skills[:2], skills[1:3]))
        if len(skills) >= 3
        else False
    )
    p1 = rising and peak in ("W2", "W3")

    peak_card = cards[peak]
    beats_both = peak_card.beats_persistence and peak_card.beats_climatology
    cov_ok = 0.70 <= peak_card.cov80 <= 0.90

    print(
        f"\n  P3  nulls degrade monotonically in h : "
        f"persistence {'yes' if p3_persist else 'NO'}, "
        f"climatology {'yes' if p3_clim else 'NO'}  -> {'PASS' if p3 else 'FAIL'}"
    )
    if not p3:
        print("      D-025: 'if they do not, the premise is wrong and H1 should be")
        print("      abandoned rather than pursued.' The nulls do not decay with h,")
        print("      so H1's mechanism has no room to operate on this target.")
    print(
        f"  P1  skill rises and peaks at h~2-3   : peak at {peak} "
        f"({skills[peak_idx]:+.1%}), rising={rising}  -> {'PASS' if p1 else 'FAIL'}"
    )
    print(
        f"  Bar (a) peak-horizon MAE beats BOTH nulls : "
        f"A1 {peak_card.a1_mae:.3f} vs persist {peak_card.persist_mae:.3f}, "
        f"clim {peak_card.clim_mae:.3f}  -> {'PASS' if beats_both else 'FAIL'}"
    )
    print(
        f"  Bar (b) 80% coverage in [70,90]%          : "
        f"{peak_card.cov80:.1%}  -> {'PASS' if cov_ok else 'FAIL'}"
    )
    if incomparable:
        verdict = "H1 NOT TESTABLE on this target — see the comparability note"
    elif p3 and p1 and beats_both and cov_ok:
        verdict = "H1 SUPPORTED"
    else:
        verdict = "H1 NOT SUPPORTED"
    print(f"\n  VERDICT: {verdict}")


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
    ap.add_argument(
        "--horizons",
        type=int,
        default=2,
        help="Number of consecutive weekly windows W1..Wn (default 2 = D-013; "
        "4 runs D-025's H1 horizon test)",
    )
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=2)
    try:
        lane, events, weights = await load(pool)
        data_max = events[-1].event_time
        horizons = build_horizons(args.horizons)
        # Last as-of whose LONGEST window is fully observed. Using W2 here would
        # score W3/W4 against truth that has not happened yet.
        max_u1 = max(u1 for _, u1 in horizons.values())
        grid_end = data_max - timedelta(days=max_u1)
        start = datetime.fromisoformat(args.start).replace(tzinfo=UTC)
        grid = mondays(start, grid_end)
        logger.info(
            "events=%d  data_max=%s  grid=%d weekly as-ofs %s -> %s",
            len(events),
            f"{data_max:%Y-%m-%d}",
            len(grid),
            f"{grid[0]:%Y-%m-%d}",
            f"{grid[-1]:%Y-%m-%d}",
        )
        truth_legs = pair_legs(
            events,
            data_max + timedelta(days=1),
            weights=weights,
            fallback_region=FALLBACK_DEST_REGION,
        )
        logger.info("hindsight legs for truth: %d", len(truth_legs))

        rows = replay(
            events,
            weights,
            truth_legs,
            lane,
            grid,
            args.regimes.split(","),
            horizons=horizons,
        )
        report(rows, args.primary)
        if len(horizons) > 2:
            report_h1(rows, args.primary)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
