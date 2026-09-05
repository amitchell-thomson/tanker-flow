"""A2 replay harness — walk-forward refit, forecast, score (MODELS.md Part A).

Build steps 3-4. Every modelling choice was fixed in D-016 before this ran: the
target, the five features and their expected signs, the model family, the training
protocol, the nulls, and the acceptance bar. This module only *measures*.

Structure mirrors `a1_replay.py` deliberately — same weekly grid, same 2018 start,
same `(lo, hi]` windows, same scoring reads — so the two models are comparable line
for line.

**Refit cadence.** Coefficients are refit at **every** as-of on that week's purged
training set, which is what makes A2 responsive where A1 was stale. That is ~440
NB fits per horizon; each is a 7-parameter L-BFGS-B solve on a few hundred rows.
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

from analysis.a2 import (
    AR_BLOCK,
    FEATURES,
    MIN_TRAIN_WEEKS,
    PHYSICAL,
    W1,
    W2,
    CountEvent,
    CountPredictive,
    GLMFit,
    Row,
    build_row,
    fit_glm,
    training_rows,
)
from config import settings
from pipeline.queues import QueueEvent, pair_queues
from pipeline.signal import TERMINAL_METADATA_SQL, LaneFilter, build_lane_filter
from pipeline.visits import VisitEvent, pair_visits

logger = logging.getLogger(__name__)

GRID_START = datetime(2018, 1, 1, tzinfo=UTC)
PIT_SEED = 20260812
HORIZONS = {"W1": W1, "W2": W2}

EVENTS_SQL = """
SELECT mmsi, event_type, event_time, zone, terminal_id, laden_flag, regime,
       COALESCE(cold_start, FALSE) AS cold_start
FROM port_events
ORDER BY event_time
"""
FLOW_SQL = (
    "SELECT terminal_id, flow_direction FROM terminals "
    "WHERE flow_direction IS NOT NULL"
)


@dataclass(frozen=True)
class ReplayRow:
    as_of: datetime
    horizon: str
    regime: str
    family: str
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
    n_train: int
    converged: bool
    k: float | None
    coefficients: tuple[float, ...]

    @property
    def year(self) -> int:
        return self.as_of.year

    @property
    def in_50(self) -> bool:
        return self.lo50 <= self.truth <= self.hi50

    @property
    def in_80(self) -> bool:
        return self.lo80 <= self.truth <= self.hi80

    def coefficient(self, name: str) -> float:
        return self.coefficients[FEATURES.index(name)]


def mondays(start: datetime, end: datetime) -> list[datetime]:
    out, d = [], start
    while d <= end:
        out.append(d)
        d += timedelta(days=7)
    return out


def build_all_rows(
    grid: list[datetime],
    events: list,
    lane: LaneFilter,
    flow_directions: dict[int, str],
    *,
    u0: float,
    u1: float,
    regime: str,
) -> list[Row]:
    """One design-matrix row per as-of, features sliced to `<= as_of`.

    The visit/queue pairings are recomputed on each slice rather than paired once
    over the whole decade: an open visit at `as_of` must look open *then*, not be
    retrospectively closed by a departure that had not happened yet.
    """
    times = [e["event_time"] for e in events]
    # The FULL stream supplies the target window, which lies after every as-of.
    # Slicing this would zero every label (see `build_row`).
    all_count_events = [
        CountEvent(
            mmsi=r["mmsi"], event_type=r["event_type"],
            event_time=r["event_time"], zone=r["zone"],
            laden_flag=r["laden_flag"], regime=r["regime"],
        )
        for r in events
    ]
    rows: list[Row] = []
    for as_of in grid:
        cut = bisect.bisect_right(times, as_of)
        sliced = events[:cut]
        feature_events = all_count_events[:cut]
        visits = pair_visits(
            [
                VisitEvent(
                    mmsi=r["mmsi"], event_type=r["event_type"],
                    event_time=r["event_time"], zone=r["zone"],
                    terminal_id=r["terminal_id"], laden_flag=r["laden_flag"],
                    cold_start=r["cold_start"],
                )
                for r in sliced
                if r["event_type"] in ("moored", "departed")
                and r["regime"] == regime
            ],
            flow_directions=flow_directions,
        )
        queues = pair_queues(
            [
                QueueEvent(
                    mmsi=r["mmsi"], event_type=r["event_type"],
                    event_time=r["event_time"], zone=r["zone"],
                    terminal_id=r["terminal_id"], laden_flag=r["laden_flag"],
                    cold_start=r["cold_start"],
                )
                for r in sliced
                if r["event_type"] in (
                    "anchorage_entry", "anchored", "anchorage_exit",
                    "moored", "departed",
                )
                and r["regime"] == regime
            ],
            flow_directions=flow_directions,
        )
        rows.append(
            build_row(
                as_of, feature_events, all_count_events, visits, queues, lane,
                u0=u0, u1=u1, regime=regime,
            )
        )
    return rows


def score_horizon(
    rows: list[Row],
    *,
    horizon: str,
    regime: str,
    u1: float,
    families: tuple[str, ...],
) -> list[ReplayRow]:
    """Walk forward: refit on the purged history at each as-of, then predict it."""
    rng = random.Random(PIT_SEED)
    out: list[ReplayRow] = []
    for i, row in enumerate(rows):
        train = training_rows(rows[:i], row.as_of, u1=u1)
        if len(train) < MIN_TRAIN_WEEKS:
            continue
        # The nulls are literally features 1 and 2 (D-016) — read them off the row
        # so the model and its baselines can never diverge in definition.
        persistence = row.feature("lag1")
        climatology = row.feature("trail4")
        for family in families:
            fit: GLMFit = fit_glm(train, family=family)
            mu = fit.predict_mu(row.features)
            d = CountPredictive.build(mu, fit.k)
            lo50, hi50 = d.interval(0.50)
            lo80, hi80 = d.interval(0.80)
            out.append(
                ReplayRow(
                    as_of=row.as_of, horizon=horizon, regime=regime,
                    family=family, truth=row.target, forecast=mu,
                    crps=d.crps(row.target), pit=d.pit(row.target, rng.random()),
                    lo50=lo50, hi50=hi50, lo80=lo80, hi80=hi80,
                    persistence=persistence, climatology=climatology,
                    n_train=fit.n_train, converged=fit.converged, k=fit.k,
                    coefficients=tuple(
                        fit.coefficient(n) for n in FEATURES
                    ),
                )
            )
    return out


# ----------------------------------------------------------------------
# Scoring
# ----------------------------------------------------------------------


def _mae(pairs) -> float:
    v = [abs(a - b) for a, b in pairs]
    return sum(v) / len(v) if v else float("nan")


def _rmse(pairs) -> float:
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
        label=label,
        n=len(rows),
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


def pit_histogram(rows: list[ReplayRow], bins: int = 10) -> list[int]:
    hist = [0] * bins
    for r in rows:
        hist[min(bins - 1, int(r.pit * bins))] += 1
    return hist


HDR = (
    f"{'slice':26s} {'n':>4s} {'truth':>6s} {'fcst':>6s} {'bias':>6s} "
    f"{'MAE':>7s} {'RMSE':>6s} {'CRPS':>6s} {'persist':>8s} {'clim':>6s} "
    f"{'cov50':>6s} {'cov80':>6s}"
)


def fmt(sc: Scorecard) -> str:
    if sc.n == 0:
        return f"{sc.label:26s} {0:>4d}  (no scored weeks)"
    flag = ""
    if sc.beats_persistence and sc.beats_climatology:
        flag = "  <= beats both"
    elif sc.beats_persistence or sc.beats_climatology:
        flag = "  <= beats one"
    return (
        f"{sc.label:26s} {sc.n:4d} {sc.mean_truth:6.2f} {sc.mean_forecast:6.2f} "
        f"{sc.bias:+6.2f} {sc.mae:7.3f} {sc.rmse:6.2f} {sc.crps:6.3f} "
        f"{sc.persist_mae:8.3f} {sc.clim_mae:6.3f} "
        f"{sc.cov50:6.1%} {sc.cov80:6.1%}{flag}"
    )


def report(rows: list[ReplayRow], primary_regime: str, primary_family: str) -> None:
    print(f"\n{'=' * 126}\nA2 REPLAY SCORECARD — weekly US loadings")
    print(f"{'=' * 126}")
    print(f"scored observations: {len(rows)}   "
          f"span: {min(r.as_of for r in rows):%Y-%m-%d} -> "
          f"{max(r.as_of for r in rows):%Y-%m-%d}")
    bad = [r for r in rows if not r.converged]
    print(f"non-converged fits: {len(bad)} of {len(rows)}")

    for horizon in ("W1", "W2"):
        hr = [r for r in rows if r.horizon == horizon]
        if not hr:
            continue
        print(f"\n--- {horizon} " + "-" * 114)
        print(HDR)
        for regime in sorted({r.regime for r in hr}):
            for family in sorted({r.family for r in hr}):
                sub = [r for r in hr if r.regime == regime and r.family == family]
                print(fmt(score(sub, f"{regime}/{family}")))

        pr = [
            r for r in hr
            if r.regime == primary_regime and r.family == primary_family
        ]
        if not pr:
            continue
        print(f"\n  by year ({primary_regime}/{primary_family}):")
        print("  " + HDR)
        for y in sorted({r.year for r in pr}):
            print("  " + fmt(score([r for r in pr if r.year == y], f"{y}")))

        print("\n  regime-episode split (MODELS.md §0·3 #1):")
        print("  " + HDR)
        print("  " + fmt(
            score([r for r in pr if r.year in (2021, 2022)], "2021-22 (crisis)")
        ))
        print("  " + fmt(
            score([r for r in pr if r.year not in (2021, 2022)], "all other years")
        ))

        hist = pit_histogram(pr)
        total = sum(hist) or 1
        print(f"\n  PIT histogram (seed={PIT_SEED}) — uniform => calibrated:")
        for i, c in enumerate(hist):
            print(f"    [{i / 10:.1f},{(i + 1) / 10:.1f})  {c:4d}  "
                  f"{'#' * round(40 * c / total * len(hist))}")

    # --- coefficients ----------------------------------------------------
    pr1 = [
        r for r in rows
        if r.horizon == "W1" and r.regime == primary_regime
        and r.family == primary_family
    ]
    if pr1:
        print(f"\n{'=' * 126}\nCOEFFICIENTS (W1, {primary_regime}/{primary_family}) "
              f"— standardised units; D-016 pre-registered sign is + for all")
        print(f"{'=' * 126}")
        print(f"{'feature':22s} {'mean':>8s} {'median':>8s} "
              f"{'% weeks +':>10s}   by era (mean)")
        eras = [(2018, 2020), (2021, 2022), (2023, 2026)]
        for name in FEATURES:
            vals = [r.coefficient(name) for r in pr1]
            vals_s = sorted(vals)
            med = vals_s[len(vals_s) // 2]
            pos = sum(1 for v in vals if v > 0) / len(vals)
            cells = []
            for lo, hi in eras:
                sub = [r.coefficient(name) for r in pr1 if lo <= r.year <= hi]
                cells.append(
                    f"{lo}-{hi}: {sum(sub) / len(sub):+.3f}" if sub
                    else f"{lo}-{hi}: -"
                )
            print(f"{name:22s} {sum(vals) / len(vals):+8.3f} {med:+8.3f} "
                  f"{pos:9.0%}   " + "  ".join(cells))
        ar = [sum(r.coefficient(n) for n in AR_BLOCK) for r in pr1]
        print(f"{'AR block (joint)':22s} {sum(ar) / len(ar):+8.3f} "
              f"{sorted(ar)[len(ar) // 2]:+8.3f} "
              f"{sum(1 for v in ar if v > 0) / len(ar):9.0%}")
        ks = [r.k for r in pr1 if r.k is not None]
        if ks:
            print(f"\n  NB dispersion k: mean {sum(ks) / len(ks):.2f}, "
                  f"median {sorted(ks)[len(ks) // 2]:.2f}  "
                  f"(large k => effectively Poisson)")

    # --- verdict ---------------------------------------------------------
    print(f"\n{'=' * 126}\nPRE-REGISTERED ACCEPTANCE (D-016)\n{'=' * 126}")
    sc = score(pr1, "primary")
    if sc.n == 0:
        print("  no scored W1 rows for the primary slice — cannot evaluate")
        return
    a = sc.beats_persistence and sc.beats_climatology
    b = 0.70 <= sc.cov80 <= 0.90
    print(f"  (a) W1 MAE beats BOTH nulls  : A2 {sc.mae:.3f} vs "
          f"persist {sc.persist_mae:.3f}, clim {sc.clim_mae:.3f}  -> "
          f"{'PASS' if a else 'FAIL'}")
    print(f"  (b) 80% coverage in [70,90]% : {sc.cov80:.1%}  -> "
          f"{'PASS' if b else 'FAIL'}")
    print(f"\n  VERDICT: {'A2 WORKS' if a and b else 'A2 does NOT meet the bar'}")
    print("\n  (c) MECHANISM — pre-registered signs (reported, binding on the "
          "narrative, not on pass/fail):")
    ar_mean = sum(sum(r.coefficient(n) for n in AR_BLOCK) for r in pr1) / len(pr1)
    print(f"      AR block (lag1+trail4) > 0 : {ar_mean:+.3f}  "
          f"-> {'HOLDS' if ar_mean > 0 else 'FALSIFIED'}")
    for name in PHYSICAL:
        m = sum(r.coefficient(name) for r in pr1) / len(pr1)
        print(f"      {name:24s} > 0 : {m:+.3f}  "
              f"-> {'HOLDS' if m > 0 else 'FALSIFIED'}")


# ----------------------------------------------------------------------
# Loading + CLI
# ----------------------------------------------------------------------


async def main() -> None:
    ap = argparse.ArgumentParser(description="A2 count-GLM replay")
    ap.add_argument("--start", default=GRID_START.date().isoformat())
    ap.add_argument("--regimes", default="noaa,gfw")
    ap.add_argument("--primary", default="noaa")
    ap.add_argument("--families", default="nb,poisson")
    ap.add_argument("--json", action="store_true", help="Also write paper/results/a2.json")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            lane = build_lane_filter(await conn.fetch(TERMINAL_METADATA_SQL))
            events = [dict(r) for r in await conn.fetch(EVENTS_SQL)]
            flow_directions = {
                r["terminal_id"]: r["flow_direction"]
                for r in await conn.fetch(FLOW_SQL)
            }
    finally:
        await pool.close()

    data_max = events[-1]["event_time"]
    families = tuple(args.families.split(","))
    all_rows: list[ReplayRow] = []
    for regime in args.regimes.split(","):
        for name, (u0, u1) in HORIZONS.items():
            grid = mondays(
                datetime.fromisoformat(args.start).replace(tzinfo=UTC),
                data_max - timedelta(days=u1),
            )
            logger.info(
                "building %s/%s: %d as-ofs %s -> %s",
                regime, name, len(grid),
                f"{grid[0]:%Y-%m-%d}", f"{grid[-1]:%Y-%m-%d}",
            )
            rows = build_all_rows(
                grid, events, lane, flow_directions, u0=u0, u1=u1, regime=regime
            )
            all_rows += score_horizon(
                rows, horizon=name, regime=regime, u1=u1, families=families
            )
    report(all_rows, args.primary, families[0])
    if args.json:
        from analysis.results_io import dump

        pr = [r for r in all_rows if r.regime == args.primary and r.family == families[0]]
        dump(
            "a2",
            {
                "primary_regime": args.primary,
                "primary_family": families[0],
                "scorecards": {
                    h: score([r for r in pr if r.horizon == h], h) for h in HORIZONS
                },
                "mean_coefficients": {
                    name: sum(r.coefficient(name) for r in pr if r.horizon == "W1")
                    / max(1, sum(1 for r in pr if r.horizon == "W1"))
                    for name in FEATURES
                },
            },
        )


if __name__ == "__main__":
    asyncio.run(main())
