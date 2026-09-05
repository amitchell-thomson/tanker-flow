"""A1 — arrival-count baseline (MODELS.md Part A; specs in DECISIONS.md D-001..).

Steps 2-4 of the build. Steps 2-3 give the **per-leg arrival probability**: given a
laden US-origin leg still open at age `a`, the probability it produces an observed EU
arrival inside `(a+u0, a+u1]`. Step 4 convolves those into the **exact predictive
distribution** of the weekly arrival count (Poisson-binomial), which is what the
D-003 scorecard needs — CRPS and interval coverage read the real PMF, not a normal
approximation to it.

Everything here is pure: it takes an already-loaded leg list (from
`compute_legs(..., point_in_time=True)`) and an `as_of`, and returns estimates
using only legs whose outcome was determined by `as_of`. No DB, no I/O.

Why an empirical curve instead of D-002's parametric posterior
-------------------------------------------------------------
D-002 specified `p = pi*[S(a+u0)-S(a+u1)] / (pi*S(a) + 1 - pi)`, whose denominator
assumes non-EU legs essentially never close ("survival ~ 1"). The decade falsifies
that: `same_zone` legs — 46 % of the matured laden export-origin population — have a
**median duration of 34 days** (p90 77 d). They are not berth shifts; they are
departures whose EU arrival we never observed, pairing instead with the vessel's
eventual return to the Gulf. They leave the open pool steadily, so the survival-~1
premise is wrong in exactly the age range A1 forecasts over.

The quantity that assumption existed to produce is directly measurable, so measure
it (D-007). Whole matured sample, `pi | still open at age a`:

    a (d)   0.0   1.0   5.0  10.0  12.0  14.0  16.0  18.0  21.0  30.0  60.0
    pi     .351  .404  .412  .412  .393  .317  .236  .172  .122  .076  .052

Flat to ~day 10, then collapsing through the 12-18 d voyage window as EU-bound legs
close and the residual open pool becomes non-EU / missed-arrival. That shape is the
model — no distributional assumption required, and it degrades an aging open leg's
arrival probability correctly rather than by fiat.

The two factors telescope (D-009)
---------------------------------
D-007's kernel splits the question into *will it be EU?* and *when?*:

    p_i(W) = pi(a) * [F_eu(a+u1) - F_eu(a+u0)] / (1 - F_eu(a))

An EU leg "open at age a" is exactly one whose duration exceeds `a`, so
`N_eu(open at a) == N_eu(dur > a)` and the middle terms cancel:

    p_i(W) = [N_eu(open a)/N(open a)] * [N_eu(dur in (a+u0,a+u1])/N_eu(dur > a)]
           =  N_eu(dur in (a+u0, a+u1]) / N(open at a)

So the decomposition is an exact factorisation of a single count ratio, not two
independent approximations — *provided both factors come from one population*. This
module therefore builds ONE `ArrivalCurve` per (origin, regime) from ONE matured
population, evaluates the telescoped form directly (no ratio, so no 0/0 and no tail
rule), and exposes `pi_at` / `f_eu` as views for interpretation and for A3.

`pi` is deliberately **capture-inclusive**: a leg whose real EU arrival we failed to
observe counts in the denominator, not the numerator. A1 therefore predicts *observed*
arrivals, which is what the truth series (D-003) also measures. The two are on the
same footing; neither is corrected for capture here (that is A7's job).
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass
from datetime import datetime, timedelta

from pipeline.legs import MAX_LEG_PAIR_DAYS, Leg
from pipeline.signal import LaneFilter


# A leg's outcome is determined — by construction, not by assumption — once it is
# this old: `pair_legs` refuses to pair a departure with an arrival beyond
# MAX_LEG_PAIR_DAYS, so nothing about it can change afterwards. Using the same
# structural constant (rather than a tuned "long enough" gate) is what keeps the
# maturity rule out of the researcher-degrees-of-freedom budget (MODELS.md §0·3 #3).
# Cost: pi is estimated from data at least this stale. Documented, not tuned.
MATURITY_DAYS = MAX_LEG_PAIR_DAYS

# Rolling estimation window on *departure* date. Not expanding: pi drifts ~4x
# across the decade (0.12 in 2016 -> 0.55 in 2022 -> 0.46 in 2025, both from the
# trade-flow regime change and the capture gradient of §0·1), so an expanding
# window is a badly lagging estimator. 365 d holds >= ~350 legs even in the thin
# early years and ~1,500 recently — comfortably above PI_MIN_LEGS. Chosen on
# sample-size grounds before any scoring; not to be retuned against results.
PI_WINDOW_DAYS = 365
PI_WIDE_WINDOW_DAYS = 730

# Minimum matured legs for a curve to be trusted; below this the ladder widens.
PI_MIN_LEGS = 100

# Minimum still-open legs at a given age for that age's estimate to be trusted.
# N(open at a) is non-increasing in a, so this is a suffix truncation: beyond
# `trusted_max_d` the curve rests on < 20 legs and A1 declines to forecast
# (`arrival_probability` returns 0.0 and the leg is reported as dropped, never
# silently zeroed — see `ForecastSet.n_beyond_support`).
MIN_OPEN_AT_AGE = 20

# Diagnostic grid for rendering pi(a); the estimators themselves are exact at
# arbitrary ages via bisect and do not use it.
CURVE_STEP_D = 0.5
CURVE_MAX_D = float(MATURITY_DAYS)

_INF = float("inf")


def is_eu_arrival(leg: Leg, lane: LaneFilter) -> bool:
    """Did this leg produce an observed EU arrival? The numerator's definition,
    and deliberately identical to the truth definition in D-003 — A1 predicts the
    same event it is scored against."""
    return leg.status == "closed" and lane.is_import(leg.dest_zone)


def leg_close_age_d(leg: Leg) -> float | None:
    """Age in days at which the leg left the open pool, or None if it never did.

    Note this is *any* close, not just an EU one: a `same_zone` return to the Gulf
    removes a leg from the open pool exactly as an EU arrival does. Conflating them
    is the error the survival-~1 assumption made."""
    if leg.arrived_ts is None:
        return None
    return (leg.arrived_ts - leg.departed_ts).total_seconds() / 86400.0


def is_open_at(leg: Leg, age_d: float) -> bool:
    """Was this leg still open (no arrival yet) at `age_d` days after departure?"""
    close = leg_close_age_d(leg)
    return close is None or close > age_d


def leg_age_d(leg: Leg, as_of: datetime) -> float:
    """Days since departure at `as_of` — the `a` every estimator is keyed on."""
    return (as_of - leg.departed_ts).total_seconds() / 86400.0


@dataclass(frozen=True)
class ArrivalCurve:
    """The A1 model for one (origin_zone, regime), as two sorted count arrays.

    `closes` — every matured leg's close age (`inf` = never closed), sorted.
    `eu_closes` — the close ages of the EU-arriving subset, sorted.

    Every estimate below is a suffix/range count over these, so all are exact at
    arbitrary ages in O(log n) and are guaranteed mutually consistent (the
    telescoping identity in the module docstring holds by construction).

    `tier` and `n_legs` record provenance: which rung of the fallback ladder
    produced the curve and how much data stands behind it.
    """

    closes: tuple[float, ...]
    eu_closes: tuple[float, ...]
    n_legs: int
    tier: str
    trusted_max_d: float

    # --- primitive counts ---

    def n_open(self, age_d: float) -> int:
        """Matured legs still open at `age_d`."""
        return len(self.closes) - bisect.bisect_right(self.closes, age_d)

    def n_eu_open(self, age_d: float) -> int:
        """EU-arriving legs still open at `age_d` (i.e. duration > age_d)."""
        return len(self.eu_closes) - bisect.bisect_right(self.eu_closes, age_d)

    # --- the two interpretable factors (views; not used by the estimator) ---

    def pi_at(self, age_d: float) -> float:
        """`pi(a)` = P(eventual observed EU arrival | still open at age a).

        The destination model. Beyond `trusted_max_d` the last trusted value is
        carried forward — this is a *diagnostic* readout; the forecast itself
        declines beyond support rather than extrapolating (`arrival_probability`).
        """
        a = min(max(age_d, 0.0), self.trusted_max_d)
        open_at = self.n_open(a)
        return self.n_eu_open(a) / open_at if open_at else 0.0

    def f_eu(self, duration_d: float) -> float:
        """`F_eu(d)` = CDF of voyage duration over EU-arriving legs only.

        The timing model. Exposed for interpretation and as A3's starting point
        (A3 replaces this ECDF with a fitted, censoring-aware distribution).
        """
        if not self.eu_closes:
            return 0.0
        return bisect.bisect_right(self.eu_closes, duration_d) / len(self.eu_closes)

    # --- the estimator ---

    def arrival_probability(self, age_d: float, u0: float, u1: float) -> float:
        """P(observed EU arrival in `(a+u0, a+u1]` | still open at age `a`).

        The telescoped form: EU legs whose duration lands in the window, over all
        legs still open at `a`. No ratio of ratios, so no 0/0 and no tail rule —
        an age with few EU legs ahead of it simply scores near zero on its own.

        **Window is left-open, right-closed** — deliberately, and unlike the
        panel's half-open *calendar-day* buckets. This is a duration window
        measured from the leg's own age, and "still open at `a`" already means
        `duration > a` (strictly). Pairing that with `(a+u0, a+u1]` makes the
        windows partition `(a, a+u1]` exactly and makes the telescoping identity
        hold *exactly* rather than up to a measure-zero boundary: `n_open` and
        `n_eu_open` are `bisect_right` suffix counts, so the window must be too.
        The only disagreement with the calendar-week truth series is an arrival
        landing precisely on a window edge — impossible for `a` (such a leg is
        already closed, hence not in the forecast population) and measure-zero
        for `a+u1` on continuous timestamps.

        Returns 0.0 beyond `trusted_max_d` (fewer than MIN_OPEN_AT_AGE legs of
        evidence): A1 declines rather than extrapolates. Callers count these.
        """
        a = max(age_d, 0.0)
        if a > self.trusted_max_d:
            return 0.0
        open_at = self.n_open(a)
        if open_at == 0:
            return 0.0
        lo = bisect.bisect_right(self.eu_closes, a + u0)
        hi = bisect.bisect_right(self.eu_closes, a + u1)
        return (hi - lo) / open_at

    def pi_grid(self) -> list[tuple[float, float, int]]:
        """`[(age_d, pi, n_open)]` on the diagnostic grid — for probes/write-up."""
        out, a = [], 0.0
        while a <= CURVE_MAX_D + 1e-9:
            out.append((a, self.pi_at(a), self.n_open(a)))
            a += CURVE_STEP_D
        return out


def matured_population(
    legs: list[Leg],
    as_of: datetime,
    lane: LaneFilter,
    *,
    window_days: int,
    origin_zone: str | None = None,
    regime: str | None = None,
) -> list[Leg]:
    """Laden US-export-origin legs whose outcome was determined by `as_of`.

    Window is `[as_of - MATURITY_DAYS - window_days, as_of - MATURITY_DAYS]` on
    departure date. `origin_zone` / `regime` = None means pooled over that axis
    (the ladder's wider rungs).
    """
    hi = as_of - timedelta(days=MATURITY_DAYS)
    lo = hi - timedelta(days=window_days)
    out = []
    for lg in legs:
        if lg.laden is not True or not lane.is_export(lg.origin_zone):
            continue
        if not (lo <= lg.departed_ts <= hi):
            continue
        if origin_zone is not None and lg.origin_zone != origin_zone:
            continue
        if regime is not None and lg.regime != regime:
            continue
        out.append(lg)
    return out


def _curve_from(population: list[Leg], lane: LaneFilter, tier: str) -> ArrivalCurve:
    """Build the sorted count arrays from an already-filtered matured population."""
    closes: list[float] = []
    eu_closes: list[float] = []
    for lg in population:
        c = leg_close_age_d(lg)
        closes.append(_INF if c is None else c)
        if is_eu_arrival(lg, lane):
            eu_closes.append(c)  # EU => closed => c is not None
    closes.sort()
    eu_closes.sort()

    # N(open at a) >= K  <=>  a < closes[n-K]. Exact, no grid scan needed.
    n = len(closes)
    trusted = closes[n - MIN_OPEN_AT_AGE] if n >= MIN_OPEN_AT_AGE else 0.0
    return ArrivalCurve(
        closes=tuple(closes),
        eu_closes=tuple(eu_closes),
        n_legs=n,
        tier=tier,
        trusted_max_d=trusted,
    )


def build_arrival_curve(
    legs: list[Leg],
    as_of: datetime,
    lane: LaneFilter,
    *,
    origin_zone: str,
    regime: str,
) -> ArrivalCurve:
    """The A1 curve for one (origin_zone, regime), via an explicit fallback ladder.

    Rungs widen one axis at a time — window, then origin, then regime — so an
    estimate is never silently pooled across a fidelity seam (MODELS.md §1) before
    the cheaper widenings have been tried. The chosen rung is recorded on the
    curve; callers surface it rather than dropping it.
    """
    ladder = [
        (f"{origin_zone}/{regime}/{PI_WINDOW_DAYS}d", PI_WINDOW_DAYS, origin_zone, regime),
        (f"{origin_zone}/{regime}/{PI_WIDE_WINDOW_DAYS}d", PI_WIDE_WINDOW_DAYS, origin_zone, regime),
        (f"pooled-origin/{regime}/{PI_WIDE_WINDOW_DAYS}d", PI_WIDE_WINDOW_DAYS, None, regime),
        (f"pooled-origin/pooled-regime/{PI_WIDE_WINDOW_DAYS}d", PI_WIDE_WINDOW_DAYS, None, None),
    ]
    last = None
    for tier, window, zone, reg in ladder:
        pop = matured_population(
            legs, as_of, lane, window_days=window, origin_zone=zone, regime=reg
        )
        last = _curve_from(pop, lane, tier)
        if last.n_legs >= PI_MIN_LEGS:
            return last
    # Every rung thin (early panel / burn-in). Return the widest attempt, marked,
    # so the caller can see the estimate is unsupported rather than infer it isn't.
    return ArrivalCurve(
        closes=last.closes,
        eu_closes=last.eu_closes,
        n_legs=last.n_legs,
        tier=f"UNSUPPORTED({last.tier})",
        trusted_max_d=last.trusted_max_d,
    )


# ----------------------------------------------------------------------
# Assembling the forecast population
# ----------------------------------------------------------------------


def open_forecast_legs(
    legs: list[Leg],
    as_of: datetime,
    lane: LaneFilter,
    *,
    regime: str | None = None,
) -> list[Leg]:
    """The at-sea laden US-origin cargoes A1 forecasts from, at `as_of`.

    **All** open statuses are kept, including `open_censored`. That is not an
    oversight: the curve's denominator is "matured legs still open at age a",
    which likewise makes no status distinction — many of those legs eventually
    resolved to phantoms, and `pi(a)`'s collapse past day ~12 is precisely the
    model learning that. Filtering phantoms out of the forecast population while
    leaving them in the estimation population would double-count the correction
    and bias the forecast up.

    `regime` restricts to one departure fidelity, for the D-003 per-regime
    breakout; the matching truth call must carry the same filter.
    """
    return [
        lg
        for lg in legs
        if lg.status.startswith("open")
        and lg.laden is True
        and lane.is_export(lg.origin_zone)
        and lg.departed_ts <= as_of
        and (regime is None or lg.regime == regime)
    ]


# ----------------------------------------------------------------------
# Step 4 — the exact predictive distribution (Poisson-binomial)
# ----------------------------------------------------------------------


def poisson_binomial_pmf(probabilities) -> tuple[float, ...]:
    """Exact PMF of `sum of independent Bernoulli(p_i)`; index `k` = P(count == k).

    Straight convolution DP: fold one Bernoulli in at a time,
    `new[k] = old[k]*(1-p) + old[k-1]*p`. All terms are positive so there is no
    cancellation — numerically stable at these sizes, and exact rather than the
    normal/Poisson approximations usually substituted for it.

    **Zero-probability legs are dropped first.** A Bernoulli(0) leaves the
    convolution unchanged, so this is exact, not an approximation — and it is what
    keeps the DP cheap: at 2025-01-06 the forecast carries 1,348 open legs but only
    **129 with p > 0** (the rest are long-overdue legs whose window holds no EU
    arrivals at all), turning an O(1348^2) fold into O(129^2).

    Independence across legs is A1's assumption, stated plainly: a terminal outage
    or a freeze would correlate arrivals and the true variance would exceed this.
    That correlation is A5's subject, not A1's.
    """
    ps = [p for p in probabilities if p > 0.0]
    pmf = [1.0]
    for p in ps:
        q = 1.0 - p
        nxt = [0.0] * (len(pmf) + 1)
        for k, mass in enumerate(pmf):
            nxt[k] += mass * q
            nxt[k + 1] += mass * p
        pmf = nxt
    return tuple(pmf)


@dataclass(frozen=True)
class Predictive:
    """The forecast as a distribution over counts, with the D-003 scoring reads."""

    pmf: tuple[float, ...]  # index k = P(count == k)

    @classmethod
    def from_probabilities(cls, probabilities) -> Predictive:
        return cls(pmf=poisson_binomial_pmf(probabilities))

    @property
    def mean(self) -> float:
        return sum(k * m for k, m in enumerate(self.pmf))

    @property
    def variance(self) -> float:
        mu = self.mean
        return sum(m * (k - mu) ** 2 for k, m in enumerate(self.pmf))

    def cdf(self, k: int) -> float:
        """P(count <= k). Clamps outside the support."""
        if k < 0:
            return 0.0
        return sum(self.pmf[: min(k, len(self.pmf) - 1) + 1])

    def quantile(self, alpha: float) -> int:
        """Smallest `k` with `cdf(k) >= alpha` — the discrete quantile function."""
        acc = 0.0
        for k, m in enumerate(self.pmf):
            acc += m
            if acc >= alpha - 1e-12:
                return k
        return len(self.pmf) - 1

    def interval(self, coverage: float) -> tuple[int, int]:
        """Central interval `[lo, hi]` holding at least `coverage` of the mass.

        Discreteness makes this **conservative**: actual coverage is >= nominal,
        so the D-003 acceptance band (80 % interval covering 70-90 % of outcomes)
        is being read against a slightly wide interval by construction. Noted so
        an over-coverage result is not mistaken for a calibration failure.
        """
        tail = (1.0 - coverage) / 2.0
        return self.quantile(tail), self.quantile(1.0 - tail)

    def crps(self, observed: int) -> float:
        """CRPS for a count forecast — the Ranked Probability Score.

        `sum_k (F(k) - 1{observed <= k})^2`, summed far enough to cover both the
        support and an observation beyond it (where `F(k) = 1` and the indicator is
        still 0, so each such k contributes 1). Lower is better; it scores the whole
        distribution, not just its mean.
        """
        upper = max(len(self.pmf) - 1, observed)
        total, cum = 0.0, 0.0
        for k in range(upper + 1):
            if k < len(self.pmf):
                cum += self.pmf[k]
            total += (cum - (1.0 if observed <= k else 0.0)) ** 2
        return total

    def pit(self, observed: int, u: float) -> float:
        """Randomised PIT value for a discrete forecast.

        `F(y-1) + u * P(Y = y)` with `u ~ U(0,1)`. The randomisation is what makes
        the PIT histogram uniform under a correctly-calibrated *discrete* forecast;
        `u` is passed in rather than drawn here so the function stays pure and the
        replay controls the seed.
        """
        below = self.cdf(observed - 1)
        mass = self.pmf[observed] if 0 <= observed < len(self.pmf) else 0.0
        return below + u * mass


@dataclass(frozen=True)
class ForecastSet:
    """Per-leg arrival probabilities for one window, plus what was left out."""

    probabilities: tuple[float, ...]
    n_legs: int
    n_beyond_support: int  # open legs older than their curve's trusted range
    tiers: tuple[str, ...]  # distinct ladder rungs used, for provenance

    @property
    def expected(self) -> float:
        """Poisson-binomial mean — `sum(p_i)`, identical to `predictive().mean`."""
        return sum(self.probabilities)

    @property
    def variance(self) -> float:
        """Poisson-binomial variance — `sum(p_i * (1 - p_i))`."""
        return sum(p * (1.0 - p) for p in self.probabilities)

    def predictive(self) -> Predictive:
        """The exact count distribution behind this forecast."""
        return Predictive.from_probabilities(self.probabilities)


# ----------------------------------------------------------------------
# Step 5 — the truth series and the two nulls (D-003)
# ----------------------------------------------------------------------
#
# Truth is computed on a **hindsight** leg load (`compute_legs` past the data max)
# — that is exactly what the `physical` basis is for, and what A1's `knowable`
# forecast is scored against. Two properties make the comparison honest:
#
#   1. It counts the SAME event the forecast predicts — `is_eu_arrival`, the one
#      definition, used by both sides. Both are therefore capture-limited in the
#      same way (see the capture-inclusive note at the top).
#   2. It is **conditional on having already departed by `as_of`**, matching the
#      forecast population. An arrival from a vessel that departed *after* `as_of`
#      is not something A1 was ever asked to see, so counting it would be scoring
#      A1 against a target it was structurally denied.
#
# Window convention is `(lo, hi]`, identical to `arrival_probability` (D-009).

WINDOW_DAYS = 7.0
W1: tuple[float, float] = (0.0, 7.0)  # the coming week
W2: tuple[float, float] = (7.0, 14.0)  # the week after
CLIMATOLOGY_WEEKS = 4


def arrivals_in_window(
    legs: list[Leg],
    lane: LaneFilter,
    *,
    lo: datetime,
    hi: datetime,
    departed_by: datetime,
    regime: str | None = None,
) -> int:
    """Observed EU arrivals in `(lo, hi]` from laden US-origin legs already
    departed at `departed_by`. The one primitive under both truth and the nulls."""
    n = 0
    for lg in legs:
        if lg.laden is not True or not lane.is_export(lg.origin_zone):
            continue
        if lg.departed_ts > departed_by:
            continue
        if regime is not None and lg.regime != regime:
            continue
        if not is_eu_arrival(lg, lane):
            continue
        if lg.arrived_ts is not None and lo < lg.arrived_ts <= hi:
            n += 1
    return n


def realised_arrivals(
    legs: list[Leg],
    as_of: datetime,
    lane: LaneFilter,
    *,
    u0: float,
    u1: float,
    regime: str | None = None,
) -> int:
    """The D-001 target: conditional EU arrival count in `(as_of+u0, as_of+u1]`."""
    return arrivals_in_window(
        legs,
        lane,
        lo=as_of + timedelta(days=u0),
        hi=as_of + timedelta(days=u1),
        departed_by=as_of,
        regime=regime,
    )


def _trailing_week(
    legs: list[Leg],
    as_of: datetime,
    lane: LaneFilter,
    *,
    weeks_back: int,
    regime: str | None,
) -> int:
    """Count for the `weeks_back`-th fully-elapsed week before `as_of`.

    `weeks_back=1` is `(as_of-7d, as_of]`. Each week is conditioned on departures
    known at *its own* start, so every value is like-for-like with the target.
    """
    hi = as_of - timedelta(days=WINDOW_DAYS * (weeks_back - 1))
    lo = as_of - timedelta(days=WINDOW_DAYS * weeks_back)
    return arrivals_in_window(
        legs, lane, lo=lo, hi=hi, departed_by=lo, regime=regime
    )


def persistence_null(
    legs: list[Leg],
    as_of: datetime,
    lane: LaneFilter,
    *,
    regime: str | None = None,
) -> float:
    """Null #1 — the last fully-elapsed week's count, for *either* horizon.

    D-003 worded this as "the same statistic at `as_of - 7d`, same horizon". Taken
    literally that is leaky for W2: the W2 statistic at `as_of-7d` spans
    `(as_of, as_of+7d]`, which has not happened yet at `as_of`. Amended in D-011 to
    the last *fully-observed* 7-day count, which is what a naive forecaster
    actually has in hand and is identical to the literal reading for W1.

    Conditioning on `departed_by = as_of-7d` rather than counting the week's
    arrivals outright makes no difference in practice — **no US->EU laden leg in
    the decade arrives within 7 days of departing** (observed minimum 7.04 d, 0 of
    3,200 under 7 d) — but it keeps the definition exactly like-for-like.
    """
    return float(_trailing_week(legs, as_of, lane, weeks_back=1, regime=regime))


def climatology_null(
    legs: list[Leg],
    as_of: datetime,
    lane: LaneFilter,
    *,
    weeks: int = CLIMATOLOGY_WEEKS,
    regime: str | None = None,
) -> float:
    """Null #2 — mean of the last `weeks` fully-elapsed weekly counts."""
    if weeks <= 0:
        return 0.0
    counts = [
        _trailing_week(legs, as_of, lane, weeks_back=w, regime=regime)
        for w in range(1, weeks + 1)
    ]
    return sum(counts) / len(counts)


def forecast_window(
    legs: list[Leg],
    as_of: datetime,
    lane: LaneFilter,
    *,
    u0: float,
    u1: float,
    curves: dict[tuple[str, str], ArrivalCurve] | None = None,
    regime: str | None = None,
) -> ForecastSet:
    """Per-leg arrival probabilities for the window `(as_of+u0, as_of+u1]`.

    Each open leg is scored against the curve for *its own* (origin_zone, regime),
    so a leg is never forecast using a fidelity it wasn't observed under. `curves`
    may be supplied to reuse curves across windows within one as-of date (the
    replay does this — the curve depends only on `as_of`, not the window).

    `regime` restricts the forecast population to one departure fidelity for the
    D-003 breakout; pass the same value to the truth/null calls.
    """
    curves = {} if curves is None else curves
    open_legs = open_forecast_legs(legs, as_of, lane, regime=regime)

    probs: list[float] = []
    beyond = 0
    tiers: list[str] = []
    for lg in open_legs:
        key = (lg.origin_zone, lg.regime)
        curve = curves.get(key)
        if curve is None:
            curve = build_arrival_curve(
                legs, as_of, lane, origin_zone=lg.origin_zone, regime=lg.regime
            )
            curves[key] = curve
        if curve.tier not in tiers:
            tiers.append(curve.tier)
        age = leg_age_d(lg, as_of)
        if age > curve.trusted_max_d:
            beyond += 1
        probs.append(curve.arrival_probability(age, u0, u1))

    return ForecastSet(
        probabilities=tuple(probs),
        n_legs=len(open_legs),
        n_beyond_support=beyond,
        tiers=tuple(tiers),
    )
