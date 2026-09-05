"""A2 — count GLM for weekly US loadings (MODELS.md Part A; spec locked in D-016).

A1 was mechanism with no knobs, and it lost to a 4-week moving average because its
inputs were 3-18 months stale (D-013). A2 is the first *fitted* model: every feature
is observable **today**, and the coefficients are refit every week.

    log lambda = b0 + sum_j bj * xj        count ~ NegativeBinomial(lambda, k)

`lambda` is the expected count for the window; the log link keeps it positive and
makes effects multiplicative. **NB is the headline, Poisson a nested cross-check** —
NB nests Poisson as `k -> inf`, and A1's diagnosed failure was under-dispersion,
which Poisson's forced `Var = mu` would repeat.

The five features and their pre-registered signs are in D-016 and are **binding**.
Two of them (`lag1`, `trail4`) are exactly the persistence and climatology nulls, by
design: A2 contains its own baselines, so the scorecard's real question is whether
the three *physical* features add anything on top of the autoregression.

Everything here is pure. The harness (`a2_replay.py`) does the DB work.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta

from pipeline.queues import Queue, QueueEvent, pair_queues
from pipeline.signal import (
    OPEN_VISIT_CEILING_DAYS,
    QUEUE_OPEN_CEILING_DAYS,
    LaneFilter,
)
from pipeline.visits import Visit, VisitEvent, pair_visits

# Horizons, matching A1's (lo, hi] convention (D-009) so the models are comparable.
W1: tuple[float, float] = (0.0, 7.0)
W2: tuple[float, float] = (7.0, 14.0)
WINDOW_DAYS = 7.0
CLIMATOLOGY_WEEKS = 4

# Feature order is fixed here and referenced by every sign test and report.
FEATURES = ("lag1", "trail4", "in_berth", "ballast_arrivals_1w", "queue_depth")
# Pre-registered signs (D-016). The AR block is tested jointly because `lag1` and
# `trail4` are collinear by construction; the physical features individually.
AR_BLOCK = ("lag1", "trail4")
PHYSICAL = ("in_berth", "ballast_arrivals_1w", "queue_depth")

MIN_TRAIN_WEEKS = 104  # D-016: >= 2 years before the first fit


# ----------------------------------------------------------------------
# Step 1 — the weekly design matrix (point-in-time by construction)
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class CountEvent:
    """The minimal `port_events` view the count features need.

    Carries `regime` directly (the stored generated column) rather than deriving it
    from `source`, because every A2 read is regime-segmented per SIGNALS.md §2.1.
    """

    mmsi: int
    event_type: str
    event_time: datetime
    zone: str
    laden_flag: bool | None
    regime: str


def loadings_in_window(
    events: list[CountEvent],
    lane: LaneFilter,
    *,
    lo: datetime,
    hi: datetime,
    regime: str | None = None,
) -> int:
    """US laden loadings in `(lo, hi]` — the target, and the two AR features.

    A *loading* is a laden `departed` from a US export zone. Verified in-DB: all
    6,531 NOAA departures from US export zones carry `laden_flag = TRUE` and none
    are NULL, so the laden filter is a no-op there — kept explicit anyway so the
    definition does not silently change meaning under another regime.
    """
    n = 0
    for e in events:
        if e.event_type != "departed" or e.laden_flag is not True:
            continue
        if not lane.is_export(e.zone):
            continue
        if regime is not None and e.regime != regime:
            continue
        if lo < e.event_time <= hi:
            n += 1
    return n


def open_visits_at(visits: list[Visit], as_of: datetime, lane: LaneFilter) -> int:
    """`in_berth` — vessels alongside a US export terminal at `as_of`.

    A visit counts if it moored at or before `as_of` and has not departed by then.
    Capped at `OPEN_VISIT_CEILING_DAYS` (signal.py's own guard): beyond that the
    "open" visit is a missed-departure phantom, not a ship still loading.
    """
    ceiling = timedelta(days=OPEN_VISIT_CEILING_DAYS)
    n = 0
    for v in visits:
        if v.flow_direction != "export" or not lane.is_export(v.zone):
            continue
        if v.moored_ts > as_of or as_of - v.moored_ts > ceiling:
            continue
        if v.departed_ts is None or v.departed_ts > as_of:
            n += 1
    return n


def open_queues_at(queues: list[Queue], as_of: datetime, lane: LaneFilter) -> int:
    """`queue_depth` — vessels waiting at a US export anchorage at `as_of`.

    Capped at `QUEUE_OPEN_CEILING_DAYS`. Without the ceiling this feature is junk: a
    naive "anchorage_entry with no later moored" count read 30 against 1 vessel
    actually in berth at 2023-01-02, stale entries accumulating (D-016).
    """
    ceiling = timedelta(days=QUEUE_OPEN_CEILING_DAYS)
    n = 0
    for q in queues:
        if q.flow_direction != "export" or not lane.is_export(q.zone):
            continue
        if q.entry_ts > as_of or as_of - q.entry_ts > ceiling:
            continue
        if q.moored_ts is None or q.moored_ts > as_of:
            n += 1
    return n


def ballast_arrivals(
    events: list[CountEvent],
    lane: LaneFilter,
    *,
    lo: datetime,
    hi: datetime,
    regime: str | None = None,
) -> int:
    """`ballast_arrivals_1w` — empty carriers reaching US export zones in `(lo, hi]`.

    The feedstock: a ship must arrive empty before it can leave full. Deliberately
    the **US-side arrival**, not the EU-side ballast departure — the latter is
    GFW-only (NOAA has exactly 0) and would put a capture-drifting covariate in a
    NOAA-target model (D-016).
    """
    n = 0
    for e in events:
        if e.event_type != "zone_entry" or e.laden_flag is not False:
            continue
        if not lane.is_export(e.zone):
            continue
        if regime is not None and e.regime != regime:
            continue
        if lo < e.event_time <= hi:
            n += 1
    return n


@dataclass(frozen=True)
class Row:
    """One weekly observation: features at `as_of`, target over the horizon."""

    as_of: datetime
    features: tuple[float, ...]  # ordered as FEATURES
    target: int

    def feature(self, name: str) -> float:
        return self.features[FEATURES.index(name)]


def build_row(
    as_of: datetime,
    feature_events: list[CountEvent],
    target_events: list[CountEvent],
    visits: list[Visit],
    queues: list[Queue],
    lane: LaneFilter,
    *,
    u0: float,
    u1: float,
    regime: str | None = None,
) -> Row:
    """Features from data at/before `as_of`; target from the future window.

    The two event streams are **separate parameters on purpose**. They are not
    interchangeable and the asymmetry is the whole point:

    - `feature_events` (with `visits` / `queues`) must be sliced to `<= as_of` —
      that is what makes the features point-in-time.
    - `target_events` must be the **full hindsight stream**, because the target
      window `(as_of+u0, as_of+u1]` lies entirely in the future of `as_of`. Pass
      a sliced stream here and every target is silently 0.

    That is not hypothetical: the first harness did exactly that, and produced a
    scorecard of all-zero targets, all-zero coefficients and 100 % coverage. Using
    the label at fit time is prevented by the purge in `training_rows`, not by
    withholding it here.
    """
    week = timedelta(days=WINDOW_DAYS)
    lag1 = loadings_in_window(
        feature_events, lane, lo=as_of - week, hi=as_of, regime=regime
    )
    trail = [
        loadings_in_window(
            feature_events,
            lane,
            lo=as_of - week * (w + 1),
            hi=as_of - week * w,
            regime=regime,
        )
        for w in range(CLIMATOLOGY_WEEKS)
    ]
    feats = (
        float(lag1),
        sum(trail) / len(trail),
        float(open_visits_at(visits, as_of, lane)),
        float(
            ballast_arrivals(
                feature_events, lane, lo=as_of - week, hi=as_of, regime=regime
            )
        ),
        float(open_queues_at(queues, as_of, lane)),
    )
    target = loadings_in_window(
        target_events,
        lane,
        lo=as_of + timedelta(days=u0),
        hi=as_of + timedelta(days=u1),
        regime=regime,
    )
    return Row(as_of=as_of, features=feats, target=target)


def training_rows(rows: list[Row], fit_at: datetime, *, u1: float) -> list[Row]:
    """Purged training set (Part C #1): only rows whose target window has closed.

    A row at `as_of` is admissible at `fit_at` only once `as_of + u1 <= fit_at`, so
    its label was actually observable. No further embargo is needed — the features
    are point-in-time by construction, so nothing else overlaps the test point.
    """
    return [r for r in rows if r.as_of + timedelta(days=u1) <= fit_at]


# ----------------------------------------------------------------------
# Step 2 — fitting (Poisson / NB MLE)
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class Standardiser:
    """Column means/sds from the **training rows only** (never the test point).

    Purely for optimiser conditioning. It is a positive linear rescaling, so it
    preserves coefficient signs and the D-016 sign tests are unaffected.
    """

    means: tuple[float, ...]
    sds: tuple[float, ...]

    @classmethod
    def fit(cls, rows: list[Row]) -> Standardiser:
        n = len(rows)
        k = len(FEATURES)
        means = [sum(r.features[j] for r in rows) / n for j in range(k)]
        sds = []
        for j in range(k):
            var = sum((r.features[j] - means[j]) ** 2 for r in rows) / max(1, n - 1)
            sds.append(math.sqrt(var) if var > 1e-12 else 1.0)
        return cls(means=tuple(means), sds=tuple(sds))

    def apply(self, features: tuple[float, ...]) -> list[float]:
        return [(x - m) / s for x, m, s in zip(features, self.means, self.sds)]


@dataclass(frozen=True)
class GLMFit:
    """A fitted count GLM. `beta[0]` is the intercept; the rest follow FEATURES."""

    beta: tuple[float, ...]
    k: float | None  # NB dispersion; None for Poisson
    family: str  # 'poisson' | 'nb'
    std: Standardiser
    n_train: int
    converged: bool

    def predict_mu(self, features: tuple[float, ...]) -> float:
        x = self.std.apply(features)
        eta = self.beta[0] + sum(b * xi for b, xi in zip(self.beta[1:], x))
        return math.exp(min(eta, 20.0))  # guard the exponential against overflow

    def coefficient(self, name: str) -> float:
        return self.beta[1 + FEATURES.index(name)]

    @property
    def ar_block_sum(self) -> float:
        """Joint AR effect — the collinearity-aware sign test from D-016."""
        return sum(self.coefficient(n) for n in AR_BLOCK)


def _design(rows: list[Row], std: Standardiser):
    X = [[1.0, *std.apply(r.features)] for r in rows]
    y = [r.target for r in rows]
    return X, y


ETA_CAP = 20.0  # guards exp() overflow; mu = e^20 is far beyond any weekly count
# Bounds on the NB dispersion. K_MAX is "effectively Poisson" — at weekly-count
# scale NB(k=1e6) and Poisson are indistinguishable, so capping there costs nothing
# and keeps the optimisation well-posed (see `fit_glm`).
K_MIN, K_MAX = 1e-3, 1e6


def _eta(beta, xi) -> float:
    return min(sum(b * v for b, v in zip(beta, xi)), ETA_CAP)


def _neg_loglik_poisson(beta, X, y) -> float:
    total = 0.0
    for xi, yi in zip(X, y):
        eta = _eta(beta, xi)
        total += math.exp(eta) - yi * eta
    return total


def _grad_poisson(beta, X, y):
    """d(-ll)/dbeta = X' (mu - y). Analytic gradients matter here: without them
    BFGS stalls on 'precision loss' and reports non-convergence even when the
    estimate is fine, which would make the `converged` flag useless as a guard."""
    g = [0.0] * len(beta)
    for xi, yi in zip(X, y):
        r = math.exp(_eta(beta, xi)) - yi
        for j, v in enumerate(xi):
            g[j] += r * v
    return g


def _neg_loglik_nb(params, X, y) -> float:
    """NB2 negative log-likelihood, parameterised by `log k` to keep k > 0."""
    beta, k = params[:-1], math.exp(min(params[-1], 15.0))
    total = 0.0
    for xi, yi in zip(X, y):
        mu = math.exp(_eta(beta, xi))
        total -= (
            math.lgamma(yi + k)
            - math.lgamma(k)
            - math.lgamma(yi + 1.0)
            + k * math.log(k / (k + mu))
            + yi * math.log(mu / (k + mu))
        )
    return total


def _grad_nb(params, X, y):
    """Analytic gradient of the NB2 negative log-likelihood.

        d(ll)/dbeta_j = sum  k (y - mu) x_j / (k + mu)
        d(ll)/dlog k  = k * sum [ psi(y+k) - psi(k) + log(k/(k+mu)) + (mu-y)/(k+mu) ]
    """
    from scipy.special import digamma

    beta, theta = params[:-1], min(params[-1], 15.0)
    k = math.exp(theta)
    g = [0.0] * len(params)
    for xi, yi in zip(X, y):
        mu = math.exp(_eta(beta, xi))
        w = k * (mu - yi) / (k + mu)  # negated -> d(-ll)/deta
        for j, v in enumerate(xi):
            g[j] += w * v
        g[-1] -= k * (
            digamma(yi + k)
            - digamma(k)
            + math.log(k / (k + mu))
            + (mu - yi) / (k + mu)
        )
    return g


def fit_glm(rows: list[Row], family: str = "nb") -> GLMFit:
    """Fit by direct MLE (`scipy.optimize`, analytic gradients). 'nb' | 'poisson'.

    A feature that is constant across the training window is centred to all-zeros
    by the standardiser, so its coefficient is unidentified: the gradient in that
    direction is exactly 0 and it stays at its 0 start. That is the correct,
    harmless outcome (the term contributes nothing to `mu`) rather than a failure.
    """
    from scipy.optimize import minimize

    std = Standardiser.fit(rows)
    X, y = _design(rows, std)
    mean_y = max(sum(y) / len(y), 1e-6)
    start = [math.log(mean_y)] + [0.0] * len(FEATURES)

    if family == "poisson":
        res = minimize(
            _neg_loglik_poisson, start, args=(X, y), jac=_grad_poisson,
            method="L-BFGS-B",
        )
        return GLMFit(
            beta=tuple(float(b) for b in res.x), k=None, family="poisson",
            std=std, n_train=len(rows), converged=bool(res.success),
        )

    # `log k` is **bounded**. In the Poisson limit (data not over-dispersed) the
    # NB likelihood is asymptotically flat in `k`, so an unbounded optimiser
    # wanders and reports non-convergence even though the fit is fine — which
    # would make `converged` useless as a diagnostic. Bounding at K_MAX
    # ("effectively Poisson") makes the problem well-posed without changing any
    # answer that matters: NB with k = 1e6 and Poisson are indistinguishable at
    # weekly-count scale.
    bounds = [(None, None)] * len(start) + [
        (math.log(K_MIN), math.log(K_MAX))
    ]
    res = minimize(
        _neg_loglik_nb, [*start, math.log(2.0)], args=(X, y), jac=_grad_nb,
        method="L-BFGS-B", bounds=bounds,
    )
    return GLMFit(
        beta=tuple(float(b) for b in res.x[:-1]),
        k=float(math.exp(res.x[-1])), family="nb",
        std=std, n_train=len(rows), converged=bool(res.success),
    )


# ----------------------------------------------------------------------
# Predictive distributions (reusing A1's scoring stack shape)
# ----------------------------------------------------------------------


PMF_TAIL_TOL = 1e-12  # unaccounted upper-tail mass permitted before truncation
PMF_MAX_SUPPORT = 100_000  # hard stop; weekly counts are ~tens


def _pmf_from(mu: float, k: float | None) -> tuple[float, ...]:
    """PMF for Poisson(mu) or NB(mu, k), by stable log-space recursion.

    The support **grows adaptively** until the unaccounted tail is below
    `PMF_TAIL_TOL`, rather than stopping at a fixed multiple of the sd. NB with
    small `k` is heavy-tailed — at mu=6, k=1.5 a "mu + 12 sd" cut still truncated
    1.6e-7 of the mass, which is enough to bias CRPS and the upper interval bound.
    """
    out: list[float] = []
    cum = 0.0
    if k is None:
        log_p = -mu
    else:
        log_p = k * math.log(k / (k + mu))
    j = 0
    while True:
        if j:
            if k is None:
                log_p += math.log(mu) - math.log(j)
            else:
                log_p += math.log((k + j - 1) / j) + math.log(mu / (k + mu))
        p = math.exp(log_p)
        out.append(p)
        cum += p
        j += 1
        if (cum >= 1.0 - PMF_TAIL_TOL and j > mu) or j > PMF_MAX_SUPPORT:
            break
    return tuple(out)


@dataclass(frozen=True)
class CountPredictive:
    """A2's forecast distribution — same reads as A1's `Predictive` (D-010)."""

    pmf: tuple[float, ...]
    mu: float

    @classmethod
    def build(cls, mu: float, k: float | None) -> CountPredictive:
        return cls(pmf=_pmf_from(mu, k), mu=mu)

    @property
    def mean(self) -> float:
        return sum(j * m for j, m in enumerate(self.pmf))

    def cdf(self, j: int) -> float:
        if j < 0:
            return 0.0
        return sum(self.pmf[: min(j, len(self.pmf) - 1) + 1])

    def quantile(self, alpha: float) -> int:
        acc = 0.0
        for j, m in enumerate(self.pmf):
            acc += m
            if acc >= alpha - 1e-12:
                return j
        return len(self.pmf) - 1

    def interval(self, coverage: float) -> tuple[int, int]:
        tail = (1.0 - coverage) / 2.0
        return self.quantile(tail), self.quantile(1.0 - tail)

    def crps(self, observed: int) -> float:
        upper = max(len(self.pmf) - 1, observed)
        total, cum = 0.0, 0.0
        for j in range(upper + 1):
            if j < len(self.pmf):
                cum += self.pmf[j]
            total += (cum - (1.0 if observed <= j else 0.0)) ** 2
        return total

    def pit(self, observed: int, u: float) -> float:
        below = self.cdf(observed - 1)
        mass = self.pmf[observed] if 0 <= observed < len(self.pmf) else 0.0
        return below + u * mass


# ----------------------------------------------------------------------
# Slicing helpers for the harness
# ----------------------------------------------------------------------


def visits_and_queues(
    visit_events: list[VisitEvent],
    queue_events: list[QueueEvent],
    flow_directions: dict[int, str],
):
    """Pair once over a pre-sliced event stream (the pure pairers, reused)."""
    return (
        pair_visits(visit_events, flow_directions=flow_directions),
        pair_queues(queue_events, flow_directions=flow_directions),
    )
