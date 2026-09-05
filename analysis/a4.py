"""A4 — Kalman state-space nowcast of the US export rate (spec locked in D-022).

Closes Part A by testing the prediction made in D-018, before A4 existed: that a
local-level state-space model is the right answer to A1's staleness and A2's
over-extrapolation — *track* the level rather than extrapolating it multiplicatively.

    x_t = x_{t-1} + w_t,   w ~ N(0, q)     latent weekly export rate
    y_t = x_t     + v_t,   v ~ N(0, r)     observed weekly loading count

`x_t` is the hidden "true current rate"; `y_t` is what we counted. The filter's whole
behaviour is set by the **signal-to-noise ratio q/r**: large `q` means the rate really
moves and the filter should chase the data; large `r` means the counts are noisy and
it should smooth. Both are fitted by exact Kalman MLE (two parameters).

**Stated up front (D-022):** a steady-state local-level filter *is* an
exponentially-weighted moving average. So A4 asks whether an optimally-tuned EWMA
beats a fixed 4-week SMA. A fixed SMA(4) lags a trend by ~1.5 weeks and an EWMA lags
less, so there is a real edge available — but a tie is a good outcome too, because it
would establish the naive mean is near-optimal and close the question.

The **local linear trend** variant (pre-registered secondary) adds a slope state and
does extrapolate; it nests local level as `q_slope -> 0`.

Pure module — the harness (`a4_replay.py`) does the DB work.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

# Diffuse initialisation: start uninformative about the level.
P0_DIFFUSE = 1e4
MIN_VAR = 1e-8  # keep variances strictly positive under optimisation
CLIMATOLOGY_WEEKS = 4
MIN_TRAIN_WEEKS = 104  # D-022: same as A2

_SQRT_PI = math.sqrt(math.pi)


# ----------------------------------------------------------------------
# Local level
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class LocalLevelFit:
    q: float  # state (level) variance
    r: float  # observation variance
    level: float  # filtered level at the end of the series
    p: float  # filtered level variance at the end
    loglik: float
    n_obs: int
    converged: bool

    @property
    def snr(self) -> float:
        """`q/r` — the signal-to-noise ratio that sets the smoothing."""
        return self.q / self.r if self.r > 0 else float("inf")

    @property
    def ewma_alpha(self) -> float:
        """Steady-state Kalman gain = the equivalent EWMA smoothing constant.

        For a local-level model, `alpha = (sqrt(s^2 + 4s) - s) / 2` with `s = q/r`.
        This is the interpretable output: alpha near 1 means "trust the last
        observation" (persistence); alpha near 0 means "smooth heavily"
        (climatology). Where A4 lands between the two nulls is exactly this number.
        """
        s = self.snr
        if not math.isfinite(s):
            return 1.0
        return (math.sqrt(s * s + 4.0 * s) - s) / 2.0

    def forecast(self, h: int) -> tuple[float, float]:
        """`h`-step-ahead predictive mean and variance."""
        return self.level, self.p + h * self.q + self.r


def local_level_filter(y: list[float], q: float, r: float):
    """Run the filter; return (loglik, final level, final variance).

    Log-likelihood via the prediction-error decomposition — the standard exact
    Kalman likelihood, which is what makes a 2-parameter MLE cheap and stable.
    """
    x, p = y[0], P0_DIFFUSE
    loglik = 0.0
    for i, obs in enumerate(y):
        # predict
        x_pred, p_pred = x, p + q
        # innovation
        e = obs - x_pred
        s = p_pred + r
        if i > 0:  # skip the diffuse first observation in the likelihood
            loglik -= 0.5 * (math.log(2.0 * math.pi * s) + e * e / s)
        # update
        k = p_pred / s
        x = x_pred + k * e
        p = (1.0 - k) * p_pred
    return loglik, x, p


def fit_local_level(y: list[float]) -> LocalLevelFit:
    """Fit `(q, r)` by MLE. Parameterised as logs to keep both positive."""
    from scipy.optimize import minimize

    var_y = max(_variance(y), 1e-6)

    def nll(params):
        q = math.exp(min(params[0], 20.0)) + MIN_VAR
        r = math.exp(min(params[1], 20.0)) + MIN_VAR
        ll, _, _ = local_level_filter(y, q, r)
        return -ll

    start = [math.log(var_y * 0.1), math.log(var_y * 0.9)]
    res = minimize(nll, start, method="Nelder-Mead",
                   options={"maxiter": 2000, "xatol": 1e-6, "fatol": 1e-6})
    q = math.exp(min(res.x[0], 20.0)) + MIN_VAR
    r = math.exp(min(res.x[1], 20.0)) + MIN_VAR
    ll, level, p = local_level_filter(y, q, r)
    return LocalLevelFit(
        q=q, r=r, level=level, p=p, loglik=ll, n_obs=len(y),
        converged=bool(res.success),
    )


# ----------------------------------------------------------------------
# Local linear trend (pre-registered secondary)
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class LocalTrendFit:
    q_level: float
    q_slope: float
    r: float
    level: float
    slope: float
    p: tuple[tuple[float, float], tuple[float, float]]
    loglik: float
    n_obs: int
    converged: bool

    def forecast(self, h: int) -> tuple[float, float]:
        """`h`-step forecast — the level *plus* `h` steps of the fitted slope.

        This is the variant that extrapolates. It nests local level at
        `q_slope -> 0`, so if the data wants no trend the fit can say so.
        """
        mean = self.level + h * self.slope
        p11, p12, p22 = self.p[0][0], self.p[0][1], self.p[1][1]
        var = (
            p11 + 2 * h * p12 + h * h * p22
            + h * self.q_level
            + (h * (h + 1) * (2 * h + 1) / 6.0) * self.q_slope
            + self.r
        )
        return mean, max(var, MIN_VAR)


def local_trend_filter(y: list[float], q_level: float, q_slope: float, r: float):
    """Local linear trend filter. State `[level, slope]`, 2x2 covariance."""
    level, slope = y[0], 0.0
    p11, p12, p22 = P0_DIFFUSE, 0.0, P0_DIFFUSE
    loglik = 0.0
    for i, obs in enumerate(y):
        # predict: level += slope; slope unchanged
        lp = level + slope
        sp = slope
        n11 = p11 + 2 * p12 + p22 + q_level
        n12 = p12 + p22
        n22 = p22 + q_slope
        # innovation on the level
        e = obs - lp
        s = n11 + r
        if i > 0:
            loglik -= 0.5 * (math.log(2.0 * math.pi * s) + e * e / s)
        k1, k2 = n11 / s, n12 / s
        level = lp + k1 * e
        slope = sp + k2 * e
        p11 = n11 - k1 * n11
        p12 = n12 - k1 * n12
        p22 = n22 - k2 * n12
    return loglik, level, slope, ((p11, p12), (p12, p22))


def fit_local_trend(y: list[float]) -> LocalTrendFit:
    from scipy.optimize import minimize

    var_y = max(_variance(y), 1e-6)

    def nll(params):
        ql = math.exp(min(params[0], 20.0)) + MIN_VAR
        qs = math.exp(min(params[1], 20.0)) + MIN_VAR
        r = math.exp(min(params[2], 20.0)) + MIN_VAR
        ll, _, _, _ = local_trend_filter(y, ql, qs, r)
        return -ll

    start = [
        math.log(var_y * 0.1), math.log(var_y * 0.01), math.log(var_y * 0.9)
    ]
    res = minimize(nll, start, method="Nelder-Mead",
                   options={"maxiter": 3000, "xatol": 1e-6, "fatol": 1e-6})
    ql = math.exp(min(res.x[0], 20.0)) + MIN_VAR
    qs = math.exp(min(res.x[1], 20.0)) + MIN_VAR
    r = math.exp(min(res.x[2], 20.0)) + MIN_VAR
    ll, level, slope, p = local_trend_filter(y, ql, qs, r)
    return LocalTrendFit(
        q_level=ql, q_slope=qs, r=r, level=level, slope=slope, p=p,
        loglik=ll, n_obs=len(y), converged=bool(res.success),
    )


def _variance(y: list[float]) -> float:
    n = len(y)
    if n < 2:
        return 1.0
    m = sum(y) / n
    return sum((v - m) ** 2 for v in y) / (n - 1)


# ----------------------------------------------------------------------
# Gaussian predictive — the scoring reads (D-022)
# ----------------------------------------------------------------------


def _phi(z: float) -> float:
    return math.exp(-0.5 * z * z) / math.sqrt(2.0 * math.pi)


def _Phi(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


@dataclass(frozen=True)
class GaussianPredictive:
    """Continuous Gaussian forecast, with the same reads A1/A2 reported.

    A4's observation model is linear-Gaussian (D-022), so unlike A1/A2 the
    predictive is continuous: CRPS uses the closed form and coverage/PIT use
    Gaussian quantiles. All three stay in observable units, so the numbers remain
    comparable to the discrete models'.
    """

    mean: float
    var: float

    @property
    def sd(self) -> float:
        return math.sqrt(max(self.var, MIN_VAR))

    def interval(self, coverage: float) -> tuple[float, float]:
        z = _inv_norm(0.5 + coverage / 2.0)
        return self.mean - z * self.sd, self.mean + z * self.sd

    def crps(self, observed: float) -> float:
        """Closed-form Gaussian CRPS (Gneiting & Raftery 2007).

        `sigma [ z(2*Phi(z) - 1) + 2*phi(z) - 1/sqrt(pi) ]`, `z = (y - mu)/sigma`.
        """
        s = self.sd
        z = (observed - self.mean) / s
        return s * (z * (2.0 * _Phi(z) - 1.0) + 2.0 * _phi(z) - 1.0 / _SQRT_PI)

    def pit(self, observed: float) -> float:
        """Continuous PIT — no randomisation needed, unlike the discrete case."""
        return _Phi((observed - self.mean) / self.sd)


def _inv_norm(p: float) -> float:
    """Inverse standard normal CDF (Acklam's rational approximation)."""
    if p <= 0.0:
        return -math.inf
    if p >= 1.0:
        return math.inf
    a = [-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02,
         1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00]
    b = [-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02,
         6.680131188771972e01, -1.328068155288572e01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00,
         -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00,
         3.754408661907416e00]
    plow, phigh = 0.02425, 1 - 0.02425
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q
                + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q
                 + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    q = p - 0.5
    rr = q * q
    return (((((a[0] * rr + a[1]) * rr + a[2]) * rr + a[3]) * rr + a[4]) * rr
            + a[5]) * q / (((((b[0] * rr + b[1]) * rr + b[2]) * rr + b[3]) * rr
                            + b[4]) * rr + 1)


# ----------------------------------------------------------------------
# Nulls (identical definitions to A2 / D-011)
# ----------------------------------------------------------------------


def persistence(y: list[float]) -> float:
    """Last observed week — same statistic A2 carried as its `lag1` feature."""
    return y[-1]


def climatology(y: list[float], weeks: int = CLIMATOLOGY_WEEKS) -> float:
    """Mean of the last `weeks` observed weeks — A2's `trail4`."""
    tail = y[-weeks:]
    return sum(tail) / len(tail)
