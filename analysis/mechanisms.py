"""H2/H3/H4 — the three mechanism tests (spec locked in D-031).

D-029 returned a clean linear null: no tanker signal predicts the HH-TTF spread
net of persistence, weather, storage and oil. Three specific mechanisms could
hide a real effect from that scan, and each is a *feature or a split*, not a
fancier model class — which is also why each costs only one test:

  H2  the EU **share** of cargo at sea, not the EU **level**. Two collinear
      levels with offsetting effects each look like noise one at a time; only
      their ratio carries "how much of the at-sea stock is Europe-bound".

  H3  an **interaction with EU storage**. Extra cargo landing at 90 % full does
      little; the same cargo at 30 % is highly price-relevant. A linear fit
      averages the two worlds and finds nothing — which is what D-029 reported.

  H4  an **observable tightness split**, the cheap form of B4's regime idea. If
      splitting on a variable already in the panel finds nothing, a fitted HMM
      with latent states and extra free parameters was not going to either.

Pure module — no DB, no I/O. `mechanisms_replay.py` supplies the data.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

# D-031: Bonferroni over the three primary tests, alpha = 0.05/3 = 0.0167.
BONFERRONI_T = 2.394
PRIMARY_HORIZON = 4  # weeks; chosen on the 14-18 d voyage mechanism, not on D-029

# D-031's coverage-confound gate for H2: if a linear time trend explains more of
# eu_share than this, the feature is judged to be tracking coverage rather than
# cargo routing, and H2 is VOID rather than a result in either direction.
MAX_TREND_R2 = 0.5


def eu_share(eu: np.ndarray, unknown: np.ndarray) -> np.ndarray:
    """Fraction of at-sea gas banded to a known EU destination.

    Days where nothing is at sea have no share to speak of; they return NaN so
    the caller drops them rather than silently reading 0 as "none bound for EU".
    """
    total = eu + unknown
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(total > 0, eu / total, np.nan)


def trend_r2(values: np.ndarray) -> float:
    """R^2 of a linear time trend — H2's confound gate.

    A share that is mostly a decade-long ramp is far more likely to be recording
    our own improving destination-resolution than any change in where cargo goes.
    """
    t = np.arange(len(values), dtype=float)
    t = t - t.mean()
    centred = values - values.mean()
    denom = float(centred @ centred)
    if denom <= 0:
        return 0.0
    slope = float(t @ centred) / float(t @ t)
    resid = centred - slope * t
    return 1.0 - float(resid @ resid) / denom


def interaction(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Centred product. Centring first makes the interaction coefficient readable
    as "how the effect of `a` changes with `b`" rather than being entangled with
    the main effects through the scale of the raw product."""
    return (a - a.mean()) * (b - b.mean())


def seasonal_norm(
    values: np.ndarray, day_of_year: np.ndarray, fit_mask: np.ndarray
) -> np.ndarray:
    """Day-of-year median of `values`, estimated on `fit_mask` rows only.

    EU storage is overwhelmingly seasonal — it troughs near 38 % in March and
    peaks near 90 % in October — so "low storage" only means "tight" relative to
    the time of year. Estimating the norm on the discovery window alone keeps the
    split rule leakage-safe when it is later applied to the holdout (D-031).
    """
    norm = np.empty(len(values))
    fit_doy = day_of_year[fit_mask]
    fit_val = values[fit_mask]
    # A +/- 15 day window: a bare day-of-year median would be estimated off ~7
    # observations in a 7-year discovery window, which is too thin to be stable.
    for i, doy in enumerate(day_of_year):
        delta = np.abs(((fit_doy - doy + 182) % 365) - 182)
        window = fit_val[delta <= 15]
        norm[i] = np.median(window) if len(window) else np.median(fit_val)
    return norm


def tightness_mask(
    storage: np.ndarray, day_of_year: np.ndarray, fit_mask: np.ndarray
) -> np.ndarray:
    """True where EU storage sits below its seasonal norm — the `tight` regime."""
    return storage < seasonal_norm(storage, day_of_year, fit_mask)


@dataclass(frozen=True)
class HypothesisResult:
    """One pre-registered hypothesis, graded against all four D-031 conditions."""

    name: str
    description: str
    expected_sign: int
    beta: float
    t_stat: float
    n_discovery: int
    year_consistency: float
    partial_r2: float
    holdout_beta: float | None
    n_holdout: int
    void_reason: str | None = None
    t_critical: float = BONFERRONI_T

    @property
    def significant(self) -> bool:
        return abs(self.t_stat) > self.t_critical

    @property
    def sign_ok(self) -> bool:
        return np.sign(self.beta) == self.expected_sign

    @property
    def years_ok(self) -> bool:
        return self.year_consistency >= 2 / 3

    @property
    def holdout_ok(self) -> bool:
        """Sign must replicate out of sample; no significance demanded (n ~ 85)."""
        if self.holdout_beta is None:
            return False
        return np.sign(self.holdout_beta) == np.sign(self.beta)

    def verdict(self) -> str:
        if self.void_reason:
            return f"VOID — {self.void_reason}"
        if not self.significant:
            return f"not significant (|t| {abs(self.t_stat):.2f} < {self.t_critical})"
        if not self.sign_ok:
            return "SIGN FALSIFIED"
        if not self.years_ok:
            return f"unstable across years ({self.year_consistency:.0%})"
        if not self.holdout_ok:
            return "fails to replicate on the holdout"
        return "SUPPORTED"
