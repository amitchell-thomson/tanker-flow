"""Frisch-Waugh-Lovell partial-effect harness (spec locked in D-028).

The question Part B exists to answer is not "does a tanker signal correlate with
the spread" — it does, because both have a winter cycle: gas-in-transit runs
about 1.48x higher in winter and Europe burns more gas when it is cold. Regress
one on the other and you rediscover winter.

The question is whether a signal carries information **net of** persistence,
weather, storage and oil. FWL answers it in three steps:

    1. y~ = residual of the spread after regressing it on the controls Z
    2. T~ = residual of the tanker signal after regressing it on the same Z
    3. regress y~ on T~  ->  the partial effect

By the FWL theorem that coefficient is *identically* the coefficient on T in the
full regression of y on [Z, T]. That is not a convenience — it is this module's
correctness test (`test_fwl.py`), and it means the harness cannot quietly compute
something other than what it claims.

What FWL adds over reading the multiple regression is interpretability: `T~` is
the part of the signal that weather and storage do **not** explain, so a non-zero
slope is edge attributable to the tanker data rather than to the season.

Inference is Newey-West throughout: the h-week targets overlap, so an OLS
standard error would be roughly sqrt(h) too small and would turn noise into
significance.

Pure module — no DB, no I/O.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from analysis.b0 import add_intercept, newey_west_lag, ols

T_CRITICAL = 1.96  # D-028's |t| bar
MIN_YEAR_OBS = 20  # a year with fewer scored weeks is not evidence either way


def residualise(target: np.ndarray, controls: np.ndarray) -> np.ndarray:
    """Return the part of `target` the controls cannot explain.

    `pinv` rather than `solve`: the control block is collinear by construction
    (HDD and the winter dummy overlap heavily), and a minimum-norm projection is
    the right degradation. The residual is unaffected by that choice — the
    projection onto the column space of Z is unique even when the basis is not.
    """
    design = add_intercept(controls)
    beta = np.linalg.pinv(design) @ target
    return target - design @ beta


@dataclass(frozen=True)
class PartialEffect:
    """One signal's effect on the spread, net of the controls."""

    feature: str
    beta: float  # $/MMBtu of h-week spread change per unit of signal
    se: float  # Newey-West
    t_stat: float
    n_obs: int
    expected_sign: int  # +1 / -1 from D-028; 0 = no prior registered
    year_signs: dict[int, int]  # sign of the per-year partial effect
    partial_r2: float  # variance of y~ explained by T~

    @property
    def significant(self) -> bool:
        return abs(self.t_stat) > T_CRITICAL

    @property
    def sign_matches(self) -> bool:
        """True when no prior was registered (nothing to violate) or it holds."""
        if self.expected_sign == 0:
            return True
        return np.sign(self.beta) == self.expected_sign

    @property
    def year_consistency(self) -> float:
        """Share of years whose partial effect carries the full-sample sign.

        The guard against a result driven by one extraordinary year: 2022 alone
        spans -90 to -25 $/MMBtu and can carry a pooled regression on its own.
        """
        if not self.year_signs:
            return 0.0
        overall = np.sign(self.beta)
        agree = sum(1 for s in self.year_signs.values() if s == overall)
        return agree / len(self.year_signs)

    def verdict(self) -> str:
        """D-028's three-part bar: |t| > 1.96, sign as pre-registered, and the
        sign holding in at least two thirds of individual years."""
        if not self.significant:
            return "not significant"
        if not self.sign_matches:
            return "SIGN FALSIFIED"
        if self.year_consistency < 2 / 3:
            return f"unstable across years ({self.year_consistency:.0%})"
        return "CARRIES INFORMATION"


def partial_effect(
    feature: str,
    y: np.ndarray,
    signal: np.ndarray,
    controls: np.ndarray,
    years: np.ndarray,
    *,
    horizon: int,
    expected_sign: int,
) -> PartialEffect:
    """Run FWL for one signal and grade it against the pre-registered bar."""
    hac_lag = newey_west_lag(horizon)
    y_resid = residualise(y, controls)
    t_resid = residualise(signal, controls)

    fit = ols(t_resid.reshape(-1, 1), y_resid, (feature,), hac_lag=hac_lag)
    beta, se, t_stat = fit.coefficient(feature)

    # Per-year refit on the same residuals: the controls are partialled out on
    # the full sample (so every year is judged against one common control fit),
    # and only the second-stage slope is re-estimated within the year.
    year_signs: dict[int, int] = {}
    for year in sorted(set(years.tolist())):
        mask = years == year
        if mask.sum() < MIN_YEAR_OBS:
            continue
        yr_t, yr_y = t_resid[mask], y_resid[mask]
        denom = float(yr_t @ yr_t)
        if denom > 0:
            year_signs[int(year)] = int(np.sign(float(yr_t @ yr_y) / denom))

    total = float(y_resid @ y_resid)
    explained = float(beta**2 * (t_resid @ t_resid))
    partial_r2 = explained / total if total > 0 else 0.0

    return PartialEffect(
        feature=feature,
        beta=beta,
        se=se,
        t_stat=t_stat,
        n_obs=len(y),
        expected_sign=expected_sign,
        year_signs=year_signs,
        partial_r2=partial_r2,
    )
