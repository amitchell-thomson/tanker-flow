"""B0 — the Part B null: AR(1)+controls, with HAC inference (spec locked in D-028).

This module is the *bar*, not the thesis. Before any claim that tanker positions
carry information about the HH-TTF spread, there has to be a defensible number
for how well the spread is predicted **without** them — from its own persistence,
the weather, storage and oil. That number is what M3 will have to beat.

The ladder (D-028), each rung the null for the next:

    M0  spread(t+h) = spread(t)                     no-change / random walk
    M1  spread(t+h) = a + rho*spread(t)             AR(1)
    M2  M1 + weather + storage + Brent + winter     <- "AR(1)+controls"

Three properties of this data force the design, and skipping any one of them
manufactures skill that is not there:

**Overlapping windows.** On a weekly grid with h = 4, consecutive observations
share three weeks of their target window, so residuals are serially correlated by
construction. An OLS standard error is then roughly sqrt(h) too small. Every
standard error here is **Newey-West HAC**; `ols` refuses to report a naive one.

**Purge + embargo.** A training row whose target window straddles the test date
has already seen the future. `walk_forward` keeps a training row only when its
target is fully realised strictly before the test date, plus an embargo.

**Persistence.** The spread is highly autocorrelated, so predicting its *level*
scores well while learning nothing. The scored target is the h-week **change**;
levels are fitted only because AR(1) is naturally written that way, and the
change is recovered from the prediction.

Pure module — no DB, no I/O. `b0_replay.py` supplies the data.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

MIN_TRAIN_WEEKS = 104  # D-028; the A2/A4 floor, so Part A and B share it
EMBARGO_WEEKS = 1
Z_80 = 1.2815515655446004  # two-sided 80% normal quantile


# ----------------------------------------------------------------------
# OLS with HAC (Newey-West) inference
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class OlsFit:
    """A fitted linear model. Standard errors are always HAC — see module docs."""

    beta: np.ndarray  # (k,) coefficients, including the intercept at index 0
    se: np.ndarray  # (k,) Newey-West standard errors
    names: tuple[str, ...]
    n_obs: int
    sigma2: float  # residual variance, for predictive intervals
    hac_lag: int

    def t_stats(self) -> np.ndarray:
        with np.errstate(divide="ignore", invalid="ignore"):
            return np.where(self.se > 0, self.beta / self.se, 0.0)

    def coefficient(self, name: str) -> tuple[float, float, float]:
        """(estimate, HAC standard error, HAC t) for one named regressor."""
        i = self.names.index(name)
        return float(self.beta[i]), float(self.se[i]), float(self.t_stats()[i])

    def predict(self, x: np.ndarray) -> np.ndarray:
        return add_intercept(x) @ self.beta


def add_intercept(x: np.ndarray) -> np.ndarray:
    x = np.atleast_2d(x)
    return np.hstack([np.ones((x.shape[0], 1)), x])


def newey_west_lag(horizon_weeks: int) -> int:
    """D-028 fixes the HAC lag at h + 1 weeks.

    With h-step overlapping windows the residual MA order is h - 1, so h + 1 is a
    deliberately conservative cover. Fixed in advance rather than data-chosen: a
    lag picked after seeing the residuals is a researcher degree of freedom.
    """
    return horizon_weeks + 1


def _hac_cov(x: np.ndarray, resid: np.ndarray, lag: int) -> np.ndarray:
    """Newey-West sandwich covariance with Bartlett weights.

    S = sum_l w_l * (Gamma_l + Gamma_l'), w_l = 1 - l/(lag+1); the Bartlett taper
    is what keeps S positive semi-definite.
    """
    n, k = x.shape
    xtx_inv = np.linalg.pinv(x.T @ x)
    u = x * resid[:, None]  # (n, k) score contributions

    s = u.T @ u
    for lag_l in range(1, lag + 1):
        if lag_l >= n:
            break
        gamma = u[lag_l:].T @ u[:-lag_l]
        weight = 1.0 - lag_l / (lag + 1.0)
        s = s + weight * (gamma + gamma.T)

    dof = max(n - k, 1)
    return xtx_inv @ s @ xtx_inv * (n / dof)


def ols(
    x: np.ndarray, y: np.ndarray, names: tuple[str, ...], *, hac_lag: int
) -> OlsFit:
    """Least squares with Newey-West standard errors.

    `names` excludes the intercept; it is prepended as 'const'. `pinv` rather than
    `solve` so a collinear design degrades to a minimum-norm solution instead of
    raising — collinearity among these features is expected (D-027).
    """
    x = np.atleast_2d(x)
    design = add_intercept(x)
    beta = np.linalg.pinv(design) @ y
    resid = y - design @ beta
    dof = max(design.shape[0] - design.shape[1], 1)
    sigma2 = float(resid @ resid / dof)
    cov = _hac_cov(design, resid, hac_lag)
    se = np.sqrt(np.maximum(np.diag(cov), 0.0))
    return OlsFit(
        beta=beta,
        se=se,
        names=("const", *names),
        n_obs=design.shape[0],
        sigma2=sigma2,
        hac_lag=hac_lag,
    )


# ----------------------------------------------------------------------
# Walk-forward with purge + embargo
# ----------------------------------------------------------------------


def walk_forward_splits(
    n: int,
    horizon: int,
    *,
    min_train: int = MIN_TRAIN_WEEKS,
    embargo: int = EMBARGO_WEEKS,
) -> list[tuple[np.ndarray, int]]:
    """Expanding-window splits as (train_indices, test_index) pairs.

    Index `i` carries features at week `i` and a target realised at `i + horizon`,
    so a training row is admissible for a test at `t` only when

        i + horizon + embargo <= t

    i.e. its target closed strictly before the test week, with the embargo of
    slack. Without this the h - 1 overlapping weeks leak the test target into
    training and the score is fiction (Part C #1).
    """
    splits: list[tuple[np.ndarray, int]] = []
    for test in range(n):
        if test + horizon >= n:
            break  # target not yet realised — nothing to score against
        usable = test - horizon - embargo
        if usable < min_train:
            continue
        splits.append((np.arange(usable + 1), test))
    return splits


# ----------------------------------------------------------------------
# The model ladder
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class Prediction:
    """One walk-forward step: predicted vs realised h-week change."""

    index: int
    predicted_change: float
    realised_change: float
    sd: float  # predictive standard deviation, for interval coverage

    @property
    def error(self) -> float:
        return self.predicted_change - self.realised_change


def run_ladder(
    level: np.ndarray,
    controls: np.ndarray,
    control_names: tuple[str, ...],
    horizon: int,
    *,
    min_train: int = MIN_TRAIN_WEEKS,
) -> dict[str, list[Prediction]]:
    """Walk M0/M1/M2 forward over the weekly grid.

    `level[i]` is the spread at week `i`; the target is `level[i + horizon]`. Each
    model predicts the *level* and is scored on the implied **change**, which is
    the same ranking but keeps M0 visible as the honest "predict no change".
    """
    n = len(level)
    hac_lag = newey_west_lag(horizon)
    splits = walk_forward_splits(n, horizon, min_train=min_train)
    out: dict[str, list[Prediction]] = {"M0": [], "M1": [], "M2": []}

    for train_idx, test in splits:
        y_train = level[train_idx + horizon]
        realised = float(level[test + horizon] - level[test])

        # M0 — no change. No parameters, so no fit; sd from the training spread
        # of realised changes, which is what a random-walk forecaster would quote.
        train_changes = level[train_idx + horizon] - level[train_idx]
        out["M0"].append(
            Prediction(test, 0.0, realised, float(np.std(train_changes, ddof=1)))
        )

        # M1 — AR(1) on the level.
        x1_train = level[train_idx].reshape(-1, 1)
        fit1 = ols(x1_train, y_train, ("ar1",), hac_lag=hac_lag)
        pred1 = float(fit1.predict(np.array([[level[test]]]))[0])
        out["M1"].append(
            Prediction(test, pred1 - level[test], realised, float(np.sqrt(fit1.sigma2)))
        )

        # M2 — AR(1) + controls.
        x2_train = np.hstack([x1_train, controls[train_idx]])
        fit2 = ols(x2_train, y_train, ("ar1", *control_names), hac_lag=hac_lag)
        x2_test = np.hstack([[level[test]], controls[test]]).reshape(1, -1)
        pred2 = float(fit2.predict(x2_test)[0])
        out["M2"].append(
            Prediction(test, pred2 - level[test], realised, float(np.sqrt(fit2.sigma2)))
        )

    return out


# ----------------------------------------------------------------------
# Scoring
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class Score:
    model: str
    n: int
    mae: float
    rmse: float
    coverage_80: float

    def skill_vs(self, null: "Score") -> float:
        """Fractional MAE improvement over a null. Positive = better."""
        return (null.mae - self.mae) / null.mae if null.mae else 0.0


def score(model: str, preds: list[Prediction]) -> Score:
    errors = np.array([p.error for p in preds])
    sds = np.array([p.sd for p in preds])
    inside = np.abs(errors) <= Z_80 * np.maximum(sds, 1e-12)
    return Score(
        model=model,
        n=len(preds),
        mae=float(np.mean(np.abs(errors))),
        rmse=float(np.sqrt(np.mean(errors**2))),
        coverage_80=float(np.mean(inside)),
    )
