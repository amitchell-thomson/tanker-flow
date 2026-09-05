"""Unit tests for the Part B null and the FWL harness (analysis.b0, analysis.fwl).

Pure-logic: synthetic designs with known answers, the FWL theorem as a property,
and the purge/embargo rules. No DB, no network.
"""

from __future__ import annotations

import numpy as np
import pytest

from analysis.b0 import (
    MIN_TRAIN_WEEKS,
    Prediction,
    add_intercept,
    newey_west_lag,
    ols,
    run_ladder,
    score,
    walk_forward_splits,
)
from analysis.fwl import partial_effect, residualise

RNG = np.random.default_rng(20260905)


# --- OLS ----------------------------------------------------------------------


def test_ols_recovers_known_coefficients():
    x = RNG.normal(size=(400, 2))
    y = 3.0 + 2.0 * x[:, 0] - 1.5 * x[:, 1] + RNG.normal(scale=0.01, size=400)
    fit = ols(x, y, ("a", "b"), hac_lag=2)
    assert fit.beta == pytest.approx([3.0, 2.0, -1.5], abs=0.01)
    assert fit.names == ("const", "a", "b")


def test_ols_is_exact_with_no_noise():
    x = RNG.normal(size=(50, 3))
    y = 1.0 + x @ np.array([0.5, -2.0, 4.0])
    fit = ols(x, y, ("a", "b", "c"), hac_lag=1)
    assert fit.beta == pytest.approx([1.0, 0.5, -2.0, 4.0], abs=1e-9)
    assert fit.sigma2 == pytest.approx(0.0, abs=1e-18)


def test_ols_survives_perfectly_collinear_design():
    """Collinearity among these features is expected (D-027); pinv must degrade
    to a minimum-norm solution rather than raise."""
    base = RNG.normal(size=(100, 1))
    x = np.hstack([base, base * 2])  # exactly collinear
    y = base[:, 0] + RNG.normal(scale=0.01, size=100)
    fit = ols(x, y, ("a", "b"), hac_lag=2)
    assert np.all(np.isfinite(fit.beta))


def test_add_intercept_prepends_ones():
    out = add_intercept(np.array([[1.0, 2.0], [3.0, 4.0]]))
    assert out.shape == (2, 3)
    assert np.all(out[:, 0] == 1.0)


# --- HAC ----------------------------------------------------------------------


def test_newey_west_lag_is_horizon_plus_one():
    """Fixed in advance (D-028) — a data-chosen lag is a researcher degree of
    freedom."""
    assert newey_west_lag(1) == 2
    assert newey_west_lag(4) == 5


def test_hac_se_equals_ols_se_at_lag_zero_iid():
    """With lag 0 and homoskedastic iid errors the sandwich reduces to the usual
    White form, which tracks the OLS SE closely — a scale sanity check."""
    x = RNG.normal(size=(2000, 1))
    y = 1.0 + 0.5 * x[:, 0] + RNG.normal(size=2000)
    fit = ols(x, y, ("a",), hac_lag=0)
    naive = np.sqrt(fit.sigma2 / (2000 * np.var(x[:, 0])))
    assert fit.se[1] == pytest.approx(naive, rel=0.15)


def test_hac_se_exceeds_naive_se_under_positive_autocorrelation():
    """The whole reason HAC is mandatory: overlapping h-week windows make
    residuals positively autocorrelated, and an OLS SE is then too small."""
    n = 1500
    x = RNG.normal(size=n)
    noise = np.zeros(n)
    for i in range(1, n):
        noise[i] = 0.85 * noise[i - 1] + RNG.normal()
    y = 0.3 * x + noise
    lag0 = ols(x.reshape(-1, 1), y, ("a",), hac_lag=0)
    lag10 = ols(x.reshape(-1, 1), y, ("a",), hac_lag=10)
    assert lag10.se[1] > lag0.se[1]


def test_hac_covariance_is_positive_semidefinite():
    """Bartlett weighting exists to guarantee this; a negative variance would
    surface as a nan standard error."""
    x = RNG.normal(size=(300, 3))
    y = RNG.normal(size=300)
    fit = ols(x, y, ("a", "b", "c"), hac_lag=8)
    assert np.all(fit.se >= 0)
    assert np.all(np.isfinite(fit.se))


# --- Walk-forward purge + embargo ---------------------------------------------


def test_walk_forward_respects_the_purge_gap():
    """No training row's target may reach the test week — that is the leak the
    purge exists to stop."""
    horizon, embargo = 4, 1
    splits = walk_forward_splits(400, horizon, min_train=100, embargo=embargo)
    for train_idx, test in splits:
        assert train_idx.max() + horizon + embargo <= test


def test_walk_forward_never_scores_an_unrealised_target():
    n, horizon = 200, 4
    for _, test in walk_forward_splits(n, horizon, min_train=100):
        assert test + horizon < n


def test_walk_forward_requires_the_minimum_training_window():
    splits = walk_forward_splits(500, 1, min_train=MIN_TRAIN_WEEKS)
    assert all(len(train) >= MIN_TRAIN_WEEKS for train, _ in splits)


def test_walk_forward_windows_expand():
    splits = walk_forward_splits(400, 1, min_train=50)
    sizes = [len(t) for t, _ in splits]
    assert sizes == sorted(sizes)
    assert sizes[-1] > sizes[0]


def test_walk_forward_is_empty_when_history_is_too_short():
    assert walk_forward_splits(50, 4, min_train=MIN_TRAIN_WEEKS) == []


def test_longer_horizon_purges_more():
    """At the same test week, a longer horizon must drop more training rows —
    its targets reach further into the test point.

    Compared at a fixed test index, not at the first split: the first split of
    any horizon holds exactly `min_train` rows by construction, so comparing
    those would compare the floor rather than the purge.
    """
    short = dict(
        (test, train) for train, test in walk_forward_splits(400, 1, min_train=100)
    )
    long = dict(
        (test, train) for train, test in walk_forward_splits(400, 8, min_train=100)
    )
    shared = sorted(set(short) & set(long))
    assert shared, "horizons must overlap somewhere to be comparable"
    probe = shared[len(shared) // 2]
    assert len(long[probe]) == len(short[probe]) - 7  # (8 + 1) - (1 + 1)


# --- Ladder + scoring ---------------------------------------------------------


def test_ladder_recovers_a_predictable_control_effect():
    """When the spread genuinely responds to a control, M2 must beat M0."""
    n = 400
    control = RNG.normal(size=(n, 1))
    level = np.cumsum(RNG.normal(scale=0.1, size=n)) + 5.0 * control[:, 0]
    out = run_ladder(level, control, ("c",), horizon=1, min_train=60)
    m0, m2 = score("M0", out["M0"]), score("M2", out["M2"])
    assert m2.mae < m0.mae


def test_ladder_cannot_beat_no_change_on_a_pure_random_walk():
    """The honest negative control: on a random walk nothing beats M0, and a
    harness that claims otherwise is leaking."""
    n = 500
    level = np.cumsum(RNG.normal(size=n))
    control = RNG.normal(size=(n, 2))
    out = run_ladder(level, control, ("a", "b"), horizon=1, min_train=104)
    m0, m2 = score("M0", out["M0"]), score("M2", out["M2"])
    assert m2.skill_vs(m0) < 0.10


def test_ladder_m0_always_predicts_no_change():
    level = np.cumsum(RNG.normal(size=300))
    out = run_ladder(level, RNG.normal(size=(300, 1)), ("c",), horizon=2, min_train=60)
    assert all(p.predicted_change == 0.0 for p in out["M0"])


def test_ladder_realised_change_matches_the_series():
    level = np.arange(300, dtype=float) ** 1.1
    out = run_ladder(level, RNG.normal(size=(300, 1)), ("c",), horizon=4, min_train=60)
    for p in out["M0"]:
        assert p.realised_change == pytest.approx(level[p.index + 4] - level[p.index])


def test_score_computes_mae_rmse_and_coverage():
    preds = [Prediction(i, 1.0, 0.0, 1.0) for i in range(100)]
    s = score("M", preds)
    assert s.mae == pytest.approx(1.0)
    assert s.rmse == pytest.approx(1.0)
    # |error| = 1.0 vs an 80% half-width of 1.28 -> every point inside.
    assert s.coverage_80 == pytest.approx(1.0)


def test_skill_is_zero_against_itself():
    preds = [Prediction(i, 0.5, 0.0, 1.0) for i in range(50)]
    s = score("M", preds)
    assert s.skill_vs(s) == pytest.approx(0.0)


# --- FWL ----------------------------------------------------------------------


def test_fwl_coefficient_equals_the_multiple_regression_coefficient():
    """The FWL theorem, used as this harness's correctness test: partialling the
    controls out of both sides must reproduce the full-regression coefficient
    exactly, not approximately."""
    n = 500
    z = RNG.normal(size=(n, 3))
    t = 0.7 * z[:, 0] + RNG.normal(size=n)  # signal correlated with the controls
    y = 1.0 + 2.0 * z[:, 0] - 0.5 * z[:, 1] + 1.3 * t + RNG.normal(scale=0.2, size=n)

    full = ols(np.hstack([z, t.reshape(-1, 1)]), y, ("z1", "z2", "z3", "t"), hac_lag=2)
    beta_full, _, _ = full.coefficient("t")

    y_res, t_res = residualise(y, z), residualise(t, z)
    beta_fwl = ols(t_res.reshape(-1, 1), y_res, ("t",), hac_lag=2).coefficient("t")[0]

    assert beta_fwl == pytest.approx(beta_full, rel=1e-10)


def test_residualise_removes_the_control_component():
    z = RNG.normal(size=(200, 2))
    target = 3.0 * z[:, 0] - 2.0 * z[:, 1] + 7.0
    assert residualise(target, z) == pytest.approx(np.zeros(200), abs=1e-9)


def test_residualise_is_orthogonal_to_the_controls():
    z = RNG.normal(size=(200, 2))
    r = residualise(RNG.normal(size=200), z)
    assert z.T @ r == pytest.approx(np.zeros(2), abs=1e-9)


def test_fwl_kills_a_purely_seasonal_confound():
    """The headline threat (MODELS.md 2): a signal and the spread share a winter
    cycle and nothing else. Partialling the season out must leave no effect."""
    n = 520
    week = np.arange(n)
    winter = np.sin(2 * np.pi * week / 52).reshape(-1, 1)
    signal = winter[:, 0] * 3 + RNG.normal(scale=0.1, size=n)
    y = winter[:, 0] * 5 + RNG.normal(scale=0.1, size=n)  # no true link to signal
    years = 2018 + week // 52

    eff = partial_effect(
        "seasonal_only", y, signal, winter, years, horizon=1, expected_sign=1
    )
    assert abs(eff.t_stat) < 1.96
    assert eff.verdict() == "not significant"


def test_fwl_detects_a_real_effect_beneath_a_shared_season():
    n = 520
    week = np.arange(n)
    winter = np.sin(2 * np.pi * week / 52).reshape(-1, 1)
    signal = winter[:, 0] * 3 + RNG.normal(size=n)
    y = winter[:, 0] * 5 + 2.0 * signal + RNG.normal(scale=0.3, size=n)
    years = 2018 + week // 52

    eff = partial_effect("real", y, signal, winter, years, horizon=1, expected_sign=1)
    assert eff.significant
    assert eff.beta == pytest.approx(2.0, rel=0.15)
    assert eff.verdict() == "CARRIES INFORMATION"


def test_fwl_flags_a_falsified_pre_registered_sign():
    """A real effect running the wrong way must be reported as a falsified sign,
    not quietly accepted as a win (the D-018 precedent)."""
    n = 400
    z = RNG.normal(size=(n, 1))
    signal = RNG.normal(size=n)
    y = -2.0 * signal + RNG.normal(scale=0.2, size=n)
    years = 2018 + np.arange(n) // 52

    eff = partial_effect("wrong_way", y, signal, z, years, horizon=1, expected_sign=1)
    assert eff.significant
    assert not eff.sign_matches
    assert eff.verdict() == "SIGN FALSIFIED"


def test_fwl_accepts_either_direction_when_no_prior_is_registered():
    n = 400
    z = RNG.normal(size=(n, 1))
    signal = RNG.normal(size=n)
    y = -2.0 * signal + RNG.normal(scale=0.2, size=n)
    years = 2018 + np.arange(n) // 52
    eff = partial_effect("no_prior", y, signal, z, years, horizon=1, expected_sign=0)
    assert eff.sign_matches  # nothing to violate


def test_fwl_flags_an_effect_carried_by_one_year():
    """A signal that only works in one extraordinary year must not pass — the
    2022 guard."""
    n = 520
    years = 2018 + np.arange(n) // 52
    z = RNG.normal(size=(n, 1))
    signal = RNG.normal(size=n)
    # Effect present only in 2022; other years are pure noise.
    y = np.where(years == 2022, 40.0 * signal, 0.0) + RNG.normal(scale=1.0, size=n)

    eff = partial_effect("one_year", y, signal, z, years, horizon=1, expected_sign=1)
    assert eff.significant
    assert eff.year_consistency < 2 / 3
    assert "unstable across years" in eff.verdict()


def test_partial_r2_is_a_fraction():
    n = 300
    z = RNG.normal(size=(n, 1))
    signal = RNG.normal(size=n)
    y = 1.5 * signal + RNG.normal(size=n)
    years = 2018 + np.arange(n) // 52
    eff = partial_effect("f", y, signal, z, years, horizon=1, expected_sign=1)
    assert 0.0 <= eff.partial_r2 <= 1.0
