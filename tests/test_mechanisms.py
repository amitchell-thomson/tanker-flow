"""Unit tests for the H2/H3/H4 mechanism tests (analysis.mechanisms, D-031).

Pure-logic: the derived features, the seasonal-norm split, the confound gate and
the four-condition grading. No DB, no network.
"""

from __future__ import annotations

import numpy as np
import pytest

from analysis.mechanisms import (
    BONFERRONI_T,
    HypothesisResult,
    eu_share,
    interaction,
    seasonal_norm,
    tightness_mask,
    trend_r2,
)

RNG = np.random.default_rng(20260905)


# --- H2: the EU share ---------------------------------------------------------


def test_eu_share_is_the_known_bound_fraction():
    share = eu_share(np.array([25.0, 50.0]), np.array([75.0, 50.0]))
    assert share == pytest.approx([0.25, 0.5])


def test_eu_share_is_nan_when_nothing_is_at_sea():
    """A day with no cargo at sea has no share; 0 would read as 'none bound for
    EU', which is a different and false claim."""
    share = eu_share(np.array([0.0]), np.array([0.0]))
    assert np.isnan(share[0])


def test_eu_share_is_one_when_every_cargo_is_resolved():
    assert eu_share(np.array([10.0]), np.array([0.0]))[0] == pytest.approx(1.0)


# --- H2's coverage-confound gate ----------------------------------------------


def test_trend_r2_is_one_for_a_pure_ramp():
    """The failure mode the gate exists to catch: a share that is really just our
    destination-resolution improving over the decade."""
    assert trend_r2(np.arange(100, dtype=float)) == pytest.approx(1.0)


def test_trend_r2_is_near_zero_for_noise():
    assert trend_r2(RNG.normal(size=2000)) < 0.05


def test_trend_r2_is_zero_for_a_constant():
    assert trend_r2(np.full(50, 3.0)) == 0.0


def test_trend_r2_ignores_sign_of_the_trend():
    assert trend_r2(-np.arange(100, dtype=float)) == pytest.approx(1.0)


# --- H3: the interaction ------------------------------------------------------


def test_interaction_is_the_centred_product():
    a = np.array([1.0, 2.0, 3.0])
    b = np.array([10.0, 20.0, 30.0])
    assert interaction(a, b) == pytest.approx([10.0, 0.0, 10.0])


def test_interaction_is_zero_when_either_input_is_constant():
    a = np.full(10, 5.0)
    assert interaction(a, RNG.normal(size=10)) == pytest.approx(np.zeros(10))


def test_interaction_is_symmetric():
    a, b = RNG.normal(size=20), RNG.normal(size=20)
    assert interaction(a, b) == pytest.approx(interaction(b, a))


# --- H4: the seasonal split ---------------------------------------------------


def test_seasonal_norm_recovers_a_known_annual_cycle():
    doy = np.tile(np.arange(1, 366), 4)
    values = 50 + 30 * np.sin(2 * np.pi * doy / 365)
    norm = seasonal_norm(values, doy, np.ones(len(values), dtype=bool))
    # A +/-15 day median tracks the cycle closely but smooths the turning points.
    assert np.max(np.abs(norm - values)) < 3.0


def test_seasonal_norm_is_fitted_only_on_the_masked_rows():
    """The split rule must not be estimated using holdout data (D-031)."""
    doy = np.tile(np.arange(1, 366), 2)
    values = np.concatenate([np.full(365, 10.0), np.full(365, 1000.0)])
    fit_mask = np.concatenate([np.ones(365, bool), np.zeros(365, bool)])
    norm = seasonal_norm(values, doy, fit_mask)
    # Norms come from the first year alone, so the inflated second year cannot
    # raise its own threshold.
    assert np.allclose(norm, 10.0)


def test_tightness_mask_flags_below_seasonal_norm():
    doy = np.tile(np.arange(1, 366), 2)
    values = np.concatenate([np.full(365, 50.0), np.full(365, 50.0)])
    values[400] = 10.0  # one deeply below-norm day
    mask = tightness_mask(values, doy, np.ones(len(values), dtype=bool))
    assert mask[400]
    assert not mask[0]


def test_tightness_mask_is_seasonal_not_absolute():
    """EU storage troughs near 38% in March and peaks near 90% in October, so a
    fixed threshold would label every spring 'tight' and every autumn 'loose'."""
    doy = np.tile(np.array([60, 280]), 50)  # alternating March / October
    values = np.tile(np.array([38.0, 90.0]), 50)
    mask = tightness_mask(values, doy, np.ones(len(values), dtype=bool))
    # Every value sits exactly at its own seasonal norm -> nothing is tight.
    assert not mask.any()


# --- Grading ------------------------------------------------------------------


def _result(**kw) -> HypothesisResult:
    base = dict(
        name="h",
        description="d",
        expected_sign=1,
        beta=1.0,
        t_stat=3.0,
        n_discovery=300,
        year_consistency=1.0,
        partial_r2=0.05,
        holdout_beta=1.0,
        n_holdout=60,
    )
    return HypothesisResult(**{**base, **kw})


def test_verdict_supported_requires_all_four_conditions():
    assert _result().verdict() == "SUPPORTED"


def test_verdict_uses_the_bonferroni_bar_not_1_96():
    """A |t| of 2.0 clears D-028's bar but not D-031's — the whole point of
    correcting for three tests."""
    assert _result(t_stat=2.0).verdict().startswith("not significant")
    assert _result(t_stat=2.5).verdict() == "SUPPORTED"
    assert BONFERRONI_T > 1.96


def test_verdict_reports_a_falsified_sign():
    assert _result(beta=-1.0, holdout_beta=-1.0).verdict() == "SIGN FALSIFIED"


def test_verdict_flags_year_instability():
    assert "unstable" in _result(year_consistency=0.4).verdict()


def test_verdict_requires_the_holdout_sign_to_replicate():
    assert _result(holdout_beta=-1.0).verdict() == "fails to replicate on the holdout"


def test_verdict_fails_when_the_holdout_is_too_small_to_estimate():
    assert _result(holdout_beta=None).verdict() == "fails to replicate on the holdout"


def test_void_short_circuits_every_other_condition():
    """A coverage-confounded feature must not be reported as a result in either
    direction, however strong its t-statistic looks."""
    r = _result(t_stat=99.0, void_reason="coverage-confounded")
    assert r.verdict().startswith("VOID")
