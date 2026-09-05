"""Unit tests for the A4 Kalman nowcast (analysis.a4). Pure-logic, no DB.

Spec: DECISIONS.md D-022.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

from analysis.a4 import (
    CLIMATOLOGY_WEEKS,
    GaussianPredictive,
    _inv_norm,
    climatology,
    fit_local_level,
    fit_local_trend,
    local_level_filter,
    persistence,
)
from analysis.a4_replay import ReplayRow, replay, score, weekly_series

D0 = datetime(2020, 1, 6, tzinfo=UTC)


def grid(n):
    return [D0 + timedelta(days=7 * i) for i in range(n)]


# --- nulls ------------------------------------------------------------------


def test_persistence_is_the_last_observation():
    assert persistence([1.0, 5.0, 9.0]) == 9.0


def test_climatology_is_the_trailing_mean():
    y = [1.0, 2.0, 3.0, 4.0, 100.0]
    assert climatology(y, weeks=CLIMATOLOGY_WEEKS) == (2 + 3 + 4 + 100) / 4


def test_climatology_handles_a_short_series():
    assert climatology([5.0, 7.0]) == 6.0


# --- the filter -------------------------------------------------------------


def test_filter_tracks_a_constant_series():
    y = [10.0] * 60
    _, level, _ = local_level_filter(y, q=0.1, r=1.0)
    assert abs(level - 10.0) < 1e-6


def test_filter_follows_a_step_change():
    y = [5.0] * 60 + [20.0] * 60
    _, level, _ = local_level_filter(y, q=1.0, r=1.0)
    assert 15.0 < level <= 20.5  # moved most of the way to the new level


def test_high_q_tracks_faster_than_low_q():
    """`q/r` is the whole behaviour: more state noise => chase the data harder."""
    y = [5.0] * 40 + [25.0] * 5
    _, fast, _ = local_level_filter(y, q=10.0, r=1.0)
    _, slow, _ = local_level_filter(y, q=0.001, r=1.0)
    assert fast > slow


def test_filter_variance_stays_positive():
    y = [3.0, 8.0, 1.0, 9.0, 4.0] * 10
    _, _, p = local_level_filter(y, q=0.5, r=2.0)
    assert p > 0


def test_loglik_is_finite_and_prefers_the_true_parameters():
    # Series generated with a fairly smooth level: a small q should beat a huge one.
    y = [10.0 + 0.05 * i for i in range(80)]
    ll_small, _, _ = local_level_filter(y, q=0.01, r=1.0)
    ll_big, _, _ = local_level_filter(y, q=100.0, r=1.0)
    assert math.isfinite(ll_small) and ll_small > ll_big


# --- MLE --------------------------------------------------------------------


def test_fit_local_level_recovers_smoothing_on_a_noisy_constant():
    """Pure noise around a constant => the level is stable => small q/r, small alpha."""
    rng = __import__("random").Random(5)
    y = [20.0 + rng.gauss(0, 3.0) for _ in range(200)]
    fit = fit_local_level(y)
    assert fit.q > 0 and fit.r > 0
    assert fit.ewma_alpha < 0.5  # smooths heavily rather than chasing noise
    assert abs(fit.level - 20.0) < 3.0


def test_fit_local_level_chases_a_fast_random_walk():
    rng = __import__("random").Random(7)
    y, x = [], 20.0
    for _ in range(200):
        x += rng.gauss(0, 3.0)
        y.append(x + rng.gauss(0, 0.3))
    fit = fit_local_level(y)
    assert fit.ewma_alpha > 0.5  # tracks, because the level really moves


def test_ewma_alpha_is_bounded_and_monotone_in_snr():
    rng = __import__("random").Random(3)
    noisy = [20.0 + rng.gauss(0, 5.0) for _ in range(150)]
    walk, x = [], 20.0
    for _ in range(150):
        x += rng.gauss(0, 5.0)
        walk.append(x)
    a_noisy = fit_local_level(noisy).ewma_alpha
    a_walk = fit_local_level(walk).ewma_alpha
    assert 0.0 <= a_noisy <= 1.0 and 0.0 <= a_walk <= 1.0
    assert a_walk > a_noisy


def test_forecast_variance_grows_with_horizon():
    fit = fit_local_level([10.0 + (i % 3) for i in range(150)])
    _, v1 = fit.forecast(1)
    _, v2 = fit.forecast(2)
    assert v2 > v1


def test_local_level_forecast_is_flat_across_horizons():
    """The defining property: a local level model does NOT extrapolate a trend."""
    fit = fit_local_level([float(i) for i in range(1, 121)])
    m1, _ = fit.forecast(1)
    m2, _ = fit.forecast(2)
    assert m1 == m2


def test_local_trend_extrapolates_where_local_level_does_not():
    """The pre-registered secondary: adding a slope state makes it extrapolate."""
    y = [float(i) for i in range(1, 121)]  # clean linear trend
    trend = fit_local_trend(y)
    m1, _ = trend.forecast(1)
    m2, _ = trend.forecast(2)
    assert m2 > m1
    assert abs(trend.slope - 1.0) < 0.3  # recovers the slope of +1/step


def test_local_trend_nests_local_level_on_a_trendless_series():
    rng = __import__("random").Random(11)
    y = [20.0 + rng.gauss(0, 2.0) for _ in range(200)]
    trend = fit_local_trend(y)
    assert abs(trend.slope) < 1.0  # no spurious trend invented


# --- Gaussian predictive ----------------------------------------------------


def test_interval_is_symmetric_and_widens_with_coverage():
    d = GaussianPredictive(mean=10.0, var=4.0)
    lo50, hi50 = d.interval(0.50)
    lo80, hi80 = d.interval(0.80)
    assert abs((hi50 + lo50) / 2 - 10.0) < 1e-9
    assert (hi80 - lo80) > (hi50 - lo50)


def test_interval_matches_known_normal_quantiles():
    d = GaussianPredictive(mean=0.0, var=1.0)
    lo, hi = d.interval(0.80)
    assert abs(hi - 1.2816) < 1e-3  # the 90th percentile of N(0,1)


def test_crps_is_zero_at_the_point_mass_limit_and_grows_with_error():
    d = GaussianPredictive(mean=10.0, var=1e-10)
    assert d.crps(10.0) < 1e-4
    wide = GaussianPredictive(mean=10.0, var=4.0)
    assert wide.crps(10.0) < wide.crps(14.0) < wide.crps(20.0)


def test_crps_closed_form_matches_numeric_integration():
    """Pin the analytic CRPS against a numeric integral of (F - 1{y<=x})^2."""
    d = GaussianPredictive(mean=3.0, var=2.0)
    y = 4.5
    lo, hi, n = -20.0, 30.0, 200_000
    step = (hi - lo) / n
    from analysis.a4 import _Phi

    total = 0.0
    for i in range(n):
        x = lo + (i + 0.5) * step
        f = _Phi((x - d.mean) / d.sd)
        total += (f - (1.0 if y <= x else 0.0)) ** 2 * step
    assert abs(d.crps(y) - total) < 1e-3


def test_pit_is_uniform_for_a_calibrated_forecast():
    d = GaussianPredictive(mean=0.0, var=1.0)
    assert abs(d.pit(0.0) - 0.5) < 1e-9
    assert d.pit(-2.0) < 0.05 and d.pit(2.0) > 0.95


def test_inv_norm_round_trips():
    from analysis.a4 import _Phi

    for p in (0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99):
        assert abs(_Phi(_inv_norm(p)) - p) < 1e-6


# --- weekly series + replay -------------------------------------------------


def test_weekly_series_is_left_open_right_closed():
    g = grid(3)
    times = [g[0], g[1], g[1] - timedelta(days=3)]
    y = weekly_series(times, g)
    # g[0] falls in no (t-7, t] except its own; g[1] and the mid-week one land in g[1]
    assert y[1] == 2.0


def test_weekly_series_counts_each_event_once():
    g = grid(10)
    times = [g[0] + timedelta(days=3 + 7 * i) for i in range(8)]
    assert sum(weekly_series(times, g)) == len(times)


def test_replay_respects_the_minimum_training_window():
    g = grid(150)
    y = [10.0 + (i % 5) for i in range(150)]
    rows = replay(g, y, models=("level",))
    assert rows
    assert all(r.n_train >= 104 for r in rows)


def test_replay_targets_are_the_following_weeks():
    g = grid(130)
    y = [float(i) for i in range(130)]
    rows = replay(g, y, models=("level",))
    by = {(r.as_of, r.horizon): r for r in rows}
    for i, t in enumerate(g):
        if (t, "W1") in by:
            assert by[(t, "W1")].truth == y[i + 1]
        if (t, "W2") in by:
            assert by[(t, "W2")].truth == y[i + 2]


def test_replay_nulls_match_the_series_definitions():
    g = grid(130)
    y = [float(i % 7) for i in range(130)]
    rows = replay(g, y, models=("level",))
    idx = {t: i for i, t in enumerate(g)}
    for r in rows:
        hist = y[: idx[r.as_of] + 1]
        assert r.persistence == persistence(hist)
        assert abs(r.climatology - climatology(hist)) < 1e-12


def test_replay_never_uses_a_future_observation():
    """A step change after the as-of must not move that as-of's forecast."""
    g = grid(130)
    base = [10.0] * 130
    spiked = base[:120] + [500.0] * 10
    r_base = {(r.as_of, r.horizon): r.forecast
              for r in replay(g, base, models=("level",))}
    r_spike = {(r.as_of, r.horizon): r.forecast
               for r in replay(g, spiked, models=("level",))}
    for key, val in r_base.items():
        if key[0] < g[119]:
            assert abs(r_spike[key] - val) < 1e-9


def mk_row(**kw):
    d = dict(as_of=D0, horizon="W1", model="level", truth=10.0, forecast=9.0,
             sd=2.0, crps=0.5, pit=0.5, lo50=8.0, hi50=10.0, lo80=6.0, hi80=12.0,
             persistence=4.0, climatology=13.0, n_train=120, alpha=0.3, snr=0.1)
    d.update(kw)
    return ReplayRow(**d)


def test_score_computes_mae_bias_and_nulls():
    rows = [mk_row(truth=10.0, forecast=8.0, persistence=4.0, climatology=13.0),
            mk_row(truth=6.0, forecast=10.0, persistence=8.0, climatology=3.0)]
    sc = score(rows, "x")
    assert abs(sc.mae - 3.0) < 1e-12
    assert abs(sc.bias - 1.0) < 1e-12
    assert abs(sc.persist_mae - 4.0) < 1e-12
    assert abs(sc.clim_mae - 3.0) < 1e-12


def test_score_coverage_uses_inclusive_bounds():
    rows = [mk_row(truth=10.0, lo50=8.0, hi50=10.0, lo80=6.0, hi80=12.0)]
    sc = score(rows, "x")
    assert sc.cov50 == 1.0 and sc.cov80 == 1.0


def test_score_empty_is_safe():
    sc = score([], "empty")
    assert sc.n == 0 and sc.mae != sc.mae
