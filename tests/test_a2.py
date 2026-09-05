"""Unit tests for the A2 count GLM (analysis.a2). Pure-logic, no DB.

Spec and pre-registered signs: DECISIONS.md D-016.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

from analysis.a2 import (
    AR_BLOCK,
    FEATURES,
    PHYSICAL,
    W1,
    W2,
    CountEvent,
    CountPredictive,
    Row,
    Standardiser,
    ballast_arrivals,
    build_row,
    fit_glm,
    loadings_in_window,
    open_queues_at,
    open_visits_at,
    training_rows,
)
from pipeline.queues import Queue
from pipeline.signal import LaneFilter
from pipeline.visits import Visit

AS_OF = datetime(2024, 6, 3, tzinfo=UTC)
LANE = LaneFilter(
    export_zones=frozenset({"usgulf", "usatlantic"}),
    import_zones=frozenset({"nweurope", "baltic", "iberian", "wmed", "emed"}),
)


def ev(etype, days_before, *, zone="usgulf", laden=True, regime="noaa", mmsi=1):
    return CountEvent(
        mmsi=mmsi,
        event_type=etype,
        event_time=AS_OF - timedelta(days=days_before),
        zone=zone,
        laden_flag=laden,
        regime=regime,
    )


def visit(moored_days_before, departed_days_before=None, *, zone="usgulf",
          flow="export", mmsi=1):
    return Visit(
        mmsi=mmsi, terminal_id=1, zone=zone, flow_direction=flow,
        moored_ts=AS_OF - timedelta(days=moored_days_before),
        departed_ts=(None if departed_days_before is None
                     else AS_OF - timedelta(days=departed_days_before)),
        laden=False, regime="noaa",
    )


def queue(entry_days_before, moored_days_before=None, *, zone="usgulf",
          flow="export", mmsi=1):
    return Queue(
        mmsi=mmsi, terminal_id=1, zone=zone, flow_direction=flow,
        entry_ts=AS_OF - timedelta(days=entry_days_before),
        moored_ts=(None if moored_days_before is None
                   else AS_OF - timedelta(days=moored_days_before)),
        anchored_ts=None, last_exit_ts=None, anchored_seen=True,
        laden=False, regime="noaa",
    )


# --- target / AR features ---------------------------------------------------


def test_loadings_counts_laden_us_departures_in_window():
    events = [
        ev("departed", 3),                              # in
        ev("departed", 3, laden=False),                 # ballast -> out
        ev("departed", 3, zone="nweurope"),             # EU origin -> out
        ev("zone_entry", 3),                            # wrong type -> out
        ev("departed", 10),                             # too old -> out
    ]
    n = loadings_in_window(
        events, LANE, lo=AS_OF - timedelta(days=7), hi=AS_OF
    )
    assert n == 1


def test_loadings_window_is_left_open_right_closed():
    # Matches A1's (lo, hi] convention (D-009) so the models are comparable.
    at_hi = [ev("departed", 0)]
    at_lo = [ev("departed", 7)]
    lo, hi = AS_OF - timedelta(days=7), AS_OF
    assert loadings_in_window(at_hi, LANE, lo=lo, hi=hi) == 1
    assert loadings_in_window(at_lo, LANE, lo=lo, hi=hi) == 0


def test_loadings_filters_by_regime():
    events = [ev("departed", 3, regime="noaa"), ev("departed", 3, regime="gfw")]
    lo, hi = AS_OF - timedelta(days=7), AS_OF
    assert loadings_in_window(events, LANE, lo=lo, hi=hi) == 2
    assert loadings_in_window(events, LANE, lo=lo, hi=hi, regime="noaa") == 1


# --- in_berth ---------------------------------------------------------------


def test_in_berth_counts_open_export_visits():
    visits = [
        visit(2),                       # moored 2d ago, still alongside -> in
        visit(3, 1),                    # departed 1d ago -> out
        visit(3, -1),                   # departs tomorrow -> still open now -> in
        visit(2, zone="nweurope", flow="import"),  # EU import -> out
        visit(-1),                      # moors tomorrow -> out
    ]
    assert open_visits_at(visits, AS_OF, LANE) == 2


def test_in_berth_applies_the_open_visit_ceiling():
    # Beyond OPEN_VISIT_CEILING_DAYS (5) an "open" visit is a missed-departure
    # phantom, not a ship still loading.
    assert open_visits_at([visit(4)], AS_OF, LANE) == 1
    assert open_visits_at([visit(30)], AS_OF, LANE) == 0


# --- queue_depth ------------------------------------------------------------


def test_queue_depth_counts_open_export_queues():
    queues = [
        queue(3),               # waiting -> in
        queue(5, 1),            # berthed 1d ago -> out
        queue(5, -1),           # berths tomorrow -> still waiting now -> in
        queue(3, zone="nweurope", flow="import"),  # EU -> out
    ]
    assert open_queues_at(queues, AS_OF, LANE) == 2


def test_queue_depth_applies_the_ceiling_that_the_naive_version_lacked():
    # D-016: the naive "anchorage_entry with no later moored" read 30 vs 1 in berth
    # at 2023-01-02 because stale entries accumulate. The ceiling is what fixes it.
    assert open_queues_at([queue(10)], AS_OF, LANE) == 1
    assert open_queues_at([queue(60)], AS_OF, LANE) == 0


# --- ballast arrivals -------------------------------------------------------


def test_ballast_arrivals_counts_empty_us_zone_entries():
    events = [
        ev("zone_entry", 2, laden=False),               # in
        ev("zone_entry", 2, laden=True),                # laden -> out
        ev("zone_entry", 2, laden=None),                # unknown -> out
        ev("zone_entry", 2, laden=False, zone="nweurope"),  # EU -> out
        ev("departed", 2, laden=False),                 # wrong type -> out
    ]
    n = ballast_arrivals(events, LANE, lo=AS_OF - timedelta(days=7), hi=AS_OF)
    assert n == 1


def test_ballast_feature_is_us_side_not_eu_side():
    """D-016: EU ballast departures are GFW-only; using them would cross a seam.

    The feature must ignore an EU ballast *departure* entirely and pick up the
    corresponding US-side ballast *arrival*.
    """
    eu_departure = ev("departed", 2, laden=False, zone="nweurope", regime="gfw")
    us_arrival = ev("zone_entry", 2, laden=False, zone="usgulf", regime="noaa")
    lo, hi = AS_OF - timedelta(days=7), AS_OF
    assert ballast_arrivals([eu_departure], LANE, lo=lo, hi=hi) == 0
    assert ballast_arrivals([us_arrival], LANE, lo=lo, hi=hi) == 1


# --- build_row --------------------------------------------------------------


def test_build_row_assembles_features_and_target():
    events = (
        [ev("departed", d) for d in (1, 3, 5)]          # lag1 = 3
        + [ev("departed", d) for d in (8, 10)]          # week 2 = 2
        + [ev("departed", 16)]                          # week 3 = 1
        + [ev("zone_entry", 2, laden=False)]            # ballast_arrivals = 1
        + [ev("departed", -2), ev("departed", -5)]      # target W1 = 2
    )
    row = build_row(
        AS_OF, events, events, [visit(1)], [queue(2)], LANE, u0=W1[0], u1=W1[1]
    )
    assert row.feature("lag1") == 3.0
    assert row.feature("trail4") == (3 + 2 + 1 + 0) / 4
    assert row.feature("in_berth") == 1.0
    assert row.feature("ballast_arrivals_1w") == 1.0
    assert row.feature("queue_depth") == 1.0
    assert row.target == 2


def test_build_row_w2_target_excludes_w1_arrivals():
    events = [ev("departed", -3), ev("departed", -10)]
    w1 = build_row(AS_OF, events, events, [], [], LANE, u0=W1[0], u1=W1[1])
    w2 = build_row(AS_OF, events, events, [], [], LANE, u0=W2[0], u1=W2[1])
    assert w1.target == 1 and w2.target == 1


def test_feature_order_is_the_declared_one():
    assert FEATURES == (
        "lag1", "trail4", "in_berth", "ballast_arrivals_1w", "queue_depth"
    )
    assert set(AR_BLOCK) | set(PHYSICAL) == set(FEATURES)
    assert not set(AR_BLOCK) & set(PHYSICAL)


# --- purge ------------------------------------------------------------------


def mk_row(days_before, target=5, feats=None):
    return Row(
        as_of=AS_OF - timedelta(days=days_before),
        features=feats or (1.0, 1.0, 1.0, 1.0, 1.0),
        target=target,
    )


def test_training_rows_purges_rows_whose_target_had_not_closed():
    """Part C #1: a label may only be used once it was actually observable."""
    rows = [mk_row(d) for d in (21, 14, 7, 3, 0)]
    kept = training_rows(rows, AS_OF, u1=W1[1])
    # Only as_of + 7d <= AS_OF survives, i.e. rows at least 7 days old.
    assert [(AS_OF - r.as_of).days for r in kept] == [21, 14, 7]


def test_training_rows_purge_is_stricter_for_the_longer_horizon():
    rows = [mk_row(d) for d in (21, 14, 7, 0)]
    assert len(training_rows(rows, AS_OF, u1=W1[1])) == 3
    assert len(training_rows(rows, AS_OF, u1=W2[1])) == 2  # needs 14d


# --- standardiser -----------------------------------------------------------


def test_standardiser_uses_training_statistics_only():
    rows = [mk_row(0, feats=(f, 0.0, 0.0, 0.0, 0.0)) for f in (1.0, 3.0, 5.0)]
    std = Standardiser.fit(rows)
    assert abs(std.means[0] - 3.0) < 1e-12
    assert abs(std.apply((3.0, 0, 0, 0, 0))[0]) < 1e-12
    assert std.apply((5.0, 0, 0, 0, 0))[0] > 0


def test_standardiser_handles_a_constant_column():
    rows = [mk_row(0, feats=(2.0, 0, 0, 0, 0)) for _ in range(5)]
    std = Standardiser.fit(rows)
    assert std.sds[0] == 1.0  # no divide-by-zero
    assert std.apply((2.0, 0, 0, 0, 0))[0] == 0.0


def test_standardisation_preserves_sign_so_sign_tests_are_valid():
    # A positive linear rescale cannot flip a coefficient's sign.
    assert all(s > 0 for s in Standardiser.fit(
        [mk_row(0, feats=(float(i), float(2 * i), float(i), float(i), float(i)))
         for i in range(1, 10)]
    ).sds)


# --- fitting ----------------------------------------------------------------


def synthetic_rows(n=200, beta=0.8, seed=7):
    """Counts from a known log-linear relationship, all five features varying."""
    rng = __import__("random").Random(seed)
    rows = []
    for i in range(n):
        x = float(i % 10)
        feats = (x, x / 2.0 + rng.random(), rng.random() * 3.0,
                 rng.random() * 5.0, rng.random() * 2.0)
        mu = math.exp(1.0 + beta * (x - 4.5) / 3.0)
        y = max(0, int(round(mu + rng.gauss(0, 0.7))))
        rows.append(Row(
            as_of=AS_OF - timedelta(days=7 * (n - i)),
            features=feats,
            target=y,
        ))
    return rows


def constant_column_rows(n=150):
    """All-constant `queue_depth` — the singular case a thin window can produce."""
    rng = __import__("random").Random(3)
    rows = []
    for i in range(n):
        x = float(i % 8)
        mu = math.exp(1.0 + 0.5 * (x - 3.5) / 2.5)
        rows.append(Row(
            as_of=AS_OF - timedelta(days=7 * (n - i)),
            features=(x, x / 2.0, rng.random(), rng.random(), 0.0),
            target=max(0, int(round(mu + rng.gauss(0, 0.5)))),
        ))
    return rows


def test_fit_survives_a_constant_feature_column():
    # The standardiser centres a constant column to zeros, so its coefficient is
    # unidentified: it must stay at 0 and contribute nothing, not blow up.
    fit = fit_glm(constant_column_rows(), family="nb")
    assert math.isfinite(fit.coefficient("queue_depth"))
    assert abs(fit.coefficient("queue_depth")) < 1e-8
    assert math.isfinite(fit.predict_mu((1.0, 1.0, 1.0, 1.0, 1.0)))


def test_poisson_fit_recovers_a_known_positive_relationship():
    fit = fit_glm(synthetic_rows(), family="poisson")
    assert fit.converged
    assert fit.family == "poisson" and fit.k is None
    assert fit.coefficient("lag1") > 0
    assert fit.n_train == 200


def test_nb_fit_recovers_the_same_sign_and_reports_dispersion():
    fit = fit_glm(synthetic_rows(), family="nb")
    assert fit.converged
    assert fit.family == "nb"
    assert fit.k is not None and fit.k > 0
    assert fit.coefficient("lag1") > 0


def test_fit_predicts_higher_counts_for_higher_feature_values():
    fit = fit_glm(synthetic_rows(), family="nb")
    lo = fit.predict_mu((0.0, 0, 0, 0, 0))
    hi = fit.predict_mu((9.0, 0, 0, 0, 0))
    assert hi > lo > 0


def test_ar_block_sum_is_the_joint_coefficient():
    fit = fit_glm(synthetic_rows(), family="nb")
    assert abs(
        fit.ar_block_sum - (fit.coefficient("lag1") + fit.coefficient("trail4"))
    ) < 1e-12


def test_predict_mu_is_guarded_against_overflow():
    fit = fit_glm(synthetic_rows(), family="nb")
    huge = fit.predict_mu((1e6, 1e6, 1e6, 1e6, 1e6))
    assert math.isfinite(huge)


# --- predictive distribution ------------------------------------------------


def test_poisson_pmf_matches_the_closed_form():
    d = CountPredictive.build(3.0, None)
    for j in range(8):
        want = math.exp(-3.0) * 3.0**j / math.factorial(j)
        assert abs(d.pmf[j] - want) < 1e-12


def test_nb_pmf_matches_the_closed_form():
    mu, k = 4.0, 2.5
    d = CountPredictive.build(mu, k)
    for j in range(8):
        want = (
            math.exp(
                math.lgamma(j + k) - math.lgamma(k) - math.lgamma(j + 1)
            )
            * (k / (k + mu)) ** k
            * (mu / (k + mu)) ** j
        )
        assert abs(d.pmf[j] - want) < 1e-10


def test_pmf_sums_to_one_and_mean_matches_mu():
    for k in (None, 1.5, 20.0):
        d = CountPredictive.build(6.0, k)
        assert abs(sum(d.pmf) - 1.0) < 1e-8
        assert abs(d.mean - 6.0) < 1e-4


def test_nb_is_wider_than_poisson_at_the_same_mean():
    """The reason NB is the headline (D-016): Poisson forces Var = mu, which is
    exactly the under-dispersion that sank A1's calibration."""
    p = CountPredictive.build(10.0, None)
    nb = CountPredictive.build(10.0, 3.0)
    plo, phi = p.interval(0.80)
    nlo, nhi = nb.interval(0.80)
    assert (nhi - nlo) > (phi - plo)


def test_interval_quantile_crps_and_pit_behave():
    d = CountPredictive.build(5.0, 4.0)
    lo, hi = d.interval(0.80)
    assert lo <= 5 <= hi
    assert d.cdf(hi) - d.cdf(lo - 1) >= 0.80 - 1e-9
    assert d.crps(5) < d.crps(20)          # closer truth scores better
    assert 0.0 <= d.pit(5, 0.5) <= 1.0
    assert d.quantile(0.0) == 0


# --- gradient correctness ---------------------------------------------------


def _finite_diff(fn, params, i, h=1e-6):
    up, dn = list(params), list(params)
    up[i] += h
    dn[i] -= h
    return (fn(up) - fn(dn)) / (2 * h)


def test_poisson_gradient_matches_finite_differences():
    """A wrong analytic gradient silently degrades every fit, so pin it."""
    from analysis.a2 import _design, _grad_poisson, _neg_loglik_poisson

    rows = synthetic_rows(60)
    X, y = _design(rows, Standardiser.fit(rows))
    beta = [0.4, 0.2, -0.1, 0.05, 0.3, -0.2]
    analytic = _grad_poisson(beta, X, y)
    for i in range(len(beta)):
        num = _finite_diff(lambda b: _neg_loglik_poisson(b, X, y), beta, i)
        assert abs(analytic[i] - num) < 1e-4 * max(1.0, abs(num))


def test_nb_gradient_matches_finite_differences_including_dispersion():
    from analysis.a2 import _design, _grad_nb, _neg_loglik_nb

    rows = synthetic_rows(60)
    X, y = _design(rows, Standardiser.fit(rows))
    params = [0.4, 0.2, -0.1, 0.05, 0.3, -0.2, math.log(2.5)]
    analytic = _grad_nb(params, X, y)
    for i in range(len(params)):  # includes the log-k component
        num = _finite_diff(lambda b: _neg_loglik_nb(b, X, y), params, i)
        assert abs(analytic[i] - num) < 1e-4 * max(1.0, abs(num))


def test_nb_dispersion_is_bounded_in_the_poisson_limit():
    """Near-Poisson data drives k -> inf; the bound keeps the fit well-posed."""
    from analysis.a2 import K_MAX

    fit = fit_glm(synthetic_rows(), family="nb")
    assert fit.converged
    assert 0 < fit.k <= K_MAX * (1 + 1e-9)


# ---------------------------------------------------------------------------
# Replay harness scoring (analysis.a2_replay)
# ---------------------------------------------------------------------------

from analysis.a2_replay import (  # noqa: E402
    ReplayRow,
    mondays,
    pit_histogram,
    score,
    score_horizon,
)


def mk_replay_row(*, truth, forecast, persistence=0.0, climatology=0.0,
                  lo50=0, hi50=0, lo80=0, hi80=0, crps=0.0, pit=0.5,
                  coefficients=(1.0, 1.0, 1.0, 1.0, 1.0), year_offset=0):
    return ReplayRow(
        as_of=AS_OF.replace(year=AS_OF.year + year_offset), horizon="W1",
        regime="noaa", family="nb", truth=truth, forecast=forecast, crps=crps,
        pit=pit, lo50=lo50, hi50=hi50, lo80=lo80, hi80=hi80,
        persistence=persistence, climatology=climatology, n_train=200,
        converged=True, k=3.0, coefficients=coefficients,
    )


def test_mondays_grid_is_weekly_and_inclusive():
    g = mondays(AS_OF, AS_OF + timedelta(days=21))
    assert g == [AS_OF + timedelta(days=7 * i) for i in range(4)]
    assert mondays(AS_OF, AS_OF - timedelta(days=1)) == []


def test_score_computes_mae_bias_and_null_columns():
    rows = [
        mk_replay_row(truth=10, forecast=8.0, persistence=4.0, climatology=13.0),
        mk_replay_row(truth=6, forecast=10.0, persistence=8.0, climatology=3.0),
    ]
    sc = score(rows, "x")
    assert sc.n == 2 and sc.mean_truth == 8.0 and sc.mean_forecast == 9.0
    assert abs(sc.bias - 1.0) < 1e-12
    assert abs(sc.mae - 3.0) < 1e-12
    assert abs(sc.persist_mae - 4.0) < 1e-12
    assert abs(sc.clim_mae - 3.0) < 1e-12


def test_score_coverage_and_null_flags():
    rows = [mk_replay_row(truth=5, forecast=5.0, persistence=2.0,
                          climatology=3.0, lo50=4, hi50=6, lo80=3, hi80=7)]
    sc = score(rows, "x")
    assert sc.cov50 == 1.0 and sc.cov80 == 1.0
    assert sc.beats_persistence and sc.beats_climatology


def test_score_empty_slice_is_safe():
    sc = score([], "empty")
    assert sc.n == 0 and sc.mae != sc.mae  # NaN


def test_replay_row_coefficient_lookup_matches_feature_order():
    r = mk_replay_row(truth=1, forecast=1.0, coefficients=(0.1, 0.2, 0.3, 0.4, 0.5))
    assert r.coefficient("lag1") == 0.1
    assert r.coefficient("queue_depth") == 0.5


def test_pit_histogram_bins_and_clamps():
    rows = [mk_replay_row(truth=1, forecast=1.0, pit=p)
            for p in (0.05, 0.15, 0.15, 1.0)]
    hist = pit_histogram(rows, bins=10)
    assert hist[0] == 1 and hist[1] == 2 and hist[9] == 1
    assert sum(hist) == len(rows)


# --- the walk-forward loop --------------------------------------------------


def walkforward_rows(n=160):
    """Rows whose target depends on a feature, for an end-to-end harness check."""
    rng = __import__("random").Random(11)
    out = []
    for i in range(n):
        x = float(2 + (i % 7))
        mu = math.exp(1.2 + 0.35 * (x - 5.0) / 2.0)
        out.append(Row(
            as_of=AS_OF - timedelta(days=7 * (n - i)),
            features=(x, x - 0.5, rng.random() * 2, rng.random() * 3,
                      rng.random()),
            target=max(0, int(round(mu + rng.gauss(0, 0.6)))),
        ))
    return out


def test_score_horizon_respects_the_minimum_training_window():
    """No fit until MIN_TRAIN_WEEKS purged rows exist, so early weeks are skipped."""
    rows = walkforward_rows(160)
    out = score_horizon(
        rows, horizon="W1", regime="noaa", u1=W1[1], families=("nb",)
    )
    assert len(out) < len(rows)
    assert all(r.n_train >= 104 for r in out)
    # The first scored row must be at least MIN_TRAIN_WEEKS into the grid.
    assert out[0].as_of >= rows[104].as_of


def test_score_horizon_nulls_come_from_the_row_features():
    """D-016: persistence and climatology ARE features 1 and 2 — reading them off
    the row is what stops model and baseline definitions ever diverging."""
    rows = walkforward_rows(160)
    out = score_horizon(
        rows, horizon="W1", regime="noaa", u1=W1[1], families=("nb",)
    )
    by_as_of = {r.as_of: r for r in rows}
    for rr in out:
        src = by_as_of[rr.as_of]
        assert rr.persistence == src.feature("lag1")
        assert rr.climatology == src.feature("trail4")


def test_score_horizon_emits_one_row_per_family():
    rows = walkforward_rows(160)
    out = score_horizon(
        rows, horizon="W1", regime="noaa", u1=W1[1], families=("nb", "poisson")
    )
    assert {r.family for r in out} == {"nb", "poisson"}
    n_as_of = len({r.as_of for r in out})
    assert len(out) == 2 * n_as_of


def test_score_horizon_never_trains_on_an_unclosed_target():
    """The purge, end to end: every training row used at `as_of` must have had its
    target window close on or before that date."""
    rows = walkforward_rows(130)
    out = score_horizon(
        rows, horizon="W1", regime="noaa", u1=W1[1], families=("nb",)
    )
    for rr in out:
        admissible = [
            r for r in rows
            if r.as_of + timedelta(days=W1[1]) <= rr.as_of
        ]
        assert rr.n_train == len(admissible)


def test_build_row_target_needs_the_unsliced_stream():
    """Regression for the all-zero-target bug.

    Passing a `<= as_of` slice as `target_events` empties the future window and
    silently zeroes every label — which produced a whole scorecard of MAE 0.000,
    zero coefficients and 100 % coverage before it was caught. The two streams are
    separate parameters precisely so this is visible at the call site.
    """
    past = [ev("departed", 3)]
    future = [ev("departed", -2), ev("departed", -5)]
    full = past + future

    good = build_row(AS_OF, past, full, [], [], LANE, u0=W1[0], u1=W1[1])
    assert good.feature("lag1") == 1.0
    assert good.target == 2

    # The bug: sliced stream used for the target.
    bad = build_row(AS_OF, past, past, [], [], LANE, u0=W1[0], u1=W1[1])
    assert bad.target == 0


def test_build_row_features_ignore_the_future_even_on_the_full_stream():
    """The mirror-image guarantee: feature windows look strictly backwards, so a
    full stream passed as `feature_events` cannot leak the future into them."""
    full = [ev("departed", 3), ev("departed", -2), ev("departed", -5)]
    row = build_row(AS_OF, full, full, [], [], LANE, u0=W1[0], u1=W1[1])
    assert row.feature("lag1") == 1.0        # only the past event
    assert row.target == 2                    # only the future ones
