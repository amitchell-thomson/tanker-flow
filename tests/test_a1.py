"""Unit tests for the A1 arrival-count baseline (analysis.a1).

Pure-logic: synthetic Leg lists, no DB. Step 2 = the age-conditional EU-arrival
rate `pi(a)`; see DECISIONS.md D-002 / D-007.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from analysis.a1 import (
    MATURITY_DAYS,
    MIN_OPEN_AT_AGE,
    PI_MIN_LEGS,
    PI_WINDOW_DAYS,
    build_arrival_curve,
    forecast_window,
    is_eu_arrival,
    is_open_at,
    leg_close_age_d,
    matured_population,
    open_forecast_legs,
)
from pipeline.legs import Leg
from pipeline.signal import LaneFilter

AS_OF = datetime(2024, 6, 1, tzinfo=UTC)
LANE = LaneFilter(
    export_zones=frozenset({"usgulf", "usatlantic"}),
    import_zones=frozenset({"nweurope", "baltic", "iberian", "wmed", "emed"}),
)

# Comfortably inside the 365 d rolling window and past the maturity gate.
MATURE_DEP = AS_OF - timedelta(days=MATURITY_DAYS + 30)


def mk_leg(
    *,
    mmsi=1,
    departed=None,
    duration_d=None,
    dest_zone=None,
    status=None,
    origin_zone="usgulf",
    laden=True,
    regime="noaa",
) -> Leg:
    """A leg with just the fields pi(a) reads. `duration_d=None` => never closed."""
    departed = departed or MATURE_DEP
    arrived = departed + timedelta(days=duration_d) if duration_d is not None else None
    if status is None:
        if arrived is None:
            status = "open_censored"
        elif dest_zone == origin_zone:
            status = "same_zone"
        else:
            status = "closed"
    return Leg(
        mmsi=mmsi,
        origin_terminal_id=1,
        origin_zone=origin_zone,
        departed_ts=departed,
        departed_lat=None,
        departed_lon=None,
        laden=laden,
        regime=regime,
        status=status,
        dest_zone=dest_zone,
        arrived_ts=arrived,
    )


def eu_leg(**kw):
    return mk_leg(dest_zone="nweurope", duration_d=kw.pop("duration_d", 14.0), **kw)


def gulf_return_leg(**kw):
    # The dominant real pattern: EU arrival missed, pairs with the return to load.
    return mk_leg(dest_zone="usgulf", duration_d=kw.pop("duration_d", 34.0), **kw)


def bulk(n, factory, **kw):
    return [factory(mmsi=i, **kw) for i in range(n)]


# --- primitives -------------------------------------------------------------


def test_is_eu_arrival_requires_closed_and_import_dest():
    assert is_eu_arrival(eu_leg(), LANE)
    assert not is_eu_arrival(gulf_return_leg(), LANE)
    # An open leg has no arrival, whatever its assumed destination.
    assert not is_eu_arrival(mk_leg(duration_d=None), LANE)


def test_leg_close_age_and_openness():
    lg = eu_leg(duration_d=14.0)
    assert leg_close_age_d(lg) == 14.0
    assert is_open_at(lg, 13.9)
    assert not is_open_at(lg, 14.0)  # closed AT 14 -> no longer open
    never = mk_leg(duration_d=None)
    assert leg_close_age_d(never) is None
    assert is_open_at(never, 10_000.0)


def test_same_zone_close_removes_leg_from_open_pool():
    # The D-007 correction: a same_zone return leaves the pool exactly as an EU
    # arrival does. Conflating "still open" with "not yet arrived in EU" was the
    # survival-~1 error.
    lg = gulf_return_leg(duration_d=34.0)
    assert is_open_at(lg, 20.0)
    assert not is_open_at(lg, 40.0)


# --- population selection ---------------------------------------------------


def test_matured_population_excludes_immature_legs():
    mature = eu_leg(mmsi=1, departed=MATURE_DEP)
    immature = eu_leg(mmsi=2, departed=AS_OF - timedelta(days=MATURITY_DAYS - 5))
    pop = matured_population(
        [mature, immature], AS_OF, LANE, window_days=PI_WINDOW_DAYS
    )
    assert [lg.mmsi for lg in pop] == [1]


def test_matured_population_excludes_legs_older_than_window():
    inside = eu_leg(mmsi=1, departed=MATURE_DEP)
    outside = eu_leg(
        mmsi=2,
        departed=AS_OF - timedelta(days=MATURITY_DAYS + PI_WINDOW_DAYS + 10),
    )
    pop = matured_population(
        [inside, outside], AS_OF, LANE, window_days=PI_WINDOW_DAYS
    )
    assert [lg.mmsi for lg in pop] == [1]


def test_matured_population_filters_to_laden_export_origin():
    keep = eu_leg(mmsi=1)
    ballast = eu_leg(mmsi=2, laden=False)
    eu_origin = mk_leg(mmsi=3, origin_zone="nweurope", dest_zone="usgulf",
                       duration_d=14.0)
    pop = matured_population(
        [keep, ballast, eu_origin], AS_OF, LANE, window_days=PI_WINDOW_DAYS
    )
    assert [lg.mmsi for lg in pop] == [1]


def test_matured_population_axis_filters():
    a = eu_leg(mmsi=1, origin_zone="usgulf", regime="noaa")
    b = eu_leg(mmsi=2, origin_zone="usatlantic", regime="noaa")
    c = eu_leg(mmsi=3, origin_zone="usgulf", regime="gfw")
    all_legs = [a, b, c]
    kw = dict(window_days=PI_WINDOW_DAYS)
    assert len(matured_population(all_legs, AS_OF, LANE, **kw)) == 3
    got = matured_population(all_legs, AS_OF, LANE, origin_zone="usgulf", **kw)
    assert {lg.mmsi for lg in got} == {1, 3}
    got = matured_population(all_legs, AS_OF, LANE, regime="noaa", **kw)
    assert {lg.mmsi for lg in got} == {1, 2}


# --- the curve --------------------------------------------------------------


def curve_for(legs):
    return build_arrival_curve(
        legs, AS_OF, LANE, origin_zone="usgulf", regime="noaa"
    )


def test_curve_recovers_a_known_rate():
    # 60 EU @14d + 140 Gulf-returns @34d. At age 0 all 200 are open, 60 EU => 0.30.
    legs = bulk(60, eu_leg, duration_d=14.0) + [
        gulf_return_leg(mmsi=100 + i, duration_d=34.0) for i in range(140)
    ]
    c = curve_for(legs)
    assert c.n_legs == 200
    assert abs(c.pi_at(0.0) - 0.30) < 1e-9


def test_curve_collapses_through_the_voyage_window():
    """The shape that replaced the parametric posterior.

    EU legs close at 14 d, Gulf-returns at 34 d. Before 14 d every leg is open and
    pi = the base rate; after 14 d the EU legs have gone and pi must be 0 — with
    the returns still open, which is precisely what survival-~1 got wrong.
    """
    legs = bulk(60, eu_leg, duration_d=14.0) + [
        gulf_return_leg(mmsi=100 + i, duration_d=34.0) for i in range(140)
    ]
    c = curve_for(legs)
    assert abs(c.pi_at(10.0) - 0.30) < 1e-9  # flat before the window
    assert c.pi_at(20.0) == 0.0  # EU legs gone, 140 non-EU still open
    assert c.n_open(20.0) == 140


def test_curve_is_capture_inclusive():
    # A never-closing leg (missed arrival) sits in the denominator forever, so pi
    # stays below 1 even when every *observed* close was an EU arrival.
    legs = bulk(50, eu_leg, duration_d=14.0) + [
        mk_leg(mmsi=100 + i, duration_d=None) for i in range(50)
    ]
    c = curve_for(legs)
    assert abs(c.pi_at(0.0) - 0.50) < 1e-9
    assert c.pi_at(20.0) == 0.0  # only the never-closers remain open


def test_curve_at_clamps_below_grid_and_carries_trusted_tail():
    legs = bulk(120, eu_leg, duration_d=14.0)
    c = curve_for(legs)
    assert c.pi_at(-5.0) == c.pi_at(0.0)
    # Past 14 d every leg has closed, so n_open falls under PI_MIN_AT_AGE and the
    # trusted range ends; queries beyond it carry the last trusted value.
    assert c.trusted_max_d < 20.0
    assert c.pi_at(80.0) == c.pi_at(c.trusted_max_d)


# --- the fallback ladder ----------------------------------------------------


def test_ladder_uses_narrowest_rung_when_well_supported():
    legs = bulk(PI_MIN_LEGS + 20, eu_leg)
    c = curve_for(legs)
    assert c.tier == f"usgulf/noaa/{PI_WINDOW_DAYS}d"


def test_ladder_widens_window_when_narrow_rung_is_thin():
    # Too few inside 365 d, plenty inside 730 d.
    near = bulk(10, eu_leg)
    far = [
        eu_leg(
            mmsi=500 + i,
            departed=AS_OF - timedelta(days=MATURITY_DAYS + PI_WINDOW_DAYS + 30),
        )
        for i in range(PI_MIN_LEGS + 20)
    ]
    c = curve_for(near + far)
    assert c.tier == "usgulf/noaa/730d"
    assert c.n_legs == len(near) + len(far)


def test_ladder_pools_origin_then_regime():
    # Nothing at usgulf; a healthy usatlantic/noaa population -> pooled-origin.
    other_origin = [
        eu_leg(mmsi=i, origin_zone="usatlantic") for i in range(PI_MIN_LEGS + 20)
    ]
    assert curve_for(other_origin).tier == "pooled-origin/noaa/730d"

    # Nothing in the noaa regime at all -> pooled-regime.
    other_regime = [
        eu_leg(mmsi=i, origin_zone="usatlantic", regime="gfw")
        for i in range(PI_MIN_LEGS + 20)
    ]
    assert curve_for(other_regime).tier == "pooled-origin/pooled-regime/730d"


def test_ladder_marks_unsupported_rather_than_silently_returning_a_thin_curve():
    c = curve_for(bulk(5, eu_leg))
    assert c.tier.startswith("UNSUPPORTED(")
    assert c.n_legs == 5


def test_ladder_reports_provenance_for_every_curve():
    # No silent caps: every curve carries the rung and the population behind it.
    c = curve_for(bulk(PI_MIN_LEGS + 20, eu_leg))
    assert c.tier and c.n_legs >= PI_MIN_LEGS
    assert c.n_open(0.0) >= MIN_OPEN_AT_AGE
    assert len(c.closes) == c.n_legs


# ---------------------------------------------------------------------------
# Step 3 — the arrival-time factor and the assembled per-leg probability
# ---------------------------------------------------------------------------


def test_f_eu_is_the_ecdf_over_eu_legs_only():
    # 40 EU @14d, 60 EU @20d, plus 100 Gulf-returns @34d that must NOT appear.
    legs = (
        bulk(40, eu_leg, duration_d=14.0)
        + [eu_leg(mmsi=200 + i, duration_d=20.0) for i in range(60)]
        + [gulf_return_leg(mmsi=400 + i, duration_d=34.0) for i in range(100)]
    )
    c = curve_for(legs)
    assert c.f_eu(13.0) == 0.0
    assert abs(c.f_eu(14.0) - 0.40) < 1e-9
    assert abs(c.f_eu(20.0) - 1.00) < 1e-9  # all EU legs in, returns excluded
    assert abs(c.f_eu(40.0) - 1.00) < 1e-9


def test_telescoping_identity_holds():
    """The D-009 property: pi(a) * conditional-window-mass == the direct ratio.

    Both factors come from one population, so the middle terms cancel exactly.
    This is the invariant that makes the two-factor story and the estimator the
    same thing rather than two approximations that happen to be multiplied.
    """
    legs = (
        bulk(80, eu_leg, duration_d=13.0)
        + [eu_leg(mmsi=200 + i, duration_d=17.0) for i in range(70)]
        + [gulf_return_leg(mmsi=400 + i, duration_d=34.0) for i in range(150)]
        + [mk_leg(mmsi=700 + i, duration_d=None) for i in range(40)]
    )
    c = curve_for(legs)
    for a in (0.0, 5.0, 10.0, 12.0, 14.0, 16.0):
        for u0, u1 in ((0.0, 7.0), (7.0, 14.0)):
            direct = c.arrival_probability(a, u0, u1)
            denom = 1.0 - c.f_eu(a)
            if denom <= 0:
                continue
            factored = c.pi_at(a) * (c.f_eu(a + u1) - c.f_eu(a + u0)) / denom
            assert abs(direct - factored) < 1e-9, (a, u0, u1)


def test_arrival_probability_picks_up_the_right_window():
    # All EU legs land at exactly 14 d. A leg aged 10 d has its arrival 4 d out:
    # inside W1 = (10,17], outside W2 = (17,24].
    legs = bulk(60, eu_leg, duration_d=14.0) + [
        gulf_return_leg(mmsi=100 + i, duration_d=34.0) for i in range(140)
    ]
    c = curve_for(legs)
    assert abs(c.arrival_probability(10.0, 0.0, 7.0) - 60 / 200) < 1e-9
    assert c.arrival_probability(10.0, 7.0, 14.0) == 0.0
    # Aged 8 d: arrival is 6 d out -> still W1.
    assert abs(c.arrival_probability(8.0, 0.0, 7.0) - 60 / 200) < 1e-9
    # Aged 3 d: arrival is 11 d out -> W2, not W1.
    assert c.arrival_probability(3.0, 0.0, 7.0) == 0.0
    assert abs(c.arrival_probability(3.0, 7.0, 14.0) - 60 / 200) < 1e-9


def test_arrival_probability_window_is_left_open_right_closed():
    # The convention that makes the telescoping exact: a duration landing on the
    # window's upper edge is IN, on the lower edge is OUT (that leg already
    # closed, so it is not in the forecast population at all).
    legs = bulk(100, eu_leg, duration_d=14.0) + [
        gulf_return_leg(mmsi=200 + i, duration_d=34.0) for i in range(100)
    ]
    c = curve_for(legs)
    # age 7 -> W1 = (7,14] contains 14; W2 = (14,21] does not.
    assert abs(c.arrival_probability(7.0, 0.0, 7.0) - 100 / 200) < 1e-9
    assert c.arrival_probability(7.0, 7.0, 14.0) == 0.0
    # age 14 -> W1 = (14,21] excludes a duration of exactly 14.
    assert c.arrival_probability(14.0, 0.0, 7.0) == 0.0


def test_arrival_probability_declines_beyond_support():
    legs = bulk(120, eu_leg, duration_d=14.0)
    c = curve_for(legs)
    # Past the trusted range the curve rests on <20 legs -> refuse, don't guess.
    assert c.arrival_probability(c.trusted_max_d + 1.0, 0.0, 7.0) == 0.0


def test_arrival_probability_is_zero_when_no_eu_legs():
    legs = [gulf_return_leg(mmsi=i, duration_d=34.0) for i in range(150)]
    c = curve_for(legs)
    assert c.pi_at(0.0) == 0.0
    assert c.arrival_probability(0.0, 0.0, 7.0) == 0.0
    assert c.f_eu(100.0) == 0.0  # no division by an empty EU set


# --- forecast population ----------------------------------------------------


def test_open_forecast_legs_keeps_all_open_statuses():
    # Including open_censored: the estimation denominator makes no status
    # distinction either, so filtering here would double-count the correction.
    legs = [
        mk_leg(mmsi=1, departed=AS_OF - timedelta(days=3), status="open_in_transit"),
        mk_leg(mmsi=2, departed=AS_OF - timedelta(days=40), status="open_censored"),
        mk_leg(mmsi=3, departed=AS_OF - timedelta(days=25), status="open_floating"),
        mk_leg(mmsi=4, departed=AS_OF - timedelta(days=30), status="open_arrival_gap"),
        eu_leg(mmsi=5, departed=AS_OF - timedelta(days=30)),  # closed -> excluded
    ]
    got = open_forecast_legs(legs, AS_OF, LANE)
    assert {lg.mmsi for lg in got} == {1, 2, 3, 4}


def test_open_forecast_legs_filters_direction_and_ladenness():
    legs = [
        mk_leg(mmsi=1, departed=AS_OF - timedelta(days=3), status="open_in_transit"),
        mk_leg(mmsi=2, departed=AS_OF - timedelta(days=3), status="open_in_transit",
               laden=False),
        mk_leg(mmsi=3, departed=AS_OF - timedelta(days=3), status="open_in_transit",
               origin_zone="nweurope"),
    ]
    assert {lg.mmsi for lg in open_forecast_legs(legs, AS_OF, LANE)} == {1}


# --- assembled forecast -----------------------------------------------------


def build_forecast_fixture():
    """Matured history + a handful of open legs at known ages."""
    history = (
        bulk(100, eu_leg, duration_d=14.0)
        + [gulf_return_leg(mmsi=300 + i, duration_d=34.0) for i in range(100)]
    )
    open_now = [
        mk_leg(mmsi=900 + i, departed=AS_OF - timedelta(days=age),
               status="open_in_transit")
        for i, age in enumerate((10.0, 8.0, 3.0))
    ]
    return history + open_now


def test_forecast_window_mean_and_variance():
    fs = forecast_window(build_forecast_fixture(), AS_OF, LANE, u0=0.0, u1=7.0)
    assert fs.n_legs == 3
    # ages 10 and 8 have their 14 d arrival inside (a, a+7]; age 3 does not.
    assert [round(p, 6) for p in fs.probabilities] == [0.5, 0.5, 0.0]
    assert abs(fs.expected - 1.0) < 1e-9
    assert abs(fs.variance - (0.25 + 0.25 + 0.0)) < 1e-9


def test_forecast_window_w2_picks_up_the_younger_leg():
    fs = forecast_window(build_forecast_fixture(), AS_OF, LANE, u0=7.0, u1=14.0)
    assert [round(p, 6) for p in fs.probabilities] == [0.0, 0.0, 0.5]
    assert abs(fs.expected - 0.5) < 1e-9


def test_forecast_window_reports_provenance_and_reuses_curves():
    legs = build_forecast_fixture()
    curves = {}
    fs1 = forecast_window(legs, AS_OF, LANE, u0=0.0, u1=7.0, curves=curves)
    assert fs1.tiers == (f"usgulf/noaa/{PI_WINDOW_DAYS}d",)
    assert ("usgulf", "noaa") in curves
    # Second window reuses the cached curve (same as_of => same curve).
    before = curves[("usgulf", "noaa")]
    forecast_window(legs, AS_OF, LANE, u0=7.0, u1=14.0, curves=curves)
    assert curves[("usgulf", "noaa")] is before


def test_forecast_window_counts_legs_beyond_support():
    # No silent caps: an open leg too old for its curve is counted, not hidden.
    legs = build_forecast_fixture() + [
        mk_leg(mmsi=5000, departed=AS_OF - timedelta(days=80),
               status="open_censored")
    ]
    fs = forecast_window(legs, AS_OF, LANE, u0=0.0, u1=7.0)
    assert fs.n_legs == 4
    assert fs.n_beyond_support == 1
    assert fs.probabilities[-1] == 0.0


def test_forecast_window_scores_each_leg_against_its_own_regime():
    # A gfw-regime open leg must not be scored on the noaa curve.
    legs = build_forecast_fixture() + [
        gulf_return_leg(mmsi=6000 + i, regime="gfw", duration_d=34.0)
        for i in range(150)
    ] + [
        mk_leg(mmsi=7000, departed=AS_OF - timedelta(days=10), regime="gfw",
               status="open_in_transit")
    ]
    fs = forecast_window(legs, AS_OF, LANE, u0=0.0, u1=7.0)
    assert set(fs.tiers) == {
        f"usgulf/noaa/{PI_WINDOW_DAYS}d", f"usgulf/gfw/{PI_WINDOW_DAYS}d"
    }
    # The gfw history has no EU arrivals at all -> that leg scores 0.
    assert fs.probabilities[-1] == 0.0


# ---------------------------------------------------------------------------
# Step 4 — the exact predictive distribution (Poisson-binomial)
# ---------------------------------------------------------------------------

import itertools  # noqa: E402

from analysis.a1 import Predictive, poisson_binomial_pmf  # noqa: E402


def brute_force_pmf(ps):
    """P(count == k) by enumerating all 2^n outcomes — the ground truth."""
    n = len(ps)
    out = [0.0] * (n + 1)
    for bits in itertools.product((0, 1), repeat=n):
        prob = 1.0
        for b, p in zip(bits, ps):
            prob *= p if b else (1.0 - p)
        out[sum(bits)] += prob
    return out


def test_pmf_matches_brute_force_enumeration():
    for ps in ([0.3], [0.2, 0.7], [0.1, 0.5, 0.9], [0.05, 0.25, 0.4, 0.6, 0.8]):
        got = poisson_binomial_pmf(ps)
        want = brute_force_pmf(ps)
        assert len(got) == len(want)
        for g, w in zip(got, want):
            assert abs(g - w) < 1e-12


def test_pmf_is_a_distribution():
    pmf = poisson_binomial_pmf([0.1, 0.35, 0.6, 0.85, 0.2])
    assert abs(sum(pmf) - 1.0) < 1e-12
    assert all(m >= 0.0 for m in pmf)


def test_pmf_reduces_to_binomial_for_equal_p():
    from math import comb

    n, p = 8, 0.3
    pmf = poisson_binomial_pmf([p] * n)
    for k in range(n + 1):
        assert abs(pmf[k] - comb(n, k) * p**k * (1 - p) ** (n - k)) < 1e-12


def test_pmf_drops_zero_probability_legs_exactly():
    # The optimisation that makes the DP cheap must not change the answer.
    base = [0.2, 0.5, 0.7]
    padded = [0.2, 0.0, 0.5, 0.0, 0.0, 0.7]
    assert poisson_binomial_pmf(base) == poisson_binomial_pmf(padded)


def test_pmf_empty_and_certain_cases():
    assert poisson_binomial_pmf([]) == (1.0,)  # no legs -> zero arrivals, certainly
    assert poisson_binomial_pmf([0.0, 0.0]) == (1.0,)
    pmf = poisson_binomial_pmf([1.0, 1.0])
    assert abs(pmf[2] - 1.0) < 1e-12


def test_predictive_moments_agree_with_closed_forms():
    ps = [0.1, 0.35, 0.6, 0.85, 0.2, 0.44]
    d = Predictive.from_probabilities(ps)
    assert abs(d.mean - sum(ps)) < 1e-12
    assert abs(d.variance - sum(p * (1 - p) for p in ps)) < 1e-12


def test_forecast_set_predictive_matches_its_own_moments():
    fs = forecast_window(build_forecast_fixture(), AS_OF, LANE, u0=0.0, u1=7.0)
    d = fs.predictive()
    assert abs(d.mean - fs.expected) < 1e-12
    assert abs(d.variance - fs.variance) < 1e-12


# --- cdf / quantile / interval ---------------------------------------------


def test_cdf_is_monotone_and_clamps():
    d = Predictive.from_probabilities([0.3, 0.6, 0.5])
    assert d.cdf(-1) == 0.0
    vals = [d.cdf(k) for k in range(-1, 6)]
    assert all(b >= a - 1e-12 for a, b in zip(vals, vals[1:]))
    assert abs(d.cdf(99) - 1.0) < 1e-12


def test_quantile_is_the_inverse_cdf():
    d = Predictive.from_probabilities([0.5, 0.5])  # P(0)=.25 P(1)=.5 P(2)=.25
    assert d.quantile(0.10) == 0
    assert d.quantile(0.25) == 0
    assert d.quantile(0.50) == 1
    assert d.quantile(0.80) == 2
    assert d.quantile(1.0) == 2


def test_interval_is_central_and_conservative():
    d = Predictive.from_probabilities([0.4] * 20)
    lo, hi = d.interval(0.80)
    assert lo <= d.mean <= hi
    # Discreteness => realised coverage is at least nominal, never below.
    assert d.cdf(hi) - d.cdf(lo - 1) >= 0.80 - 1e-12


def test_interval_widens_with_coverage():
    d = Predictive.from_probabilities([0.3] * 30)
    lo50, hi50 = d.interval(0.50)
    lo80, hi80 = d.interval(0.80)
    assert lo80 <= lo50 and hi80 >= hi50


# --- CRPS -------------------------------------------------------------------


def test_crps_is_zero_for_a_perfect_point_forecast():
    d = Predictive.from_probabilities([1.0, 1.0, 1.0])  # count == 3 with certainty
    assert d.crps(3) == 0.0


def test_crps_penalises_distance_from_truth():
    d = Predictive.from_probabilities([1.0, 1.0, 1.0])
    assert d.crps(4) == 1.0  # one unit of misplaced mass
    assert d.crps(5) == 2.0
    assert d.crps(2) == 1.0


def test_crps_rewards_the_better_calibrated_of_two_forecasts():
    truth = 5
    sharp_right = Predictive.from_probabilities([0.99] * 5)
    sharp_wrong = Predictive.from_probabilities([0.99] * 12)
    diffuse = Predictive.from_probabilities([0.25] * 20)
    assert sharp_right.crps(truth) < diffuse.crps(truth)
    assert diffuse.crps(truth) < sharp_wrong.crps(truth)


def test_crps_handles_observation_beyond_support():
    d = Predictive.from_probabilities([0.5, 0.5])  # support {0,1,2}
    # Beyond the support F(k)=1 while the indicator is still 0 -> +1 per k.
    assert d.crps(5) > d.crps(2)


# --- PIT --------------------------------------------------------------------


def test_pit_lies_in_unit_interval_across_the_support():
    d = Predictive.from_probabilities([0.2, 0.5, 0.7, 0.4])
    for y in range(0, 5):
        for u in (0.0, 0.5, 1.0):
            assert 0.0 - 1e-12 <= d.pit(y, u) <= 1.0 + 1e-12


def test_pit_brackets_the_observed_count():
    d = Predictive.from_probabilities([0.3, 0.6])
    y = 1
    assert abs(d.pit(y, 0.0) - d.cdf(0)) < 1e-12
    assert abs(d.pit(y, 1.0) - d.cdf(1)) < 1e-12


def test_pit_is_uniform_for_a_calibrated_forecast():
    # Draw counts from the forecast's own PMF at the deciles of each cell's mass;
    # a calibrated forecast must spread PIT values across [0,1] rather than pile up.
    d = Predictive.from_probabilities([0.4] * 10)
    vals = [d.pit(y, u) for y in range(len(d.pmf)) for u in (0.1, 0.5, 0.9)]
    assert min(vals) < 0.15 and max(vals) > 0.85
    assert all(b >= a - 1e-12 for a, b in zip(sorted(vals), sorted(vals)[1:]))


# ---------------------------------------------------------------------------
# Step 5 — truth series and nulls (D-003 / D-011)
# ---------------------------------------------------------------------------

from analysis.a1 import (  # noqa: E402
    CLIMATOLOGY_WEEKS,
    W1,
    W2,
    arrivals_in_window,
    climatology_null,
    persistence_null,
    realised_arrivals,
)


def arrived_leg(*, mmsi, departed_days_before, duration_d, dest_zone="nweurope",
                regime="noaa", origin_zone="usgulf", laden=True):
    """A closed leg that departed `departed_days_before` days before AS_OF."""
    return mk_leg(
        mmsi=mmsi,
        departed=AS_OF - timedelta(days=departed_days_before),
        duration_d=duration_d,
        dest_zone=dest_zone,
        regime=regime,
        origin_zone=origin_zone,
        laden=laden,
    )


def test_realised_counts_arrivals_inside_the_window():
    legs = [
        # departed 10d ago, 14d voyage -> arrives AS_OF+4d  => W1
        arrived_leg(mmsi=1, departed_days_before=10, duration_d=14.0),
        # departed 5d ago, 15d voyage -> arrives AS_OF+10d  => W2
        arrived_leg(mmsi=2, departed_days_before=5, duration_d=15.0),
        # departed 20d ago, 14d voyage -> arrived AS_OF-6d  => neither
        arrived_leg(mmsi=3, departed_days_before=20, duration_d=14.0),
    ]
    assert realised_arrivals(legs, AS_OF, LANE, u0=W1[0], u1=W1[1]) == 1
    assert realised_arrivals(legs, AS_OF, LANE, u0=W2[0], u1=W2[1]) == 1


def test_realised_excludes_legs_that_departed_after_as_of():
    """The conditioning that makes truth match the forecast population.

    A vessel that departs after `as_of` and arrives inside the window was never
    visible to A1 — counting it would score A1 against a target it was
    structurally denied. (This bites for W2: 1,200 of 3,200 real US->EU legs
    arrive within 14 d, so such legs genuinely exist.)
    """
    future = mk_leg(
        mmsi=1,
        departed=AS_OF + timedelta(days=1),
        duration_d=8.0,  # arrives AS_OF+9d, inside W2
        dest_zone="nweurope",
    )
    assert realised_arrivals([future], AS_OF, LANE, u0=W2[0], u1=W2[1]) == 0
    # ...but it IS counted from an as_of after it departed.
    later = AS_OF + timedelta(days=2)
    assert realised_arrivals([future], later, LANE, u0=W1[0], u1=W1[1]) == 1


def test_realised_counts_only_eu_arrivals_from_laden_us_legs():
    legs = [
        arrived_leg(mmsi=1, departed_days_before=10, duration_d=14.0),
        # Gulf return: closes in the window but is not an EU arrival
        arrived_leg(mmsi=2, departed_days_before=10, duration_d=14.0,
                    dest_zone="usgulf"),
        # ballast
        arrived_leg(mmsi=3, departed_days_before=10, duration_d=14.0, laden=False),
        # EU-origin leg
        arrived_leg(mmsi=4, departed_days_before=10, duration_d=14.0,
                    origin_zone="nweurope", dest_zone="usgulf"),
    ]
    assert realised_arrivals(legs, AS_OF, LANE, u0=W1[0], u1=W1[1]) == 1


def test_realised_uses_the_same_window_convention_as_the_forecast():
    # (lo, hi]: an arrival exactly at as_of+7d belongs to W1, not W2.
    exact = arrived_leg(mmsi=1, departed_days_before=7, duration_d=14.0)
    assert exact.arrived_ts == AS_OF + timedelta(days=7)
    assert realised_arrivals([exact], AS_OF, LANE, u0=W1[0], u1=W1[1]) == 1
    assert realised_arrivals([exact], AS_OF, LANE, u0=W2[0], u1=W2[1]) == 0


def test_realised_filters_by_departure_regime():
    legs = [
        arrived_leg(mmsi=1, departed_days_before=10, duration_d=14.0, regime="noaa"),
        arrived_leg(mmsi=2, departed_days_before=10, duration_d=14.0, regime="gfw"),
    ]
    assert realised_arrivals(legs, AS_OF, LANE, u0=W1[0], u1=W1[1]) == 2
    assert realised_arrivals(
        legs, AS_OF, LANE, u0=W1[0], u1=W1[1], regime="noaa"
    ) == 1


def test_arrivals_in_window_is_half_open_on_the_left():
    lo = AS_OF
    hi = AS_OF + timedelta(days=7)
    at_lo = arrived_leg(mmsi=1, departed_days_before=14, duration_d=14.0)
    assert at_lo.arrived_ts == lo
    assert arrivals_in_window(
        [at_lo], LANE, lo=lo, hi=hi, departed_by=AS_OF
    ) == 0


# --- nulls ------------------------------------------------------------------


def test_persistence_is_the_last_fully_elapsed_week():
    legs = [
        # arrives AS_OF-3d  => inside (AS_OF-7d, AS_OF]
        arrived_leg(mmsi=1, departed_days_before=17, duration_d=14.0),
        arrived_leg(mmsi=2, departed_days_before=18, duration_d=14.0),
        # arrives AS_OF-10d => the week before that
        arrived_leg(mmsi=3, departed_days_before=24, duration_d=14.0),
        # arrives AS_OF+4d  => the future; a null may never see it
        arrived_leg(mmsi=4, departed_days_before=10, duration_d=14.0),
    ]
    assert persistence_null(legs, AS_OF, LANE) == 2.0


def test_persistence_never_uses_future_information():
    """The D-011 amendment: D-003's literal wording leaks for W2.

    Only arrivals strictly at or before `as_of` may inform a null, so a forecast
    that beats it has beaten something a real observer could have produced.
    """
    future_only = [arrived_leg(mmsi=1, departed_days_before=1, duration_d=10.0)]
    assert future_only[0].arrived_ts > AS_OF
    assert persistence_null(future_only, AS_OF, LANE) == 0.0
    assert climatology_null(future_only, AS_OF, LANE) == 0.0


def test_climatology_is_the_trailing_four_week_mean():
    # One arrival in each of the four elapsed weeks -> mean 1.0
    legs = [
        arrived_leg(mmsi=w, departed_days_before=14 + 7 * (w - 1) + 3,
                    duration_d=14.0)
        for w in range(1, CLIMATOLOGY_WEEKS + 1)
    ]
    assert climatology_null(legs, AS_OF, LANE) == 1.0
    # Four arrivals all in the most recent week -> mean 1.0 too, but persistence 4.
    recent = [
        arrived_leg(mmsi=100 + i, departed_days_before=17, duration_d=14.0)
        for i in range(4)
    ]
    assert climatology_null(recent, AS_OF, LANE) == 1.0
    assert persistence_null(recent, AS_OF, LANE) == 4.0


def test_climatology_windows_do_not_overlap_or_gap():
    # One arrival per elapsed week for 8 weeks; a 4-week mean must see exactly 4.
    legs = [
        arrived_leg(mmsi=w, departed_days_before=14 + 7 * (w - 1) + 3,
                    duration_d=14.0)
        for w in range(1, 9)
    ]
    assert climatology_null(legs, AS_OF, LANE, weeks=4) == 1.0
    assert climatology_null(legs, AS_OF, LANE, weeks=8) == 1.0
    assert climatology_null(legs, AS_OF, LANE, weeks=1) == 1.0


def test_nulls_respect_regime_filtering():
    legs = [
        arrived_leg(mmsi=1, departed_days_before=17, duration_d=14.0, regime="noaa"),
        arrived_leg(mmsi=2, departed_days_before=17, duration_d=14.0, regime="gfw"),
    ]
    assert persistence_null(legs, AS_OF, LANE) == 2.0
    assert persistence_null(legs, AS_OF, LANE, regime="noaa") == 1.0


# ---------------------------------------------------------------------------
# Step 6 — replay harness scoring (analysis.a1_replay)
# ---------------------------------------------------------------------------

from analysis.a1_replay import (  # noqa: E402
    ReplayRow,
    mondays,
    pit_histogram,
    score,
)


def mk_row(*, truth, forecast, persistence=0.0, climatology=0.0, lo50=0, hi50=0,
           lo80=0, hi80=0, crps=0.0, pit=0.5, horizon="W1", regime="noaa",
           as_of=None, supported=True, n_open=10, n_beyond=0):
    return ReplayRow(
        as_of=as_of or AS_OF, horizon=horizon, regime=regime, truth=truth,
        forecast=forecast, crps=crps, pit=pit, lo50=lo50, hi50=hi50,
        lo80=lo80, hi80=hi80, persistence=persistence, climatology=climatology,
        n_open=n_open, n_beyond_support=n_beyond, supported=supported,
    )


def test_mondays_grid_is_weekly_and_inclusive():
    g = mondays(AS_OF, AS_OF + timedelta(days=21))
    assert g == [AS_OF + timedelta(days=7 * i) for i in range(4)]
    assert mondays(AS_OF, AS_OF - timedelta(days=1)) == []


def test_score_computes_mae_rmse_and_null_columns():
    rows = [
        mk_row(truth=10, forecast=8.0, persistence=4.0, climatology=13.0),
        mk_row(truth=6, forecast=10.0, persistence=8.0, climatology=3.0),
    ]
    sc = score(rows, "x")
    assert sc.n == 2
    assert sc.mean_truth == 8.0
    assert sc.mean_forecast == 9.0
    assert abs(sc.bias - 1.0) < 1e-12
    assert abs(sc.a1_mae - 3.0) < 1e-12  # |8-10|=2, |10-6|=4
    assert abs(sc.a1_rmse - ((4 + 16) / 2) ** 0.5) < 1e-12
    assert abs(sc.persist_mae - 4.0) < 1e-12  # |4-10|=6, |8-6|=2
    assert abs(sc.clim_mae - 3.0) < 1e-12  # |13-10|=3, |3-6|=3


def test_score_flags_which_nulls_are_beaten():
    beats = [mk_row(truth=10, forecast=10.0, persistence=2.0, climatology=3.0)]
    assert score(beats, "x").beats_persistence
    assert score(beats, "x").beats_climatology
    loses = [mk_row(truth=10, forecast=1.0, persistence=10.0, climatology=10.0)]
    assert not score(loses, "x").beats_persistence
    assert not score(loses, "x").beats_climatology


def test_score_coverage_counts_interval_hits():
    rows = [
        mk_row(truth=5, forecast=5.0, lo50=4, hi50=6, lo80=3, hi80=7),  # in both
        mk_row(truth=9, forecast=5.0, lo50=4, hi50=6, lo80=3, hi80=7),  # in neither
        mk_row(truth=7, forecast=5.0, lo50=4, hi50=6, lo80=3, hi80=7),  # 80 only
    ]
    sc = score(rows, "x")
    assert abs(sc.cov50 - 1 / 3) < 1e-12
    assert abs(sc.cov80 - 2 / 3) < 1e-12


def test_score_interval_bounds_are_inclusive():
    edge = [mk_row(truth=7, forecast=5.0, lo50=4, hi50=7, lo80=3, hi80=7)]
    assert score(edge, "x").cov50 == 1.0


def test_score_empty_slice_is_safe():
    sc = score([], "empty")
    assert sc.n == 0
    assert sc.a1_mae != sc.a1_mae  # NaN


def test_pit_histogram_bins_the_unit_interval():
    rows = [mk_row(truth=1, forecast=1.0, pit=p)
            for p in (0.05, 0.15, 0.15, 0.95, 1.0)]
    hist = pit_histogram(rows, bins=10)
    assert hist[0] == 1
    assert hist[1] == 2
    assert hist[9] == 2  # 0.95 and the clamped 1.0
    assert sum(hist) == len(rows)


def test_replay_row_year_and_membership_helpers():
    r = mk_row(truth=5, forecast=5.0, lo50=5, hi50=5, lo80=4, hi80=6)
    assert r.year == AS_OF.year
    assert r.in_50 and r.in_80
    r2 = mk_row(truth=99, forecast=5.0, lo50=4, hi50=6, lo80=3, hi80=7)
    assert not r2.in_50 and not r2.in_80
