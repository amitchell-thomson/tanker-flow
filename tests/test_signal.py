"""Unit tests for the signal aggregation layer (pipeline.signal).

Pure-logic: synthetic Leg / Visit objects, no DB. Mirrors tests/test_legs.py.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from config import REGIME_CUTOVER
from pipeline.legs import Leg
from pipeline.signal import (
    UNKNOWN_BAND,
    LaneFilter,
    accumulate_daily,
    amortized_cargo_contribution,
    ballast_dest_band,
    ballast_to_us_legs,
    daily_buckets,
    discharging_eu_visits,
    items_live_on,
    lane_legs,
    leg_interval,
    loading_us_visits,
    terminal_dwell_hours,
    transit_dest_band,
    visit_berth_interval,
    visit_interval,
    visit_terminal_band,
)
from pipeline.visits import Visit


# A reference "now" well after the regime cutover, matching test_legs.py.
NOW = REGIME_CUTOVER + timedelta(days=40)

SABINE = (29.74, -93.87)  # usgulf export
ROTTERDAM = (52.00, 4.00)  # nweurope import

LANE = LaneFilter(
    export_zones=frozenset({"usgulf", "usatlantic"}),
    import_zones=frozenset({"nweurope", "baltic", "iberian", "wmed", "emed"}),
)


def at(days: float):
    return REGIME_CUTOVER - timedelta(days=20) + timedelta(days=days)


def mk_leg(
    *,
    status,
    departed_ts=at(0),
    origin_zone="usgulf",
    laden=True,
    regime="mmsi_filter",
    mmsi=1,
    gas_capacity_m3=170_000,
    **kw,
) -> Leg:
    return Leg(
        mmsi=mmsi,
        origin_terminal_id=1,
        origin_zone=origin_zone,
        departed_ts=departed_ts,
        departed_lat=SABINE[0],
        departed_lon=SABINE[1],
        laden=laden,
        regime=regime,
        status=status,
        gas_capacity_m3=gas_capacity_m3,
        **kw,
    )


def mk_visit(
    *,
    moored_ts=at(0),
    departed_ts=None,
    flow_direction="import",
    zone="nweurope",
    terminal_id=10,
    laden=True,
    regime="mmsi_filter",
    mmsi=1,
    gas_capacity_m3=170_000,
) -> Visit:
    return Visit(
        mmsi=mmsi,
        terminal_id=terminal_id,
        zone=zone,
        flow_direction=flow_direction,
        moored_ts=moored_ts,
        departed_ts=departed_ts,
        laden=laden,
        regime=regime,
        gas_capacity_m3=gas_capacity_m3,
    )


def index(rows) -> dict:
    return {
        (r.signal_key, r.bucket_date, r.zone_scope, r.regime): r.value for r in rows
    }


# --- item selection -----------------------------------------------------------


def test_lane_legs_in_transit_base():
    legs = [
        mk_leg(status="same_zone", dest_zone="usgulf"),
        mk_leg(status="open_censored"),
        mk_leg(status="open_floating"),
        mk_leg(status="closed", dest_zone="nweurope"),  # kept
        mk_leg(status="open_in_transit"),  # kept
        mk_leg(status="closed", dest_zone="nweurope", laden=False),  # ballast excluded
        mk_leg(status="closed", dest_zone="usatlantic"),  # export→export excluded
    ]
    base = lane_legs(legs, LANE)
    assert {lg.status for lg in base} == {"closed", "open_in_transit"}
    assert len(base) == 2


def test_ballast_to_us_legs():
    legs = [
        # EU → US, empty, arrived: kept
        mk_leg(status="closed", origin_zone="nweurope", dest_zone="usgulf", laden=False),
        # EU departed, still at sea, empty: kept
        mk_leg(status="open_in_transit", origin_zone="nweurope", laden=False),
        # laden EU→US (would be odd) excluded — only ballast returns
        mk_leg(status="closed", origin_zone="nweurope", dest_zone="usgulf", laden=True),
        # US→EU laden (the in-transit base, not ballast) excluded
        mk_leg(status="closed", origin_zone="usgulf", dest_zone="nweurope"),
        # EU→EU empty hop excluded (dest not an export zone)
        mk_leg(status="closed", origin_zone="nweurope", dest_zone="baltic", laden=False),
    ]
    base = ballast_to_us_legs(legs, LANE)
    assert len(base) == 2
    assert {lg.status for lg in base} == {"closed", "open_in_transit"}


def test_discharging_and_loading_visit_selection():
    visits = [
        mk_visit(flow_direction="import", laden=True),  # discharging: kept
        mk_visit(flow_direction="import", laden=False),  # ballast at import: dropped
        mk_visit(flow_direction="export", laden=False, zone="usgulf", terminal_id=1),
        mk_visit(flow_direction=None, terminal_id=99),  # no flow: dropped from both
    ]
    disch = discharging_eu_visits(visits)
    assert len(disch) == 1 and disch[0].flow_direction == "import"
    load = loading_us_visits(visits)
    assert len(load) == 1 and load[0].flow_direction == "export"


# --- band assignment ----------------------------------------------------------


def test_transit_dest_band():
    assert transit_dest_band(mk_leg(status="closed", dest_zone="wmed"), LANE) == "wmed"
    assert (
        transit_dest_band(mk_leg(status="open_in_transit", dest_region="baltic"), LANE)
        == "baltic"
    )
    # Undeclared open leg → its own 'unknown' band (not folded into a fallback).
    assert transit_dest_band(mk_leg(status="open_in_transit"), LANE) == UNKNOWN_BAND
    # A laden in-transit leg whose declared dest is an EXPORT zone (the master
    # already set the next load port) is NOT banded usgulf — it's 'unknown'.
    assert (
        transit_dest_band(mk_leg(status="open_in_transit", dest_region="usgulf"), LANE)
        == UNKNOWN_BAND
    )


def test_ballast_dest_band():
    # Ballast return: an export-zone declared dest is trusted; an import-zone one
    # (stale) is not → 'unknown'.
    assert (
        ballast_dest_band(
            mk_leg(status="open_in_transit", laden=False, dest_region="usgulf"), LANE
        )
        == "usgulf"
    )
    assert (
        ballast_dest_band(
            mk_leg(status="open_in_transit", laden=False, dest_region="nweurope"), LANE
        )
        == UNKNOWN_BAND
    )
    assert (
        ballast_dest_band(
            mk_leg(status="closed", laden=False, dest_zone="usgulf"), LANE
        )
        == "usgulf"
    )


def test_visit_terminal_band():
    assert visit_terminal_band(mk_visit(terminal_id=10)) == "10"


# --- live intervals -----------------------------------------------------------


def test_leg_interval_half_open_on_arrival():
    leg = mk_leg(status="closed", departed_ts=at(0), arrived_ts=at(3), dest_zone="nweurope")
    start, end_excl = leg_interval(leg, at(10).date())
    assert (start, end_excl) == (at(0).date(), at(3).date())  # not live on arrival day


def test_leg_interval_open_runs_to_panel_end():
    leg = mk_leg(status="open_in_transit", departed_ts=at(0))
    _, end_excl = leg_interval(leg, at(5).date())
    assert end_excl == at(5).date() + timedelta(days=1)


def test_visit_interval_floors_to_mooring_day():
    # Same-day load: mooring day still counts (floor of one day).
    v = mk_visit(moored_ts=at(0), departed_ts=at(0) + timedelta(hours=6))
    start, end_excl = visit_interval(v, at(10).date())
    assert (start, end_excl) == (at(0).date(), at(0).date() + timedelta(days=1))


def test_visit_interval_open_runs_to_panel_end():
    # Open visit within the dwell ceiling runs through the panel end.
    v = mk_visit(moored_ts=at(0), departed_ts=None)
    _, end_excl = visit_interval(v, at(3).date())
    assert end_excl == at(3).date() + timedelta(days=1)


def test_visit_interval_open_capped_at_ceiling():
    # A `moored` with no observed `departed`, panel ending far later: the visit is
    # a missed-departure phantom and stops contributing after the dwell ceiling.
    from pipeline.signal import OPEN_VISIT_CEILING_DAYS

    v = mk_visit(moored_ts=at(0), departed_ts=None)
    _, end_excl = visit_interval(v, at(40).date())
    assert end_excl == at(0).date() + timedelta(days=OPEN_VISIT_CEILING_DAYS)


def test_items_live_on():
    legs = [
        mk_leg(status="closed", departed_ts=at(0), arrived_ts=at(3), dest_zone="nweurope"),
        mk_leg(status="open_in_transit", departed_ts=at(1)),
    ]
    live = items_live_on(legs, at(2).date(), leg_interval)
    assert sorted(lg.status for lg in live) == ["closed", "open_in_transit"]
    # Arrival day: closed leg no longer live (half-open); open one still is.
    live3 = items_live_on(legs, at(3).date(), leg_interval)
    assert [lg.status for lg in live3] == ["open_in_transit"]


# --- stacked daily reconstruction --------------------------------------------


def test_accumulate_daily_stacks_by_band():
    # Two open in-transit legs to different zones → two stacked bands.
    legs = [
        mk_leg(status="open_in_transit", departed_ts=at(0), dest_region="nweurope", gas_capacity_m3=170_000),
        mk_leg(status="open_in_transit", departed_ts=at(0), dest_region="wmed", gas_capacity_m3=140_000, mmsi=2),
    ]
    days = daily_buckets(at(0).date(), at(2).date())
    rows = accumulate_daily(
        legs, days, signal_key="gas_in_transit_volume",
        interval_of=leg_interval, band_of=lambda lg: transit_dest_band(lg, LANE),
    )
    by = index(rows)
    d = at(2).date()
    assert by[("gas_in_transit_volume", d, "nweurope", "all")] == 170_000
    assert by[("gas_in_transit_volume", d, "wmed", "all")] == 140_000


def test_accumulate_daily_closed_leg_stops_at_arrival():
    leg = mk_leg(status="closed", departed_ts=at(0), arrived_ts=at(2), dest_zone="nweurope", gas_capacity_m3=170_000)
    days = daily_buckets(at(0).date(), at(3).date())
    rows = accumulate_daily(
        [leg], days, signal_key="g", interval_of=leg_interval, band_of=lambda lg: transit_dest_band(lg, LANE),
    )
    by = index(rows)
    assert by[("g", at(0).date(), "nweurope", "all")] == 170_000
    assert by[("g", at(1).date(), "nweurope", "all")] == 170_000
    assert ("g", at(2).date(), "nweurope", "all") not in by  # arrival day excluded


def test_accumulate_daily_visit_in_berth_stock():
    # An open visit (in berth) contributes its gas to its terminal band every day.
    v = mk_visit(moored_ts=at(0), departed_ts=None, terminal_id=10, gas_capacity_m3=160_000)
    days = daily_buckets(at(0).date(), at(2).date())
    rows = accumulate_daily(
        [v], days, signal_key="gas_discharging_eu",
        interval_of=visit_interval, band_of=visit_terminal_band,
    )
    by = index(rows)
    for d in days:
        assert by[("gas_discharging_eu", d, "10", "all")] == 160_000


def test_accumulate_daily_null_gas_skipped():
    leg = mk_leg(status="open_in_transit", departed_ts=at(0), gas_capacity_m3=None)
    rows = accumulate_daily(
        [leg], daily_buckets(at(0).date(), at(2).date()),
        signal_key="g", interval_of=leg_interval, band_of=lambda lg: transit_dest_band(lg, LANE),
    )
    assert rows == []


def test_accumulate_daily_regime_split_at_seam():
    # A bbox-departed leg still live AFTER the cutover stays 'bbox' on every day.
    leg = mk_leg(
        status="open_in_transit",
        departed_ts=REGIME_CUTOVER - timedelta(days=2),
        regime="bbox",
        dest_region="nweurope",
        gas_capacity_m3=170_000,
    )
    days = daily_buckets(
        (REGIME_CUTOVER - timedelta(days=2)).date(),
        (REGIME_CUTOVER + timedelta(days=3)).date(),
    )
    rows = accumulate_daily(
        [leg], days, signal_key="g", interval_of=leg_interval, band_of=lambda lg: transit_dest_band(lg, LANE),
    )
    assert {r.regime for r in rows} == {"bbox", "all"}
    by = index(rows)
    post = (REGIME_CUTOVER + timedelta(days=3)).date()
    assert by[("g", post, "nweurope", "bbox")] == by[("g", post, "nweurope", "all")]


# --- berth signals as amortized flow ------------------------------------------


def test_visit_berth_interval_includes_departure_day():
    # Unlike visit_interval (half-open on departure), the amortized-flow interval
    # includes the departure day, which carries real berth hours.
    v = mk_visit(moored_ts=at(0), departed_ts=at(2))
    start, end_excl = visit_berth_interval(v, at(10).date())
    assert start == at(0).date()
    assert end_excl == at(2).date() + timedelta(days=1)


def test_terminal_dwell_hours_mean_and_fallback():
    visits = [
        mk_visit(terminal_id=10, moored_ts=at(0), departed_ts=at(1)),    # 24h
        mk_visit(terminal_id=10, moored_ts=at(2), departed_ts=at(2.5)),  # 12h
        mk_visit(terminal_id=11, moored_ts=at(0), departed_ts=None),     # open → ignored
    ]
    means, global_mean = terminal_dwell_hours(visits)
    assert means[10] == pytest.approx(18.0)  # (24 + 12) / 2
    assert 11 not in means                   # only-open terminal has no observed mean
    assert global_mean == pytest.approx(18.0)


def test_amortized_closed_visit_integrates_to_one_cargo():
    # A closed visit spanning several days deposits its cargo exactly once.
    v = mk_visit(terminal_id=10, moored_ts=at(0), departed_ts=at(2), gas_capacity_m3=170_000)
    days = daily_buckets(at(0).date(), at(10).date())
    rows = accumulate_daily(
        [v], days, signal_key="gas_loading_us",
        interval_of=visit_berth_interval, band_of=visit_terminal_band,
        contribution=amortized_cargo_contribution({}, 24.0, NOW),
    )
    total = sum(r.value for r in rows if r.regime == "all")
    assert total == pytest.approx(170_000)


def test_amortized_midnight_straddle_splits_not_doubles():
    # A 2h visit straddling midnight splits its cargo across the two days rather
    # than registering full capacity on both (the old in-berth stock bug).
    moored = datetime(2026, 6, 1, 23, 0, tzinfo=timezone.utc)
    departed = datetime(2026, 6, 2, 1, 0, tzinfo=timezone.utc)
    v = mk_visit(terminal_id=10, moored_ts=moored, departed_ts=departed, gas_capacity_m3=170_000)
    days = daily_buckets(moored.date(), departed.date() + timedelta(days=2))
    rows = accumulate_daily(
        [v], days, signal_key="gas_loading_us",
        interval_of=visit_berth_interval, band_of=visit_terminal_band,
        contribution=amortized_cargo_contribution({}, 24.0, NOW),
    )
    by = index(rows)
    assert by[("gas_loading_us", moored.date(), "10", "all")] == pytest.approx(85_000)
    assert by[("gas_loading_us", departed.date(), "10", "all")] == pytest.approx(85_000)


def test_amortized_open_visit_estimates_dwell_and_caps_at_one_cargo():
    # An open visit estimates total dwell from the terminal mean and never
    # deposits more than one cargo, even after lingering past the estimate.
    moored = NOW - timedelta(days=3)
    v = mk_visit(terminal_id=10, moored_ts=moored, departed_ts=None, gas_capacity_m3=170_000)
    days = daily_buckets(moored.date(), NOW.date())
    rows = accumulate_daily(
        [v], days, signal_key="gas_loading_us",
        interval_of=visit_berth_interval, band_of=visit_terminal_band,
        contribution=amortized_cargo_contribution({10: 24.0}, 24.0, NOW),
    )
    total = sum(r.value for r in rows if r.regime == "all")
    assert total == pytest.approx(170_000)  # capped at exactly one cargo
    active_days = sum(1 for r in rows if r.regime == "all" and r.value > 1e-6)
    assert active_days <= 2  # ~24h of estimated dwell spans at most 2 calendar days
