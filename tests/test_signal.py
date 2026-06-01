"""Unit tests for the signal aggregation layer (pipeline.signal).

Pure-logic: synthetic Leg / EventCount objects, no DB. Mirrors tests/test_legs.py.
"""

from __future__ import annotations

from datetime import timedelta

from config import REGIME_CUTOVER
from pipeline.legs import Leg
from pipeline.signal import (
    EventCount,
    LaneFilter,
    count_events_daily,
    daily_buckets,
    lane_legs,
    leg_distance_nm,
    od_matrix,
    reconstruct_ton_miles,
    reconstruct_voyage_age,
)


# A reference "now" well after the regime cutover, matching test_legs.py.
NOW = REGIME_CUTOVER + timedelta(days=40)

SABINE = (29.74, -93.87)  # usgulf export
ROTTERDAM = (52.00, 4.00)  # nweurope import centroid (test stand-in)

LANE = LaneFilter(
    export_zones=frozenset({"usgulf", "usatlantic"}),
    import_zones=frozenset({"nweurope", "baltic", "iberian", "wmed", "emed"}),
)
CENTROIDS = {"nweurope": ROTTERDAM}


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
    departed_lat=SABINE[0],
    departed_lon=SABINE[1],
    **kw,
) -> Leg:
    return Leg(
        mmsi=mmsi,
        origin_terminal_id=1,
        origin_zone=origin_zone,
        departed_ts=departed_ts,
        departed_lat=departed_lat,
        departed_lon=departed_lon,
        laden=laden,
        regime=regime,
        status=status,
        **kw,
    )


def index(rows) -> dict:
    return {
        (r.signal_key, r.bucket_date, r.zone_scope, r.regime): r.value for r in rows
    }


# --- in-transit ton-miles stock (#1/#2) ---------------------------------------


def test_ton_miles_single_closed_leg():
    # Closed leg: live on [departed, arrived), zero on/after the arrival day.
    leg = mk_leg(
        status="closed",
        departed_ts=at(0),
        arrived_ts=at(3),
        dest_zone="nweurope",
        dwt=90_000,
        distance_nm=4000.0,
    )
    days = daily_buckets(at(0).date(), at(3).date())
    rows = reconstruct_ton_miles(
        [leg], days, weight_attr="dwt", signal_key="tm", import_centroids={}
    )
    by = index(rows)
    dep, arr = at(0).date(), at(3).date()
    assert by[("tm", dep, "usgulf->eu", "all")] == 90_000 * 4000.0
    # 3 live days (dep, +1, +2) × 2 regimes (leg regime + 'all'); none on arrival day.
    assert ("tm", arr, "usgulf->eu", "all") not in by
    assert len(rows) == 6


def test_open_in_transit_runs_to_as_of():
    # Open leg has no observed arrival → distance estimated origin→dest centroid,
    # and it contributes on every day through the panel end (as_of).
    leg = mk_leg(
        status="open_in_transit",
        departed_ts=NOW - timedelta(days=10),
        dest_region="nweurope",
        dwt=80_000,
    )
    days = daily_buckets((NOW - timedelta(days=10)).date(), NOW.date())
    rows = reconstruct_ton_miles(
        [leg], days, weight_attr="dwt", signal_key="tm", import_centroids=CENTROIDS
    )
    by = index(rows)
    assert ("tm", NOW.date(), "usgulf->eu", "all") in by
    # Sabine → Rotterdam ~4500 nm × 80k dwt; sanity-bound and constant across days.
    v = by[("tm", NOW.date(), "usgulf->eu", "all")]
    assert 80_000 * 4000 < v < 80_000 * 5000
    assert by[("tm", (NOW - timedelta(days=5)).date(), "usgulf->eu", "all")] == v


def test_unknown_dest_open_leg_uses_fallback():
    # No declared destination → falls back to FALLBACK_DEST_ZONE (nweurope) so the
    # leg still contributes, at the same distance as if nweurope were declared.
    leg = mk_leg(status="open_in_transit", dest_region=None)
    declared = mk_leg(status="open_in_transit", dest_region="nweurope")
    assert leg_distance_nm(leg, CENTROIDS) == leg_distance_nm(declared, CENTROIDS)
    # Disabling the fallback drops the unknown-dest leg back to None.
    assert leg_distance_nm(leg, CENTROIDS, fallback_zone=None) is None
    # The declared leg is unaffected by the fallback toggle.
    assert leg_distance_nm(declared, CENTROIDS, fallback_zone=None) is not None


def test_fallback_skipped_without_departure_position():
    # Even with the fallback, a leg with no departure position can't be estimated.
    leg = mk_leg(
        status="open_in_transit", dest_region=None, departed_lat=None, departed_lon=None
    )
    assert leg_distance_nm(leg, CENTROIDS) is None


def test_null_distance_skipped():
    leg = mk_leg(
        status="closed",
        departed_ts=at(0),
        arrived_ts=at(3),
        dest_zone="nweurope",
        dwt=90_000,
        distance_nm=None,
    )
    days = daily_buckets(at(0).date(), at(3).date())
    rows = reconstruct_ton_miles(
        [leg], days, weight_attr="dwt", signal_key="tm", import_centroids={}
    )
    assert rows == []


def test_null_weight_skipped():
    leg = mk_leg(
        status="closed",
        departed_ts=at(0),
        arrived_ts=at(3),
        dest_zone="nweurope",
        dwt=90_000,
        gas_capacity_m3=None,
        distance_nm=4000.0,
    )
    days = daily_buckets(at(0).date(), at(3).date())
    assert (
        reconstruct_ton_miles(
            [leg],
            days,
            weight_attr="gas_capacity_m3",
            signal_key="tm_gas",
            import_centroids={},
        )
        == []
    )
    assert reconstruct_ton_miles(
        [leg], days, weight_attr="dwt", signal_key="tm_dwt", import_centroids={}
    )


def test_regime_split_at_seam():
    # A bbox-departed leg still live AFTER the cutover stays attributed to 'bbox'
    # on every day — segmentation is by the leg's regime, not the bucket date.
    leg = mk_leg(
        status="open_in_transit",
        departed_ts=REGIME_CUTOVER - timedelta(days=2),
        regime="bbox",
        dest_region="nweurope",
        dwt=70_000,
    )
    days = daily_buckets(
        (REGIME_CUTOVER - timedelta(days=2)).date(),
        (REGIME_CUTOVER + timedelta(days=3)).date(),
    )
    rows = reconstruct_ton_miles(
        [leg], days, weight_attr="dwt", signal_key="tm", import_centroids=CENTROIDS
    )
    assert {r.regime for r in rows} == {"bbox", "all"}
    by = index(rows)
    post = (REGIME_CUTOVER + timedelta(days=3)).date()
    assert (
        by[("tm", post, "usgulf->eu", "bbox")] == by[("tm", post, "usgulf->eu", "all")]
    )


# --- lane filtering ------------------------------------------------------------


def test_lane_legs_exclusions():
    legs = [
        mk_leg(status="same_zone", dest_zone="usgulf"),
        mk_leg(status="open_censored"),
        mk_leg(status="open_floating"),
        mk_leg(status="open_arrival_gap"),
        mk_leg(status="closed", dest_zone="nweurope"),  # kept
        mk_leg(status="open_in_transit"),  # kept
        mk_leg(status="closed", dest_zone="nweurope", laden=False),  # ballast excluded
        mk_leg(status="closed", dest_zone="usatlantic"),  # export→export excluded
    ]
    base = lane_legs(legs, LANE)
    assert len(base) == 2
    assert {lg.status for lg in base} == {"closed", "open_in_transit"}


# --- mean laden-voyage age (#20) ----------------------------------------------


def test_voyage_age_mean():
    legs = [
        mk_leg(status="open_in_transit", departed_ts=at(0), regime="bbox"),
        mk_leg(status="open_in_transit", departed_ts=at(2), regime="bbox"),
    ]
    days = daily_buckets(at(0).date(), at(5).date())
    rows = reconstruct_voyage_age(legs, days)
    by = index(rows)
    d5 = at(5).date()
    # ageA = 5d, ageB = 3d → mean = 4d = 96h.
    assert by[("mean_laden_voyage_age_h", d5, "usgulf->eu", "all")] == 96.0


def test_voyage_age_ignores_closed_legs():
    legs = [mk_leg(status="closed", dest_zone="nweurope", arrived_ts=at(3))]
    rows = reconstruct_voyage_age(legs, daily_buckets(at(0).date(), at(5).date()))
    assert rows == []


# --- O-D matrix (#5) -----------------------------------------------------------


def test_od_matrix_counts():
    legs = [
        mk_leg(status="closed", origin_zone="usgulf", dest_zone="nweurope"),
        mk_leg(status="closed", origin_zone="usgulf", dest_zone="nweurope"),
        mk_leg(status="closed", origin_zone="usgulf", dest_zone="wmed"),
        mk_leg(status="same_zone", origin_zone="nweurope", dest_zone="nweurope"),
        mk_leg(
            status="closed", origin_zone="usgulf", dest_zone="nweurope", laden=False
        ),
    ]
    rows = od_matrix(legs)
    by = index(rows)
    d = at(0).date()
    assert by[("od_flow_count", d, "usgulf->nweurope", "all")] == 2
    assert by[("od_flow_count", d, "usgulf->wmed", "all")] == 1
    assert "nweurope->nweurope" not in {r.zone_scope for r in rows}


# --- event-count flows (#4/#9) -------------------------------------------------


def test_event_count_daily():
    events = [
        EventCount(1, "moored", at(0), "nweurope", 10, True, "import", "bbox"),
        EventCount(2, "moored", at(0), "nweurope", 10, True, "import", "bbox"),
        EventCount(
            3, "moored", at(0), "usgulf", 1, True, "export", "bbox"
        ),  # wrong flow
        EventCount(
            4, "moored", at(0), "nweurope", 10, False, "import", "bbox"
        ),  # ballast
        EventCount(
            5, "departed", at(0), "usgulf", 1, True, "export", "bbox"
        ),  # loading
    ]
    arrivals = index(
        count_events_daily(
            events,
            signal_key="eu_arrivals",
            event_type="moored",
            flow_direction="import",
            zone_scope="eu",
        )
    )
    assert arrivals[("eu_arrivals", at(0).date(), "eu", "all")] == 2
    loadings = index(
        count_events_daily(
            events,
            signal_key="us_loadings",
            event_type="departed",
            flow_direction="export",
            zone_scope="us",
        )
    )
    assert loadings[("us_loadings", at(0).date(), "us", "all")] == 1
