"""Unit tests for the voyage-leg pairing/classification/censoring (pipeline.legs).

Pure-logic: synthetic LegEvent lists, no DB.
"""

from __future__ import annotations

from datetime import timedelta

from config import REGIME_CUTOVER
from pipeline.legs import LegEvent, pair_legs


# A reference "now" well after the regime cutover so open/censored splits work.
NOW = REGIME_CUTOVER + timedelta(days=40)

# Representative berth coordinates.
SABINE = (29.74, -93.87)  # usgulf
ROTTERDAM = (52.00, 4.00)  # nweurope
ZEEBRUGGE = (51.33, 3.20)  # nweurope


def ev(mmsi, etype, t, zone, terminal_id, laden=None, lat=0.0, lon=0.0):
    return LegEvent(
        mmsi=mmsi,
        event_type=etype,
        event_time=t,
        zone=zone,
        terminal_id=terminal_id,
        lat=lat,
        lon=lon,
        laden_flag=laden,
    )


def at(days: float):
    return REGIME_CUTOVER - timedelta(days=20) + timedelta(days=days)


def test_closed_transatlantic_leg():
    events = [
        ev(1, "departed", at(0), "usgulf", 1, laden=True, lat=SABINE[0], lon=SABINE[1]),
        ev(1, "zone_entry", at(15), "nweurope", 10, lat=ROTTERDAM[0], lon=ROTTERDAM[1]),
    ]
    legs = pair_legs(events, NOW)
    assert len(legs) == 1
    leg = legs[0]
    assert leg.status == "closed"
    assert (leg.origin_zone, leg.dest_zone) == ("usgulf", "nweurope")
    assert leg.laden is True
    # Sabine -> Rotterdam great-circle is ~4500 nm; sanity-bound it.
    assert 4000 < leg.distance_nm < 5500
    assert abs(leg.duration_h - 15 * 24) < 1


def test_same_zone_leg_flagged():
    events = [
        ev(2, "departed", at(0), "nweurope", 12, lat=ZEEBRUGGE[0], lon=ZEEBRUGGE[1]),
        ev(
            2,
            "zone_entry",
            at(0) + timedelta(hours=5),
            "nweurope",
            10,
            lat=ROTTERDAM[0],
            lon=ROTTERDAM[1],
        ),
    ]
    legs = pair_legs(events, NOW)
    assert len(legs) == 1
    assert legs[0].status == "same_zone"
    assert legs[0].distance_nm < 100  # short intra-region hop


def test_open_in_transit_vs_censored():
    events = [
        # departed 10 days ago, still en route -> in transit
        ev(3, "departed", NOW - timedelta(days=10), "usgulf", 1, laden=True),
        # departed 40 days ago, never arrived -> censored (not in transit)
        ev(4, "departed", NOW - timedelta(days=40), "usgulf", 1, laden=True),
    ]
    legs = {lg.mmsi: lg for lg in pair_legs(events, NOW)}  # censor_days default 30
    assert legs[3].status == "open_in_transit"
    assert legs[3].distance_nm is None and legs[3].arrived_ts is None
    assert legs[4].status == "open_censored"


def test_regime_tag_from_departed_time():
    events = [
        ev(5, "departed", REGIME_CUTOVER - timedelta(days=1), "usgulf", 1, laden=True),
        ev(6, "departed", REGIME_CUTOVER + timedelta(days=1), "usgulf", 1, laden=True),
    ]
    legs = {lg.mmsi: lg for lg in pair_legs(events, NOW)}
    assert legs[5].regime == "bbox"
    assert legs[6].regime == "mmsi_filter"


def test_weights_attached():
    events = [ev(7, "departed", at(0), "usgulf", 1, laden=True)]
    legs = pair_legs(events, NOW, weights={7: (90000, 170000)})
    assert (legs[0].dwt, legs[0].gas_capacity_m3) == (90000, 170000)
    # No weight entry -> Nones.
    legs2 = pair_legs(events, NOW)
    assert (legs2[0].dwt, legs2[0].gas_capacity_m3) == (None, None)


def test_multiple_departures_pair_to_their_own_next_entry():
    events = [
        ev(8, "departed", at(0), "usgulf", 1, laden=True, lat=SABINE[0], lon=SABINE[1]),
        ev(8, "zone_entry", at(15), "nweurope", 10, lat=ROTTERDAM[0], lon=ROTTERDAM[1]),
        ev(
            8,
            "departed",
            at(18),
            "nweurope",
            10,
            laden=False,
            lat=ROTTERDAM[0],
            lon=ROTTERDAM[1],
        ),
        ev(8, "zone_entry", at(33), "usgulf", 1, lat=SABINE[0], lon=SABINE[1]),
    ]
    legs = pair_legs(events, NOW)
    assert [(lg.origin_zone, lg.dest_zone, lg.status) for lg in legs] == [
        ("usgulf", "nweurope", "closed"),
        ("nweurope", "usgulf", "closed"),
    ]
    assert legs[0].laden is True and legs[1].laden is False


def test_laden_none_propagates():
    events = [ev(9, "departed", at(0), "usgulf", 1, laden=None)]
    legs = pair_legs(events, NOW)
    assert legs[0].laden is None


# --- Piece A: enriched per-O-D window + last-fix classifier --------------------
from pipeline.legs import _classify_overdue, _zone_of  # noqa: E402


def test_zone_of_coastal_vs_midocean():
    assert _zone_of(*ROTTERDAM) == "nweurope"
    assert _zone_of(*SABINE) == "usgulf"
    assert _zone_of(35.0, -40.0) is None  # mid-Atlantic


def test_classify_overdue_floating_recent_coastal():
    # Recent fix, inside a coastal region ⇒ genuine on-water floating storage.
    lf = (NOW - timedelta(days=2), ROTTERDAM[0], ROTTERDAM[1])
    assert _classify_overdue(lf, "nweurope", NOW) == "open_floating"


def test_classify_overdue_arrival_gap_stale_in_dest():
    # Stale fix, but it's in the declared destination region ⇒ arrived-and-missed.
    lf = (NOW - timedelta(days=10), ROTTERDAM[0], ROTTERDAM[1])
    assert _classify_overdue(lf, "nweurope", NOW) == "open_arrival_gap"


def test_classify_overdue_censored_midocean_or_no_fix():
    stale_mid = (NOW - timedelta(days=10), 35.0, -40.0)
    assert _classify_overdue(stale_mid, "nweurope", NOW) == "open_censored"
    assert _classify_overdue(None, "nweurope", NOW) == "open_censored"
    # Stale fix in a region that is NOT the declared destination ⇒ censored.
    lf_wrong = (NOW - timedelta(days=10), SABINE[0], SABINE[1])
    assert _classify_overdue(lf_wrong, "nweurope", NOW) == "open_censored"


def test_per_od_window_tightens_europe():
    # Departed 22 days ago to NW Europe (window 18d) ⇒ past window; with a stale
    # mid-ocean last fix it censors — whereas the flat 30d default would keep it.
    events = [ev(20, "departed", NOW - timedelta(days=22), "usgulf", 1, laden=True)]
    legs = pair_legs(
        events,
        NOW,
        dest_regions={20: "nweurope"},
        last_fixes={20: (NOW - timedelta(days=12), 35.0, -40.0)},
    )
    assert legs[0].status == "open_censored"
    assert legs[0].dest_region == "nweurope"
    # Same leg with no dest ⇒ flat 30d window ⇒ still in transit at 22 days.
    legs_flat = pair_legs(events, NOW)
    assert legs_flat[0].status == "open_in_transit"


def test_fallback_region_tightens_undeclared_open_leg():
    # An undeclared open leg at 22 days: bare 30d window keeps it in transit, but
    # with fallback_region='nweurope' it inherits the 18d window and (stale mid-ocean
    # fix) censors — the same treatment a declared-nweurope leg gets. This is the
    # consistency the signal layer relies on (it distances these as NW-Europe-bound).
    events = [ev(22, "departed", NOW - timedelta(days=22), "usgulf", 1, laden=True)]
    assert pair_legs(events, NOW)[0].status == "open_in_transit"
    tightened = pair_legs(
        events,
        NOW,
        last_fixes={22: (NOW - timedelta(days=12), 35.0, -40.0)},
        fallback_region="nweurope",
    )
    assert tightened[0].status == "open_censored"


def test_fallback_region_arrival_gap_for_undeclared_leg():
    # Undeclared, past the inherited 18d window, last fix stale but inside the
    # assumed (NW Europe) region ⇒ arrived-and-missed, not a phantom.
    events = [ev(23, "departed", NOW - timedelta(days=22), "usgulf", 1, laden=True)]
    legs = pair_legs(
        events,
        NOW,
        last_fixes={23: (NOW - timedelta(days=10), ROTTERDAM[0], ROTTERDAM[1])},
        fallback_region="nweurope",
    )
    assert legs[0].status == "open_arrival_gap"


def test_per_od_window_in_transit_within_window():
    # 12 days to NW Europe (window 18d) ⇒ still in transit.
    events = [ev(21, "departed", NOW - timedelta(days=12), "usgulf", 1, laden=True)]
    legs = pair_legs(events, NOW, dest_regions={21: "nweurope"})
    assert legs[0].status == "open_in_transit"
