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


def ev(mmsi, etype, t, zone, terminal_id, laden=None, lat=0.0, lon=0.0,
       source="state_machine"):
    return LegEvent(
        mmsi=mmsi,
        event_type=etype,
        event_time=t,
        zone=zone,
        terminal_id=terminal_id,
        lat=lat,
        lon=lon,
        laden_flag=laden,
        source=source,
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


def test_regime_tag_is_source_aware():
    # A pre-cutover NOAA backfill event tags 'noaa', not 'bbox' (its time would
    # otherwise put it in the throttled live block) — PLAN.md §3.4 / SIGNALS §0.5.
    events = [
        ev(5, "departed", REGIME_CUTOVER - timedelta(days=1), "usgulf", 1,
           laden=True, source="noaa-ais"),
        ev(6, "departed", REGIME_CUTOVER - timedelta(days=1), "usgulf", 1,
           laden=True, source="gfw_voyages"),
        ev(7, "departed", REGIME_CUTOVER - timedelta(days=1), "usgulf", 1,
           laden=True, source="state_machine"),
    ]
    legs = {lg.mmsi: lg for lg in pair_legs(events, NOW)}
    assert legs[5].regime == "noaa"
    assert legs[6].regime == "gfw"
    assert legs[7].regime == "bbox"  # live source → time split unchanged


def test_arrival_beyond_cap_treated_as_open():
    # A zone_entry > MAX_LEG_PAIR_DAYS after departure can't be this leg's arrival
    # (the disconnected-data phantom) -> open, not a multi-month closed leg.
    dep = REGIME_CUTOVER - timedelta(days=400)
    arr = dep + timedelta(days=120)  # 120d > 90d cap
    now = arr + timedelta(days=10)
    legs = pair_legs(
        [ev(9, "departed", dep, "usgulf", 1, laden=True),
         ev(9, "zone_entry", arr, "nweurope", 2)],
        now,
    )
    assert len(legs) == 1
    assert legs[0].arrived_ts is None
    assert legs[0].status != "closed"


def test_arrival_within_cap_still_pairs_closed():
    dep = REGIME_CUTOVER - timedelta(days=400)
    arr = dep + timedelta(days=18)  # a normal US->EU voyage
    legs = pair_legs(
        [ev(9, "departed", dep, "usgulf", 1, laden=True),
         ev(9, "zone_entry", arr, "nweurope", 2)],
        arr + timedelta(days=5),
    )
    assert legs[0].status == "closed"
    assert legs[0].arrived_ts == arr


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


# ---------------------------------------------------------------------------
# Point-in-time loader (DECISIONS.md D-004a)
#
# `compute_legs` is a thin DB loader, and the bug it fixes lives in *which rows
# the queries return*, not in the pairing. So these drive it against a recording
# fake connection: assert the point-in-time path selects the bounded SQL, binds
# `as_of`, and that the default path is byte-for-byte the original queries.
# ---------------------------------------------------------------------------

import asyncio  # noqa: E402

from pipeline.legs import (  # noqa: E402
    DEST_REGION_SQL,
    DEST_STATE_PIT_SQL,
    DEST_STATE_WINDOW_DAYS,
    LAST_FIX_PIT_SQL,
    LAST_FIX_SQL,
    LEG_EVENTS_PIT_SQL,
    LEG_EVENTS_SQL,
    RECENT_FIX_DAYS,
    TERMINAL_ZONE_SQL,
    UNLOCODE_SQL,
    compute_legs,
    resolve_dest_regions,
)


class FakeConn:
    """Records every (sql, args) and replays canned rows keyed by SQL constant."""

    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    async def fetch(self, sql, *args):
        self.calls.append((sql, args))
        return self.responses.get(sql, [])

    def sql_for(self, sql):
        return [args for s, args in self.calls if s == sql]


class FakePool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        conn = self.conn

        class _Ctx:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


def _run(coro):
    return asyncio.run(coro)


def test_pit_loader_uses_bounded_queries_and_binds_as_of():
    conn = FakeConn({})
    _run(compute_legs(FakePool(conn), NOW, point_in_time=True))

    # Bounded variants chosen, each carrying as_of...
    assert conn.sql_for(LEG_EVENTS_PIT_SQL) == [(NOW,)]
    assert conn.sql_for(LAST_FIX_PIT_SQL) == []  # no open_censored legs to probe
    assert conn.sql_for(DEST_STATE_PIT_SQL) == [(NOW, str(DEST_STATE_WINDOW_DAYS))]
    # ...and the un-replayable current-snapshot sources never touched.
    assert conn.sql_for(LEG_EVENTS_SQL) == []
    assert conn.sql_for(LAST_FIX_SQL) == []
    assert conn.sql_for(DEST_REGION_SQL) == []


def test_default_loader_is_unchanged_regression():
    # The live pipeline / viz / vf_rescue path must be byte-for-byte the original
    # queries — this is the guard that the fix is additive.
    conn = FakeConn({})
    _run(compute_legs(FakePool(conn), NOW))

    assert conn.sql_for(LEG_EVENTS_SQL) == [()]
    assert conn.sql_for(LAST_FIX_SQL) == [()]
    assert conn.sql_for(DEST_REGION_SQL) == [()]
    assert conn.sql_for(LEG_EVENTS_PIT_SQL) == []
    assert conn.sql_for(LAST_FIX_PIT_SQL) == []
    assert conn.sql_for(DEST_STATE_PIT_SQL) == []


def test_pit_loader_skips_enrichment_when_disabled():
    conn = FakeConn({})
    _run(compute_legs(FakePool(conn), NOW, point_in_time=True, enrich=False))
    assert conn.sql_for(LEG_EVENTS_PIT_SQL) == [(NOW,)]
    assert conn.sql_for(LAST_FIX_PIT_SQL) == []
    assert conn.sql_for(DEST_STATE_PIT_SQL) == []


def test_pit_loader_classifies_from_bounded_evidence_not_latest_fix():
    """The actual D-004a bug, end to end.

    A leg departed 40d before as_of (past the 18d NW-Europe window) whose only
    pre-as_of fix is a stale mid-ocean one is a phantom — `open_censored`. The
    unbounded loader would hand `_classify_overdue` the vessel's latest-EVER fix
    (here, one 200d AFTER as_of), which trivially passes the `now - 4d` recency
    test and mislabels it `open_floating`. The bounded query never returns it.
    """
    departed = NOW - timedelta(days=40)
    stale_midocean = (NOW - timedelta(days=30), 35.0, -40.0)
    future_fix = (NOW + timedelta(days=200), ROTTERDAM[0], ROTTERDAM[1])

    def rows(fix):
        return {
            LEG_EVENTS_PIT_SQL: [
                {
                    "mmsi": 99, "event_type": "departed", "event_time": departed,
                    "zone": "usgulf", "terminal_id": 1,
                    "lat": SABINE[0], "lon": SABINE[1],
                    "laden_flag": True, "source": "noaa-ais",
                }
            ],
            LAST_FIX_PIT_SQL: [
                {"mmsi": 99, "fix_ts": fix[0], "lat": fix[1], "lon": fix[2]}
            ],
            UNLOCODE_SQL: [],
            TERMINAL_ZONE_SQL: [],
        }

    bounded = _run(
        compute_legs(FakePool(FakeConn(rows(stale_midocean))), NOW, point_in_time=True)
    )
    assert bounded[0].status == "open_censored"

    # Same leg, but with the future fix leaking in (what the unbounded loader did).
    leaked = _run(
        compute_legs(FakePool(FakeConn(rows(future_fix))), NOW, point_in_time=True)
    )
    assert leaked[0].status == "open_floating"
    # ...and that leak is exactly a recency test the future fix cannot fail:
    assert future_fix[0] > NOW - timedelta(days=RECENT_FIX_DAYS)


def test_pit_loader_hides_post_as_of_arrival():
    # An arrival after as_of must leave the leg OPEN, not close it.
    conn = FakeConn(
        {
            LEG_EVENTS_PIT_SQL: [
                {
                    "mmsi": 7, "event_type": "departed",
                    "event_time": NOW - timedelta(days=5),
                    "zone": "usgulf", "terminal_id": 1,
                    "lat": SABINE[0], "lon": SABINE[1],
                    "laden_flag": True, "source": "noaa-ais",
                }
                # the zone_entry at NOW+3d is filtered out by the SQL bound
            ],
        }
    )
    legs = _run(compute_legs(FakePool(conn), NOW, point_in_time=True))
    assert len(legs) == 1
    assert legs[0].status == "open_in_transit"
    assert legs[0].arrived_ts is None


# --- resolve_dest_regions (pure) -------------------------------------------

UNLOCODES = {"NLRTM": 10, "USSAB": 1}
TERMINAL_ZONES = {10: "nweurope", 1: "usgulf"}


def test_resolve_dest_regions_parses_locode_to_zone():
    got = resolve_dest_regions([(1, "NLRTM")], UNLOCODES, TERMINAL_ZONES)
    assert got == {1: "nweurope"}


def test_resolve_dest_regions_uses_rhs_of_chained_declaration():
    # "USSAB>NLRTM" means next port Rotterdam, not Sabine.
    got = resolve_dest_regions([(2, "USSAB>NLRTM")], UNLOCODES, TERMINAL_ZONES)
    assert got == {2: "nweurope"}


def test_resolve_dest_regions_drops_unresolvable_declarations():
    # FOR ORDERS / empty / junk / unknown-terminal all resolve to nothing, and an
    # absent key is treated by pair_legs exactly like an undeclared vessel.
    rows = [(3, "FOR ORDERS"), (4, None), (5, ""), (6, "ZZZZZ")]
    assert resolve_dest_regions(rows, UNLOCODES, TERMINAL_ZONES) == {}


def test_resolve_dest_regions_drops_terminal_with_no_zone():
    got = resolve_dest_regions([(8, "NLRTM")], UNLOCODES, {})
    assert got == {}


def test_pit_last_fix_probe_is_narrowed_to_open_legs():
    """The two-pass narrowing: probe only vessels with an open leg.

    Vessel 1's leg closed before as_of, so its last fix cannot change anything;
    vessel 2's is open. Only 2 should reach the (expensive) LATERAL.
    """
    def dep(mmsi, days_ago):
        return {
            "mmsi": mmsi, "event_type": "departed",
            "event_time": NOW - timedelta(days=days_ago),
            "zone": "usgulf", "terminal_id": 1,
            "lat": SABINE[0], "lon": SABINE[1],
            "laden_flag": True, "source": "noaa-ais",
        }

    conn = FakeConn(
        {
            LEG_EVENTS_PIT_SQL: [
                dep(1, 30),
                {
                    "mmsi": 1, "event_type": "zone_entry",
                    "event_time": NOW - timedelta(days=16),
                    "zone": "nweurope", "terminal_id": 10,
                    "lat": ROTTERDAM[0], "lon": ROTTERDAM[1],
                    "laden_flag": None, "source": "noaa-ais",
                },
                dep(2, 30),  # no arrival -> open
            ],
            UNLOCODE_SQL: [],
            TERMINAL_ZONE_SQL: [],
            LAST_FIX_PIT_SQL: [
                {"mmsi": 2, "fix_ts": NOW - timedelta(days=1),
                 "lat": ROTTERDAM[0], "lon": ROTTERDAM[1]}
            ],
        }
    )
    legs = _run(compute_legs(FakePool(conn), NOW, point_in_time=True))

    assert conn.sql_for(LAST_FIX_PIT_SQL) == [(NOW, [2])]
    by_mmsi = {lg.mmsi: lg for lg in legs}
    assert by_mmsi[1].status == "closed"
    # Open leg got its evidence and reclassified off the no-evidence default.
    assert by_mmsi[2].status == "open_floating"
    assert by_mmsi[2].last_fix_ts == NOW - timedelta(days=1)


def test_pit_open_in_transit_leg_still_carries_last_fix():
    # Narrowing must not blank `last_fix_*` on non-overdue open legs (they are
    # within window, so evidence doesn't change status, but the fields are API).
    fix_ts = NOW - timedelta(hours=6)
    conn = FakeConn(
        {
            LEG_EVENTS_PIT_SQL: [
                {
                    "mmsi": 5, "event_type": "departed",
                    "event_time": NOW - timedelta(days=3),
                    "zone": "usgulf", "terminal_id": 1,
                    "lat": SABINE[0], "lon": SABINE[1],
                    "laden_flag": True, "source": "noaa-ais",
                }
            ],
            UNLOCODE_SQL: [],
            TERMINAL_ZONE_SQL: [],
            LAST_FIX_PIT_SQL: [
                {"mmsi": 5, "fix_ts": fix_ts, "lat": 30.0, "lon": -80.0}
            ],
        }
    )
    legs = _run(compute_legs(FakePool(conn), NOW, point_in_time=True))
    assert legs[0].status == "open_in_transit"
    assert legs[0].last_fix_ts == fix_ts
