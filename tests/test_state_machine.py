"""Fixture tests for the port-events state machine.

Synthetic vessel trajectories — no DB. Covers golden path, 4-polygon overlap
with retroactive reattribution, cold-start variants, AIS dropout, drive-by
anchorage, and re-anchoring.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pipeline.state_machine import (
    Fix,
    make_nearest_berth,
    validate_sequence,
    walk,
)


# Three terminals used in the fixtures:
#   1 = Sabine,    berth centroid ~(29.74, -93.87)
#   2 = Golden Pass, berth centroid ~(29.69, -93.83)
#   3 = Gate Rotterdam, berth centroid ~(52.00, 4.00)
BERTHS = {
    1: [(29.74, -93.87)],
    2: [(29.69, -93.83)],
    3: [(52.00, 4.00)],
}
NEAREST_BERTH = make_nearest_berth(BERTHS)

T0 = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)


def at(minutes: float) -> datetime:
    return T0 + timedelta(minutes=minutes)


def fix(minutes: float, zones, sog=0.0, lat=29.7, lon=-93.85):
    return Fix(
        fix_ts=at(minutes), lat=lat, lon=lon, sog=sog, nav_status=None, zones=zones
    )


# ----------------------------------------------------------------------
# Golden path: approach -> anchored -> berth -> moored -> depart -> exit
# ----------------------------------------------------------------------


def test_golden_path_emits_full_envelope():
    fixes = [
        # 5 min approaching, only in Sabine 'approach'
        fix(0, ((1, "approach", 0),), sog=5.0),
        fix(5, ((1, "approach", 0),), sog=5.0),
        # 35 min stationary in Sabine 'anchorage' (covers 30-min dwell)
        fix(10, ((1, "anchorage", 0), (1, "approach", 0)), sog=0.2),
        fix(20, ((1, "anchorage", 0), (1, "approach", 0)), sog=0.2),
        fix(45, ((1, "anchorage", 0), (1, "approach", 0)), sog=0.2),
        # 15 min in the approach-only band (channel transit between anchorage
        # and berth)
        fix(50, ((1, "approach", 0),), sog=4.0),
        fix(60, ((1, "approach", 0),), sog=4.0),
        # 35 min stationary in Sabine 'berth'
        fix(70, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),
        fix(80, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),
        fix(105, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),
        # Vessel undocks, 20 min outside berth but still in approach with sog>1
        fix(110, ((1, "approach", 0),), sog=3.0),
        fix(120, ((1, "approach", 0),), sog=3.0),
        fix(130, ((1, "approach", 0),), sog=3.0),
        # Leaves the area
        fix(140, (), sog=8.0),
    ]
    events = walk(iter(fixes), NEAREST_BERTH)
    validate_sequence(events)

    types = [(e.event_type, e.event_time) for e in events]
    assert types == [
        ("zone_entry", at(0)),  # first fix in approach
        ("anchorage_entry", at(10)),  # vessel crosses into anchorage polygon
        ("anchored", at(10)),  # dwell-confirmed (back-dated)
        ("anchorage_exit", at(50)),  # vessel leaves anchorage for the channel
        ("moored", at(70)),  # dwell-confirmed at berth (back-dated)
        ("departed", at(110)),  # first qualifying out-of-berth fix
        ("zone_exit", at(140)),
    ]

    # Queue time: 40 min between anchorage_entry (t=10) and anchorage_exit (t=50)
    anchorage_in = next(e for e in events if e.event_type == "anchorage_entry")
    anchorage_out = next(e for e in events if e.event_type == "anchorage_exit")
    assert anchorage_out.event_time - anchorage_in.event_time == timedelta(minutes=40)

    assert {e.terminal_id for e in events} == {1}
    assert not any(e.cold_start for e in events)


# ----------------------------------------------------------------------
# 4-polygon shared anchorage: enter Sabine/GP overlap, berth at Golden Pass.
# Expect retroactive reattribution to rewrite zone_entry + anchored to GP.
# ----------------------------------------------------------------------


def test_shared_anchorage_then_berth_at_other_terminal():
    # Fix inside the overlap matches: (Sabine, approach), (Sabine, anchorage),
    # (GP, approach), (GP, anchorage). Nearest-berth tiebreaker between Sabine
    # (~29.74,-93.87) and GP (~29.69,-93.83) at fix lat/lon (29.70,-93.85)
    # picks GP — so set the fix slightly nearer Sabine to test stickiness as
    # initially Sabine.
    shared = (
        (1, "anchorage", 0),
        (1, "approach", 0),
        (2, "anchorage", 0),
        (2, "approach", 0),
    )
    # Lat/lon chosen so Sabine berth is closer than GP berth.
    sabine_close_lat, sabine_close_lon = 29.735, -93.865
    fixes = [
        # Cold entry inside overlap — Sabine wins (closer berth)
        Fix(at(0), sabine_close_lat, sabine_close_lon, 0.2, None, shared),
        Fix(at(10), sabine_close_lat, sabine_close_lon, 0.2, None, shared),
        Fix(at(45), sabine_close_lat, sabine_close_lon, 0.2, None, shared),
        # Vessel transits to GP berth (only GP polygons match)
        Fix(at(50), 29.69, -93.83, 4.0, None, ((2, "approach", 0),)),
        Fix(at(60), 29.69, -93.83, 4.0, None, ((2, "approach", 0),)),
        # Moors at GP berth for 30+ min
        Fix(at(70), 29.69, -93.83, 0.1, None, ((2, "berth", 0), (2, "approach", 0))),
        Fix(at(80), 29.69, -93.83, 0.1, None, ((2, "berth", 0), (2, "approach", 0))),
        Fix(at(105), 29.69, -93.83, 0.1, None, ((2, "berth", 0), (2, "approach", 0))),
    ]
    events = walk(iter(fixes), NEAREST_BERTH)
    # The earlier zone_entry, anchorage_entry, anchored, anchorage_exit fires
    # were tentatively attributed to Sabine (closer berth). When the berth
    # override at t=70 finds the vessel at GP, the state machine sees that
    # every event in the current envelope had GP in candidate_terminal_ids
    # (the shared anchorage), so the envelope is rewritten to GP in place.
    types = [e.event_type for e in events]
    assert (
        types
        == [
            "zone_entry",
            "anchorage_entry",
            "anchored",
            "anchorage_exit",  # fires when vessel leaves shared anchorage for GP-only approach
            "moored",
        ]
    )
    assert all(e.terminal_id == 2 for e in events)
    validate_sequence(events)


def test_distinct_terminals_real_switch_closes_and_reopens_envelope():
    """If the earlier envelope did NOT have the new terminal in its candidate
    sets, a berth-override IS a real terminal switch — emit zone_exit for the
    old terminal and zone_entry for the new one."""
    fixes = [
        # Enter Sabine alone (no overlap with GP)
        fix(0, ((1, "anchorage", 0), (1, "approach", 0)), sog=0.2),
        fix(45, ((1, "anchorage", 0), (1, "approach", 0)), sog=0.2),
        # Vessel teleports to GP berth (synthetic — really it would transit
        # but the state machine only cares about polygon membership)
        fix(50, ((2, "berth", 0), (2, "approach", 0)), sog=0.1),
        fix(85, ((2, "berth", 0), (2, "approach", 0)), sog=0.1),
    ]
    events = walk(iter(fixes), NEAREST_BERTH)
    validate_sequence(events)
    types = [(e.event_type, e.terminal_id) for e in events]
    # The earlier envelope (Sabine) had only {1} in every candidate set, so
    # the rewrite check fails — flush anchorage_exit, close Sabine, open GP
    # cleanly.
    assert types == [
        ("zone_entry", 1),
        ("anchorage_entry", 1),
        ("anchored", 1),
        ("anchorage_exit", 1),
        ("zone_exit", 1),
        ("zone_entry", 2),
        ("moored", 2),
    ]


# ----------------------------------------------------------------------
# Cold-start: first observed fix already inside a berth.
# ----------------------------------------------------------------------


def test_cold_start_first_fix_in_berth():
    fixes = [
        # Vessel's first fix in our data is already moored at Sabine berth.
        fix(0, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),
        fix(30, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),
        # Then it leaves
        fix(60, ((1, "approach", 0),), sog=4.0),
        fix(80, ((1, "approach", 0),), sog=4.0),
        fix(100, (), sog=8.0),
    ]
    events = walk(iter(fixes), NEAREST_BERTH)
    validate_sequence(events)
    types = [e.event_type for e in events]
    assert types[0] == "zone_entry"
    assert events[0].cold_start is True
    assert events[0].event_time == at(0)  # back-dated to first fix
    assert types[1] == "moored"
    assert events[1].cold_start is True
    assert events[1].event_time == at(0)
    assert "departed" in types
    assert types[-1] == "zone_exit"


def test_cold_start_first_fix_in_anchorage():
    fixes = [
        fix(0, ((1, "anchorage", 0), (1, "approach", 0)), sog=0.3),
        fix(60, (), sog=8.0),
    ]
    events = walk(iter(fixes), NEAREST_BERTH)
    validate_sequence(events)
    types = [e.event_type for e in events]
    assert types == [
        "zone_entry",
        "anchorage_entry",
        "anchored",
        "anchorage_exit",
        "zone_exit",
    ]
    # cold_start marker propagates to all three synthetic events
    assert events[0].cold_start is True
    assert events[1].cold_start is True
    assert events[2].cold_start is True
    # anchorage_exit + zone_exit aren't cold_start — they're real observations
    assert events[3].cold_start is False
    assert events[4].cold_start is False


# ----------------------------------------------------------------------
# AIS dropout: vessel is moored, drops off AIS for hours, comes back still
# moored. Should NOT emit a spurious departed/moored pair.
# ----------------------------------------------------------------------


def test_ais_dropout_during_mooring_no_spurious_events():
    fixes = [
        # Approach + dwell + moored
        fix(0, ((1, "approach", 0),), sog=5.0),
        fix(10, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),
        fix(20, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),
        fix(45, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),
        # 6h AIS dropout (no fixes), then back in berth.
        fix(45 + 6 * 60, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),
        fix(45 + 6 * 60 + 30, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),
        # Eventually departs
        fix(45 + 7 * 60, ((1, "approach", 0),), sog=3.0),
        fix(45 + 7 * 60 + 20, ((1, "approach", 0),), sog=3.0),
        fix(45 + 7 * 60 + 30, (), sog=8.0),
    ]
    events = walk(iter(fixes), NEAREST_BERTH)
    validate_sequence(events)
    types = [e.event_type for e in events]
    # Exactly one of each — dropout did not produce spurious extras.
    assert types == [
        "zone_entry",
        "moored",
        "departed",
        "zone_exit",
    ]


# ----------------------------------------------------------------------
# Cold-end: vessel still moored at the end of the data window. No synthetic
# departed should be emitted.
# ----------------------------------------------------------------------


def test_cold_end_no_synthetic_departed():
    fixes = [
        fix(0, ((1, "approach", 0),), sog=5.0),
        fix(10, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),
        fix(45, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),
        fix(120, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),
        # Data ends here — vessel still moored.
    ]
    events = walk(iter(fixes), NEAREST_BERTH)
    validate_sequence(events)
    types = [e.event_type for e in events]
    assert types == ["zone_entry", "moored"]
    assert events[-1].event_type == "moored"


# ----------------------------------------------------------------------
# Anchorage-only visit (vessel anchors then leaves without berthing).
# ----------------------------------------------------------------------


def test_anchorage_only_visit():
    fixes = [
        fix(0, ((1, "approach", 0),), sog=5.0),
        fix(10, ((1, "anchorage", 0), (1, "approach", 0)), sog=0.2),
        fix(45, ((1, "anchorage", 0), (1, "approach", 0)), sog=0.2),
        fix(80, ((1, "approach", 0),), sog=6.0),
        fix(120, (), sog=8.0),
    ]
    events = walk(iter(fixes), NEAREST_BERTH)
    validate_sequence(events)
    types = [e.event_type for e in events]
    assert types == [
        "zone_entry",
        "anchorage_entry",
        "anchored",
        "anchorage_exit",
        "zone_exit",
    ]


# ----------------------------------------------------------------------
# Drive-by anchorage: vessel passes through the anchorage polygon briefly
# (no dwell) on the way to the berth. anchorage_entry/exit fire but
# anchored does not — useful for distinguishing real queues from incidental
# crossings.
# ----------------------------------------------------------------------


def test_drive_by_anchorage_no_anchored_event():
    fixes = [
        fix(0, ((1, "approach", 0),), sog=5.0),
        # 8 minutes in anchorage at moderate speed — too brief and too fast
        # to fire `anchored`
        fix(2, ((1, "anchorage", 0), (1, "approach", 0)), sog=4.0),
        fix(8, ((1, "anchorage", 0), (1, "approach", 0)), sog=4.0),
        fix(10, ((1, "approach", 0),), sog=4.0),
        # Reaches the berth
        fix(40, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),
        fix(75, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),
        fix(110, ((1, "approach", 0),), sog=3.0),
        fix(130, ((1, "approach", 0),), sog=3.0),
        fix(140, (), sog=8.0),
    ]
    events = walk(iter(fixes), NEAREST_BERTH)
    validate_sequence(events)
    types = [e.event_type for e in events]
    # anchorage_entry/exit pair fires, but no anchored — the dwell threshold
    # filters out incidental crossings as "real queue" events.
    assert types == [
        "zone_entry",
        "anchorage_entry",
        "anchorage_exit",
        "moored",
        "departed",
        "zone_exit",
    ]
    # Anchorage dwell = 8 minutes — a query-time filter would drop this as
    # "not a real queue".
    anchorage_in = next(e for e in events if e.event_type == "anchorage_entry")
    anchorage_out = next(e for e in events if e.event_type == "anchorage_exit")
    assert anchorage_out.event_time - anchorage_in.event_time == timedelta(minutes=8)


# ----------------------------------------------------------------------
# Re-anchoring: vessel anchors, weighs anchor, drifts in approach, anchors
# again, then berths. Two anchorage_entry/exit pairs and two anchored events.
# ----------------------------------------------------------------------


def test_re_anchoring_emits_two_anchored_events():
    fixes = [
        fix(0, ((1, "approach", 0),), sog=5.0),
        # First anchor stint
        fix(5, ((1, "anchorage", 0), (1, "approach", 0)), sog=0.2),
        fix(40, ((1, "anchorage", 0), (1, "approach", 0)), sog=0.2),
        # Weighs anchor, drifts in approach for an hour
        fix(50, ((1, "approach", 0),), sog=2.5),
        fix(80, ((1, "approach", 0),), sog=1.5),
        # Second anchor stint
        fix(100, ((1, "anchorage", 0), (1, "approach", 0)), sog=0.2),
        fix(140, ((1, "anchorage", 0), (1, "approach", 0)), sog=0.2),
        # Finally berths
        fix(150, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),
        fix(185, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),
    ]
    events = walk(iter(fixes), NEAREST_BERTH)
    validate_sequence(events)
    types = [e.event_type for e in events]
    assert types == [
        "zone_entry",
        "anchorage_entry",
        "anchored",
        "anchorage_exit",
        "anchorage_entry",
        "anchored",
        "anchorage_exit",
        "moored",
    ]
    # Total queue time across both stints
    pairs = [e for e in events if e.event_type in ("anchorage_entry", "anchorage_exit")]
    total_queue = sum(
        (
            pairs[i + 1].event_time - pairs[i].event_time
            for i in range(0, len(pairs), 2)
        ),
        timedelta(),
    )
    # First stint: 5 -> 50 = 45 min. Second stint: 100 -> 150 = 50 min. Total 95.
    assert total_queue == timedelta(minutes=95)


# ----------------------------------------------------------------------
# departed recovery: vessel undocks and its first post-undock fix is already
# outside every polygon (the approach polygon didn't contain the outbound
# channel, or a position jump). A `departed` must still be emitted, back-dated
# to the last in-polygon fix, rather than skipped straight to zone_exit.
# ----------------------------------------------------------------------


def test_moored_to_open_ocean_emits_departed():
    fixes = [
        fix(0, ((1, "approach", 0),), sog=5.0),
        fix(10, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),
        fix(45, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),  # moored (dwell)
        # Next fix is already open ocean — no approach-band fix in between.
        fix(60, (), sog=8.0),
    ]
    events = walk(iter(fixes), NEAREST_BERTH)
    validate_sequence(events)
    types = [(e.event_type, e.event_time) for e in events]
    assert types == [
        ("zone_entry", at(0)),
        ("moored", at(10)),
        ("departed", at(45)),  # back-dated to the last in-polygon (berth) fix
        ("zone_exit", at(60)),
    ]
    departed = next(e for e in events if e.event_type == "departed")
    assert departed.terminal_id == 1
    assert not departed.cold_start


# ----------------------------------------------------------------------
# Stale-envelope close: AIS goes silent inside a polygon for > stale_threshold.
# The envelope is closed at the last observed fix, flagged cold_start=True.
# (Currently the source of ~1/3 of all live zone_exit events — was untested.)
# ----------------------------------------------------------------------


def test_stale_envelope_close_between_fixes():
    """A >72h gap between two consecutive fixes while anchored closes the
    envelope at the last fix (synthetic anchorage_exit + zone_exit)."""
    fixes = [
        fix(0, ((1, "approach", 0),), sog=5.0),
        fix(10, ((1, "anchorage", 0), (1, "approach", 0)), sog=0.2),
        fix(45, ((1, "anchorage", 0), (1, "approach", 0)), sog=0.2),  # anchored
        # 73h silence, then the vessel reappears far away in open ocean.
        fix(45 + 73 * 60, (), sog=10.0),
    ]
    events = walk(iter(fixes), NEAREST_BERTH)
    validate_sequence(events)
    types = [e.event_type for e in events]
    assert types == [
        "zone_entry",
        "anchorage_entry",
        "anchored",
        "anchorage_exit",
        "zone_exit",
    ]
    # Synthetic close back-dated to the last in-polygon fix (t=45), cold_start.
    assert events[-1].event_type == "zone_exit"
    assert events[-1].event_time == at(45)
    assert events[-1].cold_start is True
    assert events[-2].event_type == "anchorage_exit"
    assert events[-2].cold_start is True


def test_stale_envelope_close_end_of_stream():
    """Stream ends with the vessel still moored and the last fix older than
    now - stale_threshold: close the envelope at the last fix. No departed
    (we lost coverage; departure is unknowable)."""
    fixes = [
        fix(0, ((1, "approach", 0),), sog=5.0),
        fix(10, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),
        fix(45, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),  # moored
    ]
    # `now` is 80h after the last fix -> end-of-stream stale close fires.
    events = walk(iter(fixes), NEAREST_BERTH, now=at(45 + 80 * 60))
    validate_sequence(events)
    types = [e.event_type for e in events]
    assert types == ["zone_entry", "moored", "zone_exit"]
    assert events[-1].cold_start is True
    assert events[-1].event_time == at(45)


def test_no_stale_close_when_now_within_threshold():
    """If now is within stale_threshold of the last fix, the envelope stays
    open (matches the cold-end behaviour)."""
    fixes = [
        fix(0, ((1, "approach", 0),), sog=5.0),
        fix(10, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),
        fix(45, ((1, "berth", 0), (1, "approach", 0)), sog=0.1),
    ]
    events = walk(iter(fixes), NEAREST_BERTH, now=at(45 + 10))  # 10 min later
    types = [e.event_type for e in events]
    assert types == ["zone_entry", "moored"]
