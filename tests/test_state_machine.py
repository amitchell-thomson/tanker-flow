"""Fixture tests for the port-events state machine.

Synthetic vessel trajectories — no DB. Covers the four scenarios called out in
the plan: golden path, 4-polygon overlap with retroactive reattribution,
cold-start, AIS dropout.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pipeline.state_machine import (
    Fix,
    make_nearest_berth,
    reattribute_overlaps,
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
    # Expected sequence and back-dated timestamps
    assert types[0] == ("zone_entry", at(0))
    assert types[1] == ("anchored", at(10))  # back-dated to first qualifying fix
    assert types[2] == ("moored", at(70))  # back-dated to first in-berth fix
    assert types[3] == ("departed", at(110))  # back-dated to first out-of-berth fix
    assert types[4] == ("zone_exit", at(140))

    # All events attributed to Sabine
    assert {e.terminal_id for e in events} == {1}
    # None of them were cold_start
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
    # The earlier zone_entry + anchored fires were tentatively attributed to
    # Sabine (closer berth). When the berth-override at 70min finds the
    # vessel at GP, the state machine checks whether the entire current
    # envelope had GP in every event's candidate_terminal_ids — it does
    # (the shared anchorage), so the envelope is rewritten to GP rather than
    # closing Sabine and opening a fresh GP envelope.
    types = [e.event_type for e in events]
    assert types == ["zone_entry", "anchored", "moored"]
    assert all(e.terminal_id == 2 for e in events)
    assert events[0].event_time == at(0)
    assert events[1].event_time == at(0)  # anchored back-dated to first qualifying fix
    assert events[2].event_time == at(70)
    # reattribute_overlaps is a no-op safety net after the inline rewrite.
    assert reattribute_overlaps(events) == events


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
    # the rewrite check fails — close Sabine, open GP cleanly.
    assert types == [
        ("zone_entry", 1),
        ("anchored", 1),
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
    types = [e.event_type for e in events]
    assert types == ["zone_entry", "anchored", "zone_exit"]
    assert events[0].cold_start is True
    assert events[1].cold_start is True


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
    assert types == ["zone_entry", "anchored", "zone_exit"]
