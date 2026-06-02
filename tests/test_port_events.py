"""Tests for port_events orchestration helpers (the teleport pre-filter).

The state-machine walk itself is covered by test_state_machine.py; here we pin
the upstream guard that strips MMSI-collision / GPS-spoof spikes before the walk
ever sees them.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pipeline.port_events import (
    TELEPORT_MAX_KN,
    TELEPORT_MIN_NM,
    _drop_teleports,
)
from pipeline.state_machine import Fix

T0 = datetime(2026, 5, 15, 0, 0, tzinfo=timezone.utc)


def at(minutes: float) -> datetime:
    return T0 + timedelta(minutes=minutes)


def fix(minutes: float, lat: float, lon: float, sog: float = 16.0) -> Fix:
    return Fix(at(minutes), lat, lon, sog, None, ())


def fresh_summary() -> dict:
    return {"teleport_fixes_dropped": 0}


def test_drops_interleaved_collision_spikes():
    # Real track: a smooth approach to Zeebrugge (~51.36N). Interleaved spikes
    # ~380 nm north (~57.7N) at 1-min spacing — the Fedor Litke MMSI collision.
    s = fresh_summary()
    fixes = [
        fix(0, 51.364, 2.864),
        fix(1, 57.702, 4.303),  # spike
        fix(2, 51.368, 2.905),
        fix(3, 51.372, 2.931),
        fix(4, 57.761, 4.300),  # spike
        fix(5, 51.382, 2.973),
    ]
    kept = _drop_teleports(fixes, s)
    assert s["teleport_fixes_dropped"] == 2
    assert [round(f.lat, 1) for f in kept] == [51.4, 51.4, 51.4, 51.4]


def test_keeps_consecutive_spikes_against_real_track():
    # Two spikes in a row must both be rejected — the gate compares to the last
    # *accepted* fix, not the raw previous one, so it doesn't lock onto the ghost.
    s = fresh_summary()
    fixes = [
        fix(0, 51.36, 2.86),
        fix(1, 57.70, 4.30),  # spike
        fix(2, 57.76, 4.30),  # spike (consecutive)
        fix(3, 51.37, 2.90),
    ]
    kept = _drop_teleports(fixes, s)
    assert s["teleport_fixes_dropped"] == 2
    assert all(f.lat < 52 for f in kept)


def test_keeps_long_hop_after_ais_dropout():
    # A 600 nm move over three dark days is slow (~8 kn), not a teleport — speed,
    # not raw distance, is what the gate keys on.
    s = fresh_summary()
    fixes = [
        fix(0, 29.7, -93.85),
        fix(3 * 24 * 60, 38.0, -75.0),  # ~700 nm, 3 days later
    ]
    kept = _drop_teleports(fixes, s)
    assert s["teleport_fixes_dropped"] == 0
    assert len(kept) == 2


def test_keeps_near_stationary_jitter():
    # Sub-mile GPS jitter at the berth over a few seconds can imply a high
    # instantaneous speed, but the distance floor keeps it from being dropped.
    s = fresh_summary()
    fixes = [
        fix(0.0, 51.3467, 3.2105, sog=0.9),
        fix(1 / 60, 51.3469, 3.2121, sog=1.6),  # ~0.07 nm in 1 s
    ]
    kept = _drop_teleports(fixes, s)
    assert s["teleport_fixes_dropped"] == 0
    assert len(kept) == 2


def test_thresholds_are_sane():
    # Guard against an accidental edit that lets a real LNG carrier (≤~20 kn) trip
    # the gate, or sets the distance floor so high a real teleport slips through.
    assert TELEPORT_MAX_KN > 25.0
    assert 1.0 < TELEPORT_MIN_NM < 50.0
