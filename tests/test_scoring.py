"""Tests for pipeline.scoring pin selection.

Focus on `_select_open_leg_pins` — the pure window-gating + arrival-ordering
core of the open-leg pin (the DB fetch is a thin wrapper around it). These pin
down the behaviours the appear-in-berth audit established:
  - both directions are eligible (laden->import and ballast->export),
  - mid-ocean (too-early) and stale (too-late) legs are excluded,
  - when the cap binds, the vessels closest to arrival win the slots.
"""

from datetime import datetime, timedelta, timezone

from pipeline.scoring import (
    DEFAULT_VOYAGE_DAYS,
    FSRU_TIER,
    PIN_MAX,
    PIN_POST_WINDOW_DAYS,
    PIN_PRE_WINDOW_DAYS,
    _select_open_leg_pins,
    assign_tier,
)

NOW = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)


def _assign(is_fsru: bool, **overrides):
    """assign_tier with neutral defaults; override only what a case exercises."""
    kwargs = dict(
        is_fsru=is_fsru,
        last_berth_fix_ts=None,
        last_anchorage_fix_ts=None,
        last_approach_fix_ts=None,
        last_polygon_fix_ts=None,
        last_bbox_fix_ts=None,
        last_fix_ts=None,
        dest_terminal_id=None,
        state_ts=None,
        parsed_eta=None,
        dist_km=None,
        bearing_deg=None,
        last_cog=None,
        now=NOW,
    )
    kwargs.update(overrides)
    return assign_tier(**kwargs)


def test_fsru_at_berth_is_forced_to_low_freq_band():
    # An FSRU sitting in a berth would normally score tier 1; it must instead be
    # demoted out of the persistent band to the dedicated FSRU scan band.
    berth_fix = NOW - timedelta(hours=1)
    tier, reason, _score = _assign(
        True, last_berth_fix_ts=berth_fix, last_polygon_fix_ts=berth_fix
    )
    assert tier == FSRU_TIER
    assert tier > 3  # never holds a persistent slot
    assert "fsru" in reason


def test_non_fsru_at_berth_still_scores_tier_1():
    # Same inputs, not an FSRU — the regular tier-1 path is untouched.
    berth_fix = NOW - timedelta(hours=1)
    tier, _reason, _score = _assign(
        False, last_berth_fix_ts=berth_fix, last_polygon_fix_ts=berth_fix
    )
    assert tier == 1


def _leg(
    mmsi: int, days_ago: float, zone: str | None
) -> tuple[int, datetime, str | None]:
    return (mmsi, NOW - timedelta(days=days_ago), zone)


def test_in_window_leg_is_pinned():
    # Departed usgulf 16d ago (expected_voyage 16) -> expected_arrival == now.
    pins = _select_open_leg_pins([_leg(1, 16, "usgulf")], NOW)
    assert pins == {1}


def test_ballast_return_to_export_is_pinned():
    # The dominant miss: ballast leg out of an EU import zone, due at a US export
    # terminal. nweurope expected_voyage 15 -> in window at 15d out.
    pins = _select_open_leg_pins([_leg(2, 15, "nweurope")], NOW)
    assert pins == {2}


def test_too_early_mid_ocean_leg_excluded():
    # Just departed: still mid-ocean, slot would idle. usgulf voyage 16,
    # pre-window 4 -> window opens at 12d out; 3d out is well before it.
    pins = _select_open_leg_pins([_leg(3, 3, "usgulf")], NOW)
    assert pins == set()


def test_stale_overdue_leg_excluded():
    # Far past expected arrival + post-window -> floating storage / missed, drop.
    too_old = DEFAULT_VOYAGE_DAYS + PIN_POST_WINDOW_DAYS + 5
    pins = _select_open_leg_pins([_leg(4, too_old, "usgulf")], NOW)
    assert pins == set()


def test_window_edges_are_inclusive():
    voyage = DEFAULT_VOYAGE_DAYS  # unknown zone -> default
    # Earliest edge: expected_arrival - pre == now.
    early = _select_open_leg_pins([_leg(5, voyage - PIN_PRE_WINDOW_DAYS, None)], NOW)
    # Latest edge: expected_arrival + post == now.
    late = _select_open_leg_pins([_leg(6, voyage + PIN_POST_WINDOW_DAYS, None)], NOW)
    assert early == {5}
    assert late == {6}


def test_cap_keeps_vessels_closest_to_arrival():
    # More than PIN_MAX in-window legs, all usgulf (voyage 16). The window is
    # days_ago in [voyage - pre, voyage + post] = [12, 24]; pack N legs across it
    # with fractional spacing so arrival times are distinct. Oldest-departed
    # (largest days_ago -> most overdue -> earliest expected_arrival) must win.
    n = PIN_MAX + 5
    lo, hi = 16 - PIN_PRE_WINDOW_DAYS, 16 + PIN_POST_WINDOW_DAYS  # [12, 24]
    legs = []
    for i in range(n):  # i=0 -> hi (oldest), i=n-1 -> lo (youngest)
        days_ago = hi - (hi - lo) * i / (n - 1)
        legs.append(_leg(1000 + i, days_ago, "usgulf"))
    assert len(legs) > PIN_MAX
    pins = _select_open_leg_pins(legs, NOW)
    assert len(pins) == PIN_MAX
    # The oldest-departed (closest to / past expected arrival) win: the first
    # PIN_MAX entries of our oldest-first list.
    expected = {1000 + i for i in range(PIN_MAX)}
    assert pins == expected


def test_unknown_zone_uses_default_window():
    # None zone -> DEFAULT_VOYAGE_DAYS; in window at exactly default days out.
    pins = _select_open_leg_pins([_leg(7, DEFAULT_VOYAGE_DAYS, None)], NOW)
    assert pins == {7}
