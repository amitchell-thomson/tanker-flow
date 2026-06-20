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
    ETA_IMMINENT_HOURS,
    ETA_PAST_GRACE_HOURS,
    ETA_STICKY_PAST_HOURS,
    FSRU_TIER,
    MANUAL_TIER_OVERRIDES,
    PIN_MAX,
    PIN_POST_WINDOW_DAYS,
    PIN_PRE_WINDOW_DAYS,
    _select_open_leg_pins,
    apply_manual_override,
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


def test_manual_override_forces_tier_and_pins_score():
    # An MMSI in the override map gets its forced tier with a score pinned above
    # any genuine fix in that tier, regardless of the computed (tier, score).
    mmsi, forced = next(iter(MANUAL_TIER_OVERRIDES.items()))
    tier, reason, score = apply_manual_override(
        mmsi, tier=5, reason="stale @ 2026-05-20", score=1.0, now=NOW
    )
    assert tier == forced
    assert "manual-override" in reason
    assert score > NOW.timestamp()  # out-sorts every real fix in the tier


def test_manual_override_is_a_noop_for_unlisted_mmsi():
    # A vessel not in the map passes through untouched.
    unlisted = -1  # never a real MMSI
    assert unlisted not in MANUAL_TIER_OVERRIDES
    result = apply_manual_override(
        unlisted, tier=3, reason="in-zone:bbox @ 2026-05-30", score=42.0, now=NOW
    )
    assert result == (3, "in-zone:bbox @ 2026-05-30", 42.0)


def test_imminent_eta_without_dest_promotes_to_tier_2():
    # VENTURE CREOLE case: a ballast carrier broadcasting "FOR ORDERS" (so
    # dest_terminal_id is NULL) with a real imminent ETA must still take a
    # persistent slot, not decay to tier 3 and go dark on final approach.
    tier, reason, _score = _assign(
        False,
        dest_terminal_id=None,
        state_ts=None,
        parsed_eta=NOW + timedelta(hours=6),
        last_bbox_fix_ts=NOW - timedelta(days=2),  # would otherwise be tier 3
    )
    assert tier == 2
    assert "eta:for-orders" in reason


def test_imminent_eta_with_dest_keeps_terminal_label():
    tier, reason, _score = _assign(
        False,
        dest_terminal_id=7,
        parsed_eta=NOW + timedelta(hours=6),
    )
    assert tier == 2
    assert "terminal_id=7" in reason


def test_just_passed_eta_still_holds_slot_within_grace():
    # A vessel running slightly late (ETA a few hours ago) is at its most
    # arrival-critical moment — it must stay in tier 2 through berthing.
    tier, reason, _score = _assign(
        False,
        dest_terminal_id=None,
        parsed_eta=NOW - timedelta(hours=ETA_PAST_GRACE_HOURS - 2),
        last_bbox_fix_ts=NOW - timedelta(days=2),
    )
    assert tier == 2
    assert "ago" in reason


def test_eta_in_sticky_tail_without_arrival_stays_tier_2():
    # Past the grace window but inside the sticky tail, with no terminal-polygon
    # fix at/after the ETA: a dark, overdue inbound carrier we have not seen
    # arrive — hold it in the persistent band (GREENERGY OCEAN, 2026-06).
    tier, reason, _score = _assign(
        False,
        dest_terminal_id=None,
        parsed_eta=NOW - timedelta(hours=ETA_PAST_GRACE_HOURS + 36),
        last_bbox_fix_ts=NOW - timedelta(days=2),
    )
    assert tier == 2
    assert "ago" in reason


def test_eta_in_sticky_tail_but_already_arrived_does_not_pin():
    # Overdue ETA inside the sticky tail, but a terminal-polygon fix landed AFTER
    # it (and is now >3d old, so tier 1 no longer fires) — the vessel
    # demonstrably arrived, so the stale ETA must not re-pin it. Falls through to
    # its position tier (bbox → tier 3) instead of sticking at tier 2.
    eta = NOW - timedelta(hours=90)  # in (grace, sticky] window
    arrival = eta + timedelta(hours=2)  # after the ETA, ~3.7d ago (> 3d window)
    tier, _reason, _score = _assign(
        False,
        dest_terminal_id=None,
        parsed_eta=eta,
        last_polygon_fix_ts=arrival,
        last_approach_fix_ts=arrival,
        last_bbox_fix_ts=NOW - timedelta(days=2),
    )
    assert tier == 3


def test_eta_past_sticky_window_does_not_promote():
    # Beyond the sticky tail the ETA is genuinely stale, not late — fall through.
    tier, _reason, _score = _assign(
        False,
        dest_terminal_id=None,
        parsed_eta=NOW - timedelta(hours=ETA_STICKY_PAST_HOURS + 2),
        last_bbox_fix_ts=NOW - timedelta(days=2),
    )
    assert tier == 3


def test_non_imminent_eta_without_dest_does_not_promote():
    # An ETA beyond the horizon is not an arrival signal; with no other freshness
    # the vessel falls through to its position-based tier (here tier 3 via bbox).
    tier, reason, _score = _assign(
        False,
        dest_terminal_id=None,
        parsed_eta=NOW + timedelta(hours=ETA_IMMINENT_HOURS + 12),
        last_bbox_fix_ts=NOW - timedelta(days=2),
    )
    assert tier == 3
    assert "for-orders" not in reason


def test_sooner_eta_outscores_later_eta_within_tier_2():
    _t1, _r1, soon = _assign(False, parsed_eta=NOW + timedelta(hours=3))
    _t2, _r2, later = _assign(False, parsed_eta=NOW + timedelta(hours=40))
    assert soon > later


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
