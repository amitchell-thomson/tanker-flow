"""Tests for the layered laden inference (lookahead window + flow_direction
fallback) and for the envelope side-of-moored classifier in port_events.py.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pipeline.laden import (
    build_draught_lookup,
    infer_laden,
    sanitize_design_draughts,
)
from pipeline.port_events import _classify_envelope_sides
from pipeline.state_machine import Event


T0 = datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)
DESIGN = 12.0  # laden threshold = 10.2 m
MMSI = 310755000


def at(minutes: float) -> datetime:
    return T0 + timedelta(minutes=minutes)


def mk_event(event_type: str, terminal_id: int = 10, minutes: float = 0) -> Event:
    return Event(
        event_type=event_type,
        event_time=at(minutes),
        terminal_id=terminal_id,
        lat=52.0,
        lon=4.0,
    )


# ----------------------------------------------------------------------
# Outbound events: flow_direction is primary at a known-flow terminal; the
# post-event draught lookahead is only the fallback when flow is unknown.
# ----------------------------------------------------------------------


def test_outbound_known_flow_prefers_flow_direction_over_stale_draught():
    """Regression for the NW-Europe mislabel: a vessel leaves an import terminal
    in ballast, but the master is slow to update and still broadcasts the laden
    draught inside the +6h window. flow_direction must win at a known-flow
    terminal so the departure isn't mislabelled laden."""
    lookup = build_draught_lookup(
        [
            (MMSI, at(-60), 11.0),  # laden, pre-undock
            (MMSI, at(57), 11.0),  # STILL laden 57 min after undock (stale)
        ]
    )
    # import terminal: leaves ballast, despite the stale laden draught reading.
    assert infer_laden(MMSI, at(0), "post", "import", DESIGN, lookup) == (
        False,
        "flow_direction",
    )
    # export terminal: leaves laden.
    assert infer_laden(MMSI, at(0), "post", "export", DESIGN, lookup) == (
        True,
        "flow_direction",
    )


def test_outbound_unknown_flow_uses_post_event_draught():
    """When the terminal flow_direction is unknown, fall back to the post-event
    draught reading inside the lookahead window."""
    lookup = build_draught_lookup(
        [
            (MMSI, at(-60), 11.0),  # pre-undock (ignored — pre-event)
            (MMSI, at(57), 9.3),  # post-discharge draught, 57 min after
        ]
    )
    assert infer_laden(MMSI, at(0), "post", None, DESIGN, lookup) == (
        False,
        "draught",
    )


def test_outbound_unknown_flow_ignores_pre_event_draught():
    """Unknown flow + only a pre-event reading: pre-event draught is stale on an
    outbound event, so there is nothing trustworthy to say."""
    lookup = build_draught_lookup([(MMSI, at(-45), 11.0)])
    assert infer_laden(MMSI, at(0), "post", None, DESIGN, lookup) == (None, None)


def test_outbound_unknown_flow_post_draught_outside_window_returns_null():
    """Unknown flow + post reading beyond LOOKAHEAD_FORWARD (6h): ignored."""
    lookup = build_draught_lookup([(MMSI, at(8 * 60), 9.3)])
    assert infer_laden(MMSI, at(0), "post", None, DESIGN, lookup) == (None, None)


def test_inbound_uses_forward_fill_not_lookahead():
    """For pre-moored events the forward-fill behavior is unchanged.
    Future draught readings must NOT influence pre-moored inference."""
    lookup = build_draught_lookup(
        [
            (MMSI, at(-1440), 11.0),  # laden draught reported pre-event
            (MMSI, at(60), 9.3),  # later (post-discharge) draught
        ]
    )
    assert infer_laden(MMSI, at(0), "pre", "import", DESIGN, lookup) == (
        True,
        "draught",
    )


# ----------------------------------------------------------------------
# Flow-direction fallback when no draught at all
# ----------------------------------------------------------------------


def test_flow_direction_only_pre_moored_import():
    lookup: dict = {}
    assert infer_laden(MMSI, at(0), "pre", "import", DESIGN, lookup) == (
        True,
        "flow_direction",
    )
    assert infer_laden(MMSI, at(0), "moored", "import", DESIGN, lookup) == (
        True,
        "flow_direction",
    )
    assert infer_laden(MMSI, at(0), "post", "import", DESIGN, lookup) == (
        False,
        "flow_direction",
    )


def test_flow_direction_only_pre_moored_export():
    lookup: dict = {}
    assert infer_laden(MMSI, at(0), "pre", "export", DESIGN, lookup) == (
        False,
        "flow_direction",
    )
    assert infer_laden(MMSI, at(0), "moored", "export", DESIGN, lookup) == (
        False,
        "flow_direction",
    )
    assert infer_laden(MMSI, at(0), "post", "export", DESIGN, lookup) == (
        True,
        "flow_direction",
    )


def test_no_moored_envelope_returns_null():
    """Anchorage-only visit: the cargo didn't change at this terminal. Without
    a draught reading we can't infer either way."""
    lookup: dict = {}
    assert infer_laden(MMSI, at(0), "no_moored", "import", DESIGN, lookup) == (
        None,
        None,
    )
    assert infer_laden(MMSI, at(0), "no_moored", "export", DESIGN, lookup) == (
        None,
        None,
    )


def test_no_flow_direction_no_draught_returns_null():
    """Unknown terminal flow_direction + no draught = no answer."""
    lookup: dict = {}
    assert infer_laden(MMSI, at(0), "pre", None, DESIGN, lookup) == (None, None)


def test_no_design_draught_falls_back_to_flow_direction():
    """If a vessel has no design_draught in vessel_registry, draught comparisons
    can't be made — but flow_direction can still answer."""
    lookup = build_draught_lookup([(MMSI, at(-60), 11.0)])
    assert infer_laden(MMSI, at(0), "post", "import", None, lookup) == (
        False,
        "flow_direction",
    )


# ----------------------------------------------------------------------
# Envelope side classifier
# ----------------------------------------------------------------------


def test_classify_full_envelope_with_moored():
    events = [
        mk_event("zone_entry", minutes=0),
        mk_event("anchorage_entry", minutes=5),
        mk_event("anchored", minutes=35),
        mk_event("anchorage_exit", minutes=60),
        mk_event("moored", minutes=70),
        mk_event("departed", minutes=120),
        mk_event("zone_exit", minutes=150),
    ]
    sides = _classify_envelope_sides(events)
    assert sides == ["pre", "pre", "pre", "pre", "moored", "post", "post"]


def test_classify_anchorage_only_envelope():
    """No moored in this envelope -> every event marked 'no_moored'."""
    events = [
        mk_event("zone_entry", minutes=0),
        mk_event("anchorage_entry", minutes=5),
        mk_event("anchored", minutes=35),
        mk_event("anchorage_exit", minutes=60),
        mk_event("zone_exit", minutes=70),
    ]
    sides = _classify_envelope_sides(events)
    assert sides == ["no_moored", "no_moored", "no_moored", "no_moored", "no_moored"]


def test_classify_two_envelopes_back_to_back():
    """A vessel can visit twice in a row (zone_entry, zone_exit, zone_entry, ...).
    Each envelope is classified independently."""
    events = [
        # Envelope 1: anchorage-only abort
        mk_event("zone_entry", terminal_id=10, minutes=0),
        mk_event("anchorage_entry", terminal_id=10, minutes=5),
        mk_event("anchored", terminal_id=10, minutes=35),
        mk_event("anchorage_exit", terminal_id=10, minutes=60),
        mk_event("zone_exit", terminal_id=10, minutes=70),
        # Envelope 2: re-entry and full visit
        mk_event("zone_entry", terminal_id=10, minutes=100),
        mk_event("moored", terminal_id=10, minutes=130),
        mk_event("zone_exit", terminal_id=10, minutes=300),
    ]
    sides = _classify_envelope_sides(events)
    assert sides == [
        "no_moored",
        "no_moored",
        "no_moored",
        "no_moored",
        "no_moored",
        "pre",
        "moored",
        "post",
    ]


def test_classify_open_visit_no_zone_exit():
    """Cold-end: vessel still moored at end of data window. Envelope has
    no zone_exit but the events should still classify correctly."""
    events = [
        mk_event("zone_entry", minutes=0),
        mk_event("moored", minutes=30),
    ]
    sides = _classify_envelope_sides(events)
    assert sides == ["pre", "moored"]


# ----------------------------------------------------------------------
# Design-draught sanitization: a single implausible masterdata value must
# not push a laden cargo below the 0.85×design threshold (the Krk/TESSALA bug).
# ----------------------------------------------------------------------


def test_sanitize_replaces_implausible_with_fleet_median():
    raw = {1: 12.0, 2: 12.5, 3: 13.0, 4: 15.0, 5: None, 6: 0.0}
    out = sanitize_design_draughts(raw)
    # plausible sorted = [12.0, 12.5, 13.0] → median = 12.5
    assert out[1] == 12.0 and out[2] == 12.5 and out[3] == 13.0
    assert out[4] == 12.5   # 15 m is out-of-band → fleet median
    assert out[5] is None   # unenriched left as-is (→ flow_direction fallback)
    assert out[6] == 0.0     # unenriched left as-is


def test_sanitize_no_plausible_values_passes_through():
    raw = {1: None, 2: 0.0, 3: 15.0}
    assert sanitize_design_draughts(raw) == raw   # nothing to median over


def test_sanitize_does_not_mutate_input():
    raw = {1: 12.0, 2: 15.0}
    _ = sanitize_design_draughts(raw)
    assert raw == {1: 12.0, 2: 15.0}


def test_sanitize_fixes_laden_misclassification():
    # TESSALA at Krk: design 15 m (bad), draught 11.2 m, moored at an import berth.
    lookup = build_draught_lookup([(MMSI, at(0), 11.2)])
    raw = {MMSI: 15.0, 901: 12.0, 902: 12.5, 903: 13.0}
    fixed = sanitize_design_draughts(raw)

    # Before: 11.2 < 0.85*15 = 12.75 → misread as ballast.
    bad, _ = infer_laden(MMSI, at(10), "moored", "import", raw[MMSI], lookup)
    assert bad is False
    # After: design → fleet median 12.5, 11.2 >= 0.85*12.5 = 10.625 → laden.
    ok, src = infer_laden(MMSI, at(10), "moored", "import", fixed[MMSI], lookup)
    assert ok is True and src == "draught"
