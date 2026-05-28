"""Tests for the layered laden inference (lookahead window + flow_direction
fallback) and for the envelope side-of-moored classifier in port_events.py.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pipeline.laden import build_draught_lookup, infer_laden
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
# Lookahead window: outbound events prefer a post-event draught reading
# ----------------------------------------------------------------------


def test_outbound_uses_post_event_draught_when_available():
    """The GASLOG GENEVA case: vessel sits at berth at laden draught,
    undocks, post-discharge draught is reported ~60 min after the event.
    The lookahead window catches it and flips laden -> False."""
    lookup = build_draught_lookup(
        [
            (MMSI, at(-1440), 11.0),  # 24h before: laden, while at berth
            (MMSI, at(-60), 11.0),  # 1h before: still laden, just before undocking
            (MMSI, at(57), 9.3),  # 57 min after: post-discharge draught
        ]
    )
    assert infer_laden(MMSI, at(0), "post", "import", DESIGN, lookup) == (
        False,
        "draught",
    )


def test_outbound_falls_back_to_flow_direction_when_no_post_event_draught():
    """Pre-event draught only — too stale to trust on an outbound event."""
    lookup = build_draught_lookup(
        [
            (MMSI, at(-1440), 11.0),  # 24h before: laden, pre-discharge
            (MMSI, at(-60), 11.0),  # 1h before: still laden
            # No post-event draught yet
        ]
    )
    assert infer_laden(MMSI, at(0), "post", "import", DESIGN, lookup) == (
        False,
        "flow_direction",
    )
    assert infer_laden(MMSI, at(0), "post", "export", DESIGN, lookup) == (
        True,
        "flow_direction",
    )


def test_outbound_ignores_pre_event_draught_even_if_recent():
    """A reading from -45 min still reflects the pre-discharge state — vessel
    hadn't physically undocked yet. Don't trust it on an outbound event."""
    lookup = build_draught_lookup([(MMSI, at(-45), 11.0)])
    assert infer_laden(MMSI, at(0), "post", "import", DESIGN, lookup) == (
        False,
        "flow_direction",
    )


def test_outbound_post_event_draught_outside_window_falls_back():
    """A post-event reading more than LOOKAHEAD_FORWARD (6h) away is ignored."""
    lookup = build_draught_lookup([(MMSI, at(8 * 60), 9.3)])
    assert infer_laden(MMSI, at(0), "post", "import", DESIGN, lookup) == (
        False,
        "flow_direction",
    )


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
