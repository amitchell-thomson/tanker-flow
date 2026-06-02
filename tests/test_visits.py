"""Unit tests for port-visit pairing (pipeline.visits).

Pure-logic: synthetic VisitEvent lists, no DB. Mirrors tests/test_legs.py.
"""

from __future__ import annotations

from datetime import timedelta

from config import REGIME_CUTOVER
from pipeline.visits import VisitEvent, pair_visits


NOW = REGIME_CUTOVER + timedelta(days=40)


def ev(mmsi, etype, t, zone, terminal_id, laden=None, cold_start=False):
    return VisitEvent(
        mmsi=mmsi,
        event_type=etype,
        event_time=t,
        zone=zone,
        terminal_id=terminal_id,
        laden_flag=laden,
        cold_start=cold_start,
    )


def at(days: float):
    return REGIME_CUTOVER - timedelta(days=20) + timedelta(days=days)


FLOW = {1: "export", 10: "import"}


def test_closed_visit_paired():
    events = [
        ev(1, "moored", at(0), "usgulf", 1, laden=False),
        ev(1, "departed", at(1), "usgulf", 1, laden=True),
    ]
    visits = pair_visits(events, flow_directions=FLOW)
    assert len(visits) == 1
    v = visits[0]
    assert v.terminal_id == 1 and v.zone == "usgulf"
    assert v.flow_direction == "export"
    assert v.moored_ts == at(0) and v.departed_ts == at(1)
    assert v.laden is False  # laden of the moored (arrival) event


def test_open_visit_has_no_departed():
    events = [ev(2, "moored", at(0), "nweurope", 10, laden=True)]
    visits = pair_visits(events, flow_directions=FLOW)
    assert len(visits) == 1
    assert visits[0].departed_ts is None
    assert visits[0].flow_direction == "import"


def test_weights_attached():
    events = [ev(3, "moored", at(0), "usgulf", 1)]
    visits = pair_visits(events, weights={3: (90_000, 170_000)}, flow_directions=FLOW)
    assert (visits[0].dwt, visits[0].gas_capacity_m3) == (90_000, 170_000)
    # No weight entry → Nones.
    assert pair_visits(events)[0].gas_capacity_m3 is None


def test_multiple_visits_pair_to_their_own_departed():
    events = [
        ev(4, "moored", at(0), "usgulf", 1, laden=False),
        ev(4, "departed", at(1), "usgulf", 1, laden=True),
        ev(4, "moored", at(15), "nweurope", 10, laden=True),
        ev(4, "departed", at(16), "nweurope", 10, laden=False),
    ]
    visits = pair_visits(events, flow_directions=FLOW)
    assert [(v.terminal_id, v.moored_ts, v.departed_ts) for v in visits] == [
        (1, at(0), at(1)),
        (10, at(15), at(16)),
    ]


def test_regime_tag_from_moored_time():
    events = [
        ev(5, "moored", REGIME_CUTOVER - timedelta(days=1), "usgulf", 1),
        ev(6, "moored", REGIME_CUTOVER + timedelta(days=1), "usgulf", 1),
    ]
    visits = {v.mmsi: v for v in pair_visits(events)}
    assert visits[5].regime == "bbox"
    assert visits[6].regime == "mmsi_filter"


def test_cold_start_visit_kept():
    # A vessel first seen already alongside still occupies the berth — kept.
    events = [
        ev(7, "moored", at(0), "nweurope", 10, laden=True, cold_start=True),
        ev(7, "departed", at(1), "nweurope", 10, laden=False),
    ]
    visits = pair_visits(events, flow_directions=FLOW)
    assert len(visits) == 1 and visits[0].cold_start is True


def test_departed_without_moored_ignored():
    # A lone `departed` (no preceding `moored`) starts no visit.
    events = [ev(8, "departed", at(0), "usgulf", 1, laden=True)]
    assert pair_visits(events, flow_directions=FLOW) == []
