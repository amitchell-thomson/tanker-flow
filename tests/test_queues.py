"""Unit tests for the anchorage-queue pairing (pipeline.queues).

Pure-logic: synthetic QueueEvent objects, no DB. Mirrors tests/test_visits.py.
"""

from __future__ import annotations

from datetime import timedelta

from config import REGIME_CUTOVER
from pipeline.queues import QueueEvent, pair_queues


def at(hours: float):
    return REGIME_CUTOVER + timedelta(hours=hours)


def ev(event_type, hours, *, mmsi=1, terminal_id=1, zone="usgulf",
       laden_flag=False, cold_start=False, source="noaa-ais"):
    return QueueEvent(
        mmsi=mmsi, event_type=event_type, event_time=at(hours), zone=zone,
        terminal_id=terminal_id, laden_flag=laden_flag, cold_start=cold_start,
        source=source,
    )


FLOWS = {1: "export", 2: "import"}


def test_basic_queue_entry_to_moored():
    evs = [ev("anchorage_entry", 0), ev("anchored", 1), ev("moored", 25)]
    qs = pair_queues(evs, flow_directions=FLOWS)
    assert len(qs) == 1
    q = qs[0]
    assert q.queue_h == 25.0
    assert q.anchored_seen is True
    assert q.flow_direction == "export"
    assert q.moored_ts is not None


def test_reentry_jitter_absorbed_into_one_queue():
    # exit/re-enter while leaving the anchorage for berth must NOT split the queue;
    # entry_ts stays the first, and one queue is emitted.
    evs = [
        ev("anchorage_entry", 0), ev("anchored", 1),
        ev("anchorage_exit", 20), ev("anchorage_entry", 21),  # jitter
        ev("anchorage_exit", 22), ev("moored", 24),
    ]
    qs = pair_queues(evs, flow_directions=FLOWS)
    assert len(qs) == 1
    assert qs[0].entry_ts == at(0)  # first entry, not the re-entry
    assert qs[0].queue_h == 24.0
    assert qs[0].anchorage_dwell_h == 22.0  # first entry → last exit before moored


def test_anchored_seen_false_for_driveby_clip():
    # entry → moored with no dwell-confirmed anchored = a drive-by clip (not a real
    # wait): counted in #15 (queued) but not #16 (meaningful).
    evs = [ev("anchorage_entry", 0), ev("moored", 3)]
    qs = pair_queues(evs, flow_directions=FLOWS)
    assert qs[0].anchored_seen is False


def test_direct_berth_emits_no_queue():
    evs = [ev("moored", 0), ev("departed", 30)]
    assert pair_queues(evs, flow_directions=FLOWS) == []


def test_departed_without_mooring_discards_run():
    # anchored then left without berthing here — not a queue-to-berth.
    evs = [ev("anchorage_entry", 0), ev("anchored", 1), ev("departed", 10)]
    assert pair_queues(evs, flow_directions=FLOWS) == []


def test_open_queue_when_still_waiting():
    evs = [ev("anchorage_entry", 0), ev("anchored", 1)]
    qs = pair_queues(evs, flow_directions=FLOWS)
    assert len(qs) == 1
    assert qs[0].moored_ts is None  # open
    assert qs[0].queue_h is None
    assert qs[0].anchored_seen is True


def test_mooring_past_window_cap_discards_run():
    # a moored 40 days after entry belongs to a later call — don't span the gap;
    # the stale run is discarded (no phantom 40-day "queue").
    evs = [ev("anchorage_entry", 0), ev("moored", 40 * 24)]
    assert pair_queues(evs, max_pair_days=30, flow_directions=FLOWS) == []


def test_two_separate_queues_for_one_vessel():
    evs = [
        ev("anchorage_entry", 0), ev("moored", 10), ev("departed", 50),
        ev("anchorage_entry", 100), ev("anchored", 101), ev("moored", 130),
    ]
    qs = pair_queues(evs, flow_directions=FLOWS)
    assert len(qs) == 2
    assert qs[0].queue_h == 10.0
    assert qs[1].queue_h == 30.0


def test_cross_terminal_mooring_does_not_pair():
    # anchorage at terminal 1 (US), next mooring at terminal 2 (EU) 16 days later =
    # a whole voyage, NOT a queue. Must not pair into a cross-ocean "queue".
    evs = [
        ev("anchorage_entry", 0, terminal_id=1, zone="usgulf"),
        ev("moored", 16 * 24, terminal_id=2, zone="nweurope"),
    ]
    assert pair_queues(evs, flow_directions=FLOWS) == []


def test_weights_and_regime_attached():
    evs = [ev("anchorage_entry", 0), ev("moored", 12)]
    qs = pair_queues(evs, weights={1: (120_000, 170_000)}, flow_directions=FLOWS)
    assert qs[0].dwt == 120_000 and qs[0].gas_capacity_m3 == 170_000
    assert qs[0].regime == "noaa"  # source noaa-ais → noaa regime
