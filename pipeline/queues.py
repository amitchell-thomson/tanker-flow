"""Anchorage-queue pairing — the wait before berthing.

The third pairing foundation, alongside `legs.py` (at-sea voyages) and `visits.py`
(berth occupancy). A *queue* pairs the **first** `anchorage_entry` of a run with the
vessel's next `moored` at the terminal — the total wait from reaching the anchorage
to going alongside. Together the three describe the whole port call:
queue → berth → voyage.

────────────────────────────────────────────────────────────────────────────
Why `anchorage_entry → moored`, and why it is robust
────────────────────────────────────────────────────────────────────────────
A vessel leaving the anchorage for its berth frequently clips the anchorage polygon
boundary several times (GPS jitter / the path out), emitting spurious
`anchorage_exit`/`anchorage_entry` pairs mid-transit. Bracketing the wait as
*first entry → final moored* and **ignoring every intermediate crossing** makes the
queue time immune to that flapping — the metric never sees the intermediate events.
(A standalone "time at anchor" = `anchored → anchorage_exit` would be defined *by*
those very crossings and is therefore fragile; we keep `anchorage_dwell_h` only as a
secondary diagnostic field, first-entry → last-exit-before-moored, re-entries
absorbed — not a headline signal.)

queue_time = anchor-wait + channel-transit; the channel transit is ~constant per
terminal (the `approach` envelope contains it), so it washes out of the
congestion *anomaly* the signal layer cares about. The dwell-confirmed `anchored`
between entry and moored is recorded as `anchored_seen` — the "did it genuinely
wait" flag that separates a real queue from a drive-by clip (#15 vs #16).

────────────────────────────────────────────────────────────────────────────
Historical depth (SIGNALS.md §0·6·1)
────────────────────────────────────────────────────────────────────────────
The live state machine emits `anchorage_entry` on NOAA fixes, so US-Gulf queues
reconstruct to 2016 (~5k entries) — trainable. GFW voyage arcs carry no anchorage
events, so EU queues exist only from the live `mmsi_filter` cutover — a thin,
live-only nowcast layer, which §3.5 of the backfill plan argues is tolerable
(EU queue time is structurally near-zero in normal markets).

Pure `pair_queues` + thin `compute_queues(pool, now)`, mirroring visits.py.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

import asyncpg

from config import regime_of

logger = logging.getLogger(__name__)

# A `moored` more than this long after the run's anchorage_entry belongs to a later
# port call (or the live block after a data gap), not this wait — leave the queue
# open (capped downstream) rather than spanning the gap. Mirrors visits.py.
MAX_QUEUE_PAIR_DAYS = 30


@dataclass(frozen=True)
class QueueEvent:
    """A port_events row relevant to anchorage queuing."""

    mmsi: int
    event_type: str  # anchorage_entry | anchored | anchorage_exit | moored | departed
    event_time: datetime
    zone: str
    terminal_id: int | None
    laden_flag: bool | None
    cold_start: bool = False
    source: str = "state_machine"


@dataclass(frozen=True)
class Queue:
    """One anchorage wait → berth (or an open wait still in progress)."""

    mmsi: int
    terminal_id: int | None
    zone: str
    flow_direction: str | None  # 'export' | 'import' | None (from terminals)
    entry_ts: datetime  # first anchorage_entry of the run
    moored_ts: datetime | None  # None ⇒ still waiting (open queue)
    anchored_ts: datetime | None  # first dwell-confirmed anchored in the run
    last_exit_ts: datetime | None  # last anchorage_exit before mooring
    anchored_seen: bool  # a dwell-confirmed `anchored` occurred (real wait vs clip)
    laden: bool | None
    regime: str
    cold_start: bool = False
    dwt: int | None = None
    gas_capacity_m3: int | None = None

    @property
    def queue_h(self) -> float | None:
        """Observed wait (h) for a completed queue; None while open."""
        if self.moored_ts is None:
            return None
        return (self.moored_ts - self.entry_ts).total_seconds() / 3600.0

    @property
    def anchorage_dwell_h(self) -> float | None:
        """Secondary diagnostic: first-entry → last-exit-before-moored (re-entries
        absorbed) — time inside the anchorage polygon, excluding the channel transit
        to berth. None when no exit was observed. Not a headline signal; kept for the
        later queue = anchor-wait + channel-transit decomposition."""
        if self.last_exit_ts is None:
            return None
        return (self.last_exit_ts - self.entry_ts).total_seconds() / 3600.0


def pair_queues(
    events: list[QueueEvent],
    *,
    max_pair_days: int = MAX_QUEUE_PAIR_DAYS,
    weights: dict[int, tuple[int | None, int | None]] | None = None,
    flow_directions: dict[int, str] | None = None,
) -> list[Queue]:
    """Pair each anchorage run (first `anchorage_entry` → next `moored`) into a Queue.

    Pure: groups by mmsi, orders by time, walks. A run opens on the first
    `anchorage_entry`; subsequent entries are absorbed (re-entry jitter), `anchored`
    sets `anchored_seen`, `anchorage_exit` updates the last-exit. A `moored` closes
    the run (its terminal/zone/laden describe the berth queued for); a `departed`
    with no intervening `moored` discards the run (the anchorage visit didn't berth
    — not a queue). A run still open at the end is an in-progress wait. A `moored`
    with no open run is a *direct berth* (no queue) — visits.py handles it; #15/#16
    count it as a non-queued arrival via the visit join in the signal layer."""
    weights = weights or {}
    flow_directions = flow_directions or {}
    by_mmsi: dict[int, list[QueueEvent]] = {}
    for e in events:
        by_mmsi.setdefault(e.mmsi, []).append(e)

    queues: list[Queue] = []
    for mmsi, evs in by_mmsi.items():
        evs = sorted(evs, key=lambda e: e.event_time)
        dwt, gas = weights.get(mmsi, (None, None))
        run: dict | None = None  # open run accumulator

        def emit(moored: QueueEvent | None) -> None:
            assert run is not None
            term = moored.terminal_id if moored else run["entry"].terminal_id
            zone = moored.zone if moored else run["entry"].zone
            laden = moored.laden_flag if moored else run["entry"].laden_flag
            ref = moored if moored else run["entry"]
            queues.append(
                Queue(
                    mmsi=mmsi,
                    terminal_id=term,
                    zone=zone,
                    flow_direction=flow_directions.get(term),
                    entry_ts=run["entry"].event_time,
                    moored_ts=moored.event_time if moored else None,
                    anchored_ts=run["anchored_ts"],
                    last_exit_ts=run["last_exit"],
                    anchored_seen=run["anchored_seen"],
                    laden=laden,
                    regime=regime_of(ref.event_time, ref.source),
                    cold_start=run["entry"].cold_start,
                    dwt=dwt,
                    gas_capacity_m3=gas,
                )
            )

        for e in evs:
            if e.event_type == "anchorage_entry":
                if run is None:
                    run = {
                        "entry": e,
                        "anchored_ts": None,
                        "anchored_seen": False,
                        "last_exit": None,
                    }
                # else: re-entry within an open run — absorbed
            elif e.event_type == "anchored":
                if run is not None:
                    run["anchored_seen"] = True
                    if run["anchored_ts"] is None:
                        run["anchored_ts"] = e.event_time
            elif e.event_type == "anchorage_exit":
                if run is not None:
                    run["last_exit"] = e.event_time
            elif e.event_type == "moored":
                if run is not None:
                    # Only a mooring at the SAME terminal as the anchorage closes the
                    # queue. A mooring elsewhere means the vessel left this anchorage
                    # without berthing here — critically, this rejects pairing a US
                    # anchorage with the EU mooring 16 days later (a whole voyage),
                    # which would otherwise manufacture a cross-ocean "queue".
                    entry = run["entry"]
                    same_terminal = (
                        e.terminal_id is not None
                        and e.terminal_id == entry.terminal_id
                    )
                    within = (
                        e.event_time - entry.event_time
                        <= timedelta(days=max_pair_days)
                    )
                    if same_terminal and within:
                        emit(e)
                    run = None  # closed, or abandoned (moored elsewhere / too late)
                # moored with no open run = direct berth (no queue)
            elif e.event_type == "departed":
                run = None  # left without berthing here — not a queue

        if run is not None:
            emit(None)  # still waiting at end of stream → open queue

    queues.sort(key=lambda q: (q.mmsi, q.entry_ts))
    return queues


# ----------------------------------------------------------------------
# Thin DB loader
# ----------------------------------------------------------------------

QUEUE_EVENTS_SQL = """
SELECT mmsi, event_type, event_time, zone, terminal_id, laden_flag, cold_start, source
FROM port_events
WHERE event_type IN ('anchorage_entry', 'anchored', 'anchorage_exit', 'moored', 'departed')
ORDER BY mmsi, event_time
"""

WEIGHTS_SQL = """
SELECT mmsi, dwt, gas_capacity_m3
FROM vessel_registry
WHERE is_lng_carrier OR is_fsru
"""

FLOW_DIRECTION_SQL = (
    "SELECT terminal_id, flow_direction FROM terminals WHERE flow_direction IS NOT NULL"
)


async def compute_queues(pool: asyncpg.Pool, now: datetime | None = None) -> list[Queue]:
    """Load the anchorage/moored events + weights + flow directions and pair them.
    `now` is accepted for signature symmetry with compute_legs/compute_visits (open
    queues are bounded by the panel in the signal layer, not here)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(QUEUE_EVENTS_SQL)
        wrows = await conn.fetch(WEIGHTS_SQL)
        frows = await conn.fetch(FLOW_DIRECTION_SQL)

    events = [
        QueueEvent(
            mmsi=r["mmsi"],
            event_type=r["event_type"],
            event_time=r["event_time"],
            zone=r["zone"],
            terminal_id=r["terminal_id"],
            laden_flag=r["laden_flag"],
            cold_start=r["cold_start"],
            source=r["source"],
        )
        for r in rows
    ]
    weights = {r["mmsi"]: (r["dwt"], r["gas_capacity_m3"]) for r in wrows}
    flow_directions = {r["terminal_id"]: r["flow_direction"] for r in frows}
    queues = pair_queues(events, weights=weights, flow_directions=flow_directions)
    logger.info(
        "paired %d anchorage queues (%d open)",
        len(queues),
        sum(1 for q in queues if q.moored_ts is None),
    )
    return queues
