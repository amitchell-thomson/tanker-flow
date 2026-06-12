"""Port-visit pairing from port_events.

A *visit* pairs a vessel's `moored` event with its next `departed` — the
berth-occupancy interval during which a vessel is **loading** (export terminal)
or **discharging** (import terminal). It is the foundation under the gas-volume
"currently loading / unloading" signals (`signal_daily` keys `gas_loading_us` /
`gas_discharging_eu`): on any day the vessel is in berth it contributes its
`gas_capacity_m3` to that terminal's band.

This is deliberately distinct from the planned `anchorage_entry → moored` *queue*
pairing (SIGNALS.md #6/#12): a visit measures time *at berth*, not time *in
queue*. Cold-start visits are kept — a vessel first observed already alongside is
still genuinely occupying the berth (unlike #4 arrivals, which are an event we
must have witnessed).

Pure logic (`pair_visits`) + a thin DB loader (`compute_visits`), mirroring the
state_machine / legs split. An **open** visit (a `moored` with no following
`departed`) is the vessel currently in berth; its `departed_ts` is None and the
signal layer runs it through the panel end.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import asyncpg

from config import regime_of

# A `moored` only pairs with a `departed` within this many days — beyond it the
# berth visit is a missed departure (the vessel left unobserved), so it's treated
# as still open (capped downstream at signal.OPEN_VISIT_CEILING_DAYS). Guards the
# disconnected-data phantom where a historical mooring (e.g. NOAA 2022) pairs with
# a live departure years later (SIGNALS.md §0.5). Real berth occupancy is hours to
# a few days; this is well above even heavy congestion queueing.
MAX_VISIT_PAIR_DAYS = 30


@dataclass(frozen=True)
class VisitEvent:
    """The minimal per-event view of a port_events row the pairing needs."""

    mmsi: int
    event_type: str
    event_time: datetime
    zone: str
    terminal_id: int | None
    laden_flag: bool | None
    cold_start: bool = False
    source: str = "state_machine"  # origin tag for regime_of (noaa-ais / gfw_* / live)


@dataclass(frozen=True)
class Visit:
    mmsi: int
    terminal_id: int | None
    zone: str
    flow_direction: str | None  # 'export' | 'import' | None (from terminals)
    moored_ts: datetime
    departed_ts: datetime | None  # None ⇒ still in berth (open visit)
    laden: bool | None  # laden_flag of the moored event
    regime: str  # ingestion regime of the moored event (config.regime_of)
    cold_start: bool = False
    dwt: int | None = None
    gas_capacity_m3: int | None = None


def pair_visits(
    events: list[VisitEvent],
    *,
    max_visit_days: int = MAX_VISIT_PAIR_DAYS,
    weights: dict[int, tuple[int | None, int | None]] | None = None,
    flow_directions: dict[int, str] | None = None,
) -> list[Visit]:
    """Pair each `moored` with its vessel's next `departed` into a Visit.

    Pure: groups `events` by mmsi, orders by event_time, walks. The visit's
    terminal/zone come from the `moored` event; the closing `departed` only
    supplies the end timestamp (None when the vessel is still alongside).
    `weights` (mmsi → (dwt, gas)) and `flow_directions` (terminal_id → direction)
    are attached per visit. See the module docstring.
    """
    weights = weights or {}
    flow_directions = flow_directions or {}
    by_mmsi: dict[int, list[VisitEvent]] = {}
    for e in events:
        by_mmsi.setdefault(e.mmsi, []).append(e)

    visits: list[Visit] = []
    for mmsi, evs in by_mmsi.items():
        evs = sorted(evs, key=lambda e: e.event_time)
        dwt, gas = weights.get(mmsi, (None, None))
        for i, m in enumerate(evs):
            if m.event_type != "moored":
                continue
            departed = next(
                (d for d in evs[i + 1 :] if d.event_type == "departed"), None
            )
            # An implausibly-distant "departure" belongs to a later visit (or the
            # live block after a historical-data gap), not this mooring — leave the
            # visit open (capped downstream) rather than spanning the gap.
            if departed is not None and (
                departed.event_time - m.event_time > timedelta(days=max_visit_days)
            ):
                departed = None
            visits.append(
                Visit(
                    mmsi=mmsi,
                    terminal_id=m.terminal_id,
                    zone=m.zone,
                    flow_direction=flow_directions.get(m.terminal_id),
                    moored_ts=m.event_time,
                    departed_ts=departed.event_time if departed else None,
                    laden=m.laden_flag,
                    regime=regime_of(m.event_time, m.source),
                    cold_start=m.cold_start,
                    dwt=dwt,
                    gas_capacity_m3=gas,
                )
            )

    visits.sort(key=lambda v: (v.mmsi, v.moored_ts))
    return visits


# ----------------------------------------------------------------------
# Thin DB loader
# ----------------------------------------------------------------------

VISIT_EVENTS_SQL = """
SELECT mmsi, event_type, event_time, zone, terminal_id, laden_flag, cold_start, source
FROM port_events
WHERE event_type IN ('moored', 'departed')
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


async def compute_visits(
    pool: asyncpg.Pool, now: datetime | None = None
) -> list[Visit]:
    """Load moored/departed events (+ weights + terminal flow_direction) and pair
    them. `now` is accepted for signature symmetry with compute_legs; the open/
    closed split is purely structural (a `moored` with no later `departed`)."""
    if now is None:
        now = datetime.now(UTC)
    async with pool.acquire() as conn:
        ev_rows = await conn.fetch(VISIT_EVENTS_SQL)
        w_rows = await conn.fetch(WEIGHTS_SQL)
        fd_rows = await conn.fetch(FLOW_DIRECTION_SQL)

    weights = {r["mmsi"]: (r["dwt"], r["gas_capacity_m3"]) for r in w_rows}
    flow_directions = {r["terminal_id"]: r["flow_direction"] for r in fd_rows}
    events = [
        VisitEvent(
            mmsi=r["mmsi"],
            event_type=r["event_type"],
            event_time=r["event_time"],
            zone=r["zone"],
            terminal_id=r["terminal_id"],
            laden_flag=r["laden_flag"],
            cold_start=r["cold_start"],
            source=r["source"],
        )
        for r in ev_rows
    ]
    return pair_visits(events, weights=weights, flow_directions=flow_directions)
