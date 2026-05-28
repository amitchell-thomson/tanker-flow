"""Forward-fill laden-state inference from sparse vessel_state.draught records.

`vessel_state` is populated only by ShipStaticData AIS messages and is
correspondingly sparse. The laden state at any event_time is therefore the
most recent `draught` at or before that event, forward-filled across any
amount of time. `laden_flag = TRUE` iff `draught >= 0.85 * design_draught`.
"""

from __future__ import annotations

import bisect
from datetime import datetime


LADEN_THRESHOLD = 0.85


def build_draught_lookup(
    rows: list[tuple[int, datetime, float | None]],
) -> dict[int, list[tuple[datetime, float]]]:
    """rows: iterable of (mmsi, state_ts, draught) sorted by (mmsi, state_ts).
    Returns mmsi -> list of (state_ts, draught) with NULL draughts dropped."""
    out: dict[int, list[tuple[datetime, float]]] = {}
    for mmsi, ts, draught in rows:
        if draught is None or draught <= 0:
            continue
        out.setdefault(mmsi, []).append((ts, float(draught)))
    return out


def laden_at(
    mmsi: int,
    event_time: datetime,
    design_draught: float | None,
    draught_lookup: dict[int, list[tuple[datetime, float]]],
) -> bool | None:
    """Forward-fill: most recent draught at or before event_time."""
    if design_draught is None or design_draught <= 0:
        return None
    series = draught_lookup.get(mmsi)
    if not series:
        return None
    keys = [ts for ts, _ in series]
    i = bisect.bisect_right(keys, event_time)
    if i == 0:
        return None
    _, latest_draught = series[i - 1]
    return latest_draught >= LADEN_THRESHOLD * design_draught
