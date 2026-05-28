"""Laden-state inference for port_events.

Two strategies, layered:

  (A) Draught vs design_draught.
      For inbound events (zone_entry, anchorage_entry, anchored,
      anchorage_exit, moored), forward-fill: most recent draught at or
      before event_time. This reflects the inbound voyage's laden state
      and is accurate because the master broadcasts the laden draught for
      hours/days before approach.

      For outbound events (departed, zone_exit) that follow a `moored` in
      the same envelope, the inbound draught report is stale — the master
      typically only rebroadcasts the new (lighter) draught 30–90 min
      AFTER physically undocking. So we use a lookahead window
      [event_time - 1h, event_time + 6h] and prefer a reading inside that
      window. If no reading exists in the window, the forward-fill answer
      is stale and we delegate to (B).

  (B) Terminal flow_direction.
      Used when (A) cannot decide — either no draught at all, or only
      stale pre-discharge readings on an outbound event. We know:
        moored at 'export' terminal: arrived ballast, leaves laden
        moored at 'import' terminal: arrived laden,  leaves ballast
      So given the event's side_of_moored (pre/moored/post) and the
      terminal's flow_direction, the laden state is fully determined.

The two strategies are combined per-event by `infer_laden`.
"""

from __future__ import annotations

import bisect
from datetime import datetime, timedelta
from typing import Literal


LADEN_THRESHOLD = 0.85
# Post-event draught lookahead for outbound events: how far AFTER event_time
# to search for a draught reading. Set to capture the typical 30–90 min lag
# between physical undocking and the master rebroadcasting the new draught.
LOOKAHEAD_FORWARD = timedelta(hours=6)

# Where the event sits within its envelope relative to the (single) moored
# event in that envelope.
#   'pre'        — inbound: zone_entry/anchorage_entry/anchored/anchorage_exit
#                  before the envelope's moored
#   'moored'     — the moored event itself
#   'post'       — outbound: departed/zone_exit after the envelope's moored
#   'no_moored'  — anchorage-only visit; the envelope contains no moored
Side = Literal["pre", "moored", "post", "no_moored"]


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


def _draught_forward_fill(
    series: list[tuple[datetime, float]], event_time: datetime
) -> float | None:
    """Most recent draught at or before event_time."""
    keys = [ts for ts, _ in series]
    i = bisect.bisect_right(keys, event_time)
    if i == 0:
        return None
    return series[i - 1][1]


def _draught_after(
    series: list[tuple[datetime, float]],
    event_time: datetime,
    forward: timedelta,
) -> float | None:
    """Most recent draught reading in (event_time, event_time + forward].

    Used for outbound events. Pre-event readings are NOT considered here
    because at the moment the vessel undocks, any "current" draught report
    is still the pre-discharge value. The post-discharge reading typically
    lands 30–90 minutes after departure; if none exists in `forward`, the
    caller should fall back to flow_direction."""
    keys = [ts for ts, _ in series]
    # Latest reading at or before event_time + forward
    upper = bisect.bisect_right(keys, event_time + forward)
    if upper == 0:
        return None
    candidate_ts, candidate_v = series[upper - 1]
    # Must be strictly after event_time to qualify
    if candidate_ts <= event_time:
        return None
    return candidate_v


def _flow_direction_inference(side: Side, flow_direction: str | None) -> bool | None:
    """Determine laden from the side-of-moored + terminal flow_direction.
    Returns None when flow_direction or side cannot decide."""
    if flow_direction not in ("import", "export"):
        return None
    if side == "no_moored":
        # Anchorage-only visit (e.g., queue then abort). The cargo did not
        # change at this terminal — we have nothing to say.
        return None
    # Pre-mooring / moored: laden state on the inbound voyage.
    #   import terminal: vessel arrived laden
    #   export terminal: vessel arrived ballast
    # Post-mooring (departed, zone_exit): cargo flipped at the berth.
    if side in ("pre", "moored"):
        return flow_direction == "import"
    # side == "post"
    return flow_direction == "export"


Source = Literal["draught", "flow_direction"]


def infer_laden(
    mmsi: int,
    event_time: datetime,
    side: Side,
    flow_direction: str | None,
    design_draught: float | None,
    draught_lookup: dict[int, list[tuple[datetime, float]]],
) -> tuple[bool | None, Source | None]:
    """Layered laden inference: draught primary, flow_direction fallback.

    Returns (laden_flag, source) — source is 'draught' when a usable draught
    reading was found, 'flow_direction' when we fell back, or None when
    neither could answer.

    For outbound (`side='post'`) events, requires a draught reading AFTER
    event_time within `LOOKAHEAD_FORWARD` — pre-event readings still reflect
    pre-discharge state and aren't trusted.
    """
    series = draught_lookup.get(mmsi)
    draught: float | None = None
    if series:
        if side == "post":
            draught = _draught_after(series, event_time, LOOKAHEAD_FORWARD)
        else:
            draught = _draught_forward_fill(series, event_time)

    if draught is not None and design_draught is not None and design_draught > 0:
        return draught >= LADEN_THRESHOLD * design_draught, "draught"

    fallback = _flow_direction_inference(side, flow_direction)
    if fallback is None:
        return None, None
    return fallback, "flow_direction"


# Kept for backward compatibility with existing callers / tests; thin wrapper.
def laden_at(
    mmsi: int,
    event_time: datetime,
    design_draught: float | None,
    draught_lookup: dict[int, list[tuple[datetime, float]]],
) -> bool | None:
    """Pure forward-fill (no flow_direction fallback). Use `infer_laden` for
    the production path."""
    if design_draught is None or design_draught <= 0:
        return None
    series = draught_lookup.get(mmsi)
    if not series:
        return None
    draught = _draught_forward_fill(series, event_time)
    if draught is None:
        return None
    return draught >= LADEN_THRESHOLD * design_draught
