"""Voyage-leg derivation from port_events.

A *leg* pairs a vessel's `departed` event with its next `zone_entry` — the
building block under the in-transit / ton-mile / round-trip signals. This module
is the foundation: it produces correctly-classified, regime-tagged, censored
legs. The market-signal *aggregation* on top (laden ton-miles in transit,
arrivals/wk, etc.) lives in the signal layer and is deliberately NOT here.

Pure logic (`pair_legs`) + a thin DB loader (`compute_legs`), mirroring the
state_machine / port_events split.

Classification (see SIGNALS.md and docs/review-2026-05-31-pre-signal-audit.md):
  - 'closed'          terminating zone_entry in a *different* zone — a real voyage
  - 'same_zone'       terminating zone_entry in the *same* zone — intra-region hop,
                      berth shift, or re-entry; ~zero cross-zone ton-miles, so the
                      signal layer should exclude these from the lane flow
  - 'open_in_transit' no terminating zone_entry yet, departed <= censor_days ago
  - 'open_censored'   no terminating zone_entry and departed > censor_days ago —
                      almost certainly arrived-and-missed (coverage gap) or sitting
                      in storage, NOT genuinely in transit. Censoring these is the
                      mandatory guard against the phantom-leg bias that inflates
                      "laden ton-miles in transit" (#1) and "mean voyage age" (#20).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

import asyncpg

from config import regime_of

from .geo import haversine_nm


# An open laden leg older than this is treated as censored, not in-transit. Set
# beyond the longest plausible laden voyage (US Gulf -> Asia ~32d); the signal
# layer may apply a tighter, destination-specific window on top.
CENSOR_OPEN_DAYS = 30

LegStatus = Literal["closed", "same_zone", "open_in_transit", "open_censored"]


@dataclass(frozen=True)
class LegEvent:
    """The minimal per-event view of a port_events row the pairing needs."""

    mmsi: int
    event_type: str
    event_time: datetime
    zone: str
    terminal_id: int | None
    lat: float | None
    lon: float | None
    laden_flag: bool | None


@dataclass(frozen=True)
class Leg:
    mmsi: int
    origin_terminal_id: int | None
    origin_zone: str
    departed_ts: datetime
    departed_lat: float | None
    departed_lon: float | None
    laden: bool | None
    regime: str  # ingestion regime of the departed event (config.regime_of)
    status: LegStatus
    dest_terminal_id: int | None = None
    dest_zone: str | None = None
    arrived_ts: datetime | None = None
    arrived_lat: float | None = None
    arrived_lon: float | None = None
    distance_nm: float | None = None  # great-circle departed -> arrival
    duration_h: float | None = None
    dwt: int | None = None
    gas_capacity_m3: int | None = None


def pair_legs(
    events: list[LegEvent],
    now: datetime,
    *,
    censor_days: int = CENSOR_OPEN_DAYS,
    weights: dict[int, tuple[int | None, int | None]] | None = None,
) -> list[Leg]:
    """Pair each `departed` with its vessel's next `zone_entry` into a Leg.

    Pure: groups `events` by mmsi, orders by event_time, walks. `weights` maps
    mmsi -> (dwt, gas_capacity_m3) for ton-mile weighting (attached to each leg).
    See the module docstring for the status taxonomy.
    """
    by_mmsi: dict[int, list[LegEvent]] = {}
    for e in events:
        by_mmsi.setdefault(e.mmsi, []).append(e)

    censor_cutoff = now - timedelta(days=censor_days)
    weights = weights or {}
    legs: list[Leg] = []

    for mmsi, evs in by_mmsi.items():
        evs = sorted(evs, key=lambda e: e.event_time)
        dwt, gas = weights.get(mmsi, (None, None))
        for i, d in enumerate(evs):
            if d.event_type != "departed":
                continue
            arrival = next(
                (z for z in evs[i + 1 :] if z.event_type == "zone_entry"), None
            )
            regime = regime_of(d.event_time)
            common = dict(
                mmsi=mmsi,
                origin_terminal_id=d.terminal_id,
                origin_zone=d.zone,
                departed_ts=d.event_time,
                departed_lat=d.lat,
                departed_lon=d.lon,
                laden=d.laden_flag,
                regime=regime,
                dwt=dwt,
                gas_capacity_m3=gas,
            )
            if arrival is None:
                status: LegStatus = (
                    "open_in_transit"
                    if d.event_time > censor_cutoff
                    else "open_censored"
                )
                legs.append(Leg(status=status, **common))
                continue

            distance = None
            if None not in (d.lat, d.lon, arrival.lat, arrival.lon):
                distance = haversine_nm(d.lat, d.lon, arrival.lat, arrival.lon)
            duration_h = (arrival.event_time - d.event_time).total_seconds() / 3600.0
            legs.append(
                Leg(
                    status="same_zone" if arrival.zone == d.zone else "closed",
                    dest_terminal_id=arrival.terminal_id,
                    dest_zone=arrival.zone,
                    arrived_ts=arrival.event_time,
                    arrived_lat=arrival.lat,
                    arrived_lon=arrival.lon,
                    distance_nm=distance,
                    duration_h=duration_h,
                    **common,
                )
            )

    legs.sort(key=lambda lg: (lg.mmsi, lg.departed_ts))
    return legs


# ----------------------------------------------------------------------
# Thin DB loader
# ----------------------------------------------------------------------

LEG_EVENTS_SQL = """
SELECT mmsi, event_type, event_time, zone, terminal_id, lat, lon, laden_flag
FROM port_events
WHERE event_type IN ('departed', 'zone_entry')
ORDER BY mmsi, event_time
"""

WEIGHTS_SQL = """
SELECT mmsi, dwt, gas_capacity_m3
FROM vessel_registry
WHERE is_lng_carrier OR is_fsru
"""


async def compute_legs(
    pool: asyncpg.Pool,
    now: datetime | None = None,
    *,
    censor_days: int = CENSOR_OPEN_DAYS,
) -> list[Leg]:
    """Load departed/zone_entry events + dwt/gas weights and pair them.

    Pass an explicit `now` (e.g. the same --as-of used for port_events) for a
    reproducible, deterministic open/censored split.
    """
    if now is None:
        now = datetime.now(UTC)
    async with pool.acquire() as conn:
        ev_rows = await conn.fetch(LEG_EVENTS_SQL)
        w_rows = await conn.fetch(WEIGHTS_SQL)
    weights = {r["mmsi"]: (r["dwt"], r["gas_capacity_m3"]) for r in w_rows}
    events = [
        LegEvent(
            mmsi=r["mmsi"],
            event_type=r["event_type"],
            event_time=r["event_time"],
            zone=r["zone"],
            terminal_id=r["terminal_id"],
            lat=r["lat"],
            lon=r["lon"],
            laden_flag=r["laden_flag"],
        )
        for r in ev_rows
    ]
    return pair_legs(events, now, censor_days=censor_days, weights=weights)
