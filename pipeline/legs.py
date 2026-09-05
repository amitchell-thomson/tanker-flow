"""Voyage-leg derivation from port_events.

A *leg* pairs a vessel's `departed` event with its next `zone_entry` — the
building block under the in-transit / ton-mile / round-trip signals. This module
is the foundation: it produces correctly-classified, regime-tagged, censored
legs. The market-signal *aggregation* on top (laden ton-miles in transit,
arrivals/wk, etc.) lives in the signal layer and is deliberately NOT here.

Pure logic (`pair_legs`) + a thin DB loader (`compute_legs`), mirroring the
state_machine / port_events split.

Classification (see SIGNALS.md and docs/review-2026-05-31-pre-signal-audit.md):
  - 'closed'           terminating zone_entry in a *different* zone — a real voyage
  - 'same_zone'        terminating zone_entry in the *same* zone — intra-region hop,
                       berth shift, or re-entry; ~zero cross-zone ton-miles, so the
                       signal layer should exclude these from the lane flow
  - 'open_in_transit'  no arrival yet, departed within the expected voyage window
                       for its declared destination (per-O-D; falls back to the
                       conservative global cap when destination is unknown)
  - 'open_floating'    past the window but a recent coastal fix shows it still
                       on-water near a market — *genuine floating storage* (#17/#19/
                       #20). Age-censoring alone would wrongly discard this.
  - 'open_arrival_gap' past the window, last fix in the destination region but
                       stale — almost certainly arrived-and-we-missed-the-entry.
                       The VF-rescue `floating_check` trigger polls these to
                       confirm (→ closes the leg or confirms floating).
  - 'open_censored'    past the window, no recent coastal evidence — a phantom
                       (arrived-and-dark elsewhere) or invisible mid-ocean idle.
                       Excluded everywhere (the guard against the phantom-leg bias
                       that inflates #1/#17/#19/#20).

The enrichment (per-O-D window + last-fix evidence) is *optional*: with no
`dest_regions`/`last_fixes` supplied, `pair_legs` collapses to the original
binary `open_in_transit | open_censored` at `censor_days`, so existing callers
and tests are unaffected.

Point-in-time replay (`compute_legs(..., point_in_time=True)`)
-------------------------------------------------------------
`pair_legs` has always been pure in `now`, but the *loader* was not: its default
queries read the latest-ever fix and the **current** `priority_watchlist`, both
of which are "as of the moment you run it". Replayed at a historical `now` that
silently leaks the future — `_classify_overdue` compares a 2026 fix against
`now - RECENT_FIX_DAYS` and calls essentially every historical open leg
`open_floating`. The live panel never hit this (`signal.py` loads once at
true-now and reconstructs `knowable` by trimming intervals), but any real
as-of backtest does. See `analysis/DECISIONS.md` D-004a.

`point_in_time=True` bounds all three sources by `now`:
  - events      `event_time <= now` (a future arrival can't close a leg early),
  - last fix    the latest fix `<= now` (per-MMSI LATERAL, not a global sort),
  - declaration the latest `vessel_state.dest` at `<= now`, parsed here —
                `priority_watchlist` is a current-snapshot table with no
                history, so it is structurally un-replayable.
The default (`False`) keeps the original queries byte-for-byte, so the live
pipeline, viz and vf_rescue are unaffected.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

import asyncpg

from config import ZONES, regime_of

from .dest_parser import parse_destination
from .geo import haversine_nm


# Last-resort open-leg window, used only when a leg has neither a declared
# destination nor a `fallback_region` to assume one. Set beyond the longest
# plausible laden voyage (US Gulf -> Asia ~32d) so a bare/unenriched call never
# censors a genuine long haul it can't attribute.
CENSOR_OPEN_DAYS = 30

# A `departed` only pairs with a `zone_entry` arrival within this many days — beyond
# it the "arrival" cannot belong to this departure and the leg is treated as open
# (then classified by the usual window/last-fix logic). Catches the disconnected-
# data phantom where a historical departure (e.g. NOAA 2022) pairs with a live
# arrival years later (SIGNALS.md §0.5: never pair across the data seam). Set well
# above any real laden voyage (incl. slow-steam / floating storage, ~ up to ~2mo)
# so a legitimate boundary-spanning voyage — a NOAA departure days before live
# coverage landing at a live arrival — is kept.
MAX_LEG_PAIR_DAYS = 90

# Per-destination-region expected laden-voyage windows (days). Beyond this, an
# open leg to that region is no longer "in transit" and gets reclassified by
# last-fix evidence. Only import regions appear — a laden leg's destination is
# always an import terminal. (SIGNALS.md §4: "tighter per-O-D window, US->EU ~18 d".)
OD_WINDOW_DAYS: dict[str, int] = {
    "nweurope": 18,
    "baltic": 20,
    "iberian": 16,
    "wmed": 20,
    "emed": 24,
}

# When an open leg never broadcast a destination (~90% of them under terrestrial
# AIS), the signal layer *assumes* the dominant US-LNG lane destination to estimate
# its distance (signal.FALLBACK_DEST_ZONE). Classification must use the SAME
# assumption, so such a leg inherits that region's voyage window above and stops
# counting as "in transit" once it is older than that voyage would take — rather
# than the looser global censor, which kept a likely-already-arrived phantom alive
# ~12 extra days and inflated the in-transit ton-mile base. Passed by compute_legs
# only when enrich=True; keep this in lockstep with signal.FALLBACK_DEST_ZONE.
FALLBACK_DEST_REGION = "nweurope"

# A fix newer than this, inside coastal AIS range, means we can still *see* the
# vessel — so a past-window open leg is on-water floating storage, not a phantom.
RECENT_FIX_DAYS = 4

# How far back a point-in-time declaration lookup will accept a `vessel_state`
# row. Mirrors `scoring.py`'s `latest_state` window so the replayed declaration
# matches what the live scorer would have had in hand on that date.
DEST_STATE_WINDOW_DAYS = 90

LegStatus = Literal[
    "closed",
    "same_zone",
    "open_in_transit",
    "open_floating",
    "open_arrival_gap",
    "open_censored",
]


def _zone_of(lat: float | None, lon: float | None) -> str | None:
    """Which config.ZONES region rectangle contains this point (coastal-AIS
    range proxy), or None if mid-ocean / outside all tracked regions."""
    if lat is None or lon is None:
        return None
    for name, lat_min, lat_max, lon_min, lon_max in ZONES:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return name
    return None


def _classify_overdue(
    last_fix: tuple[datetime | None, float | None, float | None] | None,
    dest_region: str | None,
    now: datetime,
) -> LegStatus:
    """Classify a past-window open leg from its last-fix evidence."""
    if not last_fix or last_fix[0] is None:
        return "open_censored"
    fix_ts, lat, lon = last_fix
    zone = _zone_of(lat, lon)
    if fix_ts > now - timedelta(days=RECENT_FIX_DAYS) and zone is not None:
        return "open_floating"  # still on-water near a coast — real inventory
    if dest_region is not None and zone == dest_region:
        return "open_arrival_gap"  # reached the destination region, then went dark
    return "open_censored"


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
    source: str = "state_machine"  # origin tag for regime_of (noaa-ais / gfw_* / live)


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
    # Enrichment (open legs): declared destination region + the vessel's current
    # last fix, used by the overdue classifier and the VF floating_check trigger.
    dest_region: str | None = None
    last_fix_ts: datetime | None = None
    last_fix_lat: float | None = None
    last_fix_lon: float | None = None


def pair_legs(
    events: list[LegEvent],
    now: datetime,
    *,
    censor_days: int = CENSOR_OPEN_DAYS,
    max_leg_days: int = MAX_LEG_PAIR_DAYS,
    weights: dict[int, tuple[int | None, int | None]] | None = None,
    dest_regions: dict[int, str] | None = None,
    last_fixes: dict[int, tuple[datetime | None, float | None, float | None]]
    | None = None,
    od_windows: dict[str, int] | None = None,
    fallback_region: str | None = None,
) -> list[Leg]:
    """Pair each `departed` with its vessel's next `zone_entry` into a Leg.

    Pure: groups `events` by mmsi, orders by event_time, walks. Optional enrichment
    (all keyed by mmsi, applied to that vessel's open leg):
      - `dest_regions`  mmsi -> declared destination region (sets the O-D window
                        and the arrival-gap test),
      - `last_fixes`    mmsi -> (fix_ts, lat, lon) of the vessel's latest fix,
      - `od_windows`    region -> expected-voyage days (defaults to OD_WINDOW_DAYS).
      - `fallback_region`  region whose window + arrival-gap test a *declaration-less*
                        open leg inherits (mirrors signal.FALLBACK_DEST_ZONE). When
                        None, an undeclared open leg falls back to `censor_days`.
    Omit them and behaviour collapses to the original `open_in_transit |
    open_censored` split at `censor_days`. See the module docstring.
    """
    by_mmsi: dict[int, list[LegEvent]] = {}
    for e in events:
        by_mmsi.setdefault(e.mmsi, []).append(e)

    weights = weights or {}
    dest_regions = dest_regions or {}
    last_fixes = last_fixes or {}
    od_windows = od_windows or OD_WINDOW_DAYS
    legs: list[Leg] = []

    for mmsi, evs in by_mmsi.items():
        evs = sorted(evs, key=lambda e: e.event_time)
        dwt, gas = weights.get(mmsi, (None, None))
        dest_region = dest_regions.get(mmsi)
        for i, d in enumerate(evs):
            if d.event_type != "departed":
                continue
            arrival = next(
                (z for z in evs[i + 1 :] if z.event_type == "zone_entry"), None
            )
            # Reject an implausibly-distant "arrival" — it belongs to a later voyage
            # (or the live block after a historical-data gap), not this departure.
            # Drop to the open-leg branch, which classifies it by window/last-fix.
            if arrival is not None and (
                arrival.event_time - d.event_time > timedelta(days=max_leg_days)
            ):
                arrival = None
            regime = regime_of(d.event_time, d.source)
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
                dest_region=dest_region,
            )
            if arrival is None:
                # An undeclared leg inherits `fallback_region`'s window + arrival-gap
                # test, so it's governed by the SAME destination the signal layer
                # assumes for its distance. `.get(None, ...)` yields censor_days when
                # neither a declared nor a fallback region is available.
                region = dest_region if dest_region is not None else fallback_region
                window_days = od_windows.get(region, censor_days)
                lf = last_fixes.get(mmsi)
                if d.event_time > now - timedelta(days=window_days):
                    status: LegStatus = "open_in_transit"
                else:
                    status = _classify_overdue(lf, region, now)
                legs.append(
                    Leg(
                        status=status,
                        last_fix_ts=lf[0] if lf else None,
                        last_fix_lat=lf[1] if lf else None,
                        last_fix_lon=lf[2] if lf else None,
                        **common,
                    )
                )
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
SELECT mmsi, event_type, event_time, zone, terminal_id, lat, lon, laden_flag, source
FROM port_events
WHERE event_type IN ('departed', 'zone_entry')
ORDER BY mmsi, event_time
"""

WEIGHTS_SQL = """
SELECT mmsi, dwt, gas_capacity_m3
FROM vessel_registry
WHERE is_lng_carrier OR is_fsru
"""

# Declared destination region per vessel: the parsed dest currently on the
# watchlist (last-known declaration) resolved to its terminal's geographic zone.
DEST_REGION_SQL = """
SELECT pw.mmsi, t.zone AS region
FROM priority_watchlist pw
JOIN terminals t ON t.terminal_id = pw.parsed_dest_terminal_id
WHERE pw.parsed_dest_terminal_id IS NOT NULL AND t.zone IS NOT NULL
"""

# Latest fix per LNG/FSRU vessel — the last-fix evidence for the overdue split.
# NOTE: DISTINCT ON + ORDER BY fix_ts DESC does a sequential scan of the full
# ais_fixes hypertable. This is fast against the live feed (~weeks of data) but
# will be slow once NOAA historical backfill lands (potentially 100M+ rows).
# At that point, replace with a per-MMSI subquery or a materialised latest-fix
# view; the TimescaleDB per-chunk index on (mmsi, fix_ts) makes a per-MMSI MAX
# approach faster than a global sort.
LAST_FIX_SQL = """
SELECT DISTINCT ON (a.mmsi) a.mmsi, a.fix_ts, a.lat, a.lon
FROM ais_fixes a
JOIN vessel_registry v ON v.mmsi = a.mmsi
WHERE v.is_lng_carrier OR v.is_fsru
ORDER BY a.mmsi, a.fix_ts DESC
"""

# ---------------------------------------------------------------------------
# Point-in-time variants (see the module docstring / DECISIONS.md D-004a).
# Each takes `$1 = as_of` and is bounded so nothing after that instant is
# visible. Semantically these reduce to their unbounded counterparts when
# as_of >= max(timestamp), which is what makes the as_of==now regression hold.
# ---------------------------------------------------------------------------

LEG_EVENTS_PIT_SQL = """
SELECT mmsi, event_type, event_time, zone, terminal_id, lat, lon, laden_flag, source
FROM port_events
WHERE event_type IN ('departed', 'zone_entry')
  AND event_time <= $1
ORDER BY mmsi, event_time
"""

# Per-MMSI LATERAL rather than the global `DISTINCT ON` of LAST_FIX_SQL, which
# sequential-scans the whole hypertable (see the note above) — a cost a replay
# would pay on every as-of date. The LATERAL rides the per-chunk (mmsi, fix_ts)
# index, and `$2` narrows it to just the vessels whose classification actually
# depends on last-fix evidence (see `compute_legs`: only *past-window open* legs
# consult it, and that test is independent of the evidence itself). Probing the
# whole fleet instead costs ~20 s per as-of date against the decade corpus;
# probing the handful that need it is ~1 s.
LAST_FIX_PIT_SQL = """
SELECT v.mmsi, f.fix_ts, f.lat, f.lon
FROM vessel_registry v
CROSS JOIN LATERAL (
    SELECT a.fix_ts, a.lat, a.lon
    FROM ais_fixes a
    WHERE a.mmsi = v.mmsi AND a.fix_ts <= $1
    ORDER BY a.fix_ts DESC
    LIMIT 1
) f
WHERE v.mmsi = ANY($2::bigint[])
"""

# The as-of-safe declaration source. `priority_watchlist` holds one current row
# per vessel with no history, so DEST_REGION_SQL cannot be replayed at all; the
# raw `vessel_state` broadcasts behind it can. Freshness window mirrors
# scoring.py's `latest_state` so a replayed declaration matches what the live
# scorer would have held that day. Parsing happens in Python (dest_parser).
DEST_STATE_PIT_SQL = """
SELECT DISTINCT ON (vs.mmsi) vs.mmsi, vs.dest
FROM vessel_state vs
JOIN vessel_registry v ON v.mmsi = vs.mmsi
WHERE vs.state_ts <= $1
  AND vs.state_ts > $1 - ($2 || ' days')::interval
  AND vs.dest IS NOT NULL
  AND (v.is_lng_carrier OR v.is_fsru)
ORDER BY vs.mmsi, vs.state_ts DESC
"""

UNLOCODE_SQL = "SELECT unlocode, terminal_id FROM terminals WHERE unlocode IS NOT NULL"

TERMINAL_ZONE_SQL = (
    "SELECT terminal_id, zone FROM terminals WHERE zone IS NOT NULL"
)


def resolve_dest_regions(
    state_rows: list[tuple[int, str | None]],
    unlocodes: dict[str, int],
    terminal_zones: dict[int, str],
) -> dict[int, str]:
    """mmsi -> declared destination *region*, from raw `vessel_state.dest` text.

    Pure. The point-in-time counterpart of DEST_REGION_SQL: that query reads a
    terminal_id the live scorer already parsed and stored, so replaying it means
    re-doing the same parse here against the same `dest_parser` + unlocode map.
    Unparseable / "FOR ORDERS" / non-terminal declarations resolve to nothing and
    are simply absent — the caller treats a missing key exactly as it treats an
    undeclared vessel (falls back to `fallback_region`)."""
    out: dict[int, str] = {}
    for mmsi, dest in state_rows:
        terminal_id, _is_for_orders = parse_destination(dest, unlocodes)
        if terminal_id is None:
            continue
        region = terminal_zones.get(terminal_id)
        if region is not None:
            out[mmsi] = region
    return out


async def compute_legs(
    pool: asyncpg.Pool,
    now: datetime | None = None,
    *,
    censor_days: int = CENSOR_OPEN_DAYS,
    enrich: bool = True,
    point_in_time: bool = False,
) -> list[Leg]:
    """Load departed/zone_entry events + weights (+ dest-region and last-fix
    enrichment) and pair them.

    Pass an explicit `now` (e.g. the same --as-of used for port_events) for a
    reproducible, deterministic open/censored split. Set `enrich=False` for the
    plain binary classification (no dest-region / last-fix joins).

    `point_in_time=True` additionally bounds every source query by `now` so the
    result is what a live observer could have computed on that date — required
    for as-of replay (A1), harmless but pointless at true-now. See the module
    docstring and DECISIONS.md D-004a. Default `False` preserves the original
    queries exactly for the live pipeline / viz / vf_rescue.
    """
    if now is None:
        now = datetime.now(UTC)
    async with pool.acquire() as conn:
        if point_in_time:
            ev_rows = await conn.fetch(LEG_EVENTS_PIT_SQL, now)
        else:
            ev_rows = await conn.fetch(LEG_EVENTS_SQL)
        w_rows = await conn.fetch(WEIGHTS_SQL)
        dest_regions: dict[int, str] = {}
        if enrich and point_in_time:
            state_rows = [
                (r["mmsi"], r["dest"])
                for r in await conn.fetch(
                    DEST_STATE_PIT_SQL, now, str(DEST_STATE_WINDOW_DAYS)
                )
            ]
            unlocodes = {
                r["unlocode"]: r["terminal_id"] for r in await conn.fetch(UNLOCODE_SQL)
            }
            terminal_zones = {
                r["terminal_id"]: r["zone"] for r in await conn.fetch(TERMINAL_ZONE_SQL)
            }
            dest_regions = resolve_dest_regions(state_rows, unlocodes, terminal_zones)
        elif enrich:
            for r in await conn.fetch(DEST_REGION_SQL):
                dest_regions[r["mmsi"]] = r["region"]

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
                source=r["source"],
            )
            for r in ev_rows
        ]
        pairing = dict(
            censor_days=censor_days,
            weights=weights,
            dest_regions=dest_regions,
            fallback_region=FALLBACK_DEST_REGION if enrich else None,
        )

        last_fixes: dict[int, tuple[datetime | None, float | None, float | None]] = {}
        if enrich and point_in_time:
            # Two-pass. Last-fix evidence only ever affects *open* legs — it
            # picks the `_classify_overdue` branch for past-window ones and
            # populates `last_fix_*` on the rest; closed/same_zone legs never
            # consult it. And openness is decided by departure time alone,
            # independent of the evidence, so pass 1 identifies exactly the
            # vessels worth probing. Equivalent to a single full-fleet pass,
            # but seeks ~50-100 MMSIs instead of ~830 (≈20 s → ≈1 s per as-of
            # date against the decade corpus).
            provisional = pair_legs(events, now, **pairing)
            need = sorted(
                {lg.mmsi for lg in provisional if lg.status.startswith("open")}
            )
            if need:
                for r in await conn.fetch(LAST_FIX_PIT_SQL, now, need):
                    last_fixes[r["mmsi"]] = (r["fix_ts"], r["lat"], r["lon"])
            if not last_fixes:
                return provisional  # nothing to re-classify
        elif enrich:
            for r in await conn.fetch(LAST_FIX_SQL):
                last_fixes[r["mmsi"]] = (r["fix_ts"], r["lat"], r["lon"])

    return pair_legs(events, now, last_fixes=last_fixes, **pairing)
