"""Market-signal aggregation layer.

Aggregates the classified voyage legs (`pipeline/legs.py`) and port visits
(`pipeline/visits.py`) into a tidy/long daily panel (`signal_daily`),
idempotently rebuilt like `port_events` (TRUNCATE + executemany swap). This is
the *aggregation* on top of the leg/visit foundation — it does NOT re-pair, it
consumes `compute_legs()` / `compute_visits()`.

Headline signals (v2 — gas-volume, stacked). Every signal is a **volume of gas
(m³)** reconstructed per day and broken into stacked bands carried in
`zone_scope`:

  - gas_loading_us       gas being loaded at US export berths, banded by terminal
                         (visits at flow='export' terminals)
  - gas_discharging_eu   gas being discharged at EU import berths, banded by
                         terminal (laden visits at flow='import' terminals)
  - gas_in_transit_volume  laden gas on the water US→EU, banded by destination
                         zone (open legs with no declared dest → 'unknown' band)
  - gas_ballast_to_us    empty carriers returning toward the US to reload, banded
                         by destination zone ('unknown' when undeclared)

Design decisions:
  - The unit is gas capacity (`vessel_registry.gas_capacity_m3`); there is NO
    distance weighting (this replaces the ton-mile headline). Legs/visits with a
    NULL gas capacity are skipped and counted in the summary.
  - At-sea signals are a daily **stock**: a leg contributes its full gas to its
    band on every day it is *live* (in transit / ballasting). Open intervals run
    through the panel end.
  - Berth signals (loading/discharging) are an amortized daily **flow**: a visit's
    cargo is spread across its berth hours at a constant rate so it integrates to
    exactly one cargo (`hours-on-day-d / total-berth-hours × capacity`). Total
    berth hours = the visit's observed dwell once it has departed; while it is
    still open, the terminal's mean dwell is the estimate (cumulative deposit
    capped at one cargo, so an open visit lingering past its estimated dwell
    plateaus instead of over-counting). This de-biases the old in-berth stock,
    where a visit straddling midnight registered its full cargo on both days.
  - Two bases (SIGNALS.md §0·7), both built per rebuild. `physical` = hindsight-
    clean reconstruction (an item is live on day d iff its interval covers d under
    today's classification) — validation only. `knowable` = the point-in-time
    value the live pipeline would have printed on day d (overdue legs counted over
    their pre-recognition window via `knowable_leg_interval`; berth flow amortized
    over the *estimated* dwell via `amortized_cargo_knowable`) — the model-safe
    input. `MODELS.md` consumes `knowable` only.
  - In-transit stock = laden closed legs (export→import, over [departed, arrived))
    + laden open_in_transit legs (export-origin, to as_of). Unlike the old
    ton-mile build, an open leg with no declared destination is shown as its own
    'unknown' band rather than assumed NW-Europe — the leg classifier's
    NW-Europe fallback window still governs phantom-censoring in legs.py, but the
    *display* bucketing here is honest about the missing declaration.

Pure aggregation functions + a thin DB loader, mirroring legs.py / visits.py.

Usage: `uv run python -m pipeline.signal` (or `make signals`).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import statistics
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Callable

import asyncpg
from rich.logging import RichHandler

from config import settings

from .geo import haversine_nm
from .legs import (
    CENSOR_OPEN_DAYS,
    FALLBACK_DEST_REGION,
    OD_WINDOW_DAYS,
    Leg,
    compute_legs,
)
from .queues import Queue, compute_queues
from .utils import parse_as_of
from .visits import Visit, compute_visits


logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])
logger = logging.getLogger(__name__)


BASIS_PHYSICAL = "physical"  # hindsight-clean reconstruction (validation use)
BASIS_KNOWABLE = "knowable"  # point-in-time: the value the live pipeline would
#                              have printed on each day d (the model-safe input).
#                              See SIGNALS.md §0·7 for the design + the clock rule.
ALL_BASES = (BASIS_PHYSICAL, BASIS_KNOWABLE)

# Open-leg statuses that `physical` excludes (resolved as phantom/floating/gap with
# hindsight) but `knowable` must INCLUDE over their pre-recognition window: on the
# days before the leg aged out, a live observer saw a laden vessel still in transit.
OPEN_OVERDUE_STATUSES = ("open_censored", "open_floating", "open_arrival_gap")

# Max berth dwell for an *open* visit (a `moored` with no observed `departed`).
# Beyond this the visit is treated as a missed-departure phantom — the vessel
# loaded/discharged and left while AIS-silent — and stops contributing to the
# "currently in berth" stock. This is the visit analog of the open-leg censor in
# legs.py: genuine LNG berth time is ~1 day, rarely more than a few, so a visit
# still "open" after a working week is almost always a dropped departure (it also
# stops a synthetic FSRU `moored` from pinning its host terminal forever).
OPEN_VISIT_CEILING_DAYS = 5

# Nominal LNG load/discharge dwell, used only to amortize an open visit at a
# terminal that has no closed visit yet to average (cold start).
DEFAULT_BERTH_HOURS = 24.0

# Band used for an in-transit / ballast leg whose destination was never declared
# (terrestrial AIS loses the mid-ocean dest broadcast — ~90% of open legs). The
# leg is real cargo on the water, so it is kept and surfaced under its own band
# rather than dropped or silently folded into the dominant lane.
UNKNOWN_BAND = "unknown"


# ----------------------------------------------------------------------
# SQL
# ----------------------------------------------------------------------

TRUNCATE_SQL = "TRUNCATE signal_daily RESTART IDENTITY"

TERMINAL_METADATA_SQL = (
    "SELECT terminal_id, zone, flow_direction FROM terminals WHERE zone IS NOT NULL"
)

# Per-terminal centroid from the terminal_zones polygons — the great-circle
# distance fallback for legs whose endpoint events carry no lat/lon (every GFW
# event: source 'gfw_events' stores no coordinates). Voyage distance is an O-D
# property, so the terminal centroid is an apt — arguably steadier — proxy than a
# jittery single arrival fix, and it is what gives #22/#24 their historical depth.
TERMINAL_CENTROID_SQL = """
SELECT terminal_id,
       ST_Y(ST_Centroid(ST_Collect(geom))) AS lat,
       ST_X(ST_Centroid(ST_Collect(geom))) AS lon
FROM terminal_zones
GROUP BY terminal_id
"""

INSERT_SQL = """
INSERT INTO signal_daily
    (signal_key, bucket_date, zone_scope, regime, value, n_legs,
     value_dispersion, open_fraction, estimated_fraction, basis)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
"""

# Append-only "as-printed" vintage log (carry-forward item 2, SIGNALS.md §0·7·1·4):
# only the live regimes, captured the day they are first printed.
VINTAGE_INSERT_SQL = """
INSERT INTO signal_daily_live_vintage
    (signal_key, bucket_date, zone_scope, regime, basis, value, n_legs)
VALUES ($1, $2, $3, $4, $5, $6, $7)
"""
LIVE_REGIMES = ("bbox", "mmsi_filter")


# ----------------------------------------------------------------------
# Output row + helpers
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class SignalRow:
    """One row of the signal_daily table. The three confidence components
    (SIGNALS.md §0·8) are decomposed metadata, not part of the value — the model
    layer combines them as observation variance; each is None where not meaningful."""

    signal_key: str
    bucket_date: date
    zone_scope: str
    regime: str
    value: float
    n_legs: int | None
    basis: str = BASIS_PHYSICAL
    value_dispersion: float | None = None  # MAD of per-item measurements (distributions)
    open_fraction: float | None = None  # share of value from un-terminated items
    estimated_fraction: float | None = None  # share resting on an estimated magnitude


@dataclass(frozen=True)
class LaneFilter:
    """Export/import zone membership, derived from terminals.flow_direction."""

    export_zones: frozenset[str]
    import_zones: frozenset[str]

    def is_export(self, zone: str | None) -> bool:
        return zone is not None and zone in self.export_zones

    def is_import(self, zone: str | None) -> bool:
        return zone is not None and zone in self.import_zones


def build_lane_filter(terminal_rows: list) -> LaneFilter:
    """Group zones by their terminals' flow_direction. A zone is export/import
    only if *all* its terminals agree (they do today: US zones export, EU import)."""
    dirs: dict[str, set[str]] = defaultdict(set)
    for r in terminal_rows:
        dirs[r["zone"]].add(r["flow_direction"])
    export = frozenset(z for z, d in dirs.items() if d == {"export"})
    imp = frozenset(z for z, d in dirs.items() if d == {"import"})
    return LaneFilter(export_zones=export, import_zones=imp)


def daily_buckets(start: date, end: date) -> list[date]:
    """Inclusive UTC day grid [start, end]."""
    if end < start:
        return []
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


# ----------------------------------------------------------------------
# Live intervals — the half-open [start, end) of dates an item is "live"
# ----------------------------------------------------------------------


def leg_interval(leg: Leg, panel_end: date) -> tuple[date, date]:
    """Dates a leg is in transit. Closed legs end at the arrival day (not counted
    on/after arrival); open legs run through panel_end."""
    start = leg.departed_ts.date()
    if leg.status == "closed" and leg.arrived_ts is not None:
        end_excl = leg.arrived_ts.date()
    else:
        end_excl = panel_end + timedelta(days=1)
    return start, end_excl


def _leg_window_days(leg: Leg) -> int:
    """Expected-voyage days for a leg, matching legs.py's open-leg classifier
    exactly (single source of truth): the declared destination region's window,
    else the NW-Europe fallback window, else the no-region censor floor."""
    region = leg.dest_region if leg.dest_region is not None else FALLBACK_DEST_REGION
    return OD_WINDOW_DAYS.get(region, CENSOR_OPEN_DAYS)


def knowable_leg_interval(leg: Leg, panel_end: date) -> tuple[date, date]:
    """Dates a leg was *knowably* in transit — the point-in-time view (SIGNALS.md
    §0·7). A leg is visible as in-transit from departure until either its arrival
    is *observed* (closed/same_zone → real event, knowable when it happened) or,
    if no arrival is ever seen, the day the live pipeline would have aged it out of
    the in-transit pool (departed + the O-D voyage window — the SAME horizon
    legs.py uses to flip open_in_transit → overdue). This is what makes `knowable`
    differ from `physical`: an eventually-phantom/floating/gap leg still
    contributed on the pre-recognition days, exactly as a live observer saw it."""
    start = leg.departed_ts.date()
    if leg.arrived_ts is not None:
        end_excl = leg.arrived_ts.date()  # observed arrival — knowable in real time
    else:
        horizon = start + timedelta(days=_leg_window_days(leg))
        end_excl = min(horizon, panel_end + timedelta(days=1))
    return start, end_excl


def visit_interval(visit: Visit, panel_end: date) -> tuple[date, date]:
    """Dates a vessel is in berth. The mooring day always counts (floor of one
    day, so a same-day load/discharge isn't silently dropped); the departure day
    does not (half-open). An open visit (no observed departure) runs through
    panel_end but is capped at OPEN_VISIT_CEILING_DAYS so a missed-departure
    phantom doesn't inflate the stock forever."""
    start = visit.moored_ts.date()
    if visit.departed_ts is not None:
        end_excl = max(visit.departed_ts.date(), start + timedelta(days=1))
    else:
        ceiling = start + timedelta(days=OPEN_VISIT_CEILING_DAYS)
        end_excl = min(panel_end + timedelta(days=1), ceiling)
        end_excl = max(end_excl, start + timedelta(days=1))  # always count moored day
    return start, end_excl


def visit_berth_interval(visit: Visit, panel_end: date) -> tuple[date, date]:
    """Day span a berth visit deposits its (amortized) cargo over: the mooring
    day through the departure day *inclusive* — unlike `visit_interval`, the
    departure day is included because it carries real berth hours (midnight →
    departed_ts) over which loading is still happening. An open visit runs to
    panel_end, capped at OPEN_VISIT_CEILING_DAYS."""
    start = visit.moored_ts.date()
    if visit.departed_ts is not None:
        end_day = max(visit.departed_ts.date(), start)
    else:
        end_day = min(panel_end, start + timedelta(days=OPEN_VISIT_CEILING_DAYS))
    return start, end_day + timedelta(days=1)


def terminal_dwell_hours(visits: list[Visit]) -> tuple[dict[int, float], float]:
    """Mean observed berth dwell (hours) per terminal from *closed* visits, plus
    a global-mean fallback for terminals with no closed visit yet. Used to amortize
    an *open* visit's cargo over an estimated total dwell until its real duration
    is known (closed visits use their own observed dwell)."""
    per: dict[int, list[float]] = defaultdict(list)
    for v in visits:
        if v.departed_ts is None or v.terminal_id is None:
            continue
        hours = (v.departed_ts - v.moored_ts).total_seconds() / 3600.0
        if hours > 0:
            per[v.terminal_id].append(hours)
    means = {t: sum(h) / len(h) for t, h in per.items()}
    allh = [h for hs in per.values() for h in hs]
    return means, (sum(allh) / len(allh) if allh else DEFAULT_BERTH_HOURS)


def items_live_on(items, target: date, interval_of) -> list:
    """The items whose live interval covers `target` — the exact contributor set
    behind a charted value on that day. Open items run through `target`."""
    out = []
    for it in items:
        start, end_excl = interval_of(it, target)
        if start <= target < end_excl:
            out.append(it)
    return out


# ----------------------------------------------------------------------
# Item selection — which legs/visits feed each signal
# ----------------------------------------------------------------------


def lane_legs(
    legs: list[Leg], lane: LaneFilter, *, include_overdue: bool = False
) -> list[Leg]:
    """In-transit base for #2: laden closed (export→import) + laden
    open_in_transit (export-origin). Excludes same_zone.

    `include_overdue` (knowable basis): also keep export-origin laden legs that
    resolved to phantom/floating/gap — `knowable_leg_interval` counts them only
    over their pre-recognition window. `physical` leaves them out (hindsight)."""
    out: list[Leg] = []
    for leg in legs:
        if leg.laden is not True or not lane.is_export(leg.origin_zone):
            continue
        if leg.status == "closed" and lane.is_import(leg.dest_zone):
            out.append(leg)
        elif leg.status == "open_in_transit":
            out.append(leg)
        elif include_overdue and leg.status in OPEN_OVERDUE_STATUSES:
            out.append(leg)
    return out


def ballast_to_us_legs(
    legs: list[Leg], lane: LaneFilter, *, include_overdue: bool = False
) -> list[Leg]:
    """Return base for #4: ballast (empty) legs that left an EU import zone and
    are heading back to the US — closed legs arriving at a US export zone, plus
    open_in_transit ballast legs (destination assumed US; banded 'unknown' when
    undeclared). `include_overdue` (knowable): also the pre-recognition window of
    overdue ballast legs (see lane_legs)."""
    out: list[Leg] = []
    for leg in legs:
        if leg.laden is not False or not lane.is_import(leg.origin_zone):
            continue
        if leg.status == "closed" and lane.is_export(leg.dest_zone):
            out.append(leg)
        elif leg.status == "open_in_transit":
            out.append(leg)
        elif include_overdue and leg.status in OPEN_OVERDUE_STATUSES:
            out.append(leg)
    return out


def discharging_eu_visits(visits: list[Visit]) -> list[Visit]:
    """Base for #1: laden vessels in berth at EU import terminals (discharging)."""
    return [
        v
        for v in visits
        if v.flow_direction == "import"
        and v.laden is True
        and v.terminal_id is not None
    ]


def loading_us_visits(visits: list[Visit]) -> list[Visit]:
    """Base for #3: vessels in berth at US export terminals (loading). No laden
    filter — the vessel arrives ballast and its gas_capacity_m3 is the cargo
    being loaded into it."""
    return [
        v for v in visits if v.flow_direction == "export" and v.terminal_id is not None
    ]


# ----------------------------------------------------------------------
# Band assignment — the stacked dimension carried in zone_scope
# ----------------------------------------------------------------------


def dest_band(leg: Leg, lane: LaneFilter, expect: str) -> str:
    """Destination band for an in-transit/ballast leg. Closed legs use the
    *observed* arrival zone (already constrained to the right side by the
    selectors). Open legs use the declared destination region — but only when it
    points the way the leg is actually going (`expect` = 'import' for a laden
    US→EU cargo, 'export' for a ballast US-return); otherwise 'unknown'.

    The guard matters: a master often updates the declared destination to the
    *next load port* (a US terminal) while a laden voyage is still completing, so
    an undefended `dest_region` would mis-band a laden cargo bound for Europe as
    'usgulf'. We only trust a declaration that agrees with the leg's direction."""
    if leg.status == "closed":
        return leg.dest_zone or UNKNOWN_BAND
    region = leg.dest_region
    ok = lane.is_import(region) if expect == "import" else lane.is_export(region)
    return region if ok else UNKNOWN_BAND


def transit_dest_band(leg: Leg, lane: LaneFilter) -> str:
    """Band for the laden US→EU in-transit signal (declared dest must be import)."""
    return dest_band(leg, lane, "import")


def ballast_dest_band(leg: Leg, lane: LaneFilter) -> str:
    """Band for the ballast US-return signal (declared dest must be export)."""
    return dest_band(leg, lane, "export")


def visit_terminal_band(visit: Visit) -> str:
    """Terminal band for a berth visit (the per-terminal stacking key)."""
    return str(visit.terminal_id)


# ----------------------------------------------------------------------
# Pure aggregation — per-day reconstruction over live intervals
# ----------------------------------------------------------------------


def _gas(item, _d: date) -> float | None:
    """Per-day contribution = the item's gas capacity (constant while live).
    None ⇒ the item is skipped (and not counted in n_legs). Used by the at-sea
    *stock* signals."""
    return item.gas_capacity_m3


def amortized_cargo_contribution(
    dwell_means: dict[int, float], global_mean: float, now: datetime
) -> Callable[[Visit, date], float | None]:
    """Per-day contribution for a *berth* signal as an amortized **flow**: a
    visit's cargo (`gas_capacity_m3`) is spread across its berth hours at a
    constant rate so the visit integrates to exactly one cargo. The per-day value
    is the cargo deposited between that day's bounds:

        rate = capacity / total_berth_hours
        total_berth_hours = observed (departed - moored) for a closed visit, else
            the terminal's mean dwell (global-mean fallback) for an open one.

    The cumulative deposit is capped at one cargo, so an open visit lingering past
    its estimated dwell plateaus at full capacity and then contributes 0/day (it
    stops showing once it is "loaded" on estimate) rather than over-counting; a
    closed visit re-normalizes to its true dwell on the next rebuild. None ⇒ no
    contribution that day (skipped, uncounted), so n_legs counts the vessels
    *actively* loading/discharging that day."""

    def contribution(visit: Visit, d: date) -> float | None:
        cap = visit.gas_capacity_m3
        if cap is None:
            return None
        t0 = visit.moored_ts
        if visit.departed_ts is not None:
            t_end = visit.departed_ts
            total_h = (t_end - t0).total_seconds() / 3600.0
        else:
            est = dwell_means.get(visit.terminal_id, global_mean)
            total_h = est if est > 0 else DEFAULT_BERTH_HOURS
            t_end = min(now, t0 + timedelta(days=OPEN_VISIT_CEILING_DAYS))
        if total_h <= 0:
            total_h = DEFAULT_BERTH_HOURS
        rate = cap / total_h  # m³ per hour

        def deposited_by(t: datetime) -> float:
            h = (t - t0).total_seconds() / 3600.0
            return min(float(cap), rate * h) if h > 0 else 0.0

        day_start = datetime(d.year, d.month, d.day, tzinfo=UTC)
        lo = max(day_start, t0)
        hi = min(day_start + timedelta(days=1), t_end)
        if hi <= lo:
            return None
        c = deposited_by(hi) - deposited_by(lo)
        return c if c > 1e-9 else None

    return contribution


def amortized_cargo_knowable(
    dwell_means: dict[int, float], global_mean: float
) -> Callable[[Visit, date], float | None]:
    """Knowable (point-in-time) berth flow (SIGNALS.md §0·7). Where the physical
    version amortizes a *closed* visit over its **observed** dwell (hindsight —
    integrates to exactly one cargo), the knowable version uses the **estimated**
    dwell (terminal mean) for the whole visit, because while a vessel is alongside
    the live system does not yet know the final dwell — it can only estimate the
    loading *rate*. Deposits stop at the observed departure (truncate, never
    retro-true-up). So a visit that loads faster than the mean deposits <1 cargo in
    `knowable` and a slower one plateaus at the one-cargo cap — that asymmetry is
    the genuine real-time signal, not a bug. Same single rate per visit, so the
    per-day deposits stay self-consistent."""

    def contribution(visit: Visit, d: date) -> float | None:
        cap = visit.gas_capacity_m3
        if cap is None:
            return None
        t0 = visit.moored_ts
        est = dwell_means.get(visit.terminal_id, global_mean)
        total_h = est if est > 0 else DEFAULT_BERTH_HOURS
        rate = cap / total_h
        # End at the estimated dwell end, truncated by the observed departure once
        # it is seen, and never past the open-visit phantom ceiling.
        t_end = t0 + timedelta(hours=total_h)
        if visit.departed_ts is not None:
            t_end = min(t_end, visit.departed_ts)
        t_end = min(t_end, t0 + timedelta(days=OPEN_VISIT_CEILING_DAYS))

        def deposited_by(t: datetime) -> float:
            h = (t - t0).total_seconds() / 3600.0
            return min(float(cap), rate * h) if h > 0 else 0.0

        day_start = datetime(d.year, d.month, d.day, tzinfo=UTC)
        lo = max(day_start, t0)
        hi = min(day_start + timedelta(days=1), t_end)
        if hi <= lo:
            return None
        c = deposited_by(hi) - deposited_by(lo)
        return c if c > 1e-9 else None

    return contribution


def accumulate_daily(
    items: list,
    days: list[date],
    *,
    signal_key: str,
    interval_of: Callable,
    band_of: Callable[[object], str],
    contribution: Callable[[object, date], float | None] = _gas,
    aggregate: str = "sum",  # 'sum' | 'mean'
    basis: str = BASIS_PHYSICAL,
    open_of: Callable[[object], bool] | None = None,
) -> list[SignalRow]:
    """Stacked per-day reconstruction. For each item, accumulate its
    `contribution` (None ⇒ skipped that day) into a (band, regime, day) cell over
    its live interval, then emit a sum or mean per cell. Each item is tagged by
    its own regime plus a synthetic 'all' regime, so a regime-segmented and a
    pooled series are both available. `band_of` is the stacked dimension written
    to zone_scope.

    `open_of` (the confidence component, SIGNALS.md §0·8): when given, the share of
    each cell's value contributed by items it flags `True` (un-terminated: open
    legs / open visits) is written to `open_fraction` — the censoring-exposure axis.
    None ⇒ open_fraction left NULL (signal has no open/closed distinction)."""
    if not days:
        return []
    panel_start, panel_end = days[0], days[-1]
    last_excl = panel_end + timedelta(days=1)
    # (band, regime, date) -> [total, count, open_total]
    acc: dict[tuple[str, str, date], list[float]] = defaultdict(
        lambda: [0.0, 0.0, 0.0]
    )
    for item in items:
        start, end_excl = interval_of(item, panel_end)
        band = band_of(item)
        is_open = bool(open_of(item)) if open_of is not None else False
        d = max(start, panel_start)
        hi = min(end_excl, last_excl)
        while d < hi:
            c = contribution(item, d)
            if c is not None:
                for regime in (item.regime, "all"):
                    cell = acc[(band, regime, d)]
                    cell[0] += c
                    cell[1] += 1
                    if is_open:
                        cell[2] += c
            d += timedelta(days=1)
    rows: list[SignalRow] = []
    for (band, regime, d), (total, count, open_total) in acc.items():
        value = total / count if aggregate == "mean" else total
        open_frac = (open_total / total) if (open_of is not None and total) else None
        rows.append(
            SignalRow(
                signal_key, d, band, regime, value, int(count), basis,
                open_fraction=open_frac,
            )
        )
    return rows


# ----------------------------------------------------------------------
# Phase 1 — event/measurement signals (the complement of the stock builder)
# ----------------------------------------------------------------------


def _median_mad(values: list[float]) -> tuple[float, float | None]:
    """Median and Median Absolute Deviation — a robust centre + spread pair, used
    as (value, value_dispersion) for the distributional signals. MAD (not stdev)
    so a phantom-long tail can't blow up the spread. None spread for n<2."""
    med = statistics.median(values)
    if len(values) < 2:
        return med, None
    mad = statistics.median([abs(v - med) for v in values])
    return med, mad


SLOW_STEAM_KN = 13.0  # implied-speed threshold for the slow-steaming share (#24)


def accumulate_events(
    items: list,
    days: list[date],
    *,
    signal_key: str,
    measure_of: Callable[[object], float | None],
    date_of: Callable[[object], date],
    band_of: Callable[[object], str],
    stat: str = "median",  # 'median' (+MAD) | 'count' | 'fraction'
    bases: tuple[str, ...] = ALL_BASES,
    estimated_of: Callable[[object], bool] | None = None,
) -> list[SignalRow]:
    """Per-event aggregation — the complement of accumulate_daily's interval/stock
    reconstruction. Each item yields ONE measurement attributed to ONE day (its
    completion day: arrival or departure), grouped into a (band, regime, day) cell
    and reduced by `stat`:

      - 'median'   → value = median, value_dispersion = MAD (robust spread)
      - 'count'    → value = number of events (n)
      - 'fraction' → value = mean of 0/1 measurements (a rate)

    These signals are built over *closed* items, so the measurement is fixed once
    observed — `knowable == physical` by construction (a leg's speed is learned on
    its arrival day, exactly when a live observer would learn it). We emit identical
    rows under every basis in `bases` so a basis filter never silently drops the
    signal. Each item carries its own `regime`; a synthetic 'all' regime pools."""
    if not days:
        return []
    panel_start, panel_end = days[0], days[-1]
    acc: dict[tuple[str, str, date], list[tuple[float, bool]]] = defaultdict(list)
    for item in items:
        m = measure_of(item)
        if m is None:
            continue
        d = date_of(item)
        if d < panel_start or d > panel_end:
            continue
        est = bool(estimated_of(item)) if estimated_of is not None else False
        for regime in (item.regime, "all"):
            acc[(band_of(item), regime, d)].append((m, est))
    rows: list[SignalRow] = []
    for (band, regime, d), pairs in acc.items():
        vals = [m for m, _ in pairs]
        n = len(vals)
        disp: float | None = None
        if stat == "count":
            value = float(n)
        elif stat == "fraction":
            value = sum(vals) / n
        else:  # median
            value, disp = _median_mad(vals)
        est_frac = (
            sum(1 for _, e in pairs if e) / n if estimated_of is not None else None
        )
        for basis in bases:
            rows.append(
                SignalRow(
                    signal_key, d, band, regime, value, n, basis,
                    value_dispersion=disp, estimated_fraction=est_frac,
                )
            )
    return rows


def closed_visits(visits: list[Visit], flow: str) -> list[Visit]:
    """Completed berth visits at terminals of one flow direction — the base for
    the berth-turn-time signals (#8 export, #14 import)."""
    return [
        v
        for v in visits
        if v.flow_direction == flow
        and v.departed_ts is not None
        and v.terminal_id is not None
    ]


def berth_turn_hours(v: Visit) -> float | None:
    """Berth occupancy hours = departed − moored (#8 / #14). None if non-positive."""
    h = (v.departed_ts - v.moored_ts).total_seconds() / 3600.0
    return h if h > 0 else None


def closed_lane_legs(legs: list[Leg], lane: LaneFilter) -> list[Leg]:
    """Closed, laden, cross-zone US→EU legs with a real duration — the base for
    voyage-time anomaly (#21, duration only), implied speed (#22) and slow-steaming
    (#24). Distance is NOT required here: most legs arrive via GFW, whose events
    carry no lat/lon, so `leg.distance_nm` is NULL — the speed signals recover the
    distance from terminal centroids (`leg_distance_nm`); #21 needs no distance."""
    return [
        lg
        for lg in legs
        if lg.status == "closed"
        and lg.laden is True
        and lane.is_export(lg.origin_zone)
        and lane.is_import(lg.dest_zone)
        and lg.duration_h
        and lg.duration_h > 0
    ]


def od_lane_band(leg: Leg) -> str:
    """O-D lane band, e.g. 'usgulf->nweurope' (the #5 O-D dimension)."""
    return f"{leg.origin_zone}->{leg.dest_zone}"


def leg_distance_nm(
    leg: Leg, centroids: dict[int, tuple[float, float]]
) -> float | None:
    """Great-circle voyage distance: the leg's own fix-to-fix distance when both
    endpoints had coordinates (live legs), else the origin→dest terminal-centroid
    distance (the historical GFW path, no event coords). None if neither resolves."""
    if leg.distance_nm:
        return leg.distance_nm
    o = centroids.get(leg.origin_terminal_id)
    d = centroids.get(leg.dest_terminal_id)
    if o is None or d is None:
        return None
    return haversine_nm(o[0], o[1], d[0], d[1])


def leg_speed_kn(
    leg: Leg, centroids: dict[int, tuple[float, float]]
) -> float | None:
    """Implied average voyage speed (knots) = great-circle nm / voyage hours (#22).
    Distance via `leg_distance_nm` (centroid fallback). None if no distance."""
    dist = leg_distance_nm(leg, centroids)
    return dist / leg.duration_h if dist is not None else None


def typical_od_duration_h(closed_legs: list[Leg]) -> dict[tuple[str, str], float]:
    """Median observed voyage duration (h) per O-D lane — the baseline #21 measures
    the anomaly against. Pooled over all regimes (a lane's great-circle time is a
    physical constant, not a regime artifact)."""
    per: dict[tuple[str, str], list[float]] = defaultdict(list)
    for lg in closed_legs:
        per[(lg.origin_zone, lg.dest_zone)].append(lg.duration_h)
    return {k: statistics.median(v) for k, v in per.items()}


@dataclass(frozen=True)
class RoundTrip:
    """One vessel's gap between two consecutive departures (#32). Tagged by the
    later departure's regime so it segments like every other signal."""

    mmsi: int
    regime: str
    departed_ts: datetime
    origin_zone: str
    days: float


def round_trips(legs: list[Leg]) -> list[RoundTrip]:
    """Consecutive-departure gaps per vessel = round-trip time (#32). Sorted by
    departure; each adjacent pair contributes one positive-day gap."""
    by_mmsi: dict[int, list[Leg]] = defaultdict(list)
    for lg in legs:
        by_mmsi[lg.mmsi].append(lg)
    out: list[RoundTrip] = []
    for ls in by_mmsi.values():
        ls.sort(key=lambda x: x.departed_ts)
        for a, b in zip(ls, ls[1:]):
            d = (b.departed_ts - a.departed_ts).total_seconds() / 86400.0
            if d > 0:
                out.append(RoundTrip(b.mmsi, b.regime, b.departed_ts, b.origin_zone, d))
    return out


def voyage_age_days(leg: Leg, d: date) -> float | None:
    """Per-day contribution for #20: the leg's age in days on day d (now − departed),
    a *mean* over the open legs live that day. Always ≥0 over the live interval."""
    age = (d - leg.departed_ts.date()).days
    return float(age) if age >= 0 else None


def fleet_daily(
    legs: list[Leg],
    visits: list[Visit],
    days: list[date],
    *,
    basis: str,
    leg_interval_fn: Callable,
) -> list[SignalRow]:
    """Fleet-utilisation stocks #33/#34 as daily distinct-vessel counts. A vessel
    is *active* on day d if any of its legs (in-transit interval) or visits (berth
    interval) covers d; *laden* if a laden leg covers d. These pool across vessels
    of mixed regime, so they're emitted under 'all' only. Dual-basis via
    `leg_interval_fn` (knowable caps open legs at their voyage window; physical runs
    them to panel_end)."""
    if not days:
        return []
    panel_start, panel_end = days[0], days[-1]
    last_excl = panel_end + timedelta(days=1)
    active: dict[date, set[int]] = defaultdict(set)
    laden: dict[date, set[int]] = defaultdict(set)

    def mark(item, interval_of, is_laden: bool) -> None:
        start, end_excl = interval_of(item, panel_end)
        d = max(start, panel_start)
        hi = min(end_excl, last_excl)
        while d < hi:
            active[d].add(item.mmsi)
            if is_laden:
                laden[d].add(item.mmsi)
            d += timedelta(days=1)

    for lg in legs:
        mark(lg, leg_interval_fn, lg.laden is True)
    for v in visits:
        mark(v, visit_interval, False)  # in-berth = active, not laden-at-sea

    rows: list[SignalRow] = []
    for d in days:
        a = len(active.get(d, ()))
        if a == 0:
            continue
        ln = len(laden.get(d, ()))
        rows.append(SignalRow("active_vessels", d, "fleet", "all", float(a), a, basis))
        rows.append(
            SignalRow("fleet_laden_frac", d, "fleet", "all", ln / a, a, basis)
        )
    return rows


# ----------------------------------------------------------------------
# Phase 2 — anchorage-queue signals (over pipeline/queues.py)
# ----------------------------------------------------------------------


def flow_queues(queues: list[Queue], flow: str) -> list[Queue]:
    """Queues at terminals of one flow direction with a known terminal — the base
    for the load (#6/#7, export) and discharge (#12/#13, import) queue signals."""
    return [q for q in queues if q.flow_direction == flow and q.terminal_id is not None]


def queue_band(q: Queue) -> str:
    """Per-terminal band (the queue stacking key)."""
    return str(q.terminal_id)


def terminal_queue_hours(queues: list[Queue]) -> tuple[dict[int, float], float]:
    """Mean observed wait (h) per terminal from *closed* queues, plus a global-mean
    fallback. Used to estimate an open (still-waiting) queue's eventual total wait
    so the live nowcast reflects vessels currently stuck, not just completed ones."""
    per: dict[int, list[float]] = defaultdict(list)
    for q in queues:
        h = q.queue_h
        if h is not None and h > 0 and q.terminal_id is not None:
            per[q.terminal_id].append(h)
    means = {t: sum(v) / len(v) for t, v in per.items()}
    allh = [h for hs in per.values() for h in hs]
    return means, (sum(allh) / len(allh) if allh else 0.0)


# An open (un-berthed) queue still "waiting" past this many days is almost always a
# phantom — the vessel berthed or left while AIS-silent and we never saw the close.
# Real LNG anchorage waits are hours-to-days; a fortnight is a generous ceiling.
# It caps both the open-queue depth interval and the estimated-wait magnitude, the
# queue analog of OPEN_VISIT_CEILING_DAYS / the open-leg censor.
QUEUE_OPEN_CEILING_DAYS = 14


def queue_interval(q: Queue, panel_end: date) -> tuple[date, date]:
    """Dates a vessel is in queue (depth #7/#13): entry day → mooring day (half-open;
    not counted on the day it berths). An open queue runs to panel_end but is capped
    at QUEUE_OPEN_CEILING_DAYS so a phantom (lost vessel) can't inflate depth forever."""
    start = q.entry_ts.date()
    if q.moored_ts is not None:
        end_excl = max(q.moored_ts.date(), start + timedelta(days=1))
    else:
        ceiling = start + timedelta(days=QUEUE_OPEN_CEILING_DAYS)
        end_excl = min(panel_end + timedelta(days=1), ceiling)
        end_excl = max(end_excl, start + timedelta(days=1))
    return start, end_excl


def knowable_queue_interval(q: Queue, panel_end: date) -> tuple[date, date]:
    """Knowable depth interval. Open queues are only ever 'today' (an as-of=now
    rebuild has no past open queues), so the two bases coincide for depth here."""
    return queue_interval(q, panel_end)


def queued_arrivals_index(
    queues: list[Queue],
) -> tuple[set[tuple[int, datetime]], set[tuple[int, datetime]]]:
    """(queued, meaningfully-queued) arrival keys (mmsi, moored_ts) from closed
    queues — used to compute the #15/#16 rates over the full arrival set (visits)."""
    queued: set[tuple[int, datetime]] = set()
    meaningful: set[tuple[int, datetime]] = set()
    for q in queues:
        if q.moored_ts is None:
            continue
        key = (q.mmsi, q.moored_ts)
        queued.add(key)
        if q.anchored_seen:
            meaningful.add(key)
    return queued, meaningful


# ----------------------------------------------------------------------
# Phase 3 — outage / anomaly / fleet signals
# ----------------------------------------------------------------------


def days_since_rows(
    dates_by_band: dict[str, list[date]],
    days: list[date],
    signal_key: str,
    *,
    bases: tuple[str, ...] = ALL_BASES,
) -> list[SignalRow]:
    """Per-band daily recency: days since the most recent event on/before each day —
    the outage detector (#36/#37). Pooled across sources (regime='all'): outage
    detection wants the latest event from ANY feed, so a coverage gap in one source
    doesn't masquerade as a terminal going quiet. Basis-invariant (an event is known
    when it happens), so emitted identically under both bases."""
    rows: list[SignalRow] = []
    for band, dates in dates_by_band.items():
        ds = sorted(set(dates))
        i, last = 0, None
        for d in days:
            while i < len(ds) and ds[i] <= d:
                last = ds[i]
                i += 1
            if last is not None:
                v = float((d - last).days)
                for basis in bases:
                    rows.append(SignalRow(signal_key, d, band, "all", v, None, basis))
    return rows


def queue_wow_rows(
    queues: list[Queue],
    days: list[date],
    signal_key: str,
    interval_of: Callable,
    *,
    bases: tuple[str, ...] = ALL_BASES,
) -> list[SignalRow]:
    """Week-over-week change in queue depth (#38) — a sudden jump leads an outage
    before it is confirmed. Builds the per-(terminal, regime) daily depth, then emits
    depth[d] − depth[d−7] wherever either week is non-empty."""
    if not days:
        return []
    panel_start, panel_end = days[0], days[-1]
    last_excl = panel_end + timedelta(days=1)
    acc: dict[tuple[str, str], dict[date, float]] = defaultdict(
        lambda: defaultdict(float)
    )
    for q in queues:
        s, e = interval_of(q, panel_end)
        band = queue_band(q)
        d = max(s, panel_start)
        hi = min(e, last_excl)
        while d < hi:
            for regime in (q.regime, "all"):
                acc[(band, regime)][d] += 1.0
            d += timedelta(days=1)
    week = timedelta(days=7)
    rows: list[SignalRow] = []
    for (band, regime), byday in acc.items():
        for d in days:
            prev = byday.get(d - week, 0.0)
            cur = byday.get(d, 0.0)
            if prev or cur:
                for basis in bases:
                    rows.append(
                        SignalRow(signal_key, d, band, regime, cur - prev, None, basis)
                    )
    return rows


def newbuild_rows(
    legs: list[Leg],
    visits: list[Visit],
    queues: list[Queue],
    days: list[date],
    *,
    bases: tuple[str, ...] = ALL_BASES,
) -> list[SignalRow]:
    """Fleet growth (#35): count of MMSIs making their first appearance (earliest
    leg/visit/queue event) per day. A coarse newbuild-into-service proxy banded
    'fleet'. Basis-invariant."""
    first: dict[int, date] = {}

    def note(mmsi: int, d: date) -> None:
        if mmsi not in first or d < first[mmsi]:
            first[mmsi] = d

    for lg in legs:
        note(lg.mmsi, lg.departed_ts.date())
    for v in visits:
        note(v.mmsi, v.moored_ts.date())
    for q in queues:
        note(q.mmsi, q.entry_ts.date())

    per_day = Counter(first.values())
    rows: list[SignalRow] = []
    for d in days:
        c = per_day.get(d, 0)
        if c:
            for basis in bases:
                rows.append(SignalRow("newbuild_appearances", d, "fleet", "all", float(c), c, basis))
    return rows


# ----------------------------------------------------------------------
# DB orchestration
# ----------------------------------------------------------------------


async def build_signals(
    pool: asyncpg.Pool, now: datetime, *, panel_start: date | None
) -> tuple[list[SignalRow], dict]:
    """Load the leg + visit bases + terminal metadata, run every aggregator, and
    return all signal rows (+ a summary for logging)."""
    legs = await compute_legs(pool, now, enrich=True)
    visits = await compute_visits(pool, now)
    queues = await compute_queues(pool, now)
    async with pool.acquire() as conn:
        term_rows = await conn.fetch(TERMINAL_METADATA_SQL)
        centroid_rows = await conn.fetch(TERMINAL_CENTROID_SQL)

    lane = build_lane_filter(term_rows)
    centroids: dict[int, tuple[float, float]] = {
        r["terminal_id"]: (r["lat"], r["lon"]) for r in centroid_rows
    }

    # Visit bases are basis-independent (same berth occupancy); only the per-day
    # contribution differs by basis. Leg bases differ by basis: knowable also
    # admits the overdue open legs over their pre-recognition window.
    discharging = discharging_eu_visits(visits)
    loading = loading_us_visits(visits)
    transit_phys = lane_legs(legs, lane)
    ballast_phys = ballast_to_us_legs(legs, lane)
    transit_know = lane_legs(legs, lane, include_overdue=True)
    ballast_know = ballast_to_us_legs(legs, lane, include_overdue=True)

    panel_end = now.date()
    if panel_start is None:
        starts = [lg.departed_ts.date() for lg in transit_know + ballast_know] + [
            v.moored_ts.date() for v in discharging + loading
        ]
        panel_start = min(starts) if starts else panel_end
    days = daily_buckets(panel_start, panel_end)

    load_means, load_global = terminal_dwell_hours(loading)
    disch_means, disch_global = terminal_dwell_hours(discharging)

    # Per-basis aggregator wiring. physical: hindsight intervals + observed-dwell
    # amortization. knowable: point-in-time leg intervals (overdue legs counted
    # pre-recognition) + estimated-dwell amortization. See SIGNALS.md §0·7.
    plans = {
        BASIS_PHYSICAL: dict(
            transit=transit_phys,
            ballast=ballast_phys,
            leg_interval=leg_interval,
            load_contrib=amortized_cargo_contribution(load_means, load_global, now),
            disch_contrib=amortized_cargo_contribution(disch_means, disch_global, now),
        ),
        BASIS_KNOWABLE: dict(
            transit=transit_know,
            ballast=ballast_know,
            leg_interval=knowable_leg_interval,
            load_contrib=amortized_cargo_knowable(load_means, load_global),
            disch_contrib=amortized_cargo_knowable(disch_means, disch_global),
        ),
    }

    visit_open = lambda v: v.departed_ts is None  # noqa: E731 — open berth visit
    leg_open = lambda lg: lg.status != "closed"  # noqa: E731 — un-arrived leg

    rows: list[SignalRow] = []
    for basis, p in plans.items():
        # --- Headline gas-volume stocks/flows (now carrying open_fraction) ---
        rows += accumulate_daily(
            loading,
            days,
            signal_key="gas_loading_us",
            interval_of=visit_berth_interval,
            band_of=visit_terminal_band,
            contribution=p["load_contrib"],
            basis=basis,
            open_of=visit_open,
        )
        rows += accumulate_daily(
            discharging,
            days,
            signal_key="gas_discharging_eu",
            interval_of=visit_berth_interval,
            band_of=visit_terminal_band,
            contribution=p["disch_contrib"],
            basis=basis,
            open_of=visit_open,
        )
        rows += accumulate_daily(
            p["transit"],
            days,
            signal_key="gas_in_transit_volume",
            interval_of=p["leg_interval"],
            band_of=lambda lg: transit_dest_band(lg, lane),
            basis=basis,
            open_of=leg_open,
        )
        rows += accumulate_daily(
            p["ballast"],
            days,
            signal_key="gas_ballast_to_us",
            interval_of=p["leg_interval"],
            band_of=lambda lg: ballast_dest_band(lg, lane),
            basis=basis,
            open_of=leg_open,
        )
        # --- #20 mean laden-voyage age (floating-storage proxy). Shares the exact
        #     in-transit leg base as gas_in_transit_volume — the MEAN age of the
        #     cargo at sea each day vs that signal's SUM of its volume. Under each
        #     basis a leg contributes age = d−departed over the same interval it
        #     contributes volume, so #20 inherits the same decade of depth (a closed
        #     historical leg ages over its pre-arrival days; physical and knowable
        #     differ only in how un-arrived legs are censored). ---
        rows += accumulate_daily(
            p["transit"],
            days,
            signal_key="laden_voyage_age_d",
            interval_of=p["leg_interval"],
            band_of=lambda lg: transit_dest_band(lg, lane),
            contribution=voyage_age_days,
            aggregate="mean",
            basis=basis,
            open_of=leg_open,
        )
        # --- #33/#34 fleet utilisation (basis-dependent via leg interval) ---
        rows += fleet_daily(
            legs, visits, days, basis=basis, leg_interval_fn=p["leg_interval"]
        )

    # --- Event/measurement signals (closed items ⇒ knowable == physical; emitted
    #     once under both bases). Built over the closed leg/visit bases. ---
    cl_legs = closed_lane_legs(legs, lane)
    typical = typical_od_duration_h(cl_legs)
    export_v = closed_visits(visits, "export")
    rows += accumulate_events(
        export_v, days, signal_key="load_berth_turn_h",
        measure_of=berth_turn_hours, date_of=lambda v: v.departed_ts.date(),
        band_of=visit_terminal_band, stat="median",
    )
    rows += accumulate_events(
        closed_visits(visits, "import"), days, signal_key="discharge_berth_turn_h",
        measure_of=berth_turn_hours, date_of=lambda v: v.departed_ts.date(),
        band_of=visit_terminal_band, stat="median",
    )
    def _slow(lg: Leg) -> float | None:
        s = leg_speed_kn(lg, centroids)
        return None if s is None else (1.0 if s < SLOW_STEAM_KN else 0.0)

    rows += accumulate_events(
        cl_legs, days, signal_key="voyage_speed_kn",
        measure_of=lambda lg: leg_speed_kn(lg, centroids),
        date_of=lambda lg: lg.arrived_ts.date(), band_of=od_lane_band, stat="median",
    )
    rows += accumulate_events(
        cl_legs, days, signal_key="slow_steam_frac", measure_of=_slow,
        date_of=lambda lg: lg.arrived_ts.date(), band_of=od_lane_band, stat="fraction",
    )
    rows += accumulate_events(
        cl_legs, days, signal_key="voyage_time_anomaly_d",
        measure_of=lambda lg: (lg.duration_h - typical[(lg.origin_zone, lg.dest_zone)])
        / 24.0,
        date_of=lambda lg: lg.arrived_ts.date(), band_of=od_lane_band, stat="median",
    )
    rows += accumulate_events(
        export_v, days, signal_key="us_loadings_count",
        measure_of=lambda v: 1.0, date_of=lambda v: v.departed_ts.date(),
        band_of=visit_terminal_band, stat="count",
    )
    rows += accumulate_events(
        [v for v in export_v if not v.cold_start], days,
        signal_key="us_loadings_count_warm",
        measure_of=lambda v: 1.0, date_of=lambda v: v.departed_ts.date(),
        band_of=visit_terminal_band, stat="count",
    )
    rows += accumulate_events(
        round_trips(legs), days, signal_key="round_trip_d",
        measure_of=lambda rt: rt.days, date_of=lambda rt: rt.departed_ts.date(),
        band_of=lambda rt: rt.origin_zone, stat="median",
    )

    # --- Phase 2: anchorage-queue signals (over pipeline/queues.py) -----------
    load_q = flow_queues(queues, "export")
    disch_q = flow_queues(queues, "import")
    loadq_means, loadq_global = terminal_queue_hours(load_q)
    dischq_means, dischq_global = terminal_queue_hours(disch_q)
    is_open_q = lambda q: q.moored_ts is None  # noqa: E731 — still-waiting queue

    ceiling_h = QUEUE_OPEN_CEILING_DAYS * 24.0

    def _queue_measure(means: dict[int, float], glob: float):
        """Wait hours: observed for a closed queue; for an open one the include-
        estimated eventual wait = max(waited-so-far, terminal mean) (#6/#12). An open
        queue waited past the phantom ceiling is dropped (a lost vessel, not a wait);
        otherwise the estimate is capped at the ceiling."""
        def m(q: Queue) -> float | None:
            if q.moored_ts is not None:
                return q.queue_h
            waited = (now - q.entry_ts).total_seconds() / 3600.0
            if waited > ceiling_h:
                return None  # phantom open queue — drop
            est = means.get(q.terminal_id, glob)
            val = min(max(waited, est), ceiling_h)
            return val if val > 0 else None
        return m

    def _queue_date(q: Queue) -> date:
        # closed → the day the wait resolved at berthing; open → today (current view)
        return q.moored_ts.date() if q.moored_ts is not None else panel_end

    # #6 / #12 queue time (median+MAD, per terminal; open queues estimated)
    rows += accumulate_events(
        load_q, days, signal_key="load_queue_h",
        measure_of=_queue_measure(loadq_means, loadq_global),
        date_of=_queue_date, band_of=queue_band, stat="median", estimated_of=is_open_q,
    )
    rows += accumulate_events(
        disch_q, days, signal_key="discharge_queue_h",
        measure_of=_queue_measure(dischq_means, dischq_global),
        date_of=_queue_date, band_of=queue_band, stat="median", estimated_of=is_open_q,
    )

    # #7 / #13 queue depth (daily stock count of vessels in queue, per terminal)
    for basis, qint in (
        (BASIS_PHYSICAL, queue_interval),
        (BASIS_KNOWABLE, knowable_queue_interval),
    ):
        rows += accumulate_daily(
            load_q, days, signal_key="us_queue_depth", interval_of=qint,
            band_of=queue_band, contribution=lambda q, d: 1.0, basis=basis,
            open_of=is_open_q,
        )
        rows += accumulate_daily(
            disch_q, days, signal_key="eu_queue_depth", interval_of=qint,
            band_of=queue_band, contribution=lambda q, d: 1.0, basis=basis,
            open_of=is_open_q,
        )

    # #15 / #16 queue-formation rates over ALL arrivals (visits), per terminal
    arrivals = [v for v in visits if v.terminal_id is not None]
    queued_keys, meaningful_keys = queued_arrivals_index(queues)
    rows += accumulate_events(
        arrivals, days, signal_key="queued_rate",
        measure_of=lambda v: 1.0 if (v.mmsi, v.moored_ts) in queued_keys else 0.0,
        date_of=lambda v: v.moored_ts.date(), band_of=visit_terminal_band,
        stat="fraction",
    )
    rows += accumulate_events(
        arrivals, days, signal_key="meaningful_queue_rate",
        measure_of=lambda v: 1.0 if (v.mmsi, v.moored_ts) in meaningful_keys else 0.0,
        date_of=lambda v: v.moored_ts.date(), band_of=visit_terminal_band,
        stat="fraction",
    )

    # --- Phase 3: outage / anomaly / fleet ------------------------------------
    # #36 days since last export departure, per export terminal (US outage radar)
    dep_dates: dict[str, list[date]] = defaultdict(list)
    for lg in legs:
        if lane.is_export(lg.origin_zone) and lg.origin_terminal_id is not None:
            dep_dates[str(lg.origin_terminal_id)].append(lg.departed_ts.date())
    rows += days_since_rows(dep_dates, days, "days_since_departed")
    # #37 days since last import mooring, per import terminal (EU outage radar)
    moor_dates: dict[str, list[date]] = defaultdict(list)
    for v in visits:
        if v.flow_direction == "import" and v.terminal_id is not None:
            moor_dates[str(v.terminal_id)].append(v.moored_ts.date())
    rows += days_since_rows(moor_dates, days, "days_since_moored")
    # #38 week-over-week queue-depth change (sudden congestion → leading outage)
    rows += queue_wow_rows(load_q, days, "us_queue_formation_wow", queue_interval)
    rows += queue_wow_rows(disch_q, days, "eu_queue_formation_wow", queue_interval)
    # #5 O-D flow count — closed cross-zone voyages per lane, at departure
    closed_all = [lg for lg in legs if lg.status == "closed"]
    rows += accumulate_events(
        closed_all, days, signal_key="od_flow_count", measure_of=lambda lg: 1.0,
        date_of=lambda lg: lg.departed_ts.date(), band_of=od_lane_band, stat="count",
    )
    # #39 cold-start rate per zone (dark-fleet / AIS-off proxy; live-meaningful —
    # historical cold_start is dominated by backfill synthetic entry, so read it
    # within the live regime, segmented by the regime tag).
    rows += accumulate_events(
        arrivals, days, signal_key="cold_start_rate",
        measure_of=lambda v: 1.0 if v.cold_start else 0.0,
        date_of=lambda v: v.moored_ts.date(), band_of=lambda v: v.zone, stat="fraction",
    )
    # #35 newbuild appearances per day (fleet capacity growth)
    rows += newbuild_rows(legs, visits, queues, days)

    summary = {
        "total_rows": len(rows),
        "by_key": Counter(r.signal_key for r in rows),
        "by_regime": Counter(r.regime for r in rows),
        "by_basis": Counter(r.basis for r in rows),
        "panel_start": panel_start,
        "panel_end": panel_end,
        "transit_legs": len(transit_phys),
        "transit_legs_knowable": len(transit_know),
        "transit_open": sum(1 for lg in transit_phys if lg.status == "open_in_transit"),
        "transit_overdue_knowable": sum(
            1 for lg in transit_know if lg.status in OPEN_OVERDUE_STATUSES
        ),
        "transit_unknown_band": sum(
            1 for lg in transit_phys if transit_dest_band(lg, lane) == UNKNOWN_BAND
        ),
        "ballast_legs": len(ballast_phys),
        "discharging_visits": len(discharging),
        "discharging_open": len(items_live_on(discharging, panel_end, visit_interval)),
        "loading_visits": len(loading),
        "loading_open": len(items_live_on(loading, panel_end, visit_interval)),
        "null_gas_legs": sum(
            1 for lg in transit_know + ballast_know if lg.gas_capacity_m3 is None
        ),
        "null_gas_visits": sum(
            1 for v in discharging + loading if v.gas_capacity_m3 is None
        ),
        "queues_total": len(queues),
        "queues_open": sum(1 for q in queues if q.moored_ts is None),
        "queues_us_export": len(load_q),
        "queues_eu_import": len(disch_q),
    }
    # Carry-forward item 1: open-leg censoring exposure of the in-transit stock,
    # per regime (physical). A live (mmsi_filter) value far above the historical
    # noaa/gfw is the phantom-open-leg fingerprint, not a market move.
    of_by_regime: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        if (
            r.signal_key == "gas_in_transit_volume"
            and r.basis == BASIS_PHYSICAL
            and r.open_fraction is not None
        ):
            of_by_regime[r.regime].append(r.open_fraction)
    summary["transit_open_fraction"] = {
        reg: sum(v) / len(v) for reg, v in of_by_regime.items() if v
    }
    return rows, summary


async def load_signals(pool: asyncpg.Pool, rows: list[SignalRow]) -> None:
    """Atomic swap: TRUNCATE + bulk insert in one short transaction so readers
    never see a partial panel (mirrors port_events.py)."""
    payload = [
        (
            r.signal_key,
            r.bucket_date,
            r.zone_scope,
            r.regime,
            r.value,
            r.n_legs,
            r.value_dispersion,
            r.open_fraction,
            r.estimated_fraction,
            r.basis,
        )
        for r in rows
    ]
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(TRUNCATE_SQL)
            if payload:
                await conn.executemany(INSERT_SQL, payload)


async def snapshot_live_vintage(
    pool: asyncpg.Pool, rows: list[SignalRow], panel_end: date
) -> int:
    """Carry-forward item 2 (SIGNALS.md §0·7·1·4): append today's live-regime values
    to the append-only as-printed log, so a later `knowable[d]` recompute can be
    checked against what the pipeline actually emitted on d. Only the current panel
    day under the live regimes is captured (history has no real-time vintage). Read
    side dedups on the latest printed_at per (signal_key, bucket_date, …)."""
    payload = [
        (r.signal_key, r.bucket_date, r.zone_scope, r.regime, r.basis, r.value, r.n_legs)
        for r in rows
        if r.regime in LIVE_REGIMES and r.bucket_date == panel_end
    ]
    if not payload:
        return 0
    async with pool.acquire() as conn:
        await conn.executemany(VINTAGE_INSERT_SQL, payload)
    return len(payload)


async def run(
    pool: asyncpg.Pool,
    now: datetime | None = None,
    panel_start: date | None = None,
    *,
    snapshot_vintage: bool = False,
) -> None:
    t0 = time.monotonic()
    if now is None:
        now = datetime.now(UTC)
    rows, summary = await build_signals(pool, now, panel_start=panel_start)
    await load_signals(pool, rows)
    if snapshot_vintage:
        summary["vintage_rows"] = await snapshot_live_vintage(pool, rows, now.date())
    _log_summary(summary, time.monotonic() - t0)


def _log_summary(summary: dict, wall_seconds: float) -> None:
    logger.info("=" * 60)
    logger.info(
        "signal_daily rebuild complete (%.1fs wall, %d rows)",
        wall_seconds,
        summary["total_rows"],
    )
    logger.info("  panel: %s → %s", summary["panel_start"], summary["panel_end"])
    logger.info(
        "  basis rows: %s",
        ", ".join(f"{b}={n}" for b, n in sorted(summary["by_basis"].items())),
    )
    logger.info(
        "  in-transit legs: %d physical (open: %d, unknown-dest: %d) | "
        "%d knowable (+%d overdue counted pre-recognition)",
        summary["transit_legs"],
        summary["transit_open"],
        summary["transit_unknown_band"],
        summary["transit_legs_knowable"],
        summary["transit_overdue_knowable"],
    )
    logger.info("  ballast→US legs: %d", summary["ballast_legs"])
    logger.info(
        "  anchorage queues: %d (US export: %d, EU import: %d, open: %d)",
        summary["queues_total"],
        summary["queues_us_export"],
        summary["queues_eu_import"],
        summary["queues_open"],
    )
    logger.info(
        "  EU discharging visits: %d  (in berth now: %d)",
        summary["discharging_visits"],
        summary["discharging_open"],
    )
    logger.info(
        "  US loading visits: %d  (in berth now: %d)",
        summary["loading_visits"],
        summary["loading_open"],
    )
    if summary["null_gas_legs"] or summary["null_gas_visits"]:
        logger.info(
            "  NULL gas_capacity_m3 skipped — legs: %d, visits: %d",
            summary["null_gas_legs"],
            summary["null_gas_visits"],
        )
    if summary.get("transit_open_fraction"):
        logger.info(
            "  in-transit open_fraction (physical, censoring exposure): %s",
            ", ".join(
                f"{reg}={f:.2f}"
                for reg, f in sorted(summary["transit_open_fraction"].items())
            ),
        )
    if "vintage_rows" in summary:
        logger.info(
            "  live as-printed vintage: appended %d rows for %s",
            summary["vintage_rows"],
            summary["panel_end"],
        )
    logger.info("  rows by signal_key:")
    for key, n in sorted(summary["by_key"].items()):
        logger.info("    %-32s %d", key, n)
    logger.info(
        "  rows by regime: %s",
        ", ".join(f"{r}={n}" for r, n in sorted(summary["by_regime"].items())),
    )
    logger.info("=" * 60)


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rebuild the signal_daily panel from legs + visits "
        "(TRUNCATE + rebuild)."
    )
    parser.add_argument(
        "--as-of",
        type=parse_as_of,
        default=None,
        metavar="ISO8601",
        help="Pin the wall-clock reference (passed to compute_legs and used as the "
        "panel end) for a deterministic, reproducible rebuild. Defaults to now().",
    )
    parser.add_argument(
        "--panel-start",
        type=date.fromisoformat,
        default=None,
        metavar="YYYY-MM-DD",
        help="Earliest bucket_date. Defaults to the earliest live item.",
    )
    args = parser.parse_args()

    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=4)
    try:
        # A real-time rebuild (no --as-of) snapshots the live as-printed vintage; a
        # pinned historical replay does not (it isn't "what we printed live").
        await run(
            pool,
            now=args.as_of,
            panel_start=args.panel_start,
            snapshot_vintage=args.as_of is None,
        )
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
