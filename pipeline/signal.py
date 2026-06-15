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
  - basis='physical' only: one compute_legs(now=as_of) call; an item is live on
    day d iff its interval covers d, using today's classification (hindsight-
    clean, not leakage-free). The 'knowable' point-in-time series is deferred.
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
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Callable

import asyncpg
from rich.logging import RichHandler

from config import settings

from .legs import (
    CENSOR_OPEN_DAYS,
    FALLBACK_DEST_REGION,
    OD_WINDOW_DAYS,
    Leg,
    compute_legs,
)
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

INSERT_SQL = """
INSERT INTO signal_daily
    (signal_key, bucket_date, zone_scope, regime, value, n_legs, basis)
VALUES ($1, $2, $3, $4, $5, $6, $7)
"""


# ----------------------------------------------------------------------
# Output row + helpers
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class SignalRow:
    """One row of the signal_daily table."""

    signal_key: str
    bucket_date: date
    zone_scope: str
    regime: str
    value: float
    n_legs: int | None
    basis: str = BASIS_PHYSICAL


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
) -> list[SignalRow]:
    """Stacked per-day reconstruction. For each item, accumulate its
    `contribution` (None ⇒ skipped that day) into a (band, regime, day) cell over
    its live interval, then emit a sum or mean per cell. Each item is tagged by
    its own regime plus a synthetic 'all' regime, so a regime-segmented and a
    pooled series are both available. `band_of` is the stacked dimension written
    to zone_scope."""
    if not days:
        return []
    panel_start, panel_end = days[0], days[-1]
    last_excl = panel_end + timedelta(days=1)
    # (band, regime, date) -> [total, count]
    acc: dict[tuple[str, str, date], list[float]] = defaultdict(lambda: [0.0, 0.0])
    for item in items:
        start, end_excl = interval_of(item, panel_end)
        band = band_of(item)
        d = max(start, panel_start)
        hi = min(end_excl, last_excl)
        while d < hi:
            c = contribution(item, d)
            if c is not None:
                for regime in (item.regime, "all"):
                    cell = acc[(band, regime, d)]
                    cell[0] += c
                    cell[1] += 1
            d += timedelta(days=1)
    rows: list[SignalRow] = []
    for (band, regime, d), (total, count) in acc.items():
        value = total / count if aggregate == "mean" else total
        rows.append(SignalRow(signal_key, d, band, regime, value, int(count), basis))
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
    async with pool.acquire() as conn:
        term_rows = await conn.fetch(TERMINAL_METADATA_SQL)

    lane = build_lane_filter(term_rows)

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

    rows: list[SignalRow] = []
    for basis, p in plans.items():
        rows += accumulate_daily(
            loading,
            days,
            signal_key="gas_loading_us",
            interval_of=visit_berth_interval,
            band_of=visit_terminal_band,
            contribution=p["load_contrib"],
            basis=basis,
        )
        rows += accumulate_daily(
            discharging,
            days,
            signal_key="gas_discharging_eu",
            interval_of=visit_berth_interval,
            band_of=visit_terminal_band,
            contribution=p["disch_contrib"],
            basis=basis,
        )
        rows += accumulate_daily(
            p["transit"],
            days,
            signal_key="gas_in_transit_volume",
            interval_of=p["leg_interval"],
            band_of=lambda lg: transit_dest_band(lg, lane),
            basis=basis,
        )
        rows += accumulate_daily(
            p["ballast"],
            days,
            signal_key="gas_ballast_to_us",
            interval_of=p["leg_interval"],
            band_of=lambda lg: ballast_dest_band(lg, lane),
            basis=basis,
        )

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
            r.basis,
        )
        for r in rows
    ]
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(TRUNCATE_SQL)
            if payload:
                await conn.executemany(INSERT_SQL, payload)


async def run(
    pool: asyncpg.Pool,
    now: datetime | None = None,
    panel_start: date | None = None,
) -> None:
    t0 = time.monotonic()
    if now is None:
        now = datetime.now(UTC)
    rows, summary = await build_signals(pool, now, panel_start=panel_start)
    await load_signals(pool, rows)
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
        await run(pool, now=args.as_of, panel_start=args.panel_start)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
