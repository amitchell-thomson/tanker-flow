"""Market-signal aggregation layer.

Aggregates the classified voyage legs (`pipeline/legs.py`) and raw `port_events`
into a tidy/long daily panel (`signal_daily`), idempotently rebuilt like
`port_events` (TRUNCATE + executemany swap). This is the *aggregation* on top of
the leg foundation — it does NOT re-pair legs, it consumes `compute_legs()`.

First-build signals (see analysis/SIGNALS.md):
  - #1/#2  laden ton-miles in transit, US export → EU lane, dwt- and
           gas-capacity-weighted  (signal_key 'laden_ton_miles_in_transit_{dwt,gas}')
  - #20    mean laden-voyage age of *open* (not-yet-arrived) legs
           ('mean_laden_voyage_age_h')
  - #5     origin→destination flow matrix over laden closed legs ('od_flow_count')
  - #4     EU arrivals/day  — laden `moored` at import terminals ('eu_arrivals')
  - #9     US loadings/day  — laden `departed` from export terminals ('us_loadings')

Design decisions locked with the user:
  - Output is a long panel; the headline #1/#2 lives as a signal_key value, not a
    dedicated table.
  - basis='physical' only: one compute_legs(now=as_of) call; a leg is live on day
    d iff departed<=d<arrived, using today's classification. Hindsight-clean (good
    for EIA validation, NOT leakage-free for training). The 'knowable' point-in-time
    series is deferred (the basis column reserves the slot).
  - In-transit stock = closed legs (over [departed, arrived)) + open_in_transit
    legs (to as_of). same_zone / open_censored / open_arrival_gap / open_floating
    are excluded (open_floating is floating-storage inventory #19, a separate signal).
  - The lane is defined origin-side (US export terminals) because terrestrial AIS
    sees departures cleanly but loses mid-ocean destination broadcasts. Closed legs
    additionally require an EU import destination; open legs are origin-only and
    their leg distance is *estimated* departure→declared-destination-zone centroid
    (the observed great-circle endpoint distance only exists once a leg closes).
    An open leg whose destination was never declared (~90% of them) falls back to
    the dominant lane destination (FALLBACK_DEST_ZONE = NW Europe), so outstanding
    cargo isn't dropped from the recent in-transit tail; the summary logs the count.

Pure aggregation functions + a thin DB loader, mirroring legs.py / state_machine.py.

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

from .geo import haversine_nm
from .legs import Leg, compute_legs


logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])
logger = logging.getLogger(__name__)


BASIS_PHYSICAL = "physical"

# Fallback destination for an open export-origin leg whose declared destination we
# never captured (terrestrial AIS loses the mid-ocean dest broadcast — ~90% of open
# legs today). NW Europe is the dominant US LNG lane, so we assume it and estimate
# the leg distance from the *actual* departure point to that zone's centroid rather
# than dropping the leg (which would understate the recent in-transit tail). The
# assumption is endpoint-only — US→Asia leakage is misattributed here and isolated
# separately by the O-D matrix (#5). The summary logs how many legs used it.
FALLBACK_DEST_ZONE = "nweurope"


# ----------------------------------------------------------------------
# SQL
# ----------------------------------------------------------------------

TRUNCATE_SQL = "TRUNCATE signal_daily RESTART IDENTITY"

TERMINAL_METADATA_SQL = (
    "SELECT terminal_id, zone, flow_direction FROM terminals WHERE zone IS NOT NULL"
)

# One representative coordinate per import zone — the centroid of all that zone's
# import-terminal polygons. Used to *estimate* an open leg's great-circle distance
# (origin → declared destination region) since open legs have no observed arrival.
IMPORT_ZONE_CENTROIDS_SQL = """
SELECT t.zone AS zone,
       ST_Y(ST_Centroid(ST_Collect(tz.geom))) AS lat,
       ST_X(ST_Centroid(ST_Collect(tz.geom))) AS lon
FROM terminal_zones tz
JOIN terminals t ON t.terminal_id = tz.terminal_id
WHERE t.flow_direction = 'import'
GROUP BY t.zone
"""

# Raw events for the count flows (#4 arrivals, #9 loadings). regime is the stored
# generated column on port_events.
COUNT_EVENTS_SQL = """
SELECT mmsi, event_type, event_time, zone, terminal_id, laden_flag, regime
FROM port_events
WHERE event_type IN ('moored', 'departed')
"""

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


@dataclass(frozen=True)
class EventCount:
    """Minimal per-event view for the count flows (#4/#9)."""

    mmsi: int
    event_type: str
    event_time: datetime
    zone: str
    terminal_id: int | None
    laden_flag: bool | None
    flow_direction: str | None
    regime: str


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
# Pure aggregation — in-transit stock (#1/#2/#20)
# ----------------------------------------------------------------------


def lane_legs(legs: list[Leg], lane: LaneFilter) -> list[Leg]:
    """The in-transit base: laden closed (export→import) + laden open_in_transit
    (export-origin). Excludes same_zone / open_censored / open_arrival_gap /
    open_floating per the locked decision."""
    out: list[Leg] = []
    for leg in legs:
        if leg.laden is not True or not lane.is_export(leg.origin_zone):
            continue
        if leg.status == "closed" and lane.is_import(leg.dest_zone):
            out.append(leg)
        elif leg.status == "open_in_transit":
            out.append(leg)
    return out


def _live_interval(leg: Leg, panel_end: date) -> tuple[date, date]:
    """Half-open [start, end) of dates a leg is in transit. Closed legs end at
    the arrival day (not counted on/after arrival); open legs run through panel_end."""
    start = leg.departed_ts.date()
    if leg.status == "closed" and leg.arrived_ts is not None:
        end_excl = leg.arrived_ts.date()
    else:
        end_excl = panel_end + timedelta(days=1)
    return start, end_excl


def leg_distance_nm(
    leg: Leg,
    import_centroids: dict[str, tuple[float, float]],
    *,
    fallback_zone: str | None = FALLBACK_DEST_ZONE,
) -> float | None:
    """Great-circle leg distance. Closed legs use the observed endpoint distance
    already on the leg; open legs estimate departure→declared-destination-zone
    centroid (no arrival observed yet), falling back to `fallback_zone`'s centroid
    when the destination was never declared (set `fallback_zone=None` to disable).
    None when it still can't be determined (no departure position / no centroid)."""
    if leg.status == "closed":
        return leg.distance_nm
    if leg.departed_lat is None or leg.departed_lon is None:
        return None
    centroid = import_centroids.get(leg.dest_region or "")
    if centroid is None and fallback_zone is not None:
        centroid = import_centroids.get(fallback_zone)
    if centroid is None:
        return None
    return haversine_nm(leg.departed_lat, leg.departed_lon, centroid[0], centroid[1])


def legs_live_on(legs: list[Leg], target: date, lane: LaneFilter) -> list[Leg]:
    """The in-transit-base legs that are live on `target` — i.e. exactly the legs
    the ton-mile / voyage-age reconstruction counts on that day (`departed ≤ target
    < arrived`, open legs run through `target`). Used by the viz drill-down so the
    contributor list can never disagree with the charted value."""
    out: list[Leg] = []
    for leg in lane_legs(legs, lane):
        start, end_excl = _live_interval(leg, target)
        if start <= target < end_excl:
            out.append(leg)
    return out


def _accumulate_daily(
    legs: list[Leg],
    days: list[date],
    *,
    signal_key: str,
    zone_scope: str,
    contribution: Callable[[Leg, date], float | None],
    aggregate: str,  # 'sum' | 'mean'
    basis: str = BASIS_PHYSICAL,
) -> list[SignalRow]:
    """Per-day reconstruction over the legs' live intervals. For each (regime, day)
    accumulate the per-leg `contribution` (None ⇒ leg skipped that day), then emit
    a sum or mean. Tagged by the leg's own regime plus a synthetic 'all' row."""
    if not days:
        return []
    panel_start, panel_end = days[0], days[-1]
    last_excl = panel_end + timedelta(days=1)
    # (regime, date) -> [total, count]
    acc: dict[tuple[str, date], list[float]] = defaultdict(lambda: [0.0, 0.0])
    for leg in legs:
        start, end_excl = _live_interval(leg, panel_end)
        d = max(start, panel_start)
        hi = min(end_excl, last_excl)
        while d < hi:
            c = contribution(leg, d)
            if c is not None:
                for regime in (leg.regime, "all"):
                    cell = acc[(regime, d)]
                    cell[0] += c
                    cell[1] += 1
            d += timedelta(days=1)
    rows: list[SignalRow] = []
    for (regime, d), (total, count) in acc.items():
        value = total / count if aggregate == "mean" else total
        rows.append(
            SignalRow(signal_key, d, zone_scope, regime, value, int(count), basis)
        )
    return rows


def reconstruct_ton_miles(
    legs: list[Leg],
    days: list[date],
    *,
    weight_attr: str,  # 'dwt' | 'gas_capacity_m3'
    signal_key: str,
    import_centroids: dict[str, tuple[float, float]],
    zone_scope: str = "usgulf->eu",
    fallback_zone: str | None = FALLBACK_DEST_ZONE,
    basis: str = BASIS_PHYSICAL,
) -> list[SignalRow]:
    """#1/#2 — laden ton-miles in transit = Σ capacity × leg distance over legs
    live on each day. Open legs with no declared destination fall back to
    `fallback_zone` (see leg_distance_nm). Legs with a NULL capacity or
    undeterminable distance are skipped (not counted in n_legs)."""

    def contribution(leg: Leg, _d: date) -> float | None:
        cap = getattr(leg, weight_attr)
        if cap is None:
            return None
        dist = leg_distance_nm(leg, import_centroids, fallback_zone=fallback_zone)
        if dist is None:
            return None
        return cap * dist

    return _accumulate_daily(
        legs,
        days,
        signal_key=signal_key,
        zone_scope=zone_scope,
        contribution=contribution,
        aggregate="sum",
        basis=basis,
    )


def reconstruct_voyage_age(
    legs: list[Leg],
    days: list[date],
    *,
    zone_scope: str = "usgulf->eu",
    basis: str = BASIS_PHYSICAL,
) -> list[SignalRow]:
    """#20 — mean age (days-since-departed, in hours) of *open* in-transit legs on
    each day. The best floating-storage proxy under terrestrial AIS."""
    open_legs = [lg for lg in legs if lg.status == "open_in_transit"]

    def contribution(leg: Leg, d: date) -> float | None:
        return (d - leg.departed_ts.date()).days * 24.0

    return _accumulate_daily(
        open_legs,
        days,
        signal_key="mean_laden_voyage_age_h",
        zone_scope=zone_scope,
        contribution=contribution,
        aggregate="mean",
        basis=basis,
    )


# ----------------------------------------------------------------------
# Pure aggregation — O-D matrix (#5) and event-count flows (#4/#9)
# ----------------------------------------------------------------------


def od_matrix(legs: list[Leg], *, basis: str = BASIS_PHYSICAL) -> list[SignalRow]:
    """#5 — origin→destination flow counts over laden *closed* legs, bucketed by
    departure day (the leading edge of the flow). Includes every O-D pair (the
    point is to isolate the US→EU lane vs US→Asia leakage), so no lane filter."""
    counts: dict[tuple[str, str, date], int] = defaultdict(int)
    for leg in legs:
        if leg.status != "closed" or leg.laden is not True or leg.dest_zone is None:
            continue
        scope = f"{leg.origin_zone}->{leg.dest_zone}"
        d = leg.departed_ts.date()
        for regime in (leg.regime, "all"):
            counts[(scope, regime, d)] += 1
    return [
        SignalRow("od_flow_count", d, scope, regime, float(n), n, basis)
        for (scope, regime, d), n in counts.items()
    ]


def count_events_daily(
    events: list[EventCount],
    *,
    signal_key: str,
    event_type: str,
    flow_direction: str,
    zone_scope: str,
    basis: str = BASIS_PHYSICAL,
) -> list[SignalRow]:
    """#4/#9 — daily count of laden `event_type` events at terminals of the given
    flow_direction (moored@import = arrivals; departed@export = loadings)."""
    counts: dict[tuple[str, date], int] = defaultdict(int)
    for e in events:
        if (
            e.event_type != event_type
            or e.laden_flag is not True
            or e.flow_direction != flow_direction
        ):
            continue
        d = e.event_time.date()
        for regime in (e.regime, "all"):
            counts[(regime, d)] += 1
    return [
        SignalRow(signal_key, d, zone_scope, regime, float(n), n, basis)
        for (regime, d), n in counts.items()
    ]


# ----------------------------------------------------------------------
# DB orchestration
# ----------------------------------------------------------------------


async def build_signals(
    pool: asyncpg.Pool, now: datetime, *, panel_start: date | None
) -> tuple[list[SignalRow], dict]:
    """Load the leg base + terminal metadata + count events, run every pure
    aggregator, and return all signal rows (+ a summary for logging)."""
    legs = await compute_legs(pool, now, enrich=True)
    async with pool.acquire() as conn:
        term_rows = await conn.fetch(TERMINAL_METADATA_SQL)
        centroid_rows = await conn.fetch(IMPORT_ZONE_CENTROIDS_SQL)
        ev_rows = await conn.fetch(COUNT_EVENTS_SQL)

    lane = build_lane_filter(term_rows)
    fd_by_terminal = {r["terminal_id"]: r["flow_direction"] for r in term_rows}
    import_centroids = {
        r["zone"]: (r["lat"], r["lon"])
        for r in centroid_rows
        if r["lat"] is not None and r["lon"] is not None
    }
    events = [
        EventCount(
            mmsi=r["mmsi"],
            event_type=r["event_type"],
            event_time=r["event_time"],
            zone=r["zone"],
            terminal_id=r["terminal_id"],
            laden_flag=r["laden_flag"],
            flow_direction=fd_by_terminal.get(r["terminal_id"]),
            regime=r["regime"],
        )
        for r in ev_rows
    ]

    base = lane_legs(legs, lane)
    panel_end = now.date()
    if panel_start is None:
        dep_dates = [lg.departed_ts.date() for lg in base]
        panel_start = min(dep_dates) if dep_dates else panel_end
    days = daily_buckets(panel_start, panel_end)

    rows: list[SignalRow] = []
    rows += reconstruct_ton_miles(
        base,
        days,
        weight_attr="dwt",
        signal_key="laden_ton_miles_in_transit_dwt",
        import_centroids=import_centroids,
    )
    rows += reconstruct_ton_miles(
        base,
        days,
        weight_attr="gas_capacity_m3",
        signal_key="laden_ton_miles_in_transit_gas",
        import_centroids=import_centroids,
    )
    rows += reconstruct_voyage_age(base, days)
    rows += od_matrix(legs)
    rows += count_events_daily(
        events,
        signal_key="eu_arrivals",
        event_type="moored",
        flow_direction="import",
        zone_scope="eu",
    )
    rows += count_events_daily(
        events,
        signal_key="us_loadings",
        event_type="departed",
        flow_direction="export",
        zone_scope="us",
    )

    open_base = [lg for lg in base if lg.status == "open_in_transit"]
    # An open leg used the fallback iff it has no resolvable declared-dest centroid
    # but a fallback distance is available (declared-dest distance would be None
    # with fallback disabled, yet non-None with it enabled).
    open_fallback = sum(
        1
        for lg in open_base
        if leg_distance_nm(lg, import_centroids, fallback_zone=None) is None
        and leg_distance_nm(lg, import_centroids) is not None
    )
    summary = {
        "total_rows": len(rows),
        "by_key": Counter(r.signal_key for r in rows),
        "by_regime": Counter(r.regime for r in rows),
        "panel_start": panel_start,
        "panel_end": panel_end,
        "lane_legs": len(base),
        "open_legs": len(open_base),
        "open_fallback_dest": open_fallback,
        "fallback_zone": FALLBACK_DEST_ZONE,
        "open_no_dist_estimate": sum(
            1 for lg in open_base if leg_distance_nm(lg, import_centroids) is None
        ),
        "null_gas": sum(1 for lg in base if lg.gas_capacity_m3 is None),
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
        "  in-transit base legs: %d  (open: %d)",
        summary["lane_legs"],
        summary["open_legs"],
    )
    if summary["open_fallback_dest"]:
        logger.info(
            "  open legs using %s fallback destination (no declared dest): %d",
            summary["fallback_zone"],
            summary["open_fallback_dest"],
        )
    if summary["open_no_dist_estimate"]:
        logger.info(
            "  open legs still skipped from ton-miles (no departure position): %d",
            summary["open_no_dist_estimate"],
        )
    if summary["null_gas"]:
        logger.info(
            "  base legs with NULL gas_capacity_m3 (skipped from #2): %d",
            summary["null_gas"],
        )
    logger.info("  rows by signal_key:")
    for key, n in sorted(summary["by_key"].items()):
        logger.info("    %-32s %d", key, n)
    logger.info(
        "  rows by regime: %s",
        ", ".join(f"{r}={n}" for r, n in sorted(summary["by_regime"].items())),
    )
    logger.info("=" * 60)


def _parse_as_of(raw: str) -> datetime:
    raw = raw.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rebuild the signal_daily panel from legs + port_events "
        "(TRUNCATE + rebuild)."
    )
    parser.add_argument(
        "--as-of",
        type=_parse_as_of,
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
        help="Earliest bucket_date. Defaults to the earliest in-transit departure.",
    )
    args = parser.parse_args()

    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=4)
    try:
        await run(pool, now=args.as_of, panel_start=args.panel_start)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
