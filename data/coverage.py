"""Coverage panel: how much of the in-scope LNG fleet are we actually hearing?

The interim measurement for the data-quality program (analysis/DATA_QUALITY.md
§1, §4): a read-only rollup of the coverage buckets so the residual miss-rate
stops being *unmeasured* while the EIA capture-rate metric (data/capture_rate.py)
lands dark until ~late summer 2026. This is the day-to-day "are we closing the
gap?" proxy; capture-rate is the eventual exogenous arbiter.

It surfaces four things, all from data that already exists (no new tables):

  1. Fleet coverage — every in-scope carrier (is_lng_carrier OR is_fsru, NOT
     excluded) bucketed by the recency of its last ais_fix: live (<2d) /
     stale (2-7d) / blind (7-90d) / unseen (no fix in 90d). This is the §1 table.
  2. Watchlist tiers — the priority_watchlist tier split and, per tier, how many
     are actually subscribed right now (in_slot) — the gap between "want to watch"
     and "can watch" with ~150 slots.
  3. Cold-start rate — of recent `moored` events, the fraction flagged cold_start
     (we first saw the vessel already berthed → missed its approach). The §4
     "appears in berth, arrival missed" symptom; trend toward zero = better.
  4. Unmet rescue demand — distinct MMSIs the VF budget logged as skipped_budget
     (today / last 7d): rescue candidates the glide budget couldn't serve.

READ-ONLY (no writes). Pure bucket logic + a thin DB loader, mirroring
data/capture_rate.py. Usage: `uv run python -m data.coverage` (or `make coverage`).
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import asyncpg

from config import settings

# Recency thresholds for the fleet-coverage buckets (DATA_QUALITY §1).
LIVE_MAX_DAYS = 2  # heard live: an AISstream/VF fix within 2 days
STALE_MAX_DAYS = 7  # AIS-stale: 2-7 days
BLIND_LOOKBACK_DAYS = 90  # bound the scan; older-than-this reads as "unseen"
# Window for the cold-start rate (the §4 appear-in-berth symptom).
COLDSTART_WINDOW_DAYS = 14

BUCKETS = ("live", "stale", "blind", "unseen")


def classify_recency(last_fix_ts: datetime | None, now: datetime) -> str:
    """Bucket a vessel by the age of its last fix: live / stale / blind / unseen.
    None (no fix within the lookback) is 'unseen'."""
    if last_fix_ts is None:
        return "unseen"
    age = now - last_fix_ts
    if age <= timedelta(days=LIVE_MAX_DAYS):
        return "live"
    if age <= timedelta(days=STALE_MAX_DAYS):
        return "stale"
    return "blind"


@dataclass(frozen=True)
class TierRow:
    tier: int
    n: int
    in_slot: int


@dataclass(frozen=True)
class CoverageSummary:
    buckets: dict[str, int]  # bucket -> count, keyed by BUCKETS
    fleet_total: int
    tiers: list[TierRow]
    moored_recent: int
    cold_starts: int
    unmet_today: int
    unmet_week: int

    @property
    def cold_start_rate(self) -> float | None:
        if self.moored_recent == 0:
            return None
        return self.cold_starts / self.moored_recent

    @property
    def heard_rate(self) -> float | None:
        """Fraction of the fleet heard within the last STALE_MAX_DAYS (live+stale)."""
        if self.fleet_total == 0:
            return None
        return (self.buckets["live"] + self.buckets["stale"]) / self.fleet_total

    @property
    def in_slot_total(self) -> int:
        return sum(t.in_slot for t in self.tiers)


FLEET_COVERAGE_SQL = f"""
WITH fleet AS (
    SELECT mmsi FROM vessel_registry
    WHERE (is_lng_carrier OR is_fsru) AND NOT excluded
),
last_fix AS (
    SELECT a.mmsi, max(a.fix_ts) AS last_fix_ts
    FROM ais_fixes a
    WHERE EXISTS (SELECT 1 FROM fleet f WHERE f.mmsi = a.mmsi)
      AND a.fix_ts > now() - make_interval(days => {BLIND_LOOKBACK_DAYS})
    GROUP BY a.mmsi
)
SELECT f.mmsi, lf.last_fix_ts
FROM fleet f
LEFT JOIN last_fix lf USING (mmsi)
"""

TIER_SQL = """
SELECT tier,
       count(*)                       AS n,
       count(*) FILTER (WHERE in_slot) AS in_slot
FROM priority_watchlist
GROUP BY tier
ORDER BY tier
"""

COLDSTART_SQL = f"""
SELECT count(*)                          AS moored,
       count(*) FILTER (WHERE cold_start) AS cold
FROM port_events
WHERE event_type = 'moored'
  AND event_time > now() - make_interval(days => {COLDSTART_WINDOW_DAYS})
"""

UNMET_SQL = """
SELECT
  count(DISTINCT mmsi) FILTER (
      WHERE requested_at >= date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
  ) AS today,
  count(DISTINCT mmsi) FILTER (WHERE requested_at > now() - interval '7 days') AS week
FROM vf_rescue_log
WHERE result = 'skipped_budget'
"""


def build_coverage(
    fleet_rows: list[asyncpg.Record],
    tier_rows: list[asyncpg.Record],
    coldstart_row: asyncpg.Record | None,
    unmet_row: asyncpg.Record | None,
    now: datetime,
) -> CoverageSummary:
    """Pure: assemble the coverage summary from raw records. DB-free so the bucket
    math is unit-testable."""
    buckets = {b: 0 for b in BUCKETS}
    for r in fleet_rows:
        buckets[classify_recency(r["last_fix_ts"], now)] += 1
    tiers = [
        TierRow(tier=r["tier"], n=r["n"], in_slot=r["in_slot"]) for r in tier_rows
    ]
    moored = int(coldstart_row["moored"]) if coldstart_row else 0
    cold = int(coldstart_row["cold"]) if coldstart_row else 0
    unmet_today = int(unmet_row["today"]) if unmet_row and unmet_row["today"] else 0
    unmet_week = int(unmet_row["week"]) if unmet_row and unmet_row["week"] else 0
    return CoverageSummary(
        buckets=buckets,
        fleet_total=len(fleet_rows),
        tiers=tiers,
        moored_recent=moored,
        cold_starts=cold,
        unmet_today=unmet_today,
        unmet_week=unmet_week,
    )


def _fmt_pct(x: float | None) -> str:
    return "—" if x is None else f"{x * 100:.1f}%"


def _pct_of(n: int, total: int) -> str:
    return "—" if total == 0 else f"{n / total * 100:.0f}%"


def render(s: CoverageSummary, now: datetime) -> str:
    lines: list[str] = []
    lines.append(f"LNG fleet coverage  ({now:%Y-%m-%d %H:%M} UTC)")
    lines.append("")

    lines.append(f"  Fleet coverage (in-scope carriers, n={s.fleet_total}):")
    labels = {
        "live": f"heard live (<{LIVE_MAX_DAYS}d)",
        "stale": f"AIS-stale ({LIVE_MAX_DAYS}-{STALE_MAX_DAYS}d)",
        "blind": f"blind ({STALE_MAX_DAYS}-{BLIND_LOOKBACK_DAYS}d)",
        "unseen": f"unseen (>{BLIND_LOOKBACK_DAYS}d)",
    }
    for b in BUCKETS:
        n = s.buckets[b]
        lines.append(
            f"    {labels[b]:<22} {n:>4}  {_pct_of(n, s.fleet_total):>4}"
        )
    lines.append(f"    {'heard within ' + str(STALE_MAX_DAYS) + 'd':<22} "
                 f"{s.buckets['live'] + s.buckets['stale']:>4}  {_fmt_pct(s.heard_rate):>4}")
    lines.append("")

    lines.append("  Watchlist tiers (n / subscribed now):")
    for t in s.tiers:
        lines.append(f"    tier {t.tier}   {t.n:>4}  in_slot {t.in_slot:>3}")
    lines.append(f"    subscribed total: {s.in_slot_total}")
    lines.append("")

    lines.append(
        f"  Cold-start moorings (last {COLDSTART_WINDOW_DAYS}d): "
        f"{s.cold_starts}/{s.moored_recent}  rate {_fmt_pct(s.cold_start_rate)}"
    )
    lines.append(
        f"  Unmet rescue demand (skipped_budget MMSIs): "
        f"today {s.unmet_today} · last 7d {s.unmet_week}"
    )
    return "\n".join(lines)


async def compute(pool: asyncpg.Pool, *, now: datetime) -> CoverageSummary:
    async with pool.acquire() as conn:
        fleet = await conn.fetch(FLEET_COVERAGE_SQL)
        tiers = await conn.fetch(TIER_SQL)
        coldstart = await conn.fetchrow(COLDSTART_SQL)
        unmet = await conn.fetchrow(UNMET_SQL)
    return build_coverage(fleet, tiers, coldstart, unmet, now)


async def run(now: datetime) -> None:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=2)
    try:
        summary = await compute(pool, now=now)
    finally:
        await pool.close()
    print(render(summary, now))


def _parse_as_of(raw: str) -> datetime:
    dt = datetime.fromisoformat(raw)
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="LNG fleet coverage report (live/stale/blind buckets + tiers)."
    )
    parser.add_argument(
        "--as-of",
        type=_parse_as_of,
        default=None,
        metavar="ISO8601",
        help="Pin 'now' (controls the recency buckets). Defaults to the current time.",
    )
    args = parser.parse_args()
    asyncio.run(run(args.as_of or datetime.now(UTC)))


if __name__ == "__main__":
    main()
