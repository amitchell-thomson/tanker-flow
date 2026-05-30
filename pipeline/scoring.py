"""Tier scoring for the AISstream MMSI watchlist.

Every LNG/FSRU vessel in vessel_registry is ranked into a tier based on how
relevant it is to our signal *right now*. The result lands in
priority_watchlist, which `ingestion/aisstream.py` then reads to pick the 150
MMSIs to subscribe to (100 persistent + 50 scan).

Tier definitions (also documented in CLAUDE.md):

    1 — recent fix inside any terminal_zones polygon (within 3d) — must be
        plausibly *currently* in zone, not "was there a week ago and is now
        mid-Atlantic"
    2 — vessel_state.dest parses to a known terminal, state_ts < 14d old
    3 — recent fix inside any config.ZONES rectangle (within 14d), not 1/2
    4 — any fix in the last 7d (not 1-3) — recently active globally
    5 — fix in 7-90d OR no fix at all (everything else)

Tiers 1-3 fill the persistent slot pool (top 100 by score). Tiers 4-5 fill the
scan rotation pool (50 oldest first).

Run via `make scoring` or as a background task inside `aisstream.py`. Sub-second
runtime; idempotent (full UPSERT each pass).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import asyncpg
from rich.logging import RichHandler

from config import ZONES, settings

from .dest_parser import parse_destination

logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])
logger = logging.getLogger(__name__)


# ZONES is [(name, lat_min, lat_max, lon_min, lon_max), ...]. Compose a SQL
# boolean for "fix is inside any zone rectangle." Done in code so config.py
# stays the single source of truth.
def _bbox_predicate(lat_col: str = "lat", lon_col: str = "lon") -> str:
    clauses = []
    for _, lat_min, lat_max, lon_min, lon_max in ZONES:
        clauses.append(
            f"({lat_col} BETWEEN {lat_min} AND {lat_max} AND "
            f"{lon_col} BETWEEN {lon_min} AND {lon_max})"
        )
    return "(" + " OR ".join(clauses) + ")"


# Single query that fetches everything the Python tier-assignment needs in one
# round trip. The polygon EXISTS subquery is the spatially expensive piece;
# limiting `candidate_fixes` to LNG vessels in the last 14d keeps it bounded.
SCORING_SQL = f"""
WITH lng_fleet AS (
    SELECT mmsi
    FROM vessel_registry
    WHERE (is_lng_carrier OR is_fsru) AND NOT excluded
),
candidate_fixes AS (
    SELECT a.mmsi, a.fix_ts, a.lat, a.lon
    FROM ais_fixes a
    WHERE a.fix_ts > now() - INTERVAL '14 days'
      AND EXISTS (SELECT 1 FROM lng_fleet l WHERE l.mmsi = a.mmsi)
),
zone_classified AS (
    -- Per-fix most-specific polygon containment: berth wins over anchorage
    -- wins over approach. NULL means not in any terminal_zones polygon.
    SELECT
        cf.mmsi,
        cf.fix_ts,
        (
            SELECT MIN(
                CASE tz.zone_type
                    WHEN 'berth'     THEN 1
                    WHEN 'anchorage' THEN 2
                    WHEN 'approach'  THEN 3
                END
            )
            FROM terminal_zones tz
            WHERE ST_Within(ST_SetSRID(ST_Point(cf.lon, cf.lat), 4326), tz.geom)
        ) AS specificity,
        {_bbox_predicate("cf.lat", "cf.lon")} AS in_bbox
    FROM candidate_fixes cf
),
fix_stats AS (
    SELECT
        mmsi,
        MAX(fix_ts) FILTER (WHERE specificity = 1) AS last_berth_fix_ts,
        MAX(fix_ts) FILTER (WHERE specificity = 2) AS last_anchorage_fix_ts,
        MAX(fix_ts) FILTER (WHERE specificity = 3) AS last_approach_fix_ts,
        MAX(fix_ts) FILTER (WHERE specificity IS NOT NULL) AS last_polygon_fix_ts,
        MAX(fix_ts) FILTER (WHERE in_bbox)                 AS last_bbox_fix_ts
    FROM zone_classified
    GROUP BY mmsi
),
last_fix_global AS (
    SELECT a.mmsi, MAX(a.fix_ts) AS last_fix_ts
    FROM ais_fixes a
    WHERE EXISTS (SELECT 1 FROM lng_fleet l WHERE l.mmsi = a.mmsi)
    GROUP BY a.mmsi
),
latest_state AS (
    SELECT DISTINCT ON (vs.mmsi) vs.mmsi, vs.dest, vs.state_ts
    FROM vessel_state vs
    WHERE vs.state_ts > now() - INTERVAL '90 days'
      AND EXISTS (SELECT 1 FROM lng_fleet l WHERE l.mmsi = vs.mmsi)
    ORDER BY vs.mmsi, vs.state_ts DESC
)
SELECT
    f.mmsi,
    lfg.last_fix_ts,
    fs.last_berth_fix_ts,
    fs.last_anchorage_fix_ts,
    fs.last_approach_fix_ts,
    fs.last_polygon_fix_ts,
    fs.last_bbox_fix_ts,
    ls.dest,
    ls.state_ts
FROM lng_fleet f
LEFT JOIN last_fix_global lfg USING (mmsi)
LEFT JOIN fix_stats fs USING (mmsi)
LEFT JOIN latest_state ls USING (mmsi)
"""


async def load_unlocode_map(conn: asyncpg.Connection) -> dict[str, int]:
    rows = await conn.fetch(
        "SELECT unlocode, terminal_id FROM terminals WHERE unlocode IS NOT NULL"
    )
    return {r["unlocode"]: r["terminal_id"] for r in rows}


# Within-tier-1 score bonus by zone_type. Berth fixes mean the vessel is
# actively at a terminal — the highest-signal state for `moored`/`departed`
# event timing. Bonuses are in seconds so they're directly comparable with
# epoch timestamps. ~3h spread means a berth fix counts as 3h "fresher" than
# its actual time for ordering — enough to break ties at the cull boundary
# without overriding genuinely-recent fixes from less-specific polygons.
_ZONE_TYPE_BONUS_S: dict[int, int] = {1: 3 * 3600, 2: 2 * 3600, 3: 1 * 3600}
_ZONE_TYPE_LABEL: dict[int, str] = {1: "berth", 2: "anchorage", 3: "approach"}


def _tier1_score(
    last_berth_fix_ts: datetime | None,
    last_anchorage_fix_ts: datetime | None,
    last_approach_fix_ts: datetime | None,
) -> tuple[float, datetime, str]:
    """Pick the most-recent + most-specific polygon fix as the tier-1 score.

    Returns (score_value, score_timestamp, polygon_label). Caller has already
    verified at least one of the inputs is non-null and within the 3-day window.
    """
    candidates: list[tuple[float, datetime, str]] = []
    for spec, ts in (
        (1, last_berth_fix_ts),
        (2, last_anchorage_fix_ts),
        (3, last_approach_fix_ts),
    ):
        if ts is None:
            continue
        score = ts.timestamp() + _ZONE_TYPE_BONUS_S[spec]
        candidates.append((score, ts, _ZONE_TYPE_LABEL[spec]))
    return max(candidates, key=lambda c: c[0])


def assign_tier(
    *,
    last_berth_fix_ts: datetime | None,
    last_anchorage_fix_ts: datetime | None,
    last_approach_fix_ts: datetime | None,
    last_polygon_fix_ts: datetime | None,
    last_bbox_fix_ts: datetime | None,
    last_fix_ts: datetime | None,
    dest_terminal_id: int | None,
    state_ts: datetime | None,
    now: datetime,
) -> tuple[int, str, float]:
    """Return (tier, reason, score_value) for a single vessel.

    score_value is the float written to priority_watchlist.score and used for
    within-tier ordering. For tiers 1-4, higher = more relevant. For tier 5
    the caller uses ASC ordering on `last_scan_window_at` for rotation
    instead, so the score there is just the epoch timestamp.
    """
    three_days = now - timedelta(days=3)
    fourteen_days = now - timedelta(days=14)
    seven_days = now - timedelta(days=7)
    ninety_days = now - timedelta(days=90)

    if last_polygon_fix_ts and last_polygon_fix_ts > three_days:
        score, ts, label = _tier1_score(
            last_berth_fix_ts, last_anchorage_fix_ts, last_approach_fix_ts
        )
        return (1, f"in-zone:{label} @ {ts:%Y-%m-%d}", score)

    if dest_terminal_id and state_ts and state_ts > fourteen_days:
        return (
            2,
            f"dest:terminal_id={dest_terminal_id} @ {state_ts:%Y-%m-%d}",
            state_ts.timestamp(),
        )

    if last_bbox_fix_ts and last_bbox_fix_ts > fourteen_days:
        return (3, f"in-zone:bbox @ {last_bbox_fix_ts:%Y-%m-%d}", last_bbox_fix_ts.timestamp())

    if last_fix_ts and last_fix_ts > seven_days:
        return (4, f"recent-anywhere @ {last_fix_ts:%Y-%m-%d}", last_fix_ts.timestamp())

    if last_fix_ts and last_fix_ts > ninety_days:
        return (5, f"stale @ {last_fix_ts:%Y-%m-%d}", last_fix_ts.timestamp())

    return (5, "never-seen", last_fix_ts.timestamp() if last_fix_ts else 0.0)


UPSERT_SQL = """
INSERT INTO priority_watchlist (
    mmsi, tier, score, score_reason,
    last_fix_ts, last_zone_fix_ts,
    parsed_dest_terminal_id, parsed_eta,
    in_slot, slot_kind, computed_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, FALSE, NULL, now())
ON CONFLICT (mmsi) DO UPDATE SET
    tier                    = EXCLUDED.tier,
    score                   = EXCLUDED.score,
    score_reason            = EXCLUDED.score_reason,
    last_fix_ts             = EXCLUDED.last_fix_ts,
    last_zone_fix_ts        = EXCLUDED.last_zone_fix_ts,
    parsed_dest_terminal_id = EXCLUDED.parsed_dest_terminal_id,
    computed_at             = now()
"""


async def compute_and_upsert(pool: asyncpg.Pool) -> dict[int, int]:
    """Run the scoring pass. Returns {tier: count} for logging/observability."""
    now = datetime.now(timezone.utc)
    tier_counts: dict[int, int] = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}

    async with pool.acquire() as conn:
        unlocodes = await load_unlocode_map(conn)
        logger.info(f"Loaded {len(unlocodes)} terminal unlocodes")

        rows = await conn.fetch(SCORING_SQL)
        logger.info(f"Scoring {len(rows)} LNG/FSRU vessels")

        # Also remove rows from priority_watchlist where the vessel is no
        # longer in scope (e.g. excluded=TRUE post-import). The set of valid
        # mmsis is exactly `rows`.
        valid_mmsis = {r["mmsi"] for r in rows}

        async with conn.transaction():
            for r in rows:
                dest_terminal_id, _is_for_orders = parse_destination(r["dest"], unlocodes)
                # last_zone_fix_ts in the table = polygon fix if present, else
                # the wider bbox fix; readers don't need to know which.
                last_zone_fix_ts = r["last_polygon_fix_ts"] or r["last_bbox_fix_ts"]

                tier, reason, score = assign_tier(
                    last_berth_fix_ts=r["last_berth_fix_ts"],
                    last_anchorage_fix_ts=r["last_anchorage_fix_ts"],
                    last_approach_fix_ts=r["last_approach_fix_ts"],
                    last_polygon_fix_ts=r["last_polygon_fix_ts"],
                    last_bbox_fix_ts=r["last_bbox_fix_ts"],
                    last_fix_ts=r["last_fix_ts"],
                    dest_terminal_id=dest_terminal_id,
                    state_ts=r["state_ts"],
                    now=now,
                )
                tier_counts[tier] += 1

                await conn.execute(
                    UPSERT_SQL,
                    r["mmsi"],
                    tier,
                    score,
                    reason,
                    r["last_fix_ts"],
                    last_zone_fix_ts,
                    dest_terminal_id,
                )

            # Sweep stale priority_watchlist rows (vessel no longer in scope).
            removed = await conn.execute(
                "DELETE FROM priority_watchlist WHERE mmsi <> ALL($1::BIGINT[])",
                list(valid_mmsis),
            )
            if removed and not removed.endswith("0"):
                logger.info(f"Pruned out-of-scope watchlist rows: {removed}")

    logger.info(
        "Tier counts: "
        + " ".join(f"t{t}={tier_counts[t]}" for t in sorted(tier_counts))
    )
    return tier_counts


async def main() -> None:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=2)
    try:
        await compute_and_upsert(pool)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
