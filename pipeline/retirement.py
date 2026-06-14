"""Record retired hulls — registry rows whose most recent fix is long past.

`vessel_registry` accretes hulls but never lets one go. Over a decade a carrier is
scrapped, sold out of LNG, or laid up permanently; it stops appearing in
`ais_fixes`. Recording that keeps the registry honest *at every point in time* —
so a hull retired in 2021 isn't carried as live-fleet in 2024, and the discovery /
rescue / scan workers don't spend slots or VF credits chasing a ghost.

**Silence is a soft signal, not proof — so retirement is reversible.** Our own
live coverage is partial (3-connection cap, scan rotation), and an LNG carrier can
legitimately lay up for months in a weak market. So:

  - The threshold is deliberately long — `RETIREMENT_SILENCE_DAYS = 365`. An active
    carrier reports *somewhere* (the archive + every live source feed `ais_fixes`)
    within a year; a full year of total silence is the scrap/lay-up signal.
  - A hull with **no fix at all** is NOT auto-retired (no temporal evidence — it may
    be a freshly-registered row whose archive fixes haven't been reloaded yet).
  - Retirement **clears itself** the moment a new fix lands (`to_unretire`), so a
    resurfacing vessel or a late archive reload self-corrects with no manual step.

`retirement_basis` records *why* ('silence' here; 'manual'/'igu_dropout' reserved
for the authoritative complement — a hull dropping off the annual IGU fleet report
is firmer than silence, and can be wired in later as a second basis).

Run via `make retire-stale` (`make retire-stale-dry` to preview). Consumers filter
on `retired_at IS NULL` (scoring candidate pools, discovery, rescue) — recording is
decoupled from consumption so this job is safe to run independently.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timedelta, timezone

import asyncpg

from config import settings

logger = logging.getLogger("retirement")

RETIREMENT_SILENCE_DAYS = 365

# last_fix per registry hull. Correlated max() rides the (mmsi, fix_ts) index, so
# this is ~5.6k index probes, not a scan of the 50M-row hypertable. FSRUs and
# manually-excluded rows are out of scope — an FSRU sits moored for months by
# design and has its own relocation-check lifecycle.
LAST_FIX_SQL = """
SELECT vr.mmsi, vr.retired_at,
       (SELECT max(f.fix_ts) FROM ais_fixes f WHERE f.mmsi = vr.mmsi) AS last_fix
FROM vessel_registry vr
WHERE NOT vr.excluded AND COALESCE(vr.is_fsru, FALSE) = FALSE
"""

RETIRE_SQL = """
UPDATE vessel_registry
SET retired_at = $2, retirement_basis = 'silence', updated_at = now()
WHERE mmsi = $1
"""
UNRETIRE_SQL = """
UPDATE vessel_registry
SET retired_at = NULL, retirement_basis = NULL, updated_at = now()
WHERE mmsi = $1
"""


def classify_retirements(
    rows: list[tuple[int, datetime | None, datetime | None]],
    now: datetime,
    threshold_days: int = RETIREMENT_SILENCE_DAYS,
) -> tuple[list[int], list[int]]:
    """Pure split of (mmsi, last_fix_ts, retired_at) rows into (to_retire,
    to_unretire). A hull is retired when its newest fix is older than the
    threshold; it is un-retired when a fix has landed back inside the window.
    No-fix rows are left untouched (insufficient evidence)."""
    cutoff = now - timedelta(days=threshold_days)
    to_retire, to_unretire = [], []
    for mmsi, last_fix, retired_at in rows:
        if last_fix is None:
            continue
        stale = last_fix < cutoff
        if stale and retired_at is None:
            to_retire.append(mmsi)
        elif not stale and retired_at is not None:
            to_unretire.append(mmsi)
    return to_retire, to_unretire


async def mark_retirements(
    pool: asyncpg.Pool,
    now: datetime,
    threshold_days: int = RETIREMENT_SILENCE_DAYS,
    dry_run: bool = False,
) -> tuple[int, int]:
    async with pool.acquire() as conn:
        rows = [
            (r["mmsi"], r["last_fix"], r["retired_at"])
            for r in await conn.fetch(LAST_FIX_SQL)
        ]
    to_retire, to_unretire = classify_retirements(rows, now, threshold_days)
    logger.info(
        "%d hull(s) silent >%dd → retire; %d resurfaced → un-retire (of %d in scope)%s",
        len(to_retire),
        threshold_days,
        len(to_unretire),
        len(rows),
        " [dry-run]" if dry_run else "",
    )
    if not dry_run:
        async with pool.acquire() as conn, conn.transaction():
            for mmsi in to_retire:
                await conn.execute(RETIRE_SQL, mmsi, now)
            for mmsi in to_unretire:
                await conn.execute(UNRETIRE_SQL, mmsi)
    return len(to_retire), len(to_unretire)


async def run(threshold_days: int, dry_run: bool) -> None:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=3)
    try:
        await mark_retirements(
            pool, datetime.now(timezone.utc), threshold_days, dry_run
        )
    finally:
        await pool.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    p = argparse.ArgumentParser(
        description="Record retired hulls (silence-based, reversible)"
    )
    p.add_argument(
        "--days",
        type=int,
        default=RETIREMENT_SILENCE_DAYS,
        help=f"silence threshold in days (default {RETIREMENT_SILENCE_DAYS})",
    )
    p.add_argument("--dry-run", action="store_true", help="report counts; no writes")
    args = p.parse_args()
    asyncio.run(run(args.days, args.dry_run))


if __name__ == "__main__":
    main()
