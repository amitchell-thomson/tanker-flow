"""Budgeted daily newbuild discovery.

Under the closed-loop MMSI-filter architecture there is no passive discovery, so
the only way a newly-delivered LNG carrier enters the system is:

    IGU orderbook → VF resolves IMO→MMSI → vessel_registry → AIS subscription.

This worker sweeps the orderbook hulls that are plausibly delivered by now
(`delivery_year <= current year`) and not yet in `vessel_registry`, and resolves
the ones VF can now map to a live MMSI — reusing import_igu_fleet's resolve path
(VF VESSELS + master → registry + starting fix + vessel_state).

Economics: VF bills per *returned* record, so an undelivered hull (404 / empty
body) costs 0 credits — a full sweep of unresolved hulls is free, and we pay only
the 3-credit master+AIS cost of a genuine catch. The budget is therefore a
*brake*, not a throttle, and it is subordinate to vf_rescue: a catch spends only
credits the reserve is *ahead* of its glide line (glide_surplus), within the
shared daily glide cap, capped by discovery's own small daily ceiling so it stays
rare. Catches log to vf_rescue_log (`rescue_class='discovery'`) so both workers
share one credit ledger; the post-run /status snapshot lets the next vf_rescue
pass see the spend via the refreshed balance.

Run daily:
    uv run python scripts/discover_newbuilds.py            # spend
    uv run python scripts/discover_newbuilds.py --dry-run  # preview, no spend
or via `make discover` / `make discover-dry`. Refresh the candidate pool itself
(new orderbook entries / named hulls) with the annual `make refresh-fleet`.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncpg  # noqa: E402
import httpx  # noqa: E402
from rich.logging import RichHandler  # noqa: E402

from config import settings  # noqa: E402
from ingestion.vf_rescue import (  # noqa: E402
    GLIDE_CAP_CEILING,
    discovery_credit_budget,
    load_budget_today,
    load_glide_cap,
    load_glide_surplus,
    update_account_status,
)
from scripts.import_igu_fleet import (  # noqa: E402
    RATE_LIMIT_DELAY,
    fetch_vessel,
    insert_snapshot_fix,
    insert_vessel_state,
    load_igu_capacities,
    upsert_registry,
)

logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])
logger = logging.getLogger(__name__)

# A successful catch = one VF VESSELS row with master data = 3 credits. An
# undelivered hull returns nothing and is free (see module docstring).
DISCOVERY_CREDIT_COST = 3
# Guaranteed daily floor — one catch/day even when rescue has eaten the whole
# glide (surplus ≈ 0), so a delivered hull is never starved out. Real cost is
# trivial: the candidate pool drains as hulls are caught, and an unresolved hull
# is a free miss, so this only ever "spends" when there's an actual delivery.
DISCOVERY_DAILY_FLOOR = DISCOVERY_CREDIT_COST  # 1 catch/day
# Rareness cap — discovery may go faster than the floor on genuine-surplus days,
# but never beyond this (≈ DISCOVERY_DAILY_CEILING / 3 catches/day). Spreads a
# burst (e.g. an annual IGU refresh adding many resolvable hulls) over a few runs.
DISCOVERY_DAILY_CEILING = 12

LOG_DISCOVERY_SQL = """
INSERT INTO vf_rescue_log (
    mmsi, imo, vessel_name, rescue_class, src, result, credits,
    requested_imos, returned_rows
)
VALUES ($1, $2, $3, 'discovery', 'TER', 'rescued', $4, 1, 1)
"""


async def load_undelivered_candidates(
    pool: asyncpg.Pool, csv_path: Path, now: datetime
) -> list[int]:
    """IGU IMOs plausibly delivered by now (`delivery_year <= current year`) and
    not yet in vessel_registry, earliest delivery first — under a binding budget
    the oldest hulls (most likely already transmitting) are tried first."""
    candidates: list[tuple[int, int]] = []  # (delivery_year, imo)
    with csv_path.open() as f:
        for r in csv.DictReader(f):
            if not r.get("imo") or not r.get("delivery_year"):
                continue
            try:
                imo = int(r["imo"])
                year = int(r["delivery_year"])
            except ValueError:
                continue
            if year <= now.year:
                candidates.append((year, imo))
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT imo FROM vessel_registry WHERE imo IS NOT NULL AND imo != 0"
        )
    known = {r["imo"] for r in rows}
    return [imo for _year, imo in sorted(candidates) if imo not in known]


async def run(csv_path: Path, dry_run: bool) -> None:
    now = datetime.now(timezone.utc)
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=3)
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            # Refresh the free balance snapshot so the glide budget is current.
            if not dry_run:
                await update_account_status(pool, client)

            candidates = await load_undelivered_candidates(pool, csv_path, now)
            caps = load_igu_capacities(csv_path)

            async with pool.acquire() as conn:
                cap = await load_glide_cap(conn, now)
                surplus = await load_glide_surplus(conn, now)
                spent = await load_budget_today(conn)
            budget = discovery_credit_budget(
                surplus=surplus,
                glide_cap_value=cap,
                spent_today=spent,
                floor=DISCOVERY_DAILY_FLOOR,
                ceiling=DISCOVERY_DAILY_CEILING,
                brake=GLIDE_CAP_CEILING,
            )
            logger.info(
                f"{len(candidates)} undelivered candidates (delivery_year <= "
                f"{now.year}); discovery budget {budget}cr "
                f"(surplus {surplus:.0f}, glide cap {cap}, spent today {spent})"
            )

            if dry_run:
                preview = candidates[:5]
                more = max(0, len(candidates) - len(preview))
                logger.info(
                    f"DRY RUN — no API calls / writes. Would sweep {preview} … "
                    f"(+{more} more), resolving up to "
                    f"{budget // DISCOVERY_CREDIT_COST} catch(es)."
                )
                return

            caught = misses = no_mmsi = errors = 0
            for imo in candidates:
                if budget < DISCOVERY_CREDIT_COST:
                    swept = caught + misses + no_mmsi + errors
                    logger.info(
                        f"Discovery budget exhausted after {caught} catch(es); "
                        f"{len(candidates) - swept} candidate(s) roll to next run."
                    )
                    break
                try:
                    result = await fetch_vessel(client, imo)
                except Exception as e:  # noqa: BLE001 — one bad IMO must not kill the sweep
                    logger.warning(f"IMO={imo}: request failed ({e})")
                    errors += 1
                    await asyncio.sleep(RATE_LIMIT_DELAY)
                    continue

                if result is None:
                    misses += 1  # undelivered / not in VF — free
                    await asyncio.sleep(RATE_LIMIT_DELAY)
                    continue

                ais = result.get("AIS") or {}
                master = result.get("MASTERDATA") or {}
                mmsi = ais.get("MMSI")
                if not mmsi:
                    # VF has the hull but no live MMSI yet — treat as not-yet
                    # catchable (no subscription target). Don't bill it here; the
                    # balance snapshot reconciles any incidental charge.
                    no_mmsi += 1
                    await asyncio.sleep(RATE_LIMIT_DELAY)
                    continue

                async with pool.acquire() as conn:
                    async with conn.transaction():
                        await upsert_registry(
                            conn, ais, master, igu_capacity=caps.get(imo)
                        )
                        await insert_snapshot_fix(conn, ais)
                        await insert_vessel_state(conn, ais)
                        await conn.execute(
                            LOG_DISCOVERY_SQL,
                            mmsi,
                            ais.get("IMO") or master.get("IMO") or imo,
                            master.get("NAME") or ais.get("NAME"),
                            DISCOVERY_CREDIT_COST,
                        )
                budget -= DISCOVERY_CREDIT_COST
                caught += 1
                logger.info(
                    f"CAUGHT IMO={imo} MMSI={mmsi}: "
                    f"{master.get('NAME') or ais.get('NAME') or '?'} "
                    f"(−{DISCOVERY_CREDIT_COST}cr, {budget}cr left)"
                )
                await asyncio.sleep(RATE_LIMIT_DELAY)

            # Refresh the balance again so the next vf_rescue pass sees this spend.
            if caught:
                await update_account_status(pool, client)
            logger.info(
                f"Done. caught={caught} misses={misses} "
                f"no_mmsi={no_mmsi} errors={errors}"
            )
    finally:
        await pool.close()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--csv", type=Path, default=Path("db/seed/lng_fleet_igu_2025.csv"))
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    try:
        asyncio.run(run(args.csv, args.dry_run))
    except KeyboardInterrupt:
        logger.info("Stopped.")


if __name__ == "__main__":
    main()
