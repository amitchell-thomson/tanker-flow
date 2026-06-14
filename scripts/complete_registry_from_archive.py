"""§3.6.1 — retrospective registry completion from the NOAA Tier-1 archive.

The live mirror of `scripts/discover_berth_tankers.py`, but the candidate source
is the on-disk decade **archive** (`data/noaa_archive/`) instead of the live
`discovery_candidates` table. The Tier-2 gate only ever admitted LNG hulls
*already in `vessel_registry`*; an early-era hull, a scrapped carrier, or a
newbuild we never resolved is **not lost** — its tanker fixes sit in the Parquet
archive. This one-time sweep recovers them.

The trusted inference (PLAN §3.6.1): **a tanker stopped (`sog < 1`) inside a
dedicated US LNG *berth* polygon is near-certain LNG.** A bare bbox is NOT enough
— the Calcasieu/Sabine ship channels are lined with chemical/oil terminals, so a
400 m bbox catches transiting through-traffic; precise polygon containment + the
stationary filter is what makes the hit trustworthy.

Pipeline-correctness note — **register under the *archive* MMSI, not VF's current
one.** `reload_archive` admits a fix keyed by the MMSI NOAA recorded *and* filtered
by `vessel_registry` IMO; `port_events` then walks `ais_fixes WHERE mmsi ∈ registry
(is_lng_carrier)`. So a recovered hull's *archive* MMSI must be the registry key,
or its historical fixes never reach the state machine. Over a decade an MMSI is
reused (§3.6), so before writing we **guard against collision**: an archive MMSI
that already belongs to a *different* IMO is skipped (its early fixes are the known
unrecoverable floor, alongside imo=0-in-berth hulls).

Resolution policy (PLAN §3.6.1 step 3 — "VF for the living, source-identity for
the dead"):
  - VF hit & MASTERDATA.TYPE in the LNG family (LNG Tanker/FSO/FSU)  -> register
    with full master. FSO/FSU are accepted because a hull that loaded at a US
    export berth in the archive but has since converted reads as FSO/FSU in VF's
    *current* type (see LNG_FAMILY_TYPES).
  - VF hit, clearly non-LNG type                      -> NOT registered (the berth
    hit was a false positive). Billed, logged.
  - VF miss (404/empty — scrapped pre-~2024)          -> source-identity fallback:
    register from the archive's own IMO + name, is_lng_carrier=TRUE, **fleet-mean
    gas_capacity_m3** (the archive carries no dimensions). Free.

After registration run `make backfill-noaa-reload START=… END=…` (no re-download)
then `make port-events && make signals` to fold the recovered fixes into the signal.
`--reload` does the reload inline over the swept range.

Usage:
    uv run python -m scripts.complete_registry_from_archive --dry-run     # sweep + cost, no spend
    uv run python -m scripts.complete_registry_from_archive               # sweep + VF resolve + register
    uv run python -m scripts.complete_registry_from_archive --reload      # + reload Tier-2 over swept range
    uv run python -m scripts.complete_registry_from_archive --sample-monthly  # fast preview (15th of each month)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncpg  # noqa: E402
import httpx  # noqa: E402
import pandas as pd  # noqa: E402
from rich.logging import RichHandler  # noqa: E402
from rich.progress import (  # noqa: E402
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from shapely import wkt  # noqa: E402
from shapely.geometry import Point  # noqa: E402
from shapely.strtree import STRtree  # noqa: E402

from config import settings  # noqa: E402
from ingestion.historical.noaa_ais import archive_path, daterange, reload_archive  # noqa: E402
from ingestion.vf_rescue import update_account_status  # noqa: E402
from scripts.import_igu_fleet import RATE_LIMIT_DELAY, fetch_vessel  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])
logger = logging.getLogger(__name__)

ARCHIVE_START = date(2016, 1, 1)
ARCHIVE_END = date(2025, 12, 31)
US_ZONES = ("usgulf", "usatlantic")
VF_RECORD_CREDITS = 3  # one returned VF VESSELS record = master+AIS

# VF types we accept for HISTORICAL registration. Stricter than nothing, looser
# than the live `== 'LNG Tanker'` gate: a hull that loaded at a US *export* berth
# during the archive but has since been converted reads as 'FSO'/'FSU' in VF's
# *current* masterdata (e.g. EXCALIBUR, ENERGOS GRAND ex-Golar Grand, GASLOG
# SINGAPORE). Those are real historical LNG voyages, so the strict gate would
# wrongly drop them. The sog<1 + LNG-berth-polygon evidence backstops the FSO
# ambiguity — an *oil* FSO never sits inside an LNG export berth — so accepting
# the LNG-family here is safe; any other VF type (oil/chemical tanker) is rejected.
LNG_FAMILY_TYPES = frozenset({"LNG Tanker", "FSO", "FSU"})


def is_registerable_type(vf_type: str | None) -> bool:
    """Pure gate: does VF's current type permit historical LNG registration?"""
    return vf_type in LNG_FAMILY_TYPES


BERTH_POLY_SQL = """
SELECT t.terminal_name, ST_AsText(tz.geom) AS wkt
FROM terminal_zones tz
JOIN terminals t ON t.terminal_id = tz.terminal_id
WHERE t.zone = ANY($1::text[]) AND tz.zone_type = 'berth'
"""

# imo -> (is_lng_carrier, is_fsru); only IMOs present in the registry.
REGISTRY_IMO_SQL = """
SELECT imo, COALESCE(is_lng_carrier, FALSE) AS is_lng, COALESCE(is_fsru, FALSE) AS is_fsru
FROM vessel_registry WHERE imo IS NOT NULL AND imo <> 0
"""
# mmsi -> imo for EVERY registry row (the MMSI-reuse collision guard).
REGISTRY_MMSI_SQL = "SELECT mmsi, imo FROM vessel_registry"
FLEET_MEAN_GAS_SQL = (
    "SELECT round(avg(gas_capacity_m3)) AS m FROM vessel_registry "
    "WHERE is_lng_carrier AND gas_capacity_m3 IS NOT NULL"
)

LOG_SQL = """
INSERT INTO vf_rescue_log (
    mmsi, imo, vessel_name, rescue_class, src, result, credits,
    requested_imos, returned_rows, detail
)
VALUES ($1, $2, $3, 'archive_completion', $4, $5, $6, 1, $7, $8)
"""


# --------------------------------------------------------------------------- #
# Sweep (Tier-1 archive) — pure geometry, no DB
# --------------------------------------------------------------------------- #
@dataclass
class BerthHull:
    """One IMO seen stopped in a US LNG berth somewhere in the archive."""

    imo: int
    mmsis: set[int] = field(default_factory=set)
    names: set[str] = field(default_factory=set)
    last_ts: datetime | None = None
    n_fixes: int = 0


def _bbox(polys) -> tuple[float, float, float, float]:
    xs = [b for p in polys for b in (p.bounds[0], p.bounds[2])]
    ys = [b for p in polys for b in (p.bounds[1], p.bounds[3])]
    return min(xs), min(ys), max(xs), max(ys)


def berth_hits(df: pd.DataFrame, polys: list, tree: STRtree, bbox) -> pd.DataFrame:
    """Rows of one archive frame that are STOPPED inside an LNG berth polygon.

    Cheap funnel: drop no-IMO rows, keep stationary (sog<1 or unreported), bbox
    prefilter, then precise point-in-polygon on the small survivor set."""
    xmin, ymin, xmax, ymax = bbox
    df = df.dropna(subset=["imo_int"])
    df = df[(df["SOG"].isna()) | (df["SOG"] < 1.0)]
    df = df[df["LON"].between(xmin, xmax) & df["LAT"].between(ymin, ymax)]
    if df.empty:
        return df
    keep = [
        any(polys[i].covers(pt) for i in tree.query(pt))
        for pt in (Point(lon, lat) for lon, lat in zip(df["LON"], df["LAT"]))
    ]
    return df[keep]


def sweep_archive(
    days: list[date], polys: list, tree: STRtree, bbox, archive_dir: Path
) -> dict[int, BerthHull]:
    hulls: dict[int, BerthHull] = {}
    present = [d for d in days if archive_path(d, archive_dir).exists()]
    with Progress(
        TextColumn("[cyan]sweeping archive"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("· {task.percentage:>3.0f}% · hulls {task.fields[hulls]}"),
        TimeRemainingColumn(),
        TimeElapsedColumn(),
    ) as prog:
        task = prog.add_task("sweep", total=len(present), hulls=0)
        for d in present:
            df = pd.read_parquet(
                archive_path(d, archive_dir),
                columns=[
                    "MMSI",
                    "LAT",
                    "LON",
                    "SOG",
                    "imo_int",
                    "VesselName",
                    "fix_ts",
                ],
            )
            hit = berth_hits(df, polys, tree, bbox)
            for r in hit.itertuples(index=False):
                imo = int(r.imo_int)
                h = hulls.setdefault(imo, BerthHull(imo))
                h.mmsis.add(int(r.MMSI))
                if r.VesselName and str(r.VesselName) != "nan":
                    h.names.add(str(r.VesselName))
                ts = r.fix_ts.to_pydatetime()
                h.last_ts = ts if h.last_ts is None or ts > h.last_ts else h.last_ts
                h.n_fixes += 1
            prog.update(task, advance=1, hulls=len(hulls))
    return hulls


# --------------------------------------------------------------------------- #
# Diff vs registry — pure categorisation
# --------------------------------------------------------------------------- #
@dataclass
class GapDecision:
    imo: int
    category: str  # in_scope | fsru | reflag | absent
    names: list[str]
    register_mmsis: list[int] = field(default_factory=list)
    collision_mmsis: list[int] = field(default_factory=list)
    last_ts: datetime | None = None


def classify_gap(
    hulls: dict[int, BerthHull],
    imo_class: dict[int, tuple[bool, bool]],
    mmsi_to_imo: dict[int, int],
) -> list[GapDecision]:
    """Split swept hulls into action categories. Pure — unit-tested.

    in_scope : already is_lng_carrier — nothing to do.
    fsru     : already is_fsru — by design (host short-circuit), not a loss.
    reflag   : in the registry but not flagged LNG/FSRU — VF-confirm then flag.
    absent   : never registered — resolve + register.
    For reflag/absent, an archive MMSI already bound to a *different* IMO is a
    reuse collision: dropped to collision_mmsis (unrecoverable floor), never
    overwritten.
    """
    out: list[GapDecision] = []
    for imo in sorted(hulls):
        h = hulls[imo]
        names = sorted(h.names)
        is_lng, is_fsru = imo_class.get(imo, (False, False))
        if imo in imo_class and is_lng:
            out.append(GapDecision(imo, "in_scope", names, last_ts=h.last_ts))
            continue
        if imo in imo_class and is_fsru:
            out.append(GapDecision(imo, "fsru", names, last_ts=h.last_ts))
            continue
        category = "reflag" if imo in imo_class else "absent"
        register, collide = [], []
        for mmsi in sorted(m for m in h.mmsis if m):
            owner = mmsi_to_imo.get(mmsi)
            (collide if owner is not None and owner != imo else register).append(mmsi)
        out.append(GapDecision(imo, category, names, register, collide, h.last_ts))
    return out


# --------------------------------------------------------------------------- #
# Registration (guarded upsert, keyed on the archive MMSI)
# --------------------------------------------------------------------------- #
async def upsert_hull(
    conn,
    mmsi: int,
    imo: int,
    name: str | None,
    master: dict | None,
    fleet_mean_gas: int | None,
) -> None:
    """Write/flag a registry row keyed on the ARCHIVE mmsi. `master` is VF
    MASTERDATA when resolvable, else None (source-identity fallback). The
    ON CONFLICT guard refuses to clobber a row that belongs to a different IMO —
    defence in depth behind classify_gap's collision filter."""
    m = master or {}
    gas = m.get("GAS") or fleet_mean_gas
    status = "ok" if master else "not_found"
    await conn.execute(
        """
        INSERT INTO vessel_registry (
            mmsi, imo, vessel_name, flag, vf_vessel_type, year_built, builder,
            owner, manager, length_m, beam_m, gross_tonnage, net_tonnage, dwt,
            design_draught, gas_capacity_m3, is_lng_carrier, is_fsru,
            enriched_at, vf_enrichment_status, updated_at
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,TRUE,FALSE,
            CASE WHEN $17 THEN now() END, $18, now()
        )
        ON CONFLICT (mmsi) DO UPDATE SET
            imo             = COALESCE(vessel_registry.imo, EXCLUDED.imo),
            vessel_name     = COALESCE(vessel_registry.vessel_name, EXCLUDED.vessel_name),
            flag            = COALESCE(EXCLUDED.flag, vessel_registry.flag),
            vf_vessel_type  = COALESCE(EXCLUDED.vf_vessel_type, vessel_registry.vf_vessel_type),
            year_built      = COALESCE(EXCLUDED.year_built, vessel_registry.year_built),
            builder         = COALESCE(EXCLUDED.builder, vessel_registry.builder),
            owner           = COALESCE(EXCLUDED.owner, vessel_registry.owner),
            manager         = COALESCE(EXCLUDED.manager, vessel_registry.manager),
            length_m        = COALESCE(EXCLUDED.length_m, vessel_registry.length_m),
            beam_m          = COALESCE(EXCLUDED.beam_m, vessel_registry.beam_m),
            gross_tonnage   = COALESCE(EXCLUDED.gross_tonnage, vessel_registry.gross_tonnage),
            net_tonnage     = COALESCE(EXCLUDED.net_tonnage, vessel_registry.net_tonnage),
            dwt             = COALESCE(EXCLUDED.dwt, vessel_registry.dwt),
            design_draught  = COALESCE(EXCLUDED.design_draught, vessel_registry.design_draught),
            gas_capacity_m3 = COALESCE(EXCLUDED.gas_capacity_m3, vessel_registry.gas_capacity_m3),
            is_lng_carrier  = TRUE,
            enriched_at     = COALESCE(vessel_registry.enriched_at, EXCLUDED.enriched_at),
            vf_enrichment_status = COALESCE(vessel_registry.vf_enrichment_status, EXCLUDED.vf_enrichment_status),
            updated_at      = now()
        WHERE vessel_registry.imo IS NULL OR vessel_registry.imo = EXCLUDED.imo
        """,
        mmsi,
        imo,
        (m.get("NAME") or name),
        m.get("FLAG"),
        m.get("TYPE"),
        m.get("BUILT"),
        m.get("BUILDER"),
        m.get("OWNER"),
        m.get("MANAGER"),
        m.get("LENGTH"),
        m.get("BEAM"),
        m.get("GT"),
        m.get("NT"),
        m.get("DWT"),
        m.get("MAXDRAUGHT"),
        gas,
        bool(master),
        status,
    )


async def _resolve_and_register(
    pool: asyncpg.Pool,
    client: httpx.AsyncClient,
    decision: GapDecision,
    fleet_mean_gas: int | None,
) -> str:
    """One gap hull: VF-resolve by IMO (once), then register every recoverable
    archive MMSI. Returns an outcome label for the run tally."""
    imo, name = decision.imo, (decision.names[0] if decision.names else None)
    try:
        result = await fetch_vessel(client, imo)
    except Exception as e:
        logger.warning(f"IMO={imo} ({name}): VF request failed ({e})")
        async with pool.acquire() as conn:
            await conn.execute(
                LOG_SQL,
                decision.register_mmsis[0] if decision.register_mmsis else 0,
                imo,
                name,
                None,
                "error",
                0,
                0,
                repr(e)[:200],
            )
        return "error"

    master = (result or {}).get("MASTERDATA") or None
    vf_type = master.get("TYPE") if master else None

    # VF type vetoes only clearly-non-LNG hulls: a returned record outside the
    # LNG family (oil/chemical tanker) means the berth hit was a false positive —
    # record (billed) and drop. FSO/FSU pass (see LNG_FAMILY_TYPES).
    if master and not is_registerable_type(vf_type):
        async with pool.acquire() as conn:
            await conn.execute(
                LOG_SQL,
                decision.register_mmsis[0] if decision.register_mmsis else 0,
                imo,
                name,
                "TER",
                "not_lng",
                VF_RECORD_CREDITS,
                1,
                f"vf_type={vf_type}",
            )
        logger.info(f"NOT LNG  IMO={imo} ({name}): VF type={vf_type} — skipped")
        return "not_lng"

    if not decision.register_mmsis:
        logger.warning(
            f"IMO={imo} ({name}): all archive MMSIs collide with live rows — skipped"
        )
        return "collision"

    credits = VF_RECORD_CREDITS if master else 0  # a VF miss is free
    src = "TER" if master else None
    async with pool.acquire() as conn, conn.transaction():
        for mmsi in decision.register_mmsis:
            await upsert_hull(conn, mmsi, imo, name, master, fleet_mean_gas)
        await conn.execute(
            LOG_SQL,
            decision.register_mmsis[0],
            imo,
            (master.get("NAME") if master else name),
            src,
            "rescued",
            credits,
            1 if master else 0,
            "source_identity_fallback" if not master else f"vf_type={vf_type}",
        )
    via = "VF master" if master else "source-identity (VF miss)"
    logger.info(
        f"REGISTERED IMO={imo} ({name}) via {via} → MMSI {decision.register_mmsis} "
        f"is_lng_carrier=TRUE"
        + (
            f"  [{len(decision.collision_mmsis)} colliding MMSI skipped]"
            if decision.collision_mmsis
            else ""
        )
    )
    return "registered"


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
async def run(args: argparse.Namespace) -> None:
    archive_dir = Path(args.archive_dir)
    start = date.fromisoformat(args.start) if args.start else ARCHIVE_START
    end = date.fromisoformat(args.end) if args.end else ARCHIVE_END
    if args.sample_monthly:
        days = [
            date(y, m, 15)
            for y in range(start.year, end.year + 1)
            for m in range(1, 13)
        ]
        days = [d for d in days if start <= d <= end]
    else:
        days = list(daterange(start, end))

    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=3)
    try:
        rows = await pool.fetch(BERTH_POLY_SQL, list(US_ZONES))
        polys = [wkt.loads(r["wkt"]) for r in rows]
        if not polys:
            logger.error("No US LNG berth polygons found — seed terminal_zones first.")
            return
        tree, bbox = STRtree(polys), _bbox(polys)
        logger.info(
            f"{len(polys)} US LNG berth polygons; sweeping {len(days)} archive days"
        )

        hulls = await asyncio.to_thread(
            sweep_archive, days, polys, tree, bbox, archive_dir
        )

        imo_class = {
            r["imo"]: (r["is_lng"], r["is_fsru"])
            for r in await pool.fetch(REGISTRY_IMO_SQL)
        }
        mmsi_to_imo = {r["mmsi"]: r["imo"] for r in await pool.fetch(REGISTRY_MMSI_SQL)}
        fleet_mean_gas = await pool.fetchval(FLEET_MEAN_GAS_SQL)
        fleet_mean_gas = int(fleet_mean_gas) if fleet_mean_gas else None

        decisions = classify_gap(hulls, imo_class, mmsi_to_imo)
        by_cat: dict[str, list[GapDecision]] = {}
        for d in decisions:
            by_cat.setdefault(d.category, []).append(d)
        gap = by_cat.get("reflag", []) + by_cat.get("absent", [])

        logger.info(
            f"Swept {len(hulls)} distinct hulls in a US LNG berth: "
            f"in_scope={len(by_cat.get('in_scope', []))} "
            f"fsru={len(by_cat.get('fsru', []))} (by design) "
            f"reflag={len(by_cat.get('reflag', []))} absent={len(by_cat.get('absent', []))}"
        )
        for d in gap:
            logger.info(
                f"  GAP[{d.category}] IMO={d.imo} {', '.join(d.names) or '?'} "
                f"→ register {d.register_mmsis}"
                + (
                    f" (skip colliding {d.collision_mmsis})"
                    if d.collision_mmsis
                    else ""
                )
            )
        if not gap:
            logger.info("No registry gap — nothing to resolve.")
            return

        if args.dry_run:
            max_cost = len(gap) * VF_RECORD_CREDITS
            logger.info(
                f"[dry-run] would VF-resolve {len(gap)} IMOs "
                f"(≤{max_cost} credits — VF misses are free); no spend, no writes."
            )
            return

        fleet_note = (
            f"fleet-mean gas fallback = {fleet_mean_gas:,} m³"
            if fleet_mean_gas
            else "no fleet-mean gas"
        )
        logger.info(f"Resolving {len(gap)} gap hulls via VF ({fleet_note})…")
        tally: dict[str, int] = {}
        async with httpx.AsyncClient(timeout=15.0) as client:
            for d in gap:
                outcome = await _resolve_and_register(pool, client, d, fleet_mean_gas)
                tally[outcome] = tally.get(outcome, 0) + 1
                await asyncio.sleep(RATE_LIMIT_DELAY)
            if tally.get("registered") or tally.get("not_lng"):
                await update_account_status(pool, client)
        logger.info(f"Registry completion done. Outcomes: {tally}")

        if args.reload and tally.get("registered"):
            logger.info(
                f"Reloading Tier-2 from archive {start}..{end} (no re-download)…"
            )
            await reload_archive(pool, start, end, archive_dir)
            logger.info(
                "Reload done. Now rebuild downstream: `make port-events && make signals`."
            )
        elif tally.get("registered"):
            logger.info(
                "Next: `make backfill-noaa-reload START=%s END=%s` then "
                "`make port-events && make signals`." % (start, end)
            )
    finally:
        await pool.close()


def main() -> None:
    p = argparse.ArgumentParser(
        description="Retrospective registry completion from the NOAA archive (§3.6.1)"
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Sweep + categorise + cost estimate; no VF spend, no writes",
    )
    p.add_argument(
        "--sample-monthly",
        action="store_true",
        help="Sweep only the 15th of each month (fast preview)",
    )
    p.add_argument(
        "--reload",
        action="store_true",
        help="After registering, reload Tier-2 from the archive over the swept range",
    )
    p.add_argument("--start", help="sweep range start YYYY-MM-DD (default 2016-01-01)")
    p.add_argument("--end", help="sweep range end YYYY-MM-DD (default 2025-12-31)")
    p.add_argument("--archive-dir", default="data/noaa_archive")
    args = p.parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        logger.info("Stopped.")


if __name__ == "__main__":
    main()
