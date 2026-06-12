"""NOAA Marine Cadastre historical AIS loader — two-tier, strict one-at-a-time.

For each UTC day (2016+, the US LNG export era — PLAN.md §1.1) this downloads the
nationwide daily zip, keeps **all tankers** (`VesselType 80–89`) to a compressed
Parquet archive (Tier 1 — the density source + "download once" insurance), and
loads the **LNG-carrier subset within 50 km of a US terminal** into `ais_fixes` +
`vessel_state` (Tier 2 — the state-machine pipeline). See PLAN.md §3.6/§3.8.

Storage discipline (PLAN.md §3.8): the raw zip is processed then **deleted before
the next day**, so peak raw footprint is ONE ~270 MB file no matter how many days
run — the full decade is ~1.1 TB of *downloads* but never stored at once. The
growing artefact is only the tanker Parquet archive (~20–30 GB for the decade at a
measured 4.6 % tanker fraction).

Tier 2 resolves LNG carriers by **IMO** against `vessel_registry` (stable over a
decade vs reused MMSIs). Historical hulls not yet in the registry are **not lost** —
they sit in the Tier-1 archive and are re-filtered into `ais_fixes` (no re-download)
once the registry is widened (the §3.6 admit-historical-hulls follow-on).

Usage:
    uv run python -m ingestion.historical.noaa_ais --date 2022-01-01
    uv run python -m ingestion.historical.noaa_ais --start 2022-01-01 --end 2022-01-31
    uv run python -m ingestion.historical.noaa_ais --local data/noaa_raw/AIS_2022_01_01.zip
    make backfill-noaa  (wraps a date range)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

import asyncpg
import httpx
import pandas as pd

import config

logger = logging.getLogger("noaa_ais")

URL_TEMPLATE = (
    "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{y}/AIS_{y}_{m:02d}_{d:02d}.zip"
)
RAW_DIR = Path("data/noaa_raw")
ARCHIVE_DIR = Path("data/noaa_archive")
NOAA_SOURCE = "noaa-ais"
TERMINAL_BUFFER_M = 50_000  # 50 km — PLAN.md §3.8
US_ZONES = ("usgulf", "usatlantic")

# Columns we parse from the daily CSV (the file has 17; we keep what the two tiers
# need). VesselType filters tankers; IMO keys LNG resolution; Draft feeds laden.
USECOLS = [
    "MMSI", "BaseDateTime", "LAT", "LON", "SOG", "COG",
    "VesselName", "IMO", "VesselType", "Status", "Draft",
]


def day_url(d: date) -> str:
    return URL_TEMPLATE.format(y=d.year, m=d.month, d=d.day)


def archive_path(d: date, archive_dir: Path = ARCHIVE_DIR) -> Path:
    # Hive-style year= partition so a density query can prune by year.
    return archive_dir / f"year={d.year}" / f"AIS_{d.year}_{d.month:02d}_{d.day:02d}.parquet"


def parse_imo(value) -> int | None:
    """NOAA IMO is 'IMO9830305' (string) — strip the prefix to the bare number."""
    try:
        return int(str(value).replace("IMO", "").strip())
    except (ValueError, TypeError):
        return None


# ----------------------------------------------------------------------------- #
# Download + read (Tier 1)
# ----------------------------------------------------------------------------- #
def download_zip(d: date, raw_dir: Path) -> Path:
    """Stream the daily zip to disk (one file at a time). Returns the local path."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    dest = raw_dir / f"AIS_{d.year}_{d.month:02d}_{d.day:02d}.zip"
    url = day_url(d)
    with httpx.stream("GET", url, follow_redirects=True, timeout=None) as r:
        if r.status_code == 404:
            raise FileNotFoundError(f"NOAA has no file for {d} ({url})")
        r.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in r.iter_bytes(chunk_size=1 << 20):
                fh.write(chunk)
    return dest


def read_tankers(zip_path: Path) -> pd.DataFrame:
    """Read the daily CSV (pandas auto-decompresses the single-member zip), keep
    tankers, normalise the fields the loader needs."""
    # engine='pyarrow' is multithreaded — ~3x faster than the default C parser on
    # these 7M-row daily files (profiled 9.0s -> 2.7s), same result.
    df = pd.read_csv(zip_path, usecols=USECOLS, engine="pyarrow")
    df = df[df["VesselType"].between(80, 89)].copy()
    df["fix_ts"] = pd.to_datetime(df["BaseDateTime"], utc=True)
    df["imo_int"] = df["IMO"].map(parse_imo)
    # NOAA Draft of 0 means "unreported" (same sentinel rule as models.py).
    df.loc[df["Draft"] <= 0, "Draft"] = pd.NA
    return df


def write_archive(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = ["MMSI", "fix_ts", "LAT", "LON", "SOG", "COG", "VesselName",
            "imo_int", "VesselType", "Status", "Draft"]
    df[cols].to_parquet(path, compression="zstd", index=False)


# ----------------------------------------------------------------------------- #
# Tier 2: LNG-in-buffer -> ais_fixes + vessel_state
# ----------------------------------------------------------------------------- #
STAGE_DDL = """
CREATE TEMP TABLE noaa_stage (
    fix_ts TIMESTAMPTZ, mmsi BIGINT, lat DOUBLE PRECISION, lon DOUBLE PRECISION,
    sog REAL, cog REAL, nav_status SMALLINT, draught REAL
) ON COMMIT DROP
"""

# Insert only fixes within 50 km of a US export/import terminal polygon. The
# candidate set is already LNG-only (filtered in Python by IMO), so this spatial
# EXISTS runs over a handful of vessels.
INSERT_FIXES = f"""
INSERT INTO ais_fixes (fix_ts, mmsi, lat, lon, sog, cog, nav_status, source)
SELECT s.fix_ts, s.mmsi, s.lat, s.lon, s.sog, s.cog, s.nav_status, '{NOAA_SOURCE}'
FROM noaa_stage s
WHERE EXISTS (
    SELECT 1 FROM terminal_zones tz
    JOIN terminals t ON t.terminal_id = tz.terminal_id
    WHERE t.zone = ANY($1::text[])
      AND ST_DWithin(
            ST_SetSRID(ST_Point(s.lon, s.lat), 4326)::geography,
            tz.geom::geography, {TERMINAL_BUFFER_M})
)
ON CONFLICT (fix_ts, mmsi) DO NOTHING
"""

# Draught for laden inference — only near-terminal fixes that have a reported draft.
INSERT_STATE = f"""
INSERT INTO vessel_state (state_ts, mmsi, draught, source)
SELECT s.fix_ts, s.mmsi, s.draught, '{NOAA_SOURCE}'
FROM noaa_stage s
WHERE s.draught IS NOT NULL
  AND EXISTS (
    SELECT 1 FROM terminal_zones tz
    JOIN terminals t ON t.terminal_id = tz.terminal_id
    WHERE t.zone = ANY($1::text[])
      AND ST_DWithin(
            ST_SetSRID(ST_Point(s.lon, s.lat), 4326)::geography,
            tz.geom::geography, {TERMINAL_BUFFER_M})
)
ON CONFLICT (state_ts, mmsi) DO NOTHING
"""


async def load_tier2(pool: asyncpg.Pool, df: pd.DataFrame) -> tuple[int, int]:
    """Stage the day's LNG-carrier fixes and insert those near a US terminal."""
    lng = df[df["imo_int"].notna()].copy()
    async with pool.acquire() as conn:
        lng_imos = {
            r["imo"]
            for r in await conn.fetch(
                "SELECT imo FROM vessel_registry WHERE is_lng_carrier AND imo IS NOT NULL"
            )
        }
        cand = lng[lng["imo_int"].isin(lng_imos)]
        if cand.empty:
            return 0, 0
        records = [
            (
                row.fix_ts.to_pydatetime(),
                int(row.MMSI),
                float(row.LAT),
                float(row.LON),
                None if pd.isna(row.SOG) else float(row.SOG),
                None if pd.isna(row.COG) else float(row.COG),
                None if pd.isna(row.Status) else int(row.Status),
                None if pd.isna(row.Draft) else float(row.Draft),
            )
            for row in cand.itertuples(index=False)
        ]
        async with conn.transaction():
            await conn.execute(STAGE_DDL)
            await conn.copy_records_to_table("noaa_stage", records=records)
            fixes = await conn.execute(INSERT_FIXES, list(US_ZONES))
            state = await conn.execute(INSERT_STATE, list(US_ZONES))
    # asyncpg returns e.g. "INSERT 0 4269" — pull the row count.
    return int(fixes.split()[-1]), int(state.split()[-1])


# ----------------------------------------------------------------------------- #
# Per-day orchestration
# ----------------------------------------------------------------------------- #
async def process_day(
    pool: asyncpg.Pool | None,
    d: date,
    *,
    local: Path | None = None,
    raw_dir: Path = RAW_DIR,
    archive_dir: Path = ARCHIVE_DIR,
    keep_zip: bool = False,
    force: bool = False,
) -> None:
    arc = archive_path(d, archive_dir)
    if arc.exists() and not force and local is None:
        logger.info("%s already archived (%s) — skipping", d, arc.name)
        return

    zip_path = local if local is not None else download_zip(d, raw_dir)
    try:
        df = read_tankers(zip_path)
        write_archive(df, arc)
        msg = f"{d}: archived {len(df):,} tanker fixes -> {arc}"
        if pool is not None:
            fixes, state = await load_tier2(pool, df)
            msg += f" | tier-2: {fixes:,} ais_fixes + {state:,} vessel_state (LNG near US terminals)"
        logger.info(msg)
    finally:
        # One-at-a-time: drop the raw zip before the next day (unless it was a
        # caller-supplied --local file, which we leave alone).
        if local is None and not keep_zip and zip_path.exists():
            zip_path.unlink()


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


async def main() -> None:
    ap = argparse.ArgumentParser(description="NOAA historical AIS loader (two-tier).")
    ap.add_argument("--date", help="single UTC day YYYY-MM-DD")
    ap.add_argument("--start", help="range start YYYY-MM-DD")
    ap.add_argument("--end", help="range end YYYY-MM-DD (inclusive)")
    ap.add_argument("--local", help="process an already-downloaded zip (no download)")
    ap.add_argument("--archive-dir", default=str(ARCHIVE_DIR))
    ap.add_argument("--raw-dir", default=str(RAW_DIR))
    ap.add_argument("--keep-zip", action="store_true", help="don't delete the raw zip after processing")
    ap.add_argument("--force", action="store_true", help="re-archive even if the parquet exists")
    ap.add_argument("--no-db", action="store_true", help="Tier-1 archive only; skip ais_fixes load")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    archive_dir, raw_dir = Path(args.archive_dir), Path(args.raw_dir)
    pool = None if args.no_db else await asyncpg.create_pool(config.settings.database_url)
    try:
        if args.local:
            lp = Path(args.local)
            d = _date_from_name(lp.name)
            await process_day(pool, d, local=lp, archive_dir=archive_dir,
                              keep_zip=args.keep_zip, force=args.force)
        elif args.date:
            await process_day(pool, _d(args.date), raw_dir=raw_dir, archive_dir=archive_dir,
                              keep_zip=args.keep_zip, force=args.force)
        elif args.start and args.end:
            for d in daterange(_d(args.start), _d(args.end)):
                try:
                    await process_day(pool, d, raw_dir=raw_dir, archive_dir=archive_dir,
                                      keep_zip=args.keep_zip, force=args.force)
                except FileNotFoundError as e:
                    logger.warning("%s", e)  # NOAA gap day — skip, keep going
        else:
            ap.error("give --date, --start/--end, or --local")
    finally:
        if pool is not None:
            await pool.close()


def _d(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _date_from_name(name: str) -> date:
    # AIS_2022_01_01.zip -> date(2022,1,1)
    parts = name.replace(".zip", "").split("_")
    return date(int(parts[1]), int(parts[2]), int(parts[3]))


if __name__ == "__main__":
    asyncio.run(main())
