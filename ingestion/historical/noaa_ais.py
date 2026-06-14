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
import re
import ssl
from contextlib import nullcontext
from datetime import date, datetime, timedelta
from pathlib import Path

import asyncpg
import httpx
import pandas as pd
from rich.console import Console, Group
from rich.live import Live
from rich.progress import (
    BarColumn,
    DownloadColumn,
    MofNCompleteColumn,
    Progress,
    ProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TransferSpeedColumn,
)
from rich.text import Text

import config

logger = logging.getLogger("noaa_ais")

# NOAA migrated the daily AIS off coast.noaa.gov (legacy `AIS_YYYY_MM_DD.zip`) to
# this Azure blob (`csv2` tree, zstd-compressed CSV, lowercase schema), which also
# carries the newest year the old host never published. Verified 2026-06.
URL_TEMPLATE = (
    "https://noaaocm.blob.core.windows.net/ais/csv2/csv{y}/ais-{y}-{m:02d}-{d:02d}.csv.zst"
)
RAW_DIR = Path("data/noaa_raw")
ARCHIVE_DIR = Path("data/noaa_archive")
NOAA_SOURCE = "noaa-ais"
TERMINAL_BUFFER_M = 50_000  # 50 km — PLAN.md §3.8
US_ZONES = ("usgulf", "usatlantic")
# Fallback total for a download progress bar when the server omits Content-Length
# (Azure sends it; this only fires for the rare gap). A daily .csv.zst is ~150-300 MB.
EXPECTED_ZIP_BYTES = 220_000_000

# Over a multi-hour decade backfill, the CDN intermittently drops a TLS stream
# mid-download (observed: `ssl.SSLError: record layer failure`) or returns a 5xx.
# These are transient — retry the day with exponential backoff rather than let one
# bad stream escape asyncio.gather and abort the whole run.
DOWNLOAD_RETRIES = 5

# csv2 daily-CSV columns (lowercase, snake_case) → our internal names. We read this
# subset and rename so the rest of the loader (archive + Tier-2) is schema-agnostic.
USECOLS = [
    "mmsi", "base_date_time", "latitude", "longitude", "sog", "cog",
    "vessel_name", "imo", "vessel_type", "status", "draft",
]
RENAME = {
    "mmsi": "MMSI", "base_date_time": "BaseDateTime", "latitude": "LAT",
    "longitude": "LON", "sog": "SOG", "cog": "COG", "vessel_name": "VesselName",
    "imo": "IMO", "vessel_type": "VesselType", "status": "Status", "draft": "Draft",
}


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
async def _download(
    client: httpx.AsyncClient,
    d: date,
    raw_dir: Path,
    dl_progress: Progress | None = None,
) -> Path:
    """Stream one daily zip to disk over a shared async client (so many days
    download in parallel — NOAA throttles a single TCP stream to ~21 Mbps, well
    below a fast line, so parallel streams aggregate). Raises FileNotFoundError on
    a NOAA gap day so the caller can skip it. When `dl_progress` is given, drives a
    live per-file byte bar (Content-Length, or EXPECTED_ZIP_BYTES if absent)."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    dest = raw_dir / f"ais-{d.year}-{d.month:02d}-{d.day:02d}.csv.zst"
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            async with client.stream("GET", day_url(d)) as r:
                if r.status_code == 404:
                    raise FileNotFoundError(f"NOAA has no file for {d} ({day_url(d)})")
                r.raise_for_status()
                total = int(r.headers.get("content-length") or 0) or EXPECTED_ZIP_BYTES
                task = dl_progress.add_task(d.isoformat(), total=total) if dl_progress else None
                try:
                    # Re-opened in "wb" each attempt — a partial stream is truncated,
                    # so a retry restarts the day cleanly (no resume-within-file).
                    with dest.open("wb") as fh:
                        async for chunk in r.aiter_bytes(chunk_size=1 << 20):
                            fh.write(chunk)
                            if task is not None:
                                dl_progress.update(task, advance=len(chunk))
                finally:
                    if task is not None:
                        dl_progress.remove_task(task)
            return dest
        except FileNotFoundError:
            raise  # NOAA gap day — not transient; let the caller skip it
        except (httpx.TransportError, ssl.SSLError, httpx.HTTPStatusError) as e:
            # Retry transport/TLS drops and 5xx; surface a 4xx immediately.
            transient = (
                not isinstance(e, httpx.HTTPStatusError)
                or e.response.status_code >= 500
            )
            if not transient or attempt == DOWNLOAD_RETRIES:
                raise
            backoff = min(2**attempt, 30)
            logger.warning(
                "download %s failed (attempt %d/%d): %r — retrying in %ds",
                d, attempt, DOWNLOAD_RETRIES, e, backoff,
            )
            await asyncio.sleep(backoff)
    # Unreachable: the loop either returns dest or raises on the final attempt.
    raise RuntimeError(f"download {d}: exhausted retries without raising")


def read_tankers(path: Path, lng_imos: frozenset[int]) -> pd.DataFrame:
    """Read one daily csv2 file (zstd-compressed CSV), rename to our internal
    schema, and keep the **union** of (a) all typed tankers and (b) any known LNG
    carrier by IMO regardless of vessel_type.

    The union matters because early AIS (2016–2017) under-reports `vessel_type`:
    on a 2016 day ~46% of fixes carry no type at all and the registered LNG fleet
    is **100% untyped** (verified), so a `VesselType 80-89` filter alone drops the
    entire Sabine-era LNG signal. Matching the known fleet by IMO recovers it; the
    typed-tanker arm still feeds the density map + the future berth-sweep. Unknown
    LNG hulls (not yet in the registry) are still missed in untyped years — they
    get filled later by registry completion (PLAN §3.6.1)."""
    # engine='pyarrow' is multithreaded + releases the GIL during the read, so it
    # never stalls concurrent downloads (~3x faster than the C parser, same result).
    df = pd.read_csv(path, usecols=USECOLS, engine="pyarrow", compression="zstd")
    df = df.rename(columns=RENAME)
    df["fix_ts"] = pd.to_datetime(df["BaseDateTime"], utc=True)
    # Vectorised IMO parse ('IMO9830305' -> 9830305). A per-row .map(parse_imo) over
    # ~300k rows is a Python loop that HOLDS THE GIL in the reader thread, starving
    # the async download loop (frozen bars). These str ops are C-level / GIL-light.
    df["imo_int"] = pd.to_numeric(
        df["IMO"].astype("string").str.replace("IMO", "", regex=False).str.strip(),
        errors="coerce",
    ).astype("Int64")
    is_tanker = df["VesselType"].between(80, 89)
    is_known_lng = df["imo_int"].isin(lng_imos)
    df = df[is_tanker | is_known_lng].copy()
    # Draft of 0 means "unreported" (same sentinel rule as models.py).
    df.loc[df["Draft"] <= 0, "Draft"] = pd.NA
    return df


def write_archive(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = ["MMSI", "fix_ts", "LAT", "LON", "SOG", "COG", "VesselName",
            "imo_int", "VesselType", "Status", "Draft"]
    df[cols].to_parquet(path, compression="zstd", index=False)


def _read_and_archive(
    d: date, zip_path: Path, archive_dir: Path, lng_imos: frozenset[int]
) -> pd.DataFrame:
    """The CPU-bound half (decompress + parse + parquet write). Run in a worker
    thread so it never stalls concurrent downloads on the event loop; pyarrow's
    reader releases the GIL, so several of these genuinely run in parallel."""
    df = read_tankers(zip_path, lng_imos)
    write_archive(df, archive_path(d, archive_dir))
    return df


# ----------------------------------------------------------------------------- #
# Tier 2: LNG-carrier fixes -> ais_fixes (+ near-terminal draught -> vessel_state)
# ----------------------------------------------------------------------------- #
STAGE_DDL = """
CREATE TEMP TABLE noaa_stage (
    fix_ts TIMESTAMPTZ, mmsi BIGINT, lat DOUBLE PRECISION, lon DOUBLE PRECISION,
    sog REAL, cog REAL, nav_status SMALLINT, draught REAL
) ON COMMIT DROP
"""

# ALL LNG-carrier fixes (US-coastal — the staged set is already LNG-only, filtered
# in Python by IMO) go into ais_fixes, so the density map shows the full shipping
# lanes, not just blobs at the terminals. Mid-Gulf / approach fixes match no
# terminal polygon and produce NO port_events (the state machine resolves them to
# open-ocean TRANSIT) — they cost rebuild time but never change the signal. (Draught
# below stays near-terminal: it's only used for laden inference at berths.)
INSERT_FIXES = f"""
INSERT INTO ais_fixes (fix_ts, mmsi, lat, lon, sog, cog, nav_status, source)
SELECT s.fix_ts, s.mmsi, s.lat, s.lon, s.sog, s.cog, s.nav_status, '{NOAA_SOURCE}'
FROM noaa_stage s
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


LNG_IMOS_SQL = (
    "SELECT imo FROM vessel_registry WHERE is_lng_carrier AND imo IS NOT NULL"
)


async def fetch_lng_imos(pool: asyncpg.Pool) -> frozenset[int]:
    """The registered LNG-carrier IMO set — the identity key for both the archive
    union (read_tankers) and the Tier-2 ais_fixes selection. Fetched once per run."""
    async with pool.acquire() as conn:
        return frozenset(r["imo"] for r in await conn.fetch(LNG_IMOS_SQL))


async def load_tier2(
    pool: asyncpg.Pool, df: pd.DataFrame, lng_imos: frozenset[int]
) -> tuple[int, int]:
    """Stage the day's LNG-carrier fixes -> all into ais_fixes (full lanes for the
    density map), near-terminal draught -> vessel_state. Selected by registry IMO
    regardless of vessel_type (so untyped early-AIS LNG hulls are kept)."""
    cand = df[df["imo_int"].isin(lng_imos)]
    if cand.empty:
        return 0, 0
    async with pool.acquire() as conn:
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
            fixes = await conn.execute(INSERT_FIXES)  # all LNG fixes (no zone filter)
            state = await conn.execute(INSERT_STATE, list(US_ZONES))  # near-terminal draught
    # asyncpg returns e.g. "INSERT 0 4269" — pull the row count.
    return int(fixes.split()[-1]), int(state.split()[-1])


# ----------------------------------------------------------------------------- #
# Orchestration — bounded-concurrent download, one-at-a-time processing
# ----------------------------------------------------------------------------- #
DEFAULT_CONCURRENCY = 6  # parallel day downloads (run natively for full line speed)
MAX_CONCURRENT_READS = 3  # cap simultaneous CSV reads — each transiently holds the
#                           full 7M-row file in memory, so this bounds RAM separately
#                           from the (higher) download concurrency.


def _emit(console: Console | None, msg: str) -> None:
    """Per-day line: above the live bars when a progress display is active, else a
    plain log line (redirected output / single-file paths)."""
    if console is not None:
        console.log(msg)
    else:
        logger.info(msg)


async def _archive_and_load(
    pool: asyncpg.Pool | None,
    d: date,
    zip_path: Path,
    archive_dir: Path,
    read_sem: asyncio.Semaphore,
    lng_imos: frozenset[int],
) -> str:
    """Tier-1 archive (in a thread, memory-bounded by read_sem) + Tier-2 DB load.
    Returns the one-line summary for the caller to emit."""
    async with read_sem:
        df = await asyncio.to_thread(_read_and_archive, d, zip_path, archive_dir, lng_imos)
    msg = f"{d}: archived {len(df):,} tanker fixes"
    if pool is not None:
        fixes, state = await load_tier2(pool, df, lng_imos)
        msg += f" | tier-2: {fixes:,} ais_fixes + {state:,} vessel_state (LNG near US terminals)"
    return msg


async def _process_one(
    sem: asyncio.Semaphore,
    read_sem: asyncio.Semaphore,
    client: httpx.AsyncClient,
    pool: asyncpg.Pool | None,
    d: date,
    *,
    raw_dir: Path,
    archive_dir: Path,
    keep_zip: bool,
    force: bool,
    lng_imos: frozenset[int],
    overall: Progress | None = None,
    overall_task=None,
    downloads: Progress | None = None,
    console: Console | None = None,
) -> None:
    """One day: download (bounded by `sem`) -> archive -> Tier-2 -> delete the zip.
    `sem` bounds days *in flight*, so at most `sem._value` zips ever sit on disk —
    peak raw footprint stays ~N*270MB regardless of range length (PLAN.md §3.8).
    Advances the overall (days) bar exactly once, on every path."""
    try:
        arc = archive_path(d, archive_dir)
        if arc.exists() and not force:
            _emit(console, f"{d} already archived — skipping")
            return
        async with sem:
            try:
                zip_path = await _download(client, d, raw_dir, downloads)
            except FileNotFoundError as e:
                _emit(console, str(e))  # NOAA gap day — skip, keep going
                return
            try:
                _emit(console, await _archive_and_load(
                    pool, d, zip_path, archive_dir, read_sem, lng_imos))
            finally:
                if not keep_zip and zip_path.exists():
                    zip_path.unlink()
    except Exception as e:
        # One bad day (download retries exhausted, corrupt zip, DB blip) must not
        # abort the gather. Leave it un-archived — the next run's arc.exists() skip
        # resumes the range and retries exactly the days that didn't land.
        _emit(console, f"{d}: FAILED — {e!r} (left un-archived; rerun to retry)")
    finally:
        if overall is not None:
            overall.advance(overall_task)


def _fmt_duration(seconds: float) -> str:
    """Compact human duration: '9h42m', '7m18s', '48s' — deliberately *not* the
    H:MM:SS clock-time shape, so the ETA reads as 'time remaining', not 'time of day'."""
    s = int(seconds)
    if s >= 3600:
        return f"{s // 3600}h{(s % 3600) // 60:02d}m"
    if s >= 60:
        return f"{s // 60}m{s % 60:02d}s"
    return f"{s}s"


class ETAColumn(ProgressColumn):
    """ETA shown as both time-left and the projected wall-clock finish. Prefers
    rich's recent-rate estimate (`task.speed`, the responsive EMA used by the old
    TimeRemainingColumn — reflects NOAA's current throttled throughput), but falls
    back to the overall average rate (elapsed ÷ fraction done) whenever that EMA
    returns None — which is exactly when the old column blanked, as day completions
    clump. So the number stays the familiar recent-rate one yet never goes blank,
    and the wall-clock finish answers 'what time does it end?' directly."""

    def render(self, task) -> Text:
        if not task.total or not task.completed:
            return Text("estimating…", style="cyan")
        remaining_days = task.total - task.completed
        if task.speed:  # recent-rate EMA (days/sec) — preferred when populated
            remaining = remaining_days / task.speed
        elif task.elapsed:  # fall back to average rate so the field never blanks
            remaining = task.elapsed * remaining_days / task.completed
        else:
            return Text("estimating…", style="cyan")
        now = datetime.now()
        finish = now + timedelta(seconds=remaining)
        clock = finish.strftime("%H:%M" if finish.date() == now.date() else "%a %H:%M")
        return Text(f"{_fmt_duration(remaining)} left → finish {clock}", style="cyan")


class AggregateSpeedColumn(ProgressColumn):
    """Sum of transfer speed across the live per-download bars — the true aggregate
    line throughput. The days bar moves no bytes of its own, so without this the only
    speed shown is per-file; the parallel streams aggregate well above any single one."""

    def __init__(self, downloads: Progress) -> None:
        super().__init__()
        self._downloads = downloads

    def render(self, task) -> Text:
        speed = sum(t.speed or 0.0 for t in self._downloads.tasks)
        if speed <= 0:
            return Text("— MB/s", style="dim")
        return Text(f"{speed / 1e6:.1f} MB/s", style="bold green")


def _build_progress() -> tuple[Console, Progress, Progress]:
    """Overall days bar (average-rate ETA + aggregate throughput) above a set of live
    per-download byte bars with per-file transfer speed."""
    console = Console()
    downloads = Progress(
        TextColumn("  [dim]↓ {task.description}"),
        BarColumn(bar_width=26),
        DownloadColumn(),
        TransferSpeedColumn(),
        console=console,
    )
    overall = Progress(
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("days · {task.percentage:>3.0f}% ·"),
        ETAColumn(),
        TextColumn("·"),
        AggregateSpeedColumn(downloads),
        TextColumn("· elapsed"),
        TimeElapsedColumn(),
        console=console,
    )
    return console, overall, downloads


async def run_range(
    pool: asyncpg.Pool | None,
    start: date,
    end: date,
    *,
    concurrency: int = DEFAULT_CONCURRENCY,
    raw_dir: Path = RAW_DIR,
    archive_dir: Path = ARCHIVE_DIR,
    keep_zip: bool = False,
    force: bool = False,
) -> None:
    days = list(daterange(start, end))
    # Fetch the registered LNG IMO set once (the archive union + Tier-2 key). With
    # --no-db (pool=None) there's no registry to read, so the archive falls back to
    # typed-tankers-only — untyped LNG hulls need a DB to be recognised.
    lng_imos = await fetch_lng_imos(pool) if pool is not None else frozenset()
    sem = asyncio.Semaphore(concurrency)
    read_sem = asyncio.Semaphore(min(concurrency, MAX_CONCURRENT_READS))
    limits = httpx.Limits(
        max_connections=concurrency + 2, max_keepalive_connections=concurrency
    )
    timeout = httpx.Timeout(connect=30.0, read=300.0, write=30.0, pool=30.0)

    # Bars only on a real terminal; redirected output falls back to plain log lines.
    overall = downloads = console = overall_task = None
    live_cm: object = nullcontext()
    probe = Console()
    if probe.is_terminal:
        console, overall, downloads = _build_progress()
        overall_task = overall.add_task("backfill", total=len(days))
        live_cm = Live(Group(overall, downloads), console=console, refresh_per_second=8)

    # `live_cm` (Live, or nullcontext) is a SYNCHRONOUS context manager — enter it
    # with a plain `with` nested inside the async client (not `async with`).
    async with httpx.AsyncClient(
        follow_redirects=True, limits=limits, timeout=timeout
    ) as client:
        with live_cm:
            await asyncio.gather(
                *(
                    _process_one(
                        sem, read_sem, client, pool, d,
                        raw_dir=raw_dir, archive_dir=archive_dir,
                        keep_zip=keep_zip, force=force, lng_imos=lng_imos,
                        overall=overall, overall_task=overall_task,
                        downloads=downloads, console=console,
                    )
                    for d in days
                )
            )


async def process_local(
    pool: asyncpg.Pool | None, local_path: Path, archive_dir: Path, *, force: bool = False
) -> None:
    """Process an already-downloaded zip (no download, no delete) — dev/validation."""
    d = _date_from_name(local_path.name)
    arc = archive_path(d, archive_dir)
    if arc.exists() and not force:
        logger.info("%s already archived — skipping", d)
        return
    lng_imos = await fetch_lng_imos(pool) if pool is not None else frozenset()
    logger.info(
        await _archive_and_load(pool, d, local_path, archive_dir, asyncio.Semaphore(1), lng_imos)
    )


async def reload_archive(
    pool: asyncpg.Pool, start: date, end: date, archive_dir: Path = ARCHIVE_DIR
) -> None:
    """Re-run Tier-2 from the on-disk archive (no download) — for when the Tier-2
    filter changes (e.g. widening ais_fixes to all LNG fixes). Idempotent: existing
    rows are skipped (ON CONFLICT DO NOTHING), only the newly-admitted fixes land.
    The two-tier design's payoff (PLAN.md §3.8): re-scope ais_fixes without a
    1.1 TB re-download."""
    lng_imos = await fetch_lng_imos(pool)
    for d in daterange(start, end):
        arc = archive_path(d, archive_dir)
        if not arc.exists():
            continue
        df = await asyncio.to_thread(pd.read_parquet, arc)
        fixes, state = await load_tier2(pool, df, lng_imos)
        logger.info("%s: reloaded from archive -> +%d ais_fixes, +%d vessel_state",
                    d, fixes, state)


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
    ap.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY,
                    help=f"parallel day downloads (default {DEFAULT_CONCURRENCY}; "
                         "peak disk ~= N x 270MB. Run NATIVELY for full speed — the "
                         "agent sandbox throttles + breaks parallel TLS)")
    ap.add_argument("--archive-dir", default=str(ARCHIVE_DIR))
    ap.add_argument("--raw-dir", default=str(RAW_DIR))
    ap.add_argument("--keep-zip", action="store_true", help="don't delete the raw zip after processing")
    ap.add_argument("--force", action="store_true", help="re-archive even if the parquet exists")
    ap.add_argument("--no-db", action="store_true", help="Tier-1 archive only; skip ais_fixes load")
    ap.add_argument("--reload", action="store_true",
                    help="re-run Tier-2 from the on-disk archive over --start/--end "
                         "(no download) — e.g. after the Tier-2 filter changed")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # httpx/httpcore log a line per request at INFO — that's one per daily zip,
    # which clobbers the live progress bars. Quiet them to WARNING.
    for noisy in ("httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    archive_dir, raw_dir = Path(args.archive_dir), Path(args.raw_dir)
    pool = (
        None
        if args.no_db
        else await asyncpg.create_pool(
            config.settings.database_url,
            min_size=2,
            max_size=max(10, args.concurrency + 2),
        )
    )
    try:
        if args.reload:
            if not (args.start and args.end):
                ap.error("--reload needs --start/--end")
            await reload_archive(pool, _d(args.start), _d(args.end), archive_dir)
        elif args.local:
            await process_local(pool, Path(args.local), archive_dir, force=args.force)
        elif args.date:
            d = _d(args.date)
            await run_range(pool, d, d, concurrency=1, raw_dir=raw_dir,
                            archive_dir=archive_dir, keep_zip=args.keep_zip, force=args.force)
        elif args.start and args.end:
            await run_range(pool, _d(args.start), _d(args.end),
                            concurrency=args.concurrency, raw_dir=raw_dir,
                            archive_dir=archive_dir, keep_zip=args.keep_zip, force=args.force)
        else:
            ap.error("give --date, --start/--end, or --local")
    finally:
        if pool is not None:
            await pool.close()


def _d(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _date_from_name(name: str) -> date:
    # ais-2022-01-01.csv.zst (or legacy AIS_2022_01_01.zip) -> date(2022,1,1)
    m = re.search(r"(\d{4})[-_](\d{2})[-_](\d{2})", name)
    if not m:
        raise ValueError(f"cannot parse a date from {name!r}")
    return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))


if __name__ == "__main__":
    asyncio.run(main())
