"""GFW Events API historical backfill → port_events (source = 'gfw_events').

The EU side of the historical corpus. Free raw AIS (NOAA) reaches only US waters,
so for every voyage's *arrival* in Europe — and for the whole world before 2016 —
we lean on Global Fishing Watch's fused terrestrial+satellite PORT_VISIT events,
pulled over REST with the API token in `config.gfw_api_key`.

This single loader replaces the PLAN's separate Phase-2 (Voyages) and Phase-3
(Events) modules: the probe on 2026-06-12 found GFW exposes **no** REST voyages
endpoint, but the PORT_VISIT events endpoint serves everything we need
(start/end, lat/lon, anchorage id) over one dataset, no bulk-download Data Use
Agreement required. See memory `project_gfw_api_capability`.

────────────────────────────────────────────────────────────────────────────
How a GFW port visit becomes signal
────────────────────────────────────────────────────────────────────────────
GFW gives one row per *port visit*: a vessel sat at an anchorage from `start` to
`end`. We proximity-match that anchorage to our `terminal_zones` polygons (each
event carries its own lat/lon, so no GFW anchorage-id mapping is needed) and, for
a visit that lands at one of our 25 terminals, emit **three synthetic
`port_events` rows** that the *unchanged* pairing layer turns into signal:

    zone_entry @ start   → the ARRIVAL endpoint legs.py pairs the *incoming* leg to
    moored     @ start   → the berth occupancy visits.py turns into a load/discharge
    departed   @ end      → closes the visit AND is the DEPARTURE endpoint of the
                            *outgoing* leg (legs.py pairs it to the next zone_entry)

So a vessel's visit sequence A→B→C reconstructs as legs A→B and B→C purely from
the paired endpoints — we never download a voyage arc, we *rebuild* it from
consecutive visits, reusing legs.py / visits.py exactly as they stand.

`laden_flag` is set from the terminal's `flow_direction` (PLAN §3.3 — GFW has no
draught, so this is the only laden source available), and it is set to the
physically-correct value for each endpoint so the signal selectors fire:

    terminal flow │ moored (arrival) │ departed (leaving)
    ──────────────┼──────────────────┼───────────────────
    export  (US)  │ ballast (False)  │ laden   (True)   ← arrives empty, loads, leaves full
    import  (EU)  │ laden   (True)   │ ballast (False)  ← arrives full, discharges, leaves empty

  signal.py then reads:
    • gas_loading_us       ← export-terminal visits  (no laden filter)            → moored/departed here
    • gas_discharging_eu   ← import-terminal visits where moored is laden=True     → EU moored=True ✓
    • gas_in_transit_volume← laden legs export-origin → import-dest                → departed(US,True)→zone_entry(EU)
    • gas_ballast_to_us    ← ballast legs import-origin → export-dest              → departed(EU,False)→zone_entry(US)

────────────────────────────────────────────────────────────────────────────
How it interlocks with NOAA (and why reconcile.py exists)
────────────────────────────────────────────────────────────────────────────
NOAA owns the US side exhaustively (real fixes → real draught → real dwell/queue);
GFW owns the EU side (no free raw AIS there). They are **complementary halves of
one leg**: the headline `gas_in_transit_volume` for a US→EU cargo pairs NOAA's
draught-laden `departed` (US) with GFW's `zone_entry` (EU) — a fully-observed leg
that neither source could produce alone.

But this loader emits GFW events at US terminals too (to fill the days NOAA never
covered — pre-2016, NOAA gap days, the ~23 % NOAA misses). Where NOAA *did* see a
US visit, the GFW copy is a duplicate that would pair the leg twice and double the
volume. `reconcile.py` (run AFTER this loader and after every `make port-events`,
BEFORE `make signals`) deletes exactly those NOAA-covered GFW US visits, keeping
NOAA's superior events and any GFW US visit NOAA missed. See reconcile.py §3.7.

Identity is IMO-keyed (PLAN §3.6): we resolve each registry IMO → GFW vessel_id(s)
via vessel search, and attribute every event back to the registry MMSI (the one
weights join on). Caveat: a hull that changed MMSI between a historical event and
today won't cross-pair with its NOAA events under the old MMSI — bounded, and
absent in the validated 2022 corpus where MMSI is stable.

Usage:
    uv run python -m ingestion.historical.gfw_events --start 2017-01-01 --end 2025-12-31
    uv run python -m ingestion.historical.gfw_events --start 2022-01-01 --end 2022-12-31 --mmsi 538003350
    uv run python -m ingestion.historical.gfw_events --start 2022-01-01 --end 2022-03-31 --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import asyncpg
import httpx

import config

logger = logging.getLogger("gfw_events")

GFW_BASE = "https://gateway.api.globalfishingwatch.org/v3"
VESSEL_DATASET = "public-global-vessel-identity:latest"
PORTVISIT_DATASET = "public-global-port-visits-events:latest"
GFW_SOURCE = "gfw_events"

# A GFW port-visit anchorage within this many metres of one of our terminal_zones
# polygons is that terminal's visit. 25 km comfortably contains the sea-buoy
# anchorages off Sabine/Rotterdam etc. while staying well clear of the next port;
# validated on AL KHATTIYA (8/43 2022 visits matched, all real terminals).
TERMINAL_BUFFER_M = 25_000

# confidences=4 keeps only visits with BOTH a visible AIS entry and exit (PLAN
# §1.3) — drops AIS-dark partial visits we can't bracket. Quality over recall.
VISIT_CONFIDENCE = 4

# Batch sizes. Rate limits are generous (50k req/day, measured), so these are for
# throughput, not throttling. The events endpoint accepts many vessels per call
# and attributes each event back via event.vessel.id.
VESSELS_PER_EVENTS_CALL = 50
EVENTS_PAGE = 200
SEARCH_CONCURRENCY = 8


# ────────────────────────────────────────────────────────────────────────────
# Pure: a port visit → three synthetic port_events rows
# ────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class PortVisit:
    """One GFW PORT_VISIT, after attribution + terminal match."""

    mmsi: int
    start: datetime
    end: datetime | None  # None ⇒ open/ongoing visit (no observed exit) — rare in history
    terminal_id: int
    zone: str
    flow_direction: str | None  # 'export' | 'import'


def _arrival_laden(flow_direction: str | None) -> bool | None:
    """Laden state of a vessel *arriving* at a terminal (the moored/zone_entry
    endpoint): laden at an import terminal (came to discharge), ballast at an
    export terminal (came to load)."""
    if flow_direction == "import":
        return True
    if flow_direction == "export":
        return False
    return None


def _departure_laden(flow_direction: str | None) -> bool | None:
    """Laden state of a vessel *leaving* a terminal (the departed endpoint): laden
    off an export terminal (just loaded), ballast off an import terminal (just
    discharged). This is the value legs.py reads for the outgoing leg's direction."""
    if flow_direction == "export":
        return True
    if flow_direction == "import":
        return False
    return None


# port_events column order (matches pipeline/port_events.py INSERT_SQL):
# (mmsi, event_type, zone, terminal_id, event_time, lat, lon,
#  laden_flag, laden_source, cold_start, source)
def visit_to_events(visit: PortVisit) -> list[tuple]:
    """Emit the synthetic (zone_entry, moored, departed) rows for one visit.

    zone_entry + moored fire at `start` (arrival/berthing — GFW gives no separate
    berthing time, the visit interval is all we have); departed fires at `end`. An
    open visit (no `end`) emits only zone_entry + moored — visits.py leaves it open
    and the signal layer caps it, same as a live open visit. `lat`/`lon` are left
    NULL: GFW's representative position is the anchorage, not the leg endpoint, and
    legs.py only computes great-circle distance for *closed* legs (both endpoints
    state-machine/NOAA-sourced), which a GFW endpoint never is. cold_start=True
    marks the synthetic provenance (PLAN §1.3)."""
    arr = _arrival_laden(visit.flow_direction)
    rows = [
        (visit.mmsi, "zone_entry", visit.zone, visit.terminal_id, visit.start,
         None, None, arr, "flow_direction", True, GFW_SOURCE),
        (visit.mmsi, "moored", visit.zone, visit.terminal_id, visit.start,
         None, None, arr, "flow_direction", True, GFW_SOURCE),
    ]
    if visit.end is not None:
        rows.append(
            (visit.mmsi, "departed", visit.zone, visit.terminal_id, visit.end,
             None, None, _departure_laden(visit.flow_direction),
             "flow_direction", True, GFW_SOURCE)
        )
    return rows


# ────────────────────────────────────────────────────────────────────────────
# GFW REST client
# ────────────────────────────────────────────────────────────────────────────
def _array_params(key: str, values: list[str]) -> list[tuple[str, str]]:
    """GFW encodes array query params as key[0]=, key[1]=, … — build that list."""
    return [(f"{key}[{i}]", v) for i, v in enumerate(values)]


async def _get(client: httpx.AsyncClient, path: str, params: list[tuple[str, str]]) -> dict:
    r = await client.get(f"{GFW_BASE}{path}", params=params)
    r.raise_for_status()
    return r.json()


async def resolve_vessel_id(
    client: httpx.AsyncClient, imo: int, mmsi: int, sem: asyncio.Semaphore
) -> list[tuple[str, int]]:
    """Search GFW for one IMO → list of (vessel_id, mmsi) to query events for.

    A hull can have several `selfReportedInfo` ids (one per MMSI it has broadcast);
    we take them all and attribute each back to the *registry* MMSI (IMO-keyed
    identity, PLAN §3.6). Verifies the entry's IMO matches so a fuzzy search hit on
    another vessel is rejected."""
    # NB: /vessels/search takes `limit` but REJECTS `offset` ("property offset
    # should not exist") — unlike /events, which requires both. GFW inconsistency.
    params = _array_params("datasets", [VESSEL_DATASET]) + [
        ("query", str(imo)), ("limit", "5")
    ]
    async with sem:
        try:
            data = await _get(client, "/vessels/search", params)
        except httpx.HTTPStatusError as e:
            logger.warning("search imo=%s failed: %s", imo, e)
            return []
    out: list[tuple[str, int]] = []
    for entry in data.get("entries", []):
        reg = entry.get("registryInfo") or []
        sri = entry.get("selfReportedInfo") or []
        entry_imos = {str(r.get("imo")) for r in reg} | {str(s.get("imo")) for s in sri}
        if str(imo) not in entry_imos:
            continue  # search matched on something other than this IMO — skip
        for s in sri:
            vid = s.get("id")
            if vid:
                out.append((vid, mmsi))
    return out


async def fetch_visits(
    client: httpx.AsyncClient,
    vessel_ids: list[str],
    start: date,
    end: date,
) -> list[dict]:
    """Page all PORT_VISIT events for a batch of vessel_ids over [start, end]."""
    base = (
        _array_params("datasets", [PORTVISIT_DATASET])
        + _array_params("vessels", vessel_ids)
        + _array_params("confidences", [str(VISIT_CONFIDENCE)])
        + [("start-date", start.isoformat()), ("end-date", end.isoformat())]
    )
    events: list[dict] = []
    offset = 0
    while True:
        page = await _get(
            client, "/events", base + [("limit", str(EVENTS_PAGE)), ("offset", str(offset))]
        )
        entries = page.get("entries", [])
        events.extend(entries)
        if len(entries) < EVENTS_PAGE:
            break
        offset += EVENTS_PAGE
    return events


# ────────────────────────────────────────────────────────────────────────────
# Terminal proximity match (open Q#2: against terminal_zones, not terminals)
# ────────────────────────────────────────────────────────────────────────────
MATCH_SQL = """
WITH stage(idx, lat, lon) AS (
    SELECT * FROM unnest($1::int[], $2::float8[], $3::float8[])
)
SELECT s.idx, m.terminal_id, t.zone, t.flow_direction
FROM stage s
CROSS JOIN LATERAL (
    SELECT tz.terminal_id,
           ST_Distance(
               ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)::geography,
               tz.geom::geography) AS dist
    FROM terminal_zones tz
    ORDER BY ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326) <-> tz.geom
    LIMIT 1
) m
JOIN terminals t ON t.terminal_id = m.terminal_id
WHERE m.dist < $4
"""


async def match_terminals(
    pool: asyncpg.Pool, points: list[tuple[float, float]]
) -> dict[int, tuple[int, str, str | None]]:
    """idx → (terminal_id, zone, flow_direction) for points within the buffer of a
    terminal_zones polygon. Each event carries its own lat/lon, so this is a direct
    nearest-polygon lookup — no GFW anchorage-id table needed."""
    if not points:
        return {}
    idx = list(range(len(points)))
    lats = [p[0] for p in points]
    lons = [p[1] for p in points]
    async with pool.acquire() as conn:
        rows = await conn.fetch(MATCH_SQL, idx, lats, lons, float(TERMINAL_BUFFER_M))
    return {r["idx"]: (r["terminal_id"], r["zone"], r["flow_direction"]) for r in rows}


# ────────────────────────────────────────────────────────────────────────────
# DB I/O
# ────────────────────────────────────────────────────────────────────────────
REGISTRY_SQL = """
SELECT mmsi, imo FROM vessel_registry
WHERE is_lng_carrier = TRUE AND imo IS NOT NULL AND imo > 0
"""

# Idempotent reload: clear this loader's rows in the window, then insert. Bounded
# to [start, end] so a single-year rerun doesn't disturb other years' GFW rows.
CLEAR_SQL = """
DELETE FROM port_events
WHERE source = 'gfw_events' AND event_time >= $1 AND event_time < $2
"""

INSERT_SQL = """
INSERT INTO port_events
    (mmsi, event_type, zone, terminal_id, event_time, lat, lon,
     laden_flag, laden_source, cold_start, source)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
"""


def _parse_ts(s: str | None) -> datetime | None:
    if not s:
        return None
    # GFW timestamps look like '2022-01-06T10:09:18.000Z'.
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


async def run(
    pool: asyncpg.Pool,
    start: date,
    end: date,
    *,
    only_mmsi: int | None = None,
    dry_run: bool = False,
) -> None:
    async with pool.acquire() as conn:
        reg = await conn.fetch(REGISTRY_SQL)
    fleet = [(r["mmsi"], r["imo"]) for r in reg]
    if only_mmsi is not None:
        fleet = [(m, i) for m, i in fleet if m == only_mmsi]
    if not fleet:
        logger.warning("No in-scope LNG carriers with an IMO (only_mmsi=%s).", only_mmsi)
        return
    logger.info("Resolving %d hulls IMO → GFW vessel_id …", len(fleet))

    timeout = httpx.Timeout(connect=30.0, read=120.0, write=30.0, pool=30.0)
    headers = {"Authorization": f"Bearer {config.settings.gfw_api_key}"}
    async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
        sem = asyncio.Semaphore(SEARCH_CONCURRENCY)
        resolved = await asyncio.gather(
            *(resolve_vessel_id(client, imo, mmsi, sem) for mmsi, imo in fleet)
        )
        vid_mmsi: dict[str, int] = {}
        for pairs in resolved:
            for vid, mmsi in pairs:
                vid_mmsi[vid] = mmsi
        logger.info("Resolved %d vessel_ids for %d hulls.", len(vid_mmsi), len(fleet))

        # Fetch port visits in vessel batches, attribute each via event.vessel.id.
        all_vids = list(vid_mmsi)
        raw: list[dict] = []
        for i in range(0, len(all_vids), VESSELS_PER_EVENTS_CALL):
            batch = all_vids[i : i + VESSELS_PER_EVENTS_CALL]
            raw.extend(await fetch_visits(client, batch, start, end))
        logger.info("Fetched %d raw PORT_VISIT events.", len(raw))

    # Attribute + collect candidate points for the spatial match.
    cand: list[tuple[int, datetime, datetime | None, float, float]] = []
    for e in raw:
        vid = (e.get("vessel") or {}).get("id")
        mmsi = vid_mmsi.get(vid)
        pos = e.get("position") or {}
        lat, lon = pos.get("lat"), pos.get("lon")
        s = _parse_ts(e.get("start"))
        if mmsi is None or lat is None or lon is None or s is None:
            continue
        cand.append((mmsi, s, _parse_ts(e.get("end")), lat, lon))

    matched = await match_terminals(pool, [(c[3], c[4]) for c in cand])
    visits = [
        PortVisit(mmsi=cand[idx][0], start=cand[idx][1], end=cand[idx][2],
                  terminal_id=tid, zone=zone, flow_direction=flow)
        for idx, (tid, zone, flow) in matched.items()
    ]
    logger.info(
        "%d/%d visits matched a terminal (<%d km); %d at US export, %d at EU import.",
        len(visits), len(cand), TERMINAL_BUFFER_M // 1000,
        sum(1 for v in visits if v.flow_direction == "export"),
        sum(1 for v in visits if v.flow_direction == "import"),
    )

    rows = [row for v in visits for row in visit_to_events(v)]
    if dry_run:
        logger.info("[dry-run] would write %d synthetic port_events rows.", len(rows))
        return

    win_lo = datetime(start.year, start.month, start.day, tzinfo=timezone.utc)
    win_hi = datetime(end.year, end.month, end.day, tzinfo=timezone.utc) + timedelta(days=1)
    async with pool.acquire() as conn:
        async with conn.transaction():
            cleared = await conn.execute(CLEAR_SQL, win_lo, win_hi)
            if rows:
                await conn.executemany(INSERT_SQL, rows)
    logger.info("Wrote %d port_events rows (cleared: %s). Run reconcile.py next.",
                len(rows), cleared)


async def main() -> None:
    ap = argparse.ArgumentParser(description="GFW PORT_VISIT events → port_events backfill.")
    ap.add_argument("--start", required=True, help="range start YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="range end YYYY-MM-DD (inclusive)")
    ap.add_argument("--mmsi", type=int, help="restrict to one registry MMSI (dev/validation)")
    ap.add_argument("--dry-run", action="store_true", help="fetch + match, no DB write")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for noisy in ("httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    if not config.settings.gfw_api_key:
        ap.error("GFW_API_KEY is empty — set it in .env")

    pool = await asyncpg.create_pool(config.settings.database_url, min_size=2, max_size=6)
    try:
        await run(pool, _d(args.start), _d(args.end), only_mmsi=args.mmsi, dry_run=args.dry_run)
    finally:
        await pool.close()


def _d(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


if __name__ == "__main__":
    asyncio.run(main())
