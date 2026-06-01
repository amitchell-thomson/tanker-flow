"""VesselFinder live-position rescue worker (signal-framed).

The laden-ton-miles signal is built from port *events*: a laden `departed` from an
export terminal opens a leg, a `zone_entry`/`moored` at an import terminal closes
it. We never need a vessel's mid-ocean position — only that we capture those two
endpoint events. AISstream occasionally drops a vessel in the final approach
(e.g. FEDOR LITKE going silent berthing at Dunkerque), which loses the event and
biases the signal.

This worker is the backstop for exactly that: a vessel that is **at or
approaching one of our terminals** (so it's in coastal waters), is AIS-silent
within an actionable band, and has a port event pending. It fetches the vessel's
current position from VesselFinder's `/vessels` feed (terrestrial — 1 credit; the
vessel is coastal so satellite is never needed) and injects it as a normal
`ais_fixes` row (source='vesselfinder'). Everything downstream re-acquires it for
free — `port_events` emits the missing entry/moored/departed, scoring re-tiers it.
No special-casing anywhere else.

What it does NOT do: track vessels mid-crossing (no event at risk), or rescue
long-stale vessels (the event has already passed). Both are deliberately excluded
by the near-terminal geometry and the staleness ceiling.

VF credits are a finite, non-renewing reserve, so `vf_rescue_log` is both the
audit trail and the restart-safe ledger: today's SUM(credits) gates the daily
cap, a per-mmsi recency check is the cooldown. Expected spend is ~1-3 credits/day.

Run as a background task in ingestion/aisstream.py, or manually via
`make vf-rescue` (`--dry-run` for a no-spend preview, `--mmsi N` for a one-off).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import asyncpg
import httpx
from rich.logging import RichHandler

from config import settings
from pipeline import legs as legs_module
from pipeline.geo import haversine_nm

from .models import VesselFinderAIS, VesselFinderLiveResponse

logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])
logger = logging.getLogger(__name__)

VF_LIVE_API_BASE = "https://api.vesselfinder.com/vessels"
# Free account-balance endpoint (returns no position ⇒ no credit charge).
VF_STATUS_API_BASE = "https://api.vesselfinder.com/status"
RATE_LIMIT_DELAY = 1.0  # seconds between VF requests (mirrors vesselfinder.py)

# --- Budget (credits, not calls) ----------------------------------------------
# Every rescue targets a coastal vessel ⇒ terrestrial ⇒ 1 credit. Expected spend
# is ~1-3 credits/day, so this cap is a safety ceiling, not a target: it bounds a
# pathological day (e.g. clearing a backlog) and the per-vessel cooldown does the
# rest. Read from the vf_rescue_log ledger at the top of every run so a crash-loop
# can't bypass it. 6000-credit reserve ÷ realistic spend ⇒ multi-year runway.
DAILY_CREDIT_CAP = 20
TER_COST = 1
SAT_COST = 10  # defensive only — we never request satellite (sat=0); 1cr in practice
# Best-estimate of the VF credit reserve when this worker started logging — used
# only for the TUI "remaining" runway readout (= reserve − SUM(credits) logged).
CREDIT_RESERVE_ESTIMATE = 6000
# Cooldown after a poll. The default (settled / no_position / rejected) is long;
# a vessel caught still *moving* in its approach is re-polled much sooner so we
# capture the actual entry/moored event rather than waiting a full cycle (#2).
PER_VESSEL_COOLDOWN_HOURS = 12
RECHECK_MOVING_HOURS = 2
MAX_CANDIDATES_PER_RUN = 30  # bounds worst-case spend/run; budget trims further
IMO_BATCH_SIZE = 20  # IMOs per GET (batching supported)

# --- Cadence ------------------------------------------------------------------
VF_RESCUE_INTERVAL_SECONDS = 1800  # 30 min — responsive enough to catch a vessel
# in its approach/berth window before the event is irretrievably mis-timed.

# --- Trigger geometry + freshness band ----------------------------------------
# Base inclusion: last fix within NEAR_KM of a terminal zone (coastal ⇒
# terrestrially visible ⇒ a port event is plausibly imminent). ~25 km ≈ 13 nm.
NEAR_KM = 25.0
# #3 — extend inclusion to vessels NEAR_KM..CLOSING_INCLUDE_KM out *if they are
# heading at the terminal* (cog within CLOSING_ANGLE_DEG of the bearing to it).
# This catches the "went silent 40 km out, arrived during the gap" case that a
# pure-proximity gate misses, without pulling in outbound/loitering vessels.
CLOSING_INCLUDE_KM = 50.0
CLOSING_ANGLE_DEG = 60.0
# Silence band. Below MIN: give AIS a chance to self-resume (most gaps are short).
# Above CEILING: the event has almost certainly already passed — a late poll
# can't recover its timing. The ceiling is the single biggest cost-saver vs an
# unbounded "longest-silent-first" design.
MIN_SILENCE_HOURS = 4
STALE_CEILING_HOURS = 48
# #3 — a vessel already in/at the approach envelope (or clearly closing) is one
# fix from a signal event, so trigger it on a much shorter silence than the
# general band — we can't afford to wait the full MIN_SILENCE for these.
FINAL_APPROACH_KM = 15.0
FINAL_APPROACH_SILENCE_HOURS = 2

# --- Sanity gates on the returned position ------------------------------------
VF_INTERVAL_MINUTES = 60  # server-side max age of returned positions
MAX_POSITION_AGE_HOURS = 3  # client-side freshness belt-and-suspenders
MAX_TELEPORT_KN = 32.0  # reject identity/teleport errors (LNG cruise ~19 kn)

# Lookback for "is there an open port visit" — mirror the pin window in scoring.
EVENT_LOOKBACK_DAYS = 20

# --- Trigger #4: destination capture at departure -----------------------------
# A laden departure with no destination we can resolve gets one coastal poll
# (VF often has a normalised LOCODE, or the dest our subscription missed) while
# the vessel is still in range. Fills the O-D censor window + signals #3/#5/#27/
# #31/#44. Not silence-driven — fires whether or not the vessel is dark.
DEST_CAPTURE_WINDOW_HOURS = 48

# --- Trigger #5: outage confirmation ------------------------------------------
# A terminal with no departed/moored for OUTAGE_DAYS, but active in the recent
# past, is a suspected outage (#36-#38). We poll the vessels we last saw there to
# confirm real-stoppage vs an AIS coverage gap before it drives the signal —
# deliberately reaching past the normal silence ceiling (the whole point is the
# longer-silent vessels #1-#3 skip).
OUTAGE_DAYS = 7
OUTAGE_DORMANT_DAYS = 60  # terminal must have been active within this to count
OUTAGE_VESSEL_MIN_SILENT_HOURS = 36  # only poll vessels actually gone quiet
OUTAGE_VESSEL_MAX_SILENT_DAYS = 21  # ...but not hopelessly stale
OUTAGE_MAX_VESSELS = 5  # cap polls per suspected-outage sweep

# Rescue classes, by the event at risk. import_arrival / export_departure protect
# the leg-defining events (prevent in-transit over/under-count); outage_check
# guards the high-leverage outage signals — all rank highest. dest_capture and
# import_berth (moored / queue timing) are next; export_arrival (ballast
# approaching to load) is lowest. 'manual' (operator override) jumps the queue.
CLASS_PRIORITY = {
    "manual": -1,
    "import_arrival": 0,
    "export_departure": 0,
    "outage_check": 0,
    "dest_capture": 1,
    "import_berth": 1,
    "floating_check": 1,
    "export_arrival": 2,
}


# Candidate set: in-scope vessels whose last fix is near a terminal (or that have
# an open port visit), not in cooldown, with the geometry + last-event signals the
# Python classifier needs. Staleness band + class assignment live in
# classify_candidate so they stay unit-testable.
CANDIDATE_SQL = """
WITH fleet AS (
    -- Carriers only: FSRUs are permanent installations moored at their own
    -- terminals (their last port_event is always `moored`), so they'd look like
    -- perpetual import_berth candidates. They generate no cargo legs — the
    -- carriers *delivering* to them are separate vessels and stay in scope.
    SELECT mmsi, imo, vessel_name FROM vessel_registry
    WHERE is_lng_carrier AND NOT is_fsru AND NOT excluded
      AND imo IS NOT NULL AND imo <> 0
),
last_pos AS (
    SELECT DISTINCT ON (a.mmsi)
        a.mmsi, a.fix_ts AS last_fix_ts, a.lat AS last_lat, a.lon AS last_lon, a.cog
    FROM ais_fixes a
    WHERE EXISTS (SELECT 1 FROM fleet f WHERE f.mmsi = a.mmsi)
    ORDER BY a.mmsi, a.fix_ts DESC
),
nearest AS (
    SELECT lp.mmsi, lp.last_fix_ts, lp.last_lat, lp.last_lon, lp.cog AS last_cog,
           n.flow_direction AS near_flow, n.dist_km AS near_km,
           -- bearing from the vessel to the nearest zone centroid (0-360),
           -- compared to cog to decide if it is closing on the terminal (#3).
           degrees(ST_Azimuth(
               ST_SetSRID(ST_Point(lp.last_lon, lp.last_lat), 4326)::geography,
               n.centroid::geography
           )) AS bearing_deg
    FROM last_pos lp
    CROSS JOIN LATERAL (
        SELECT t.flow_direction, ST_Centroid(tz.geom) AS centroid,
               ST_Distance(
                   ST_SetSRID(ST_Point(lp.last_lon, lp.last_lat), 4326)::geography,
                   tz.geom::geography
               ) / 1000.0 AS dist_km
        FROM terminal_zones tz JOIN terminals t USING (terminal_id)
        ORDER BY ST_SetSRID(ST_Point(lp.last_lon, lp.last_lat), 4326) <-> tz.geom
        LIMIT 1
    ) n
),
last_event AS (
    -- id DESC breaks ties when a cold-start cluster emits several events at one
    -- timestamp (e.g. zone_entry..zone_exit in a single fix gap). Rows are
    -- inserted in DFA order, so the highest id is the most-final state — without
    -- this, DISTINCT ON could pick the cluster's `zone_entry` and make a
    -- long-departed vessel look like an open visit.
    SELECT DISTINCT ON (pe.mmsi) pe.mmsi, pe.event_type, t.flow_direction AS ev_flow
    FROM port_events pe LEFT JOIN terminals t ON t.terminal_id = pe.terminal_id
    WHERE pe.event_time > now() - make_interval(days => $1)
    ORDER BY pe.mmsi, pe.event_time DESC, pe.id DESC
),
-- Cooldown: a vessel is excluded while the latest log row's recheck_at is still
-- in the future (variable per the moving/settled outcome of that poll, #2).
recent_cooldown AS (
    SELECT mmsi FROM (
        SELECT DISTINCT ON (mmsi) mmsi, recheck_at
        FROM vf_rescue_log ORDER BY mmsi, requested_at DESC
    ) latest
    WHERE recheck_at IS NOT NULL AND recheck_at > now()
)
SELECT
    n.mmsi, f.imo, f.vessel_name,
    n.last_fix_ts, n.last_lat, n.last_lon, n.near_flow, n.near_km,
    n.last_cog, n.bearing_deg,
    le.event_type AS last_event_type, le.ev_flow AS last_event_flow
FROM nearest n
JOIN fleet f USING (mmsi)
LEFT JOIN last_event le USING (mmsi)
WHERE n.mmsi NOT IN (SELECT mmsi FROM recent_cooldown)
  AND (
      n.near_km < $2
      OR (le.event_type IS NOT NULL AND le.event_type NOT IN ('departed', 'zone_exit'))
  )
"""

MANUAL_CANDIDATE_SQL = """
SELECT
    v.mmsi, v.imo, v.vessel_name,
    lp.last_fix_ts, lp.last_lat, lp.last_lon
FROM vessel_registry v
LEFT JOIN LATERAL (
    SELECT a.fix_ts AS last_fix_ts, a.lat AS last_lat, a.lon AS last_lon
    FROM ais_fixes a WHERE a.mmsi = v.mmsi ORDER BY a.fix_ts DESC LIMIT 1
) lp ON TRUE
WHERE v.mmsi = $1 AND v.imo IS NOT NULL AND v.imo <> 0
"""

# #4 — laden vessels that departed an export terminal in the last
# DEST_CAPTURE_WINDOW_HOURS for which we still have NO destination broadcast
# (none captured around departure). One coastal poll fills it.
DEST_CANDIDATE_SQL = """
WITH fleet AS (
    SELECT mmsi, imo, vessel_name FROM vessel_registry
    WHERE is_lng_carrier AND NOT is_fsru AND NOT excluded
      AND imo IS NOT NULL AND imo <> 0
),
recent_laden_dep AS (
    SELECT pe.mmsi, max(pe.event_time) AS dep_ts
    FROM port_events pe
    WHERE pe.event_type = 'departed' AND pe.laden_flag = TRUE
      AND pe.event_time > now() - make_interval(hours => $1)
    GROUP BY pe.mmsi
),
last_pos AS (
    SELECT DISTINCT ON (a.mmsi) a.mmsi, a.fix_ts AS last_fix_ts,
           a.lat AS last_lat, a.lon AS last_lon
    FROM ais_fixes a
    WHERE EXISTS (SELECT 1 FROM recent_laden_dep r WHERE r.mmsi = a.mmsi)
    ORDER BY a.mmsi, a.fix_ts DESC
),
recent_cooldown AS (
    SELECT mmsi FROM (
        SELECT DISTINCT ON (mmsi) mmsi, recheck_at
        FROM vf_rescue_log ORDER BY mmsi, requested_at DESC
    ) latest WHERE recheck_at IS NOT NULL AND recheck_at > now()
)
SELECT f.mmsi, f.imo, f.vessel_name, lp.last_fix_ts, lp.last_lat, lp.last_lon
FROM recent_laden_dep d
JOIN fleet f USING (mmsi)
JOIN last_pos lp USING (mmsi)
WHERE f.mmsi NOT IN (SELECT mmsi FROM recent_cooldown)
  AND NOT EXISTS (
      SELECT 1 FROM vessel_state vs
      WHERE vs.mmsi = d.mmsi AND vs.dest IS NOT NULL AND vs.dest <> ''
        AND vs.state_ts > d.dep_ts - INTERVAL '2 days'
  )
"""

# #5 — vessels last seen at a terminal that has gone quiet (no departed/moored
# for OUTAGE_DAYS but active in the recent past), now silent. Polling them
# confirms a real outage vs an AIS gap before #36-#38 fire.
OUTAGE_CANDIDATE_SQL = """
WITH fleet AS (
    SELECT mmsi, imo, vessel_name FROM vessel_registry
    WHERE is_lng_carrier AND NOT is_fsru AND NOT excluded
      AND imo IS NOT NULL AND imo <> 0
),
suspected_outage AS (
    SELECT pe.terminal_id
    FROM port_events pe
    WHERE pe.event_type IN ('departed', 'moored') AND pe.terminal_id IS NOT NULL
    GROUP BY pe.terminal_id
    HAVING max(pe.event_time) < now() - make_interval(days => $1)
       AND max(pe.event_time) > now() - make_interval(days => $2)
),
visitor AS (
    SELECT DISTINCT ON (pe.mmsi) pe.mmsi, pe.event_time AS visit_ts
    FROM port_events pe
    WHERE pe.terminal_id IN (SELECT terminal_id FROM suspected_outage)
      AND pe.event_time > now() - make_interval(days => $2)
    ORDER BY pe.mmsi, pe.event_time DESC
),
last_pos AS (
    SELECT DISTINCT ON (a.mmsi) a.mmsi, a.fix_ts AS last_fix_ts,
           a.lat AS last_lat, a.lon AS last_lon
    FROM ais_fixes a
    WHERE EXISTS (SELECT 1 FROM visitor v WHERE v.mmsi = a.mmsi)
    ORDER BY a.mmsi, a.fix_ts DESC
),
recent_cooldown AS (
    SELECT mmsi FROM (
        SELECT DISTINCT ON (mmsi) mmsi, recheck_at
        FROM vf_rescue_log ORDER BY mmsi, requested_at DESC
    ) latest WHERE recheck_at IS NOT NULL AND recheck_at > now()
)
SELECT f.mmsi, f.imo, f.vessel_name, lp.last_fix_ts, lp.last_lat, lp.last_lon
FROM visitor vi
JOIN fleet f USING (mmsi)
JOIN last_pos lp USING (mmsi)
WHERE f.mmsi NOT IN (SELECT mmsi FROM recent_cooldown)
  AND lp.last_fix_ts < now() - make_interval(hours => $3)
  AND lp.last_fix_ts > now() - make_interval(days => $4)
ORDER BY vi.visit_ts DESC
LIMIT $5
"""

LOG_SQL = """
INSERT INTO vf_rescue_log (
    mmsi, imo, vessel_name, rescue_class, sat, src, result,
    credits, requested_imos, returned_rows, fix_ts, detail, recheck_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
"""

BUDGET_SQL = """
SELECT COALESCE(SUM(credits), 0) AS spent
FROM vf_rescue_log
WHERE requested_at >= date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
"""


# --------------------------------------------------------------------------- #
# Pure helpers (no I/O — unit-tested in tests/test_vf_rescue.py)
# --------------------------------------------------------------------------- #
@dataclass
class Candidate:
    mmsi: int
    imo: int
    vessel_name: str | None
    last_fix_ts: datetime | None
    last_lat: float | None
    last_lon: float | None
    rescue_class: str
    silent_h: float


def parse_vf_timestamp(ts: str | None) -> datetime | None:
    """VF `"2017-08-11 11:15:15 UTC"` (or without the suffix) → tz-aware UTC."""
    if not ts:
        return None
    s = ts.strip()
    if s.endswith("UTC"):
        s = s[:-3].strip()
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def vf_eta_to_ais_dict(eta_str: str | None) -> str | None:
    """VF ETA timestamp → the AIS `{"Month","Day","Hour","Minute"}` JSON string
    that scoring._parse_eta consumes (year/seconds dropped; _parse_eta re-infers
    the year). None on empty / "0000-..." / unparseable."""
    if not eta_str or eta_str.strip().startswith("0000"):
        return None
    dt = parse_vf_timestamp(eta_str)
    if dt is None:
        return None
    return json.dumps(
        {"Month": dt.month, "Day": dt.day, "Hour": dt.hour, "Minute": dt.minute}
    )


def row_credits(src: str | None) -> int:
    return SAT_COST if src == "SAT" else TER_COST


def credits_for_rows(rows: list[VesselFinderAIS]) -> int:
    return sum(row_credits(r.SRC) for r in rows)


def position_sanity(
    *,
    vf_fix_ts: datetime | None,
    vf_lat: float,
    vf_lon: float,
    last_fix_ts: datetime | None,
    last_lat: float | None,
    last_lon: float | None,
    now: datetime,
) -> str:
    """Return 'ok' | 'rejected_stale' | 'rejected_teleport' for a VF position."""
    if vf_fix_ts is None:
        return "rejected_stale"
    if (now - vf_fix_ts).total_seconds() / 3600.0 > MAX_POSITION_AGE_HOURS:
        return "rejected_stale"
    if last_fix_ts is not None and vf_fix_ts <= last_fix_ts:
        # Not newer than what we already have — injecting it is at best a no-op
        # and at worst could re-open a closed envelope. Skip.
        return "rejected_stale"
    if last_fix_ts is not None and last_lat is not None and last_lon is not None:
        elapsed_h = (vf_fix_ts - last_fix_ts).total_seconds() / 3600.0
        if elapsed_h > 0:
            nm = haversine_nm(last_lat, last_lon, vf_lat, vf_lon)
            if nm / elapsed_h > MAX_TELEPORT_KN:
                return "rejected_teleport"
    return "ok"


def is_closing(
    last_cog: float | None, bearing_deg: float | None, near_km: float | None
) -> bool:
    """True if the vessel is within CLOSING_INCLUDE_KM and heading at the nearest
    terminal (cog within CLOSING_ANGLE_DEG of the bearing to it). Needs cog —
    which is sparse on older fixes, so this only *adds* candidates, never removes
    them (the NEAR_KM proximity gate stands on its own)."""
    if last_cog is None or bearing_deg is None or near_km is None:
        return False
    if near_km > CLOSING_INCLUDE_KM:
        return False
    diff = abs((last_cog - bearing_deg + 180.0) % 360.0 - 180.0)
    return diff <= CLOSING_ANGLE_DEG


def classify_candidate(
    *,
    mmsi: int,
    imo: int,
    vessel_name: str | None,
    last_fix_ts: datetime | None,
    last_lat: float | None,
    last_lon: float | None,
    near_flow: str | None,
    near_km: float | None,
    last_cog: float | None,
    bearing_deg: float | None,
    last_event_type: str | None,
    last_event_flow: str | None,
    now: datetime,
) -> Candidate | None:
    """Assign a rescue class from the near-terminal geometry + open-visit state,
    or None. A candidate must be at/approaching a terminal and silent within the
    actionable band; vessels in the final approach (or clearly closing) trigger
    on a shorter silence (#3)."""
    if last_fix_ts is None:
        return None
    silent_h = (now - last_fix_ts).total_seconds() / 3600.0
    if silent_h > STALE_CEILING_HOURS:
        return None  # event has passed; a late poll can't recover its timing

    open_visit = last_event_type is not None and last_event_type not in (
        "departed",
        "zone_exit",
    )
    near = near_km is not None and near_km <= NEAR_KM
    closing = is_closing(last_cog, bearing_deg, near_km)

    # Inclusion: at/near a terminal, in an open visit, or closing from range.
    if not (near or open_visit or closing):
        return None

    # Shorter silence threshold for vessels one fix from an event (#3).
    in_final_approach = (
        near_km is not None and near_km <= FINAL_APPROACH_KM
    ) or closing
    min_silence = (
        FINAL_APPROACH_SILENCE_HOURS if in_final_approach else MIN_SILENCE_HOURS
    )
    if silent_h < min_silence:
        return None

    if open_visit and last_event_flow == "export":
        cls = "export_departure"  # at an export berth, awaiting laden `departed`
    elif open_visit and last_event_flow == "import":
        cls = "import_berth"  # in an import zone/queue, awaiting `moored`/exit
    elif near_flow == "import":
        cls = "import_arrival"  # approaching an import terminal, awaiting `zone_entry`
    elif near_flow == "export":
        cls = "export_arrival"  # ballast approaching an export terminal to load
    else:
        return None
    return Candidate(
        mmsi=mmsi,
        imo=imo,
        vessel_name=vessel_name,
        last_fix_ts=last_fix_ts,
        last_lat=last_lat,
        last_lon=last_lon,
        rescue_class=cls,
        silent_h=silent_h,
    )


def terrestrial_budget(spent: int, cap: int, n_candidates: int) -> int:
    """How many candidates we can afford this run (1 credit each, worst case all
    return a position)."""
    return max(0, min(n_candidates, cap - spent))


def is_settled(navstat: int | None, speed: float | None) -> bool:
    """A vessel is 'settled' (moored/at anchor/stopped) — its next signal event
    isn't imminent, so it gets the normal cooldown. A vessel still *moving*
    (approaching/maneuvering to berth) gets the short RECHECK so we re-poll and
    capture the actual entry/moored (#2). NAVSTAT 1=at anchor, 5=moored."""
    if navstat in (1, 5):
        return True
    return speed is not None and speed < 1.0


def _chunks(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def merge_candidates(*lists: list[Candidate]) -> list[Candidate]:
    """Combine candidate sources, keeping one Candidate per mmsi — the
    highest-priority class (a vessel that is both silent-near-a-terminal and
    dest-unknown is polled once, labelled by its more urgent class)."""
    best: dict[int, Candidate] = {}
    for lst in lists:
        for c in lst:
            cur = best.get(c.mmsi)
            if (
                cur is None
                or CLASS_PRIORITY[c.rescue_class] < CLASS_PRIORITY[cur.rescue_class]
            ):
                best[c.mmsi] = c
    return list(best.values())


def rescue_result(
    *, position_ok: bool, rescue_class: str, dest_obtained: bool, position_status: str
) -> str:
    """The vf_rescue_log result for a returned position. A `dest_capture` poll
    succeeds when it yields a destination even if the position itself is
    redundant (the vessel was visible); event-capture classes succeed on a
    usable position."""
    if position_ok:
        return "rescued"
    if rescue_class == "dest_capture" and dest_obtained:
        return "rescued"
    return position_status


# --------------------------------------------------------------------------- #
# Async I/O
# --------------------------------------------------------------------------- #
async def _fetch_live(client: httpx.AsyncClient, imos: list[int]) -> httpx.Response:
    return await client.get(
        VF_LIVE_API_BASE,
        params={
            "userkey": settings.vf_api_key,
            "imo": ",".join(str(i) for i in imos),
            "format": "json",
            "interval": VF_INTERVAL_MINUTES,
            "sat": 0,  # terrestrial only — every rescue target is coastal
        },
    )


async def fetch_live_batch(
    client: httpx.AsyncClient, imos: list[int]
) -> list[VesselFinderAIS]:
    """One batched GET → list of parsed AIS positions. Vessels with no position
    within `interval` are simply absent from the response."""
    resp = await _fetch_live(client, imos)
    resp.raise_for_status()
    out: list[VesselFinderAIS] = []
    for item in resp.json():
        try:
            out.append(VesselFinderLiveResponse.model_validate(item).AIS)
        except Exception as e:
            logger.warning(f"VF live row failed to parse: {e}")
    return out


async def load_budget_today(conn: asyncpg.Connection) -> int:
    row = await conn.fetchrow(BUDGET_SQL)
    return int(row["spent"])


async def fetch_account_status(
    client: httpx.AsyncClient,
) -> tuple[int, datetime | None]:
    """Query the free /status endpoint → (remaining_credits, expiration_date)."""
    resp = await client.get(
        VF_STATUS_API_BASE,
        params={"userkey": settings.vf_api_key, "format": "json"},
    )
    resp.raise_for_status()
    data = resp.json()
    return int(data["CREDITS"]), parse_vf_timestamp(data.get("EXPIRATION_DATE"))


async def update_account_status(pool: asyncpg.Pool, client: httpx.AsyncClient) -> None:
    """Fetch the balance and snapshot it to vf_account_status (best-effort)."""
    try:
        credits, expires = await fetch_account_status(client)
    except Exception as e:
        logger.warning(f"vf_rescue: /status fetch failed ({e})")
        return
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO vf_account_status (credits, expiration_date) VALUES ($1, $2)",
            credits,
            expires,
        )
    logger.info(f"vf_rescue: balance {credits} credits (expires {expires:%Y-%m-%d})")


async def insert_rescue_fix(
    conn: asyncpg.Connection,
    ais: VesselFinderAIS,
    fix_ts: datetime,
    *,
    position_ok: bool,
) -> None:
    """Persist what VF returned. The `vessel_state` row (dest/eta/draught) is
    written whenever VF carries voyage data — even for a redundant position —
    so a `dest_capture` poll fills the destination on a visible vessel. The
    `ais_fixes` position is injected only when it passed sanity (`position_ok`),
    so we never inject a stale/teleport fix that would corrupt port_events."""
    if position_ok:
        await conn.execute(
            """
            INSERT INTO ais_fixes (fix_ts, mmsi, lat, lon, nav_status, sog, cog, source)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'vesselfinder')
            ON CONFLICT (fix_ts, mmsi) DO NOTHING
            """,
            fix_ts,
            ais.MMSI,
            ais.LATITUDE,
            ais.LONGITUDE,
            ais.NAVSTAT,
            ais.SPEED,
            ais.COURSE,
        )
    dest = ais.LOCODE or ais.DESTINATION
    eta_json = vf_eta_to_ais_dict(ais.ETA)
    if dest or ais.DRAUGHT is not None or eta_json is not None:
        await conn.execute(
            """
            INSERT INTO vessel_state (state_ts, mmsi, draught, dest, eta, source)
            VALUES ($1, $2, $3, $4, $5, 'vesselfinder')
            ON CONFLICT (state_ts, mmsi) DO NOTHING
            """,
            fix_ts,
            ais.MMSI,
            ais.DRAUGHT,
            dest,
            eta_json,
        )


async def log_rescue(
    conn: asyncpg.Connection,
    c: Candidate,
    *,
    src: str | None,
    result: str,
    credits: int,
    requested_imos: int,
    returned_rows: int,
    fix_ts: datetime | None,
    recheck_at: datetime | None,
    detail: str | None = None,
) -> None:
    await conn.execute(
        LOG_SQL,
        c.mmsi,
        c.imo,
        c.vessel_name,
        c.rescue_class,
        False,  # sat — never; column retained for the audit schema
        src,
        result,
        credits,
        requested_imos,
        returned_rows,
        fix_ts,
        detail,
        recheck_at,
    )


async def _load_candidates(conn: asyncpg.Connection, now: datetime) -> list[Candidate]:
    rows = await conn.fetch(CANDIDATE_SQL, EVENT_LOOKBACK_DAYS, CLOSING_INCLUDE_KM)
    out: list[Candidate] = []
    for r in rows:
        c = classify_candidate(
            mmsi=r["mmsi"],
            imo=r["imo"],
            vessel_name=r["vessel_name"],
            last_fix_ts=r["last_fix_ts"],
            last_lat=r["last_lat"],
            last_lon=r["last_lon"],
            near_flow=r["near_flow"],
            near_km=r["near_km"],
            last_cog=r["last_cog"],
            bearing_deg=r["bearing_deg"],
            last_event_type=r["last_event_type"],
            last_event_flow=r["last_event_flow"],
            now=now,
        )
        if c is not None:
            out.append(c)
    return out


def _row_to_candidate(r, rescue_class: str, now: datetime) -> Candidate:
    last = r["last_fix_ts"]
    silent_h = (now - last).total_seconds() / 3600.0 if last else 0.0
    return Candidate(
        mmsi=r["mmsi"],
        imo=r["imo"],
        vessel_name=r["vessel_name"],
        last_fix_ts=last,
        last_lat=r["last_lat"],
        last_lon=r["last_lon"],
        rescue_class=rescue_class,
        silent_h=silent_h,
    )


async def _load_dest_candidates(
    conn: asyncpg.Connection, now: datetime
) -> list[Candidate]:
    """#4 — recent laden departures with no destination yet."""
    rows = await conn.fetch(DEST_CANDIDATE_SQL, DEST_CAPTURE_WINDOW_HOURS)
    return [_row_to_candidate(r, "dest_capture", now) for r in rows]


async def _load_outage_candidates(
    conn: asyncpg.Connection, now: datetime
) -> list[Candidate]:
    """#5 — vessels last seen at a suspected-outage terminal, now silent."""
    rows = await conn.fetch(
        OUTAGE_CANDIDATE_SQL,
        OUTAGE_DAYS,
        OUTAGE_DORMANT_DAYS,
        OUTAGE_VESSEL_MIN_SILENT_HOURS,
        OUTAGE_VESSEL_MAX_SILENT_DAYS,
        OUTAGE_MAX_VESSELS,
    )
    return [_row_to_candidate(r, "outage_check", now) for r in rows]


# #6 — registry lookup for floating_check candidates (imo/name), with the same
# recheck_at cooldown the other triggers use.
FLOATING_ENRICH_SQL = """
SELECT v.mmsi, v.imo, v.vessel_name
FROM vessel_registry v
WHERE v.mmsi = ANY($1::bigint[]) AND v.imo IS NOT NULL AND v.imo <> 0
  AND v.mmsi NOT IN (
      SELECT mmsi FROM (
          SELECT DISTINCT ON (mmsi) mmsi, recheck_at
          FROM vf_rescue_log ORDER BY mmsi, requested_at DESC
      ) l WHERE l.recheck_at IS NOT NULL AND l.recheck_at > now()
  )
"""


async def _load_floating_candidates(
    pool: asyncpg.Pool, now: datetime
) -> list[Candidate]:
    """#6 — open laden legs classified `open_arrival_gap` by pipeline.legs (the
    vessel reached its destination region, then went AIS-dark before the entry
    fired). One coastal poll resolves it: a fix at a berth closes the leg (a
    recovered arrival), a fix loitering laden confirms floating storage, no
    position leaves it censored. Reuses the leg classifier rather than
    duplicating the per-O-D / last-fix logic in SQL."""
    legs = await legs_module.compute_legs(pool, now)
    gap = {
        lg.mmsi: lg
        for lg in legs
        if lg.status == "open_arrival_gap" and lg.laden and lg.last_fix_ts is not None
    }
    if not gap:
        return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(FLOATING_ENRICH_SQL, list(gap.keys()))
    out: list[Candidate] = []
    for r in rows:
        lg = gap[r["mmsi"]]
        out.append(
            Candidate(
                mmsi=r["mmsi"],
                imo=r["imo"],
                vessel_name=r["vessel_name"],
                last_fix_ts=lg.last_fix_ts,
                last_lat=lg.last_fix_lat,
                last_lon=lg.last_fix_lon,
                rescue_class="floating_check",
                silent_h=(now - lg.last_fix_ts).total_seconds() / 3600.0,
            )
        )
    return out


async def _load_manual_candidate(
    conn: asyncpg.Connection, mmsi: int, now: datetime
) -> Candidate | None:
    r = await conn.fetchrow(MANUAL_CANDIDATE_SQL, mmsi)
    return _row_to_candidate(r, "manual", now) if r is not None else None


async def _run_pass(
    pool: asyncpg.Pool,
    client: httpx.AsyncClient,
    candidates: list[Candidate],
    *,
    now: datetime,
) -> tuple[set[int], int]:
    """Fetch + persist one terrestrial pass over `candidates`. Returns (mmsis that
    returned a position, credits spent)."""
    returned: set[int] = set()
    spent = 0
    normal_recheck = now + timedelta(hours=PER_VESSEL_COOLDOWN_HOURS)
    moving_recheck = now + timedelta(hours=RECHECK_MOVING_HOURS)
    for batch in _chunks(candidates, IMO_BATCH_SIZE):
        try:
            ais_rows = await fetch_live_batch(client, [c.imo for c in batch])
        except httpx.HTTPStatusError as e:
            logger.warning(f"vf_rescue: batch HTTP {e.response.status_code} — skipping")
            async with pool.acquire() as conn:
                for c in batch:
                    # error ⇒ no cooldown (recheck_at NULL): retry next cycle.
                    await log_rescue(
                        conn,
                        c,
                        src=None,
                        result="error",
                        credits=0,
                        requested_imos=len(batch),
                        returned_rows=0,
                        fix_ts=None,
                        recheck_at=None,
                        detail=f"HTTP {e.response.status_code}",
                    )
            await asyncio.sleep(RATE_LIMIT_DELAY)
            continue
        except Exception as e:
            logger.warning(f"vf_rescue: batch request failed ({e}) — skipping")
            async with pool.acquire() as conn:
                for c in batch:
                    await log_rescue(
                        conn,
                        c,
                        src=None,
                        result="error",
                        credits=0,
                        requested_imos=len(batch),
                        returned_rows=0,
                        fix_ts=None,
                        recheck_at=None,
                        detail=str(e),
                    )
            await asyncio.sleep(RATE_LIMIT_DELAY)
            continue

        by_imo = {a.IMO: a for a in ais_rows if a.IMO is not None}
        by_mmsi = {a.MMSI: a for a in ais_rows}
        async with pool.acquire() as conn:
            for c in batch:
                a = by_imo.get(c.imo) or by_mmsi.get(c.mmsi)
                if a is None:
                    await log_rescue(
                        conn,
                        c,
                        src=None,
                        result="no_position",
                        credits=0,
                        requested_imos=len(batch),
                        returned_rows=len(ais_rows),
                        fix_ts=None,
                        recheck_at=normal_recheck,
                    )
                    continue
                returned.add(c.mmsi)
                credits = row_credits(a.SRC)
                spent += credits
                fix_ts = parse_vf_timestamp(a.TIMESTAMP)
                status = position_sanity(
                    vf_fix_ts=fix_ts,
                    vf_lat=a.LATITUDE,
                    vf_lon=a.LONGITUDE,
                    last_fix_ts=c.last_fix_ts,
                    last_lat=c.last_lat,
                    last_lon=c.last_lon,
                    now=now,
                )
                position_ok = status == "ok"
                # Always persist VF voyage data (fills dest for #4); inject the
                # position only when it's good.
                if fix_ts is not None:
                    await insert_rescue_fix(conn, a, fix_ts, position_ok=position_ok)
                result = rescue_result(
                    position_ok=position_ok,
                    rescue_class=c.rescue_class,
                    dest_obtained=bool(a.LOCODE or a.DESTINATION),
                    position_status=status,
                )
                # #2 — a rescued-but-still-moving vessel is re-polled soon; a
                # settled one (or a redundant/rejected position) waits the cooldown.
                recheck_at = (
                    moving_recheck
                    if position_ok and not is_settled(a.NAVSTAT, a.SPEED)
                    else normal_recheck
                )
                await log_rescue(
                    conn,
                    c,
                    src=a.SRC,
                    result=result,
                    credits=credits,
                    requested_imos=len(batch),
                    returned_rows=len(ais_rows),
                    fix_ts=fix_ts,
                    recheck_at=recheck_at,
                )
        await asyncio.sleep(RATE_LIMIT_DELAY)
    return returned, spent


async def run_rescue(
    pool: asyncpg.Pool,
    *,
    dry_run: bool = False,
    only_mmsi: int | None = None,
) -> dict:
    """One rescue cycle: select coastal silent vessels with a pending port event,
    fetch their positions (terrestrial) within budget, inject the good ones."""
    now = datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        spent = await load_budget_today(conn)
        if only_mmsi is not None:
            manual = await _load_manual_candidate(conn, only_mmsi, now)
            candidates = [manual] if manual else []
            sources: list[list[Candidate]] = []
        else:
            sources = [
                await _load_candidates(conn, now),  # #1-#3 event capture
                await _load_dest_candidates(conn, now),  # #4 destination capture
                await _load_outage_candidates(conn, now),  # #5 outage confirmation
            ]
    if only_mmsi is None:
        # #6 floating-vs-phantom uses pipeline.legs (manages its own pool conns),
        # so it runs after releasing the connection above.
        sources.append(await _load_floating_candidates(pool, now))
        candidates = merge_candidates(*sources)
        # Priority class first; within a class, most-overdue first.
        candidates.sort(key=lambda c: (CLASS_PRIORITY[c.rescue_class], -c.silent_h))
        candidates = candidates[:MAX_CANDIDATES_PER_RUN]

    remaining = DAILY_CREDIT_CAP - spent
    n = terrestrial_budget(spent, DAILY_CREDIT_CAP, len(candidates))
    chosen = candidates[:n]
    summary = {
        "selected": len(candidates),
        "planned": n,
        "budget_remaining": remaining,
        "rescued": 0,
        "credits_spent": 0,
    }

    if dry_run:
        by_class: dict[str, int] = {}
        for c in candidates:
            by_class[c.rescue_class] = by_class.get(c.rescue_class, 0) + 1
        logger.info(
            f"vf_rescue DRY-RUN: {len(candidates)} candidates {by_class} · "
            f"plan {n} · est ≤{min(n * TER_COST, remaining)}cr · "
            f"budget {spent}/{DAILY_CREDIT_CAP} today"
        )
        return summary

    async with httpx.AsyncClient(timeout=10.0) as client:
        # Refresh the account balance every live run (free /status call) so the
        # TUI stays current even on runs that rescue nothing.
        await update_account_status(pool, client)

        if remaining <= 0:
            logger.info(
                f"vf_rescue: daily cap reached ({spent}/{DAILY_CREDIT_CAP}cr) — skip"
            )
            return summary
        if not chosen:
            logger.info("vf_rescue: no coastal silent candidates")
            return summary

        returned, credits_spent = await _run_pass(pool, client, chosen, now=now)
    summary["rescued"] = len(returned)
    summary["credits_spent"] = credits_spent

    logger.info(
        f"vf_rescue: selected={summary['selected']} rescued={summary['rescued']} "
        f"spent={credits_spent}cr (today {spent + credits_spent}/{DAILY_CREDIT_CAP})"
    )
    return summary


async def main_async(args: argparse.Namespace) -> None:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=3)
    try:
        if args.status:
            async with httpx.AsyncClient(timeout=10.0) as client:
                await update_account_status(pool, client)
            return
        await run_rescue(pool, dry_run=args.dry_run, only_mmsi=args.mmsi)
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="VesselFinder live-position rescue")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Select candidates and estimate cost without any API calls or writes",
    )
    parser.add_argument(
        "--mmsi",
        type=int,
        default=None,
        help="Rescue a single MMSI now (bypasses geometry filters; still budget-capped)",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Fetch + store the VF account balance (free /status call) and exit",
    )
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
