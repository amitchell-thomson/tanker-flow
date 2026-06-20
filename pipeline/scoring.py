"""Tier scoring for the AISstream MMSI watchlist.

Every LNG/FSRU vessel in vessel_registry is ranked into a tier based on how
relevant it is to our signal *right now*. The result lands in
priority_watchlist, which `ingestion/aisstream.py` then reads to pick the 150
MMSIs to subscribe to (100 persistent + 50 scan).

Tier definitions (also documented in CLAUDE.md):

    1 — recent fix inside any terminal_zones polygon (within 3d) — must be
        plausibly *currently* in zone, not "was there a week ago and is now
        mid-Atlantic"
    2 — a parsed ETA within ETA_IMMINENT_HOURS ahead (or ETA_PAST_GRACE_HOURS
        just past — a slightly-late vessel stays pinned through berthing),
        regardless of whether the declared dest resolves ("FOR ORDERS" carriers
        still count), OR vessel_state.dest parses to a known terminal with
        state_ts < 14d old. The ETA path rescues long-voyage vessels whose
        declaration is stale or absent but whose arrival is imminent
    3 — recent fix inside any config.ZONES rectangle (within 14d), not 1/2;
        ordered within-tier by closing-ness (proximity + heading to the nearest
        zone, see _closing_bonus) so the scarce slots go to vessels approaching
    4 — any fix in the last 7d (not 1-3) — recently active globally
    5 — fix in 7-90d OR no fix at all (everything else)

Tiers 1-3 fill the persistent slot pool (top 100 by score). Tier-3 vessels that
don't win a persistent slot, plus tiers 4-5, fill the scan rotation pool — see
ingestion.aisstream.load_scan_mmsis.

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
    SELECT mmsi, is_fsru
    FROM vessel_registry
    WHERE (is_lng_carrier OR is_fsru) AND NOT excluded
),
candidate_fixes AS (
    SELECT a.mmsi, a.fix_ts, a.lat, a.lon, a.cog
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
    SELECT DISTINCT ON (vs.mmsi) vs.mmsi, vs.dest, vs.eta, vs.state_ts
    FROM vessel_state vs
    WHERE vs.state_ts > now() - INTERVAL '90 days'
      AND EXISTS (SELECT 1 FROM lng_fleet l WHERE l.mmsi = vs.mmsi)
    ORDER BY vs.mmsi, vs.state_ts DESC
),
-- Most recent position (+ course) per candidate vessel, used to measure how
-- close it is to entering a terminal and whether it is heading that way.
-- Drives the tier-3 closing-ness ordering (see _closing_bonus).
latest_pos AS (
    SELECT DISTINCT ON (mmsi) mmsi, lat, lon, cog
    FROM candidate_fixes
    ORDER BY mmsi, fix_ts DESC
),
nearest_zone AS (
    SELECT
        lp.mmsi,
        lp.cog,
        d.dist_km,
        -- Bearing from the vessel to the nearest zone's centroid (0-360).
        -- Compared against cog to decide if the vessel is closing on it.
        degrees(ST_Azimuth(
            ST_SetSRID(ST_Point(lp.lon, lp.lat), 4326)::geography,
            d.centroid::geography
        )) AS bearing_deg
    FROM latest_pos lp
    CROSS JOIN LATERAL (
        SELECT
            ST_Distance(
                ST_SetSRID(ST_Point(lp.lon, lp.lat), 4326)::geography,
                tz.geom::geography
            ) / 1000.0 AS dist_km,
            ST_Centroid(tz.geom) AS centroid
        FROM terminal_zones tz
        ORDER BY ST_SetSRID(ST_Point(lp.lon, lp.lat), 4326) <-> tz.geom
        LIMIT 1
    ) d
)
SELECT
    f.mmsi,
    f.is_fsru,
    lfg.last_fix_ts,
    fs.last_berth_fix_ts,
    fs.last_anchorage_fix_ts,
    fs.last_approach_fix_ts,
    fs.last_polygon_fix_ts,
    fs.last_bbox_fix_ts,
    ls.dest,
    ls.eta,
    ls.state_ts,
    nz.dist_km,
    nz.bearing_deg,
    nz.cog AS last_cog
FROM lng_fleet f
LEFT JOIN last_fix_global lfg USING (mmsi)
LEFT JOIN fix_stats fs USING (mmsi)
LEFT JOIN latest_state ls USING (mmsi)
LEFT JOIN nearest_zone nz USING (mmsi)
"""


def _parse_eta(eta_json: str | dict | None, now: datetime) -> datetime | None:
    """Parse AIS ETA (Month/Day/Hour/Minute, no year) into a UTC datetime.

    AISstream delivers ETA as JSONB like ``{"Day": 2, "Hour": 14, "Month": 6,
    "Minute": 0}``. AIS itself has no year field on ETA — we infer the current
    year unless the month/day has already passed by more than 30 days, in
    which case the year is bumped (vessels declare ETAs months ahead).

    Returns None when the AIS sentinels for "not available" appear (Month=0,
    Day=0, Hour=24, Minute=60) or when the components are out of range.
    """
    if eta_json is None:
        return None
    if isinstance(eta_json, str):
        try:
            import json

            eta_json = json.loads(eta_json)
        except (ValueError, TypeError):
            return None
    if not isinstance(eta_json, dict):
        return None
    try:
        month = int(eta_json.get("Month", 0))
        day = int(eta_json.get("Day", 0))
        hour = int(eta_json.get("Hour", 24))
        minute = int(eta_json.get("Minute", 60))
    except (TypeError, ValueError):
        return None
    if month == 0 or day == 0 or hour >= 24 or minute >= 60:
        return None
    if not (
        1 <= month <= 12 and 1 <= day <= 31 and 0 <= hour < 24 and 0 <= minute < 60
    ):
        return None
    try:
        candidate = datetime(now.year, month, day, hour, minute, tzinfo=timezone.utc)
    except ValueError:
        # e.g. Feb 30. AIS encoders sometimes pad this — treat as unavailable.
        return None
    if candidate < now - timedelta(days=30):
        try:
            candidate = candidate.replace(year=now.year + 1)
        except ValueError:
            return None
    return candidate


async def load_unlocode_map(conn: asyncpg.Connection) -> dict[str, int]:
    rows = await conn.fetch(
        "SELECT unlocode, terminal_id FROM terminals WHERE unlocode IS NOT NULL"
    )
    return {r["unlocode"]: r["terminal_id"] for r in rows}


# --- Predictive promotion + closing-ness tuning (watchlist coverage) ---
#
# Item 2 — imminent-ETA promotion. A vessel with a parsed ETA within this horizon
# is force-promoted into the persistent band (tier 2) even when its vessel_state
# is older than the 14-day tier-2 freshness window. An imminent ETA is itself a
# freshness signal, and these are exactly the inbound vessels we must not lose to
# tier-decay on a long voyage. The ETA path is NOT gated on a resolved terminal:
# a ballast carrier approaching a US load terminal commonly broadcasts a real ETA
# but "FOR ORDERS" as its destination (the discharge port is still being traded),
# which leaves dest_terminal_id NULL — yet the arrival is real and imminent and we
# must hold a slot through final approach (VENTURE CREOLE went dark this way,
# 2026-06). The plain dest-declaration path below still requires a resolved
# terminal, since a fresh "FOR ORDERS" alone carries no arrival timing.
ETA_IMMINENT_HOURS = 48
# A just-passed ETA still holds the slot for this grace window: an inbound vessel
# is at its most arrival-critical right around its declared ETA, and ETAs are
# routinely a few hours optimistic. Without grace, a vessel running slightly late
# drops out of the persistent band at the worst possible moment — exactly during
# final approach / berthing. Mirrors vf_rescue's ETA_RESCUE_PAST_GRACE_HOURS.
ETA_PAST_GRACE_HOURS = 12
# Sticky-ETA tail. Beyond the grace window, an ETA is normally treated as stale
# and the vessel decays. But a vessel that declared an arrival, is now overdue,
# and that we have NOT seen arrive (no terminal-polygon fix at/after its ETA) is a
# dark inbound carrier mid-slip — exactly the one we must keep a persistent slot
# on, not drop. GREENERGY OCEAN (2026-06) decayed tier-2 → tier-5 this way: ETA
# ~60h past, grace only 12h, no captured arrival, so it fell out of the band while
# actually approaching Freeport. We hold it at tier 2 up to this many hours past
# the ETA. A vessel that genuinely arrived is already tier 1 (recent polygon fix)
# and never reaches the tier-2 branch; the polygon-fix guard below additionally
# rejects an arrived-and-departed vessel whose stale ETA would otherwise re-pin it.
ETA_STICKY_PAST_HOURS = 96
# Large base so imminent-ETA vessels sort above plain dest-declarations within
# tier 2 (sooner ETA ⇒ higher score). Well above epoch-second scores (~1.7e9)
# yet representable in the REAL score column.
ETA_SCORE_BASE = 1.0e10

# Item 3 — tier-3 closing-ness. Tier 3 ("seen in the wider zone bbox within
# 14d") has more vessels than persistent slots, so the within-tier ordering
# decides which ones hold a slot. Bias it toward vessels physically near a
# terminal and heading toward it, rather than pure fix-recency. Bonuses are in
# seconds so they compose with the epoch-second score base (same convention as
# _ZONE_TYPE_BONUS_S below).
PROX_RANGE_KM = 300.0  # only vessels within this of the nearest zone earn a bonus
PROX_BONUS_MAX_S = (
    3 * 86400
)  # at 0 km from the nearest zone edge, scaled to 0 at PROX_RANGE_KM
CLOSING_BONUS_S = (
    1 * 86400
)  # cog points within CLOSING_MAX_ANGLE_DEG of the bearing to the zone
CLOSING_MAX_ANGLE_DEG = 60.0


def _closing_bonus(
    dist_km: float | None, bearing_deg: float | None, cog: float | None
) -> float:
    """Seconds-equivalent score bonus for a tier-3 vessel that is near a
    terminal zone and (optionally) heading toward it. Zero outside PROX_RANGE_KM
    or when position is unknown."""
    if dist_km is None or dist_km >= PROX_RANGE_KM:
        return 0.0
    bonus = PROX_BONUS_MAX_S * (1.0 - dist_km / PROX_RANGE_KM)
    if cog is not None and bearing_deg is not None:
        # Smallest signed angle between course and bearing-to-zone, in [0, 180].
        diff = abs((cog - bearing_deg + 180.0) % 360.0 - 180.0)
        if diff <= CLOSING_MAX_ANGLE_DEG:
            bonus += CLOSING_BONUS_S
    return bonus


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


# FSRUs are floating *terminals*: once deployed they sit moored at their host
# berth for months, and an FSRU's own AIS fixes never drive the signal anyway —
# pipeline.port_events short-circuits an FSRU to one synthetic `moored` at its
# declared host terminal. A persistent (continuous-polling) subscription on a
# stationary FSRU is therefore a wasted slot; all we need is an occasional check
# that it hasn't relocated. So an FSRU is force-assigned tier FSRU_TIER — out of
# the persistent band (tiers 1-3) — and served by a dedicated low-frequency scan
# quota (see ingestion.aisstream SCAN_FSRU_SLOTS). It is also excluded from
# inline in-zone promotion and the in-port pin so nothing bounces it back up.
FSRU_TIER = 5


def assign_tier(
    *,
    is_fsru: bool,
    last_berth_fix_ts: datetime | None,
    last_anchorage_fix_ts: datetime | None,
    last_approach_fix_ts: datetime | None,
    last_polygon_fix_ts: datetime | None,
    last_bbox_fix_ts: datetime | None,
    last_fix_ts: datetime | None,
    dest_terminal_id: int | None,
    state_ts: datetime | None,
    parsed_eta: datetime | None,
    dist_km: float | None,
    bearing_deg: float | None,
    last_cog: float | None,
    now: datetime,
) -> tuple[int, str, float]:
    """Return (tier, reason, score_value) for a single vessel.

    score_value is the float written to priority_watchlist.score and used for
    within-tier ordering. For tiers 1-4, higher = more relevant. For tier 5
    the caller uses ASC ordering on `last_scan_window_at` for rotation
    instead, so the score there is just the epoch timestamp.
    """
    # FSRUs are pinned terminals, not tracked carriers — force them to the
    # low-frequency band regardless of position (see FSRU_TIER above). Score is
    # the last-fix epoch purely for a stable ordering; the dedicated FSRU scan
    # pool rotates by last_scan_window_at, not score.
    if is_fsru:
        return (
            FSRU_TIER,
            "fsru:host-watch",
            last_fix_ts.timestamp() if last_fix_ts else 0.0,
        )

    three_days = now - timedelta(days=3)
    fourteen_days = now - timedelta(days=14)
    seven_days = now - timedelta(days=7)
    ninety_days = now - timedelta(days=90)

    if last_polygon_fix_ts and last_polygon_fix_ts > three_days:
        score, ts, label = _tier1_score(
            last_berth_fix_ts, last_anchorage_fix_ts, last_approach_fix_ts
        )
        return (1, f"in-zone:{label} @ {ts:%Y-%m-%d}", score)

    # Tier 2 — inbound to the persistent band. Fires on EITHER an imminent parsed
    # ETA (regardless of state age OR whether the declared dest resolves) OR a
    # fresh dest-declaration to a known terminal. The ETA path rescues long-voyage
    # vessels whose arrival is imminent, including "FOR ORDERS" carriers with no
    # parseable dest (Item 2).
    eta_imminent = parsed_eta is not None and (
        now - timedelta(hours=ETA_PAST_GRACE_HOURS)
        <= parsed_eta
        <= now + timedelta(hours=ETA_IMMINENT_HOURS)
    )
    # Sticky tail: further past the grace window (up to ETA_STICKY_PAST_HOURS) but
    # only when we have no evidence the vessel arrived — no terminal-polygon fix
    # at or after its declared ETA. Keeps a dark, overdue inbound carrier in the
    # persistent band through a slipped arrival (see ETA_STICKY_PAST_HOURS).
    eta_sticky = (
        parsed_eta is not None
        and now - timedelta(hours=ETA_STICKY_PAST_HOURS)
        <= parsed_eta
        < now - timedelta(hours=ETA_PAST_GRACE_HOURS)
        and (last_polygon_fix_ts is None or last_polygon_fix_ts < parsed_eta)
    )
    if eta_imminent or eta_sticky:
        hours_to_eta = (parsed_eta - now).total_seconds() / 3600.0
        dest_label = (
            f"terminal_id={dest_terminal_id}" if dest_terminal_id else "for-orders"
        )
        # Negative hours_to_eta ⇒ ETA already passed (within grace); render as
        # "Nh ago" so the reason stays readable.
        when = (
            f"in {hours_to_eta:.0f}h"
            if hours_to_eta >= 0
            else f"{-hours_to_eta:.0f}h ago"
        )
        return (
            2,
            f"eta:{dest_label} {when}",
            ETA_SCORE_BASE - (parsed_eta - now).total_seconds(),
        )
    if dest_terminal_id and state_ts and state_ts > fourteen_days:
        return (
            2,
            f"dest:terminal_id={dest_terminal_id} @ {state_ts:%Y-%m-%d}",
            state_ts.timestamp(),
        )

    if last_bbox_fix_ts and last_bbox_fix_ts > fourteen_days:
        score = last_bbox_fix_ts.timestamp() + _closing_bonus(
            dist_km, bearing_deg, last_cog
        )
        return (
            3,
            f"in-zone:bbox @ {last_bbox_fix_ts:%Y-%m-%d}",
            score,
        )

    if last_fix_ts and last_fix_ts > seven_days:
        return (4, f"recent-anywhere @ {last_fix_ts:%Y-%m-%d}", last_fix_ts.timestamp())

    if last_fix_ts and last_fix_ts > ninety_days:
        return (5, f"stale @ {last_fix_ts:%Y-%m-%d}", last_fix_ts.timestamp())

    return (5, "never-seen", last_fix_ts.timestamp() if last_fix_ts else 0.0)


# --- Manual tier overrides (operator escape hatch) ---
#
# Maps MMSI -> forced tier. A vessel listed here is assigned that tier on every
# scoring pass, overriding both the computed position/ETA logic AND the FSRU
# short-circuit. This is the durable equivalent of hand-editing priority_watchlist
# (which the next scoring pass — hourly, and before every reconnect — would
# otherwise clobber within the hour). Use sparingly: it's a deliberate "I need
# eyes on this hull regardless of what the heuristics say" lever, not a tuning
# knob. Editing this dict requires a scoring run (`make scoring`) or an ingester
# restart to take effect.
#
# CAVEAT: forcing a vessel into the persistent band (tiers 1-3) only re-acquires
# it if it is actually transmitting AIS — a persistent slot cannot raise a
# long-silent vessel from the dead. For a vessel gone dark near a terminal, the
# tool is `vf_rescue.py --mmsi <mmsi>` (fetches a live position from VesselFinder).
MANUAL_TIER_OVERRIDES: dict[int, int] = {
    636023760: 1,  # ORION HUGO — operator pin (2026-06-11)
}

# An override's score is pinned to "now + this" so it sorts to the top of its
# tier and reliably holds a persistent slot (the ingester takes the top 100 by
# tier ASC, score DESC — a low natural score from a stale last fix would
# otherwise be culled at the 100-boundary). One day clears the in-tier-1 zone-type
# bonus (_ZONE_TYPE_BONUS_S, ≤3h) so the override out-sorts every genuine fix.
_OVERRIDE_SCORE_BONUS_S = 86400


def apply_manual_override(
    mmsi: int,
    tier: int,
    reason: str,
    score: float,
    now: datetime,
) -> tuple[int, str, float]:
    """Apply MANUAL_TIER_OVERRIDES to a computed (tier, reason, score).

    Pure: returns the inputs unchanged when the MMSI has no override. When it
    does, forces the configured tier, marks the reason, and pins the score high
    so the vessel holds its slot regardless of fix recency (see
    _OVERRIDE_SCORE_BONUS_S).
    """
    forced = MANUAL_TIER_OVERRIDES.get(mmsi)
    if forced is None:
        return tier, reason, score
    return (forced, f"manual-override:t{forced}", now.timestamp() + _OVERRIDE_SCORE_BONUS_S)


# Open-leg pin: a vessel with an open leg (a `departed` with no later
# `zone_entry`) is mid-voyage and will re-enter terrestrial AIS range on its
# approach to the next terminal. We can't *hear* it mid-ocean, so a persistent
# slot only does work inside that approach window — holding one across the whole
# dark crossing just idles a scarce slot. So we pin BOTH directions (laden ->
# import arrival AND ballast -> export-terminal loading), but only while each
# leg's expected approach window is open, ranked by closeness to expected
# arrival (see ingestion.aisstream.load_persistent_mmsis for slot allocation).
#
# This shape is from the appear-in-berth audit. The old pin was laden-only and
# ordered by departed_ts DESC, so it (a) never pinned the ballast return to a US
# export terminal — the dominant miss (New Apex, SM Bluebird, ...) — and (b)
# once open legs exceeded the cap, kept the *freshest* departures (still
# mid-ocean, slot idle) over the vessels actually *due to arrive*. Window-gating
# + arrival ordering fixes both, and is self-selecting: a short intra-region leg
# closes (gets a zone_entry) before its window opens, so only genuine long-haul
# legs ever reach the pin.
# >= max expected voyage + post-window (cf. legs.CENSOR_OPEN_DAYS).
PIN_LOOKBACK_DAYS = 30
PIN_MAX = 30
# Subscribe up to 4d early (absorbs early arrivals / model error); stay
# subscribed up to 8d late (late arrival / re-acquire after an arrival-gap).
PIN_PRE_WINDOW_DAYS = 4
PIN_POST_WINDOW_DAYS = 8

# Expected voyage length keyed by the DEPARTURE zone. An open leg's *destination*
# isn't known yet, but departure zone + the still-open-this-long condition pins
# down the haul (US export <-> EU import is the transatlantic ~14-18d spine).
# Coarse Phase-1 constants; Phase 2 replaces these with rolling medians of
# observed departed->zone_entry durations per O-D pair (see legs.py).
EXPECTED_VOYAGE_DAYS: dict[str, int] = {
    "usgulf": 16,  # US export -> EU import (laden out)
    "usatlantic": 15,
    "nweurope": 15,  # EU import -> US export (ballast return)
    "baltic": 16,
    "iberian": 15,
    "wmed": 16,
    "emed": 17,
}
DEFAULT_VOYAGE_DAYS = 16

OPEN_LEG_PIN_SQL = """
SELECT DISTINCT ON (pe.mmsi)
       pe.mmsi, pe.event_time AS departed_ts, pe.zone AS depart_zone
FROM port_events pe
WHERE pe.event_type = 'departed'
  AND pe.event_time > now() - make_interval(days => $1)
  AND NOT EXISTS (
      SELECT 1 FROM port_events z
      WHERE z.mmsi = pe.mmsi
        AND z.event_type = 'zone_entry'
        AND z.event_time > pe.event_time
  )
ORDER BY pe.mmsi, pe.event_time DESC
"""


def _select_open_leg_pins(
    open_legs: list[tuple[int, datetime, str | None]], now: datetime
) -> set[int]:
    """Pure pin selection: keep only legs whose approach window is open *now*,
    rank by closeness to expected arrival, cap at PIN_MAX.

    `open_legs` is [(mmsi, departed_ts, depart_zone), ...] — the most recent
    open departure per vessel. Returns the pinned MMSI set.
    """
    pre = timedelta(days=PIN_PRE_WINDOW_DAYS)
    post = timedelta(days=PIN_POST_WINDOW_DAYS)

    due: list[tuple[datetime, int]] = []
    for mmsi, departed_ts, depart_zone in open_legs:
        voyage = EXPECTED_VOYAGE_DAYS.get(depart_zone, DEFAULT_VOYAGE_DAYS)
        expected_arrival = departed_ts + timedelta(days=voyage)
        # In window: close enough that the vessel is plausibly back in
        # terrestrial range, not yet so overdue it's floating storage.
        if expected_arrival - pre <= now <= expected_arrival + post:
            due.append((expected_arrival, mmsi))

    # Earliest expected arrival first — an overdue leg (expected_arrival in the
    # past) sorts ahead of a not-yet-due one, so when the cap binds the scarce
    # slots go to the vessels actually approaching now.
    due.sort(key=lambda t: t[0])
    return {mmsi for _, mmsi in due[:PIN_MAX]}


async def load_open_leg_pins(conn: asyncpg.Connection) -> set[int]:
    """MMSIs on an open leg whose approach window is open now, both directions —
    see the PIN_* / EXPECTED_VOYAGE_DAYS notes above."""
    rows = await conn.fetch(OPEN_LEG_PIN_SQL, PIN_LOOKBACK_DAYS)
    open_legs = [(r["mmsi"], r["departed_ts"], r["depart_zone"]) for r in rows]
    return _select_open_leg_pins(open_legs, datetime.now(timezone.utc))


# In-port pin: a vessel whose most recent port_event is an in-port state (it has
# entered/anchored/moored and not yet departed) is physically in a berth queue
# or alongside, generating the moored/departed events the signal needs. The
# 3-day tier-1 window plus the self-referential watchlist (a vessel only emits
# in-zone fixes while subscribed) means a long port queue can decay out of the
# persistent block and then stay dark — a self-reinforcing blind spot. Pinning
# any vessel still "open" in a visit guarantees its slot until it departs.
# Bounded by recency + a cap so it can't crowd out the tier band (see Item 4).
INPORT_PIN_LOOKBACK_DAYS = 20
INPORT_PIN_MAX = 30

INPORT_PIN_SQL = """
WITH last_event AS (
    SELECT DISTINCT ON (pe.mmsi) pe.mmsi, pe.event_type, pe.event_time
    FROM port_events pe
    WHERE pe.event_time > now() - make_interval(days => $1)
    ORDER BY pe.mmsi, pe.event_time DESC
)
SELECT mmsi FROM last_event
WHERE event_type NOT IN ('departed', 'zone_exit')
  -- FSRUs always sit "open" in a visit (one synthetic `moored`, never a
  -- `departed`), so the in-port pin would hold one in a persistent slot forever.
  -- They're deliberately demoted to the low-frequency FSRU scan band (FSRU_TIER)
  -- instead, so exclude them here.
  AND mmsi NOT IN (SELECT mmsi FROM vessel_registry WHERE is_fsru)
ORDER BY event_time DESC
LIMIT $2
"""


async def load_inport_pins(conn: asyncpg.Connection) -> set[int]:
    """MMSIs currently 'open' in a port visit (last event is not a departure) —
    see INPORT_PIN_* and the SQL comment above."""
    rows = await conn.fetch(INPORT_PIN_SQL, INPORT_PIN_LOOKBACK_DAYS, INPORT_PIN_MAX)
    return {r["mmsi"] for r in rows}


# A promotion is "notable" (worth logging) when a vessel moves UP into the
# persistent-subscription band (tiers 1-3) — i.e. it just became important
# enough to hold a guaranteed slot. Movements within 4/5 aren't logged.
_PERSISTENT_TIER_MAX = 3

LOG_PROMOTION_SQL = """
INSERT INTO tier_promotions (mmsi, vessel_name, old_tier, new_tier, via, reason, zone)
VALUES ($1, $2, $3, $4, 'scoring', $5, NULL)
"""


UPSERT_SQL = """
INSERT INTO priority_watchlist (
    mmsi, tier, score, score_reason,
    last_fix_ts, last_zone_fix_ts,
    parsed_dest_terminal_id, parsed_eta,
    is_pinned, in_slot, slot_kind, computed_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, FALSE, NULL, now())
ON CONFLICT (mmsi) DO UPDATE SET
    tier                    = EXCLUDED.tier,
    score                   = EXCLUDED.score,
    score_reason            = EXCLUDED.score_reason,
    last_fix_ts             = EXCLUDED.last_fix_ts,
    last_zone_fix_ts        = EXCLUDED.last_zone_fix_ts,
    parsed_dest_terminal_id = EXCLUDED.parsed_dest_terminal_id,
    parsed_eta              = EXCLUDED.parsed_eta,
    is_pinned               = EXCLUDED.is_pinned,
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

        open_leg_pins = await load_open_leg_pins(conn)
        inport_pins = await load_inport_pins(conn)
        pinned = open_leg_pins | inport_pins
        logger.info(
            f"Pins: {len(open_leg_pins)} open-leg + {len(inport_pins)} in-port "
            f"= {len(pinned)} unique"
        )

        # Snapshot current tiers + names BEFORE the upsert so we can detect
        # promotions into the persistent band and log them to tier_promotions.
        prev_tiers: dict[int, int] = {
            r["mmsi"]: r["tier"]
            for r in await conn.fetch("SELECT mmsi, tier FROM priority_watchlist")
        }
        names: dict[int, str | None] = {
            r["mmsi"]: r["vessel_name"]
            for r in await conn.fetch(
                "SELECT mmsi, vessel_name FROM vessel_registry "
                "WHERE is_lng_carrier OR is_fsru"
            )
        }
        promotions: list[tuple] = []

        # Also remove rows from priority_watchlist where the vessel is no
        # longer in scope (e.g. excluded=TRUE post-import). The set of valid
        # mmsis is exactly `rows`.
        valid_mmsis = {r["mmsi"] for r in rows}

        async with conn.transaction():
            for r in rows:
                dest_terminal_id, _is_for_orders = parse_destination(
                    r["dest"], unlocodes
                )
                # last_zone_fix_ts in the table = polygon fix if present, else
                # the wider bbox fix; readers don't need to know which.
                last_zone_fix_ts = r["last_polygon_fix_ts"] or r["last_bbox_fix_ts"]

                parsed_eta = _parse_eta(r["eta"], now)

                tier, reason, score = assign_tier(
                    is_fsru=r["is_fsru"],
                    last_berth_fix_ts=r["last_berth_fix_ts"],
                    last_anchorage_fix_ts=r["last_anchorage_fix_ts"],
                    last_approach_fix_ts=r["last_approach_fix_ts"],
                    last_polygon_fix_ts=r["last_polygon_fix_ts"],
                    last_bbox_fix_ts=r["last_bbox_fix_ts"],
                    last_fix_ts=r["last_fix_ts"],
                    dest_terminal_id=dest_terminal_id,
                    state_ts=r["state_ts"],
                    parsed_eta=parsed_eta,
                    dist_km=r["dist_km"],
                    bearing_deg=r["bearing_deg"],
                    last_cog=r["last_cog"],
                    now=now,
                )
                tier, reason, score = apply_manual_override(
                    r["mmsi"], tier, reason, score, now
                )
                tier_counts[tier] += 1

                # Notable promotion: moved up INTO the persistent band. old_tier
                # absent (first time seen) counts as a promotion iff it lands in
                # the band — it just appeared as slot-worthy.
                old_tier = prev_tiers.get(r["mmsi"])
                if tier <= _PERSISTENT_TIER_MAX and (
                    old_tier is None or tier < old_tier
                ):
                    promotions.append(
                        (r["mmsi"], names.get(r["mmsi"]), old_tier, tier, reason)
                    )

                await conn.execute(
                    UPSERT_SQL,
                    r["mmsi"],
                    tier,
                    score,
                    reason,
                    r["last_fix_ts"],
                    last_zone_fix_ts,
                    dest_terminal_id,
                    parsed_eta,
                    r["mmsi"] in pinned,
                )

            if promotions:
                await conn.executemany(LOG_PROMOTION_SQL, promotions)

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
        + f"  (promotions logged: {len(promotions)})"
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
