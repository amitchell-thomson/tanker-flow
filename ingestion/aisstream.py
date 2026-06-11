# ingestion/aisstream.py
import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import asyncpg
import websockets
from rich.logging import RichHandler

from config import settings

from pipeline import port_events, scoring

from . import vf_rescue
from .dynamic_enrichment import EnrichmentState, enrichment_worker, load_known_mmsis
from .metrics import MinuteAggregator, classify_zone, record_event
from .models import AISMessage, PositionReport, ShipStaticData

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler()],
)
logger = logging.getLogger(__name__)


# MMSI-filtered subscriptions are sparse — at ~50 MMSIs with variable broadcast
# cadence + terrestrial-AIS coverage gaps, going a couple of minutes without any
# of the 50 reporting is normal on a healthy connection. The watchdog should
# only fire if the connection is genuinely dead.
SILENCE_THRESHOLD_SECONDS = 300
# At ~150 fixes/min total across 3 connections (≈ <1 fix/s per connection), 1k
# is several minutes of headroom for any transient parser stall — plenty for the
# MMSI-filtered firehose, which is sparse by design.
RAW_QUEUE_MAXSIZE = 1000
FLUSH_INTERVAL_SECONDS = 0.5

# Plan: subscribe to specific LNG-carrier + FSRU MMSIs across N parallel WebSockets
# (server-side MMSI filter), instead of pulling all vessels in 7 wide bboxes and
# discarding 95% client-side. This sidesteps AISstream's per-account throttle —
# the previous bbox+rotation design was throttled to ~25-50% per-LNG-carrier
# visibility; MMSI filtering achieves ~100% on the priority list. See README.
NUM_CONNECTIONS = 3
MMSI_CAP_PER_CONNECTION = 50  # AISstream's documented limit

# Slot allocation: chunks 0 and 1 take the persistent block; chunk 2 is the
# scan-rotation connection that cycles through tier-4/5 vessels.
PERSISTENT_CONNECTIONS = 2
PERSISTENT_SLOTS = PERSISTENT_CONNECTIONS * MMSI_CAP_PER_CONNECTION  # 100
SCAN_CHUNK_INDEX = NUM_CONNECTIONS - 1
SCAN_SLOTS = MMSI_CAP_PER_CONNECTION  # 50
# The scan connection rotates through three priority-ordered pools, each with a
# reserved quota (with roll-over so SCAN_SLOTS is always filled when candidates
# exist). Picks within a pool take the least-recently-scanned vessels.
#
#   overflow — persistent-band vessels (tier<=3) that did NOT win one of the
#       100 persistent slots this cycle. Tier-1/2 almost always fit, so in
#       practice this is the tier-3 overflow: vessels seen in the wider zone
#       bbox but crowded out of the persistent block. Before this pool existed
#       they fell into a coverage hole — too low-tier for a persistent slot,
#       excluded from the old tier>=4-only scan — so they went fully dark with
#       no path back to a slot. These are the highest-value unsubscribed
#       vessels (near a zone), so they get first call on the scan slots.
#   tier4 — recently-active-anywhere rotation.
#   tier5 — stale / never-seen discovery. A reserved quota stops tier 4 (~400
#       candidates) from consuming every slot and starving tier-5 vessels,
#       which could then never accrue a fix to be promoted out.
#   fsru — deployed floating terminals (scoring.FSRU_TIER). They sit moored for
#       months and their own fixes never drive the signal, so they don't earn a
#       persistent slot — but we still want an occasional confirmation they
#       haven't relocated. A small dedicated quota gives ~46 FSRUs a low-
#       frequency rotation without diluting the tier-4/5 discovery pools, which
#       explicitly exclude FSRUs.
SCAN_OVERFLOW_SLOTS = 15
SCAN_TIER5_SLOTS = 10
SCAN_FSRU_SLOTS = 3
SCAN_TIER4_SLOTS = (
    SCAN_SLOTS - SCAN_OVERFLOW_SLOTS - SCAN_TIER5_SLOTS - SCAN_FSRU_SLOTS
)  # 22

# --- Multi-worker sharding (Stage 3) ------------------------------------------
# Read once at import. WORKER_COUNT=1 (default) ⇒ every helper below is a no-op
# and the SQL + source labels are byte-identical to the single-worker ingester.
# A second egress IP (Oracle VM + Tailscale — see the runbook) runs WORKER_COUNT=2
# with WORKER_ID=1 to hold the disjoint odd-MMSI half of the fleet. The partition
# key is a stable mmsi-modulo applied to ALL pools (persistent + scan + the
# slot-clear), so each vessel is owned end-to-end by exactly one worker — no
# cross-pool fall-through, no runtime coordination between workers.
WORKER_ID = settings.worker_id
WORKER_COUNT = settings.worker_count


def _worker_partition_sql(
    worker_id: int, worker_count: int, mmsi_col: str = "mmsi"
) -> str:
    """SQL predicate selecting only this worker's share of vessels, by a stable
    HASH of the MMSI. Returns 'TRUE' (which the planner folds away) when unscaled,
    so single-worker SQL is unchanged.

    Why hash rather than `mmsi % WORKER_COUNT`: LNG-carrier MMSIs are heavily
    even-skewed (~81% even in the live fleet), so a raw modulo on the value hands
    one worker ~4x the other's load. hashtext() decorrelates the digit skew for a
    ~50/50 split (measured 384/397). It is deterministic and computed server-side,
    so both workers bucket each vessel identically — disjoint, stable, no
    coordination. The ((h % n) + n) % n form is non-negative regardless of
    hashtext's signed result."""
    if worker_count <= 1:
        return "TRUE"
    h = f"hashtext({mmsi_col}::text)"
    return f"((({h} % {worker_count}) + {worker_count}) % {worker_count} = {worker_id})"


def _source_label(worker_id: int, worker_count: int, chunk_index: int) -> str:
    """Per-connection source label. A single worker keeps the historical
    'aisstream-mmsi-{1,2,3}' (so ais_fixes.source + the TUI are unchanged);
    multi-worker uses 'aisstream-w{id}-{n}' so the two workers' per-source stats
    never collide."""
    if worker_count <= 1:
        return f"aisstream-mmsi-{chunk_index + 1}"
    return f"aisstream-w{worker_id}-{chunk_index + 1}"

# Reconnect every hour. Each reconnect:
#   1. Triggers a fresh scoring run (see scoring_loop) just beforehand
#   2. Re-queries priority_watchlist for the current top-150
#   3. Closes + reopens the WebSocket with the new MMSI chunk
# The 1h cadence is what makes scan rotation work — chunk 2 swaps its 50
# vessels each cycle, cycling through ~650 tier-4/5 candidates over ~13h.
RECONNECT_INTERVAL_SECONDS = 3600

# Scoring cadence — decoupled from the 1h reconnect. Re-ranking the watchlist is
# a sub-second SQL pass, so running it every 5 min keeps tiers fresh (and logs
# promotions) without waiting an hour. A vessel the scan discovers in-zone is
# promoted instantly at flush (promote_inzone); this loop is the catch-all that
# also handles demotions, dest-based tier-2, and refines the inline tier.
SCORING_INTERVAL_SECONDS = 300

# Periodic port_events rebuild cadence. The rebuild is a full recompute from
# ais_fixes (~6s over the current hypertable) but writes via an atomic staging
# swap, so a 2-min cadence keeps derived events/legs near-live at ~5% of one core
# without ever exposing a partial table. Minutes-scale latency is well inside what
# the signal layer needs (outage/queue nowcasts, not per-second updates).
PORT_EVENTS_INTERVAL_SECONDS = 120

# Use full-globe bbox; the MMSI filter does the actual constraining.
GLOBAL_BBOX = [[[-85.0, -180.0], [85.0, 180.0]]]

TANKER_TYPES = set(range(80, 90))


@dataclass
class IngestionState:
    """Per-connection: MMSI filter set + counters + buffers.

    `source_name` is plumbed into ais_fixes / vessel_state so downstream
    queries can distinguish persistent (mmsi-1/-2) from scan (mmsi-3) fixes.
    """

    source_name: str = "aisstream"
    non_tanker_mmsis: set[int] = field(default_factory=set)
    fix_inserts: int = 0
    registry_upserts: int = 0
    state_inserts: int = 0

    fix_buf: list[tuple] = field(default_factory=list)
    registry_buf: list[tuple] = field(default_factory=list)
    state_buf: list[tuple] = field(default_factory=list)
    # MMSI -> geographic zone for fixes seen in this flush window that landed
    # inside a config.ZONES rectangle. Drives instant in-zone tier promotion at
    # flush time so a vessel the scan rotation just discovered near a terminal
    # isn't dropped before the next hourly scoring pass. Cleared each flush.
    inzone_mmsi: dict[int, str] = field(default_factory=dict)


def build_subscribe_payload(api_key: str, mmsis: list[int]) -> dict:
    return {
        "APIKey": api_key,
        "BoundingBoxes": GLOBAL_BBOX,
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
        "FiltersShipMMSI": [str(m) for m in mmsis],
    }


async def load_persistent_mmsis(pool: asyncpg.Pool) -> list[int]:
    """The PERSISTENT_SLOTS persistent subscriptions: open-leg PINS first
    (vessels with a recent open laden leg — forced in regardless of tier so we
    re-acquire them on the return approach, fixing M1), then the top tier-1-3
    vessels by (tier ASC, score DESC) to fill the remainder. Total is capped at
    PERSISTENT_SLOTS; pins are bounded to PIN_MAX << PERSISTENT_SLOTS in
    scoring, so the tier block is never fully crowded out. Falls back to the
    cold-start query if priority_watchlist is empty (first boot)."""
    # This worker's mmsi-modulo share only ('TRUE' = all, when unscaled). Each
    # worker independently takes the top PERSISTENT_SLOTS of its own partition,
    # so the union covers ~PERSISTENT_SLOTS×WORKER_COUNT vessels with no overlap.
    part = _worker_partition_sql(WORKER_ID, WORKER_COUNT)
    async with pool.acquire() as conn:
        pinned = await conn.fetch(
            f"""
            SELECT mmsi FROM priority_watchlist
            WHERE is_pinned AND {part}
            ORDER BY tier ASC, score DESC
            LIMIT $1
            """,
            PERSISTENT_SLOTS,
        )
        pinned_mmsis = [r["mmsi"] for r in pinned]
        fill = await conn.fetch(
            f"""
            SELECT mmsi FROM priority_watchlist
            WHERE tier <= 3 AND NOT is_pinned AND {part}
            ORDER BY tier ASC, score DESC
            LIMIT $1
            """,
            PERSISTENT_SLOTS - len(pinned_mmsis),
        )
        mmsis = pinned_mmsis + [r["mmsi"] for r in fill]
        if mmsis:
            return mmsis
        # Cold start: priority_watchlist not yet populated. Fall back to the
        # is_lng_carrier OR is_fsru list ordered by recency so the ingester
        # still has something useful to subscribe to.
        logger.warning("priority_watchlist empty — using cold-start fallback")
        rows = await conn.fetch(
            f"""
            SELECT v.mmsi
            FROM vessel_registry v
            LEFT JOIN LATERAL (
                SELECT MAX(fix_ts) AS last_fix
                FROM ais_fixes a
                WHERE a.mmsi = v.mmsi AND a.fix_ts > now() - INTERVAL '90 days'
            ) f ON TRUE
            WHERE (v.is_lng_carrier OR v.is_fsru) AND NOT v.excluded
              AND {_worker_partition_sql(WORKER_ID, WORKER_COUNT, "v.mmsi")}
            ORDER BY f.last_fix DESC NULLS LAST, v.mmsi
            LIMIT $1
            """,
            PERSISTENT_SLOTS,
        )
    return [r["mmsi"] for r in rows]


# A vessel is "scannable" if it is not already held in a persistent slot and is
# either persistent-band overflow (tier<=3 that didn't win a slot — slot_kind is
# NULL or 'scan' from the previous cycle) or tier>=4. slot_kind is one cycle
# stale, so a vessel newly promoted to persistent may be briefly double-covered;
# that is harmless (one wasted scan slot for <=1h) and self-corrects next cycle.
_SCANNABLE = "((tier <= 3 AND (slot_kind IS NULL OR slot_kind = 'scan')) OR tier >= 4)"


async def load_scan_mmsis(pool: asyncpg.Pool) -> list[int]:
    """Next SCAN_SLOTS vessels for the scan-rotation connection, drawn from
    three priority-ordered pools (overflow → tier-4 → tier-5; see the
    SCAN_*_SLOTS constants) with roll-over so the slots are always filled when
    candidates exist. The tier-4/5 pools take the least-recently-scanned vessels
    (`last_scan_window_at ASC NULLS FIRST`); the persistent-band overflow pool is
    instead ordered by score DESC so the closest/most-closing crowded-out
    approachers are swept first (DATA_QUALITY §3 — see the overflow pick below). A
    final write-back stamps the picked batch with now() so it isn't re-picked on
    the next reconnect.

    The write-back is what makes rotation actually rotate: without it, watchdog
    reconnects (~every 5 min when scan vessels are silent) would re-select the
    same MMSIs forever.

    NOT is_pinned throughout: pinned vessels already hold a persistent slot
    (load_persistent_mmsis), so picking them here would waste a scan slot.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            # FSRUs get a dedicated low-frequency quota and are excluded from
            # every other pool (incl. the rollover) so a deployed terminal can't
            # consume a discovery slot. Loaded once per call.
            fsru_mmsis = [
                r["mmsi"]
                for r in await conn.fetch(
                    "SELECT mmsi FROM vessel_registry WHERE is_fsru"
                )
            ]
            picked: list[int] = []

            # This worker's mmsi-modulo share only ('TRUE' when unscaled), so the
            # scan rotation of two workers covers disjoint vessels.
            part = _worker_partition_sql(WORKER_ID, WORKER_COUNT)

            # Default ordering is pure least-recently-scanned rotation, which
            # sweeps a whole pool fairly. The overflow pool overrides it with a
            # closing-aware order (see the overflow pick below).
            rotation_order = "tier ASC, last_scan_window_at ASC NULLS FIRST"

            async def pick(
                where: str,
                quota: int,
                *,
                only_fsru: bool = False,
                order_by: str = rotation_order,
            ) -> None:
                if quota <= 0:
                    return
                fsru_clause = (
                    "AND mmsi = ANY($3::BIGINT[])"
                    if only_fsru
                    else "AND mmsi <> ALL($3::BIGINT[])"
                )
                rows = await conn.fetch(
                    f"""
                    SELECT mmsi FROM priority_watchlist
                    WHERE {where} AND NOT is_pinned AND {part}
                      AND mmsi <> ALL($1::BIGINT[])
                      {fsru_clause}
                    ORDER BY {order_by}
                    LIMIT $2
                    FOR UPDATE
                    """,
                    picked,
                    quota,
                    fsru_mmsis,
                )
                picked.extend(r["mmsi"] for r in rows)

            # Persistent-band overflow first (highest-value unsubscribed
            # vessels), then the tier-4/5 rotation quotas, then the FSRU
            # host-watch quota (lowest priority — relocation confirmation only).
            #
            # The overflow pool is ordered by score DESC (within tier), not pure
            # rotation: these tier<=3 vessels carry a real recent position, so
            # their score already encodes closing-ness (proximity + heading, via
            # scoring._closing_bonus). Scanning the closest/most-closing of the
            # crowded-out approachers first — rather than by luck of last-scan —
            # is the cheap fix for Class-B arrivals (DATA_QUALITY §3). It is a
            # deliberate bias: a far/stale overflow vessel waits longer here, but
            # it can still surface via the rollover pick below. tier-4/5 stay on
            # rotation — a dark tier-5 vessel has no recent fix, so its closing-ness
            # is unknowable and there is nothing to rank it by.
            await pick(
                "tier <= 3 AND (slot_kind IS NULL OR slot_kind = 'scan')",
                SCAN_OVERFLOW_SLOTS,
                order_by="tier ASC, score DESC, last_scan_window_at ASC NULLS FIRST",
            )
            await pick("tier = 4", SCAN_TIER4_SLOTS)
            await pick("tier = 5", SCAN_TIER5_SLOTS)
            await pick("TRUE", SCAN_FSRU_SLOTS, only_fsru=True)
            # Roll over any shortfall onto whatever is scannable, tier-first so
            # leftover overflow is preferred over tier-4 over tier-5. FSRUs stay
            # excluded — their reserved quota is the only path to a scan slot.
            await pick(_SCANNABLE, SCAN_SLOTS - len(picked))

            if picked:
                await conn.execute(
                    "UPDATE priority_watchlist SET last_scan_window_at = now() "
                    "WHERE mmsi = ANY($1::BIGINT[])",
                    picked,
                )
    return picked


async def mark_slot_assignments(
    pool: asyncpg.Pool, persistent: list[int], scan: list[int]
) -> None:
    """Write back which MMSIs won slots this cycle (in_slot/slot_kind/slot_worker
    — the TUI reads these for the tier breakdown panel).

    Each worker clears + sets ONLY its own mmsi-modulo partition, never a global
    clear: with two workers both writing priority_watchlist a global
    `SET in_slot = FALSE` would clobber the other worker's just-set slots. The
    partition is disjoint by construction, so the two clears never collide. At
    WORKER_COUNT=1 the clause is 'TRUE' → a full clear, exactly as before."""
    part = _worker_partition_sql(WORKER_ID, WORKER_COUNT)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                f"UPDATE priority_watchlist "
                f"SET in_slot = FALSE, slot_kind = NULL, slot_worker = NULL "
                f"WHERE {part}"
            )
            if persistent:
                await conn.execute(
                    "UPDATE priority_watchlist "
                    "SET in_slot = TRUE, slot_kind = 'persistent', slot_worker = $2 "
                    "WHERE mmsi = ANY($1::BIGINT[])",
                    persistent,
                    WORKER_ID,
                )
                # Relabel the pinned subset (they live in the persistent block)
                # so the TUI can distinguish forced open-leg pins from tier slots.
                await conn.execute(
                    "UPDATE priority_watchlist SET slot_kind = 'pinned' "
                    "WHERE mmsi = ANY($1::BIGINT[]) AND is_pinned",
                    persistent,
                )
            if scan:
                await conn.execute(
                    "UPDATE priority_watchlist "
                    "SET in_slot = TRUE, slot_kind = 'scan', slot_worker = $2 "
                    "WHERE mmsi = ANY($1::BIGINT[])",
                    scan,
                    WORKER_ID,
                )


def chunk_persistent(mmsis: list[int], num_chunks: int) -> list[list[int]]:
    """Interleave persistent MMSIs across `num_chunks` chunks (the persistent
    connections). The input is already in tier-priority order; interleaving
    spreads activity evenly so no single connection is starved.
    """
    chunks: list[list[int]] = [[] for _ in range(num_chunks)]
    for i, m in enumerate(mmsis):
        chunks[i % num_chunks].append(m)
    return chunks


def parse_message(
    raw: str | bytes,
    ingest_state: IngestionState,
    minute_agg: MinuteAggregator,
) -> None:
    """Pure-CPU: parse, filter, append to buffers, observe stats. No I/O.

    No dynamic enrichment here — the MMSI filter means unknown MMSIs never
    flow through us in the first place. The `non_tanker_mmsis` defensive
    filter is kept as a cheap guard.
    """
    try:
        data = json.loads(raw)
        msg = AISMessage.model_validate(data).root
    except Exception as e:
        logger.warning(f"Discarding invalid message: {e}")
        return

    mmsi = msg.MetaData.MMSI

    if isinstance(msg, PositionReport):
        if mmsi in ingest_state.non_tanker_mmsis:
            return

        ingest_state.fix_buf.append(
            (
                msg.MetaData.time_utc,
                mmsi,
                msg.Message.Latitude,
                msg.Message.Longitude,
                msg.Message.NavigationalStatus,
                msg.Message.Sog,
                msg.Message.Cog,
                ingest_state.source_name,
            )
        )
        zone = classify_zone(msg.Message.Latitude, msg.Message.Longitude)
        lag_s = (datetime.now(timezone.utc) - msg.MetaData.time_utc).total_seconds()
        minute_agg.observe_fix(mmsi, msg.MetaData.time_utc, lag_s, zone)
        if zone is not None:
            # Remember the latest in-zone position for this MMSI this flush;
            # flush_buffers promotes these in one batched UPDATE.
            ingest_state.inzone_mmsi[mmsi] = zone

    elif isinstance(msg, ShipStaticData):
        vessel_type = msg.Message.Type

        if vessel_type is not None and vessel_type not in TANKER_TYPES:
            ingest_state.non_tanker_mmsis.add(mmsi)
            return

        if vessel_type in TANKER_TYPES:
            ingest_state.non_tanker_mmsis.discard(mmsi)

        ingest_state.registry_buf.append(
            (
                mmsi,
                msg.Message.ImoNumber,
                msg.MetaData.ShipName,
                msg.Message.CallSign,
                msg.Message.Type,
            )
        )
        ingest_state.state_buf.append(
            (
                msg.MetaData.time_utc,
                mmsi,
                msg.Message.MaximumStaticDraught,
                msg.Message.Destination,
                json.dumps(msg.Message.Eta) if msg.Message.Eta is not None else None,
                ingest_state.source_name,
            )
        )


# Instant in-zone promotion: any subscribed vessel whose fix lands inside a
# config.ZONES rectangle is promoted to tier INLINE_PROMOTE_TIER immediately,
# so the scan rotation can't drop it before the next hourly scoring pass (the
# SM BLUEBIRD case: discovered by scan at a US terminal, then rotated out before
# promotion). Gated on tier > INLINE_PROMOTE_TIER so it only ever promotes; the
# next scoring pass refines the exact tier. Each promotion is logged to
# tier_promotions (via='inline') with the vessel name + zone for the TUI.
INLINE_PROMOTE_TIER = 3

INLINE_PROMOTE_SQL = """
WITH inz AS (
    SELECT m AS mmsi, z AS zone
    FROM unnest($1::bigint[], $2::text[]) AS t(m, z)
),
promoted AS (
    UPDATE priority_watchlist pw
    SET tier            = $3,
        score           = extract(epoch FROM now()),
        score_reason    = 'live: in ' || inz.zone,
        last_zone_fix_ts = now(),
        computed_at     = now()
    FROM inz
    WHERE pw.mmsi = inz.mmsi AND pw.tier > $3
      -- FSRUs are deliberately held in the low-frequency band; an in-zone fix
      -- (they're always in zone) must not promote them into a persistent slot.
      AND NOT EXISTS (
          SELECT 1 FROM vessel_registry vr
          WHERE vr.mmsi = pw.mmsi AND vr.is_fsru
      )
    RETURNING pw.mmsi, pw.tier AS new_tier, inz.zone
)
INSERT INTO tier_promotions (mmsi, vessel_name, old_tier, new_tier, via, reason, zone)
SELECT p.mmsi, vr.vessel_name, NULL, p.new_tier, 'inline',
       'live fix in ' || p.zone, p.zone
FROM promoted p
LEFT JOIN vessel_registry vr ON vr.mmsi = p.mmsi
"""


async def promote_inzone(conn: asyncpg.Connection, inzone: dict[int, str]) -> None:
    """Promote vessels seen in-zone this flush to INLINE_PROMOTE_TIER (promote
    only) and log each to tier_promotions. One batched round-trip.

    old_tier is recorded NULL: the UPDATE's RETURNING gives the post-update tier,
    and capturing the pre-update tier would need an extra read — not worth it for
    a log row. The TUI renders a NULL old_tier as '·'.
    """
    mmsis = list(inzone.keys())
    zones = [inzone[m] for m in mmsis]
    await conn.execute(INLINE_PROMOTE_SQL, mmsis, zones, INLINE_PROMOTE_TIER)


async def flush_buffers(pool: asyncpg.Pool, ingest_state: IngestionState) -> None:
    """Swap-and-write the in-memory buffers in one batched round-trip per table."""
    fix_batch = ingest_state.fix_buf
    registry_batch = ingest_state.registry_buf
    state_batch = ingest_state.state_buf
    inzone = ingest_state.inzone_mmsi
    if not fix_batch and not registry_batch and not state_batch:
        return
    ingest_state.fix_buf = []
    ingest_state.registry_buf = []
    ingest_state.state_buf = []
    ingest_state.inzone_mmsi = {}

    async with pool.acquire() as conn:
        if fix_batch:
            try:
                await conn.executemany(
                    """
                    INSERT INTO ais_fixes
                        (fix_ts, mmsi, lat, lon, nav_status, sog, cog, source)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (fix_ts, mmsi) DO NOTHING
                    """,
                    fix_batch,
                )
                ingest_state.fix_inserts += len(fix_batch)
            except Exception as e:
                logger.warning(
                    f"Batch fix insert failed ({len(fix_batch)} rows): {e} — "
                    f"re-queuing for retry"
                )
                # Re-queue ahead of anything that arrived during the failed write so
                # a transient DB error doesn't silently drop fixes. Safe to replay:
                # the insert is ON CONFLICT (fix_ts, mmsi) DO NOTHING.
                ingest_state.fix_buf = fix_batch + ingest_state.fix_buf

        if registry_batch:
            try:
                await conn.executemany(
                    """
                    INSERT INTO vessel_registry
                        (mmsi, imo, vessel_name, call_sign, vessel_type)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (mmsi) DO UPDATE SET
                        vessel_name = EXCLUDED.vessel_name,
                        call_sign = EXCLUDED.call_sign,
                        vessel_type = EXCLUDED.vessel_type
                    """,
                    registry_batch,
                )
                ingest_state.registry_upserts += len(registry_batch)
            except Exception as e:
                logger.warning(
                    f"Batch registry upsert failed ({len(registry_batch)} rows): {e}"
                    f" — re-queuing for retry"
                )
                # Idempotent upsert (ON CONFLICT (mmsi) DO UPDATE) — safe to replay.
                ingest_state.registry_buf = registry_batch + ingest_state.registry_buf

        if state_batch:
            try:
                await conn.executemany(
                    """
                    INSERT INTO vessel_state
                        (state_ts, mmsi, draught, dest, eta, source)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (state_ts, mmsi) DO NOTHING
                    """,
                    state_batch,
                )
                ingest_state.state_inserts += len(state_batch)
            except Exception as e:
                logger.warning(
                    f"Batch state insert failed ({len(state_batch)} rows): {e}"
                    f" — re-queuing for retry"
                )
                # ON CONFLICT (state_ts, mmsi) DO NOTHING — safe to replay.
                ingest_state.state_buf = state_batch + ingest_state.state_buf

        if inzone:
            try:
                await promote_inzone(conn, inzone)
            except Exception as e:
                logger.warning(f"Inline in-zone promotion failed: {e}")


async def connect_and_drain(
    url: str,
    pool: asyncpg.Pool,
    ingest_state: IngestionState,
    mmsis: list[int],
    source_name: str,
) -> None:
    """One MMSI-filtered WebSocket lifecycle: subscribe + drain until disconnect.

    Tasks: drain_socket, parser, flusher, watchdog, planned_reconnect.
    No rotation — the MMSI filter is fixed for this connection's lifetime; a
    fresh chunk is loaded from vessel_registry on each reconnect. Liveness is
    derived downstream from `ingestion_stats_minute` per source (see viz/tui.py)
    rather than from a separate heartbeat table.
    """
    minute_agg = MinuteAggregator(source=source_name)
    payload = build_subscribe_payload(settings.aisstream_api_key, mmsis)

    logger.info(f"[{source_name}] Connecting to aisstream.io ({len(mmsis)} MMSIs)...")
    await record_event(pool, source_name, "connect", {"mmsi_count": len(mmsis)})
    async with websockets.connect(url, ping_timeout=None) as ws:
        await ws.send(json.dumps(payload))
        logger.info(f"[{source_name}] Subscribed.")
        await record_event(pool, source_name, "subscribed", {"mmsi_count": len(mmsis)})
        minute_agg.note_connection_start()

        last_message_time = time.monotonic()
        raw_q: asyncio.Queue = asyncio.Queue(maxsize=RAW_QUEUE_MAXSIZE)
        last_logged_fixes = ingest_state.fix_inserts

        async def drain_socket():
            nonlocal last_message_time
            async for raw in ws:
                last_message_time = time.monotonic()
                await raw_q.put(raw)

        async def parser():
            while True:
                raw = await raw_q.get()
                try:
                    parse_message(raw, ingest_state, minute_agg)
                finally:
                    raw_q.task_done()

        async def flusher():
            nonlocal last_logged_fixes
            while True:
                await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
                minute_agg.observe_q_depth(raw_q.qsize())
                try:
                    await flush_buffers(pool, ingest_state)
                except Exception as e:
                    logger.warning(f"Flush failed: {e}")
                try:
                    await minute_agg.maybe_flush(pool)
                except Exception as e:
                    logger.warning(f"Minute-stats flush failed: {e}")
                if ingest_state.fix_inserts // 1000 > last_logged_fixes // 1000:
                    logger.info(
                        f"[{source_name}] fixes={ingest_state.fix_inserts}, "
                        f"registry={ingest_state.registry_upserts}, "
                        f"state={ingest_state.state_inserts}, "
                        f"raw_q={raw_q.qsize()}"
                    )
                    last_logged_fixes = ingest_state.fix_inserts

        async def watchdog():
            while True:
                await asyncio.sleep(15)
                silence = time.monotonic() - last_message_time
                if silence > SILENCE_THRESHOLD_SECONDS:
                    logger.warning(
                        f"[{source_name}] No messages for {silence:.0f}s — triggering reconnect"
                    )
                    await record_event(
                        pool,
                        source_name,
                        "watchdog_reconnect",
                        {"silence_s": int(silence)},
                    )
                    await ws.close()

        async def planned_reconnect():
            """Force a fresh WS after RECONNECT_INTERVAL_SECONDS so the outer
            loop re-queries vessel_registry and picks up new MMSIs."""
            await asyncio.sleep(RECONNECT_INTERVAL_SECONDS)
            logger.info(
                f"[{source_name}] Planned reconnect after "
                f"{RECONNECT_INTERVAL_SECONDS}s — closing ws to refresh watchlist"
            )
            await record_event(
                pool,
                source_name,
                "planned_reconnect",
                {"interval_s": RECONNECT_INTERVAL_SECONDS},
            )
            await ws.close()

        tasks = [
            asyncio.create_task(drain_socket()),
            asyncio.create_task(parser()),
            asyncio.create_task(flusher()),
            asyncio.create_task(watchdog()),
            asyncio.create_task(planned_reconnect()),
        ]
        try:
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            try:
                await flush_buffers(pool, ingest_state)
            except Exception as e:
                logger.warning(f"Final flush on disconnect failed: {e}")
            try:
                await minute_agg.force_flush(pool)
            except Exception as e:
                logger.warning(f"Minute-stats final flush failed: {e}")
            await record_event(pool, source_name, "disconnect")


async def ingest():
    pool = await asyncpg.create_pool(settings.database_url, min_size=2, max_size=8)
    logger.info("DB pool created")

    enrich_state = EnrichmentState(known_mmsis=await load_known_mmsis(pool))
    logger.info(
        f"Pre-loaded {len(enrich_state.known_mmsis)} known MMSIs from vessel_registry"
    )

    url = "wss://stream.aisstream.io/v0/stream"

    # Singleton background tasks must run on exactly ONE worker — they recompute
    # shared state (scoring / port_events) or spend the shared VF credit budget
    # (rescue + masterdata enrichment). The Settings validator defaults each flag
    # to "primary only" (worker 0); this guard refuses an explicit misconfig that
    # would double-spend VF credits from a non-primary worker.
    if settings.worker_id != 0 and settings.run_vf_rescue:
        raise SystemExit(
            f"worker {settings.worker_id}: RUN_VF_RESCUE must be false on a "
            "non-primary worker (VF rescue + enrichment share a finite credit "
            "budget and must run on worker 0 only)."
        )
    logger.info(
        "worker %d/%d · run_scoring=%s run_port_events=%s run_vf_rescue=%s",
        settings.worker_id,
        settings.worker_count,
        settings.run_scoring,
        settings.run_port_events,
        settings.run_vf_rescue,
    )

    bg_tasks: list[asyncio.Task] = []

    # enrichment_worker drains the VesselFinder masterdata queue (VF-credit cost),
    # so it is grouped with the VF singletons (worker 0 only). Under MMSI-only mode
    # no new MMSIs get queued, but keeping it running lets the batch path
    # (`make enrich`) still feed it indirectly.
    if settings.run_vf_rescue:
        bg_tasks.append(asyncio.create_task(enrichment_worker(pool, enrich_state)))

    async def scoring_loop():
        while True:
            await asyncio.sleep(SCORING_INTERVAL_SECONDS)
            try:
                await scoring.compute_and_upsert(pool)
            except Exception as e:
                logger.warning(f"Scoring run failed: {e}")

    if settings.run_scoring:
        # Run scoring once before opening any sockets so the first reconnect has a
        # fresh priority_watchlist, then re-run on the scoring cadence so promoted
        # vessels get persistent slots on the next cycle. If the first run fails
        # (e.g. priority_watchlist not yet migrated), log + continue —
        # load_persistent_mmsis has a cold-start fallback that keeps us useful.
        try:
            await scoring.compute_and_upsert(pool)
        except Exception as e:
            logger.warning(f"Initial scoring run failed: {e}")
        bg_tasks.append(asyncio.create_task(scoring_loop()))

    async def port_events_loop():
        # Periodic full rebuild so derived port_events/legs stay near-live rather
        # than waiting for a manual `make port-events`. run() builds all events
        # off-table then writes via an atomic staging swap (TRUNCATE + bulk INSERT
        # in one transaction), so concurrent readers (the viz API) never see a
        # partial table. Uses wall-clock `now` for stale-envelope closing — the
        # --as-of override is only for reproducible offline rebuilds.
        while True:
            await asyncio.sleep(PORT_EVENTS_INTERVAL_SECONDS)
            try:
                await port_events.run(pool)
            except Exception as e:
                logger.warning(f"port_events rebuild failed: {e}")

    if settings.run_port_events:
        bg_tasks.append(asyncio.create_task(port_events_loop()))

    async def vf_rescue_loop():
        # Backstop for AIS gaps: periodically fetch live positions from
        # VesselFinder for high-value vessels that have gone AIS-silent and inject
        # them as ais_fixes so the pipeline re-acquires them. Credit-budgeted (see
        # ingestion/vf_rescue.py). No initial run — it needs a populated
        # priority_watchlist + port_events, which the loops above produce first.
        while True:
            await asyncio.sleep(vf_rescue.VF_RESCUE_INTERVAL_SECONDS)
            try:
                await vf_rescue.run_rescue(pool)
            except Exception as e:
                logger.warning(f"vf_rescue run failed: {e}")

    if settings.run_vf_rescue:
        bg_tasks.append(asyncio.create_task(vf_rescue_loop()))

    async def connection_loop(source_name: str, chunk_index: int):
        """Reconnect loop owning one MMSI-filtered subscription. On each
        (re)connect:

        - chunk_index < SCAN_CHUNK_INDEX → persistent block (interleaved half
          of the top PERSISTENT_SLOTS by tier/score from priority_watchlist)
        - chunk_index == SCAN_CHUNK_INDEX → scan rotation (next SCAN_SLOTS
          oldest tier-4/5 candidates, rotating each 1h reconnect)
        """
        while True:
            try:
                if chunk_index == SCAN_CHUNK_INDEX:
                    my_mmsis = await load_scan_mmsis(pool)
                    persistent_mmsis = []  # set by other connections' loops, written below for observability only when we're a persistent conn
                else:
                    persistent_mmsis = await load_persistent_mmsis(pool)
                    chunks = chunk_persistent(persistent_mmsis, PERSISTENT_CONNECTIONS)
                    my_mmsis = chunks[chunk_index]
                    # Persistent conn 0 is the only one that writes the slot
                    # assignments — coordinated single-writer, no races.
                    if chunk_index == 0:
                        scan_mmsis = await load_scan_mmsis(pool)
                        try:
                            await mark_slot_assignments(
                                pool, persistent_mmsis, scan_mmsis
                            )
                        except Exception as e:
                            logger.warning(f"mark_slot_assignments failed: {e}")

                if not my_mmsis:
                    logger.warning(f"[{source_name}] empty MMSI chunk; sleeping 60s")
                    await asyncio.sleep(60)
                    continue
                ingest_state = IngestionState(source_name=source_name)
                await connect_and_drain(url, pool, ingest_state, my_mmsis, source_name)
            except websockets.ConnectionClosed as e:
                logger.warning(
                    f"[{source_name}] Websocket closed: {e}. Reconnecting in 30s"
                )
                await record_event(
                    pool,
                    source_name,
                    "error",
                    {"kind": "ConnectionClosed", "msg": str(e)},
                )
                await asyncio.sleep(30)
            except Exception as e:
                logger.warning(
                    f"[{source_name}] Unexpected error: {e}. Reconnecting in 60s"
                )
                await record_event(
                    pool,
                    source_name,
                    "error",
                    {"kind": type(e).__name__, "msg": str(e)},
                )
                await asyncio.sleep(60)

    try:
        await asyncio.gather(
            *[
                connection_loop(_source_label(WORKER_ID, WORKER_COUNT, i), i)
                for i in range(NUM_CONNECTIONS)
            ]
        )
    finally:
        for t in bg_tasks:
            t.cancel()
        await asyncio.gather(*bg_tasks, return_exceptions=True)
        await pool.close()


def main():
    asyncio.run(ingest())


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Ingestion Stopped.")
