"""Pipeline-health primitives: the connection roster + the pure liveness /
threshold classifiers that drive the web Health tab (viz/static/js/health.js via
the /api/health/* endpoints in viz/app.py).

This logic used to live inside the Textual TUI (viz/tui.py, now retired). It is
config-derived, not stored in the DB: the WebSocket-connection roster is a
function of `config.settings.worker_count` and the AISstream sharding constants,
and the health states are pure functions of scalar ages/rates. Homing it here —
beside data/coverage.py and data/capture_rate.py, the other read-only rollup
helpers — keeps the API as a thin SQL+serialize layer and makes the classifiers
trivially unit-testable (tests/test_pipeline_health.py).

Imports only `config.settings` + `ingestion.aisstream` — no `textual`, no DB
pool — so viz/app.py imports it cleanly.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from config import settings
from ingestion.aisstream import NUM_CONNECTIONS, _source_label

# --- The connection plan: one descriptor per live WebSocket -------------------
# The connections are N egress IPs × NUM_CONNECTIONS each (AISstream caps 3/IP).
# Per worker, chunks 0..N-2 are the persistent block and the last chunk is the
# scan rotation. Drives the connections table, status dots, overnight cache, and
# reconnect counts — all from one source of truth.
EGRESS_NAMES = {0: "home", 1: "oracle"}

# Reconnect cadence (planned) — used to judge "connected but idle" for the
# sparse (scan-rotation) connections, which can legitimately go a while between
# fixes. ~70 min, covers the 1h planned-reconnect cycle.
RECONNECT_GRACE_S = 4200


@dataclass(frozen=True)
class Conn:
    source: str  # ais_fixes.source label this connection writes
    worker: int
    egress: str  # human egress name (home / oracle)
    role: str  # 'persistent' | 'scan'
    covers: str  # one-line "what does it do"
    sparse: bool  # idle minutes are normal (scan) → wider tolerances


def build_connections(worker_count: int) -> list[Conn]:
    conns: list[Conn] = []
    for w in range(worker_count):
        egress = EGRESS_NAMES.get(w, f"egress-{w}")
        half = f" · half {chr(65 + w)}" if worker_count > 1 else ""
        for c in range(NUM_CONNECTIONS):
            if c < NUM_CONNECTIONS - 1:
                conns.append(
                    Conn(
                        _source_label(w, worker_count, c),
                        w,
                        egress,
                        "persistent",
                        f"top tiers 1-3{half}",
                        False,
                    )
                )
                continue
            # Last chunk: scan rotation.
            conns.append(
                Conn(
                    _source_label(w, worker_count, c),
                    w,
                    egress,
                    "scan",
                    f"tier 4/5 rotation{half}",
                    True,
                )
            )
    return conns


WORKER_COUNT = max(1, settings.worker_count)
CONNECTIONS = build_connections(WORKER_COUNT)
# Kept for the aggregate/overnight loops that key off source labels.
EXPECTED_SOURCES = [c.source for c in CONNECTIONS]
# This (home) worker's scan-rotation connection (if any) — its last `subscribed`
# event drives the scan-rotation countdown.
HOME_SCAN_SOURCE = next(
    (c.source for c in CONNECTIONS if c.worker == 0 and c.role == "scan"), None
)


# --- Liveness / threshold classifiers (pure functions) ------------------------


def conn_state(role: str, fix_age_s: float, evt_age_s: float) -> str:
    """Per-connection health: 'up' (data flowing), 'idle' (connected, no recent
    fix — normal for the sparse scan rotation), or 'down'. Role-aware so an idle
    scan conn isn't mistaken for an outage, while a silent persistent conn is."""
    if fix_age_s < 120:
        return "up"
    sparse = role == "scan"
    if evt_age_s < (RECONNECT_GRACE_S if sparse else 600):
        return "idle"
    return "down"


def aggregate_liveness(states: Iterable[str]) -> str:
    """Roll a set of per-connection states into one word for the status strip:
    'live' (all up), 'alive' (some idle, none down), 'degraded' (some down but
    some still up/idle), 'down' (nothing up)."""
    states = list(states)
    up = states.count("up")
    idle = states.count("idle")
    dead = states.count("down")
    if dead == 0 and idle == 0:
        return "live"
    if dead == 0:
        return "alive"
    if up + idle > 0:
        return "degraded"
    return "down"


def lag_color(p95_s: float) -> str:
    """Colour band for a p95 ingest-lag value (seconds)."""
    return "green" if p95_s < 10 else "yellow" if p95_s < 30 else "red"


def missing_color(missing_pct: float, sparse: bool) -> str:
    """Colour band for a 12h missing-minute percentage. Sparse (scan-rotation)
    connections are idle most minutes by design, so tolerate far more."""
    lo, hi = (60, 85) if sparse else (10, 30)
    return "green" if missing_pct < lo else "yellow" if missing_pct < hi else "red"


def watchdog_ok(wd: int, planned: int, sparse: bool) -> bool:
    """Whether the 12h watchdog-reconnect count is within tolerance. Planned
    reconnects run ≈1/h; a watchdog rate above half the planned rate on a
    persistent conn signals AISstream forcing drops."""
    return wd == 0 or sparse or wd <= max(1, planned // 2)


def scoring_health(age_s: int | None) -> str:
    """Colour band for the scoring-loop heartbeat age (seconds). Scoring runs
    every 60 min; >90 min means the background task is stalled."""
    if age_s is None:
        return "dim"
    if age_s > 5400:
        return "red"
    if age_s > 3900:
        return "yellow"
    return "green"


def watchlist_bucket(age_s: int | None, near_terminal: bool, sog: float | None) -> str:
    """Bucket a watched vessel's last-fix age into reporting / silent / dormant.
    'silent' is restricted to strong evidence of a real silence (near a terminal,
    slow, quiet <24h) rather than a vessel simply sailing out of terrestrial-AIS
    range."""
    if age_s is None:
        return "dormant"
    if age_s < 1800:
        return "reporting"
    if age_s < 86400 and near_terminal and (sog is None or sog < 8.0):
        return "silent"
    return "dormant"


# --- Shared label / palette dicts (echoed by the API) -------------------------

# Tier-name labels — drawn from pipeline/scoring.py definitions. Keep in sync if
# those rules change.
TIER_LABELS: dict[int, str] = {
    1: "in zone (polygon)",
    2: "declared inbound",
    3: "in zone (bbox)",
    4: "recent anywhere",
    5: "stale / unseen",
}

# Unified tier palette — CSS colour names mirroring the web frontend's tier ring
# colours so the surfaces read the same.
TIER_PALETTE: dict[int, str] = {
    1: "green",
    2: "yellow",
    3: "yellow",
    4: "white",
    5: "grey",
}

# Watchlist explorer sort modes: label -> ORDER BY SQL. The label is whitelisted
# server-side (the ORDER BY is interpolated, so unknown keys are rejected).
# `dest_terminal_name` is the alias used in the explorer SELECT.
EXPLORER_SORTS: dict[str, str] = {
    "tier": "tier ASC, score DESC",
    "score": "score DESC",
    "last_fix": "last_fix_ts DESC NULLS LAST",
    "dest": "dest_terminal_name ASC NULLS LAST, last_fix_ts DESC NULLS LAST",
    "name": "LOWER(COALESCE(vessel_name,'')) ASC",
}
