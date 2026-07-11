"""Unit tests for data.pipeline_health.

Pure roster construction + the liveness / threshold classifiers that back the
web Health tab. No DB, no textual — everything here is a function of scalars.
"""

from __future__ import annotations

from data import pipeline_health as ph


# --- build_connections --------------------------------------------------------
def test_roster_single_worker():
    conns = ph.build_connections(1)
    # NUM_CONNECTIONS per worker (3): two persistent + one scan.
    assert len(conns) == 3
    assert [c.role for c in conns] == ["persistent", "persistent", "scan"]
    # Single-worker keeps the historical 'aisstream-mmsi-{1,2,3}' labels.
    assert [c.source for c in conns] == [
        "aisstream-mmsi-1",
        "aisstream-mmsi-2",
        "aisstream-mmsi-3",
    ]
    assert all(c.egress == "home" for c in conns)
    # Only the scan conn is sparse (idle-tolerant).
    assert [c.sparse for c in conns] == [False, False, True]


def test_roster_two_workers():
    conns = ph.build_connections(2)
    assert len(conns) == 6
    assert [c.egress for c in conns] == ["home"] * 3 + ["oracle"] * 3
    # Multi-worker uses the collision-free 'aisstream-w{id}-{n}' labels.
    assert conns[0].source == "aisstream-w0-1"
    assert conns[5].source == "aisstream-w1-3"
    # Each worker has exactly one scan (sparse) connection.
    assert sum(c.sparse for c in conns) == 2
    assert ph.build_connections(2)[3].role == "persistent"


def test_home_scan_source_is_worker0_scan():
    # HOME_SCAN_SOURCE is derived at import time from the configured roster; it
    # must be a worker-0 scan connection when one exists.
    assert ph.HOME_SCAN_SOURCE in {c.source for c in ph.CONNECTIONS}
    scan0 = next(c for c in ph.CONNECTIONS if c.worker == 0 and c.role == "scan")
    assert ph.HOME_SCAN_SOURCE == scan0.source


# --- conn_state ---------------------------------------------------------------
def test_conn_state_up_on_recent_fix():
    # A recent fix (<120s) is 'up' regardless of role or event age.
    assert ph.conn_state("persistent", 10, 1e9) == "up"
    assert ph.conn_state("scan", 119, 1e9) == "up"


def test_conn_state_persistent_idle_then_down():
    # No recent fix: persistent tolerates only a 600s event grace.
    assert ph.conn_state("persistent", 500, 300) == "idle"
    assert ph.conn_state("persistent", 500, 900) == "down"


def test_conn_state_scan_wider_grace():
    # Scan (sparse) gets the full reconnect grace before it reads as down.
    assert ph.conn_state("scan", 500, ph.RECONNECT_GRACE_S - 1) == "idle"
    assert ph.conn_state("scan", 500, ph.RECONNECT_GRACE_S + 1) == "down"


# --- aggregate_liveness -------------------------------------------------------
def test_aggregate_liveness_words():
    assert ph.aggregate_liveness(["up", "up", "up"]) == "live"
    assert ph.aggregate_liveness(["up", "idle"]) == "alive"
    assert ph.aggregate_liveness(["up", "down"]) == "degraded"
    assert ph.aggregate_liveness(["down", "down"]) == "down"


# --- colour / threshold classifiers -------------------------------------------
def test_lag_color():
    assert ph.lag_color(5) == "green"
    assert ph.lag_color(10) == "yellow"
    assert ph.lag_color(29) == "yellow"
    assert ph.lag_color(30) == "red"


def test_missing_color_role_aware():
    # Persistent: strict (10/30). Sparse: lenient (60/85).
    assert ph.missing_color(5, sparse=False) == "green"
    assert ph.missing_color(20, sparse=False) == "yellow"
    assert ph.missing_color(40, sparse=False) == "red"
    assert ph.missing_color(40, sparse=True) == "green"
    assert ph.missing_color(70, sparse=True) == "yellow"
    assert ph.missing_color(90, sparse=True) == "red"


def test_watchdog_ok():
    assert ph.watchdog_ok(0, 0, sparse=False) is True
    assert ph.watchdog_ok(10, 0, sparse=True) is True  # sparse never faults
    assert ph.watchdog_ok(1, 24, sparse=False) is True  # within half of planned
    assert ph.watchdog_ok(20, 24, sparse=False) is False


def test_scoring_health():
    assert ph.scoring_health(None) == "dim"
    assert ph.scoring_health(60) == "green"
    assert ph.scoring_health(4000) == "yellow"
    assert ph.scoring_health(6000) == "red"


def test_watchlist_bucket():
    assert ph.watchlist_bucket(None, False, None) == "dormant"
    assert ph.watchlist_bucket(600, False, None) == "reporting"
    # Near a terminal, slow, quiet <24h → real silence.
    assert ph.watchlist_bucket(3600, True, 0.2) == "silent"
    # Same age but moving fast → just transiting out of range.
    assert ph.watchlist_bucket(3600, True, 12.0) == "dormant"
    # Near-terminal but stale >24h → dormant.
    assert ph.watchlist_bucket(90000, True, 0.0) == "dormant"
