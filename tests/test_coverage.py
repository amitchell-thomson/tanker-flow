"""Unit tests for the coverage panel (data.coverage).

Pure bucket logic only — the recency classification and summary assembly from
plain records. No DB.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from data import coverage as cov

NOW = datetime(2026, 6, 11, 12, 0, tzinfo=UTC)


def _ago(**kw) -> datetime:
    return NOW - timedelta(**kw)


# --- classify_recency ---------------------------------------------------------
def test_classify_recency_buckets():
    assert cov.classify_recency(_ago(hours=6), NOW) == "live"
    assert cov.classify_recency(_ago(days=2), NOW) == "live"  # boundary inclusive
    assert cov.classify_recency(_ago(days=3), NOW) == "stale"
    assert cov.classify_recency(_ago(days=7), NOW) == "stale"  # boundary inclusive
    assert cov.classify_recency(_ago(days=30), NOW) == "blind"
    assert cov.classify_recency(None, NOW) == "unseen"


# --- build_coverage -----------------------------------------------------------
def _fleet():
    return [
        {"mmsi": 1, "last_fix_ts": _ago(hours=1)},  # live
        {"mmsi": 2, "last_fix_ts": _ago(days=1)},  # live
        {"mmsi": 3, "last_fix_ts": _ago(days=4)},  # stale
        {"mmsi": 4, "last_fix_ts": _ago(days=20)},  # blind
        {"mmsi": 5, "last_fix_ts": None},  # unseen
    ]


def test_build_coverage_buckets_and_totals():
    s = cov.build_coverage(
        _fleet(),
        [
            {"tier": 1, "n": 30, "in_slot": 30},
            {"tier": 5, "n": 500, "in_slot": 10},
        ],
        {"moored": 100, "cold": 8},
        {"today": 3, "week": 11},
        NOW,
    )
    assert s.fleet_total == 5
    assert s.buckets == {"live": 2, "stale": 1, "blind": 1, "unseen": 1}
    assert s.heard_rate == 3 / 5  # live + stale
    assert s.in_slot_total == 40
    assert s.cold_start_rate == 0.08
    assert s.unmet_today == 3 and s.unmet_week == 11


def test_build_coverage_handles_empty_inputs():
    s = cov.build_coverage([], [], None, None, NOW)
    assert s.fleet_total == 0
    assert s.buckets == {"live": 0, "stale": 0, "blind": 0, "unseen": 0}
    assert s.heard_rate is None  # no fleet ⇒ undefined, not a div-by-zero
    assert s.cold_start_rate is None  # no moorings ⇒ undefined
    assert s.in_slot_total == 0
    assert s.unmet_today == 0 and s.unmet_week == 0


def test_render_runs_and_reports_key_numbers():
    s = cov.build_coverage(
        _fleet(),
        [{"tier": 1, "n": 30, "in_slot": 30}],
        {"moored": 10, "cold": 2},
        {"today": 1, "week": 4},
        NOW,
    )
    out = cov.render(s, NOW)
    assert "Fleet coverage" in out
    assert "Watchlist tiers" in out
    assert "Cold-start" in out
    assert "20.0%" in out  # cold-start rate 2/10
