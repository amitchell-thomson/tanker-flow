"""Unit tests for retirement classification (pure logic, no DB)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pipeline.retirement import classify_retirements

NOW = datetime(2026, 6, 14, tzinfo=timezone.utc)


def test_long_silent_hull_is_retired():
    rows = [(1, NOW - timedelta(days=400), None)]
    to_retire, to_unretire = classify_retirements(rows, NOW, threshold_days=365)
    assert to_retire == [1] and to_unretire == []


def test_recently_seen_hull_is_not_retired():
    rows = [(1, NOW - timedelta(days=30), None)]
    assert classify_retirements(rows, NOW, 365) == ([], [])


def test_resurfaced_hull_is_unretired():
    # already retired, but a fix landed 10d ago -> reversible
    rows = [(1, NOW - timedelta(days=10), NOW - timedelta(days=200))]
    to_retire, to_unretire = classify_retirements(rows, NOW, 365)
    assert to_retire == [] and to_unretire == [1]


def test_still_silent_retired_hull_stays_retired():
    rows = [(1, NOW - timedelta(days=500), NOW - timedelta(days=100))]
    assert classify_retirements(rows, NOW, 365) == ([], [])


def test_no_fix_row_is_left_untouched():
    # never seen -> insufficient evidence either way (e.g. pre-reload historical row)
    rows = [(1, None, None), (2, None, NOW - timedelta(days=50))]
    assert classify_retirements(rows, NOW, 365) == ([], [])


def test_boundary_exactly_at_threshold_is_not_stale():
    rows = [(1, NOW - timedelta(days=365), None)]
    # cutoff = NOW-365d; last_fix == cutoff is NOT < cutoff -> not stale
    assert classify_retirements(rows, NOW, 365) == ([], [])
