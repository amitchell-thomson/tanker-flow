"""Unit tests for the Stage-3 multi-worker sharding helpers.

The partition + label logic must be a strict no-op at WORKER_COUNT=1 (so the
single-worker ingester is byte-identical to pre-sharding) and a clean disjoint
partition at WORKER_COUNT>1.
"""

from __future__ import annotations

from config import Settings
from ingestion import aisstream as a


# --- _worker_partition_sql ----------------------------------------------------
def test_partition_is_noop_when_unscaled():
    # WORKER_COUNT<=1 ⇒ 'TRUE' (the planner folds it away) so SQL is unchanged.
    assert a._worker_partition_sql(0, 1) == "TRUE"
    assert a._worker_partition_sql(0, 0) == "TRUE"


def test_partition_clause_scaled():
    # Hash-based (not raw `mmsi % n`): LNG MMSIs are ~81% even, so a raw modulo
    # imbalances ~4:1. hashtext decorrelates the digit skew.
    assert a._worker_partition_sql(0, 2) == "(((hashtext(mmsi::text) % 2) + 2) % 2 = 0)"
    assert a._worker_partition_sql(1, 2) == "(((hashtext(mmsi::text) % 2) + 2) % 2 = 1)"
    assert (
        a._worker_partition_sql(2, 3, "v.mmsi")
        == "(((hashtext(v.mmsi::text) % 3) + 3) % 3 = 2)"
    )


def test_partition_formula_is_disjoint_and_complete():
    # The clause buckets each vessel with ((hashtext % n) + n) % n, which must
    # land every vessel in exactly one of [0, n) regardless of the hash's sign.
    # The hash itself is Postgres's job; verify the bucketing arithmetic over
    # arbitrary (including negative) hash values.
    wc = 3
    counts = {0: 0, 1: 0, 2: 0}
    for h in range(-100, 100):
        b = ((h % wc) + wc) % wc
        assert 0 <= b < wc  # always a valid worker id
        counts[b] += 1
    assert all(v > 0 for v in counts.values())  # complete: every worker gets some
    assert sum(counts.values()) == 200  # each assigned exactly once


# --- _source_label ------------------------------------------------------------
def test_source_label_single_worker_is_historical():
    assert a._source_label(0, 1, 0) == "aisstream-mmsi-1"
    assert a._source_label(0, 1, 2) == "aisstream-mmsi-3"


def test_source_label_multi_worker_is_unique():
    # The two workers' three conns must not collide in the stats tables.
    assert a._source_label(0, 2, 0) == "aisstream-w0-1"
    assert a._source_label(1, 2, 0) == "aisstream-w1-1"
    assert a._source_label(1, 2, 2) == "aisstream-w1-3"


# --- singleton-flag defaults (config validator) -------------------------------
def test_singleton_flags_default_to_primary_only():
    # Worker 0 (the default) runs every singleton; a non-primary worker that
    # leaves the flags unset runs none (pure ingestion).
    primary = Settings(worker_id=0)
    assert (primary.run_scoring, primary.run_port_events, primary.run_vf_rescue) == (
        True,
        True,
        True,
    )
    secondary = Settings(worker_id=1)
    assert (
        secondary.run_scoring,
        secondary.run_port_events,
        secondary.run_vf_rescue,
    ) == (False, False, False)


def test_singleton_flags_explicit_override_honoured():
    # An explicit env value wins over the primary-only default (e.g. lifting
    # scoring to a non-primary coordinator later).
    w = Settings(worker_id=1, run_scoring=True)
    assert w.run_scoring is True
    assert w.run_vf_rescue is False  # still defaulted off
