"""Unit tests for the §3.6.1 registry-completion categorisation (pure logic).

No DB, no archive — synthetic BerthHull dicts exercise classify_gap, especially
the MMSI-reuse collision guard that protects live rows.
"""

from __future__ import annotations

from datetime import datetime, timezone

from scripts.complete_registry_from_archive import (
    BerthHull,
    classify_gap,
    is_registerable_type,
)


def test_registerable_type_accepts_lng_family():
    # LNG carriers since converted to floating storage read as FSO/FSU in VF's
    # current masterdata but were real LNG voyages in the archive.
    assert is_registerable_type("LNG Tanker")
    assert is_registerable_type("FSO")
    assert is_registerable_type("FSU")


def test_registerable_type_rejects_non_lng_and_unknown():
    assert not is_registerable_type("Oil/Chemical Tanker")
    assert not is_registerable_type("Crude Oil Tanker")
    assert not is_registerable_type(None)


def hull(imo, mmsis, names=("X",)):
    return BerthHull(
        imo=imo,
        mmsis=set(mmsis),
        names=set(names),
        last_ts=datetime(2023, 1, 1, tzinfo=timezone.utc),
        n_fixes=len(mmsis),
    )


def test_in_scope_is_skipped():
    hulls = {100: hull(100, [1])}
    out = classify_gap(hulls, imo_class={100: (True, False)}, mmsi_to_imo={1: 100})
    assert [d.category for d in out] == ["in_scope"]
    assert out[0].register_mmsis == []


def test_fsru_is_by_design_not_a_gap():
    hulls = {200: hull(200, [2])}
    out = classify_gap(hulls, imo_class={200: (False, True)}, mmsi_to_imo={2: 200})
    assert out[0].category == "fsru"
    assert out[0].register_mmsis == []


def test_absent_hull_registers_its_archive_mmsi():
    hulls = {300: hull(300, [30])}
    out = classify_gap(hulls, imo_class={}, mmsi_to_imo={})
    assert out[0].category == "absent"
    assert out[0].register_mmsis == [30]
    assert out[0].collision_mmsis == []


def test_present_but_unflagged_is_reflag():
    # in registry (mmsi 40 -> imo 400) but neither lng nor fsru
    hulls = {400: hull(400, [40])}
    out = classify_gap(hulls, imo_class={400: (False, False)}, mmsi_to_imo={40: 400})
    assert out[0].category == "reflag"
    # same-IMO MMSI is registerable (upsert flips is_lng_carrier on the existing row)
    assert out[0].register_mmsis == [40]


def test_mmsi_reuse_collision_is_skipped_not_overwritten():
    # archive shows imo 500 used mmsi 50; but mmsi 50 now belongs to live imo 999
    hulls = {500: hull(500, [50, 51])}
    out = classify_gap(hulls, imo_class={}, mmsi_to_imo={50: 999})
    d = out[0]
    assert d.category == "absent"
    assert d.collision_mmsis == [50]  # protected — never clobbered
    assert d.register_mmsis == [51]  # the free MMSI still recovers


def test_zero_mmsi_is_dropped():
    hulls = {600: hull(600, [0, 60])}
    out = classify_gap(hulls, imo_class={}, mmsi_to_imo={})
    assert out[0].register_mmsis == [60]  # imo=0 sentinel excluded
