"""Tests for the berth-discovery VF-result classifier.

The registration gate is pure: given a VF VESSELS result, decide (is_lng,
vf_type, credits). The DB-bound parts (berth spatial select, registry upsert)
are covered by import_igu_fleet's path and exercised live.
"""

from scripts.discover_berth_tankers import VF_RECORD_CREDITS, classify_vf_result


def test_lng_tanker_is_registerable_and_billed():
    result = {"MASTERDATA": {"TYPE": "LNG Tanker", "NAME": "ORION HUGO"}, "AIS": {}}
    is_lng, vf_type, credits = classify_vf_result(result)
    assert is_lng is True
    assert vf_type == "LNG Tanker"
    assert credits == VF_RECORD_CREDITS


def test_non_lng_tanker_is_rejected_but_still_billed():
    # A chemical tanker that clipped a berth: VF returns a record (3 credits),
    # but the type-check rejects it — we do NOT register it.
    result = {"MASTERDATA": {"TYPE": "Chemical/Oil Products Tanker"}, "AIS": {}}
    is_lng, vf_type, credits = classify_vf_result(result)
    assert is_lng is False
    assert vf_type == "Chemical/Oil Products Tanker"
    assert credits == VF_RECORD_CREDITS  # billed-per-record, even when rejected


def test_fsru_is_not_an_lng_tanker():
    # VF classifies FSRUs as 'Offshore Support Vessel'; berth-discovery only adds
    # LNG Tankers, so an FSRU caught in a berth is rejected here.
    result = {"MASTERDATA": {"TYPE": "Offshore Support Vessel"}, "AIS": {}}
    is_lng, vf_type, credits = classify_vf_result(result)
    assert is_lng is False
    assert vf_type == "Offshore Support Vessel"


def test_vf_miss_is_free_and_not_lng():
    # 404 / empty body — VF doesn't know this IMO. No record, no charge.
    for miss in (None, {}):
        is_lng, vf_type, credits = classify_vf_result(miss)
        assert is_lng is False
        assert vf_type is None
        assert credits == 0


def test_record_without_type_is_not_lng():
    # Master present but TYPE missing — treat as not-LNG (still a returned record).
    result = {"MASTERDATA": {"NAME": "MYSTERY"}, "AIS": {}}
    is_lng, vf_type, credits = classify_vf_result(result)
    assert is_lng is False
    assert vf_type is None
    assert credits == VF_RECORD_CREDITS
