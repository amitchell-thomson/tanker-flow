"""Unit tests for ingestion.vf_rescue pure functions.

No DB / no network — covers the load-bearing conversions, the VF model's
sentinel-nulling, candidate classification boundaries, credit arithmetic, the
position sanity gate, and the budget planners.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from ingestion import vf_rescue as vr
from ingestion.models import VesselFinderLiveResponse
from pipeline.scoring import _parse_eta

NOW = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)


def ago(hours: float) -> datetime:
    return NOW - timedelta(hours=hours)


# --- parse_vf_timestamp -------------------------------------------------------
@pytest.mark.parametrize(
    "raw,expected",
    [
        (
            "2017-08-11 11:15:15 UTC",
            datetime(2017, 8, 11, 11, 15, 15, tzinfo=timezone.utc),
        ),
        ("2017-08-11 11:15:15", datetime(2017, 8, 11, 11, 15, 15, tzinfo=timezone.utc)),
        ("", None),
        (None, None),
        ("not a timestamp", None),
    ],
)
def test_parse_vf_timestamp(raw, expected):
    assert vr.parse_vf_timestamp(raw) == expected


# --- vf_eta_to_ais_dict (+ round-trip through scoring._parse_eta) -------------
def test_vf_eta_to_ais_dict_shape():
    assert vr.vf_eta_to_ais_dict("2026-06-03 14:30:00") == (
        '{"Month": 6, "Day": 3, "Hour": 14, "Minute": 30}'
    )


@pytest.mark.parametrize("raw", [None, "", "0000-00-00 00:00:00", "garbage"])
def test_vf_eta_to_ais_dict_none(raw):
    assert vr.vf_eta_to_ais_dict(raw) is None


def test_vf_eta_roundtrips_through_parse_eta():
    """The whole point of the conversion: scoring._parse_eta must accept it."""
    js = vr.vf_eta_to_ais_dict("2026-06-03 14:30:00")
    parsed = _parse_eta(js, NOW)
    assert parsed == datetime(2026, 6, 3, 14, 30, tzinfo=timezone.utc)


# --- VesselFinderAIS model sentinels ------------------------------------------
def _ais(**overrides):
    base = {
        "MMSI": 304491000,
        "IMO": 9175717,
        "TIMESTAMP": "2026-06-01 11:15:15 UTC",
        "LATITUDE": 40.07,
        "LONGITUDE": 154.48,
        "COURSE": 285.6,
        "SPEED": 14.0,
        "HEADING": 286,
        "NAVSTAT": 0,
        "DESTINATION": "ROTTERDAM",
        "LOCODE": "NLRTM",
        "ETA": "2026-06-03 03:00:00",
        "DRAUGHT": 8.7,
        "SRC": "TER",
    }
    base.update(overrides)
    return {"AIS": base}


def test_model_parses_and_ignores_extras():
    ais = VesselFinderLiveResponse.model_validate(
        _ais(NAME="MARENO", ZONE="North Pacific Ocean", ECA=False)
    ).AIS
    assert ais.MMSI == 304491000
    assert ais.LOCODE == "NLRTM"
    assert ais.SRC == "TER"


def test_model_sentinel_nulling():
    ais = VesselFinderLiveResponse.model_validate(
        _ais(COURSE=360.0, HEADING=511, DRAUGHT=0.0, LOCODE=None)
    ).AIS
    assert ais.COURSE is None
    assert ais.HEADING is None
    assert ais.DRAUGHT is None
    assert ais.LOCODE is None


# --- credits ------------------------------------------------------------------
def test_row_credits():
    assert vr.row_credits("TER") == 1
    assert vr.row_credits("SAT") == 10
    assert vr.row_credits(None) == 1


def test_credits_for_rows():
    rows = [
        VesselFinderLiveResponse.model_validate(_ais(SRC="TER")).AIS,
        VesselFinderLiveResponse.model_validate(_ais(SRC="SAT")).AIS,
    ]
    assert vr.credits_for_rows(rows) == 11
    assert vr.credits_for_rows([]) == 0


# --- position_sanity ----------------------------------------------------------
def test_sanity_ok():
    assert (
        vr.position_sanity(
            vf_fix_ts=NOW,
            vf_lat=52.0,
            vf_lon=4.0,
            last_fix_ts=ago(10),
            last_lat=52.1,
            last_lon=4.1,
            now=NOW,
        )
        == "ok"
    )


def test_sanity_ok_no_last_position():
    # First acquisition: teleport gate skipped.
    assert (
        vr.position_sanity(
            vf_fix_ts=NOW,
            vf_lat=52.0,
            vf_lon=4.0,
            last_fix_ts=None,
            last_lat=None,
            last_lon=None,
            now=NOW,
        )
        == "ok"
    )


def test_sanity_stale_by_age():
    assert (
        vr.position_sanity(
            vf_fix_ts=ago(5),
            vf_lat=52.0,
            vf_lon=4.0,
            last_fix_ts=ago(30),
            last_lat=52.0,
            last_lon=4.0,
            now=NOW,
        )
        == "rejected_stale"
    )


def test_sanity_stale_not_newer():
    # VF position older-or-equal than what we already have.
    assert (
        vr.position_sanity(
            vf_fix_ts=ago(2),
            vf_lat=52.0,
            vf_lon=4.0,
            last_fix_ts=ago(1),
            last_lat=52.0,
            last_lon=4.0,
            now=NOW,
        )
        == "rejected_stale"
    )


def test_sanity_teleport():
    # ~2500 nm in 1h ⇒ absurd implied speed.
    assert (
        vr.position_sanity(
            vf_fix_ts=NOW,
            vf_lat=10.0,
            vf_lon=4.0,
            last_fix_ts=ago(1),
            last_lat=52.0,
            last_lon=4.0,
            now=NOW,
        )
        == "rejected_teleport"
    )


# --- is_closing / is_settled --------------------------------------------------
def test_is_closing_toward_within_range():
    assert vr.is_closing(last_cog=90.0, bearing_deg=90.0, near_km=40.0) is True
    assert (
        vr.is_closing(last_cog=90.0, bearing_deg=120.0, near_km=40.0) is True
    )  # 30° off


def test_is_closing_away_or_far_or_nocog():
    assert (
        vr.is_closing(last_cog=90.0, bearing_deg=270.0, near_km=40.0) is False
    )  # opposite
    assert (
        vr.is_closing(last_cog=90.0, bearing_deg=90.0, near_km=80.0) is False
    )  # too far
    assert (
        vr.is_closing(last_cog=None, bearing_deg=90.0, near_km=40.0) is False
    )  # no cog


def test_is_settled():
    assert vr.is_settled(navstat=5, speed=0.0) is True  # moored
    assert vr.is_settled(navstat=1, speed=0.2) is True  # at anchor
    assert vr.is_settled(navstat=0, speed=0.3) is True  # stopped
    assert vr.is_settled(navstat=0, speed=3.4) is False  # moving (Fedor Litke berthing)
    assert vr.is_settled(navstat=None, speed=None) is False


# --- classify_candidate (signal-framed: near-terminal geometry + band) --------
def _classify(**overrides):
    base = dict(
        mmsi=1,
        imo=2,
        vessel_name="X",
        last_fix_ts=ago(8),
        last_lat=1.0,
        last_lon=2.0,
        near_flow="import",
        near_km=20.0,  # near (<25) but not final-approach (<15)
        last_cog=None,
        bearing_deg=None,
        last_event_type=None,
        last_event_flow=None,
        now=NOW,
    )
    base.update(overrides)
    return vr.classify_candidate(**base)


def test_classify_never_seen_excluded():
    assert _classify(last_fix_ts=None) is None


def test_classify_below_min_silence():
    # near but not final-approach ⇒ 4h threshold; 3h silence is too fresh.
    assert _classify(last_fix_ts=ago(3)) is None


def test_classify_above_staleness_ceiling():
    assert _classify(last_fix_ts=ago(60)) is None  # > STALE_CEILING_HOURS (48)


def test_classify_not_coastal_excluded():
    # Beyond NEAR_KM, no open visit, not closing ⇒ no event at risk.
    assert _classify(near_km=500.0) is None


def test_classify_import_arrival():
    c = _classify(near_flow="import", near_km=20.0)
    assert c is not None and c.rescue_class == "import_arrival"


def test_classify_export_arrival():
    c = _classify(near_flow="export", near_km=20.0)
    assert c is not None and c.rescue_class == "export_arrival"


def test_classify_import_berth_open_visit():
    # Fedor Litke shape: in an import zone, last event anchorage_exit (not a
    # departure) ⇒ awaiting `moored`.
    c = _classify(
        near_flow="import",
        near_km=0.0,
        last_event_type="anchorage_exit",
        last_event_flow="import",
    )
    assert c is not None and c.rescue_class == "import_berth"


def test_classify_export_departure_open_visit():
    c = _classify(
        near_flow="export",
        near_km=0.0,
        last_event_type="moored",
        last_event_flow="export",
    )
    assert c is not None and c.rescue_class == "export_departure"


# --- #3: fast final-approach trigger ------------------------------------------
def test_classify_fast_trigger_in_approach():
    # In the approach envelope (<15 km): a 2.5h silence already qualifies...
    assert _classify(near_km=5.0, last_fix_ts=ago(2.5)) is not None
    # ...but the same silence does NOT qualify when merely near (20 km).
    assert _classify(near_km=20.0, last_fix_ts=ago(2.5)) is None


# --- #3: closing inclusion beyond NEAR_KM -------------------------------------
def test_classify_closing_beyond_near_km():
    # 40 km out (> NEAR_KM) but heading at the terminal ⇒ included as arrival,
    # and closing ⇒ fast trigger (2.5h silence qualifies).
    c = _classify(near_km=40.0, last_cog=90.0, bearing_deg=90.0, last_fix_ts=ago(2.5))
    assert c is not None and c.rescue_class == "import_arrival"


def test_classify_far_not_closing_excluded():
    # 40 km out, heading away ⇒ not closing, not near ⇒ excluded.
    assert _classify(near_km=40.0, last_cog=270.0, bearing_deg=90.0) is None


# --- last_event tie-break (cold-start cluster) --------------------------------
def test_candidate_sql_breaks_last_event_ties_by_id():
    """A cold-start cluster can emit zone_entry..zone_exit at one timestamp.
    Without an id tiebreaker, DISTINCT ON could resolve last_event to the
    cluster's `zone_entry` and make a long-departed vessel look like an open
    visit (the spurious PRISM DIVERSITY rescue). The last_event CTE must order
    event_time DESC THEN id DESC so the most-final event (highest id, inserted
    in DFA order) wins the tie."""
    assert "ORDER BY pe.mmsi, pe.event_time DESC, pe.id DESC" in vr.CANDIDATE_SQL


# --- merge_candidates (#4/#5 source merge) ------------------------------------
def _cand(mmsi, cls):
    return vr.Candidate(
        mmsi=mmsi,
        imo=mmsi,
        vessel_name="X",
        last_fix_ts=ago(10),
        last_lat=1.0,
        last_lon=2.0,
        rescue_class=cls,
        silent_h=10.0,
    )


def test_merge_dedups_keeping_highest_priority():
    silence = [_cand(1, "import_arrival"), _cand(2, "import_berth")]
    dest = [_cand(2, "dest_capture"), _cand(3, "dest_capture")]
    merged = {c.mmsi: c.rescue_class for c in vr.merge_candidates(silence, dest)}
    assert merged == {
        1: "import_arrival",
        2: "import_berth",  # priority 1 beats dest_capture's... both are 1; first wins
        3: "dest_capture",
    }


def test_merge_higher_priority_wins():
    # outage_check (0) should beat dest_capture (1) for the same mmsi.
    merged = vr.merge_candidates([_cand(5, "dest_capture")], [_cand(5, "outage_check")])
    assert len(merged) == 1 and merged[0].rescue_class == "outage_check"


# --- rescue_result (#4 dest-capture success semantics) ------------------------
def test_rescue_result_position_ok():
    assert (
        vr.rescue_result(
            position_ok=True,
            rescue_class="import_arrival",
            dest_obtained=False,
            position_status="ok",
        )
        == "rescued"
    )


def test_rescue_result_dest_capture_succeeds_on_dest():
    # Visible vessel: position redundant (rejected_stale) but we got the dest.
    assert (
        vr.rescue_result(
            position_ok=False,
            rescue_class="dest_capture",
            dest_obtained=True,
            position_status="rejected_stale",
        )
        == "rescued"
    )


def test_rescue_result_dest_capture_no_dest():
    assert (
        vr.rescue_result(
            position_ok=False,
            rescue_class="dest_capture",
            dest_obtained=False,
            position_status="rejected_stale",
        )
        == "rejected_stale"
    )


def test_rescue_result_event_class_rejected():
    assert (
        vr.rescue_result(
            position_ok=False,
            rescue_class="import_arrival",
            dest_obtained=True,
            position_status="rejected_teleport",
        )
        == "rejected_teleport"
    )


# --- terrestrial budget -------------------------------------------------------
def test_terrestrial_budget_trims_to_remaining():
    assert vr.terrestrial_budget(spent=18, cap=20, n_candidates=10) == 2
    assert vr.terrestrial_budget(spent=20, cap=20, n_candidates=10) == 0
    assert vr.terrestrial_budget(spent=0, cap=20, n_candidates=5) == 5
