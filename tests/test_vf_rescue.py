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


# --- #7 imminent-ETA rescue window --------------------------------------------
def test_eta_within_rescue_window():
    # Imminent (within the forward horizon) and just-arrived (within past grace).
    assert vr.eta_within_rescue_window(NOW + timedelta(hours=6), NOW) is True
    assert vr.eta_within_rescue_window(NOW - timedelta(hours=6), NOW) is True
    assert vr.eta_within_rescue_window(NOW, NOW) is True


def test_eta_outside_rescue_window():
    # Too far ahead (beyond ETA_RESCUE_HORIZON_HOURS) and long past (beyond grace).
    assert vr.eta_within_rescue_window(NOW + timedelta(hours=48), NOW) is False
    assert vr.eta_within_rescue_window(NOW - timedelta(hours=48), NOW) is False
    assert vr.eta_within_rescue_window(None, NOW) is False


def test_eta_silence_band_is_well_formed_and_above_voyage_window():
    # The candidate SQL filters on [MIN, MAX] silence. The MAX ceiling drops
    # weeks-stale re-inferred ETAs; it must sit above the longest US->EU voyage
    # window (a vessel silent <= one voyage could still be genuinely en route),
    # else the gate would discard real in-progress approaches.
    from pipeline.scoring import EXPECTED_VOYAGE_DAYS

    assert vr.ETA_RESCUE_MIN_SILENCE_HOURS < vr.ETA_RESCUE_MAX_SILENCE_HOURS
    assert vr.ETA_RESCUE_MAX_SILENCE_HOURS >= max(EXPECTED_VOYAGE_DAYS.values()) * 24
    # The SQL must actually bind the ceiling as the second parameter.
    assert "$2" in vr.ETA_CANDIDATE_SQL


def test_eta_arrival_is_lowest_laden_priority_surplus_only():
    # eta_arrival is P≥1 (speculative) — it must NOT be exempt from the glide cap
    # the way the P0 leg-defenders are; it spends only the surplus.
    assert vr.CLASS_PRIORITY["eta_arrival"] >= 1
    cands = _sorted_cands("import_arrival", "eta_arrival")
    # Behind the line ⇒ only the P0 import_arrival is polled, eta_arrival skipped.
    chosen, skipped = vr.split_budget(cands, spent=0, cap=14, surplus=-1.0)
    assert [c.rescue_class for c in chosen] == ["import_arrival"]
    assert [c.rescue_class for c in skipped] == ["eta_arrival"]


def test_igu_fleet_import_uses_canonical_eta_converter():
    """Both VF→vessel_state writers must emit the AIS-shaped ETA scoring can
    parse. The fleet import once wrote `{"raw": ...}` directly, which silently
    killed tier-2 imminent-ETA promotion. Pin the reuse so it can't regress to
    a private copy: the script must reference the same converter object."""
    from scripts import import_igu_fleet

    assert import_igu_fleet.vf_eta_to_ais_dict is vr.vf_eta_to_ais_dict


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


# --- heading_toward / is_closing / is_settled ---------------------------------
def test_heading_toward_is_range_agnostic():
    # Unlike is_closing, heading_toward has no distance gate — used by the #8
    # wide-basin sweep, where the vessel is far beyond CLOSING_INCLUDE_KM.
    assert vr.heading_toward(90.0, 90.0, 60.0) is True
    assert vr.heading_toward(90.0, 140.0, 60.0) is True  # 50° off, within 60°
    assert vr.heading_toward(90.0, 200.0, 60.0) is False  # 110° off
    assert vr.heading_toward(None, 90.0, 60.0) is False  # no cog
    assert vr.heading_toward(90.0, None, 60.0) is False  # no bearing


def test_approach_sweep_is_lowest_priority():
    # #8 is surplus-only: it must not outrank the leg-defining P0 classes.
    assert vr.CLASS_PRIORITY["approach_sweep"] >= 1
    assert vr.CLASS_PRIORITY["approach_sweep"] > vr.CLASS_PRIORITY["import_arrival"]


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


def test_classify_export_arrival_loiter_band_skipped():
    # Burn control: export_arrival in the NEAR_KM..FINAL_APPROACH_KM band (20 km,
    # not closing) is the low-value loiter case and is no longer rescued, even at
    # a healthy 8h silence — only final-approach/closing ballast arrivals qualify.
    assert _classify(near_flow="export", near_km=20.0, last_fix_ts=ago(8)) is None


def test_classify_export_arrival_final_approach():
    # <=FINAL_APPROACH_KM and silent past the export-arrival floor (8h) ⇒ rescued.
    c = _classify(near_flow="export", near_km=10.0, last_fix_ts=ago(8))
    assert c is not None and c.rescue_class == "export_arrival"


def test_classify_export_arrival_short_gap_skipped():
    # In final approach but silent only 5h (< EXPORT_ARRIVAL_MIN_SILENCE_HOURS):
    # give AIS longer to self-heal for this low-value class than the 2h that an
    # import (laden) final-approach arrival would trigger on.
    assert _classify(near_flow="export", near_km=10.0, last_fix_ts=ago(5)) is None


def test_classify_export_arrival_closing_qualifies():
    # Closing from beyond NEAR_KM (40 km, heading at terminal) counts as final
    # approach, so a ballast vessel genuinely bearing down still gets rescued.
    c = _classify(
        near_flow="export",
        near_km=40.0,
        last_cog=90.0,
        bearing_deg=90.0,
        last_fix_ts=ago(8),
    )
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


def test_classify_open_visit_survives_general_ceiling():
    # LNG JUNO shape (06-04): moored at an export terminal, dark 60h
    # (> STALE_CEILING_HOURS). The pending `departed` still fires off a late
    # fix, so the open visit keeps the vessel eligible.
    c = _classify(
        near_flow="export",
        near_km=0.0,
        last_event_type="moored",
        last_event_flow="export",
        last_fix_ts=ago(60),
    )
    assert c is not None and c.rescue_class == "export_departure"


def test_classify_open_visit_import_survives_general_ceiling():
    # Same carve-out on the import side: anchored in queue, dark 3 days.
    c = _classify(
        near_flow="import",
        near_km=0.0,
        last_event_type="anchored",
        last_event_flow="import",
        last_fix_ts=ago(72),
    )
    assert c is not None and c.rescue_class == "import_berth"


def test_classify_open_visit_has_its_own_ceiling():
    # Past OPEN_VISIT_STALE_CEILING_HOURS (7d) even an open visit is abandoned.
    assert (
        _classify(
            near_flow="export",
            near_km=0.0,
            last_event_type="moored",
            last_event_flow="export",
            last_fix_ts=ago(200),
        )
        is None
    )


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
    silence = [_cand(1, "import_arrival"), _cand(2, "floating_check")]
    dest = [_cand(2, "dest_capture"), _cand(3, "dest_capture")]
    merged = {c.mmsi: c.rescue_class for c in vr.merge_candidates(silence, dest)}
    assert merged == {
        1: "import_arrival",
        2: "floating_check",  # tie (both P1) ⇒ first source (silence) wins
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


# --- glide surplus (leaky bucket, integrated form) ------------------------------
def test_glide_surplus_on_the_line_is_zero():
    # Halfway to expiry having spent exactly half the anchor reserve.
    anchor_ts = NOW - timedelta(days=100)
    expires = NOW + timedelta(days=100)
    s = vr.glide_surplus(
        anchor_credits=1000, anchor_ts=anchor_ts, expires=expires, balance=500, now=NOW
    )
    assert s == pytest.approx(0.0)


def test_glide_surplus_signs():
    anchor_ts = NOW - timedelta(days=100)
    expires = NOW + timedelta(days=100)
    kw = dict(anchor_credits=1000, anchor_ts=anchor_ts, expires=expires, now=NOW)
    # Underspent (balance above the line) ⇒ positive surplus for P≥1.
    assert vr.glide_surplus(balance=600, **kw) == pytest.approx(100.0)
    # Overspent (P0 burst pushed below the line) ⇒ negative: P≥1 starves.
    assert vr.glide_surplus(balance=450, **kw) == pytest.approx(-50.0)


def test_glide_surplus_current_numbers():
    # Live shape at implementation time (06-07): anchored ~4898cr with ~363d
    # left, balance 4856 three days later ⇒ essentially on the line.
    anchor_ts = NOW - timedelta(days=3)
    expires = NOW + timedelta(days=360)
    s = vr.glide_surplus(
        anchor_credits=4898, anchor_ts=anchor_ts, expires=expires, balance=4856, now=NOW
    )
    assert -5.0 < s < 5.0


def test_glide_surplus_expired_anchor_is_all_spendable():
    s = vr.glide_surplus(
        anchor_credits=100,
        anchor_ts=NOW - timedelta(days=10),
        expires=NOW - timedelta(days=1),
        balance=37,
        now=NOW,
    )
    assert s == 37.0


# --- split budget (P0 exempt, P≥1 spends the surplus) ----------------------------
def _sorted_cands(*classes):
    cands = [_cand(i, cls) for i, cls in enumerate(classes)]
    cands.sort(key=lambda c: (vr.CLASS_PRIORITY[c.rescue_class], -c.silent_h))
    return cands


def test_split_budget_p0_exempt_from_cap():
    # Cap exhausted (spent == cap): P0 still chosen, P≥1 skipped even with surplus.
    # (dest_capture is the P1 representative; import_berth is now P0.)
    cands = _sorted_cands("import_arrival", "export_departure", "dest_capture")
    chosen, skipped = vr.split_budget(cands, spent=14, cap=14, surplus=100.0)
    assert [c.rescue_class for c in chosen] == ["import_arrival", "export_departure"]
    assert [c.rescue_class for c in skipped] == ["dest_capture"]


def test_split_budget_p1_spends_surplus_within_cap():
    cands = _sorted_cands("import_arrival", "import_berth", "dest_capture")
    chosen, skipped = vr.split_budget(cands, spent=0, cap=14, surplus=100.0)
    assert len(chosen) == 3 and not skipped


def test_split_budget_negative_surplus_starves_p1():
    # Behind the glide line: only the P0 candidate is polled.
    cands = _sorted_cands("import_arrival", "dest_capture", "export_arrival")
    chosen, skipped = vr.split_budget(cands, spent=0, cap=14, surplus=-3.0)
    assert [c.rescue_class for c in chosen] == ["import_arrival"]
    assert len(skipped) == 2


def test_split_budget_p1_limited_by_fractional_surplus():
    # surplus 1.7 ⇒ floor ⇒ exactly one P≥1 slot.
    cands = _sorted_cands("dest_capture", "dest_capture", "dest_capture")
    chosen, skipped = vr.split_budget(cands, spent=0, cap=14, surplus=1.7)
    assert len(chosen) == 1 and len(skipped) == 2


def test_split_budget_brake_bounds_everything():
    # The disaster brake stops even P0 — a runaway classifier can't drain the
    # reserve.
    cands = _sorted_cands("import_arrival", "export_departure", "outage_check")
    chosen, skipped = vr.split_budget(cands, spent=39, cap=14, surplus=100.0, brake=40)
    assert len(chosen) == 1 and len(skipped) == 2


def test_split_budget_manual_is_exempt():
    chosen, skipped = vr.split_budget(
        [_cand(1, "manual")], spent=14, cap=14, surplus=-10.0
    )
    assert len(chosen) == 1 and not skipped


# --- discovery credit budget (floor + surplus, brake-bounded) -------------------
_DISC = dict(glide_cap_value=14, floor=3, ceiling=12, brake=40)


def test_discovery_budget_floor_guarantees_a_catch_behind_the_line():
    # Rescue ate the whole glide (surplus ≈ 0 / negative): the floor still lets
    # one delivered hull be caught — discovery is subordinate, not starved.
    assert vr.discovery_credit_budget(surplus=-1.0, spent_today=0, **_DISC) == 3


def test_discovery_budget_surplus_beats_floor_on_slack_days():
    # Genuine surplus ⇒ discovery may go faster than the floor (floor(8.9) = 8).
    assert vr.discovery_credit_budget(surplus=8.9, spent_today=0, **_DISC) == 8


def test_discovery_budget_capped_by_ceiling():
    # Huge surplus, but the rareness ceiling bounds the run.
    assert vr.discovery_credit_budget(surplus=100.0, spent_today=0, **_DISC) == 12


def test_discovery_budget_bounded_by_remaining_glide_cap():
    # Rescue spent most of the glide cap ⇒ the surplus slice shrinks to 14−10 = 4
    # (still above the floor, so 4 wins).
    assert vr.discovery_credit_budget(surplus=100.0, spent_today=10, **_DISC) == 4


def test_discovery_budget_floor_bounded_by_brake():
    # Near the disaster brake, even the floor is trimmed to what's left (40−39).
    assert vr.discovery_credit_budget(surplus=-5.0, spent_today=39, **_DISC) == 1


def test_discovery_budget_brake_exhausted_blocks_everything():
    # Total daily spend hit the brake ⇒ no discovery spend, floor included.
    assert vr.discovery_credit_budget(surplus=100.0, spent_today=40, **_DISC) == 0


# --- no_position backoff --------------------------------------------------------
def test_no_position_backoff_escalates_and_caps():
    # First miss keeps the normal cooldown, then doubles per consecutive miss.
    assert vr.no_position_backoff_hours(0) == vr.PER_VESSEL_COOLDOWN_HOURS  # 12h
    assert vr.no_position_backoff_hours(1) == 24.0
    assert vr.no_position_backoff_hours(2) == 48.0
    assert vr.no_position_backoff_hours(3) == 96.0
    assert vr.no_position_backoff_hours(4) == vr.NO_POSITION_BACKOFF_CEILING_HOURS
    # Deep streaks stay clamped (no overflow into absurd cooldowns).
    assert vr.no_position_backoff_hours(20) == vr.NO_POSITION_BACKOFF_CEILING_HOURS


# --- glide-path cap -------------------------------------------------------------
def test_glide_cap_matches_current_glide():
    # 4898 credits, ~363 days to expiry → ceil(13.49) = 14 (the hand-set cap).
    expires = NOW + timedelta(days=363)
    assert vr.glide_cap(4898, expires, NOW) == 14


def test_glide_cap_ceils_so_last_fraction_is_spent():
    # 10 credits over 4 days = 2.5/day → 3 (spending slower forfeits credits).
    assert vr.glide_cap(10, NOW + timedelta(days=4), NOW) == 3


def test_glide_cap_no_snapshot_falls_back():
    assert vr.glide_cap(None, None, NOW) == vr.DAILY_CREDIT_CAP
    assert vr.glide_cap(4898, None, NOW) == vr.DAILY_CREDIT_CAP
    assert vr.glide_cap(None, NOW + timedelta(days=100), NOW) == vr.DAILY_CREDIT_CAP


def test_glide_cap_exhausted_reserve_is_zero():
    assert vr.glide_cap(0, NOW + timedelta(days=100), NOW) == 0
    assert vr.glide_cap(-3, NOW + timedelta(days=100), NOW) == 0


def test_glide_cap_final_day_spends_whats_left():
    # Under a day to expiry: now-or-never, but still ceiling-clamped.
    assert vr.glide_cap(7, NOW + timedelta(hours=12), NOW) == 7
    assert vr.glide_cap(500, NOW + timedelta(hours=12), NOW) == vr.GLIDE_CAP_CEILING
    # Already expired (stale snapshot edge): same path.
    assert vr.glide_cap(7, NOW - timedelta(days=1), NOW) == 7


def test_glide_cap_clamped_against_balance_drift():
    # A topped-up/corrupt balance must not trigger a spending spree.
    assert vr.glide_cap(100_000, NOW + timedelta(days=30), NOW) == vr.GLIDE_CAP_CEILING


def test_glide_target_date_applies_headroom():
    # Spend-faster buffer: the effective target is GLIDE_HEADROOM_DAYS before expiry.
    expires = NOW + timedelta(days=363)
    assert vr._glide_target_date(expires) == expires - timedelta(
        days=vr.GLIDE_HEADROOM_DAYS
    )
    assert vr._glide_target_date(None) is None


def test_glide_headroom_raises_cap():
    # Against the headroom-shifted target the daily cap is strictly higher than
    # against the raw expiry — i.e. the reserve depletes faster (the owner's
    # spend-faster directive). 4898 cr, 363 d → 14 flat; ~303 d → 17.
    expires = NOW + timedelta(days=363)
    flat = vr.glide_cap(4898, expires, NOW)
    faster = vr.glide_cap(4898, vr._glide_target_date(expires), NOW)
    assert faster > flat
