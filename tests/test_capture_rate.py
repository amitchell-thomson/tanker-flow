"""Unit tests for the capture-rate validator (data.capture_rate).

Pure-logic: the unit conversion (the part the design said to lock) + the
month-comparability/ratio join, from plain records. No DB.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

from data import capture_rate as cr


# --- The conversion constant: lock it so a silent edit can't move the ratio. ---


def test_nominal_cargo_is_about_3_7_bcf():
    # 174k m³ LNG → ~3.69 Bcf of gas (EIA 600× expansion). The design's loose
    # "~3.4 Bcf" anchor (~553×) is explicitly NOT what we use.
    bcf = cr.m3_lng_to_mmcf(cr.NOMINAL_CARGO_M3) / 1000
    assert 3.6 < bcf < 3.8
    # Not the 3.4 anchor — guards against anyone "correcting" to the design number.
    assert bcf > 3.5


def test_implied_cargoes_is_volume_over_cargo_size():
    # March 2026 EIA: 573,479 MMcf. At 174k m³ ≈ 3.687 Bcf/cargo → ~155 cargoes.
    implied = cr.implied_cargoes(573_479, cr.NOMINAL_CARGO_M3)
    assert 150 < implied < 160


def test_capture_rate_is_captured_over_implied():
    implied = cr.implied_cargoes(573_479, cr.NOMINAL_CARGO_M3)
    # Capturing exactly the implied count → 100%.
    assert (
        cr.capture_rate(round(implied), 573_479, cr.NOMINAL_CARGO_M3)
        == round(implied) / implied
    )
    # Half the cargoes → ~50%.
    half = cr.capture_rate(round(implied / 2), 573_479, cr.NOMINAL_CARGO_M3)
    assert 0.48 < half < 0.52


def test_observed_mean_denominator_differs_from_nominal():
    # A smaller observed mean cargo → more implied cargoes → lower capture rate.
    nominal = cr.capture_rate(100, 500_000, cr.NOMINAL_CARGO_M3)
    smaller = cr.capture_rate(100, 500_000, 150_000)
    assert smaller < nominal


# --- Month tagging / join --------------------------------------------------------

NOW = datetime(2026, 9, 15, tzinfo=UTC)  # well after several post-cutover months


def _cap(month, captured, mmsi, mean):
    return {
        "month": month,
        "captured": captured,
        "captured_mmsi": mmsi,
        "mean_gas_m3": mean,
    }


def _eia(period, value):
    return {"period": period, "value": value}


def test_pre_cutover_month_is_not_meaningful_even_if_published():
    # March 2026 is pre-cutover (bbox). Published + revised, but regime-biased.
    rows = cr.build_rows(
        [_cap(date(2026, 3, 1), 150, 0, 170_000)],
        [_eia(date(2026, 3, 1), 573_479)],
        NOW,
    )
    (row,) = rows
    assert row.comparable is True
    assert row.post_cutover is False
    assert row.meaningful is False
    assert row.rate_nominal is not None  # still computed, just not "meaningful"


def test_post_cutover_published_revised_month_is_meaningful():
    # June 2026: wholly post-cutover, old enough (relative to NOW=Sep) to be firm.
    rows = cr.build_rows(
        [_cap(date(2026, 6, 1), 120, 120, 172_000)],
        [_eia(date(2026, 6, 1), 560_000)],
        NOW,
    )
    (row,) = rows
    assert row.post_cutover is True
    assert row.revised is True
    assert row.meaningful is True


def test_recent_post_cutover_month_not_yet_revised():
    # August 2026 relative to NOW=Sep-15 is within the 2-month revision window.
    rows = cr.build_rows(
        [_cap(date(2026, 8, 1), 100, 100, 172_000)],
        [_eia(date(2026, 8, 1), 555_000)],
        NOW,
    )
    (row,) = rows
    assert row.comparable is True
    assert row.revised is False
    assert row.meaningful is False


def test_captured_month_without_eia_is_not_comparable():
    rows = cr.build_rows(
        [_cap(date(2026, 6, 1), 120, 120, 172_000)],
        [],  # EIA hasn't published June yet
        NOW,
    )
    (row,) = rows
    assert row.comparable is False
    assert row.rate_nominal is None


def test_first_post_cutover_month_is_the_month_after_the_seam():
    # Cutover is 2026-05-30 → May is mixed → first clean month is June.
    assert cr.FIRST_POST_CUTOVER_MONTH == date(2026, 6, 1)


def test_render_reports_no_meaningful_month_when_none_qualify():
    # The live situation today: captures start April, EIA ends March → no overlap.
    rows = cr.build_rows(
        [
            _cap(date(2026, 4, 1), 36, 0, 170_597),
            _cap(date(2026, 6, 1), 23, 23, 173_321),
        ],
        [_eia(date(2026, 3, 1), 573_479)],
        datetime(2026, 6, 9, tzinfo=UTC),
    )
    out = cr.render(rows, datetime(2026, 6, 9, tzinfo=UTC))
    assert "No meaningful month yet" in out
    assert "first meaningful month = 2026-06" in out
