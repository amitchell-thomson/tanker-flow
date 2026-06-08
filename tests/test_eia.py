"""Unit tests for the EIA loader (data.eia).

Pure-logic: captured-shape v2 payloads + the pure merge model, no network/DB.
"""

from __future__ import annotations

from datetime import date

from data.eia import (
    SERIES,
    EiaRow,
    _start_period,
    merge_rows,
    parse_eia_response,
)


# A monthly LNG-exports page, shaped like EIA v2 `response.data[]`.
MONTHLY_PAYLOAD = {
    "response": {
        "frequency": "monthly",
        "data": [
            {
                "period": "2026-03",
                "series": "N9133US2",
                "value": "350000",
                "units": "MMcf",
            },
            {
                "period": "2026-04",
                "series": "N9133US2",
                "value": 362500,
                "units": "MMcf",
            },
        ],
    }
}

# A weekly storage page (period is a full date) with a published gap (null value).
WEEKLY_PAYLOAD = {
    "response": {
        "frequency": "weekly",
        "data": [
            {
                "period": "2026-05-29",
                "series": "NW2_EPG0_SWO_R48_BCF",
                "value": 2600,
                "units": "Bcf",
            },
            {
                "period": "2026-06-05",
                "series": "NW2_EPG0_SWO_R48_BCF",
                "value": None,
                "units": "Bcf",
            },
        ],
    }
}

EMPTY_PAYLOAD = {"response": {"frequency": "monthly", "data": []}}


def test_parse_monthly_period_to_first_of_month():
    rows = parse_eia_response(
        MONTHLY_PAYLOAD, series_id="N9133US2", frequency="monthly", default_unit="MMcf"
    )
    assert len(rows) == 2
    assert rows[0] == EiaRow(
        series_id="N9133US2",
        period=date(2026, 3, 1),
        value=350000.0,
        unit="MMcf",
        frequency="monthly",
    )
    # Numeric and string values both coerce to float.
    assert rows[1].value == 362500.0


def test_parse_weekly_keeps_full_date_and_nulls_gap():
    rows = parse_eia_response(
        WEEKLY_PAYLOAD,
        series_id="NW2_EPG0_SWO_R48_BCF",
        frequency="weekly",
        default_unit="Bcf",
    )
    assert rows[0].period == date(2026, 5, 29)
    assert rows[0].value == 2600.0
    # EIA publishes gaps as null → stored as NULL, not 0.
    assert rows[1].value is None


def test_parse_empty_response():
    assert (
        parse_eia_response(
            EMPTY_PAYLOAD,
            series_id="N9133US2",
            frequency="monthly",
            default_unit="MMcf",
        )
        == []
    )


def test_parse_falls_back_to_default_unit_when_row_units_missing():
    payload = {"response": {"data": [{"period": "2026-04", "value": 1}]}}
    rows = parse_eia_response(
        payload, series_id="X", frequency="monthly", default_unit="MMcf"
    )
    assert rows[0].unit == "MMcf"


def test_merge_rows_is_idempotent():
    rows = parse_eia_response(
        MONTHLY_PAYLOAD, series_id="N9133US2", frequency="monthly", default_unit="MMcf"
    )
    once = merge_rows({}, rows)
    twice = merge_rows(once, rows)
    # Applying the same page again is a no-op (one row per (series_id, period)).
    assert len(once) == 2
    assert once == twice


def test_merge_rows_overwrites_on_revision():
    base = parse_eia_response(
        MONTHLY_PAYLOAD, series_id="N9133US2", frequency="monthly", default_unit="MMcf"
    )
    merged = merge_rows({}, base)
    revised = EiaRow(
        series_id="N9133US2",
        period=date(2026, 4, 1),
        value=999999.0,  # EIA revised the April value
        unit="MMcf",
        frequency="monthly",
    )
    merged = merge_rows(merged, [revised])
    # Same key → overwritten, not duplicated.
    assert len(merged) == 2
    assert merged[("N9133US2", date(2026, 4, 1))].value == 999999.0


def test_start_period_monthly_steps_back_revision_window():
    s = SERIES["lng_exports"]  # monthly, revision_window=3
    # 3 months before 2026-03 is 2025-12.
    assert _start_period(date(2026, 3, 1), s) == "2025-12"


def test_start_period_monthly_year_boundary():
    s = SERIES["lng_exports"]
    assert _start_period(date(2026, 1, 1), s) == "2025-10"


def test_start_period_weekly_and_daily_format_as_full_date():
    weekly = SERIES["storage_l48"]  # weekly, revision_window=8 → 56 days
    assert _start_period(date(2026, 6, 5), weekly) == "2026-04-10"
    daily = SERIES["hh_spot"]  # daily, revision_window=30
    assert _start_period(date(2026, 6, 30), daily) == "2026-05-31"
