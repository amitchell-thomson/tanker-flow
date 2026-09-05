"""Unit tests for the market/control-series loader (data.market).

Pure-logic only: captured-shape provider payloads, the degree-day maths and the
pure merge model. No network, no DB.
"""

from __future__ import annotations

from datetime import date

from data.market import (
    DEGREE_DAY_BASE_C,
    PANEL_START,
    SERIES,
    TTF_HISTORY_STARTS,
    MarketRow,
    degree_days,
    merge_rows,
    parse_agsi_records,
    parse_fred_observations,
    parse_yahoo_chart,
    start_date_for,
    weighted_temperature,
)

TTF = SERIES["ttf_front_month"]
EURUSD = SERIES["eurusd"]
STORAGE = SERIES["eu_storage_full"]
HDD_NWE = SERIES["hdd_nwe"]
CDD_NWE = SERIES["cdd_nwe"]


# --- Yahoo --------------------------------------------------------------------

# Epoch seconds for 2026-08-27 / 08-28 at 07:00 UTC, shaped like the chart API.
YAHOO_PAYLOAD = {
    "chart": {
        "result": [
            {
                "meta": {"symbol": "TTF=F", "currency": "EUR"},
                "timestamp": [1787814000, 1787900400],
                "indicators": {"quote": [{"close": [68.095, None]}]},
            }
        ]
    }
}


def test_parse_yahoo_chart_maps_closes_and_nulls():
    rows = parse_yahoo_chart(YAHOO_PAYLOAD, TTF)
    assert [r.period for r in rows] == [date(2026, 8, 27), date(2026, 8, 28)]
    assert rows[0].value == 68.095
    # A null close (exchange holiday) is preserved as NULL, not dropped or zeroed.
    assert rows[1].value is None
    assert {r.series_id for r in rows} == {"ttf_front_month"}
    assert {r.unit for r in rows} == {"EUR/MWh"}
    assert {r.source for r in rows} == {"yahoo"}


def test_parse_yahoo_chart_handles_empty_result():
    assert parse_yahoo_chart({"chart": {"result": None}}, TTF) == []
    assert parse_yahoo_chart({}, TTF) == []


# --- FRED ---------------------------------------------------------------------

FRED_PAYLOAD = {
    "observations": [
        {"date": "2016-01-01", "value": "."},
        {"date": "2016-01-04", "value": "1.0803"},
        {"date": "2016-01-05", "value": "1.0743"},
    ]
}


def test_parse_fred_observations_treats_dot_as_null():
    rows = parse_fred_observations(FRED_PAYLOAD, EURUSD)
    assert len(rows) == 3
    # FRED writes '.' on holidays — that is a gap, not a zero rate.
    assert rows[0].value is None
    assert rows[1].value == 1.0803
    assert rows[2].period == date(2016, 1, 5)
    assert rows[0].source == "fred"


# --- AGSI+ --------------------------------------------------------------------

AGSI_RECORDS = [
    {"gasDayStart": "2016-01-03", "full": "69.04", "gasInStorage": "729.5001"},
    {"gasDayStart": "2016-01-04", "full": "68.60", "gasInStorage": "724.8"},
    {"gasDayStart": "2016-01-05", "full": "-", "gasInStorage": "720.1"},
]


def test_parse_agsi_records_pulls_registry_field():
    rows = parse_agsi_records(AGSI_RECORDS, STORAGE)
    assert [r.value for r in rows] == [69.04, 68.60, None]  # '-' is a gap
    assert rows[0].unit == "%"
    assert rows[0].series_id == "eu_storage_full"


def test_parse_agsi_records_same_payload_different_field():
    """One payload feeds two registry entries — fill % and absolute TWh."""
    rows = parse_agsi_records(AGSI_RECORDS, SERIES["eu_storage_twh"])
    assert [r.value for r in rows] == [729.5001, 724.8, 720.1]
    assert rows[0].unit == "TWh"


# --- Degree days --------------------------------------------------------------


def test_weighted_temperature_uses_declared_weights():
    points = (("a", 0.0, 0.0, 0.75), ("b", 0.0, 0.0, 0.25))
    per_point = {
        "a": {date(2020, 1, 1): 10.0},
        "b": {date(2020, 1, 1): 2.0},
    }
    assert weighted_temperature(per_point, points) == {date(2020, 1, 1): 8.0}


def test_weighted_temperature_renormalises_over_reporting_points():
    """A missing grid point must rescale the weights, not drag the mean down."""
    points = (("a", 0.0, 0.0, 0.75), ("b", 0.0, 0.0, 0.25))
    per_point = {
        "a": {date(2020, 1, 1): 10.0},
        "b": {date(2020, 1, 1): None},
    }
    assert weighted_temperature(per_point, points) == {date(2020, 1, 1): 10.0}


def test_weighted_temperature_skips_days_with_no_reports():
    points = (("a", 0.0, 0.0, 1.0),)
    per_point = {"a": {date(2020, 1, 1): None}}
    assert weighted_temperature(per_point, points) == {}


def test_degree_days_hdd_and_cdd_are_complementary():
    cold, hot = 8.3, 28.3
    temps = {date(2020, 1, 1): cold, date(2020, 7, 1): hot}
    hdd = {r.period: r.value for r in degree_days(temps, HDD_NWE)}
    cdd = {r.period: r.value for r in degree_days(temps, CDD_NWE)}
    # Base is 18.3C, so a 10C day is 10 HDD / 0 CDD and a 28.3C day the mirror.
    assert hdd[date(2020, 1, 1)] == 10.0
    assert cdd[date(2020, 1, 1)] == 0.0
    assert hdd[date(2020, 7, 1)] == 0.0
    assert cdd[date(2020, 7, 1)] == 10.0


def test_degree_days_are_never_negative_and_pivot_at_base():
    temps = {date(2020, 4, 1): DEGREE_DAY_BASE_C}
    assert degree_days(temps, HDD_NWE)[0].value == 0.0
    assert degree_days(temps, CDD_NWE)[0].value == 0.0


def test_degree_days_emit_sorted_periods():
    temps = {date(2020, 3, 1): 5.0, date(2020, 1, 1): 5.0, date(2020, 2, 1): 5.0}
    assert [r.period for r in degree_days(temps, HDD_NWE)] == [
        date(2020, 1, 1),
        date(2020, 2, 1),
        date(2020, 3, 1),
    ]


# --- Incremental start dates --------------------------------------------------


def test_start_date_for_full_backfill_is_panel_start():
    assert start_date_for(EURUSD, None, full=True) == PANEL_START


def test_start_date_for_yahoo_is_clamped_to_its_first_trading_day():
    """TTF=F has no data before 2017-10-23; asking for 2016 would waste the call
    and make the resulting gap look like a load failure."""
    assert start_date_for(TTF, None, full=True) == TTF_HISTORY_STARTS


def test_start_date_for_incremental_steps_back_a_revision_window():
    latest = date(2026, 8, 28)
    start = start_date_for(EURUSD, latest, full=False)
    assert start < latest  # re-pulls a trailing window to absorb revisions
    assert start == date(2026, 8, 14)  # fred window = 14 days


def test_start_date_for_incremental_never_precedes_panel_start():
    assert start_date_for(EURUSD, PANEL_START, full=False) == PANEL_START


# --- Merge model --------------------------------------------------------------


def _row(series_id: str, day: date, value: float | None) -> MarketRow:
    return MarketRow(
        series_id=series_id,
        period=day,
        value=value,
        unit="x",
        frequency="daily",
        source="test",
    )


def test_merge_rows_is_idempotent():
    rows = [_row("a", date(2020, 1, 1), 1.0), _row("a", date(2020, 1, 2), 2.0)]
    once = merge_rows({}, rows)
    twice = merge_rows(once, rows)
    assert once == twice
    assert len(once) == 2


def test_merge_rows_last_write_wins_on_revision():
    day = date(2020, 1, 1)
    merged = merge_rows({}, [_row("a", day, 1.0)])
    merged = merge_rows(merged, [_row("a", day, 9.9)])
    assert merged[("a", day)].value == 9.9


def test_merge_rows_keys_are_series_scoped():
    day = date(2020, 1, 1)
    merged = merge_rows({}, [_row("a", day, 1.0), _row("b", day, 2.0)])
    assert len(merged) == 2


# --- Registry integrity -------------------------------------------------------


def test_registry_keys_match_their_series_id():
    """The dict key is the stored series_id; a mismatch would silently write rows
    under a name no consumer queries."""
    for key, series in SERIES.items():
        assert series.key == key


def test_every_series_has_a_known_source():
    assert {s.source for s in SERIES.values()} <= {"yahoo", "fred", "agsi", "openmeteo"}
