"""Unit tests for the Part B panel assembler (data.model_panel).

Pure-logic only: publication lag, forward-fill, band aggregation and the spread
conversion. No network, no DB.
"""

from __future__ import annotations

from datetime import date

from data.model_panel import (
    MAX_STALENESS_DAYS,
    MWH_PER_MMBTU,
    SERIES_FEATURES,
    SIGNAL_FEATURES,
    aggregate_bands,
    daily_grid,
    forward_fill,
    shift_for_publication,
    spread_usd_per_mmbtu,
)

D = date


# --- Publication lag ----------------------------------------------------------


def test_shift_for_publication_moves_value_to_its_knowable_date():
    """EIA weekly storage for the week ending Friday is public the next Thursday;
    a model standing on the Friday must not see it."""
    week_ending = {D(2026, 1, 2): 3000.0}
    shifted = shift_for_publication(week_ending, 6)
    assert shifted == {D(2026, 1, 8): 3000.0}
    assert D(2026, 1, 2) not in shifted


def test_shift_for_publication_zero_lag_is_identity():
    obs = {D(2026, 1, 2): 1.0, D(2026, 1, 3): 2.0}
    assert shift_for_publication(obs, 0) == obs


def test_shift_for_publication_does_not_mutate_input():
    obs = {D(2026, 1, 2): 1.0}
    shift_for_publication(obs, 5)
    assert obs == {D(2026, 1, 2): 1.0}


# --- Forward fill -------------------------------------------------------------


def test_forward_fill_bridges_a_weekend():
    grid = daily_grid(D(2026, 1, 2), D(2026, 1, 5))  # Fri..Mon
    filled = forward_fill({D(2026, 1, 2): 3.5}, grid, 5)
    assert [filled[d] for d in grid] == [3.5, 3.5, 3.5, 3.5]


def test_forward_fill_stops_at_the_staleness_ceiling():
    """A discontinued feed must go NULL, not flat-line forever — otherwise a dead
    series looks like a constant signal."""
    grid = daily_grid(D(2026, 1, 1), D(2026, 1, 10))
    filled = forward_fill({D(2026, 1, 1): 7.0}, grid, 3)
    assert filled[D(2026, 1, 4)] == 7.0  # exactly at the ceiling
    assert filled[D(2026, 1, 5)] is None  # one day past it
    assert filled[D(2026, 1, 10)] is None


def test_forward_fill_bridges_vendor_nulls_with_the_last_real_value():
    """FRED writes '.' on holidays. That is a gap to bridge, not a blank day."""
    grid = daily_grid(D(2026, 1, 1), D(2026, 1, 3))
    filled = forward_fill(
        {D(2026, 1, 1): 1.16, D(2026, 1, 2): None, D(2026, 1, 3): 1.17}, grid, 5
    )
    assert [filled[d] for d in grid] == [1.16, 1.16, 1.17]


def test_forward_fill_leaves_leading_days_null():
    """Nothing is knowable before the first observation — no back-fill."""
    grid = daily_grid(D(2026, 1, 1), D(2026, 1, 4))
    filled = forward_fill({D(2026, 1, 3): 5.0}, grid, 5)
    assert filled[D(2026, 1, 1)] is None
    assert filled[D(2026, 1, 2)] is None
    assert filled[D(2026, 1, 3)] == 5.0


def test_forward_fill_refreshes_the_clock_on_each_observation():
    grid = daily_grid(D(2026, 1, 1), D(2026, 1, 7))
    filled = forward_fill({D(2026, 1, 1): 1.0, D(2026, 1, 4): 2.0}, grid, 3)
    assert filled[D(2026, 1, 7)] == 2.0  # 3 days after the *second* observation


def test_forward_fill_covers_every_grid_date():
    grid = daily_grid(D(2026, 1, 1), D(2026, 1, 20))
    assert set(forward_fill({D(2026, 1, 1): 1.0}, grid, 2)) == set(grid)


# --- Band aggregation ---------------------------------------------------------

# (bucket_date, zone_scope, value, n_legs)
BANDED = [
    (D(2026, 1, 1), "nweurope", 100.0, 10),
    (D(2026, 1, 1), "iberian", 40.0, 2),
    (D(2026, 1, 1), "unknown", 500.0, 30),
]


def test_aggregate_bands_sums_stocks_across_named_zones():
    out = aggregate_bands(BANDED, "sum", "named")
    assert out == {D(2026, 1, 1): 140.0}  # 'unknown' excluded


def test_aggregate_bands_can_isolate_the_unknown_band():
    assert aggregate_bands(BANDED, "sum", "unknown") == {D(2026, 1, 1): 500.0}


def test_aggregate_bands_any_includes_every_band():
    assert aggregate_bands(BANDED, "sum", "any") == {D(2026, 1, 1): 640.0}


def test_aggregate_bands_weights_durations_by_leg_count():
    """A 40 h wait seen on 2 vessels and a 100 h wait seen on 10 must not average
    to 70 — the weighted mean is 90."""
    out = aggregate_bands(BANDED, "wmean", "named")
    assert out[D(2026, 1, 1)] == (100.0 * 10 + 40.0 * 2) / 12


def test_aggregate_bands_wmean_falls_back_to_plain_mean_without_leg_counts():
    """Composites carry no n_legs; dropping those days would blank the feature."""
    rows = [(D(2026, 1, 1), "all", 3.0, None), (D(2026, 1, 1), "all", 5.0, 0)]
    assert aggregate_bands(rows, "wmean", "any") == {D(2026, 1, 1): 4.0}


def test_aggregate_bands_skips_null_values():
    rows = [(D(2026, 1, 1), "nweurope", None, 5), (D(2026, 1, 1), "iberian", 7.0, 1)]
    assert aggregate_bands(rows, "sum", "named") == {D(2026, 1, 1): 7.0}


def test_aggregate_bands_emits_no_day_when_every_band_is_null():
    rows = [(D(2026, 1, 1), "nweurope", None, 5)]
    assert aggregate_bands(rows, "sum", "named") == {}


# --- Spread conversion --------------------------------------------------------


def test_spread_conversion_matches_the_documented_formula():
    ttf_usd, spread = spread_usd_per_mmbtu(2.80, 62.0, 1.16)
    assert ttf_usd == 62.0 / MWH_PER_MMBTU * 1.16
    assert spread == 2.80 - ttf_usd
    # Sanity against the World Bank's independent Aug-2026 print of $21.11.
    assert abs(ttf_usd - 21.11) < 0.2


def test_spread_is_none_when_any_leg_is_missing():
    assert spread_usd_per_mmbtu(None, 62.0, 1.16) == (None, None)
    assert spread_usd_per_mmbtu(2.8, None, 1.16) == (None, None)
    assert spread_usd_per_mmbtu(2.8, 62.0, None) == (None, None)


def test_spread_sign_flips_when_hh_exceeds_ttf():
    """The 2020 COVID collapse and the 2021 Uri freeze both put HH above TTF; the
    target must be able to go positive."""
    _, spread = spread_usd_per_mmbtu(23.86, 18.0, 1.16)
    assert spread > 0


# --- Grid + registry integrity ------------------------------------------------


def test_daily_grid_is_contiguous_and_inclusive():
    grid = daily_grid(D(2026, 1, 1), D(2026, 1, 31))
    assert len(grid) == 31
    assert grid[0] == D(2026, 1, 1) and grid[-1] == D(2026, 1, 31)


def test_every_series_feature_declares_a_known_frequency():
    for feat in SERIES_FEATURES:
        assert feat.frequency in MAX_STALENESS_DAYS


def test_publication_lags_are_non_negative():
    """A negative lag would make a value visible before it existed."""
    assert all(f.publication_lag_days >= 0 for f in SERIES_FEATURES)


def test_feature_columns_are_unique():
    """Two features sharing a name would silently overwrite each other in the
    tidy table, which is keyed on (bucket_date, feature)."""
    names = [f.column for f in SERIES_FEATURES] + [f.column for f in SIGNAL_FEATURES]
    assert len(names) == len(set(names))


def test_signal_features_declare_valid_aggregation_and_bands():
    for sig in SIGNAL_FEATURES:
        assert sig.how in ("sum", "wmean")
        assert sig.bands in ("named", "unknown", "any")
