"""Unit tests for pipeline.dest_parser.

Fixtures derived from the destination-frequency survey on real
vessel_state.dest values across 90 days of LNG/FSRU traffic.
"""

import pytest

from pipeline.dest_parser import parse_destination


# Minimal fake terminals.unlocode map sufficient for the test cases. Real one
# has ~34 entries.
LOCODES = {
    "NLRTM": 10,
    "NLEEM": 11,
    "BEZEE": 12,
    "USSAB": 1,
    "USPLQ": 2,
    "USCLU": 3,
    "USCRP": 4,
    "USCAU": 5,
    "USFPO": 6,
    "ESCAR": 24,
    "PLSWI": 29,
    "DEWVN": 16,
}


@pytest.mark.parametrize("dest", [None, "", "   "])
def test_empty_returns_none(dest):
    assert parse_destination(dest, LOCODES) == (None, False)


@pytest.mark.parametrize(
    "dest",
    [
        "FOR ORDERS",
        "FOR ORDER",
        "for orders",  # case-insensitive
        "OPEN SEA FOR ORDERS",
        "GERMANY FOR ORDERS",
        "EU FOR ORDERS",
        "  AT SEA  ",  # whitespace-tolerant
        "TBN",
    ],
)
def test_for_orders_markers(dest):
    assert parse_destination(dest, LOCODES) == (None, True)


def test_straight_locode():
    assert parse_destination("NLRTM", LOCODES) == (10, False)
    assert parse_destination("BEZEE", LOCODES) == (12, False)
    assert parse_destination("USSAB", LOCODES) == (1, False)


def test_locode_with_internal_space():
    # Common: "NL RTM", "BE ZEE", "US NSS"
    assert parse_destination("NL RTM", LOCODES) == (10, False)
    assert parse_destination("BE ZEE", LOCODES) == (12, False)


def test_chained_destination_uses_rhs():
    # USSAB>NLRTM = "vessel at Sabine, headed to Rotterdam" — we care about
    # the next destination.
    assert parse_destination("USSAB>NLRTM", LOCODES) == (10, False)
    assert parse_destination("USFPO > NLRTM", LOCODES) == (10, False)
    # RHS that resolves to no terminal we track → None (USXXX is unmapped).
    assert parse_destination("KRYOS > USXXX", LOCODES) == (None, False)


def test_chained_to_for_orders():
    # Heading "to orders" after departure — should be (None, True).
    assert parse_destination("USSAB > FOR ORDERS", LOCODES) == (None, True)


def test_freeform_name_recognized():
    assert parse_destination("ROTTERDAM", LOCODES) == (10, False)
    assert parse_destination("EEMSHAVEN", LOCODES) == (11, False)
    assert parse_destination("SABINE", LOCODES) == (1, False)
    assert parse_destination("Cameron", LOCODES) == (5, False)


def test_freeform_with_spaces():
    assert parse_destination("Sabine Pass", LOCODES) == (1, False)
    assert parse_destination("Corpus Christi", LOCODES) == (4, False)


def test_unknown_returns_none():
    # Real US-Gulf ports we deliberately DON'T track (Galveston, Houston,
    # Beaumont/Port Arthur area) must stay unresolved — mapping them would
    # mis-attribute non-LNG-terminal traffic. (USNSS/USLCH are handled
    # separately below; a 2026-06-11 DB audit confirmed they're really ours.)
    assert parse_destination("USGLS", LOCODES) == (None, False)
    assert parse_destination("USHOU", LOCODES) == (None, False)
    assert parse_destination("RANDOM GARBAGE", LOCODES) == (None, False)


def test_us_gulf_operator_shorthand_aliases():
    # Data-grounded aliases (2026-06-11 audit of where declaring LNG carriers
    # actually moor): USNSS → Sabine Pass, USLCH (Lake Charles) → Calcasieu Pass.
    # Both the spaced and unspaced forms normalise to the same key.
    assert parse_destination("USNSS", LOCODES) == (1, False)
    assert parse_destination("US NSS", LOCODES) == (1, False)
    assert parse_destination("USLCH", LOCODES) == (3, False)
    assert parse_destination("US LCH", LOCODES) == (3, False)
    # A named alias still wins over an appended "FOR ORDERS" qualifier.
    assert parse_destination("USNSS FOR ORDERS", LOCODES) == (1, False)


def test_terminal_not_in_provided_locodes_returns_none():
    # If the caller's LOCODES map omits a terminal (e.g. because it doesn't
    # have a seeded unlocode), even a perfect-LOCODE input still misses.
    sparse = {"NLRTM": 10}
    assert parse_destination("BEZEE", sparse) == (None, False)


def test_plaquemines_resolves_via_uspql():
    # Vessels broadcast USPLQ for Plaquemines (the seed previously had USPMS,
    # which never appears in real traffic).
    assert parse_destination("USPLQ", LOCODES) == (2, False)
    assert parse_destination("PLAQUEMINES", LOCODES) == (2, False)


@pytest.mark.parametrize(
    "dest",
    [
        "FORORDERS",  # no space
        "FOR  ORDERS",  # double space
        "USG FOR ORDERS",  # region prefix, no concrete port
        "BALTIC FOR ORDERS",
        "ATLANTIC FOR ORDER",
    ],
)
def test_for_orders_variants(dest):
    assert parse_destination(dest, LOCODES) == (None, True)


def test_suffix_decorated_locode_recovers_leading_token():
    # Operators append ETA/distance notes after a valid LOCODE.
    assert parse_destination("ESCAR<D9 HRS", LOCODES) == (24, False)
    assert parse_destination("BEZEE DE 86 HRS", LOCODES) == (12, False)


def test_named_port_wins_over_for_orders_qualifier():
    # "USCRP FOR ORDERS" declares Corpus (even if uncommitted) — resolve it.
    assert parse_destination("USCRP FOR ORDERS", LOCODES) == (4, False)


def test_for_orders_does_not_match_inside_words():
    # The \b guard must not trip on "BEFORE"; neither input resolves either.
    assert parse_destination("BEFORE DEPARTURE", LOCODES) == (None, False)
    assert parse_destination("FORMOSA", LOCODES) == (None, False)


def test_leading_token_does_not_slice_longer_alnum_runs():
    # The leading-token fallback must not slice the first 5 chars out of a
    # longer word: "USCAUTION" is not Cameron (USCAU), "NLRTMOUTH" not Rotterdam.
    assert parse_destination("USCAUTION", LOCODES) == (None, False)
    assert parse_destination("NLRTMOUTH", LOCODES) == (None, False)
    # A space/punctuation-delimited LOCODE still resolves via the leading token.
    assert parse_destination("USCAU OUTBOUND", LOCODES) == (5, False)
