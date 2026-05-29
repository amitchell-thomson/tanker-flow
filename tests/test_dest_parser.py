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
    "USCRP": 4,
    "USCAU": 5,
    "USFPO": 6,
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
        "for orders",            # case-insensitive
        "OPEN SEA FOR ORDERS",
        "GERMANY FOR ORDERS",
        "EU FOR ORDERS",
        "  AT SEA  ",            # whitespace-tolerant
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
    assert parse_destination("KRYOS > USNSS", LOCODES) == (None, False)


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
    # USNSS appears 6000+ times in our data but isn't a known LNG terminal
    # LOCODE (looks like operator shorthand). We don't fail — we return None.
    assert parse_destination("USNSS", LOCODES) == (None, False)
    assert parse_destination("RANDOM GARBAGE", LOCODES) == (None, False)


def test_terminal_not_in_provided_locodes_returns_none():
    # If the caller's LOCODES map omits a terminal (e.g. because it doesn't
    # have a seeded unlocode), even a perfect-LOCODE input still misses.
    sparse = {"NLRTM": 10}
    assert parse_destination("BEZEE", sparse) == (None, False)
