"""Parse the free-text AIS `vessel_state.dest` field into a terminal_id.

`dest` is master-typed at the bridge. It's noisy: a vessel that just left
Sabine bound for Rotterdam might broadcast any of `NLRTM`, `NL RTM`, `NLRTM`,
`ROTTERDAM`, `USSAB>NLRTM`, `FOR ORDERS`, or empty. This parser produces a
canonical (terminal_id, is_for_orders) tuple — used by pipeline/scoring.py
to assign tier-2 ("declared inbound") status.

Resolution strategy:
1. Strip + uppercase; treat empty as `(None, False)`.
2. If matches a known "no real destination" marker → `(None, True)`.
3. If contains `>` → use the right-hand side (the next port in the chain).
4. Strip internal spaces (UN/LOCODE is contiguous).
5. Look up against the per-terminal `unlocode` map.
6. If still no hit, apply an in-code normalizer that maps common freeform
   names ("ROTTERDAM", "EEMSHAVEN", "SABINE") to their canonical LOCODE.
   Re-check the LOCODE map.
7. Return `(terminal_id, False)` on hit, `(None, False)` on miss.

The freeform-name normaliser lives here as code (not data) because the
mapping is tied to parser behaviour, not to the identity of the terminal —
operators may rename a terminal, but `ROTTERDAM` will always mean the
Rotterdam LOCODE.
"""

from __future__ import annotations

import re

# Markers vessels use when they have no committed destination. None of these
# should be treated as a terminal match.
FOR_ORDERS_MARKERS: frozenset[str] = frozenset(
    {
        "FOR ORDERS",
        "FOR ORDER",
        "EU FOR ORDERS",
        "EUROPE FOR ORDERS",
        "GERMANY FOR ORDERS",
        "ASIA FOR ORDERS",
        "OPEN SEA",
        "OPEN SEA FOR ORDERS",
        "AT SEA",
        "AWAITING ORDERS",
        "TBN",  # to be nominated
        "ORDERS",
        "NIL",
    }
)

# Plain-name aliases → canonical UN/LOCODE. Includes common operator/news
# shorthand and short freeform forms that vessels broadcast. Keys are matched
# after normalization (uppercase, spaces stripped).
FREEFORM_TO_LOCODE: dict[str, str] = {
    # NW Europe
    "ROTTERDAM": "NLRTM",
    "GATE": "NLRTM",
    "GATETERMINAL": "NLRTM",
    "EEMSHAVEN": "NLEEM",
    "EEMS": "NLEEM",
    "ZEEBRUGGE": "BEZEE",
    "DUNKERQUE": "FRDKK",
    "DUNKIRK": "FRDKK",
    "ISLEOFGRAIN": "GBIOG",
    "GRAIN": "GBIOG",
    "SOUTHHOOK": "GBMIL",
    "MILFORDHAVEN": "GBMIL",
    "BRUNSBUTTEL": "DEBRB",
    "WILHELMSHAVEN": "DEWVN",
    "WHV": "DEWVN",
    "LUBMIN": "DELUB",
    # Baltic
    "SWINOUJSCIE": "PLSWI",
    "MUKRAN": "DEMUK",
    "KLAIPEDA": "LTKLJ",
    # Iberian
    "SINES": "PTSIE",
    "BILBAO": "ESBIO",
    "HUELVA": "ESHUV",
    # W Med
    "BARCELONA": "ESBCN",
    "CARTAGENA": "ESCAR",
    "SAGUNTO": "ESSAG",
    "ADRIATICLNG": "ITRVS",
    "PORTOVIRO": "ITRVS",
    "PIOMBINO": "ITPIO",
    "RAVENNA": "ITRAN",
    "KRK": "HRKRK",
    # E Med
    "REVITHOUSSA": "GRRVT",
    "ALEXANDROUPOLIS": "GRAXD",
    # US Gulf
    "SABINE": "USSAB",
    "SABINEPASS": "USSAB",
    "PLAQUEMINES": "USPLQ",
    "CALCASIEU": "USCLU",
    "CORPUSCHRISTI": "USCRP",
    "CORPUS": "USCRP",
    "CAMERON": "USCAU",
    "FREEPORT": "USFPO",
    "GOLDENPASS": "USPSX",
    # US Atlantic
    "COVEPOINT": "USCVL",
    "ELBAISLAND": "USEII",
    "ELBA": "USEII",
}


# Matches a "for orders" / "for order" phrase anywhere, tolerant of zero or
# more internal spaces ("FORORDERS", "FOR  ORDERS", "USG FOR ORDERS"). The
# leading \b avoids matching inside words like "BEFORE".
FOR_ORDERS_RE = re.compile(r"\bFOR\s*ORDERS?\b")


def _is_for_orders(s: str) -> bool:
    """True if the normalized (upper) string declares no committed port."""
    return s in FOR_ORDERS_MARKERS or FOR_ORDERS_RE.search(s) is not None


def _resolve_locode(s: str, unlocode_to_terminal: dict[str, int]) -> int | None:
    """Resolve an already-normalized (upper, stripped) destination to a
    terminal_id, or None. Tries, in order: exact compact LOCODE ("NL RTM"),
    freeform-name alias ("ROTTERDAM"), then a leading LOCODE-shaped token —
    which recovers suffix-decorated values like "ESCAR<D9 HRS" or
    "BEZEE DE 86 HRS" that operators append ETA/distance notes to."""
    compact = s.replace(" ", "")
    if compact in unlocode_to_terminal:
        return unlocode_to_terminal[compact]
    if compact in FREEFORM_TO_LOCODE:
        canonical = FREEFORM_TO_LOCODE[compact]
        if canonical in unlocode_to_terminal:
            return unlocode_to_terminal[canonical]

    # Leading LOCODE-shaped token: 2 letters + optional single space + 3 alnum.
    m = re.match(r"([A-Z]{2}\s?[A-Z0-9]{3})", s)
    if m:
        lead = m.group(1).replace(" ", "")
        if lead in unlocode_to_terminal:
            return unlocode_to_terminal[lead]
        canonical = FREEFORM_TO_LOCODE.get(lead)
        if canonical is not None and canonical in unlocode_to_terminal:
            return unlocode_to_terminal[canonical]
    return None


def parse_destination(
    dest_str: str | None,
    unlocode_to_terminal: dict[str, int],
) -> tuple[int | None, bool]:
    """Return (terminal_id, is_for_orders).

    - `(terminal_id, False)` — vessel declared a destination matching one of
      our terminals (a named port wins even when a "FOR ORDERS" qualifier is
      appended, e.g. "USCRP FOR ORDERS").
    - `(None, True)` — vessel declared an explicit "no destination yet"
      marker (FOR ORDERS, OPEN SEA, TBN, …) and named no port we track.
    - `(None, False)` — empty, unparseable, or matches a terminal we don't
      track.
    """
    if not dest_str:
        return (None, False)

    s = dest_str.strip().upper()
    if not s:
        return (None, False)

    # Chained destinations: "USSAB>NLRTM" / "USSAB > NLRTM" — the right-hand
    # side is where the vessel is headed *next*. Use it.
    if ">" in s:
        s = s.split(">")[-1].strip()
        if not s:
            return (None, False)

    # A concrete terminal takes precedence over a "for orders" qualifier.
    terminal_id = _resolve_locode(s, unlocode_to_terminal)
    if terminal_id is not None:
        return (terminal_id, False)

    if _is_for_orders(s):
        return (None, True)

    return (None, False)
