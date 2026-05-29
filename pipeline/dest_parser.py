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
        "TBN",            # to be nominated
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
    "PLAQUEMINES": "USPMS",
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


def parse_destination(
    dest_str: str | None,
    unlocode_to_terminal: dict[str, int],
) -> tuple[int | None, bool]:
    """Return (terminal_id, is_for_orders).

    - `(terminal_id, False)` — vessel declared a destination matching one of
      our terminals.
    - `(None, True)` — vessel declared an explicit "no destination yet"
      marker (FOR ORDERS, etc.).
    - `(None, False)` — empty, unparseable, or matches a terminal we don't
      track.
    """
    if not dest_str:
        return (None, False)

    s = dest_str.strip().upper()
    if not s:
        return (None, False)

    if s in FOR_ORDERS_MARKERS:
        return (None, True)

    # Chained destinations: "USSAB>NLRTM" or "USSAB > NLRTM" — the right-hand
    # side is where the vessel is headed *next*. Use it.
    if ">" in s:
        s = s.split(">")[-1].strip()
        # An empty RHS or another FOR-ORDERS marker after stripping → bail.
        if not s:
            return (None, False)
        if s in FOR_ORDERS_MARKERS:
            return (None, True)

    # Direct LOCODE-with-spaces case: "NL RTM" / "BE ZEE".
    compact = s.replace(" ", "")
    if compact in unlocode_to_terminal:
        return (unlocode_to_terminal[compact], False)

    # Freeform name? Normalize and re-look-up.
    if compact in FREEFORM_TO_LOCODE:
        canonical = FREEFORM_TO_LOCODE[compact]
        if canonical in unlocode_to_terminal:
            return (unlocode_to_terminal[canonical], False)

    return (None, False)
