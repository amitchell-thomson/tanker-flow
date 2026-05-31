"""Geographic helpers shared across the pipeline.

Pure functions — no DB, no deps beyond the stdlib. Used by the state machine's
nearest-berth tiebreaker and by the voyage-leg derivation (great-circle leg
distance for ton-miles).
"""

from __future__ import annotations

import math


def haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in nautical miles."""
    r_nm = 3440.065  # earth radius in nm
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    )
    return 2 * r_nm * math.asin(math.sqrt(a))
