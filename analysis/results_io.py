"""Machine-readable replay results for the paper.

Every replay prints a scorecard for humans. The paper needs the same numbers as
data — no number in a table or in prose is typed by hand — so each replay can
also `dump()` what it printed to `paper/results/<name>.json`. `paper/tables.py`
reads those files and writes the LaTeX fragments and the `numbers.tex` macros.

Dataclasses are serialised with `asdict`; dates and datetimes as ISO strings;
NaN/inf as null so the JSON stays valid.
"""

from __future__ import annotations

import dataclasses
import json
import math
from datetime import date, datetime
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent.parent / "paper" / "results"


def _clean(obj):
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return _clean(dataclasses.asdict(obj))
    if isinstance(obj, dict):
        return {str(k): _clean(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_clean(v) for v in obj]
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if hasattr(obj, "item"):  # numpy scalars
        return _clean(obj.item())
    return obj


def dump(name: str, payload: dict) -> Path:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = RESULTS_DIR / f"{name}.json"
    out.write_text(json.dumps(_clean(payload), indent=2, sort_keys=True) + "\n")
    print(f"[results] wrote {out.relative_to(RESULTS_DIR.parent.parent)}")
    return out
