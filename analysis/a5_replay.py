"""A5 replay harness — run the outage detectors and score them (D-019).

Read-only. Every constant was fixed in D-019 before this ran; this module only
measures. Note the D-017 rule is applied here at last: each terminal's scored span
ends at **its own last departure**, not the panel's data max.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import timedelta

import asyncpg

from analysis.a5 import (
    BURN_IN_DAYS,
    DETECT_WINDOW_D,
    bocpd,
    daily_counts,
    date_range,
    label_outages,
    null_absolute,
    null_rate_relative,
    score_detector,
    summarise,
)
from config import settings

logger = logging.getLogger(__name__)

DEPARTURES_SQL = """
SELECT p.terminal_id, t.terminal_name, p.event_time::date AS day
FROM port_events p
JOIN terminals t ON t.terminal_id = p.terminal_id
WHERE p.event_type = 'departed'
  AND p.laden_flag IS TRUE
  AND p.zone IN ('usgulf', 'usatlantic')
  AND p.regime = $1
ORDER BY p.terminal_id, day
"""

COVE_POINT = "Cove Point"


async def load(pool: asyncpg.Pool, regime: str):
    async with pool.acquire() as conn:
        rows = await conn.fetch(DEPARTURES_SQL, regime)
    return [(r["terminal_id"], r["terminal_name"], r["day"]) for r in rows]


def run_detectors(departures):
    """Alarms per terminal for BOCPD and the two nulls, plus each span's length."""
    by_terminal: dict[int, list] = {}
    names: dict[int, str] = {}
    for tid, name, day in departures:
        by_terminal.setdefault(tid, []).append(day)
        names[tid] = name

    bocpd_alarms: dict[int, list] = {}
    n1_alarms: dict[int, list] = {}
    n2_alarms: dict[int, list] = {}
    spans: dict[int, int] = {}

    for tid, dep_days in by_terminal.items():
        dep_days = sorted(dep_days)
        # D-017: the span ends at this terminal's OWN last departure.
        start, end = dep_days[0], dep_days[-1]
        if (end - start).days <= BURN_IN_DAYS:
            continue
        days = date_range(start, end)
        spans[tid] = len(days)
        counts = daily_counts(days, dep_days)

        steps = bocpd(days, counts)
        # Collapse consecutive alarm days into one alarm per episode, so a long
        # outage is not counted as dozens of separate alarms.
        alarms = []
        prev = None
        for s in steps:
            if s.alarm:
                if prev is None or (s.day - prev).days > 1:
                    alarms.append(s.day)
                prev = s.day
        bocpd_alarms[tid] = alarms
        n1_alarms[tid] = null_absolute(days, dep_days)
        n2_alarms[tid] = null_rate_relative(days, dep_days)

    return bocpd_alarms, n1_alarms, n2_alarms, spans, names


def report_block(label, alarms, outages, spans):
    dets, far, tyears = score_detector(alarms, outages, spans)
    recall, med, n_hit = summarise(dets)
    med_s = f"{med:.0f}" if med is not None else "-"
    print(f"  {label:22s} recall {recall:5.0%} ({n_hit}/{len(dets)})   "
          f"median delay {med_s:>4s} d   false alarms/terminal-yr {far:5.2f}")
    return dets, far, med, recall


def report(outages, bocpd_alarms, n1_alarms, n2_alarms, spans, names):
    print(f"\n{'=' * 100}\nA5 OUTAGE-DETECTION SCORECARD\n{'=' * 100}")
    tyears = sum(spans.values()) / 365.25
    print(f"labelled outages: {len(outages)}   terminals: {len(spans)}   "
          f"terminal-years: {tyears:.1f}   detection window: {DETECT_WINDOW_D} d")

    print(f"\n{'terminal':16s} {'start':12s} {'gap_d':>6s} {'base':>6s} {'ratio':>7s}"
          f"   {'BOCPD':>7s} {'N1':>5s} {'N2':>5s}   (delay in days)")
    for o in outages:
        cells = []
        for al in (bocpd_alarms, n1_alarms, n2_alarms):
            hits = [
                a for a in al.get(o.terminal_id, [])
                if o.start <= a <= o.start + timedelta(days=DETECT_WINDOW_D)
            ]
            cells.append(f"{(min(hits) - o.start).days:d}" if hits else "-")
        print(f"{o.terminal_name:16s} {o.start!s:12s} {o.gap_d:6.0f} "
              f"{o.baseline_gap_d:6.1f} {o.ratio:7.1f}   "
              f"{cells[0]:>7s} {cells[1]:>5s} {cells[2]:>5s}")

    print("\n--- ALL LABELLED OUTAGES (headline) " + "-" * 62)
    b = report_block("BOCPD", bocpd_alarms, outages, spans)
    n1 = report_block("N1 absolute (14d)", n1_alarms, outages, spans)
    n2 = report_block("N2 rate-relative", n2_alarms, outages, spans)

    ex = [o for o in outages if o.terminal_name != COVE_POINT]
    print(f"\n--- EXCLUDING THE COVE POINT CLUSTER (n={len(ex)}, D-019 secondary) "
          + "-" * 25)
    report_block("BOCPD", bocpd_alarms, ex, spans)
    report_block("N1 absolute (14d)", n1_alarms, ex, spans)
    report_block("N2 rate-relative", n2_alarms, ex, spans)

    # --- verdict ---------------------------------------------------------
    print(f"\n{'=' * 100}\nPRE-REGISTERED ACCEPTANCE (D-019)\n{'=' * 100}")
    _, b_far, b_med, b_recall = b
    _, n1_far, n1_med, _ = n1
    _, n2_far, n2_med, _ = n2
    null_meds = [m for m in (n1_med, n2_med) if m is not None]
    best_null_far = min(n1_far, n2_far)
    if b_med is None or not null_meds:
        print("  a detector produced no detections — cannot evaluate")
        return
    faster = all(b_med < m for m in null_meds)
    no_worse_far = b_far <= best_null_far
    print(f"  BOCPD median delay {b_med:.0f} d vs N1 {n1_med:.0f} d, "
          f"N2 {n2_med:.0f} d  -> {'faster than both' if faster else 'NOT faster'}")
    print(f"  BOCPD false alarms/terminal-yr {b_far:.2f} vs best null "
          f"{best_null_far:.2f}  -> {'no worse' if no_worse_far else 'WORSE'}")
    if faster and no_worse_far:
        verdict = "A5 WORKS"
    elif faster:
        verdict = "INCONCLUSIVE (faster but noisier — D-019 counts this as no win)"
    else:
        verdict = "A5 does NOT meet the bar"
    print(f"\n  VERDICT: {verdict}")


async def main() -> None:
    ap = argparse.ArgumentParser(description="A5 outage-detection replay")
    ap.add_argument("--regime", default="noaa")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=2)
    try:
        departures = await load(pool, args.regime)
    finally:
        await pool.close()
    logger.info("loaded %d laden US departures (regime=%s)",
                len(departures), args.regime)

    outages = label_outages(departures)
    logger.info("labelled %d outages", len(outages))
    b, n1, n2, spans, names = run_detectors(departures)
    # Only score outages at terminals that actually have a scored span.
    outages = [o for o in outages if o.terminal_id in spans]
    report(outages, b, n1, n2, spans, names)


if __name__ == "__main__":
    asyncio.run(main())
