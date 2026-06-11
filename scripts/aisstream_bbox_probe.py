#!/usr/bin/env python
"""Stage-3c throttle probe: does a *small* terminal-approach bbox subscription
stay under AISstream's per-account throttle?

Context. The 2026-05-30 cutover abandoned bbox-mode subscriptions because
AISstream's per-account throttle stochastically dropped vessels under the *large*
7-zone load (~23% per-LNG-carrier visibility in 20-min windows; 96% of phantom
open legs date to that regime). MMSI filtering fixed it by keeping each
subscription's "ask" tiny. The Stage-3c hypothesis: a bbox subscription over just
the **31 terminal-approach boxes** (a far smaller ask than 7 whole zones) may stay
under the throttle — and a bbox catch-all is the only free way to hear vessels we
did NOT predict (Class A not-in-fleet, Class B unresolved-dest) right at the berth.

This probe does NOT deploy anything. It opens one bbox-only connection (no MMSI
filter) over the small terminal boxes, listens for `--minutes`, and reports the
per-vessel report cadence. A healthy/unthrottled feed gives each vessel a steady
cadence (gaps of seconds-to-minutes); a throttled feed drops vessels (long gaps,
sparse coverage) — the symptom that killed bbox-mode. Read the gap stats and
decide: steady ⇒ deploy 3c; dropping ⇒ abandon 3c and spend the second IP's conns
on pure-MMSI sharding instead.

**Run it from the Oracle worker's IP, not home.** The home IP already holds its
3-connection cap (the live ingester), so a 4th connection from home is refused
(429). The probe needs its own connection budget — which is exactly what the
Oracle egress provides. It reaches the home DB over Tailscale to fetch the boxes.

Usage (on the Oracle VM, after the Tailscale link is up):
    uv run python scripts/aisstream_bbox_probe.py --minutes 20
    uv run python scripts/aisstream_bbox_probe.py --minutes 20 --pad 0.25 --key alt
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncpg  # noqa: E402
import websockets  # noqa: E402

from config import settings  # noqa: E402

AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"

# Per-terminal bounding box from the terminal_zones polygons (the approach
# envelopes), padded by --pad degrees so we catch vessels on final approach, not
# only those already alongside. One small box per terminal = the "small ask".
BOXES_SQL = """
SELECT t.terminal_name,
       ST_YMin(e.ext) AS lat_min, ST_YMax(e.ext) AS lat_max,
       ST_XMin(e.ext) AS lon_min, ST_XMax(e.ext) AS lon_max
FROM (SELECT terminal_id, ST_Extent(geom) AS ext FROM terminal_zones GROUP BY terminal_id) e
JOIN terminals t USING (terminal_id)
WHERE t.in_signal_scope
ORDER BY t.terminal_name
"""

# A report cadence gap longer than this is the throttle symptom (a vessel we
# should be hearing steadily went quiet on the bbox feed).
GAP_ALARM_SECONDS = 300.0


@dataclass
class VesselStat:
    first_seen: float
    last_seen: float
    count: int = 0
    gaps: list[float] = field(default_factory=list)


async def load_boxes(pad: float) -> list[list[list[float]]]:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=1)
    try:
        rows = await pool.fetch(BOXES_SQL)
    finally:
        await pool.close()
    boxes = []
    for r in rows:
        # AISstream box format: [[lat_min, lon_min], [lat_max, lon_max]].
        boxes.append(
            [
                [r["lat_min"] - pad, r["lon_min"] - pad],
                [r["lat_max"] + pad, r["lon_max"] + pad],
            ]
        )
    print(f"Derived {len(boxes)} terminal-approach boxes (pad {pad}°)")
    return boxes


async def run(minutes: float, pad: float, api_key: str) -> None:
    boxes = await load_boxes(pad)
    if not boxes:
        print("No in-scope terminal boxes found — is terminal_zones seeded?")
        return

    deadline = time.monotonic() + minutes * 60.0
    stats: dict[int, VesselStat] = {}
    total_msgs = 0
    started = time.monotonic()

    sub = {
        "APIKey": api_key,
        "BoundingBoxes": boxes,
        "FilterMessageTypes": ["PositionReport"],
    }
    print(f"Subscribing (bbox-only, no MMSI filter) for {minutes:.0f} min…")
    async with websockets.connect(AISSTREAM_URL, ping_interval=20) as ws:
        await ws.send(json.dumps(sub))
        while time.monotonic() < deadline:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=30)
            except asyncio.TimeoutError:
                continue
            try:
                msg = json.loads(raw)
                mmsi = int(msg["MetaData"]["MMSI"])
            except (ValueError, KeyError, TypeError):
                continue
            now = time.monotonic()
            total_msgs += 1
            st = stats.get(mmsi)
            if st is None:
                stats[mmsi] = VesselStat(first_seen=now, last_seen=now, count=1)
            else:
                st.gaps.append(now - st.last_seen)
                st.last_seen = now
                st.count += 1

    elapsed_min = (time.monotonic() - started) / 60.0
    _report(stats, total_msgs, elapsed_min)


def _report(stats: dict[int, VesselStat], total_msgs: int, elapsed_min: float) -> None:
    distinct = len(stats)
    print("\n" + "=" * 64)
    print(f"bbox probe — {elapsed_min:.1f} min")
    print(f"  distinct MMSIs heard : {distinct}")
    print(f"  total messages       : {total_msgs}  ({total_msgs / max(elapsed_min, 1e-9):.0f}/min)")
    # Cadence: among vessels seen >=3 times (enough for a gap series), how steady?
    multi = [s for s in stats.values() if len(s.gaps) >= 2]
    if multi:
        med_gaps = [statistics.median(s.gaps) for s in multi]
        max_gaps = [max(s.gaps) for s in multi]
        dropped = sum(1 for g in max_gaps if g > GAP_ALARM_SECONDS)
        overall_med = statistics.median(med_gaps)
        p90 = (
            f" (p90 {statistics.quantiles(med_gaps, n=10)[-1]:.0f}s)"
            if len(med_gaps) >= 10
            else ""
        )
        print(f"  vessels w/ cadence   : {len(multi)} (>=3 reports)")
        print(f"  median report gap    : {overall_med:.0f}s{p90}")
        print(
            f"  vessels w/ gap >{GAP_ALARM_SECONDS:.0f}s : "
            f"{dropped}/{len(multi)} ({dropped / len(multi) * 100:.0f}%)  "
            f"← the throttle symptom; high = bbox-mode still drops vessels"
        )
    print("=" * 64)
    print("Decide: steady cadence + low gap% ⇒ deploy the 3c catch-all; "
          "sparse / high gap% ⇒ abandon 3c, use the 2nd IP for pure-MMSI sharding.")


def main() -> None:
    p = argparse.ArgumentParser(description="Stage-3c bbox throttle probe")
    p.add_argument("--minutes", type=float, default=20.0, help="probe duration")
    p.add_argument("--pad", type=float, default=0.2, help="degrees to pad each box")
    p.add_argument(
        "--key",
        choices=["main", "alt"],
        default="main",
        help="which AISstream key (per-IP cap is unaffected by key; see conn-test)",
    )
    args = p.parse_args()
    key = settings.aisstream_api_key_alt if args.key == "alt" else settings.aisstream_api_key
    asyncio.run(run(args.minutes, args.pad, key))


if __name__ == "__main__":
    main()
