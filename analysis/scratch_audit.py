"""THROWAWAY audit script (park-readiness audit 2026-06-02). NOT production code.

Read-only. Loads the leg/visit/signal inputs ONCE from the live DB, then re-runs
the pure pairing/aggregation functions in-memory under varied constants to:
  A3  quantify phantom legs + the censoring inflation guard
  A4  sweep the judgment-call constants (OD windows, censor cap, recent-fix,
      open-visit ceiling) and report how the headline contributor set / current
      stock moves
  A5  null gas / coverage rates
  A6  DFA validate_sequence on the live per-vessel event streams

Writes NOTHING. Run: PYTHONPATH=. uv run python analysis/scratch_audit.py
"""

from __future__ import annotations

import asyncio
from collections import Counter
from datetime import UTC, datetime

import asyncpg

import pipeline.legs as L
import pipeline.signal as S
from config import settings
from pipeline.legs import LegEvent, pair_legs
from pipeline.signal import (
    ballast_to_us_legs,
    build_lane_filter,
    discharging_eu_visits,
    items_live_on,
    lane_legs,
    leg_interval,
    loading_us_visits,
    transit_dest_band,
    visit_interval,
)
from pipeline.visits import VisitEvent, pair_visits


def hr(t):
    print("\n" + "=" * 70 + f"\n{t}\n" + "=" * 70)


def stock_now(items, panel_end, interval_of):
    """Current-day live stock (sum gas_capacity_m3 of items live on panel_end)."""
    live = items_live_on(items, panel_end, interval_of)
    vol = sum(it.gas_capacity_m3 or 0 for it in live)
    return len(live), vol


async def main():
    now = datetime.now(UTC)
    panel_end = now.date()
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=2)
    async with pool.acquire() as c:
        ev = await c.fetch(L.LEG_EVENTS_SQL)
        w = await c.fetch(L.WEIGHTS_SQL)
        dr = await c.fetch(L.DEST_REGION_SQL)
        lf = await c.fetch(L.LAST_FIX_SQL)
        vev = await c.fetch(
            "SELECT mmsi,event_type,event_time,zone,terminal_id,laden_flag,cold_start FROM port_events WHERE event_type IN ('moored','departed') ORDER BY mmsi,event_time"
        )
        fd = await c.fetch(
            "SELECT terminal_id,flow_direction FROM terminals WHERE flow_direction IS NOT NULL"
        )
        term = await c.fetch(S.TERMINAL_METADATA_SQL)
        allev = await c.fetch(
            "SELECT mmsi,event_type,event_time FROM port_events ORDER BY mmsi,event_time"
        )
        fsru = await c.fetch("SELECT mmsi FROM vessel_registry WHERE is_fsru")
    await pool.close()

    print(f"now (pinned) = {now.isoformat()}   panel_end = {panel_end}")
    print(
        f"leg-events rows={len(ev)}  visit-events rows={len(vev)}  weights={len(w)}  dest_regions={len(dr)}  last_fixes={len(lf)}"
    )

    weights = {r["mmsi"]: (r["dwt"], r["gas_capacity_m3"]) for r in w}
    dest_regions = {r["mmsi"]: r["region"] for r in dr}
    last_fixes = {r["mmsi"]: (r["fix_ts"], r["lat"], r["lon"]) for r in lf}
    flow_directions = {r["terminal_id"]: r["flow_direction"] for r in fd}
    legevents = [
        LegEvent(
            r["mmsi"],
            r["event_type"],
            r["event_time"],
            r["zone"],
            r["terminal_id"],
            r["lat"],
            r["lon"],
            r["laden_flag"],
        )
        for r in ev
    ]
    visitevents = [
        VisitEvent(
            r["mmsi"],
            r["event_type"],
            r["event_time"],
            r["zone"],
            r["terminal_id"],
            r["laden_flag"],
            r["cold_start"],
        )
        for r in vev
    ]
    lane = build_lane_filter(term)

    def make_legs(*, censor=30, od=None, recent=4, fallback="nweurope"):
        old_recent = L.RECENT_FIX_DAYS
        L.RECENT_FIX_DAYS = recent
        try:
            return pair_legs(
                legevents,
                now,
                censor_days=censor,
                weights=weights,
                dest_regions=dest_regions,
                last_fixes=last_fixes,
                od_windows=od or L.OD_WINDOW_DAYS,
                fallback_region=fallback,
            )
        finally:
            L.RECENT_FIX_DAYS = old_recent

    legs = make_legs()
    visits = pair_visits(visitevents, weights=weights, flow_directions=flow_directions)

    # ---- A3: leg status distribution + phantom quantification ----
    hr("A3  leg status distribution (baseline: censor=30, OD default, recent=4)")
    status = Counter(lg.status for lg in legs)
    for s, n in sorted(status.items()):
        print(f"  {s:18s} {n}")
    print(f"  TOTAL legs {len(legs)}")
    # by regime
    print("  by regime:")
    for rg in ("bbox", "mmsi_filter"):
        c2 = Counter(lg.status for lg in legs if lg.regime == rg)
        tot = sum(c2.values())
        phantom = c2.get("open_censored", 0) + c2.get("open_arrival_gap", 0)
        opn = sum(v for k, v in c2.items() if k.startswith("open"))
        print(
            f"    {rg:12s} total={tot:3d}  open={opn:3d}  censored={c2.get('open_censored', 0):3d}  arrival_gap={c2.get('open_arrival_gap', 0):3d}  floating={c2.get('open_floating', 0):3d}  in_transit={c2.get('open_in_transit', 0):3d}  phantom%={(100 * phantom / opn if opn else 0):.0f}"
        )

    # ---- A3: censoring inflation guard ----
    hr("A3  in-transit current-day stock: censoring ON vs OFF")
    transit = lane_legs(legs, lane)
    n_on, v_on = stock_now(transit, panel_end, leg_interval)
    # censoring OFF: treat every laden export-origin open leg as in-transit
    transit_off = []
    for lg in legs:
        if lg.laden is not True or not lane.is_export(lg.origin_zone):
            continue
        if lg.status == "closed" and lane.is_import(lg.dest_zone):
            transit_off.append(lg)
        elif lg.status.startswith("open"):
            transit_off.append(lg)
    n_off, v_off = stock_now(transit_off, panel_end, leg_interval)
    print(f"  censoring ON  (open_in_transit only): {n_on} legs live now, {v_on:,} m3")
    print(
        f"  censoring OFF (all open kept):         {n_off} legs live now, {v_off:,} m3"
    )
    print(
        f"  inflation if censoring removed: +{n_off - n_on} legs, +{v_off - v_on:,} m3 ({(100 * v_off / v_on - 100) if v_on else 0:.0f}% over)"
    )

    # ---- A4: sensitivity sweeps ----
    hr("A4  sensitivity: OD window (nweurope) — leg-status + in-transit stock")
    for win in (12, 15, 18, 21, 25, 30):
        lg2 = make_legs(od={**L.OD_WINDOW_DAYS, "nweurope": win})
        st = Counter(lg.status for lg in lg2)
        tr = lane_legs(lg2, lane)
        n, v = stock_now(tr, panel_end, leg_interval)
        print(
            f"  nweurope_window={win:3d}d  in_transit={st.get('open_in_transit', 0):3d} censored={st.get('open_censored', 0):3d} floating={st.get('open_floating', 0):3d} arr_gap={st.get('open_arrival_gap', 0):3d} | stock_now: {n} legs {v:,} m3"
        )

    hr("A4  sensitivity: CENSOR_OPEN_DAYS (affects undeclared/no-fallback legs)")
    for cz in (18, 24, 30, 40, 60):
        lg2 = make_legs(censor=cz)
        st = Counter(lg.status for lg in lg2)
        print(
            f"  censor_days={cz:3d}  in_transit={st.get('open_in_transit', 0):3d} censored={st.get('open_censored', 0):3d} floating={st.get('open_floating', 0):3d} arr_gap={st.get('open_arrival_gap', 0):3d}"
        )

    hr("A4  sensitivity: RECENT_FIX_DAYS (floating vs phantom boundary)")
    for rf in (2, 3, 4, 7, 14):
        lg2 = make_legs(recent=rf)
        st = Counter(lg.status for lg in lg2)
        print(
            f"  recent_fix_days={rf:3d}  floating={st.get('open_floating', 0):3d} censored={st.get('open_censored', 0):3d} arr_gap={st.get('open_arrival_gap', 0):3d}"
        )

    hr("A4  sensitivity: OPEN_VISIT_CEILING_DAYS (in-berth stock)")
    loading = loading_us_visits(visits)
    discharging = discharging_eu_visits(visits)
    old_ceil = S.OPEN_VISIT_CEILING_DAYS
    for cd in (1, 2, 3, 5, 10, 30):
        S.OPEN_VISIT_CEILING_DAYS = cd
        nl, vl = stock_now(loading, panel_end, visit_interval)
        nd, vd = stock_now(discharging, panel_end, visit_interval)
        print(
            f"  open_visit_ceiling={cd:3d}d  loading_us now: {nl} visits {vl:,} m3 | discharging_eu now: {nd} visits {vd:,} m3"
        )
    S.OPEN_VISIT_CEILING_DAYS = old_ceil

    # ---- A5: null gas + coverage ----
    hr("A5  null gas_capacity_m3 + coverage")
    transit = lane_legs(legs, lane)
    ballast = ballast_to_us_legs(legs, lane)
    null_leg = sum(1 for lg in transit + ballast if lg.gas_capacity_m3 is None)
    null_vis = sum(1 for v in discharging + loading if v.gas_capacity_m3 is None)
    print(
        f"  in-transit+ballast legs={len(transit) + len(ballast)}  null gas={null_leg}"
    )
    print(
        f"  discharging+loading visits={len(discharging) + len(loading)}  null gas={null_vis}"
    )
    unk = sum(1 for lg in transit if transit_dest_band(lg, lane) == S.UNKNOWN_BAND)
    print(f"  in-transit legs in 'unknown' dest band: {unk}/{len(transit)}")
    print(f"  vessels with a declared/resolved dest region: {len(dest_regions)}")

    # ---- A6: DFA validate_sequence on live streams ----
    hr("A6  DFA validate_sequence on live per-vessel event streams")
    try:
        from types import SimpleNamespace

        from pipeline.state_machine import validate_sequence  # type: ignore

        fsru_mmsis = {r["mmsi"] for r in fsru}
        by = {}
        for r in allev:
            by.setdefault(r["mmsi"], []).append(
                SimpleNamespace(event_type=r["event_type"], event_time=r["event_time"])
            )
        print(
            "  NOTE: validate_sequence runs at BUILD time in port_events.py:346 (emission order, non-FSRU);"
        )
        print(
            "  this re-scan uses event_time order, which differs on tied timestamps + FSRU bare-moored."
        )
        real, tie_artifact, fsru_artifact = [], [], []
        for mmsi, seq in by.items():
            if mmsi in fsru_mmsis:
                # FSRU bare-moored bypasses the walk; not validated at build time
                try:
                    validate_sequence(seq)
                except Exception:  # noqa: BLE001
                    fsru_artifact.append(mmsi)
                continue
            prev = None
            prev_ts = None
            for e in seq:
                # replicate _ALLOWED_NEXT check, but mark tied-timestamp transitions
                try:
                    validate_sequence(
                        [SimpleNamespace(event_type=prev, event_time=prev_ts)]
                        if prev
                        else []
                    )
                except Exception:  # noqa: BLE001
                    pass
                prev = e.event_type
                prev_ts = e.event_time
            try:
                validate_sequence(seq)
            except AssertionError as ex:
                # is the violating transition between two events sharing a timestamp?
                tied = False
                for a, b in zip(seq, seq[1:]):
                    if a.event_time == b.event_time:
                        tied = True
                        break
                (tie_artifact if tied else real).append((mmsi, str(ex)))
        print(
            f"  non-FSRU vessels={len(by) - len(fsru_mmsis & set(by))}  FSRU artifacts={len(fsru_artifact)}"
        )
        print(
            f"  tied-timestamp artifacts={len(tie_artifact)}  GENUINE DFA breaks={len(real)}"
        )
        for m, e in real[:10]:
            print(f"    REAL FAIL mmsi={m}: {e}")
    except Exception as e:  # noqa: BLE001
        print(f"  (validate_sequence not importable as expected: {e})")


if __name__ == "__main__":
    asyncio.run(main())
