"""Signal validation sweep — the model-readiness gate (analysis/VALIDATION.md).

Runs the tiered checks over signal_daily (+ cross-refs to port_events, eia_series,
signal_daily_live_vintage) and prints a per-signal pass/fail report. Exit code is
non-zero if any *blocking* check (tiers 0-5) fails — so it can gate CI / `make signals`.

    uv run python -m analysis.validate_signals
"""

from __future__ import annotations

import asyncio
import sys
from collections import defaultdict
from dataclasses import dataclass

import asyncpg

from config import settings

# ---------------------------------------------------------------------------
# Signal inventory + expectations (mirrors analysis/SIGNALS.md)
# ---------------------------------------------------------------------------

ALL_KEYS = {
    "gas_loading_us", "gas_discharging_eu", "gas_in_transit_volume", "gas_ballast_to_us",
    "load_berth_turn_h", "discharge_berth_turn_h", "voyage_speed_kn", "slow_steam_frac",
    "voyage_time_anomaly_d", "laden_voyage_age_d", "us_loadings_count",
    "us_loadings_count_warm", "round_trip_d", "fleet_laden_frac", "active_vessels",
    "load_queue_h", "discharge_queue_h", "us_queue_depth", "eu_queue_depth",
    "queued_rate", "meaningful_queue_rate", "days_since_departed", "days_since_moored",
    "us_queue_formation_wow", "eu_queue_formation_wow", "od_flow_count",
    "cold_start_rate", "newbuild_appearances", "declared_eu_share",
    "net_export_pressure", "net_absorption_pressure", "spread_thrust",
    "implied_storage_build", "diversion_arbitrage",
}

# value_dispersion is populated ONLY for these (MAD of per-item measurements).
DISTRIBUTIONAL = {
    "load_berth_turn_h", "discharge_berth_turn_h", "voyage_speed_kn",
    "voyage_time_anomaly_d", "round_trip_d", "load_queue_h", "discharge_queue_h",
}
# open_fraction populated for these stocks/flows.
STOCKS = {
    "gas_loading_us", "gas_discharging_eu", "gas_in_transit_volume", "gas_ballast_to_us",
    "laden_voyage_age_d", "us_queue_depth", "eu_queue_depth",
}
# estimated_fraction populated ONLY for the queue-time signals.
QUEUE_TIME = {"load_queue_h", "discharge_queue_h"}
# knowable == physical by construction (closed-item measurements).
CLOSED_EVENT = {
    "load_berth_turn_h", "discharge_berth_turn_h", "voyage_speed_kn", "slow_steam_frac",
    "voyage_time_anomaly_d", "round_trip_d", "us_loadings_count",
    "us_loadings_count_warm", "od_flow_count",
}
# Live-only by data availability (EU anchorage / declared dest have no backfill).
EU_LIVE_ONLY = {
    "discharge_queue_h", "eu_queue_depth", "eu_queue_formation_wow", "declared_eu_share",
    "net_absorption_pressure", "spread_thrust", "implied_storage_build",
    "diversion_arbitrage",
}
# Expected to carry a decade of NOAA history.
US_DEEP = {
    "gas_loading_us", "gas_in_transit_volume", "us_loadings_count",
    "us_loadings_count_warm", "load_berth_turn_h", "load_queue_h", "voyage_speed_kn",
    "slow_steam_frac", "voyage_time_anomaly_d", "laden_voyage_age_d", "round_trip_d",
    "od_flow_count", "net_export_pressure",
}
# Fractions must lie in [0, 1].
FRACTION = {
    "slow_steam_frac", "queued_rate", "meaningful_queue_rate", "cold_start_rate",
    "declared_eu_share", "fleet_laden_frac",
}
# (signal_key, lo, hi) physical range bounds for Tier 2.
RANGE_BOUNDS = [
    # Lower bound 0.5 (not 3): a positive implied speed below 3 kn is a genuine
    # slow-steaming / floating voyage, not an error. 25 kn is the hard physical guard
    # (no real laden carrier exceeds it; the centroid distance only under-states it).
    ("voyage_speed_kn", 0.5, 25.0),
    ("load_queue_h", 0.0, 14 * 24.0),
    ("discharge_queue_h", 0.0, 14 * 24.0),
    ("load_berth_turn_h", 0.0, 30 * 24.0),
    ("discharge_berth_turn_h", 0.0, 30 * 24.0),
    ("laden_voyage_age_d", 0.0, 400.0),
    ("days_since_departed", 0.0, 100000.0),
    ("days_since_moored", 0.0, 100000.0),
]


@dataclass
class Finding:
    tier: int
    key: str
    status: str  # PASS | WARN | FAIL | SKIP
    detail: str


class Sweep:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn
        self.findings: list[Finding] = []

    def add(self, tier, key, status, detail=""):
        self.findings.append(Finding(tier, key, status, detail))

    # ---- Tier 0: structural integrity --------------------------------------
    async def tier0(self):
        rows = await self.conn.fetch("SELECT DISTINCT signal_key FROM signal_daily")
        present = {r["signal_key"] for r in rows}
        missing = ALL_KEYS - present
        unexpected = present - ALL_KEYS
        if missing:
            self.add(0, "-", "FAIL", f"missing keys: {sorted(missing)}")
        if unexpected:
            self.add(0, "-", "WARN", f"unexpected keys: {sorted(unexpected)}")
        if not missing:
            self.add(0, "-", "PASS", f"all {len(ALL_KEYS)} keys present")

        # both bases per key
        bad = await self.conn.fetch(
            "SELECT signal_key, count(DISTINCT basis) b FROM signal_daily "
            "GROUP BY signal_key HAVING count(DISTINCT basis) <> 2"
        )
        for r in bad:
            self.add(0, r["signal_key"], "FAIL", f"only {r['b']} basis (need 2)")
        if not bad:
            self.add(0, "-", "PASS", "every key has both bases")

        # finite values
        nan = await self.conn.fetchval(
            "SELECT count(*) FROM signal_daily WHERE value <> value "
            "OR value = 'Infinity'::float8 OR value = '-Infinity'::float8"
        )
        self.add(0, "-", "FAIL" if nan else "PASS",
                 f"{nan} non-finite values" if nan else "all values finite")

        neg = await self.conn.fetchval(
            "SELECT count(*) FROM signal_daily WHERE n_legs < 0"
        )
        self.add(0, "-", "FAIL" if neg else "PASS",
                 f"{neg} negative n_legs" if neg else "n_legs >= 0")

        dup = await self.conn.fetchval(
            "SELECT count(*) FROM (SELECT 1 FROM signal_daily GROUP BY "
            "signal_key, bucket_date, zone_scope, regime, basis HAVING count(*) > 1) q"
        )
        self.add(0, "-", "FAIL" if dup else "PASS",
                 f"{dup} duplicate cells" if dup else "uniqueness holds")

    # ---- Tier 1: coverage & continuity -------------------------------------
    async def tier1(self):
        spans = await self.conn.fetch(
            "SELECT signal_key, min(bucket_date) lo, max(bucket_date) hi "
            "FROM signal_daily WHERE basis='physical' GROUP BY signal_key"
        )
        span = {r["signal_key"]: (r["lo"], r["hi"]) for r in spans}
        # EU-live-only: assert no pre-2026 rows (cross-ocean queue regression test)
        for k in sorted(EU_LIVE_ONLY):
            lo = span.get(k, (None, None))[0]
            if lo is None:
                self.add(1, k, "WARN", "no rows")
            elif lo.year < 2026:
                self.add(1, k, "FAIL", f"live-only signal has pre-2026 rows (from {lo})")
            else:
                self.add(1, k, "PASS", f"live-only, from {lo}")
        # US-deep: assert decade depth
        for k in sorted(US_DEEP):
            lo = span.get(k, (None, None))[0]
            if lo is None or lo.year > 2017:
                self.add(1, k, "WARN", f"expected decade depth, starts {lo}")
            else:
                self.add(1, k, "PASS", f"decade-deep from {lo}")
        # holes in the mature continuous stocks
        for k in ("gas_in_transit_volume", "gas_loading_us", "gas_discharging_eu"):
            gap = await self.conn.fetchval(
                """
                SELECT max(d - prev) FROM (
                  SELECT bucket_date d, lag(bucket_date) OVER (ORDER BY bucket_date) prev
                  FROM (SELECT DISTINCT bucket_date FROM signal_daily
                        WHERE signal_key=$1 AND basis='physical' AND regime='all'
                          AND bucket_date BETWEEN '2022-01-01' AND '2025-12-31') s
                ) q
                """,
                k,
            )
            gd = int(gap) if gap else 0  # date - date → integer days in Postgres
            self.add(1, k, "PASS" if gd <= 1 else "WARN",
                     f"longest 2022-25 gap {gd}d")

    # ---- Tier 2: range / plausibility --------------------------------------
    async def tier2(self):
        for key, lo, hi in RANGE_BOUNDS:
            r = await self.conn.fetchrow(
                "SELECT count(*) n, min(value) mn, max(value) mx FROM signal_daily "
                "WHERE signal_key=$1 AND (value < $2 OR value > $3)", key, lo, hi,
            )
            self.add(2, key, "FAIL" if r["n"] else "PASS",
                     f"{r['n']} out of [{lo},{hi}]" if r["n"]
                     else f"in [{lo},{hi}]")
        for key in sorted(FRACTION):
            n = await self.conn.fetchval(
                "SELECT count(*) FROM signal_daily WHERE signal_key=$1 "
                "AND (value < 0 OR value > 1)", key)
            self.add(2, key, "FAIL" if n else "PASS",
                     f"{n} outside [0,1]" if n else "fraction in [0,1]")
        # confidence cols in [0,1]
        for col in ("open_fraction", "estimated_fraction"):
            n = await self.conn.fetchval(
                f"SELECT count(*) FROM signal_daily WHERE {col} IS NOT NULL "
                f"AND ({col} < 0 OR {col} > 1)")
            self.add(2, col, "FAIL" if n else "PASS",
                     f"{n} {col} outside [0,1]" if n else f"{col} in [0,1]")
        gneg = await self.conn.fetchval(
            "SELECT count(*) FROM signal_daily WHERE signal_key LIKE 'gas_%' "
            "AND value < 0")
        self.add(2, "gas_*", "FAIL" if gneg else "PASS",
                 f"{gneg} negative gas volumes" if gneg else "gas volumes >= 0")
        dneg = await self.conn.fetchval(
            "SELECT count(*) FROM signal_daily WHERE value_dispersion < 0")
        self.add(2, "value_dispersion", "FAIL" if dneg else "PASS",
                 f"{dneg} negative MAD" if dneg else "MAD >= 0")

    # ---- Tier 3: cross-source / ground truth -------------------------------
    async def tier3(self):
        # NOAA/GFW residual on gas_loading_us (recent years should be ~1.15-1.25x)
        for yr in (2024, 2025):
            r = await self.conn.fetchrow(
                """
                SELECT sum(value) FILTER (WHERE regime='noaa') noaa,
                       sum(value) FILTER (WHERE regime='all')  alll
                FROM signal_daily WHERE signal_key='gas_loading_us' AND basis='physical'
                  AND extract(year FROM bucket_date)=$1
                """, yr)
            if r["noaa"] and r["noaa"] > 0:
                ratio = r["alll"] / r["noaa"]
                self.add(3, "gas_loading_us", "PASS" if ratio < 1.4 else "WARN",
                         f"{yr} all/noaa = {ratio:.2f}x (want <1.4)")
        # capture vs EIA (time-gated)
        has_eia = (
            await self.conn.fetchval("SELECT count(*) FROM eia_series")
            if await self._table_exists("eia_series") else 0
        )
        self.add(3, "capture_rate", "SKIP",
                 f"EIA rows={has_eia}; run make capture-rate (firms ~mid-2026)"
                 if has_eia else "EIA series not loaded yet (time-gated)")
        # live-vs-historical level divergence
        rows = await self.conn.fetch(
            """
            SELECT signal_key,
                   avg(value) FILTER (WHERE regime='mmsi_filter') live,
                   avg(value) FILTER (WHERE regime IN ('noaa','gfw')) hist
            FROM signal_daily WHERE basis='physical'
              AND signal_key IN ('gas_loading_us','gas_discharging_eu',
                                 'gas_in_transit_volume','load_berth_turn_h',
                                 'discharge_berth_turn_h','voyage_speed_kn')
            GROUP BY signal_key
            """)
        for r in rows:
            if not r["live"] or not r["hist"]:
                continue
            ratio = r["live"] / r["hist"]
            ok = 0.5 <= ratio <= 2.0 or r["signal_key"] == "gas_in_transit_volume"
            self.add(3, r["signal_key"], "PASS" if ok else "WARN",
                     f"live/hist level = {ratio:.2f}x")

    # ---- Tier 4: leakage / basis integrity ---------------------------------
    async def tier4(self):
        for k in sorted(CLOSED_EVENT):
            n = await self.conn.fetchval(
                """
                SELECT count(*) FROM signal_daily p JOIN signal_daily kn
                  USING (signal_key, bucket_date, zone_scope, regime)
                WHERE p.signal_key=$1 AND p.basis='physical' AND kn.basis='knowable'
                  AND abs(p.value - kn.value) > 1e-6
                """, k)
            self.add(4, k, "FAIL" if n else "PASS",
                     f"{n} cells where knowable<>physical" if n
                     else "knowable==physical")
        # vintage self-validation (time-gated)
        if not await self._table_exists("signal_daily_live_vintage"):
            self.add(4, "vintage", "SKIP", "no vintage table")
            return
        # Vintage self-validation: the recomputed `knowable` series must reproduce
        # what the pipeline PRINTED live on day d. Three things make this honest:
        #   1. basis='knowable' only — `physical` embeds hindsight and drifts every
        #      rebuild by design; comparing it is not a leakage test.
        #   2. last snapshot per (key,date,scope,regime) — the log is append-only with
        #      multiple intraday snapshots; only the final daily print is canonical.
        #   3. fully-settled days only — `knowable` legitimately keeps changing for the
        #      most recent ~SETTLE_DAYS while open legs/visits/queues close (their
        #      estimated/open fractions resolve). A day is only testable once every open
        #      item it carried has had time to terminate. Until a logged vintage day ages
        #      past that window the property is *not yet verifiable* → SKIP, not FAIL.
        SETTLE_DAYS = 18  # ~ the longest O-D voyage + open-visit/queue window
        testable = await self.conn.fetchval(
            """
            SELECT count(DISTINCT bucket_date) FROM signal_daily_live_vintage
            WHERE basis='knowable' AND bucket_date < current_date - $1::int
            """, SETTLE_DAYS)
        if not testable:
            self.add(4, "vintage", "SKIP",
                     f"no vintage day older than the {SETTLE_DAYS}d settling window yet "
                     "— knowable stability not verifiable until the live tail accrues")
        else:
            mism = await self.conn.fetchval(
                """
                WITH last_snap AS (
                  SELECT DISTINCT ON (signal_key, bucket_date, zone_scope, regime)
                         signal_key, bucket_date, zone_scope, regime, value
                  FROM signal_daily_live_vintage
                  WHERE basis='knowable' AND bucket_date < current_date - $1::int
                  ORDER BY signal_key, bucket_date, zone_scope, regime, printed_at DESC)
                SELECT count(*) FROM last_snap v
                JOIN signal_daily s USING (signal_key, bucket_date, zone_scope, regime)
                WHERE s.basis='knowable' AND abs(v.value - s.value) > 1e-6
                """, SETTLE_DAYS)
            self.add(4, "vintage", "FAIL" if mism else "PASS",
                     f"{mism} knowable<>printed (over {testable} settled day(s))" if mism
                     else f"knowable==as-printed ({testable} settled day(s))")

    # ---- Tier 5: confidence-column correctness -----------------------------
    async def tier5(self):
        async def leak(col, allowed):
            rows = await self.conn.fetch(
                f"SELECT DISTINCT signal_key FROM signal_daily WHERE {col} IS NOT NULL")
            present = {r["signal_key"] for r in rows}
            stray = present - allowed
            missing = allowed - present
            if stray:
                self.add(5, col, "FAIL", f"populated on unexpected keys: {sorted(stray)}")
            elif missing:
                self.add(5, col, "WARN", f"absent on expected keys: {sorted(missing)}")
            else:
                self.add(5, col, "PASS", f"populated exactly on {len(allowed)} keys")

        # value_dispersion: never on non-distributional; on distributional keys it
        # needs a multi-item cell — a too-sparse signal (all n=1, e.g. thin live-only
        # discharge_queue_h) legitimately has no MAD, which is expected not a gap.
        disp = {r["signal_key"] for r in await self.conn.fetch(
            "SELECT DISTINCT signal_key FROM signal_daily WHERE value_dispersion IS NOT NULL")}
        stray = disp - DISTRIBUTIONAL
        if stray:
            self.add(5, "value_dispersion", "FAIL", f"populated on non-distributional: {sorted(stray)}")
        for k in sorted(DISTRIBUTIONAL - disp):
            multi = await self.conn.fetchval(
                "SELECT count(*) FROM signal_daily WHERE signal_key=$1 AND n_legs>=2", k)
            self.add(5, "value_dispersion",
                     "WARN" if multi else "PASS",
                     f"{k}: missing despite {multi} multi-item cells" if multi
                     else f"{k}: no MAD (too sparse, expected)")
        if not stray and DISTRIBUTIONAL <= disp:
            self.add(5, "value_dispersion", "PASS",
                     f"populated on all {len(DISTRIBUTIONAL)} distributional keys")
        await leak("estimated_fraction", QUEUE_TIME)
        # open_fraction: present on stocks (subset check — counts/composites may be NULL)
        of = {r["signal_key"] for r in await self.conn.fetch(
            "SELECT DISTINCT signal_key FROM signal_daily WHERE open_fraction IS NOT NULL")}
        miss = STOCKS - of
        self.add(5, "open_fraction", "WARN" if miss else "PASS",
                 f"absent on stocks {sorted(miss)}" if miss
                 else f"present on all {len(STOCKS)} stocks")
        # in-transit fingerprint
        r = await self.conn.fetchrow(
            """
            SELECT avg(open_fraction) FILTER (WHERE regime IN ('noaa','gfw')) hist,
                   avg(open_fraction) FILTER (WHERE regime='mmsi_filter') live
            FROM signal_daily WHERE signal_key='gas_in_transit_volume' AND basis='physical'
            """)
        ok = (r["hist"] or 0) < 0.05 and (r["live"] or 0) > 0.3
        self.add(5, "gas_in_transit_volume", "PASS" if ok else "WARN",
                 f"open_fraction hist={r['hist']:.2f} live={r['live']:.2f} "
                 "(expect ~0 / high)")

    # ---- Tier 6: economic-sign / event validation (confirmatory) -----------
    async def tier6(self):
        fid = await self.conn.fetchval(
            "SELECT terminal_id FROM terminals WHERE terminal_name ILIKE '%freeport%'")
        if fid:
            gap = await self.conn.fetchval(
                "SELECT max(value) FROM signal_daily WHERE signal_key='days_since_departed' "
                "AND zone_scope=$1 AND bucket_date BETWEEN '2022-06-01' AND '2022-12-31'",
                str(fid))
            self.add(6, "freeport_outage", "PASS" if (gap or 0) >= 30 else "WARN",
                     f"max days_since_departed at Freeport H2-2022 = {gap}")
        else:
            self.add(6, "freeport_outage", "SKIP", "Freeport terminal not found")
        # seasonality: in-transit higher in winter (Dec-Feb) than summer (Jun-Aug)
        r = await self.conn.fetchrow(
            """
            SELECT avg(value) FILTER (WHERE extract(month FROM bucket_date) IN (12,1,2)) win,
                   avg(value) FILTER (WHERE extract(month FROM bucket_date) IN (6,7,8)) sum
            FROM signal_daily WHERE signal_key='gas_in_transit_volume'
              AND basis='physical' AND regime='all'
            """)
        if r["win"] and r["sum"]:
            self.add(6, "seasonality", "PASS" if r["win"] >= r["sum"] else "WARN",
                     f"in-transit winter/summer = {r['win']/r['sum']:.2f}x")

    # ---- Tier 7: regime-seam (confirmatory) --------------------------------
    async def tier7(self):
        regs = {r["regime"] for r in await self.conn.fetch(
            "SELECT DISTINCT regime FROM signal_daily WHERE signal_key='gas_discharging_eu'")}
        ok = {"gfw", "mmsi_filter"} <= regs
        self.add(7, "eu_fidelity_seam", "PASS" if ok else "WARN",
                 f"EU discharge regimes: {sorted(regs)} (expect gfw + mmsi_filter)")

    async def _table_exists(self, name: str) -> bool:
        return await self.conn.fetchval("SELECT to_regclass($1) IS NOT NULL", name)

    async def run(self):
        for t in (self.tier0, self.tier1, self.tier2, self.tier3, self.tier4,
                  self.tier5, self.tier6, self.tier7):
            await t()


TIER_NAMES = {
    0: "Structural", 1: "Coverage", 2: "Range", 3: "Cross-source",
    4: "Leakage/basis", 5: "Confidence", 6: "Economic-sign", 7: "Regime-seam",
}
BLOCKING = {0, 1, 2, 3, 4, 5}
ICON = {"PASS": "✔", "WARN": "⚠", "FAIL": "✗", "SKIP": "·"}


def report(findings: list[Finding]) -> int:
    by_tier: dict[int, list[Finding]] = defaultdict(list)
    for f in findings:
        by_tier[f.tier].append(f)
    counts: dict[str, int] = defaultdict(int)
    print("\n" + "=" * 72)
    print("SIGNAL VALIDATION SWEEP")
    print("=" * 72)
    for tier in sorted(by_tier):
        block = "BLOCKING" if tier in BLOCKING else "confirmatory"
        print(f"\n── Tier {tier} · {TIER_NAMES[tier]} ({block}) " + "─" * 20)
        for f in by_tier[tier]:
            counts[f.status] += 1
            print(f"  {ICON[f.status]} {f.status:4} {f.key:24} {f.detail}")
    print("\n" + "=" * 72)
    blocking_fail = sum(
        1 for f in findings if f.status == "FAIL" and f.tier in BLOCKING)
    print(f"SUMMARY  PASS={counts['PASS']}  WARN={counts['WARN']}  "
          f"FAIL={counts['FAIL']}  SKIP={counts['SKIP']}")
    if blocking_fail:
        print(f"GATE: ✗ NOT model-ready — {blocking_fail} blocking failure(s).")
    else:
        print("GATE: ✔ model-ready (no blocking failures). "
              "Review WARNs + time-gated SKIPs.")
    print("=" * 72)
    return 1 if blocking_fail else 0


async def main():
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            sweep = Sweep(conn)
            await sweep.run()
    finally:
        await pool.close()
    sys.exit(report(sweep.findings))


if __name__ == "__main__":
    asyncio.run(main())
