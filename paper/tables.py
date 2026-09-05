"""Generate every table and every in-prose number for the paper.

Reads `paper/results/*.json` (written by the replays with `--json`) and the live
DB (inventory, capture rate, physical scale, the D-006 leakage recount), then
writes:

  paper/tables/*.tex     booktabs fragments, \\input by main.tex
  paper/numbers.tex      \\newcommand macros — every number that appears in prose

No number in the paper is typed by hand. If a macro is missing, LaTeX fails on
an undefined control sequence, which is the intended failure: a sentence cannot
quietly carry a stale figure.

Usage:
  uv run python -m paper.tables
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import asyncpg

from config import settings

HERE = Path(__file__).resolve().parent
RESULTS = HERE / "results"
TABLES = HERE / "tables"
NUMBERS = HERE / "numbers.tex"

EU_DEMAND_BCM_PER_YEAR = 350.0  # approximate recent EU gas demand — cite in text
M3_GAS_PER_M3_LNG = 600.0  # volumetric expansion, the constant in data/capture_rate.py
NOMINAL_CARGO_M3 = 174_000
MMCF_PER_M3_LNG = 0.021189  # data/capture_rate.py

macros: dict[str, str] = {}


def macro(name: str, value) -> None:
    """Register a \\newcommand. Names are letters only (LaTeX forbids digits)."""
    assert name.isalpha(), name
    macros[name] = str(value)


def fmt_pct(x: float, nd: int = 1) -> str:
    return f"{100 * x:.{nd}f}\\%"


def fmt_signed_pct(x: float, nd: int = 1) -> str:
    """Signed percentage with a real minus: a text-mode "-" is a hyphen."""
    sign = "$-$" if x < 0 else "+"
    return f"{sign}{abs(100 * x):.{nd}f}\\%"


def fmt_num(x: float, nd: int = 3) -> str:
    return f"{x:.{nd}f}"


def fmt_int(x: int | float) -> str:
    return f"{int(round(x)):,}"


def fmt_sci(x: float) -> str:
    """LaTeX scientific notation, e.g. 1.93e-07 -> 1.93 \\times 10^{-7}."""
    if x == 0:
        return "0"
    mant, exp = f"{x:.2e}".split("e")
    return f"${mant} \\times 10^{{{int(exp)}}}$"


def load(name: str) -> dict:
    path = RESULTS / f"{name}.json"
    if not path.exists():
        raise SystemExit(f"missing {path} — run the replay with --json first")
    return json.loads(path.read_text())


def write_table(name: str, body: str) -> None:
    TABLES.mkdir(parents=True, exist_ok=True)
    (TABLES / f"{name}.tex").write_text(body.strip() + "\n")
    print(f"  wrote tables/{name}.tex")


# ----------------------------------------------------------------------
# Live-DB numbers: inventory, capture rate, physical scale, leakage recount
# ----------------------------------------------------------------------

INVENTORY_SQL = {
    "fixes": "SELECT count(*) FROM ais_fixes",
    "fix_lo": "SELECT min(fix_ts)::date FROM ais_fixes",
    "fix_hi": "SELECT max(fix_ts)::date FROM ais_fixes",
    "events": "SELECT count(*) FROM port_events",
    "fleet": "SELECT count(*) FROM vessel_registry WHERE is_lng_carrier OR is_fsru",
    "laden_dep": (
        "SELECT count(*) FROM port_events WHERE event_type='departed' AND laden_flag "
        "AND zone IN ('usgulf','usatlantic')"
    ),
    "signal_rows": "SELECT count(*) FROM signal_daily",
    "signal_keys": "SELECT count(DISTINCT signal_key) FROM signal_daily",
    "panel_rows": "SELECT count(*) FROM model_panel",
    "panel_features": "SELECT count(DISTINCT feature) FROM model_panel",
}

REGIME_SQL = "SELECT regime, count(*) n, max(event_time)::date hi FROM port_events GROUP BY 1 ORDER BY 2 DESC"

CAPTURE_SQL = """
WITH cap AS (
  SELECT date_trunc('year', event_time)::date yr, count(*) n
  FROM port_events
  WHERE event_type='departed' AND laden_flag AND zone IN ('usgulf','usatlantic') AND regime='noaa'
  GROUP BY 1),
eia AS (
  SELECT date_trunc('year', period)::date yr, sum(value) mmcf
  FROM eia_series WHERE series_id='N9133US2' GROUP BY 1)
SELECT extract(year FROM c.yr)::int yr, c.n captured,
       e.mmcf/($1::float*$2::float) implied, c.n/(e.mmcf/($1::float*$2::float)) rate
FROM cap c JOIN eia e USING (yr) WHERE c.yr >= DATE '2016-01-01' ORDER BY 1
"""

SCALE_SQL = """
SELECT avg(value) FILTER (WHERE feature='gas_in_transit_eu') mean_m3,
       stddev(value) FILTER (WHERE feature='gas_in_transit_eu') sd_m3,
       stddev(value) FILTER (WHERE feature='spread_hh_ttf') spread_sd
FROM model_panel WHERE bucket_date >= DATE '2018-01-01'
"""

COVERAGE_SQL = """
SELECT date_trunc('month', bucket_date)::date mo, avg(value) v
FROM model_panel WHERE feature='gas_in_transit_eu'
  AND bucket_date BETWEEN DATE '2025-12-01' AND DATE '2026-01-31'
GROUP BY 1 ORDER BY 1
"""


async def db_numbers(pool: asyncpg.Pool) -> dict:
    async with pool.acquire() as conn:
        inv = {k: await conn.fetchval(q) for k, q in INVENTORY_SQL.items()}
        regimes = await conn.fetch(REGIME_SQL)
        capture = await conn.fetch(CAPTURE_SQL, NOMINAL_CARGO_M3, MMCF_PER_M3_LNG)
        scale = await conn.fetchrow(SCALE_SQL)
        cov = await conn.fetch(COVERAGE_SQL)

    # --- D-006 leakage recount (both loader paths, live) ---
    from pipeline.legs import compute_legs

    as_of = datetime(2020, 6, 1, tzinfo=timezone.utc)
    naive = await compute_legs(pool, now=as_of)
    bounded = await compute_legs(pool, now=as_of, point_in_time=True)
    future_closed = sum(
        1 for lg in naive if lg.arrived_ts is not None and lg.arrived_ts > as_of
    )
    declared = sum(1 for lg in naive if getattr(lg, "dest_region", None))
    return {
        "inv": inv,
        "regimes": regimes,
        "capture": capture,
        "scale": scale,
        "coverage": cov,
        "leak": {
            "naive": len(naive),
            "bounded": len(bounded),
            "future_closed": future_closed,
            "declared": declared,
        },
    }


# ----------------------------------------------------------------------
# Tables
# ----------------------------------------------------------------------


def table_inventory(db: dict) -> None:
    inv, regimes = db["inv"], db["regimes"]
    reg_rows = "\n".join(
        f"  \\quad {r['regime'].replace('_', '\\_')} & {fmt_int(r['n'])} & to {r['hi']} \\\\"
        for r in regimes
    )
    write_table(
        "inventory",
        f"""
\\begin{{tabular}}{{lrl}}
\\toprule
layer & rows & span \\\\
\\midrule
AIS position fixes & {fmt_int(inv['fixes'])} & {inv['fix_lo']} to {inv['fix_hi']} \\\\
port events (state machine) & {fmt_int(inv['events'])} & \\\\
{reg_rows}
in-scope fleet (LNG carriers + FSRUs) & {fmt_int(inv['fleet'])} & \\\\
laden US export departures & {fmt_int(inv['laden_dep'])} & \\\\
signal panel rows (\\texttt{{signal\\_daily}}) & {fmt_int(inv['signal_rows'])} & {inv['signal_keys']} keys, two bases \\\\
model panel rows (\\texttt{{model\\_panel}}) & {fmt_int(inv['panel_rows'])} & {inv['panel_features']} features \\\\
\\bottomrule
\\end{{tabular}}
""",
    )
    macro("nFixes", fmt_int(inv["fixes"]))
    macro("nEvents", fmt_int(inv["events"]))
    macro("nFleet", fmt_int(inv["fleet"]))
    macro("nLadenDepartures", fmt_int(inv["laden_dep"]))
    macro("nSignalRows", fmt_int(inv["signal_rows"]))
    macro("nSignalKeys", inv["signal_keys"])
    macro("nPanelRows", fmt_int(inv["panel_rows"]))
    macro("nPanelFeatures", inv["panel_features"])
    macro("fixSpanLo", inv["fix_lo"])
    macro("fixSpanHi", inv["fix_hi"])
    by = {r["regime"]: r for r in regimes}
    macro("noaaEnds", by["noaa"]["hi"])
    macro("gfwEnds", by["gfw"]["hi"])
    macro("nBackfillEvents", fmt_int(by["noaa"]["n"] + by["gfw"]["n"]))
    macro("nLiveEvents", fmt_int(by["mmsi_filter"]["n"] + by["bbox"]["n"]))


def table_capture(db: dict) -> None:
    rows = db["capture"]
    body = "\n".join(
        f"  {r['yr']} & {fmt_int(r['captured'])} & {r['implied']:.0f} & {fmt_pct(r['rate'], 0)} \\\\"
        for r in rows
    )
    write_table(
        "capture",
        f"""
\\begin{{tabular}}{{lrrr}}
\\toprule
year & captured & EIA-implied & capture \\\\
\\midrule
{body}
\\bottomrule
\\end{{tabular}}
""",
    )
    by = {r["yr"]: r["rate"] for r in rows}
    for y in (2018, 2020, 2023, 2024, 2025):
        macro(f"capture{_word(y)}", fmt_pct(by[y], 0))


def _word(y: int) -> str:
    return {2018: "TwentyEighteen", 2020: "TwentyTwenty", 2023: "TwentyThree",
            2024: "TwentyFour", 2025: "TwentyFive"}[y]


def numbers_scale_and_leak(db: dict) -> None:
    sc, lk, cov = db["scale"], db["leak"], db["coverage"]
    mean_m3, sd_m3, spread_sd = float(sc["mean_m3"]), float(sc["sd_m3"]), float(sc["spread_sd"])
    daily_bcm = EU_DEMAND_BCM_PER_YEAR / 365.0
    macro("stockMeanMm", f"{mean_m3 / 1e6:.2f}")
    macro("stockMeanBcm", f"{mean_m3 * M3_GAS_PER_M3_LNG / 1e9:.2f}")
    macro("stockDaysDemand", f"{mean_m3 * M3_GAS_PER_M3_LNG / 1e9 / daily_bcm:.1f}")
    macro("stockSdDaysDemand", f"{sd_m3 * M3_GAS_PER_M3_LNG / 1e9 / daily_bcm:.2f}")
    macro("spreadWeeklySd", f"{spread_sd:.2f}")
    macro("euDemandBcm", f"{EU_DEMAND_BCM_PER_YEAR:.0f}")

    macro("leakNaive", fmt_int(lk["naive"]))
    macro("leakBounded", fmt_int(lk["bounded"]))
    macro("leakFutureClosed", fmt_int(lk["future_closed"]))
    macro("leakFutureShare", fmt_pct(lk["future_closed"] / lk["naive"], 0))
    macro("leakDeclared", fmt_int(lk["declared"]))

    by = {str(r["mo"])[:7]: float(r["v"]) for r in cov}
    macro("stockDecTwentyFive", f"{by['2025-12'] / 1e6:.2f}")
    macro("stockJanTwentySix", f"{by['2026-01'] / 1e6:.2f}")
    macro("coverageDropShare", fmt_pct(1 - by["2026-01"] / by["2025-12"], 0))


def table_part_a() -> None:
    a1, a2, a4, a5 = load("a1"), load("a2"), load("a4"), load("a5")
    s1, s2, s4 = a1["scorecards"]["W1"], a2["scorecards"]["W1"], a4["scorecards"]["W1"]

    def skill(mae, clim):
        return (mae - clim) / clim

    rows = [
        ("A1 arrival-count climatology", s1["a1_mae"], s1["persist_mae"], s1["clim_mae"], s1["cov80"]),
        ("A2 negative-binomial GLM", s2["mae"], s2["persist_mae"], s2["clim_mae"], s2["cov80"]),
        ("A4 Kalman local level", s4["mae"], s4["persist_mae"], s4["clim_mae"], s4["cov80"]),
    ]
    body = "\n".join(
        f"  {n} & {fmt_num(m)} & {fmt_num(p)} & {fmt_num(c)} & {fmt_signed_pct(skill(m, c))} & {fmt_pct(cv)} \\\\"
        for n, m, p, c, cv in rows
    )
    write_table(
        "part_a",
        f"""
\\begin{{tabular}}{{lrrrrr}}
\\toprule
model & MAE & persistence & climatology & vs climatology & 80\\% cov. \\\\
\\midrule
{body}
\\bottomrule
\\end{{tabular}}
""",
    )
    macro("aOneMae", fmt_num(s1["a1_mae"]))
    macro("aOnePersist", fmt_num(s1["persist_mae"]))
    macro("aOneClim", fmt_num(s1["clim_mae"]))
    macro("aOneCov", fmt_pct(s1["cov80"]))
    macro("aOneSkill", fmt_signed_pct(skill(s1["a1_mae"], s1["clim_mae"])))
    macro("aTwoMae", fmt_num(s2["mae"]))
    macro("aTwoClim", fmt_num(s2["clim_mae"]))
    macro("aTwoSkill", fmt_signed_pct(skill(s2["mae"], s2["clim_mae"])))
    macro("aFourMae", fmt_num(s4["mae"]))
    macro("aFourClim", fmt_num(s4["clim_mae"]))
    macro("aFourPersist", fmt_num(s4["persist_mae"]))
    macro("aFourSkill", fmt_signed_pct(skill(s4["mae"], s4["clim_mae"])))
    macro("aFourAlpha", f"{a4['alpha_mean']:.3f}")
    macro("aFourWindow", f"{a4['effective_window_weeks']:.1f}")
    macro("aFourCov", fmt_pct(s4["cov80"]))
    macro("aTwoBallastCoef", f"{a2['mean_coefficients'].get('ballast_arrivals_1w', float('nan')):+.3f}")

    h = a5["headline"]
    for key, tag in (("bocpd", "Bocpd"), ("n1_absolute", "NOne"), ("n2_rate_relative", "NTwo")):
        b = h[key]
        macro(f"aFive{tag}Recall", fmt_pct(b["recall"], 0))
        macro(f"aFive{tag}Hits", f"{b['n_hit']}/{b['n_outages']}")
        macro(f"aFive{tag}Delay", f"{b['median_delay_d']:.0f}" if b["median_delay_d"] is not None else "--")
        macro(f"aFive{tag}Far", f"{b['false_alarms_per_terminal_yr']:.2f}")
    macro("aFiveOutages", a5["n_outages"])
    macro("aFiveTerminalYears", f"{a5['terminal_years']:.0f}")
    write_table(
        "part_a5",
        f"""
\\begin{{tabular}}{{lrrr}}
\\toprule
detector & recall & median delay (d) & false alarms / terminal-yr \\\\
\\midrule
BOCPD (pre-registered) & {fmt_pct(h['bocpd']['recall'], 0)} ({h['bocpd']['n_hit']}/{h['bocpd']['n_outages']}) & {h['bocpd']['median_delay_d']:.0f} & {h['bocpd']['false_alarms_per_terminal_yr']:.2f} \\\\
N1 absolute silence (14 d) & {fmt_pct(h['n1_absolute']['recall'], 0)} ({h['n1_absolute']['n_hit']}/{h['n1_absolute']['n_outages']}) & {h['n1_absolute']['median_delay_d']:.0f} & {h['n1_absolute']['false_alarms_per_terminal_yr']:.2f} \\\\
N2 rate-relative silence & {fmt_pct(h['n2_rate_relative']['recall'], 0)} ({h['n2_rate_relative']['n_hit']}/{h['n2_rate_relative']['n_outages']}) & {h['n2_rate_relative']['median_delay_d']:.0f} & {h['n2_rate_relative']['false_alarms_per_terminal_yr']:.2f} \\\\
\\bottomrule
\\end{{tabular}}
""",
    )


def table_h1() -> None:
    h1 = load("a1_h1")
    sc = h1["scorecards"]
    rows = []
    for w in ("W1", "W2", "W3", "W4"):
        s = sc[w]
        best = min(s["persist_mae"], s["clim_mae"])
        rows.append(
            f"  {w} & {s['mean_truth']:.2f} & {fmt_num(s['a1_mae'])} & {fmt_num(s['persist_mae'])} & "
            f"{fmt_num(s['clim_mae'])} & {fmt_signed_pct((best - s['a1_mae']) / best)} & {fmt_pct(s['cov80'])} \\\\"
        )
    write_table(
        "h1",
        f"""
\\begin{{tabular}}{{lrrrrrr}}
\\toprule
window & mean truth & A1 MAE & persistence & climatology & ``skill'' & 80\\% cov. \\\\
\\midrule
{chr(10).join(rows)}
\\bottomrule
\\end{{tabular}}
""",
    )
    macro("hOneTruthWOne", f"{sc['W1']['mean_truth']:.2f}")
    macro("hOneTruthWFour", f"{sc['W4']['mean_truth']:.2f}")
    macro("hOnePersistWFour", f"{sc['W4']['persist_mae']:.2f}")
    b4 = min(sc["W4"]["persist_mae"], sc["W4"]["clim_mae"])
    macro("hOneSkillWFour", fmt_signed_pct((b4 - sc["W4"]["a1_mae"]) / b4))
    b1 = min(sc["W1"]["persist_mae"], sc["W1"]["clim_mae"])
    b2 = min(sc["W2"]["persist_mae"], sc["W2"]["clim_mae"])
    macro("hOneSkillWOne", fmt_signed_pct((b1 - sc["W1"]["a1_mae"]) / b1))
    macro("hOneSkillWTwo", fmt_signed_pct((b2 - sc["W2"]["a1_mae"]) / b2))


PRETTY = {
    "gas_in_transit_eu": "gas in transit, EU-bound",
    "gas_in_transit_unknown": "gas in transit, unresolved",
    "gas_loading_us": "US loading rate",
    "gas_discharging_eu": "EU discharge rate",
    "gas_ballast_to_us": "ballast returning to US",
    "laden_voyage_age_d": "laden voyage age",
    "load_queue_h": "US load-queue wait",
    "net_export_pressure": "net export pressure",
}
SIGN = {1: "$+$", -1: "$-$", 0: "none"}


def table_part_b(name: str, suffix: str) -> None:
    b = load(name)
    macro(f"bWeeks{suffix}", b["n_weeks"])
    macro(f"bGridLo{suffix}", b["grid"][0])
    macro(f"bGridHi{suffix}", b["grid"][1])

    # ladder
    rows = []
    for h in ("1", "4"):
        sc = b["horizons"][h]["scores"]
        m0 = sc["M0"]["mae"]
        for m in ("M0", "M1", "M2"):
            s = sc[m]
            skill = "--" if m == "M0" else fmt_signed_pct((m0 - s["mae"]) / m0)
            rows.append(
                f"  {h} & {m} & {s['n']} & {fmt_num(s['mae'])} & {fmt_num(s['rmse'])} & {fmt_pct(s['coverage_80'])} & {skill} \\\\"
            )
            if m == "M2":
                macro(f"bMTwoSkillH{'One' if h == '1' else 'Four'}{suffix}", fmt_signed_pct((m0 - s["mae"]) / m0))
            if m == "M0":
                macro(f"bMZeroMaeH{'One' if h == '1' else 'Four'}{suffix}", fmt_num(s["mae"]))
    write_table(
        f"ladder{suffix.lower()}",
        f"""
\\begin{{tabular}}{{llrrrrr}}
\\toprule
$h$ (wk) & model & $n$ & MAE & RMSE & 80\\% cov. & skill vs M0 \\\\
\\midrule
{chr(10).join(rows)}
\\bottomrule
\\end{{tabular}}
""",
    )

    # fwl
    rows = []
    max_t = 0.0
    max_r2 = 0.0
    for key, pretty in PRETTY.items():
        e1 = b["horizons"]["1"]["fwl"][key]
        e4 = b["horizons"]["4"]["fwl"][key]
        max_t = max(max_t, abs(e1["t_stat"]), abs(e4["t_stat"]))
        max_r2 = max(max_r2, e1["partial_r2"], e4["partial_r2"])
        rows.append(
            f"  {pretty} & {SIGN[e1['expected_sign']]} & {e1['t_stat']:+.2f} & {e1['partial_r2']:.3f} & "
            f"{e4['t_stat']:+.2f} & {e4['partial_r2']:.3f} & {fmt_pct(e4['year_consistency'], 0)} \\\\"
        )
    write_table(
        f"fwl{suffix.lower()}",
        f"""
\\begin{{tabular}}{{llrrrrr}}
\\toprule
 & & \\multicolumn{{2}}{{c}}{{$h=1$}} & \\multicolumn{{2}}{{c}}{{$h=4$}} & \\\\
\\cmidrule(lr){{3-4}} \\cmidrule(lr){{5-6}}
signal & prior & HAC $t$ & partial $R^2$ & HAC $t$ & partial $R^2$ & year cons. \\\\
\\midrule
{chr(10).join(rows)}
\\bottomrule
\\end{{tabular}}
""",
    )
    macro(f"bMaxT{suffix}", f"{max_t:.2f}")
    macro(f"bMaxRsq{suffix}", f"{max_r2:.3f}")
    e = b["horizons"]["4"]["fwl"]["gas_in_transit_eu"]
    macro(f"bTransitTFour{suffix}", f"{e['t_stat']:+.2f}")
    e = b["horizons"]["1"]["fwl"]["gas_in_transit_eu"]
    macro(f"bTransitTOne{suffix}", f"{e['t_stat']:+.2f}")


def table_mechanisms(name: str, suffix: str) -> None:
    m = load(name)
    macro(f"mechDiscLo{suffix}", m["discovery"][0])
    macro(f"mechDiscHi{suffix}", m["discovery"][1])
    macro(f"mechDiscN{suffix}", m["discovery"][2])
    macro(f"mechHoldLo{suffix}", m["holdout"][0])
    macro(f"mechHoldHi{suffix}", m["holdout"][1])
    macro(f"mechHoldN{suffix}", m["holdout"][2])
    macro(f"mechTrendRsq{suffix}", f"{m['h2_trend_r2']:.3f}")
    macro("bonferroniT", f"{m['bonferroni_t']:.3f}")
    macro(f"mechTight{suffix}", m["h4_split"]["tight"])
    macro(f"mechLoose{suffix}", m["h4_split"]["loose"])

    pretty = {
        "eu_share": "H2: EU share of at-sea stock",
        "transit_x_storage": "H3: transit $\\times$ EU storage",
        "transit_in_tight": "H4: transit, tight regime",
        "transit_in_loose": "H4: transit, loose regime",
    }
    rows = []
    max_t = 0.0
    for r in m["results"]:
        hb = "--" if r["holdout_beta"] is None else fmt_sci(r["holdout_beta"])
        if r["name"] != "transit_in_loose":
            max_t = max(max_t, abs(r["t_stat"]))
        rows.append(
            f"  {pretty[r['name']]} & {SIGN[r['expected_sign']]} & {fmt_sci(r['beta'])} & {r['t_stat']:+.2f} & "
            f"{fmt_pct(r['year_consistency'], 0)} & {r['partial_r2']:.3f} & {hb} \\\\"
        )
        tag = {"eu_share": "HTwo", "transit_x_storage": "HThree",
               "transit_in_tight": "HFourTight", "transit_in_loose": "HFourLoose"}[r["name"]]
        macro(f"mech{tag}T{suffix}", f"{r['t_stat']:+.2f}")
        macro(f"mech{tag}Beta{suffix}", fmt_sci(r["beta"]))
        macro(f"mech{tag}Hold{suffix}", hb)
    write_table(
        f"mechanisms{suffix.lower()}",
        f"""
\\begin{{tabular}}{{llrrrrr}}
\\toprule
hypothesis & prior & $\\hat\\beta$ (discovery) & HAC $t$ & year cons. & partial $R^2$ & $\\hat\\beta$ (holdout) \\\\
\\midrule
{chr(10).join(rows)}
\\bottomrule
\\end{{tabular}}
""",
    )
    macro(f"mechMaxT{suffix}", f"{max_t:.2f}")


def table_prereg() -> None:
    """The pre-registration timeline, from the log's own headings + git."""
    entries = [
        ("D-001--D-003", "2026-08-10", "A1 target, kernel, replay protocol", "4239d13"),
        ("D-016", "2026-08-12", "A2 design lock", "4239d13"),
        ("D-019", "2026-08-12", "A5 design lock", "4239d13"),
        ("D-022", "2026-08-12", "A4 design lock", "4239d13"),
        ("D-025", "2026-08-12", "H1 horizon test pre-specified", "4239d13"),
        ("D-028", "2026-09-05", "Part B null + FWL: target, controls, HAC lag, signs", "e483a1b"),
        ("D-031", "2026-09-05", "H2/H3/H4: signs, Bonferroni bar, holdout", "c489550"),
    ]
    body = "\n".join(f"  {d} & {dt} & {what} & \\texttt{{{c}}} \\\\" for d, dt, what, c in entries)
    write_table(
        "prereg",
        f"""
\\begin{{tabular}}{{llp{{7.2cm}}l}}
\\toprule
entry & dated & what was fixed before the fit & commit \\\\
\\midrule
{body}
\\bottomrule
\\end{{tabular}}
""",
    )


def numbers_power() -> None:
    from analysis.mechanisms import BONFERRONI_T

    n = load("mechanisms_coverage")["discovery"][2]
    floor = BONFERRONI_T**2 / (n + BONFERRONI_T**2)
    macro("powerN", n)
    macro("powerFloor", fmt_pct(floor, 2))
    macro("vintagePostDay", 68)  # make validate-signals, 2026-09-05 (D-034)
    macro("vintageSameDay", 909)
    macro("seasonalityRatio", "1.35")  # make validate-signals, 2026-09-05 (D-034)
    macro("hacLagRule", "$h+1$")


def write_numbers() -> None:
    lines = ["% GENERATED by paper/tables.py — do not edit by hand"]
    for k in sorted(macros):
        lines.append(f"\\newcommand{{\\{k}}}{{{macros[k]}}}")
    NUMBERS.write_text("\n".join(lines) + "\n")
    print(f"  wrote numbers.tex ({len(macros)} macros)")


async def main() -> None:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=3)
    try:
        db = await db_numbers(pool)
    finally:
        await pool.close()
    table_inventory(db)
    table_capture(db)
    numbers_scale_and_leak(db)
    table_part_a()
    table_h1()
    table_part_b("b0_coverage", "Cov")
    table_part_b("b0", "Full")
    table_mechanisms("mechanisms_coverage", "Cov")
    table_mechanisms("mechanisms", "Full")
    table_prereg()
    numbers_power()
    write_numbers()


if __name__ == "__main__":
    asyncio.run(main())
