"""Capture-rate validation: captured US LNG-export cargoes ÷ EIA-implied cargoes.

Phase 1b of the EIA work (docs/design-2026-06-08-data-eia.md), the metric the
park is blocked on (park-checkups #13): AIS alone never reveals what it *missed*,
so the only way to answer "what fraction of real US LNG exports do we capture?"
is to divide our captured cargoes by an exogenous ground truth — EIA's monthly
national LNG-export volume (`eia_series`, series N9133US2, MMcf).

  captured(month)  = # laden `departed` events from US export terminals that month
  implied(month)   = EIA_export_volume_MMcf ÷ mean_cargo_size_MMcf
  capture_rate     = captured ÷ implied

This is a READ-ONLY report (no table, no writes) — run it, read the number.

Lands dark by design. EIA publishes with a ~2-month lag and revises recent
months, and our `mmsi_filter` regime only began 2026-05-30, so a month is
*meaningful* only when it is (a) wholly post-cutover, (b) published, and (c)
revised. June 2026 is the first wholly-post-cutover month, so the first
trustworthy reading firms only once EIA has published *and* revised June —
roughly late summer 2026 (exact EIA calendar varies; the `revised` gate keys
off elapsed months, not a hardcoded date). Until then the report says so
rather than printing a biased or empty number.

Unit conversion (the part the design said to nail). EIA exports are a *volume of
natural gas* (MMcf); we measure *LNG* (m³). Both the volumetric path (EIA's own
stated ~600× liquid→gas expansion) and the energy path (~22 MMBtu/m³ ÷ ~1037
Btu/cf) converge on ~3.69 Bcf for a 174k m³ cargo. We use the volumetric 600×
figure; the design's looser "~3.4 Bcf" anchor corresponds to ~553× and is not
used. The ratio is sensitive to this constant at roughly ±5% across the plausible
580–630× range, so it is a single named, unit-tested constant here.

Pure conversion fns + a thin DB loader, mirroring the rest of the codebase.

Usage: `uv run python -m data.capture_rate` (or `make capture-rate`).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime

import asyncpg
from rich.logging import RichHandler

from config import REGIME_CUTOVER, settings

logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])
logger = logging.getLogger(__name__)

# --- Unit conversion (LNG m³ → natural gas MMcf) -------------------------------
# EIA's own figure: liquefying natural gas shrinks it to ~1/600 of its gaseous
# volume. cf per m³ is exact. 174_000 m³ → ~3.69 Bcf, matching the energy-basis
# cross-check (~22 MMBtu/m³ ÷ ~1037 Btu/cf). See module docstring.
LNG_GAS_EXPANSION = 600.0  # m³ gas per m³ LNG
CF_PER_M3 = 35.314666  # cubic feet per m³ (exact-ish)
MMCF_PER_M3_LNG = LNG_GAS_EXPANSION * CF_PER_M3 / 1_000_000  # ≈ 0.021189

# Nominal modern LNG carrier cargo, for the fixed-denominator ratio reported
# alongside the observed-mean one (design open-decision #3: report both).
NOMINAL_CARGO_M3 = 174_000.0

# EIA revises roughly the two most recent published months; older months are firm.
EIA_REVISION_MONTHS = 2
EIA_SERIES_ID = "N9133US2"


def m3_lng_to_mmcf(m3: float) -> float:
    """Convert a volume of LNG (m³) to the equivalent natural gas volume (MMcf)."""
    return m3 * MMCF_PER_M3_LNG


def implied_cargoes(eia_mmcf: float, mean_cargo_m3: float) -> float:
    """EIA monthly export volume (MMcf) → implied cargo count at a given mean
    cargo size (m³ LNG)."""
    return eia_mmcf / m3_lng_to_mmcf(mean_cargo_m3)


def capture_rate(captured: int, eia_mmcf: float, mean_cargo_m3: float) -> float:
    """captured cargoes ÷ EIA-implied cargoes."""
    return captured / implied_cargoes(eia_mmcf, mean_cargo_m3)


def _month_start(d: date) -> date:
    return d.replace(day=1)


def _add_months(d: date, n: int) -> date:
    idx = d.year * 12 + (d.month - 1) + n
    return date(idx // 12, idx % 12 + 1, 1)


# First month lying wholly after the regime cutover: the cutover month itself is
# mixed (pre- and post-), so the first clean month is the one after it.
FIRST_POST_CUTOVER_MONTH = _add_months(_month_start(REGIME_CUTOVER.date()), 1)


@dataclass(frozen=True)
class MonthRow:
    month: date
    captured: int  # laden departeds from US export terminals
    captured_mmsi: int  # ...of which in the mmsi_filter regime
    mean_gas_m3: float | None  # observed mean cargo size that month
    eia_mmcf: float | None  # EIA national export volume (None until published)
    post_cutover: bool  # month wholly after the 2026-05-30 cutover
    revised: bool  # old enough that EIA has firmed it

    @property
    def comparable(self) -> bool:
        return self.eia_mmcf is not None

    @property
    def meaningful(self) -> bool:
        # A trustworthy capture rate: published, firmed, and single-regime.
        return self.comparable and self.revised and self.post_cutover

    def rate(self, cargo_m3: float) -> float | None:
        if self.eia_mmcf is None:
            return None
        return capture_rate(self.captured, self.eia_mmcf, cargo_m3)

    @property
    def rate_nominal(self) -> float | None:
        return self.rate(NOMINAL_CARGO_M3)

    @property
    def rate_observed(self) -> float | None:
        if self.mean_gas_m3 is None:
            return None
        return self.rate(self.mean_gas_m3)


CAPTURED_SQL = """
SELECT date_trunc('month', pe.event_time)::date          AS month,
       count(*)                                          AS captured,
       count(*) FILTER (WHERE pe.regime = 'mmsi_filter') AS captured_mmsi,
       avg(vr.gas_capacity_m3)                           AS mean_gas_m3
FROM port_events pe
JOIN terminals t
  ON t.terminal_id = pe.terminal_id
 AND t.flow_direction = 'export'
 AND t.country = 'US'
LEFT JOIN vessel_registry vr ON vr.mmsi = pe.mmsi
WHERE pe.event_type = 'departed'
GROUP BY 1
ORDER BY 1
"""

EIA_SQL = """
SELECT period, value
FROM eia_series
WHERE series_id = $1 AND frequency = 'monthly'
"""


def build_rows(
    captured_rows: list[asyncpg.Record],
    eia_rows: list[asyncpg.Record],
    now: datetime,
) -> list[MonthRow]:
    """Pure: join captured-departure months to EIA monthly volume + tag each month.

    Kept free of DB/IO so the comparability + ratio logic is unit-testable from
    plain records.
    """
    eia_by_month = {r["period"]: r["value"] for r in eia_rows}
    this_month = _month_start(now.date())
    # EIA firms a month once it is at least EIA_REVISION_MONTHS old.
    revised_before = _add_months(this_month, -EIA_REVISION_MONTHS)

    rows: list[MonthRow] = []
    for r in captured_rows:
        month = r["month"]
        eia = eia_by_month.get(month)
        rows.append(
            MonthRow(
                month=month,
                captured=r["captured"],
                captured_mmsi=r["captured_mmsi"],
                mean_gas_m3=(
                    float(r["mean_gas_m3"]) if r["mean_gas_m3"] is not None else None
                ),
                eia_mmcf=(float(eia) if eia is not None else None),
                post_cutover=month >= FIRST_POST_CUTOVER_MONTH,
                revised=month < revised_before,
            )
        )
    return rows


def _fmt_pct(x: float | None) -> str:
    return "—" if x is None else f"{x * 100:5.1f}%"


def _next_meaningful_note(rows: list[MonthRow], now: datetime) -> str:
    """What the first trustworthy reading is waiting on, in plain terms."""
    # EIA publishes month M around the end of M+2; firms it ~M+3.
    published = _add_months(FIRST_POST_CUTOVER_MONTH, EIA_REVISION_MONTHS + 1)
    have_captured = any(r.post_cutover for r in rows)
    captured_note = (
        "captured departures present"
        if have_captured
        else "no post-cutover captures yet"
    )
    return (
        f"first meaningful month = {FIRST_POST_CUTOVER_MONTH:%Y-%m} "
        f"(first wholly post-cutover); {captured_note}; "
        f"EIA publishes+firms it ~{published:%Y-%m}. Nothing trustworthy before then."
    )


def render(rows: list[MonthRow], now: datetime) -> str:
    lines: list[str] = []
    lines.append(
        "US LNG-export capture rate  (captured departeds ÷ EIA-implied cargoes)"
    )
    lines.append(
        f"  conversion: 1 m³ LNG = {MMCF_PER_M3_LNG:.6f} MMcf "
        f"({LNG_GAS_EXPANSION:.0f}× expansion); nominal cargo {NOMINAL_CARGO_M3:,.0f} m³ "
        f"≈ {m3_lng_to_mmcf(NOMINAL_CARGO_M3) / 1000:.2f} Bcf"
    )
    lines.append("")
    hdr = (
        f"  {'month':<8} {'capt':>4} {'mmsi':>4} {'mean_m³':>8} "
        f"{'EIA_MMcf':>9} {'impl_174k':>9} {'rate174':>8} {'rate_obs':>8}  flags"
    )
    lines.append(hdr)
    lines.append("  " + "-" * (len(hdr) - 2))
    for r in rows:
        impl = (
            f"{implied_cargoes(r.eia_mmcf, NOMINAL_CARGO_M3):9.1f}"
            if r.eia_mmcf is not None
            else f"{'—':>9}"
        )
        flags = []
        if not r.post_cutover:
            flags.append("pre/mixed-regime")
        if r.comparable and not r.revised:
            flags.append("unrevised")
        if not r.comparable:
            flags.append("no-EIA-yet")
        if r.meaningful:
            flags.append("MEANINGFUL")
        lines.append(
            f"  {r.month:%Y-%m} {r.captured:>4} {r.captured_mmsi:>4} "
            f"{(f'{r.mean_gas_m3:8.0f}' if r.mean_gas_m3 else '       —')} "
            f"{(f'{r.eia_mmcf:9.0f}' if r.eia_mmcf is not None else f'{chr(0x2014):>9}')} "
            f"{impl} {_fmt_pct(r.rate_nominal):>8} {_fmt_pct(r.rate_observed):>8}  "
            f"{', '.join(flags)}"
        )
    lines.append("")

    meaningful = [r for r in rows if r.meaningful]
    if meaningful:
        last = meaningful[-1]
        lines.append(
            f"  Capture rate, last meaningful month ({last.month:%Y-%m}): "
            f"{_fmt_pct(last.rate_nominal)} (nominal 174k) / "
            f"{_fmt_pct(last.rate_observed)} (observed mean)"
        )
    else:
        lines.append("  No meaningful month yet — " + _next_meaningful_note(rows, now))
    return "\n".join(lines)


async def compute(pool: asyncpg.Pool, *, now: datetime) -> list[MonthRow]:
    async with pool.acquire() as conn:
        captured = await conn.fetch(CAPTURED_SQL)
        eia = await conn.fetch(EIA_SQL, EIA_SERIES_ID)
    if not eia:
        logger.warning(
            "eia_series has no monthly %s rows — run `make eia` first", EIA_SERIES_ID
        )
    return build_rows(captured, eia, now)


async def run(now: datetime) -> None:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=2)
    try:
        rows = await compute(pool, now=now)
    finally:
        await pool.close()
    print(render(rows, now))


def _parse_as_of(raw: str) -> datetime:
    dt = datetime.fromisoformat(raw)
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="US LNG-export capture-rate report (captured ÷ EIA-implied)."
    )
    parser.add_argument(
        "--as-of",
        type=_parse_as_of,
        default=None,
        metavar="ISO8601",
        help="Pin 'now' (controls which months count as revised/comparable). "
        "Defaults to the current time.",
    )
    args = parser.parse_args()
    asyncio.run(run(args.as_of or datetime.now(UTC)))


if __name__ == "__main__":
    main()
