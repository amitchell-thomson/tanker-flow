"""Assemble the Part B modelling panel: one aligned daily grid.

The spread model needs the target and every feature on one calendar. They do not
arrive on one: prices print on business days, EU storage daily, US storage
weekly, LNG exports monthly, and the tanker signals daily but split across
destination-zone and per-terminal bands. This module joins them.

Three things here are load-bearing, and each is a way the panel can be silently
wrong rather than obviously broken:

**1. Forward-fill, never an inner join.** Henry Hub spot prints on 68.9 % of
panel days and EUR/USD is similar. An inner join across them silently drops a
third of the history and nothing in the output says so. Every non-daily series
is instead carried forward onto the daily grid, with a per-frequency staleness
ceiling (`MAX_STALENESS_DAYS`) so a *discontinued* series goes NULL rather than
flat-lining its last value across years.

**2. Publication lag.** A weekly EIA storage number stamped "week ending Friday"
is not public until the following Thursday. Stamping it at the week-ending date
and letting a model read it that day leaks six days of the future. Every source
therefore declares `publication_lag_days`, and its value is shifted to the first
date it was actually knowable. The tanker signals declare zero because the
`knowable` basis is already point-in-time by construction — that is what the
basis *means* (SIGNALS.md), so lagging them again would double-count.

**3. Band aggregation differs by signal type.** `signal_daily` is banded — gas
volumes by destination zone, berth/queue signals per terminal. A volume is a
stock and must be **summed** across bands; a duration is an average and must be
**weighted by `n_legs`**, because a 40-hour queue measured on one vessel and a
6-hour queue measured on twelve do not average to 23. Summing a duration, or
averaging a volume, both produce a plausible-looking series that is wrong.

Output is tidy `(bucket_date, feature, value)`, matching `signal_daily`'s shape,
so adding a feature never migrates a schema. `load_wide()` pivots it to the
DataFrame a model consumes.

Usage:
  uv run python -m data.model_panel              # rebuild the panel
  uv run python -m data.model_panel --summary    # rebuild, then print coverage
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass
from datetime import date, timedelta

import asyncpg
from rich.logging import RichHandler

from config import settings

logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])
logger = logging.getLogger(__name__)

PANEL_START = date(2016, 1, 1)

# How long a value may be carried forward before the feature goes NULL. Set from
# the series' own cadence plus slack for holidays: a daily price may bridge a
# long weekend, a weekly number its week, a monthly number its month. Past that
# we are no longer filling a gap, we are inventing data.
MAX_STALENESS_DAYS = {"daily": 5, "weekly": 10, "monthly": 45}

# TTF is quoted in EUR/MWh; Henry Hub in USD/MMBtu. 1 MWh = 3.412 MMBtu, so
# dividing by 3.412 and multiplying by USD-per-EUR puts both on one basis.
# Differencing them without this leaks EUR/USD in as fake signal (MODELS.md 2).
MWH_PER_MMBTU = 3.412


@dataclass(frozen=True)
class SeriesFeature:
    """A price/fundamental series drawn from eia_series or market_series."""

    column: str  # output feature name
    table: str  # 'eia_series' | 'market_series'
    series_id: str
    frequency: str  # drives MAX_STALENESS_DAYS
    publication_lag_days: int
    note: str = ""


@dataclass(frozen=True)
class SignalFeature:
    """A tanker signal drawn from signal_daily, aggregated across its bands."""

    column: str
    signal_key: str
    how: str  # 'sum' (stocks/flows) | 'wmean' (durations/rates, n_legs-weighted)
    bands: str  # 'named' (exclude 'unknown') | 'unknown' | 'any'
    note: str = ""


# --- The control set + target legs --------------------------------------------
# Publication lags are the first date the number was *public*, not the date it
# describes. Where a lag is uncertain the conservative (longer) value is used:
# over-lagging costs a little freshness, under-lagging invents skill.
SERIES_FEATURES: tuple[SeriesFeature, ...] = (
    SeriesFeature(
        column="hh_spot",
        table="eia_series",
        series_id="RNGWHHD",
        frequency="daily",
        publication_lag_days=1,  # EIA posts the daily spot the next business day
        note="Henry Hub spot, $/MMBtu — the US leg of the spread",
    ),
    SeriesFeature(
        column="us_storage_bcf",
        table="eia_series",
        series_id="NW2_EPG0_SWO_R48_BCF",
        frequency="weekly",
        publication_lag_days=6,  # week ending Fri -> published Thu 10:30 ET
        note="Lower-48 working gas in storage",
    ),
    SeriesFeature(
        column="ttf_eur_mwh",
        table="market_series",
        series_id="ttf_front_month",
        frequency="daily",
        publication_lag_days=0,  # a settlement price is known at the close
        note="TTF front-month, EUR/MWh — the EU leg of the spread",
    ),
    SeriesFeature(
        column="eurusd",
        table="market_series",
        series_id="eurusd",
        frequency="daily",
        publication_lag_days=0,
        note="USD per EUR",
    ),
    SeriesFeature(
        column="brent",
        table="market_series",
        series_id="brent",
        frequency="daily",
        publication_lag_days=0,
        note="Brent crude, $/bbl — oil-indexed LNG control",
    ),
    SeriesFeature(
        column="eu_storage_pct",
        table="market_series",
        series_id="eu_storage_full",
        frequency="daily",
        publication_lag_days=1,  # AGSI+ publishes gas day D on D+1
        note="EU aggregate storage fill",
    ),
    # Degree days are deliberately NOT lagged. ERA5 reanalysis publishes ~5 days
    # behind, so these are not knowable in real time — but their job here is to
    # be *partialled out* (FWL, MODELS.md 2): the question is "what does the
    # weather that actually happened explain", not "what could I have known".
    # Lagging them would blunt exactly the confounder they exist to remove. If a
    # live predictor is ever wanted, take it from a forecast feed, not from here.
    SeriesFeature(
        column="hdd_us",
        table="market_series",
        series_id="hdd_us",
        frequency="daily",
        publication_lag_days=0,
        note="CONTEMPORANEOUS control, not live-knowable",
    ),
    SeriesFeature(
        column="cdd_us",
        table="market_series",
        series_id="cdd_us",
        frequency="daily",
        publication_lag_days=0,
        note="CONTEMPORANEOUS control, not live-knowable",
    ),
    SeriesFeature(
        column="hdd_nwe",
        table="market_series",
        series_id="hdd_nwe",
        frequency="daily",
        publication_lag_days=0,
        note="CONTEMPORANEOUS control, not live-knowable",
    ),
    SeriesFeature(
        column="cdd_nwe",
        table="market_series",
        series_id="cdd_nwe",
        frequency="daily",
        publication_lag_days=0,
        note="CONTEMPORANEOUS control, not live-knowable",
    ),
)

# --- Tanker signals -----------------------------------------------------------
# Only decade-deep keys. The MODELS.md Part B composites other than
# `net_export_pressure` (spread_thrust, implied_storage_build,
# diversion_arbitrage, declared_eu_share) are live-only — they depend on EU
# anchorage events that GFW does not carry — and hold weeks of history, not
# years. They are excluded here rather than silently contributing a column that
# is NULL for 99 % of the panel. See DECISIONS.md D-027.
SIGNAL_FEATURES: tuple[SignalFeature, ...] = (
    SignalFeature(
        column="gas_in_transit_eu",
        signal_key="gas_in_transit_volume",
        how="sum",
        bands="named",
        note="m3 at sea banded to a known EU destination zone",
    ),
    SignalFeature(
        column="gas_in_transit_unknown",
        signal_key="gas_in_transit_volume",
        how="sum",
        bands="unknown",
        note="m3 at sea with no resolved destination — the majority of the stock",
    ),
    SignalFeature(
        column="gas_loading_us",
        signal_key="gas_loading_us",
        how="sum",
        bands="any",
        note="amortised m3/day loading across US export berths",
    ),
    SignalFeature(
        column="gas_discharging_eu",
        signal_key="gas_discharging_eu",
        how="sum",
        bands="any",
        note="amortised m3/day discharging across EU import berths",
    ),
    SignalFeature(
        column="gas_ballast_to_us",
        signal_key="gas_ballast_to_us",
        how="sum",
        bands="any",
        note="ballast capacity returning to US load terminals",
    ),
    SignalFeature(
        column="laden_voyage_age_d",
        signal_key="laden_voyage_age_d",
        how="wmean",
        bands="named",
        note="age of the laden at-sea stock — floating-storage/urgency proxy",
    ),
    SignalFeature(
        column="load_queue_h",
        signal_key="load_queue_h",
        how="wmean",
        bands="any",
        note="anchorage wait before a US load berth",
    ),
    SignalFeature(
        column="net_export_pressure",
        signal_key="net_export_pressure",
        how="wmean",
        bands="any",
        note="z(US loadings) - z(US load-queue); the one decade-deep composite",
    ),
)

# Both bases exist in signal_daily; MODELS.md consumes only `knowable` (the
# leakage-free point-in-time series). `regime='all'` is the only tag spanning the
# decade — the per-regime tags end when their backfill source does.
SIGNAL_BASIS = "knowable"
SIGNAL_REGIME = "all"


# --- Pure assembly ------------------------------------------------------------


def shift_for_publication(
    observations: dict[date, float | None], lag_days: int
) -> dict[date, float | None]:
    """Move each observation to the first date it was publicly knowable.

    A value describing period P published `lag_days` later must not be visible to
    a model standing at P. Shifting the key forward is what makes a walk-forward
    backtest honest about what was on the screen at the time.
    """
    if lag_days == 0:
        return dict(observations)
    return {
        day + timedelta(days=lag_days): value for day, value in observations.items()
    }


def forward_fill(
    observations: dict[date, float | None],
    grid: list[date],
    max_staleness_days: int,
) -> dict[date, float | None]:
    """Carry observations onto every grid date, up to a staleness ceiling.

    Only non-NULL observations refresh the carry: a vendor gap (a holiday '.')
    should be bridged by the previous real value, not blank the feature. Beyond
    `max_staleness_days` the feature goes NULL, so a series that stops updating
    stops contributing instead of flat-lining forever — the failure mode that
    makes a dead feed look like a constant signal.
    """
    out: dict[date, float | None] = {}
    last_value: float | None = None
    last_day: date | None = None
    for day in grid:
        observed = observations.get(day)
        if observed is not None:
            last_value, last_day = observed, day
        if last_day is not None and (day - last_day).days <= max_staleness_days:
            out[day] = last_value
        else:
            out[day] = None
    return out


def aggregate_bands(
    rows: list[tuple[date, str, float | None, int | None]],
    how: str,
    bands: str,
) -> dict[date, float | None]:
    """Collapse a banded signal to one value per day.

    `rows` are `(bucket_date, zone_scope, value, n_legs)`. Stocks and flows sum
    across bands; durations and rates take an `n_legs`-weighted mean, because
    each band's value is itself an average over a different number of legs.
    """
    selected: dict[date, list[tuple[float, int]]] = {}
    for day, scope, value, n_legs in rows:
        if value is None:
            continue
        if bands == "named" and scope == "unknown":
            continue
        if bands == "unknown" and scope != "unknown":
            continue
        selected.setdefault(day, []).append((value, n_legs or 0))

    out: dict[date, float | None] = {}
    for day, items in selected.items():
        if how == "sum":
            out[day] = sum(v for v, _ in items)
        else:
            weight = sum(w for _, w in items)
            if weight > 0:
                out[day] = sum(v * w for v, w in items) / weight
            else:
                # No leg counts (some composites carry none) — fall back to an
                # unweighted mean rather than dropping the day.
                out[day] = sum(v for v, _ in items) / len(items)
    return out


def spread_usd_per_mmbtu(
    hh: float | None, ttf_eur_mwh: float | None, eurusd: float | None
) -> tuple[float | None, float | None]:
    """(TTF in $/MMBtu, HH - TTF). None if any leg is missing.

    Verified against the World Bank Pink Sheet's independent $/MMBtu European
    gas series: slope 0.9990, R^2 0.9999 over 106 months (scripts/check_ttf_roll.py).
    """
    if hh is None or ttf_eur_mwh is None or eurusd is None:
        return None, None
    ttf_usd = ttf_eur_mwh / MWH_PER_MMBTU * eurusd
    return ttf_usd, hh - ttf_usd


def daily_grid(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


# --- Thin DB layer ------------------------------------------------------------

UPSERT_SQL = """
INSERT INTO model_panel (bucket_date, feature, value)
VALUES ($1, $2, $3)
ON CONFLICT (bucket_date, feature) DO UPDATE SET
    value      = EXCLUDED.value,
    computed_at = now()
"""

SERIES_SQL = "SELECT period, value FROM {table} WHERE series_id = $1 ORDER BY period"

SIGNAL_SQL = """
SELECT bucket_date, zone_scope, value, n_legs
FROM signal_daily
WHERE signal_key = $1 AND basis = $2 AND regime = $3
ORDER BY bucket_date
"""


async def assemble(
    conn: asyncpg.Connection, end: date
) -> dict[str, dict[date, float | None]]:
    """Build every feature column on the daily grid. No writes."""
    grid = daily_grid(PANEL_START, end)
    columns: dict[str, dict[date, float | None]] = {}

    for feat in SERIES_FEATURES:
        rows = await conn.fetch(SERIES_SQL.format(table=feat.table), feat.series_id)
        observed = {r["period"]: r["value"] for r in rows}
        shifted = shift_for_publication(observed, feat.publication_lag_days)
        staleness = MAX_STALENESS_DAYS[feat.frequency]
        columns[feat.column] = forward_fill(shifted, grid, staleness)

    for sig in SIGNAL_FEATURES:
        rows = await conn.fetch(SIGNAL_SQL, sig.signal_key, SIGNAL_BASIS, SIGNAL_REGIME)
        banded = [
            (r["bucket_date"], r["zone_scope"], r["value"], r["n_legs"]) for r in rows
        ]
        collapsed = aggregate_bands(banded, sig.how, sig.bands)
        # Signals are daily but sparse (a day with no legs has no row). Bridge
        # only a few days: a long blank is real absence, not a missing print.
        columns[sig.column] = forward_fill(collapsed, grid, MAX_STALENESS_DAYS["daily"])

    # Derived target legs, computed after the fill so both inputs are aligned.
    ttf_usd: dict[date, float | None] = {}
    spread: dict[date, float | None] = {}
    for day in grid:
        usd, diff = spread_usd_per_mmbtu(
            columns["hh_spot"][day],
            columns["ttf_eur_mwh"][day],
            columns["eurusd"][day],
        )
        ttf_usd[day], spread[day] = usd, diff
    columns["ttf_usd_mmbtu"] = ttf_usd
    columns["spread_hh_ttf"] = spread
    return columns


async def rebuild(pool: asyncpg.Pool, end: date | None = None) -> int:
    """TRUNCATE + rebuild `model_panel`, the same contract as signal_daily."""
    async with pool.acquire() as conn:
        if end is None:
            end = await conn.fetchval("SELECT max(bucket_date) FROM signal_daily")
        columns = await assemble(conn, end)
        payload = [
            (day, feature, value)
            for feature, series in columns.items()
            for day, value in series.items()
        ]
        async with conn.transaction():
            await conn.execute("TRUNCATE model_panel")
            await conn.executemany(UPSERT_SQL, payload)
    return len(payload)


async def load_wide(pool: asyncpg.Pool, *, start: date | None = None):
    """Return the panel as a wide DataFrame indexed by date — what a model reads.

    Columns are features, rows are calendar days. NaN means "not knowable that
    day", which is the honest representation: a model must decide whether to drop
    those rows or impute, and hiding the gap behind a fill here would take that
    decision away from it.
    """
    import pandas as pd

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT bucket_date, feature, value FROM model_panel "
            "WHERE $1::date IS NULL OR bucket_date >= $1 ORDER BY bucket_date",
            start,
        )
    frame = pd.DataFrame(
        [(r["bucket_date"], r["feature"], r["value"]) for r in rows],
        columns=["bucket_date", "feature", "value"],
    )
    return frame.pivot(index="bucket_date", columns="feature", values="value")


async def summarise(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT feature, count(value) non_null, count(*) total,
                      min(bucket_date) FILTER (WHERE value IS NOT NULL) lo,
                      max(bucket_date) FILTER (WHERE value IS NOT NULL) hi
               FROM model_panel GROUP BY 1 ORDER BY 1"""
        )
        complete = await conn.fetchval(
            """SELECT count(*) FROM (
                 SELECT bucket_date FROM model_panel
                 WHERE feature IN ('spread_hh_ttf','hdd_nwe','eu_storage_pct','gas_in_transit_eu')
                 GROUP BY bucket_date HAVING count(value) = 4) t"""
        )
    print(f"\n{'feature':<24}{'non-null':>9}{'cover':>8}  {'from':<12}{'to':<12}")
    for r in rows:
        pct = 100 * r["non_null"] / r["total"] if r["total"] else 0
        print(
            f"{r['feature']:<24}{r['non_null']:>9}{pct:>7.1f}%  "
            f"{str(r['lo'] or '-'):<12}{str(r['hi'] or '-'):<12}"
        )
    print(
        f"\nDays with spread + weather + EU storage + at-sea stock all present: {complete}"
    )


async def run(*, summary: bool) -> None:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=3)
    try:
        n = await rebuild(pool)
        logger.info("model_panel rebuilt — %d rows", n)
        if summary:
            await summarise(pool)
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Assemble the Part B model panel")
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Print per-feature coverage after rebuild",
    )
    args = parser.parse_args()
    asyncio.run(run(summary=args.summary))


if __name__ == "__main__":
    main()
