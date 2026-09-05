"""Non-EIA market + control series loader (Part B spread model).

One-shot, idempotent batch fetcher — the sibling of `data/eia.py`, sharing its
shape (pure parsers + pure `merge_rows` + a thin async upsert) but pooling four
upstream providers into one `market_series` table instead of one API into
`eia_series`. Together the two tables carry everything `analysis/MODELS.md` §2
lists as the spread model's target and control set:

    HH spot, US L48 storage       -> eia_series      (data/eia.py, Phase 2)
    TTF, EUR/USD, Brent,          -> market_series   (this module)
    EU storage, degree days

Why one module and not four (`data/ttf.py`, `data/fx.py`, ...): every series
lands in the same table with the same key, so the upsert, the merge model, the
CLI and the incremental logic are shared; only the *parse* differs per provider.
Each provider therefore contributes one small pure function and one thin fetch,
and a new series is a registry entry rather than a new file.

Providers:
  - `yahoo`     — chart JSON, no key. TTF front-month futures (the EU price leg).
                  Unofficial endpoint: treat a successful pull as a snapshot, not
                  a dependable runtime feed. History starts 2017-10-23.
  - `fred`      — St. Louis Fed, free key (`FRED_API_KEY`). EUR/USD, Brent.
  - `agsi`      — GIE AGSI+, free key (`GIE_AGSI_API_KEY`). EU gas storage.
                  Paginated (300/page, descending); `continent=eu` is the EU
                  aggregate route (`country=eu` returns empty).
  - `openmeteo` — ERA5 reanalysis archive, no key. Daily mean temperature at a
                  handful of demand-centre points, folded into regional
                  heating/cooling degree days.

Empty key ⇒ that provider's series are skipped with a clear log, not a crash
(same degradation as the EIA loader).

Usage:
  uv run python -m data.market                 # incremental upsert, all series
  uv run python -m data.market --full          # historical backfill from 2016
  uv run python -m data.market --probe KEY     # live fetch, print, no DB write
  uv run python -m data.market --series KEY ...
"""

from __future__ import annotations

import argparse
import asyncio
import io
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

import asyncpg
import httpx
from rich.logging import RichHandler

from config import settings

logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])
logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 60.0

# Panel start. The signal panel begins 2016-01-01; nothing earlier is useful as a
# control, and every provider here reaches at least this far except Yahoo (see
# TTF_HISTORY_STARTS below).
PANEL_START = date(2016, 1, 1)

# Degree-day base temperature: 65 degF in Celsius, the standard HDD/CDD pivot.
DEGREE_DAY_BASE_C = 18.3

# Yahoo's TTF=F front-month series begins here (verified 2026-09-05 against the
# chart endpoint's firstTradeDate). Earlier panel days have no daily TTF from any
# free source; the scored window starts 2018-01 anyway (DECISIONS.md D-008), so
# this covers it with a run-up. Recorded as a constant so the gap is explicit in
# code rather than discovered as a surprise NULL in the assembler.
TTF_HISTORY_STARTS = date(2017, 10, 23)

# World Bank Pink Sheet monthly commodity prices. Two things make this series
# worth carrying despite being monthly: it is **CC BY 4.0** — the only price
# series here we may redistribute — and it publishes European gas already in
# $/MMBtu, so it cross-checks BOTH the EUR/MWh -> $/MMBtu conversion and the
# continuous-roll discontinuities in the daily `TTF=F` series (scripts/check_ttf_roll.py).
#
# The document id in this URL ROLLS when the World Bank republishes. A stale id
# keeps returning HTTP 200 with a truncated file rather than 404ing — the classic
# `...-0350012021/...` link still served data ending 2024M12 when checked on
# 2026-09-05. `WORLDBANK_STALE_AFTER_DAYS` turns that silent truncation into a
# loud warning; refresh the id from the "Pink Sheet" links on
# https://www.worldbank.org/en/research/commodity-markets when it fires.
WORLDBANK_PINK_SHEET_URL = (
    "https://thedocs.worldbank.org/en/doc/"
    "74e8be41ceb20fa0da750cda2f6b9e4e-0050012026/related/CMO-Historical-Data-Monthly.xlsx"
)
WORLDBANK_SHEET = "Monthly Prices"
WORLDBANK_STALE_AFTER_DAYS = 120


# --- Degree-day demand centres -----------------------------------------------
# Approximate gas-demand weights, chosen by judgement, NOT official population or
# consumption weights: the aim is a regional temperature index whose *variation*
# tracks heating load, and the spread model reads the standardised series, so the
# exact weights matter far less than their stability. Documented here so the
# choice is auditable and easy to revise.
US_DEGREE_DAY_POINTS: tuple[tuple[str, float, float, float], ...] = (
    ("chicago", 41.88, -87.63, 0.30),  # Midwest — largest US heating gas load
    ("new_york", 40.71, -74.01, 0.30),  # Northeast
    ("atlanta", 33.75, -84.39, 0.20),  # Southeast
    ("houston", 29.76, -95.37, 0.20),  # South — cooling/power burn
)

NWE_DEGREE_DAY_POINTS: tuple[tuple[str, float, float, float], ...] = (
    ("essen", 51.46, 7.01, 0.35),  # Germany — largest EU gas consumer
    ("amsterdam", 52.37, 4.90, 0.25),  # NL — the TTF hub's own market
    ("london", 51.51, -0.13, 0.25),  # UK
    ("paris", 48.86, 2.35, 0.15),  # FR
)


@dataclass(frozen=True)
class MarketSeries:
    """One row in the fetch registry. A new control series is one entry here."""

    key: str  # our short handle + the stored series_id
    source: str  # 'yahoo' | 'fred' | 'agsi' | 'openmeteo'
    frequency: str  # 'daily' | 'weekly' | 'monthly'
    unit: str
    description: str
    spec: dict = field(default_factory=dict)  # provider-specific parameters


SERIES: dict[str, MarketSeries] = {
    # --- The spread target's EU leg -----------------------------------------
    "ttf_front_month": MarketSeries(
        key="ttf_front_month",
        source="yahoo",
        frequency="daily",
        unit="EUR/MWh",
        description="ICE Dutch TTF front-month gas futures (rolled continuous)",
        spec={"symbol": "TTF=F"},
    ),
    # --- FX + oil ------------------------------------------------------------
    "eurusd": MarketSeries(
        key="eurusd",
        source="fred",
        frequency="daily",
        unit="USD/EUR",
        description="US dollars per euro — the MODELS.md 2 spread conversion factor",
        spec={"fred_id": "DEXUSEU"},
    ),
    "brent": MarketSeries(
        key="brent",
        source="fred",
        frequency="daily",
        unit="$/bbl",
        description="Brent crude spot — oil-indexed LNG contract control",
        spec={"fred_id": "DCOILBRENTEU"},
    ),
    # --- EU storage ----------------------------------------------------------
    "eu_storage_full": MarketSeries(
        key="eu_storage_full",
        source="agsi",
        frequency="daily",
        unit="%",
        description="EU aggregate gas storage fill level",
        spec={"field": "full"},
    ),
    "eu_storage_twh": MarketSeries(
        key="eu_storage_twh",
        source="agsi",
        frequency="daily",
        unit="TWh",
        description="EU aggregate gas in storage",
        spec={"field": "gasInStorage"},
    ),
    # --- Degree days ---------------------------------------------------------
    "hdd_us": MarketSeries(
        key="hdd_us",
        source="openmeteo",
        frequency="daily",
        unit="degC-day",
        description="US weighted heating degree days (base 18.3C)",
        spec={"points": US_DEGREE_DAY_POINTS, "kind": "hdd"},
    ),
    "cdd_us": MarketSeries(
        key="cdd_us",
        source="openmeteo",
        frequency="daily",
        unit="degC-day",
        description="US weighted cooling degree days (base 18.3C)",
        spec={"points": US_DEGREE_DAY_POINTS, "kind": "cdd"},
    ),
    "hdd_nwe": MarketSeries(
        key="hdd_nwe",
        source="openmeteo",
        frequency="daily",
        unit="degC-day",
        description="NW Europe weighted heating degree days (base 18.3C)",
        spec={"points": NWE_DEGREE_DAY_POINTS, "kind": "hdd"},
    ),
    "cdd_nwe": MarketSeries(
        key="cdd_nwe",
        source="openmeteo",
        frequency="daily",
        unit="degC-day",
        description="NW Europe weighted cooling degree days (base 18.3C)",
        spec={"points": NWE_DEGREE_DAY_POINTS, "kind": "cdd"},
    ),
    # --- Redistributable monthly cross-check --------------------------------
    "ttf_eu_monthly": MarketSeries(
        key="ttf_eu_monthly",
        source="worldbank",
        frequency="monthly",
        unit="$/MMBtu",
        description="World Bank Pink Sheet 'Natural gas, Europe' (TTF) — CC BY 4.0",
        spec={"url": WORLDBANK_PINK_SHEET_URL, "column": "Natural gas, Europe"},
    ),
}


@dataclass(frozen=True)
class MarketRow:
    series_id: str
    period: date
    value: float | None
    unit: str
    frequency: str
    source: str


# --- Pure parsers -------------------------------------------------------------


def _coerce(raw: object) -> float | None:
    """Vendor gaps arrive as None, '', '.', or '-' depending on the provider."""
    if raw is None or raw in ("", ".", "-", "n/a", "N/A"):
        return None
    try:
        return float(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def parse_yahoo_chart(payload: dict, series: MarketSeries) -> list[MarketRow]:
    """Pure: Yahoo `/v8/finance/chart` JSON -> rows.

    Timestamps are epoch seconds at the session open in the exchange's timezone;
    we take the UTC date, which is the trading day for a European settlement.
    """
    result = (payload.get("chart") or {}).get("result")
    if not result:
        return []
    block = result[0]
    stamps = block.get("timestamp") or []
    quote = (block.get("indicators") or {}).get("quote") or [{}]
    closes = quote[0].get("close") or []
    rows: list[MarketRow] = []
    for ts, close in zip(stamps, closes):
        rows.append(
            MarketRow(
                series_id=series.key,
                period=datetime.fromtimestamp(ts, timezone.utc).date(),
                value=_coerce(close),
                unit=series.unit,
                frequency=series.frequency,
                source=series.source,
            )
        )
    return rows


def parse_fred_observations(payload: dict, series: MarketSeries) -> list[MarketRow]:
    """Pure: FRED `series/observations` JSON -> rows. FRED writes '.' for holidays."""
    rows: list[MarketRow] = []
    for obs in payload.get("observations", []):
        raw_date = obs.get("date")
        if not raw_date:
            continue
        rows.append(
            MarketRow(
                series_id=series.key,
                period=datetime.strptime(raw_date, "%Y-%m-%d").date(),
                value=_coerce(obs.get("value")),
                unit=series.unit,
                frequency=series.frequency,
                source=series.source,
            )
        )
    return rows


def parse_agsi_records(records: list[dict], series: MarketSeries) -> list[MarketRow]:
    """Pure: AGSI+ `data[]` records -> rows, pulling the registry's `field`.

    `gasDayStart` is the gas day the observation belongs to. AGSI+ writes '-' for
    fields a country/aggregate does not report.
    """
    field_name = series.spec["field"]
    rows: list[MarketRow] = []
    for rec in records:
        raw_date = rec.get("gasDayStart")
        if not raw_date:
            continue
        rows.append(
            MarketRow(
                series_id=series.key,
                period=datetime.strptime(raw_date, "%Y-%m-%d").date(),
                value=_coerce(rec.get(field_name)),
                unit=series.unit,
                frequency=series.frequency,
                source=series.source,
            )
        )
    return rows


PERIOD_RE = re.compile(r"^(\d{4})M(\d{1,2})$")


def parse_worldbank_grid(
    grid: list[list[object]], series: MarketSeries
) -> list[MarketRow]:
    """Pure: the Pink Sheet's raw cell grid -> rows for one named commodity.

    The sheet has several preamble rows, then a two-row header (commodity name,
    then unit), then `YYYYMmm` period labels down column 0. The commodity column
    is located **by header text**, not by a fixed index, because the World Bank
    adds and reorders commodities between editions — a hardcoded column silently
    starts reading a different commodity rather than failing.
    """
    wanted = str(series.spec["column"]).strip().casefold()
    col = None
    for row in grid:
        for idx, cell in enumerate(row):
            if isinstance(cell, str) and cell.strip().casefold() == wanted:
                col = idx
                break
        if col is not None:
            break
    if col is None:
        raise ValueError(
            f"column {series.spec['column']!r} not found in the Pink Sheet — "
            "the edition may have renamed it"
        )

    rows: list[MarketRow] = []
    for row in grid:
        if not row:
            continue
        match = PERIOD_RE.match(str(row[0]).strip())
        if not match:
            continue
        year, month = int(match.group(1)), int(match.group(2))
        value = _coerce(row[col]) if col < len(row) else None
        rows.append(
            MarketRow(
                series_id=series.key,
                period=date(year, month, 1),
                value=value,
                unit=series.unit,
                frequency=series.frequency,
                source=series.source,
            )
        )
    return rows


def weighted_temperature(
    per_point: dict[str, dict[date, float | None]],
    points: tuple[tuple[str, float, float, float], ...],
) -> dict[date, float]:
    """Pure: per-point daily mean temperatures -> one weighted regional series.

    A date is emitted only if at least one point reports; weights are
    renormalised over the points that actually reported that day, so a single
    missing grid point rescales rather than biases the index downward.
    """
    weights = {name: w for name, _lat, _lon, w in points}
    combined: dict[date, float] = {}
    all_dates: set[date] = set()
    for series_map in per_point.values():
        all_dates.update(series_map)
    for day in all_dates:
        num = 0.0
        den = 0.0
        for name, series_map in per_point.items():
            temp = series_map.get(day)
            if temp is None:
                continue
            w = weights.get(name, 0.0)
            num += w * temp
            den += w
        if den > 0:
            combined[day] = num / den
    return combined


def degree_days(
    temps: dict[date, float],
    series: MarketSeries,
    *,
    base_c: float = DEGREE_DAY_BASE_C,
) -> list[MarketRow]:
    """Pure: daily mean temperature -> HDD or CDD rows.

    HDD = max(0, base - T), CDD = max(0, T - base); the standard definitions.
    """
    kind = series.spec["kind"]
    rows: list[MarketRow] = []
    for day in sorted(temps):
        temp = temps[day]
        value = max(0.0, base_c - temp) if kind == "hdd" else max(0.0, temp - base_c)
        rows.append(
            MarketRow(
                series_id=series.key,
                period=day,
                value=round(value, 4),
                unit=series.unit,
                frequency=series.frequency,
                source=series.source,
            )
        )
    return rows


def merge_rows(
    existing: dict[tuple[str, date], MarketRow],
    new_rows: list[MarketRow],
) -> dict[tuple[str, date], MarketRow]:
    """Pure model of the DB upsert: last write wins per (series_id, period).

    Mirrors `ON CONFLICT (series_id, period) DO UPDATE` so idempotency is
    testable without a DB — the same contract as `data.eia.merge_rows`.
    """
    merged = dict(existing)
    for row in new_rows:
        merged[(row.series_id, row.period)] = row
    return merged


# --- Thin fetchers ------------------------------------------------------------

UPSERT_SQL = """
INSERT INTO market_series (series_id, period, value, unit, frequency, source)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (series_id, period) DO UPDATE SET
    value      = EXCLUDED.value,
    unit       = EXCLUDED.unit,
    frequency  = EXCLUDED.frequency,
    source     = EXCLUDED.source,
    fetched_at = now()
"""

YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/"
FRED_OBSERVATIONS = "https://api.stlouisfed.org/fred/series/observations"
AGSI_API = "https://agsi.gie.eu/api"
OPENMETEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"

AGSI_PAGE_SIZE = 300  # server-side maximum


async def _fetch_yahoo(
    client: httpx.AsyncClient, series: MarketSeries, start: date
) -> list[MarketRow]:
    period1 = int(
        datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp()
    )
    resp = await client.get(
        YAHOO_CHART + series.spec["symbol"],
        params={"period1": str(period1), "period2": "9999999999", "interval": "1d"},
        headers={"User-Agent": "Mozilla/5.0"},
    )
    resp.raise_for_status()
    return parse_yahoo_chart(resp.json(), series)


async def _fetch_fred(
    client: httpx.AsyncClient, series: MarketSeries, start: date
) -> list[MarketRow]:
    resp = await client.get(
        FRED_OBSERVATIONS,
        params={
            "series_id": series.spec["fred_id"],
            "api_key": settings.fred_api_key,
            "file_type": "json",
            "observation_start": start.isoformat(),
        },
    )
    resp.raise_for_status()
    return parse_fred_observations(resp.json(), series)


async def _fetch_agsi(
    client: httpx.AsyncClient, series: MarketSeries, start: date
) -> list[MarketRow]:
    """Paginate the AGSI+ EU aggregate. Pages are descending; `last_page` bounds
    the loop, and we re-read it each page in case the range shifts under us."""
    headers = {"x-key": settings.gie_agsi_api_key}
    params = {
        "continent": "eu",
        "from": start.isoformat(),
        "to": date.today().isoformat(),
        "size": str(AGSI_PAGE_SIZE),
    }
    records: list[dict] = []
    page = 1
    while True:
        resp = await client.get(
            AGSI_API, params={**params, "page": str(page)}, headers=headers
        )
        resp.raise_for_status()
        payload = resp.json()
        records.extend(payload.get("data", []))
        last_page = int(payload.get("last_page") or 1)
        if page >= last_page:
            break
        page += 1
    return parse_agsi_records(records, series)


async def _fetch_openmeteo(
    client: httpx.AsyncClient, series: MarketSeries, start: date
) -> list[MarketRow]:
    """One archive request per demand centre, then weight and convert.

    ERA5 lags real time by ~5 days; the archive simply ends earlier than today
    rather than erroring, so no end-date arithmetic is needed.
    """
    points = series.spec["points"]
    per_point: dict[str, dict[date, float | None]] = {}
    for name, lat, lon, _weight in points:
        resp = await client.get(
            OPENMETEO_ARCHIVE,
            params={
                "latitude": str(lat),
                "longitude": str(lon),
                "start_date": start.isoformat(),
                "end_date": date.today().isoformat(),
                "daily": "temperature_2m_mean",
                "timezone": "UTC",
            },
        )
        resp.raise_for_status()
        daily = resp.json().get("daily") or {}
        times = daily.get("time") or []
        temps = daily.get("temperature_2m_mean") or []
        per_point[name] = {
            datetime.strptime(t, "%Y-%m-%d").date(): _coerce(v)
            for t, v in zip(times, temps)
        }
    return degree_days(weighted_temperature(per_point, points), series)


async def _fetch_worldbank(
    client: httpx.AsyncClient, series: MarketSeries, start: date
) -> list[MarketRow]:
    """Download the Pink Sheet workbook and parse one commodity column.

    `start` filters after parsing rather than before: the workbook is a single
    small file covering 1960-present, so there is nothing to page or bound.
    """
    import pandas as pd

    resp = await client.get(series.spec["url"], follow_redirects=True)
    resp.raise_for_status()
    frame = pd.read_excel(
        io.BytesIO(resp.content), sheet_name=WORLDBANK_SHEET, header=None
    )
    rows = [
        r
        for r in parse_worldbank_grid(frame.values.tolist(), series)
        if r.period >= start
    ]
    if rows:
        newest = max(r.period for r in rows)
        if (date.today() - newest).days > WORLDBANK_STALE_AFTER_DAYS:
            logger.warning(
                "[%s] Pink Sheet ends %s — the pinned document id is likely stale. "
                "Refresh WORLDBANK_PINK_SHEET_URL from "
                "https://www.worldbank.org/en/research/commodity-markets",
                series.key,
                newest,
            )
    return rows


FETCHERS = {
    "yahoo": _fetch_yahoo,
    "fred": _fetch_fred,
    "agsi": _fetch_agsi,
    "openmeteo": _fetch_openmeteo,
    "worldbank": _fetch_worldbank,
}

# Trailing days re-pulled on an incremental run so late-arriving or revised
# vendor values overwrite ours. ERA5 is the slow one (~5 day publication lag).
# The Pink Sheet revises published months, so re-pull a couple of them.
REVISION_WINDOW_DAYS = {
    "yahoo": 7,
    "fred": 14,
    "agsi": 14,
    "openmeteo": 21,
    "worldbank": 90,
}


def _provider_key_missing(series: MarketSeries) -> str | None:
    """Return the env var name if this series' provider has no key configured."""
    if series.source == "fred" and not settings.fred_api_key:
        return "FRED_API_KEY"
    if series.source == "agsi" and not settings.gie_agsi_api_key:
        return "GIE_AGSI_API_KEY"
    return None


def start_date_for(series: MarketSeries, latest: date | None, *, full: bool) -> date:
    """Where to begin this fetch: the panel start on a backfill, else a revision
    window back from the latest stored period. Yahoo's series cannot begin before
    its own first trading day, so it is clamped."""
    if full or latest is None:
        start = PANEL_START
    else:
        window = REVISION_WINDOW_DAYS.get(series.source, 14)
        start = max(latest - timedelta(days=window), PANEL_START)
    if series.source == "yahoo":
        start = max(start, TTF_HISTORY_STARTS)
    return start


async def _latest_period(conn: asyncpg.Connection, series_id: str) -> date | None:
    return await conn.fetchval(
        "SELECT max(period) FROM market_series WHERE series_id = $1", series_id
    )


async def fetch_and_upsert(
    pool: asyncpg.Pool,
    series_set: list[MarketSeries],
    *,
    full: bool = False,
) -> None:
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        for series in series_set:
            missing = _provider_key_missing(series)
            if missing:
                logger.warning(
                    "[%s] skipped: %s is empty (set it in .env)", series.key, missing
                )
                continue
            async with pool.acquire() as conn:
                latest = None if full else await _latest_period(conn, series.key)
                start = start_date_for(series, latest, full=full)
                logger.info(
                    "[%s] %s from %s (%s)",
                    series.key,
                    "full backfill" if (full or latest is None) else "incremental",
                    start,
                    series.source,
                )
                try:
                    rows = await FETCHERS[series.source](client, series, start)
                except httpx.HTTPStatusError as e:
                    logger.error(
                        "[%s] HTTP %s from %s — skipping",
                        series.key,
                        e.response.status_code,
                        series.source,
                    )
                    continue
                if not rows:
                    logger.info("[%s] no rows returned", series.key)
                    continue
                await conn.executemany(
                    UPSERT_SQL,
                    [
                        (r.series_id, r.period, r.value, r.unit, r.frequency, r.source)
                        for r in rows
                    ],
                )
                non_null = sum(1 for r in rows if r.value is not None)
                logger.info(
                    "[%s] upserted %d rows (%s -> %s), %d non-null",
                    series.key,
                    len(rows),
                    min(r.period for r in rows),
                    max(r.period for r in rows),
                    non_null,
                )


async def probe(key: str) -> None:
    """Live-fetch a short window for one series and print it. No DB writes."""
    series = SERIES.get(key)
    if series is None:
        logger.error("unknown series key %r (known: %s)", key, ", ".join(SERIES))
        return
    missing = _provider_key_missing(series)
    if missing:
        logger.error("%s is empty — set it in .env to probe %s", missing, key)
        return
    # A 30-day window shows nothing for a monthly series, so scale the probe to
    # the series' own cadence.
    lookback = {"monthly": 400, "weekly": 120}.get(series.frequency, 30)
    start = max(date.today() - timedelta(days=lookback), PANEL_START)
    if series.source == "yahoo":
        start = max(start, TTF_HISTORY_STARTS)
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        rows = await FETCHERS[series.source](client, series, start)
    print(f"\n--- {key} ({series.source}, {series.description}) ---")
    for r in rows[-10:]:
        print(f"  {r.period}  {r.value}  {r.unit}")
    print(f"  ({len(rows)} rows since {start})")


def _select_series(keys: list[str] | None) -> list[MarketSeries]:
    if keys:
        return [SERIES[k] for k in keys]
    return list(SERIES.values())


async def run(keys: list[str] | None, *, full: bool) -> None:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=3)
    try:
        await fetch_and_upsert(pool, _select_series(keys), full=full)
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Market + control series loader (Part B spread model)"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help=f"Backfill from {PANEL_START} (default: incremental from latest stored)",
    )
    parser.add_argument(
        "--series",
        nargs="+",
        metavar="KEY",
        choices=list(SERIES),
        help=f"Restrict to specific series. Choices: {', '.join(SERIES)}",
    )
    parser.add_argument(
        "--probe",
        metavar="KEY",
        choices=list(SERIES),
        help="Live-fetch a recent window and print it without writing to the DB",
    )
    args = parser.parse_args()

    try:
        if args.probe:
            asyncio.run(probe(args.probe))
        else:
            asyncio.run(run(args.series, full=args.full))
    except KeyboardInterrupt:
        logger.info("Stopped.")


if __name__ == "__main__":
    main()
