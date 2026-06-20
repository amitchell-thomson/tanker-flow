"""EIA ground-truth + fundamentals loader.

One-shot, idempotent batch fetcher (sibling to `ingestion/vesselfinder.py`):
hit the EIA v2 JSON API, upsert tidy rows into `eia_series` keyed by
(series_id, period). Two roles, one module (see docs/design-2026-06-08-data-eia.md):

  Phase 1 — capture-rate ground truth (the long pole, pipeline-health #13):
    monthly US LNG export volume. Lets us compute captured cargoes (our
    `departed` events) ÷ EIA-implied cargoes per month — the signal's capture
    *ratio*, not just its structural completeness.
  Phase 2 — Henry Hub fundamentals (deferred until spread-model work):
    weekly Lower-48 working gas in storage + Henry Hub daily spot. These enter
    the spread model's control set; one registry entry each, no new code.

Module shape mirrors the pure-fn + thin-DB-loader split used across pipeline/:
  - `parse_eia_response` — pure: v2 `response.data[]` → typed `EiaRow`s. No
    network; unit-testable against a captured JSON fixture.
  - `merge_rows` — pure: simulate the upsert (last-write-wins per key) for
    idempotency tests without a DB.
  - `fetch_and_upsert` — thin async loader: paginate the v2 API, parse, upsert.

Incremental by default: fetch only periods at/after a short trailing window
before the latest stored period (to catch EIA revisions); `--full` backfills
history. Empty `eia_api_key` degrades to a clear "disabled" log, not a crash
(same pattern as `vf_api_key`).

NOTE (implementation step 0): the v2 route paths + `series` facets below are
best-known from the legacy v1 series IDs and MUST be confirmed against the live
API before trusting the data. Run `uv run python -m data.eia --probe lng_exports`
(etc.) to hit the live endpoint and print what comes back — fix the one registry
entry if a route 404s. See the design doc.

Usage:
  uv run python -m data.eia                  # incremental upsert, active series
  uv run python -m data.eia --full           # historical backfill
  uv run python -m data.eia --probe KEY      # live fetch one page, print, no DB
  uv run python -m data.eia --series KEY ...  # restrict to specific series keys
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import asyncpg
import httpx
from rich.logging import RichHandler

from config import settings

logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])
logger = logging.getLogger(__name__)

EIA_BASE = "https://api.eia.gov/v2/"
PAGE_LENGTH = 5000  # v2 max rows per page
REQUEST_TIMEOUT = 30.0


@dataclass(frozen=True)
class EiaSeries:
    """One row in the fetch registry. Adding a Phase-2 series is one entry here."""

    key: str  # our short handle (CLI + logs)
    route: str  # v2 route path under EIA_BASE, e.g. 'natural-gas/move/expc/data'
    facet_series: str  # EIA `series` facet value, e.g. 'N9133US2'
    frequency: str  # 'monthly' | 'weekly' | 'daily'
    unit: str  # 'MMcf' | 'Bcf' | '$/MMBtu'
    phase: int  # 1 = active now; 2 = deferred (fundamentals)
    revision_window: int  # periods to re-pull before max(stored) to catch revisions


# Trailing periods to re-fetch even when incremental, per frequency, so EIA's
# revisions to recent periods overwrite our stored values.
SERIES: dict[str, EiaSeries] = {
    # Phase 1 — capture-rate ground truth. Legacy v1: NG.N9133US2.M (MMcf).
    "lng_exports": EiaSeries(
        key="lng_exports",
        route="natural-gas/move/expc/data",
        facet_series="N9133US2",
        frequency="monthly",
        unit="MMcf",
        phase=1,
        revision_window=3,  # months
    ),
    # Phase 2 — fundamentals (deferred; verify routes when spread-model work starts).
    # Legacy v1: NG.NW2_EPG0_SWO_R48_BCF.W (Bcf).
    "storage_l48": EiaSeries(
        key="storage_l48",
        route="natural-gas/stor/wkly/data",
        facet_series="NW2_EPG0_SWO_R48_BCF",
        frequency="weekly",
        unit="Bcf",
        phase=2,
        revision_window=8,  # weeks
    ),
    # Legacy v1: NG.RNGWHHD.D ($/MMBtu).
    "hh_spot": EiaSeries(
        key="hh_spot",
        route="natural-gas/pri/fut/data",
        facet_series="RNGWHHD",
        frequency="daily",
        unit="$/MMBtu",
        phase=2,
        revision_window=30,  # days
    ),
}

ACTIVE_PHASE = 1  # series with phase <= this are fetched by a no-arg `make eia`


@dataclass(frozen=True)
class EiaRow:
    series_id: str
    period: date
    value: float | None
    unit: str
    frequency: str


def _parse_period(raw: str) -> date:
    """EIA `period` → date. Monthly is 'YYYY-MM' (→ first of month); weekly/daily
    'YYYY-MM-DD'; annual 'YYYY' (→ Jan 1). Anything else raises."""
    for fmt, builder in (
        ("%Y-%m-%d", lambda d: d.date()),
        ("%Y-%m", lambda d: d.date().replace(day=1)),
        ("%Y", lambda d: d.date().replace(month=1, day=1)),
    ):
        try:
            return builder(datetime.strptime(raw, fmt))
        except ValueError:
            continue
    raise ValueError(f"unrecognised EIA period format: {raw!r}")


def _coerce_value(raw: object) -> float | None:
    """EIA values arrive as numbers, numeric strings, or null/'' for gaps."""
    if raw is None or raw == "":
        return None
    try:
        return float(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def parse_eia_response(
    payload: dict,
    *,
    series_id: str,
    frequency: str,
    default_unit: str,
) -> list[EiaRow]:
    """Pure: map a v2 `{response: {data: [...]}}` page to typed rows.

    `series_id` is the canonical id we store (the registry's `facet_series`), so
    the stored key is stable even if the EIA route/facet changes. Unit is taken
    from each row's `units` field when present, else `default_unit`.
    """
    data = payload.get("response", {}).get("data", [])
    rows: list[EiaRow] = []
    for item in data:
        period_raw = item.get("period")
        if period_raw is None:
            continue
        rows.append(
            EiaRow(
                series_id=series_id,
                period=_parse_period(str(period_raw)),
                value=_coerce_value(item.get("value")),
                unit=item.get("units") or default_unit,
                frequency=frequency,
            )
        )
    return rows


def merge_rows(
    existing: dict[tuple[str, date], EiaRow],
    new_rows: list[EiaRow],
) -> dict[tuple[str, date], EiaRow]:
    """Pure model of the DB upsert: last write wins per (series_id, period).

    Mirrors `ON CONFLICT (series_id, period) DO UPDATE`, so re-applying the same
    page is a no-op and a revised value overwrites. Lets the idempotency test run
    without a DB.
    """
    merged = dict(existing)
    for row in new_rows:
        merged[(row.series_id, row.period)] = row
    return merged


UPSERT_SQL = """
INSERT INTO eia_series (series_id, period, value, unit, frequency)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (series_id, period) DO UPDATE SET
    value      = EXCLUDED.value,
    unit       = EXCLUDED.unit,
    frequency  = EXCLUDED.frequency,
    fetched_at = now()
"""


def _start_period(latest: date, series: EiaSeries) -> str:
    """Incremental `start=` facet: step `revision_window` periods back from the
    latest stored period and format for the series' frequency."""
    if series.frequency == "monthly":
        month_index = latest.year * 12 + (latest.month - 1) - series.revision_window
        start = date(month_index // 12, month_index % 12 + 1, 1)
        return start.strftime("%Y-%m")
    days = {"weekly": 7, "daily": 1}[series.frequency] * series.revision_window
    return (latest - timedelta(days=days)).strftime("%Y-%m-%d")


async def _latest_period(conn: asyncpg.Connection, series_id: str) -> date | None:
    return await conn.fetchval(
        "SELECT max(period) FROM eia_series WHERE series_id = $1", series_id
    )


async def _fetch_pages(
    client: httpx.AsyncClient,
    series: EiaSeries,
    *,
    start: str | None,
    max_pages: int | None = None,
) -> list[EiaRow]:
    """Paginate the v2 `/data` endpoint (offset/length) until the page is short."""
    base_params: list[tuple[str, str]] = [
        ("api_key", settings.eia_api_key),
        ("frequency", series.frequency),
        ("data[0]", "value"),
        ("facets[series][]", series.facet_series),
        ("sort[0][column]", "period"),
        ("sort[0][direction]", "asc"),
        ("length", str(PAGE_LENGTH)),
    ]
    if start is not None:
        base_params.append(("start", start))

    rows: list[EiaRow] = []
    offset = 0
    page_no = 0
    while True:
        params = base_params + [("offset", str(offset))]
        resp = await client.get(EIA_BASE + series.route, params=params)
        resp.raise_for_status()
        payload = resp.json()
        page = parse_eia_response(
            payload,
            series_id=series.facet_series,
            frequency=series.frequency,
            default_unit=series.unit,
        )
        rows.extend(page)
        page_no += 1
        if len(page) < PAGE_LENGTH:
            break
        if max_pages is not None and page_no >= max_pages:
            break
        offset += PAGE_LENGTH
    return rows


async def fetch_and_upsert(
    pool: asyncpg.Pool,
    series_set: list[EiaSeries],
    *,
    full: bool = False,
) -> None:
    if not settings.eia_api_key:
        logger.warning("EIA loader disabled: EIA_API_KEY is empty (set it in .env)")
        return

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        for series in series_set:
            async with pool.acquire() as conn:
                latest = (
                    None if full else await _latest_period(conn, series.facet_series)
                )
                start = (
                    None if (full or latest is None) else _start_period(latest, series)
                )
                mode = "full backfill" if start is None else f"incremental from {start}"
                logger.info(
                    "[%s] %s — %s (%s, %s)",
                    series.key,
                    series.route,
                    mode,
                    series.facet_series,
                    series.frequency,
                )
                try:
                    rows = await _fetch_pages(client, series, start=start)
                except httpx.HTTPStatusError as e:
                    logger.error(
                        "[%s] HTTP %s from %s — skipping (verify route/facet with --probe)",
                        series.key,
                        e.response.status_code,
                        series.route,
                    )
                    continue
                if not rows:
                    logger.info("[%s] no rows returned", series.key)
                    continue
                await conn.executemany(
                    UPSERT_SQL,
                    [
                        (r.series_id, r.period, r.value, r.unit, r.frequency)
                        for r in rows
                    ],
                )
                logger.info(
                    "[%s] upserted %d rows (%s → %s)",
                    series.key,
                    len(rows),
                    rows[0].period,
                    rows[-1].period,
                )


async def probe(key: str) -> None:
    """Live-fetch one page for a series, print raw JSON + parsed rows. No DB writes.
    This is the step-0 route verification tool."""
    series = SERIES.get(key)
    if series is None:
        logger.error("unknown series key %r (known: %s)", key, ", ".join(SERIES))
        return
    if not settings.eia_api_key:
        logger.error("EIA_API_KEY is empty — set it in .env to probe the live API")
        return

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        params = [
            ("api_key", settings.eia_api_key),
            ("frequency", series.frequency),
            ("data[0]", "value"),
            ("facets[series][]", series.facet_series),
            ("sort[0][column]", "period"),
            ("sort[0][direction]", "desc"),
            ("length", "5"),
        ]
        resp = await client.get(EIA_BASE + series.route, params=params)

    print(f"\n--- GET {EIA_BASE}{series.route} (status {resp.status_code}) ---")
    try:
        payload = resp.json()
        print(json.dumps(payload, indent=2)[:4000])
    except Exception:
        print(resp.text[:2000])
        return

    if not resp.is_success:
        return

    rows = parse_eia_response(
        payload,
        series_id=series.facet_series,
        frequency=series.frequency,
        default_unit=series.unit,
    )
    print(f"\n--- Parsed {len(rows)} rows ---")
    for r in rows:
        print(f"  {r.period}  {r.value}  {r.unit}")


def _select_series(keys: list[str] | None) -> list[EiaSeries]:
    if keys:
        return [SERIES[k] for k in keys]
    return [s for s in SERIES.values() if s.phase <= ACTIVE_PHASE]


async def run(keys: list[str] | None, *, full: bool) -> None:
    series_set = _select_series(keys)
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=3)
    try:
        await fetch_and_upsert(pool, series_set, full=full)
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="EIA ground-truth + fundamentals loader"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Backfill full history (default: incremental from latest stored period)",
    )
    parser.add_argument(
        "--series",
        nargs="+",
        metavar="KEY",
        choices=list(SERIES),
        help=f"Restrict to specific series keys (default: phase<={ACTIVE_PHASE}). "
        f"Choices: {', '.join(SERIES)}",
    )
    parser.add_argument(
        "--probe",
        metavar="KEY",
        choices=list(SERIES),
        help="Live-fetch one page and print it without writing to the DB (route check)",
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
