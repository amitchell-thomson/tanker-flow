# Design: `data/eia.py` — EIA ground-truth + fundamentals loader

**2026-06-08.** Implementation plan for the EIA ingester named in the CLAUDE.md
data-flow diagram (`EIA API → data/eia.py → US natural gas storage`) and flagged
as the **long pole** in park-checkup item 13: *until `data/eia.py` exists there is
no capture-rate validation* — we cannot tell what fraction of real US LNG exports
our AIS pipeline actually captures, because AIS alone never reveals what it missed.

**Status: PLAN (not yet built).** Unlike the spread model, this is **not** gated on
corpus maturity — it can be built now, and the data it pulls (monthly exports) has
its own ~2–3 month lead time before variance is usable, so starting early is
value-additive rather than premature.

---

## TL;DR

`data/eia.py` is a one-shot, idempotent batch fetcher (sibling to
`ingestion/vesselfinder.py`): hit the EIA v2 JSON API, upsert tidy rows into a new
`eia_series` table keyed by `(series_id, period)`. It serves **two roles**, built
in two phases:

1. **Phase 1 — capture-rate ground truth (the long pole).** Monthly US LNG export
   volume. Lets us compute *captured cargoes (our `departed` events) ÷ EIA-implied
   cargoes* per month and finally validate the signal's capture **ratio**, not just
   its structural completeness.
2. **Phase 2 — Henry Hub fundamentals (model controls).** Weekly Lower-48 working
   gas in storage + Henry Hub spot price. These enter the spread model's control
   set (MODELS.md V.1 / build-order #1) and define the HH leg of the target. Build
   when the spread-model work actually starts; deferred-by-design today.

Phase 1 is the priority — it's the metric the park is blocked on.

---

## Why two roles, one module

Both roles are "fetch an EIA time series and store it"; only the series IDs and
cadence differ. One module, one table, two `make` targets (or one target with a
`--phase`/series-set arg) keeps it simple and avoids a second API-client copy.

| Role | Series (EIA name) | Freq | Consumer |
|---|---|---|---|
| Capture-rate truth | **US natural gas exports by LNG** (MMcf) | monthly | a validation query / new park-checkup item; `analysis/` |
| HH fundamental | **Weekly Lower-48 working gas in underground storage** (Bcf) | weekly | spread-model control panel (MODELS.md V.1) |
| Target leg | **Henry Hub natural gas spot price** ($/MMBtu) | daily | spread target (HH vs TTF) |

**Exact EIA v2 route IDs + facets must be confirmed against the live API browser
(`api.eia.gov/v2/natural-gas`) as implementation step 0** — do not hardcode from
memory. Best-known legacy (v1) series IDs to map from, *to verify*:
- LNG exports, monthly: `NG.N9133US2.M` *(confirm)*
- Lower-48 weekly storage, Bcf: `NG.NW2_EPG0_SWO_R48_BCF.W` *(confirm)*
- Henry Hub spot, daily: `NG.RNGWHHD.D` *(confirm)*

The v2 API wraps these under faceted routes (e.g. `natural-gas/stor/wkly/data`,
`natural-gas/move/expc/data`); the loader should target v2, not the deprecated v1
`/series` endpoint.

---

## Config

Add to `config.py` `Settings` (mirrors `vf_api_key`):
```python
eia_api_key: str = ""   # free key from https://www.eia.gov/opendata/
```
Add `EIA_API_KEY` to `.env` and the `.env` documentation in CLAUDE.md. Empty-string
default so a missing key degrades to a clear "EIA loader disabled" log, not a crash
(same pattern as `vf_api_key`).

## Schema — `eia_series` (tidy/long, new table)

Follow the `signal_daily` tidy/long convention rather than a column-per-series wide
table, so new series cost zero schema change:

```sql
CREATE TABLE eia_series (
    series_id   text        NOT NULL,   -- canonical EIA route+facet id we fetched
    period      date        NOT NULL,   -- EIA 'period' (week-ending / month / day)
    value       double precision,       -- NULL allowed: EIA publishes gaps
    unit        text        NOT NULL,   -- 'MMcf' | 'Bcf' | '$/MMBtu' ...
    frequency   text        NOT NULL,   -- 'monthly' | 'weekly' | 'daily'
    fetched_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (series_id, period)
);
```
- **Idempotent upsert** `ON CONFLICT (series_id, period) DO UPDATE` — EIA revises
  recent periods, so re-fetching must overwrite, not duplicate. `fetched_at` records
  the latest pull (revision visibility).
- Add to `db/init/schema.sql` (source of truth) **and** an Alembic migration in
  `db/migrations/` (the schema has live data; `make reset` is destructive). After a
  fresh reset, `alembic stamp head` per the existing convention.
- This is exogenous reference data, **not** derived from our pipeline, so it gets
  its own table — it is never rebuilt by `make signals`; it is appended/upserted.

## Module shape (`data/eia.py`)

Mirror the pure-fn + thin-DB-loader split used across `pipeline/` and the
one-shot-batch style of `ingestion/vesselfinder.py`:

- `EIA_BASE = "https://api.eia.gov/v2/"`; key passed as the `api_key` query param.
- **Pure parse fn** `parse_eia_response(payload: dict) -> list[EiaRow]` — maps the
  v2 `response.data[]` array (each row has `period` + a value column + `units`) to
  typed rows. Unit-testable against a captured JSON fixture, no network.
- **Thin async loader** `fetch_and_upsert(pool, series_set, *, full=False)` —
  paginates the v2 API (`offset`/`length`, 5000 rows/page max), parses, upserts.
- **Incremental by default**: fetch only periods after `max(period)` already stored
  for each series (`start=` facet), but always re-pull a short trailing window
  (e.g. last 3 months / 8 weeks) to catch EIA revisions. `--full` backfills history
  (5–10 yr) for a fresh DB.
- A small `SERIES` registry (id, unit, frequency, phase) so Phase 2 series are one
  table entry, not new code.
- httpx (already a dep via the stack) or `urllib`; rate limits are generous
  (EIA: ~5 req/s, daily cap) — a simple sequential loop is fine, no throttle class
  needed.

## Makefile + scheduling

```
make eia         # incremental upsert of the active series set
make eia-full    # one-time historical backfill
```
EIA publishes weekly storage Thursdays ~10:30 ET and monthly exports with a ~2
month lag. A weekly cron (alongside the existing 04:00 backup cron) is enough; no
always-on process. Phase 1 (monthly exports) realistically needs running only
weekly/monthly.

## Phase-1 deliverable: the capture-rate validation

The point of building this now. Once monthly LNG exports are in `eia_series`:

1. Convert EIA monthly export volume (MMcf) → **implied cargo count**: `volume ÷
   mean cargo size`, where mean cargo size = mean `gas_capacity_m3` of our captured
   US-Gulf laden `departed` legs (unit-reconciled m³ ↔ MMcf, ~1 cargo ≈ 3.4 Bcf for
   a 174k m³ carrier — confirm the conversion).
2. Per month: `captured = count(departed, regime='mmsi_filter', export zone)` vs
   `implied = EIA-derived`. The ratio is the **capture rate** — the first real
   answer to "what fraction are we seeing?"
3. Surface it: either a new park-checkup item ("capture rate, last full month: X%")
   or an `analysis/` script. Caveat to bake in: only **whole, already-revised**
   months are comparable (EIA's 2-month lag + revisions), and the pre-cutover seam
   means the series only starts being meaningful for months fully after
   2026-05-30 — i.e. first usable reading ~late July 2026 (June is the first full
   post-cutover month, published ~end-July with revisions).

Until that first full month lands, the loader is **plumbing that earns nothing
yet** — which is exactly why it should be built now so the clock starts.

## Tests

- `parse_eia_response` against 2–3 captured JSON fixtures (monthly export, weekly
  storage, an empty/gapped response). Pure, no network — the project's testing norm.
- Upsert idempotency: same payload twice → one row per period, `value` overwritten
  on revision (against a test DB or a parametrized pure-merge fn).
- Unit/conversion guard for the cargo-count derivation (lock the m³↔MMcf factor).

## Phasing & effort

1. **Phase 1a — plumbing** (~1 session): config key, `eia_series` table + migration,
   `data/eia.py` with the parse fn + incremental upsert, `make eia`/`eia-full`,
   tests. Verify v2 route IDs live (step 0). Backfill monthly LNG exports.
2. **Phase 1b — capture-rate metric** (~½ session): the cargo-count conversion +
   the validation query + a park-checkup item. Lands dark (no full post-cutover
   month yet); wires the clock.
3. **Phase 2 — fundamentals** (~½ session, *deferred until spread-model work*): add
   weekly storage + HH spot to the `SERIES` registry; they flow into the model
   control panel when that exists. No new code, just registry entries + a backfill.

## Open decisions before building

1. **Series-of-record for exports.** Monthly LNG exports by total vs by point of
   exit (per-terminal would let us validate *per-terminal* capture, matching
   park-checkup #13's per-terminal table — richer but more facets to manage).
   Lean: start with national total (Phase 1), add point-of-exit later if the
   per-terminal capture question becomes pressing.
2. **HH spot here vs a market-data source.** EIA daily HH spot is free and
   sufficient for the target; no need for a paid feed. Keep it in this module
   (Phase 2) rather than a separate price loader.
3. **Cargo-size denominator.** Fixed nominal (174k m³) vs our observed mean
   `gas_capacity_m3` per terminal. Observed mean is more honest but couples the
   validator to our own (possibly biased) registry. Lean: report both ratios.
