# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**tanker-flow** derives a leading Henry Hub / European LNG price spread signal from live LNG carrier positions. It ingests AIS (Automatic Identification System) vessel position data, processes it into port events at major LNG export/import terminals across seven geographic zones, and aggregates laden ton-miles in transit as a market signal.

## Commands

**Database (Docker/TimescaleDB):**
```bash
make up          # Start TimescaleDB container
make down        # Stop container
make psql        # Open psql shell inside container
make logs        # Tail TimescaleDB logs
make reset       # DESTRUCTIVE: wipe all data and restart
make db-ui       # Open sqlit TUI against the tanker-flow connection
```

**Seeding / pipeline:**
```bash
make seed-terminals  # Load terminal reference data
make seed-zones      # Import terminal zone polygons from db/seed/terminal_zones.gpkg
make seed-unlocodes  # Seed terminals.unlocode from db/seed/terminal_unlocodes.sql
make ingest          # Run AIS ingestion + monitoring TUI together
make enrich          # Run VesselFinder enrichment (terminal-only by default)
make refresh-fleet   # Re-parse the latest IGU PDF and import any newly-listed IMOs
make port-events     # Recompute port_events from ais_fixes (idempotent — TRUNCATEs then rebuilds)
make scoring         # Recompute priority_watchlist (also runs hourly inside the ingester)
make signals         # Rebuild signal_daily panel (laden ton-miles in transit + flow signals)
make viz             # Start FastAPI viz at localhost:8000
```

**VesselFinder rescue (credit-budgeted live-position backstop):**
```bash
make vf-rescue       # Fetch live positions for AIS-silent near-terminal vessels
make vf-rescue-dry   # No-spend candidate + cost preview
make vf-status       # Fetch + store the VF account balance (free /status call)
```

**Python (managed with uv):**
```bash
uv run python ingestion/aisstream.py   # Run AIS ingestion
uv run pytest                          # Run all tests
uv run pytest tests/test_foo.py        # Run a single test file
uv run ruff check .                    # Lint
uv run ruff format .                   # Format
```

**Environment:** Requires a `.env` file with `DB_PASSWORD`, `DB_USER`, `DB_NAME`, `DB_HOST`, `DB_PORT`, `AISSTREAM_API_KEY`, and `VF_API_KEY` (VesselFinder enrichment + rescue). `config.py` loads these via `pydantic-settings`.

## Architecture

### Data Flow
```
AISstream WebSocket → ingestion/aisstream.py → ais_fixes (TimescaleDB hypertable)
VesselFinder API   → ingestion/vesselfinder.py → vessel_registry (enrichment)
VesselFinder API   → ingestion/vf_rescue.py   → ais_fixes (live-position backstop for AIS gaps)
ais_fixes          → pipeline/port_events.py  → port_events (state machine)
port_events        → pipeline/legs.py          → classified voyage legs (in-memory)
legs + port_events → pipeline/signal.py        → signal_daily (market-signal panel)
EIA API            → data/eia.py               → US natural gas storage (Henry Hub fundamentals)
```

### Database Schema (`db/init/schema.sql`)
`schema.sql` is the source of truth for the full current schema. It is the Docker init script — `make reset` recreates the DB from it. Alembic migrations in `db/migrations/` record how the schema evolved; after a fresh `make reset`, run `alembic stamp head` so Alembic treats the schema as already applied before adding future migrations.

Tables and views:

- **`ais_fixes`** — TimescaleDB hypertable partitioned by `fix_ts` (1-day chunks). Append-only raw fixes from two sources: `aisstream` (real-time WebSocket, labelled `aisstream-mmsi-{1,2,3}` per connection) and `vesselfinder` (weekly reconciliation **and** the `vf_rescue.py` live-position backstop — both inject as normal fixes). The `source` column distinguishes them.
- **`vessel_state`** — TimescaleDB hypertable. Voyage-specific fields (draught, destination, ETA) from `ShipStaticData` messages, keyed by `(state_ts, mmsi)`.
- **`vessel_registry`** — One row per MMSI. Populated passively as vessels are seen; enriched with VesselFinder masterdata. `enriched_at IS NULL` means not yet enriched. `vf_enrichment_status` tracks enrichment outcome (`ok` | `not_found` | `error` | `no_imo`). Enrichment is zone-gated (queued only when a fix lands inside a `terminal_zones` polygon), so pending rows outside terminals are noise rather than a backlog to clear.
- **`terminals`** — One row per LNG terminal. Has a `zone` column grouping terminals into seven geographic zones (see below). `in_signal_scope` flags whether the terminal is active in the signal. `is_fsru` distinguishes FSRUs from land-based terminals. `unlocode` (UN/LOCODE like `NLRTM`) is seeded via `make seed-unlocodes` and used by `pipeline/dest_parser.py` to resolve `vessel_state.dest` → `terminal_id` for tier-2 scoring.
- **`terminal_zones`** — PostGIS polygons imported from QGIS. `zone_type` is one of `berth` | `anchorage` | `approach`. `approach` is a macro-envelope drawn to geometrically contain anchorage + channel + berth so a single visit stays "open" while the vessel transits between them (otherwise the ~4–6 h channel transit at Sabine/Rotterdam etc. would emit a spurious zone_exit). One or more rows per terminal, distinguished by `sub_zone`.
- **`port_events`** — Derived table, recomputed by `make port-events` from `ais_fixes` + `terminal_zones`. One row per per-vessel transition. `event_type` in {`zone_entry`, `anchorage_entry`, `anchored`, `anchorage_exit`, `moored`, `departed`, `zone_exit`}. `anchorage_entry`/`exit` are raw polygon-crossing markers (no dwell/SOG filter) — they bracket every visit to an anchorage and let downstream queue-time signals be measured without the ~30 min back-dating bias of `anchored`. `anchored` remains the dwell-confirmed marker (≥30 min stationary at sog<1). `zone` is one of seven geographic zones; `terminal_id` names the specific terminal. `lat`/`lon` carry the event position (used downstream for great-circle ton-miles). `cold_start = TRUE` flags synthetic events emitted when a vessel's first observed fix was already inside a polygon. `laden_flag` is derived per event by forward-filling the most recent `vessel_state.draught` and comparing to `vessel_registry.design_draught` (≥0.85 × design ⇒ laden).
- **`signal_daily`** — Derived table, rebuilt by `make signals` (TRUNCATE + rebuild, like `port_events`) from the classified voyage legs (`pipeline/legs.py`) + port visits (`pipeline/visits.py`). Tidy/long daily panel: one row per `(signal_key, bucket_date, zone_scope, regime, basis)`. The headline signals are **gas volumes (m³), `zone_scope` carrying the stacked band**, split by kind: the **at-sea signals are daily stocks** (gas live on the water each day) and the **berth signals are amortized daily flows** (each cargo spread across its berth hours so it integrates to one cargo, unit m³/day): `gas_loading_us` (US loading rate, banded by terminal), `gas_discharging_eu` (EU discharge rate, laden, banded by terminal), `gas_in_transit_volume` (laden gas at sea US→EU stock, banded by destination zone; undeclared-destination open legs go to the `'unknown'` band), `gas_ballast_to_us` (empty carriers returning to reload, weighted by the capacity they'll carry, banded by destination zone). `value` is m³ (m³/day for the two berth signals); `n_legs` is the contributing leg/visit count for that band/day. The berth flow de-biases an earlier in-berth *stock* where a visit straddling midnight double-registered its full cargo on both days. These replaced the earlier ton-mile headline set (`laden_ton_miles_in_transit_*`, `eu_arrivals`, `us_loadings`, `mean_laden_voyage_age_h`, `od_flow_count`). `regime` is segmented per SIGNALS.md §0.5 (never aggregate a model across the 2026-05-30 seam) — tagged by the *item's* regime (fixed at departure/mooring), with a synthetic `'all'` row summing both. `basis='physical'` is the hindsight-clean reconstruction (an item contributes on day `d` iff its live interval covers `d`); `'knowable'` (leakage-free point-in-time) is reserved but not yet built.
- **`tier_promotions`** — Append-only log of vessels promoted **up into** the persistent band (tiers 1-3), written by `scoring.py` (`via='scoring'`) and the inline state machine (`via='inline'`). Powers the TUI promotions panel.
- **`vf_rescue_log`** — Append-only audit trail **and** restart-safe credit ledger for `vf_rescue.py`. One row per VF live-position lookup attempt: `rescue_class`, `result` (`rescued` | `no_position` | `rejected_stale` | `rejected_teleport` | `error` | `dry_run` | `skipped_budget`), `credits` billed, and `recheck_at` (per-vessel cooldown). Today's `SUM(credits)` gates the daily cap; the latest row per MMSI is the cooldown. `skipped_budget` rows (0 credits, no cooldown, one per vessel per UTC day) audit candidates the daily budget couldn't serve — a vessel with a skipped row and no later billed row the same day is measured unmet demand.
- **`vf_account_status`** — VesselFinder account-balance snapshots from the free `/status` endpoint, appended each rescue run. Latest row = current `credits` + `expiration_date`; consecutive rows give the true burn rate.
- **`ingestion_events`** — Append-only lifecycle log written by `aisstream.py` (connect / subscribed / planned_reconnect / watchdog_reconnect / disconnect / error). Per-connection liveness is derived from `ingestion_stats_minute` (max bucket per source) rather than a separate heartbeat table.
- **`ingestion_stats_minute` / `ingestion_zone_minute`** — Per-minute ingestion stats (lag mean/p95, distinct_mmsi, queue saturation, connection age; per-zone fix counts in the latter) written by the in-process `MinuteAggregator`. Drive the TUI health and field zones.
- **`priority_watchlist`** — Derived hourly by `pipeline/scoring.py`. One row per LNG/FSRU vessel in scope (~780 today), ranked into 5 tiers. The ingester reads `top 100 WHERE tier<=3` (open-leg + in-port pins first) for persistent slots, and for scan rotation reads 50 from three priority-ordered pools: **persistent-band overflow** (tier≤3 that missed a persistent slot — the closest such vessels held the slots, the rest rotate here), then tier-4, then a reserved tier-5 discovery quota (`SCAN_OVERFLOW_SLOTS`/`SCAN_TIER4_SLOTS`/`SCAN_TIER5_SLOTS` = 15/25/10, with roll-over). The overflow pool closes the old coverage hole where tier-3 vessels crowded out of the persistent block were excluded from the tier≥4-only scan and went fully dark. `in_slot` / `slot_kind` are written by `aisstream.py` after each 1h reconnect so the TUI can render who's currently subscribed; `last_scan_window_at` is bumped to `now()` inside `load_scan_mmsis` itself so the same vessels don't get re-picked on every watchdog reconnect.
- **`fixes_per_minute` / `fixes_per_hour`** — TimescaleDB continuous aggregates over `ais_fixes`, used by the monitoring TUI.

### Terminal zones

| Zone | Terminals |
|------|-----------|
| `usgulf` | Sabine Pass, Freeport, Calcasieu Pass, Golden Pass, Cameron, Corpus Christi, Plaquemines |
| `usatlantic` | Cove Point, Elba Island |
| `nweurope` | Gate/Rotterdam, Zeebrugge, Dunkerque, South Hook, Isle of Grain, Eemshaven FSRU, Brunsbuttel FSRU, Wilhelmshaven 1 & 2 FSRU |
| `baltic` | Mukran (Deutsche Ostsee), Swinoujscie, Klaipeda FSRU |
| `iberian` | Sines, Bilbao, Huelva |
| `wmed` | Barcelona, Cartagena, Sagunto, Adriatic LNG, Piombino FSRU, Ravenna FSRU, Krk |
| `emed` | Revithoussa, Alexandroupolis FSRU |

### Ingestion (`ingestion/`)
- **`aisstream.py`** — Async WebSocket subscriber using `websockets` + `asyncpg`. Runs three parallel WebSocket connections, each subscribed to a disjoint chunk (≤50 MMSIs per AISstream's `FiltersShipMMSI` cap). Slot allocation: **chunks 0+1 are the persistent block** (top 100 vessels by tier from `priority_watchlist`); **chunk 2 is the scan rotation** (50 slots drawn from three priority-ordered pools — persistent-band overflow → tier-4 → tier-5, with roll-over; see `priority_watchlist` above — each ordered by `last_scan_window_at ASC NULLS FIRST`; the loader writes back `now()` in the same transaction so each reconnect — planned 1h *or* the 5-min watchdog — picks a different 50). Each subscription uses a full-globe bbox; the MMSI filter does the constraining. Source labels (now plumbed through to `ais_fixes.source` as well as the stats tables): `aisstream-mmsi-1` / `aisstream-mmsi-2` / `aisstream-mmsi-3`. `RECONNECT_INTERVAL_SECONDS = 3600` triggers a planned reconnect every 1h, and a `scoring_loop` background task re-runs `pipeline.scoring.compute_and_upsert(pool)` just before each one so the priority_watchlist is fresh.
- **`dynamic_enrichment.py`** — Worker draining the VesselFinder enrichment queue (rate-limited to 1 req/s). The old passive-discovery hook (`maybe_queue()` triggered by unknown-MMSI fixes in terminal polygons) has been removed: under server-side MMSI filtering, unknown MMSIs never reach us. The queue is now fed by `vesselfinder.py --terminal-only` (batch path) and a planned daily LNG-fleet refresh.
- **`vesselfinder.py`** — One-shot batch enrichment via the VesselFinder masterdata API. Defaults to `--terminal-only` (only MMSIs with at least one fix inside a `terminal_zones` polygon). Rate-limited to 1 req/s. Vessels with `imo = 0` are sub-IMO and short-circuited to `vf_enrichment_status = 'no_imo'` without an API call (LNG carriers always have IMOs from day one, so the `imo=0 → real IMO` transition is intentionally not handled).
- **`vf_rescue.py`** — Live-position backstop for AIS gaps, run as a background task in `aisstream.py` (every 30 min) and via `make vf-rescue`. The signal is built from leg-defining port *events* (laden `departed` from an export terminal → `zone_entry`/`moored` at an import terminal), so when AISstream drops a vessel **at or approaching one of our terminals** that event is at risk. The worker selects coastal vessels gone AIS-silent within an actionable band (proximity + heading gates; a `STALE_CEILING_HOURS` ceiling skips vessels whose event has already passed), fetches their current position from VF's `/vessels` feed (terrestrial — 1 credit; the target is coastal so satellite is never requested), sanity-checks it (freshness + teleport gates), and injects it as a normal `ais_fixes` row (`source='vesselfinder'`) — the existing pipeline re-acquires it for free. Candidates are classified by the event at risk (`import_arrival` / `export_departure` / `outage_check` highest, then `dest_capture` / `import_berth`, then `export_arrival`; `manual` via `--mmsi` jumps the queue). `export_arrival` (ballast approaching to load) is the lowest-value class — it only times the ballast-leg *close* (`gas_ballast_to_us`), never a headline laden signal — so it's held to a stricter bar than the laden classes: it fires only in final approach (≤`FINAL_APPROACH_KM`, or actively closing) **and** after ≥`EXPORT_ARRIVAL_MIN_SILENCE_HOURS` (8h, 2× the general floor), trimming its loiter/short-gap tail; reacquisition still closes the leg from the next live fix. VF credits are a finite reserve that **expires unused** on a fixed date, and every credit spent on a near-terminal silent vessel buys signal, so the policy is to deplete the reserve to ~zero *exactly at expiry* (spend slower ⇒ forfeit credits; faster ⇒ go dark for the final stretch), not to minimise it: the daily cap is **derived each run** from the latest `vf_account_status` snapshot (`glide_cap`: ceil(reserve ÷ days-to-expiry), ≈ **14/day** as of 2026-06; `DAILY_CREDIT_CAP` is the no-snapshot fallback, `GLIDE_CAP_CEILING` clamps against balance drift) and the scarce daily budget is spent best-first by `CLASS_PRIORITY`; candidates the budget can't serve are logged as `result='skipped_budget'` so unmet demand is measurable. `vf_rescue_log` is the restart-safe daily-credit ledger + per-vessel cooldown. `--dry-run` previews candidates/cost without spending; `--status` snapshots the free account balance.
- **`models.py`** — Pydantic models validating raw AISstream JSON. Key quirk: `MetaData.time_utc` arrives as a non-standard Go timestamp string (`"2026-04-12 19:10:08.192247737 +0000 UTC"`) and is parsed by a `field_validator`. `MaximumStaticDraught` of `0.0` is treated as `NULL` (unreported). Also holds the VesselFinder masterdata models and `VesselFinderAIS` (the live `/vessels` position parsed by `vf_rescue.py`, with the same sentinel-nulling: COURSE 360 / HEADING 511 / DRAUGHT 0 → NULL).

### Viz (`viz/`)

Two surfaces with a strict role split: TUI for pipeline ops, web for data + (upcoming) signals. The web carries a small live/stale pulse HUD as a quick "is data flowing?" confirmation, but per-source diagnostics live only in the TUI.

- **`tui.py`** — Textual TUI launched by `make ingest`; pipeline-health dashboard. Organised in three horizontal bands:
  - **Health zone** (top): dense status row (per-source dots, watchlist coverage, scoring heartbeat, clock), per-source granularity strip (lag mean/p95, distinct_mmsi, queue saturation from `ingestion_stats_minute`), recent errors feed (`ingestion_events WHERE event_type='error'`), per-source reconnect rate (watchdog vs planned, last 1h), ingest-lag chart, and fixes/hour chart.
  - **Watchlist zone** (middle): tier breakdown, scan rotation countdown, session-scoped promotions log, and a full scrollable `priority_watchlist` explorer with per-vessel score + reason. Keybindings: `1`–`5` tier filter, `0` clear, `s` cycle sort (tier/score/last_fix/name), `/` name search, `Esc` reset filters.
  - **Field zone** (bottom): zone occupancy bar, per-terminal staleness, silent vessels list.
- **`app.py`** + **`static/`** — FastAPI + Leaflet map at `localhost:8000`. Frontend is split into ES modules served from `static/`:
  - `index.html` — markup shell
  - `css/style.css` — extracted stylesheet
  - `js/config.js` — colors, basemap defs, tier scale, formatters
  - `js/map.js` — Leaflet init, basemap switching, layer-toggle plumbing
  - `js/vessels.js` — `/api/vessels` rendering, marker styling, selection, freshness fade
  - `js/zones.js` — `/api/terminal-zones` + `/api/bounding-boxes`
  - `js/track.js` — vessel-track + event-marker layers
  - `js/events.js` — port-events panel + recent-fixes panel
  - `js/density.js` — shipping-lane raster
  - `js/hud.js` — status line + live ingestion-pulse HUD
  - `js/main.js` — entry point, wires controls and intervals
  - A future `js/signal.js` (laden ton-miles in transit / Henry Hub–TTF spread) slots in alongside these without further plumbing.

### Pipeline (`pipeline/`)
- **`port_events.py`** — Orchestration. TRUNCATEs `port_events`, runs a single on-the-fly spatial join via `ST_Within` to attach a candidate-zones array to every in-scope fix, streams the result via asyncpg cursor split by MMSI, feeds each vessel's stream into the pure state machine, post-processes (laden, DFA validation), and batch-inserts the events. In-scope vessels are `vessel_registry WHERE is_lng_carrier = TRUE OR is_fsru = TRUE` — FSRUs are admitted explicitly because VesselFinder classifies them as `'Offshore Support Vessel'` (so `is_lng_carrier = FALSE`).
- **`scoring.py`** — Builds the `priority_watchlist` table by ranking every LNG/FSRU vessel into one of 5 tiers from current `ais_fixes` + `vessel_state` + spatial joins against `terminal_zones` and the `config.ZONES` rectangles. Tiers: 1 = in any terminal_zones polygon (last **3d** — tightened from 14d so we don't waste persistent slots on vessels long-since departed); 2 = declared inbound via parsed `vessel_state.dest` with state_ts < 14d **OR** a parsed ETA within `ETA_IMMINENT_HOURS` (rescues long-voyage vessels whose declaration is stale but arrival is imminent); 3 = in the wider config.ZONES bbox (14d), ordered within-tier by **closing-ness** (proximity + heading to the nearest zone via `_closing_bonus`) so the scarce slots go to vessels actually approaching; 4 = any fix in last 7d; 5 = stale/unseen. Ingester reads top-100 by `(tier ASC, score DESC)` for persistent slots; scan rotation covers tier-3 persistent-band overflow + tiers 4/5 (see `priority_watchlist` above). `is_pinned` covers two cases: an **open leg in its expected approach window** — both directions, laden→import arrival *and* ballast→export-terminal loading (the ballast return was the dominant appear-in-berth miss), gated by `EXPECTED_VOYAGE_DAYS` per departure zone (US export ↔ EU import ~14–18d) and ranked by closeness to expected arrival so the scarce slots favour vessels approaching *now* rather than just-departed ones still mid-ocean (these are coarse Phase-1 constants; Phase 2 swaps them for per-O-D rolling medians from `legs.py` durations) — **and** a vessel currently open in a port visit (in-port pin — protects long berth queues from decaying out of coverage). Single sub-second SQL pass + in-memory tier-assignment loop. Runs once at ingester startup, then every 1h as a background task; also runnable manually via `make scoring`.
- **`dest_parser.py`** — Pure function `parse_destination(dest_str, unlocode_to_terminal) -> (terminal_id, is_for_orders)`. Handles UN/LOCODE direct (`NLRTM`), internal-space LOCODE (`NL RTM`), chained (`USSAB>NLRTM` → uses RHS), "FOR ORDERS" markers, and freeform names (`ROTTERDAM`, `EEMSHAVEN`) via an in-code normalizer. Tier-2 scoring relies on this. Aliases for known LNG terminals live as constants in this module; UN/LOCODEs themselves live on `terminals.unlocode`.
- **`state_machine.py`** — Pure per-vessel walk; no DB. States: TRANSIT / IN_ENVELOPE / ANCHORED / MOORED / DEPARTED. **Three-layer fix resolution**: (1) berth override — any candidate at `zone_type='berth'` wins regardless of stickiness; (2) stickiness — if a visit envelope is open for terminal A and any candidate matches A, stay with A; (3) cold entry — most-specific `zone_type` wins (berth > anchorage > approach), tiebreak by nearest berth centroid (over all sub_zones). **Inline reattribution**: when a berth-override would force a terminal switch mid-envelope, if every earlier event in the current envelope had the new terminal in its `candidate_terminal_ids`, the envelope is rewritten in place instead of emitting spurious `zone_exit`/`zone_entry`. **Anchorage tracking**: while in IN_ENVELOPE or ANCHORED, every fix's resolved `zone_type` is compared to an `in_anchorage` flag — entering the anchorage polygon emits `anchorage_entry`, leaving emits `anchorage_exit`. These are suppressed while MOORED/DEPARTED to avoid spurious events from jitter near overlapping berth/anchorage polygons. `anchorage_exit` also transitions ANCHORED → IN_ENVELOPE, allowing re-anchoring in the same envelope. **Cold-start**: a first-observed fix already inside a berth/anchorage emits synthetic `zone_entry` (+ `anchorage_entry` for the anchorage case) + `moored`/`anchored` with `cold_start=TRUE`. **AIS-dropout safe**: transitions require an actual fix *outside* the relevant polygon; time gaps alone don't trigger anything. **Back-dated timestamps** for dwell-confirmed events (`anchored`, `moored`, `departed`): they fire at the moment of transition (first qualifying fix), not at dwell-confirmation. Raw polygon crossings (`anchorage_entry`/`exit`, `zone_entry`/`exit`) fire at the actual observed fix. **Reacquisition after a gap**: a berth fix (sog < 1) that is the first of its arming sequence *and* lands ≥ the moored dwell window after the previous fix confirms `moored` immediately (back-dated to that fix) instead of waiting out the dwell — a single isolated fix can never exhibit the 30-min dwell, so this captures vessels picked back up already alongside (e.g. a one-off `vf_rescue.py` fix after a mid-visit AIS dropout), same reasoning as cold-start.
- **`laden.py`** — Builds an in-memory `mmsi → [(state_ts, draught)]` lookup from `vessel_state` once per run, then bisect-lookups the most recent draught at-or-before each event's time. Forward-fill (any age) because `vessel_state` is sparse.
- **`legs.py`** — Voyage-leg foundation under the flow signals. Pure `pair_legs` + thin `compute_legs(pool, now)` loader. Pairs each `departed` with its vessel's next `zone_entry` and **classifies**: `closed` (cross-zone real voyage), `same_zone` (intra-region hop — ~zero cross-zone ton-miles), `open_in_transit` (no arrival yet, within the per-O-D voyage window), `open_floating` (past window but a recent coastal fix → genuine floating storage), `open_arrival_gap` (past window, stale fix in dest region → arrived-and-missed), `open_censored` (past window, no coastal evidence → phantom, excluded everywhere). Each leg is regime-tagged (`config.regime_of` on the departure) and carries `dwt`/`gas_capacity_m3` + great-circle `distance_nm` (closed legs only). The signal layer aggregates over this; it does **not** re-pair.
- **`visits.py`** — Port-visit pairing: pure `pair_visits` + thin `compute_visits(pool, now)`. A *visit* pairs a `moored` with the vessel's next `departed` — the berth-occupancy interval during which a vessel loads (export terminal) or discharges (import terminal). Carries terminal/zone/`flow_direction` + `dwt`/`gas_capacity_m3` + the moored event's `laden`/`regime`/`cold_start`. An **open** visit (a `moored` with no following `departed`) is the vessel currently in berth; `departed_ts=None`. This is the foundation under `gas_loading_us` / `gas_discharging_eu`; it is distinct from the still-planned `anchorage_entry → moored` *queue* pairing (#6/#12).
- **`signal.py`** — The signal aggregation layer. Pure aggregators (`accumulate_daily`) + a thin DB loader; rebuilds `signal_daily` (TRUNCATE + atomic executemany swap, like `port_events`). Consumes `legs.compute_legs()` + `visits.compute_visits()`. Headline signals are **gas volumes (m³)** reconstructed per day: the at-sea legs as a **stock** over each leg's live interval (`gas_in_transit_volume`, `gas_ballast_to_us`, banded by destination zone), and the berth visits as an **amortized flow** (`gas_loading_us` / `gas_discharging_eu`, banded by terminal). The flow spreads each cargo across its berth hours at a constant rate (`amortized_cargo_contribution`) so a closed visit integrates to exactly one cargo (m³/day); an **open** visit estimates its total dwell from the terminal's closed-visit mean (`terminal_dwell_hours`) until it departs, with the cumulative deposit capped at one cargo. The unit is `gas_capacity_m3` — **no distance weighting** (this replaced the ton-mile headline). `basis='physical'` (an item contributes iff its interval covers day `d`). Open visits are also capped at `OPEN_VISIT_CEILING_DAYS = 5` so a missed-departure phantom (AIS dropout while alongside) can't smear forever — the visit analog of the open-leg censor in `legs.py`. `--as-of`/`--panel-start` for reproducible rebuilds. Queue-time family (#6/#12) and the leakage-free `basis='knowable'` series are deferred. See `analysis/SIGNALS.md`.
- **FSRU short-circuit** — `is_fsru=TRUE` MMSIs skip the state-machine walk; one `moored` event is emitted at the FSRU's declared host terminal (`terminals.fsru_host_mmsi`), timestamped at the FSRU's first observed fix, with `cold_start=TRUE`. FSRUs without a declared host are logged and skipped.

### Planned but not yet implemented
Queue/berth-time signals (#6/#7/#12/#13 — need a separate `anchorage_entry → moored` visit-pairing module), the leakage-free `basis='knowable'` point-in-time signal series, `data/eia.py` (US storage), and `analysis/` (spread model — premature until the post-cutover training corpus accrues; see `analysis/MODELS.md`).
