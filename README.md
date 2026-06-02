# tanker-flow

Derives a leading Henry Hub / TTF spread signal from live LNG carrier positions. AIS vessel fixes are ingested in real time, classified against terminal zone polygons, and aggregated into laden ton-miles in transit — a forward-looking proxy for LNG trade flows between US export terminals and NW European import terminals.

---

## Architecture

```
IGU PDF / VF VESSELS ─► scripts/import_igu_fleet.py ► vessel_registry (~780 LNG/FSRU vessels, one-shot)
ais_fixes, vessel_state ─► pipeline/scoring.py ─────► priority_watchlist (5-tier ranking, hourly)
                                                       ↓
priority_watchlist  ──► ingestion/aisstream.py ──► ais_fixes         (TimescaleDB hypertable)
                       (3 WS, MMSI-filtered:        vessel_state      (draught / dest / ETA)
                        100 persistent + 50 scan;
                        1h reconnect cycle)
                                                    terminals          (35 LNG terminals + unlocode)
                                                    terminal_zones     (berth + anchorage + approach polygons)
VesselFinder API    ──► ingestion/vesselfinder.py ──► vessel_registry  (masterdata enrichment by IMO)
VesselFinder API    ──► ingestion/vf_rescue.py ─────► ais_fixes        (live-position backstop for AIS gaps)
ais_fixes           ──► pipeline/port_events.py ──► port_events       (per-visit state machine)
port_events         ──► pipeline/signal.py ────────► laden_ton_miles   [not yet implemented]
EIA API             ──► data/eia.py ───────────────► Henry Hub fundamentals [not yet implemented]
```

---

## Stack

| Component | Technology |
|---|---|
| Database | TimescaleDB (PostgreSQL + PostGIS) on Docker |
| Ingestion | Python / asyncpg / websockets / httpx |
| Enrichment | VesselFinder masterdata + live-position API |
| Terminal zones | QGIS → GeoPackage → PostGIS |
| Monitoring | Textual TUI + FastAPI |
| Config | pydantic-settings / `.env` |
| Package manager | uv |

---

## Database schema

- **`ais_fixes`** — append-only raw position fixes, partitioned by day. Sources: `aisstream-mmsi-1` / `aisstream-mmsi-2` / `aisstream-mmsi-3` (one per parallel MMSI-filtered WebSocket) and `vesselfinder` (weekly reconciliation **and** the `vf_rescue.py` live-position backstop — both inject as normal fixes).
- **`vessel_state`** — voyage-specific fields (draught, destination, ETA) from `ShipStaticData` messages.
- **`vessel_registry`** — one row per MMSI, enriched with VesselFinder masterdata. ~780 LNG/FSRU vessels bulk-imported from the IGU 2025 World LNG Report (`db/seed/lng_fleet_igu_2025.csv`). `is_lng_carrier` and `is_fsru` flags drive vessel filtering.
- **`terminals`** — 35 LNG export/import terminals with scope and type metadata. `unlocode` column (e.g. `NLRTM`) is used by `pipeline/dest_parser.py` to resolve free-text `vessel_state.dest` strings to a terminal_id. `flow_direction` (`export` / `import`) labels each terminal's role.
- **`terminal_zones`** — berth, anchorage, and approach polygons (MultiPolygon/4326) linked to terminals. Drawn in QGIS, imported via `make seed-zones`. The `approach` macro-zone contains anchorage + channel + berth as one envelope so a single port visit stays "open" while the vessel transits between them. See `qgis/README.md` for coverage.
- **`priority_watchlist`** — derived hourly by `pipeline/scoring.py`. One row per LNG/FSRU vessel, ranked into 5 tiers based on current zone proximity / declared destination / activity. The ingester reads this to pick which 150 of ~780 vessels to subscribe to each cycle (100 persistent + 50 scan rotation). `slot_kind` (`persistent` / `pinned` / `scan`) and `in_slot` record who is currently subscribed; `last_scan_window_at` drives scan rotation fairness.
- **`tier_promotions`** — append-only log of vessels promoted **up into** the persistent band (tiers 1-3), written by `scoring.py` (`via='scoring'`) and the inline state machine (`via='inline'`). Powers the TUI promotions panel.
- **`port_events`** — derived table, recomputable from `ais_fixes` via `make port-events`. One row per per-vessel transition: `zone_entry → [anchorage_entry → [anchored] → anchorage_exit]* → [moored → departed]? → zone_exit`. `anchorage_entry`/`anchorage_exit` are raw polygon-crossing markers (no dwell, no SOG filter) — bracket every visit to the anchorage polygon, so `queue_time = anchorage_exit - anchorage_entry`. `anchored` is the dwell-confirmed sibling (≥30 min stationary). Combined: presence of `anchorage_entry` answers "did this vessel queue?"; presence of `anchored` answers "did it queue meaningfully?". Each row also carries `terminal_id`, `lat`/`lon` (for great-circle ton-miles downstream), `laden_flag` (forward-filled draught vs `design_draught`), and `cold_start` (vessel already in a polygon at first observation).
- **`vf_rescue_log`** — append-only audit trail **and** restart-safe credit ledger for `ingestion/vf_rescue.py`. One row per VF live-position lookup attempt: `rescue_class`, `result` (`rescued` / `no_position` / `rejected_stale` / `rejected_teleport` / `error` / `dry_run`), `credits` billed, and `recheck_at` (the per-vessel cooldown). Today's `SUM(credits)` gates the daily cap; the latest row per MMSI is the cooldown.
- **`vf_account_status`** — VesselFinder account-balance snapshots from the free `/status` endpoint, appended each rescue run. The TUI shows the latest `credits` + `expiration_date`; consecutive rows give the true burn rate.
- **`ingestion_events`** — append-only lifecycle log (connect / subscribe / planned_reconnect / watchdog_reconnect / disconnect / error). Per-connection liveness is derived from `ingestion_stats_minute` instead of a dedicated heartbeat table.
- **`ingestion_stats_minute` / `ingestion_zone_minute`** — per-minute ingestion stats written by the in-process `MinuteAggregator`: lag mean/p95, distinct MMSI, queue saturation, and connection age per source (and per zone in the latter). Drive the TUI health and field zones.
- **`fixes_per_minute` / `fixes_per_hour`** — TimescaleDB continuous aggregates for the monitoring TUI.

---

## Ingestion

`ingestion/aisstream.py` runs **three parallel WebSocket connections** to AISstream, each subscribed via server-side `FiltersShipMMSI` to a disjoint chunk of up to 50 LNG-carrier MMSIs (AISstream's per-subscription cap). Connections 1+2 (100 slots total) hold the **persistent block** — the top-ranked vessels by tier from `priority_watchlist`. Connection 3 is the **scan rotation** — 50 slots drawn from three priority-ordered pools (`SCAN_OVERFLOW_SLOTS`/`SCAN_TIER4_SLOTS`/`SCAN_TIER5_SLOTS` = 15/25/10, with roll-over so the slots are always filled): **persistent-band overflow** (tier ≤ 3 vessels crowded out of the persistent block — the highest-value unsubscribed vessels), then **tier 4**, then a reserved **tier-5** discovery quota. The overflow pool closes the old hole where tier-3 vessels excluded from a tier ≥ 4-only scan went fully dark. Each pool picks the least-recently-scanned vessels (`last_scan_window_at ASC NULLS FIRST`) and writes back `now()` so rotation actually rotates. All three connections reconnect on a 1h planned cadence (and a 5-min silence watchdog), immediately after a scoring run re-ranks the watchlist. Source labels in `ais_fixes` and `ingestion_stats_minute`: `aisstream-mmsi-{1,2,3}`. See `ingestion/README.md` for the throttle / filter constraints that drove this design.

`pipeline/scoring.py` (called by the ingester every hour, also runnable via `make scoring`) ranks every LNG/FSRU vessel into one of 5 tiers from current `ais_fixes` + `vessel_state` data:

- **Tier 1** — recent fix inside any `terminal_zones` polygon (within 3d).
- **Tier 2** — `vessel_state.dest` resolves to a known terminal **and** either the declaration is fresh (`state_ts` < 14d) **or** a parsed ETA falls within `ETA_IMMINENT_HOURS` (48h). The ETA path rescues long-voyage vessels whose declaration is stale but whose arrival is imminent.
- **Tier 3** — recent fix inside the wider `config.ZONES` rectangle (within 14d), not already tier 1/2. Ordered within-tier by *closing-ness* (proximity + heading toward the nearest zone, via `_closing_bonus`) so the scarce persistent slots go to vessels actually approaching.
- **Tier 4** — any fix in the last 7d (not 1-3).
- **Tier 5** — fix in 7-90d or no fix at all.

The top 100 (tiers 1-3 by score) get persistent slots; the rest of tier 3 plus tiers 4-5 feed the scan rotation. `is_pinned` force-holds a slot for two cases: a recent **open laden leg** (re-acquire on return) and a vessel **currently open in a port visit** (last `port_event` is not a departure — protects long berth queues from decaying out of coverage). Both are bounded by recency + a cap.

`scripts/import_igu_fleet.py` is the one-shot importer that brought the global LNG fleet into `vessel_registry` from the IGU 2025 PDF. Uses VF's VESSELS endpoint to resolve IMO → MMSI (since AISstream's filter takes MMSI only) and to capture a position + dest + ETA snapshot for each vessel.

`ingestion/vesselfinder.py` enriches `vessel_registry` with masterdata from the VesselFinder API — vessel type, DWT, gas capacity, owner/manager, `is_lng_carrier`, `is_fsru`. Runs as a one-shot batch (default `--terminal-only`), rate-limited to one request per second. Vessels with `imo = 0` are sub-IMO and marked `vf_enrichment_status = 'no_imo'` without an API call.

`ingestion/vf_rescue.py` is the **live-position backstop for AIS gaps** — runs as a background task in the ingester (every 30 min) and is runnable via `make vf-rescue`. The signal is built from leg-defining port *events* (laden `departed` from an export terminal → `zone_entry`/`moored` at an import terminal), so when AISstream drops a vessel **at or approaching one of our terminals**, that event is at risk. The worker selects coastal vessels that have gone AIS-silent within an actionable band (geometry + heading gates; a `STALE_CEILING_HOURS` ceiling skips vessels whose event has already passed), fetches their current position from VesselFinder's `/vessels` feed (terrestrial — 1 credit; the target is coastal so satellite is never requested), sanity-checks it (freshness + teleport gates), and injects it as a normal `ais_fixes` row (`source='vesselfinder'`) — the existing pipeline re-acquires the vessel for free. Candidates are classified by the event at risk: `import_arrival` / `export_departure` / `outage_check` (highest priority), `dest_capture` / `import_berth`, then `export_arrival`; `manual` (a `--mmsi` override) jumps the queue. `export_arrival` (ballast approaching to load) is the lowest-value class — it only times the ballast-leg close, never a headline laden signal — so it's held to a stricter geometry + silence bar (final-approach/closing **and** ≥8h silent) than the laden classes. VF credits are a finite reserve that **expires unused** on a fixed date, so rather than minimise spend the worker aims to deplete the reserve to ~zero *right at expiry*: `DAILY_CREDIT_CAP` is set to the glide-path rate (reserve ÷ days-to-expiry, ≈14/day) and the daily budget is spent best-first by priority class. `vf_rescue_log` is the restart-safe daily-credit cap and per-vessel cooldown. Use `make vf-rescue-dry` for a no-spend candidate/cost preview and `make vf-status` for the free account balance.

---

## Commands

```bash
# Database
make up              # Start TimescaleDB container
make down            # Stop container
make psql            # Open psql shell
make reset           # DESTRUCTIVE: wipe and recreate DB

# Seeding
make seed-terminals  # Load terminal reference data
make seed-zones      # Import terminal zone polygons from QGIS GeoPackage (upserts)
make seed-unlocodes  # Seed terminals.unlocode from db/seed/terminal_unlocodes.sql

# Ingestion
make ingest          # Run AIS ingestion + monitoring TUI
make enrich          # Run VesselFinder enrichment batch
make refresh-fleet   # Re-parse the latest IGU PDF and import any newly-listed IMOs

# VesselFinder rescue (credit-budgeted live-position backstop)
make vf-rescue       # Fetch live positions for AIS-silent near-terminal vessels
make vf-rescue-dry   # No-spend candidate + cost preview
make vf-status       # Fetch + store the VF account balance (free /status call)

# Pipeline
make port-events     # Recompute port_events from ais_fixes + terminal_zones (idempotent)
make scoring         # Recompute priority_watchlist (also runs hourly inside the ingester)

# Viz
make viz             # Start FastAPI monitoring web app (localhost:8000)
```

---

## What is not yet implemented

| Component | Description |
|---|---|
| `pipeline/signal.py` | Aggregates `port_events` into laden ton-miles in transit by zone pair |
| `data/eia.py` | Pulls US natural gas storage data from the EIA API (Henry Hub fundamentals) |
| `analysis/` | Spread prediction model and exploratory notebooks |

---

## How this was built

I built tanker-flow with AI assistance (Claude Code) as a deliberate part of
the workflow — and I'd rather show that than hide it. Directing AI well to
produce a correct, non-trivial system is part of the skillset, not a shortcut
around it.

**What's mine:** the architecture, the domain model (the port-event state
machine, the regime-segmented signal, the credit-budgeted VesselFinder rescue
backstop), and every consequential design call and tradeoff. AI accelerated
implementation, refactors, and data exploration under that direction.

**How it's kept rigorous:**
- **Design-doc-first** — `CLAUDE.md` is the living architecture
  spec the assistant (and any reader) works from.
- **Audit-before-build** — [`docs/`](docs/) holds the data audits I run before
  committing to a change (e.g. root-causing every "vessel appeared in berth
  directly" event before touching the scoring pin).
- **Verify, don't trust** — findings are checked against live query output,
  never asserted.

Every line is one I can explain and defend.
