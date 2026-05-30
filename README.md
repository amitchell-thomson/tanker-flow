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
ais_fixes           ──► pipeline/port_events.py ──► port_events       (per-visit state machine)
port_events         ──► pipeline/signal.py ────────► laden_ton_miles   [not yet implemented]
EIA API             ──► data/eia.py ───────────────► Henry Hub fundamentals [not yet implemented]
```

---

## Stack

| Component | Technology |
|---|---|
| Database | TimescaleDB (PostgreSQL + PostGIS) on Docker |
| Ingestion | Python / asyncpg / websockets |
| Enrichment | VesselFinder masterdata API |
| Terminal zones | QGIS → GeoPackage → PostGIS |
| Monitoring | Textual TUI + FastAPI |
| Config | pydantic-settings / `.env` |
| Package manager | uv |

---

## Database schema

- **`ais_fixes`** — append-only raw position fixes, partitioned by day. Sources: `aisstream-mmsi-1` / `aisstream-mmsi-2` / `aisstream-mmsi-3` (one per parallel MMSI-filtered WebSocket) and `vesselfinder` (weekly reconciliation).
- **`vessel_state`** — voyage-specific fields (draught, destination, ETA) from `ShipStaticData` messages.
- **`vessel_registry`** — one row per MMSI, enriched with VesselFinder masterdata. ~780 LNG/FSRU vessels bulk-imported from the IGU 2025 World LNG Report (`db/seed/lng_fleet_igu_2025.csv`). `is_lng_carrier` and `is_fsru` flags drive vessel filtering.
- **`terminals`** — 35 LNG export/import terminals with scope and type metadata. `unlocode` column (e.g. `NLRTM`) is used by `pipeline/dest_parser.py` to resolve free-text `vessel_state.dest` strings to a terminal_id.
- **`priority_watchlist`** — derived hourly by `pipeline/scoring.py`. One row per LNG/FSRU vessel, ranked into 5 tiers based on current zone proximity / declared destination / activity. The ingester reads this to pick which 150 of ~780 vessels to subscribe to each cycle (100 persistent + 50 scan rotation).
- **`terminal_zones`** — berth, anchorage, and approach polygons (MultiPolygon/4326) linked to terminals. Drawn in QGIS, imported via `make seed-zones`. The `approach` macro-zone contains anchorage + channel + berth as one envelope so a single port visit stays "open" while the vessel transits between them. See `qgis/README.md` for coverage.
- **`port_events`** — derived table, recomputable from `ais_fixes` via `make port-events`. One row per per-vessel transition: `zone_entry → [anchorage_entry → [anchored] → anchorage_exit]* → [moored → departed]? → zone_exit`. `anchorage_entry`/`anchorage_exit` are raw polygon-crossing markers (no dwell, no SOG filter) — bracket every visit to the anchorage polygon, so `queue_time = anchorage_exit - anchorage_entry`. `anchored` is the dwell-confirmed sibling (≥30 min stationary). Combined: presence of `anchorage_entry` answers "did this vessel queue?"; presence of `anchored` answers "did it queue meaningfully?". Each row also carries `terminal_id`, `lat`/`lon` (for great-circle ton-miles downstream), `laden_flag` (forward-filled draught vs `design_draught`), and `cold_start` (vessel already in a polygon at first observation).
- **`ingestion_events`** — append-only lifecycle log (connect / subscribe / planned_reconnect / watchdog_reconnect / disconnect / error). Per-connection liveness is derived from `ingestion_stats_minute` instead of a dedicated heartbeat table.
- **`fixes_per_minute` / `fixes_per_hour`** — TimescaleDB continuous aggregates for the monitoring TUI.

---

## Ingestion

`ingestion/aisstream.py` runs **three parallel WebSocket connections** to AISstream, each subscribed via server-side `FiltersShipMMSI` to a disjoint chunk of up to 50 LNG-carrier MMSIs (AISstream's per-subscription cap). Connections 1+2 (100 slots total) hold the **persistent block** — the top-ranked vessels by tier from `priority_watchlist`. Connection 3 is the **scan rotation** — 50 slots that cycle through the rest of the global fleet, swapping every hour (or sooner on the 5-min silence watchdog). All three reconnect on a 1h planned cadence, immediately after a scoring run re-ranks the watchlist. Source labels in `ais_fixes` and `ingestion_stats_minute`: `aisstream-mmsi-{1,2,3}`. See `ingestion/README.md` for the throttle / filter constraints that drove this design.

`pipeline/scoring.py` (called by the ingester every hour, also runnable via `make scoring`) ranks every LNG/FSRU vessel into one of 5 tiers from current `ais_fixes` + `vessel_state` data. Tier 1 = currently in a `terminal_zones` polygon (3d window); tier 2 = `vessel_state.dest` parses to one of our terminals; tier 3 = inside the wider `config.ZONES` rectangle; tier 4 = recent fix anywhere; tier 5 = stale/unseen. The top 100 (tiers 1-3) get persistent slots; tier 4-5 rotate through the 50 scan slots ordered by `last_scan_window_at`.

`scripts/import_igu_fleet.py` is the one-shot importer that brought the global LNG fleet into `vessel_registry` from the IGU 2025 PDF. Uses VF's VESSELS endpoint to resolve IMO → MMSI (since AISstream's filter takes MMSI only) and to capture a position + dest + ETA snapshot for each vessel.

`ingestion/vesselfinder.py` enriches `vessel_registry` with masterdata from the VesselFinder API — vessel type, DWT, gas capacity, owner/manager, `is_lng_carrier`, `is_fsru`. Runs as a one-shot batch (default `--terminal-only`), rate-limited to one request per second. Vessels with `imo = 0` are sub-IMO and marked `vf_enrichment_status = 'no_imo'` without an API call.

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

# Ingestion
make ingest          # Run AIS ingestion + monitoring TUI
make enrich          # Run VesselFinder enrichment batch
make seed-unlocodes  # Seed terminals.unlocode from db/seed/terminal_unlocodes.sql
make refresh-fleet   # Re-parse the latest IGU PDF and import any newly-listed IMOs

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
