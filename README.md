# tanker-flow

Derives a leading Henry Hub / TTF spread signal from live LNG carrier positions. AIS vessel fixes are ingested in real time, classified against terminal zone polygons, and aggregated into laden ton-miles in transit — a forward-looking proxy for LNG trade flows between US export terminals and NW European import terminals.

---

## Architecture

```
AISstream WebSocket ──► ingestion/aisstream.py ──► ais_fixes        (TimescaleDB hypertable)
VesselFinder API    ──► ingestion/vesselfinder.py ► vessel_registry  (masterdata enrichment)
                                                    terminals         (40 LNG terminals)
                                                    terminal_zones    (berth + anchorage + approach polygons)
ais_fixes           ──► pipeline/port_events.py ──► port_events      (per-visit state machine)
port_events         ──► pipeline/signal.py ────────► laden_ton_miles  [not yet implemented]
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
- **`vessel_registry`** — one row per MMSI, enriched with VesselFinder masterdata. `is_lng_carrier` and `is_fsru` flags drive vessel filtering.
- **`terminals`** — 40 LNG export/import terminals with scope and type metadata.
- **`terminal_zones`** — berth, anchorage, and approach polygons (MultiPolygon/4326) linked to terminals. Drawn in QGIS, imported via `make seed-zones`. The `approach` macro-zone contains anchorage + channel + berth as one envelope so a single port visit stays "open" while the vessel transits between them. See `qgis/README.md` for coverage.
- **`port_events`** — derived table, recomputable from `ais_fixes` via `make port-events`. One row per per-vessel transition: `zone_entry → [anchorage_entry → [anchored] → anchorage_exit]* → [moored → departed]? → zone_exit`. `anchorage_entry`/`anchorage_exit` are raw polygon-crossing markers (no dwell, no SOG filter) — bracket every visit to the anchorage polygon, so `queue_time = anchorage_exit - anchorage_entry`. `anchored` is the dwell-confirmed sibling (≥30 min stationary). Combined: presence of `anchorage_entry` answers "did this vessel queue?"; presence of `anchored` answers "did it queue meaningfully?". Each row also carries `terminal_id`, `lat`/`lon` (for great-circle ton-miles downstream), `laden_flag` (forward-filled draught vs `design_draught`), and `cold_start` (vessel already in a polygon at first observation).
- **`ingestion_events`** — append-only lifecycle log (connect / subscribe / planned_reconnect / watchdog_reconnect / disconnect / error). Per-connection liveness is derived from `ingestion_stats_minute` instead of a dedicated heartbeat table.
- **`fixes_per_minute` / `fixes_per_hour`** — TimescaleDB continuous aggregates for the monitoring TUI.

---

## Ingestion

`ingestion/aisstream.py` subscribes to seven AISstream WebSocket bounding boxes (US Gulf, US Atlantic, Iberian Atlantic, NW Europe, Baltic, W Mediterranean, E Mediterranean) and writes `PositionReport` and `ShipStaticData` messages for vessel types 80–89 (tankers). Maintains a connection pool with 30s/60s reconnect backoff.

`ingestion/dynamic_enrichment.py` watches incoming fixes during ingestion and queues an MMSI for VesselFinder lookup the first time it appears inside a `terminal_zones` polygon — enrichment is zone-gated, not lazy, so pending rows outside terminals are noise rather than backlog.

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

# Pipeline
make port-events     # Recompute port_events from ais_fixes + terminal_zones (idempotent)

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
