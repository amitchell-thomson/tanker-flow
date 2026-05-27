# tanker-flow

Derives a leading Henry Hub / TTF spread signal from live LNG carrier positions. AIS vessel fixes are ingested in real time, classified against terminal zone polygons, and aggregated into laden ton-miles in transit — a forward-looking proxy for LNG trade flows between US export terminals and NW European import terminals.

---

## Architecture

```
AISstream WebSocket ──► ingestion/aisstream.py ──► ais_fixes        (TimescaleDB hypertable)
VesselFinder API    ──► ingestion/vesselfinder.py ► vessel_registry  (masterdata enrichment)
                                                    terminals         (40 LNG terminals)
                                                    terminal_zones    (berth + anchorage polygons)
ais_fixes           ──► pipeline/port_events.py ──► port_events      [not yet implemented]
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

- **`ais_fixes`** — append-only raw position fixes, partitioned by day. Sources: `aisstream` (real-time) and `vesselfinder` (weekly reconciliation).
- **`vessel_state`** — voyage-specific fields (draught, destination, ETA) from `ShipStaticData` messages.
- **`vessel_registry`** — one row per MMSI, enriched with VesselFinder masterdata. `is_lng_carrier` and `is_fsru` flags drive vessel filtering.
- **`terminals`** — 40 LNG export/import terminals with scope and type metadata.
- **`terminal_zones`** — berth and anchorage polygons (MultiPolygon/4326) linked to terminals. Drawn in QGIS, imported via `make seed-zones`. See `qgis/README.md` for coverage.
- **`port_events`** — derived table (recomputable from `ais_fixes`). Zone entries, anchorings, moorings, departures.
- **`ingestion_heartbeat`** — one row per ingestion source, upserted every 10s for health monitoring.
- **`fixes_per_minute` / `fixes_per_hour`** — TimescaleDB continuous aggregates for the monitoring TUI.

---

## Ingestion

`ingestion/aisstream.py` subscribes to two AISstream WebSocket bounding boxes (US Gulf, NW Europe) and writes `PositionReport` and `ShipStaticData` messages for vessel types 80–89 (tankers). Maintains a connection pool with 30s/60s reconnect backoff.

`ingestion/vesselfinder.py` enriches `vessel_registry` with masterdata from the VesselFinder API — vessel type, DWT, gas capacity, owner/manager. Runs as a one-shot batch, rate-limited to one request per second.

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

# Viz
make viz             # Start FastAPI monitoring web app (localhost:8000)
```

---

## What is not yet implemented

| Component | Description |
|---|---|
| `pipeline/port_events.py` | State machine: classifies `ais_fixes` against `terminal_zones` to produce `port_events` |
| `pipeline/signal.py` | Aggregates `port_events` into laden ton-miles in transit by zone pair |
| `data/eia.py` | Pulls US natural gas storage data from the EIA API (Henry Hub fundamentals) |
| `analysis/` | Spread prediction model and exploratory notebooks |
| US Atlantic AIS box | Third bounding box for Cove Point / Elba Island (`usatlantic` zone) not yet wired into `aisstream.py` |
| Vessel enrichment | `vessel_registry` populated (4,441 vessels) but VesselFinder enrichment not yet run — no `is_lng_carrier` flags set |
