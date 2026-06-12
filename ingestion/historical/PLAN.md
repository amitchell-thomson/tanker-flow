# Historical Backfill Plan

The live ingestion pipeline is production-grade but was only deployed 2026-05-30.
Rather than waiting months for a meaningful modelling corpus, we backfill historical
port events from free/open sources and feed them into the existing `signal_daily`
pipeline.

---

## 1. Data sources

### 1.1 NOAA / Marine Cadastre (US, raw AIS)

| Field       | Detail                                                      |
|-------------|-------------------------------------------------------------|
| Coverage    | Full US coastline — all seven `usgulf` / `usatlantic` terminals |
| Depth       | 2015 to ~1–3 months ago (quarterly release lag)             |
| Granularity | ~1-min intervals for Class A vessels (LNG carriers)         |
| Format      | Zipped CSV by UTM zone × year × month                       |
| Licence     | Public domain (US federal data)                             |
| URL         | marinecadastre.gov/nationalaisdata/                         |
| Cost        | Free                                                        |

**Role in pipeline:** Primary backfill path for the US supply side. Raw fixes go into
`ais_fixes` (source = `'noaa-ais'`), the existing state machine runs over them, and
`port_events` / `legs` / `visits` / `signal_daily` all rebuild normally.

No schema changes required — `ais_fixes.source` already exists and the state machine
is already source-agnostic. The only operational change is that `make port-events` will
run over a much larger `ais_fixes` table (potentially 100M+ rows); the `LAST_FIX_SQL`
comment in `legs.py` flags the query that will need reworking at that point.

### 1.2 GFW AIS Voyages (EU + global, bulk voyage arcs)

| Field       | Detail                                                                |
|-------------|-----------------------------------------------------------------------|
| Coverage    | Global (terrestrial + satellite AIS fused)                            |
| Depth       | 2017 to present                                                        |
| Granularity | Voyage arc: one row per trip (departure anchorage → arrival anchorage) |
| Format      | Bulk download via GFW Data Download Portal (Parquet/CSV)              |
| Licence     | CC BY-SA 4.0 (non-commercial restriction dropped for this dataset)    |
| Cost        | Free with registration                                                 |

Schema (confirmed from GFW Data Portal dataset card):
```
ssvid                 – MMSI (string)
vessel_id             – GFW internal vessel identifier
trip_id               – unique trip ID
trip_start            – UTC timestamp, voyage departure
trip_end              – UTC timestamp, voyage arrival
trip_start_anchorage_id  – GFW anchorage ID at departure
trip_end_anchorage_id    – GFW anchorage ID at arrival
trip_start_visit_id      – GFW port-visit ID at departure
trip_end_visit_id        – GFW port-visit ID at arrival
```

**Role in pipeline:** Provides the inter-terminal voyage arc (departed_ts, arrived_ts,
departure terminal, arrival terminal) for the 2017–2026 window, including EU arrivals
that no free raw AIS can deliver. An adapter maps each voyage row to a pair of
synthetic `port_events` rows (`departed` at the origin terminal, `zone_entry` at the
destination terminal) and writes them directly into `port_events`.

GFW voyage rows give us the leg endpoints but not internal terminal events (anchored,
moored). `laden_flag` is inferred from flow_direction (export terminal departure →
laden = TRUE; ballast return = FALSE) with `laden_source = 'flow_direction'`.
GFW rows must not be rebuilt by `make port-events` — see §3.1.

### 1.3 GFW Events API (EU + global, port visits 2012–present)

| Field       | Detail                                                                |
|-------------|-----------------------------------------------------------------------|
| Coverage    | Global                                                                |
| Depth       | 2012–present (2012–2016: lower satellite density but port zones ok)   |
| Granularity | PORT_VISIT events: start, end, anchorage lat/lon, MMSI, name, type    |
| Format      | REST API, JSON                                                         |
| Licence     | CC BY-SA 4.0                                                           |
| Auth        | Free with GFW registration (Bearer token)                             |

**Role in pipeline:** Fills the 2012–2016 gap before GFW Voyages. For each LNG carrier
MMSI, query `GET /v3/events?event_type=PORT_VISIT&vessel_id=<gfw_id>&confidences=4`
and filter responses by anchorage proximity to our terminal polygons. Each matched
PORT_VISIT becomes a synthetic `moored` + `departed` pair in `port_events`
(`cold_start = TRUE`, `laden_source = 'flow_direction'`).

The `confidences=4` filter requires both a visible entry AND exit in AIS; AIS-dark
berth visits are dropped. This is a known gap — acceptable for the modelling corpus.

### 1.4 GIE ALSI (EU terminal throughput, 2017–present)

| Field       | Detail                                                                    |
|-------------|---------------------------------------------------------------------------|
| Coverage    | 100% of EU27 large-scale LNG import terminals                             |
| Depth       | ~2017 to present (daily, T+2 lag)                                         |
| Granularity | Daily send-out (GWh/day) + storage inventory (GWh) per terminal (EIC code) |
| Format      | REST API (api.gie.eu); `gie-py` Python client                              |
| Licence     | Free with registration; non-commercial?  check TOS before publishing      |
| Cost        | Free with API key                                                          |

All 25 target terminals are covered via `gie-py`'s `ALSITerminal` enum
(Rotterdam Gate, Eemshaven, Zeebrugge, Dunkerque, IoG, South Hook,
Wilhelmshaven 1+2, Brunsbüttel, Mukran, Świnoujście, Klaipėda, Sines, Bilbao,
Huelva, Barcelona, Cartagena, Sagunto, Adriatic LNG, Piombino, Ravenna,
Revithoussa, Alexandroupolis).

**Role in pipeline:** Volume ground-truth for the EU demand side. Does NOT provide
individual cargo events or vessel MMSIs — it is a daily flow aggregate. Goes into a
new `alsi_daily` table rather than `port_events`. The signal layer can use it as
a direct regressor (daily EU regasification rate) alongside the AIS-derived
gas_discharging_eu, or as a calibration denominator for the EU capture rate.

### 1.5 ENTSO-G Transparency Platform (EU LNG flows, cross-check)

| Field       | Detail                                                          |
|-------------|------------------------------------------------------------------|
| Coverage    | 50+ EU LNG entry points                                          |
| Depth       | ~2017–present                                                    |
| Granularity | Daily (kWh/day) per terminal                                     |
| Format      | REST API, JSON/CSV; no auth required                             |
| Licence     | Open                                                             |

**Role in pipeline:** Cross-check vs. ALSI. Stored in `alsi_daily` with `source = 'entsog'`
so the two can be compared per terminal per day. Not a primary signal input — used to
detect ALSI data gaps or corrections.

---

## 2. Schema changes

### 2.1 `port_events.source` column

GFW-derived events live in `port_events` alongside state-machine events but must
survive `make port-events` rebuilds. The rebuild currently does `TRUNCATE port_events`
— changing it to only delete rows where `source IN ('state_machine')` (or NULL)
preserves GFW rows.

```sql
ALTER TABLE port_events
    ADD COLUMN source TEXT NOT NULL DEFAULT 'state_machine';

ALTER TABLE port_events
    ADD CONSTRAINT port_events_source_check
    CHECK (source IN ('state_machine', 'gfw_voyages', 'gfw_events'));
```

`port_events.py` writes `source = 'state_machine'` on all inserts.
The TRUNCATE becomes:

```sql
DELETE FROM port_events WHERE source = 'state_machine';
```

Migration: `db/migrations/versions/<rev>_add_port_events_source.py`

### 2.2 `alsi_daily` table (new)

```sql
CREATE TABLE alsi_daily (
    id          BIGSERIAL PRIMARY KEY,
    terminal_id INTEGER   NOT NULL REFERENCES terminals(terminal_id),
    date        DATE      NOT NULL,
    source      TEXT      NOT NULL CHECK (source IN ('alsi', 'entsog')),
    send_out_gwh_d  NUMERIC,
    inventory_gwh   NUMERIC,
    fetched_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE (terminal_id, date, source)
);
```

Migration: `db/migrations/versions/<rev>_add_alsi_daily.py`

`terminals` will need `eic_code TEXT` and `entsog_key TEXT` columns for the API
joins (add in the same migration or a companion one).

---

## 3. Architecture decisions

### 3.1 Why GFW events write directly to `port_events` (not via `ais_fixes`)

GFW does not expose raw position fixes. Feeding synthetic fixes into `ais_fixes`
and running the state machine over them would produce artefacts (the machine was
designed for continuous dense position streams, not two-point arcs). Direct
port_events insertion is cleaner and semantically correct — we know the vessel was
at a terminal; we just set `cold_start = TRUE` to mark synthetic provenance.

Consequence: `make port-events` must not destroy GFW rows. The `source` column
(§2.1) is the guard.

### 3.2 Why NOAA goes via `ais_fixes` (not direct to `port_events`)

NOAA has sub-minute density — the full continuous position stream. Running it
through the existing state machine gets us all event subtypes (`anchored`,
`moored`, `anchorage_entry/exit`) and the laden inference from draught. Skipping
the state machine would discard this richness. The state machine is already pure
and source-agnostic.

### 3.3 `laden_flag` for GFW-derived events

GFW provides vessel type and the terminal's zone is known — so `laden_source =
'flow_direction'` is the right choice:
- departure from a US export terminal → laden = TRUE
- departure from an EU import terminal → laden = FALSE

This is the same logic already used by `port_events.py` for `laden_source =
'flow_direction'` events today.

### 3.4 Regime tagging

`port_events.regime` is a GENERATED column keyed on `event_time`. Historical events
will all land in `regime = 'bbox'` (pre-2026-05-30). `signal.py` already handles
the regime seam correctly (SIGNALS.md §0.5). No changes needed.

`regime` captures the live-pipeline collection method (bbox throttling vs MMSI filter).
It does NOT distinguish NOAA backfill from AISstream-bbox events, even though their
coverage properties differ (NOAA: 100% Class A, no watchlist bias; AISstream-bbox:
~10–40% per-vessel visibility due to throttling). Use `source` for that split in
any model that needs to condition on data-collection quality within the bbox window.

### 3.5 EU queue time is not a meaningful gap

LNG carriers lose cargo continuously to boil-off (~0.1–0.15% per day at cryogenic
temperature). A laden vessel waiting at anchorage off Rotterdam is literally burning
product. Operators therefore pre-coordinate berthing windows with terminal schedulers
and arrive as close to their slot as possible. EU import terminal queue times are
structurally near-zero in normal market conditions — not a signal, a scheduling
artefact.

The US export side is the opposite: vessels arrive ballast (no cargo at risk) and
queue at the sea buoy for loading arm slots. Sabine Pass, Freeport, and Calcasieu
Pass each have single-lane approach channels and finite loading arm capacity. Berth
queue depth at US export terminals IS a meaningful leading indicator: a stack of
carriers waiting to load signals delayed departures and tightening in-transit supply
~14–18 days later in Europe.

Consequence: the `anchorage_entry → moored` queue-time signal (planned but deferred,
#6/#12 in SIGNALS.md) matters primarily for US terminals and is fully derivable from
NOAA historical data. Not having EU raw AIS historically is therefore barely a gap
for queue-time analysis. Stress events (e.g. 2022 post-Ukraine EU congestion) show
up better in ALSI inventory drawdown than in AIS queue depth anyway.

### 3.6 Vessel registry matching

NOAA fixes carry MMSI directly — no lookup needed. GFW events carry `ssvid` (=MMSI)
and `vessel_id`. The loader must JOIN GFW events against `vessel_registry` on MMSI
to get `terminal_id` from the anchorage lat/lon and to filter to LNG carriers only.
Vessels not in the registry (not yet enriched) are skipped.

---

## 4. File structure

```
ingestion/historical/
├── PLAN.md                  ← this file
├── __init__.py
├── noaa_ais.py              ← NOAA bulk-CSV loader → ais_fixes
├── gfw_voyages.py           ← GFW AIS Voyages bulk adapter → port_events
├── gfw_events.py            ← GFW Events API adapter → port_events
└── alsi.py                  ← GIE ALSI + ENTSO-G loader → alsi_daily
```

The existing pipeline (`port_events.py`, `legs.py`, `signal.py`) is unchanged;
it just reads a richer `port_events` table.

Makefile targets to add:
```makefile
backfill-noaa       # Download + load a year of NOAA UTM-zone CSVs into ais_fixes
backfill-gfw        # Load GFW Voyages parquet + backfill GFW Events API gap (2012-17)
load-alsi           # Incremental ALSI + ENTSO-G fetch into alsi_daily
load-alsi-full      # Full historical backfill (--full flag)
```

---

## 5. Build order

The phases below are ordered by value-per-effort and dependency. Do not start a
phase until the previous phase is committed and tests pass.

**Phase 1 — Schema + NOAA US backfill (highest leverage)**

1. Alembic migration: add `port_events.source` column; update `port_events.py`
   TRUNCATE to `DELETE WHERE source = 'state_machine'`.
2. Write `noaa_ais.py`: download UTM-zone zips for target years/months, parse CSV
   columns (MMSI, BaseDateTime, LAT, LON, SOG, COG, Heading, VesselName, IMO,
   CallSign, VesselType, Status, Length, Width, Draft, Cargo, TransceiverClass),
   filter to `VesselType IN (80..89)` (tankers) + registry-join on MMSI/IMO, batch
   upsert into `ais_fixes` with `source = 'noaa-ais'`.
3. Run `make port-events` over NOAA + live data combined → verify state machine
   produces sensible events at Sabine/Freeport for 2022–2025.
4. Run `make signals` → confirm `signal_daily` now has pre-2026 history for
   `gas_loading_us` / `gas_in_transit_volume`.

**Phase 2 — GFW Voyages (EU arrivals, 2017+)**

1. Alembic migration: add `alsi_daily` table + `terminals.eic_code` /
   `terminals.entsog_key` columns.
2. Write `gfw_voyages.py`: read downloaded Parquet, filter to LNG carrier MMSIs
   (registry join), match `trip_start_anchorage_id` / `trip_end_anchorage_id`
   against a pre-built GFW anchorage-id → terminal_id mapping (proximity lookup
   against `terminals.lat/lon`), emit synthetic `departed` + `zone_entry` events
   into `port_events` with `source = 'gfw_voyages'`.
3. Run `make signals` → confirm EU arrival signals appear in `signal_daily`.

**Phase 3 — GFW Events API (2012–2016 gap fill)**

1. Write `gfw_events.py`: authenticate with Bearer token, batch `vessel_id` lookup
   for all LNG carrier MMSIs, fetch PORT_VISIT events with `confidences=4`, filter
   by anchorage proximity to terminal polygons, emit `moored` + `departed` pairs
   with `source = 'gfw_events'`.
2. Run `make signals` → extend history to 2012.

**Phase 4 — GIE ALSI + ENTSO-G (EU volume ground-truth)**

1. Write `alsi.py` using `gie-py` to fetch daily `send_out` + `inventory` per
   terminal, upsert into `alsi_daily`.
2. Add ENTSO-G fetcher for cross-check rows (`source = 'entsog'`).
3. Expose `alsi_daily` in `viz/app.py` for visual spot-check.

---

## 6. Key open questions

1. **NOAA UTM zone mapping**: Which UTM zones cover the `usgulf` / `usatlantic`
   terminals? Need a zone-to-terminal mapping so we only download the relevant
   files (US Gulf is UTM 15–16; US Atlantic is UTM 17–18). Confirm at
   marinecadastre.gov/nationalaisdata/ before downloading.

2. **GFW anchorage IDs → terminal_id**: GFW's anchorage registry (`anchorages.csv`
   or via API) needs a one-time mapping to `terminals.terminal_id`. Build a
   proximity lookup (haversine < threshold km) + manual review for ambiguous cases
   (Rotterdam, Barcelona port clusters).

3. **GFW registration**: Requires account + Data Use Agreement at
   globalfishingwatch.org/data-download. The AIS Voyages dataset may need a
   separate request form beyond basic registration.

4. **GFW Voyages download size**: Full global file covers all vessel types; likely
   several GB. Filter early to `vessel_type = 'BUNKER_OR_TANKER'` or to the LNG
   carrier MMSI list before loading into memory.

5. **`LAST_FIX_SQL` rework timing**: The `DISTINCT ON` query in `legs.py` (noted
   with a comment) will full-scan the hypertable once NOAA rows land. Rework to a
   per-MMSI MAX subquery before Phase 1 `make signals` to avoid OOM.

6. **ALSI TOS check**: Confirm GIE API licence permits use in a commercially-oriented
   research/portfolio context before publishing any ALSI-derived output.
