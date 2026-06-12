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
| Depth       | 2015 available, but **start at 2016** — see note below       |
| Granularity | ~1-min intervals for Class A vessels (LNG carriers)         |
| Format      | **Nationwide daily zipped CSV** (`AIS_YYYY_MM_DD.zip`) — *verified 2026-06* |
| Licence     | Public domain (US federal data)                             |
| URL         | `https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{YYYY}/AIS_{YYYY}_{MM}_{DD}.zip` |
| Volume      | ~300 MB/zip, ~900 MB/day decompressed, **~108 GB/yr** (all vessels) |
| Cost        | Free                                                        |

> **Plan correction (verified 2026-06, resolves open Q#1).** The current
> MarineCadastre layout is **nationwide daily files**, not the old UTM-zone ×
> month split (that was the 2009–2017 geodatabase era). So there is **no UTM-zone →
> terminal mapping to build** — open Q#1 is moot. The trade-off is volume: each day
> is one ~900 MB nationwide CSV of *all* vessel types, so 2016→now is ~1.1 TB of
> *downloads* (transient — we fetch and decompress it but never store it whole).
> `noaa_ais.py` streams each daily CSV into the **two-tier load of §3.8**: all
> tankers (`VesselType 80–89`) to a compressed Parquet archive on disk (~tens of GB,
> the density source + "download once" insurance), and only the LNG-in-terminal-
> buffer slice into `ais_fixes`. See §6 open Q#1 for the where-to-run-it decision.

**Role in pipeline:** Primary backfill path for the US supply side. Raw fixes go into
`ais_fixes` (source = `'noaa-ais'`), the existing state machine runs over them, and
`port_events` / `legs` / `visits` / `signal_daily` all rebuild normally.

No schema changes required — `ais_fixes.source` already exists and the state machine
is already source-agnostic. The only operational change is that `make port-events` will
run over a much larger `ais_fixes` table (potentially 100M+ rows); the `LAST_FIX_SQL`
comment in `legs.py` flags the query that will need reworking at that point.

**Start the backfill window at 2016, not 2015.** US LNG exports effectively begin
with **Sabine Pass cargo #1 in February 2016** — before that the US Gulf export
terminals had no laden departures to observe, so 2015 NOAA data contains no US
supply-side signal. Backfilling from 2016 captures the entire US export era from
its origin (you watch new terminals — Calcasieu Pass 2022, Plaquemines 2023 — light
up from zero, which is signal, not noise) and saves a year of downloads with
nothing in them. 2016 is the floor for the NOAA US backfill.

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

### 3.4 Regime tagging — `regime` must become source-aware (a required change)

`port_events.regime` is today a STORED GENERATED column keyed purely on
`event_time` (`'bbox'` before the 2026-05-30 09:27 UTC cutover, `'mmsi_filter'`
after). Under backfill this is **wrong**: every NOAA-historical event is
pre-cutover, so it would land in `regime = 'bbox'` — tagging the *cleanest*
source (exhaustive Class A, no throttle) as if it were the throttled bbox block.
`signal.py`'s "never aggregate across the seam" segmentation keys on `regime`, so
it would silently lump clean NOAA with throttled bbox. This is not "no changes
needed" — it is the central correctness change for the backfill.

**`regime` must tag *fidelity*, not just calendar.** Redefine the generated
column to be source-aware (the `source` column from §2.1 makes this possible):

```sql
regime TEXT GENERATED ALWAYS AS (
    CASE
        WHEN source = 'noaa-ais'                  THEN 'noaa'         -- exhaustive Class A, US, 2015+
        WHEN source IN ('gfw_voyages','gfw_events') THEN 'gfw'        -- arc-fidelity, endpoints only
        WHEN event_time < TIMESTAMPTZ '2026-05-30 09:27:00+00' THEN 'bbox'
        ELSE 'mmsi_filter'
    END) STORED
```

Why these four values capture the real data-generating processes:

| `regime` | Source | Span | Fidelity |
|---|---|---|---|
| `noaa` | NOAA bulk CSV | 2015–~1–3 mo ago, US | exhaustive Class A, draught present — **highest** |
| `gfw` | GFW Voyages / Events | 2017–present (2012+ events), EU+global | voyage-arc: endpoints only, no internal events, no draught |
| `bbox` | AISstream throttled | 2026-04-14 → 05-30 | ~23% per-vessel capture — **lowest** |
| `mmsi_filter` | AISstream MMSI filter | 2026-05-30 → present | near-100% capture for subscribed LNG MMSIs |

**Consequence — the US-side seam disappears.** NOAA coverage runs to ~1–3 months
ago, so it retroactively *overwrites* the throttled `bbox` dates on the US side.
`noaa` and `mmsi_filter` are the **same fidelity** for US events (NOAA exhaustive;
the MMSI filter subscribes to every LNG MMSI), so the US export series
(`gas_loading_us`, #6–11) concatenates into one continuous, seam-free **2015→now**
line. The 2026-05-30 obstacle that shaped `MODELS.md` is a US-side non-issue once
NOAA lands. The **EU side keeps a fidelity step** at the `gfw → mmsi_filter`
boundary (arc-fidelity → full-fidelity) — that one is real and must enter any EU
model as a regime indicator.

**Code ripple (do this in Phase 1, before any `make signals`):**
- `config.regime_of(ts)` currently takes only a timestamp. It must become
  `regime_of(ts, source)` to mirror the new generated column. Callers:
  `pipeline/legs.py:209`, `pipeline/visits.py:99` (both already have the event's
  `source` in scope once `port_events.source` exists).
- `signal_daily.regime`'s CHECK constraint (`IN ('bbox','mmsi_filter','all')`)
  must widen to include `'noaa'` and `'gfw'`.
- `signal.py`'s segmentation generalises from "never cross the 2026-05-30 seam" to
  the **multi-fidelity rule**: only concatenate series of equal fidelity; render
  every fidelity change as a visible discontinuity (US `noaa`⧺`mmsi_filter` =
  continuous; EU `gfw`→`mmsi_filter` = a modelled level-shift, never a blend).

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

### 3.6 Vessel identity — key on IMO, not MMSI; admit historical hulls

The live pipeline resolves vessels by MMSI against `vessel_registry`. For the
backfill that rule breaks twice over a decade, so **historical identity is
IMO-keyed and self-sufficient from the source's own fields** — never a plain
MMSI-join against the live registry:

- **MMSI is reused.** Over 2016–2026 an MMSI is reassigned when a hull is scrapped
  or re-flagged, so a 2017 NOAA fix joined to the 2026 registry *on MMSI* can
  attach to the wrong vessel. IMO is the stable, lifetime identifier. NOAA CSVs
  carry `IMO` directly; GFW carries a stable `vessel_id` (and `ssvid`=MMSI). Resolve
  and dedup historical vessels on **IMO** (GFW: via `vessel_id`), using MMSI only as
  the join key *within a known time window* once IMO has pinned the hull.
- **The registry was populated only since 2026-04**, so "skip MMSIs not in the
  registry" (the live rule) would **drop most of the 2016–2024 fleet** — scrapped
  carriers, and hulls VF no longer returns. The historical loaders must instead
  **create/upsert `vessel_registry` rows from the source's own identity fields**
  (NOAA: `IMO` + `VesselName` + `VesselType`; GFW: `vessel_id` + type), keyed on
  IMO, admitting any LNG carrier the source identifies — not gated on prior live
  enrichment.
- **Weights for un-enrichable hulls.** Scrapped vessels VF can't enrich won't have
  `gas_capacity_m3`; fall back to a DWT-derived estimate (or fleet-mean) so they
  still contribute to the volume signals. `laden_source` and the §3.7 dedup are
  unaffected.

GFW events/voyages still resolve `terminal_id` from the anchorage lat/lon
(proximity to `terminals`); the change here is purely *which identifier* keys the
vessel and that unknown historical hulls are **admitted, not skipped**.

### 3.7 NOAA ⋈ GFW reconciliation — do NOT double-count US departures

This is the highest-priority signal-correctness rule and the reason for a new
`reconcile.py` (§4). A US→EU laden voyage has its `departed` produced **twice**:
once by the NOAA state machine (real fix stream) and once as a synthetic event by
the GFW Voyages adapter (`trip_start` → synthetic `departed`). If both land in
`port_events`, `legs.py` pairs the leg **twice** — NOAA-`departed` → GFW-`zone_entry`
*and* GFW-`departed` → GFW-`zone_entry` — and **`gas_in_transit_volume` doubles for
every US-origin laden leg.**

The two sources are **complementary halves of one leg**, not redundant copies:
NOAA owns the clean *departure* endpoint (with draught → real laden), GFW owns the
*arrival* endpoint (EU terminals no free raw AIS can deliver). The reconciliation
rule:

- **GFW Voyages contributes only the endpoint NOAA cannot see.**
  - **US-origin legs:** keep the NOAA `departed`; emit **only** the GFW `zone_entry`
    (EU arrival). Suppress the GFW `departed`.
  - **Legs with no NOAA-covered endpoint** (EU→EU, non-US origins): emit the full
    synthetic `departed` + `zone_entry` pair as today.
- **Match key:** GFW `trip_start` ↔ NOAA `departed` on `(mmsi,
  |Δt| ≤ MATCH_TOLERANCE_HOURS, same origin terminal)`. A match suppresses the GFW
  `departed`.
- **Free QC cross-check:** where both sources see a US departure, NOAA's
  draught-laden vs GFW's flow_direction-laden should agree — a disagreement flags a
  bad draught read or a mis-typed terminal.

This reconciliation is the leg-level reason the headline `gas_in_transit_volume`
gets clean pre-2026 history (NOAA departure ⋈ GFW arrival = a fully-observed leg),
*provided* the dedup runs. Without it, the same history is inflated 2×.

### 3.8 Two-tier load — full tanker archive on disk, lean LNG slice into `ais_fixes`

The download is ~1.1 TB regardless (NOAA serves whole daily files, no server-side
filter — §1.1), so the only choice is what we *keep*. We keep **all of it that is a
tanker**, in two tiers with different purposes:

**Tier 1 — archive (Parquet on disk, all tankers nationwide).** `noaa_ais.py`
streams each daily CSV, keeps every `VesselType 80–89` (tanker) fix nationwide, and
writes it to a compressed Parquet archive (partitioned by year/month). This is the
**"download once" insurance** (a later-discovered or misclassified LNG hull is
re-filtered from disk, never re-downloaded) and the **density-plot source** (a
historical US tanker-density raster — `density.js` / `/api/fix-density` reads it
directly). Tankers are a minority of position rows and AIS compresses ~5–10× as
columnar Parquet, so the decade archive is ~tens of GB (estimate — confirm with one
sample file), not a TB. Not in TimescaleDB.

**Tier 2 — pipeline (`ais_fixes`).** All **LNG-carrier** fixes (registry-resolved by
IMO, §3.6), US-coastal, load into the hypertable. *Positions* are admitted without a
spatial gate so the **density map shows the full US shipping lanes**, not just blobs
at the terminals (the earlier 50 km gate was dropped on request — the lanes are the
point of the density view). Mid-Gulf / approach fixes match no terminal polygon, so
the state machine resolves them to open-ocean TRANSIT and they produce **no
`port_events`** — they never touch the signal, they only cost rebuild time. *Draught*
(`vessel_state`), by contrast, stays **within ~50 km of a terminal** — laden
inference only happens at berths, so nationwide draught is pointless bloat.

> *Cost & mitigation:* admitting all positions ~4×'s the historical `ais_fixes`
> (decade ≈ 53M vs ~13M) and the hourly `port_events` rebuild scans them. If that
> rebuild gets slow, add a near-terminal pre-filter to the state-machine query
> (`pipeline/port_events.py` `SPATIAL_JOIN_SQL`) so the full set stays in `ais_fixes`
> for density while the state machine only walks near-terminal fixes.

Re-filtering Tier 1 → Tier 2 (a widened filter, or a missed LNG hull) is a local
Parquet scan + insert — no network, no re-download: `make backfill-noaa-reload
START=.. END=..` (`reload_archive` / `--reload`).

---

## 4. File structure

```
ingestion/historical/
├── PLAN.md                  ← this file
├── __init__.py
├── noaa_ais.py              ← NOAA bulk-CSV loader → ais_fixes (spatially pre-filtered, §3.8)
├── gfw_voyages.py           ← GFW AIS Voyages bulk adapter → port_events
├── gfw_events.py            ← GFW Events API adapter → port_events
├── reconcile.py             ← NOAA ⋈ GFW dedup (§3.7): suppress GFW departed where NOAA covers it
└── alsi.py                  ← GIE ALSI + ENTSO-G loader → alsi_daily
```

The existing pipeline (`port_events.py`, `legs.py`, `signal.py`) is unchanged
*except* for the `regime` source-awareness in §3.4 (`config.regime_of` signature +
generated column + `signal.py` segmentation); it otherwise just reads a richer
`port_events` table.

### 4.1 Signal fidelity by source — which signals get real history

The fidelity matrix below drives the build order: it says which signals get deep
history (build/validate the model on them now) and which begin only at the live
cutover (no historical training set). Mirrored in `SIGNALS.md` §0.6.

| Signal family | `noaa` (US) | `gfw` (EU) | `mmsi_filter` (live) |
|---|---|---|---|
| US loadings #9–11, `gas_loading_us` | ✅ full, 2015+ | — | ✅ |
| US queue/berth #6–8 | ✅ full, 2015+ | — | ✅ |
| EU arrivals #4, `gas_discharging_eu` (count) | — | ✅ count, 2017+ | ✅ |
| EU berth-amortized `gas_discharging_eu` | — | ⚠️ no real dwell → estimate from terminal mean | ✅ |
| EU queue #12–16 | — | ❌ no `anchorage_entry` | ✅ |
| In-transit #1/#2, `gas_in_transit_volume` | ⚠️ departure only | ⚠️ arrival only | ✅ |
| **#1 reconstructed: NOAA dep ⋈ GFW arr (§3.7)** | **✅ full leg, 2017+** | | ✅ |
| Voyage age #20 | ✅ dep obs | arr obs | ✅ |
| laden source | ✅ draught | ❌ flow_direction | ✅ draught |

Reading: the US supply side (loadings, queue, berth, `gas_loading_us`) gets a
clean, seam-free decade — train/validate the §1–§3 `MODELS.md` physical nowcasts
on it freely. EU queue/berth (#12–16) gets **no history** (GFW arcs can't produce
`anchorage_entry`); §3.5 already argues EU queue is structurally near-zero, so
this is tolerable, but those signals begin only at the live cutover. The headline
in-transit volume gets full history *only via the §3.7 reconciliation*.

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

1. Alembic migration: add `port_events.source` column; **redefine the `regime`
   generated column to be source-aware (§3.4)** and widen `signal_daily.regime`'s
   CHECK to include `'noaa'`/`'gfw'`; update `config.regime_of` to
   `regime_of(ts, source)` and its two callers (`legs.py:209`, `visits.py:99`);
   update `port_events.py` TRUNCATE to `DELETE WHERE source = 'state_machine'`.
2. Write `noaa_ais.py`: download UTM-zone zips for **2016→present** (the US LNG
   export era — §1.1), parse CSV columns (MMSI, BaseDateTime, LAT, LON, SOG, COG,
   Heading, VesselName, IMO, CallSign, VesselType, Status, Length, Width, Draft,
   Cargo, TransceiverClass), filter to `VesselType IN (80..89)` (tankers), resolve
   vessel identity **by IMO** and upsert any LNG hull into `vessel_registry`
   (§3.6 — admit historical hulls, don't skip), batch upsert into `ais_fixes` with
   `source = 'noaa-ais'`.
   - **2a. Spatial pre-filter (§3.8):** keep only fixes within ~50 km of a US
     `terminal_zones` polygon before insert — keeps `ais_fixes` lean and makes the
     `LAST_FIX_SQL` rework (open Q #5) unnecessary.
3. Run `make port-events` over NOAA + live data combined → verify state machine
   produces sensible events at Sabine/Freeport for 2022–2025, and that NOAA events
   carry `regime = 'noaa'` (not `'bbox'`).
4. Run `make signals` → confirm `signal_daily` now has pre-2026 history for
   `gas_loading_us`, and that the US series concatenates `noaa`⧺`mmsi_filter`
   seam-free (the `bbox` window is NOAA-overwritten).

**Phase 2 — GFW Voyages (EU arrivals, 2017+)**

1. Alembic migration: add `alsi_daily` table + `terminals.eic_code` /
   `terminals.entsog_key` columns.
2. Write `gfw_voyages.py`: read downloaded Parquet, filter to LNG carrier MMSIs
   (registry join), match `trip_start_anchorage_id` / `trip_end_anchorage_id`
   against a pre-built GFW anchorage-id → terminal_id mapping (proximity lookup
   against `terminals.lat/lon`), emit synthetic `departed` + `zone_entry` events
   into `port_events` with `source = 'gfw_voyages'`.
3. **Write `reconcile.py` (§3.7) and run it before `make signals`:** suppress the
   GFW `departed` for any US-origin leg whose departure NOAA already covers
   (match on `mmsi` + `|Δt|` + origin terminal), so `legs.py` pairs each US→EU leg
   once. Without this the in-transit volume doubles.
4. Run `make signals` → confirm EU arrival signals appear, and that
   `gas_in_transit_volume` is **not** doubled vs the Phase-1 US-only baseline (the
   reconciliation invariant — a quick `n_legs` sanity check on the US→EU lane).

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

1. **NOAA download location — the where-to-run-it decision** *(open; replaces the
   now-resolved UTM-zone question — see §1.1: the layout is nationwide daily files,
   so there is no UTM-zone mapping to build)*. 2016→now is ~1 TB of daily
   nationwide CSVs to fetch and decompress (we keep only the §3.8-filtered tankers).
   Three places to run it: **(a)** the home box (simplest; ~1 TB over home
   broadband ties up bandwidth for days); **(b)** the existing Oracle worker-1 VM
   (ingress is free, streams one file at a time, runs unattended — good fit); **(c)**
   a temporary larger cloud instance, ideally in the same region as the source if
   it is mirrored to an S3/GCS open-data bucket, so the filter runs *next to the
   data* and only the tiny result egresses. **Recommended phasing:** validate the
   loader on a **single year first (2022 — it contains the Freeport outage, a known
   labelled test event)**, which runs anywhere, before committing infra to the full
   2016→now sweep.

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

5. **`LAST_FIX_SQL` rework timing** — *largely resolved by §3.8.* The `DISTINCT ON`
   query in `legs.py` would full-scan the hypertable once NOAA rows land, but the
   §3.8 spatial pre-filter keeps `ais_fixes` 1–2 orders of magnitude smaller than
   the naive load, so the scan stays tractable. Re-measure after Phase 1; only
   rework to a per-MMSI MAX subquery if the pre-filtered table still strains it.

6. **ALSI TOS check**: Confirm GIE API licence permits use in a commercially-oriented
   research/portfolio context before publishing any ALSI-derived output.
