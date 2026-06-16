# Signals

The market-signal catalogue derived from LNG carrier positions. Every signal is a
leading indicator of the **Henry Hub / TTF spread**: the spread *widens* when US gas
backs up (slow exports, full storage) or Europe tightens (cold, low storage); it
*narrows* when US LNG reaches Europe quickly and in volume. Each signal is one of a
**supply pulse** (S, US export pace), a **demand pulse** (D, EU absorption), an
**arbitrage indicator** (A, where the marginal cargo goes), or an **inventory proxy**
(I, gas on-water or in queue).

This document is the reference for **what is built**. The modelling layer that turns
these into a spread forecast lives in [`MODELS.md`](MODELS.md).

---

## 1 · The substrate

```
ais_fixes ─► port_events ─►┬─ legs.py    (departed → next zone_entry : at-sea voyages)
 (+ NOAA/GFW backfill)     ├─ visits.py  (moored → departed         : berth occupancy)
                           └─ queues.py  (anchorage_entry → moored   : the wait to berth)
                                  └─► signal.py ─► signal_daily (the panel)
```

`signal.py` **aggregates** the three pairings into `signal_daily` (a tidy/long daily
panel, rebuilt TRUNCATE+swap by `make signals`). It never re-pairs. One row per
`(signal_key, bucket_date, zone_scope, regime, basis)`; `value` plus the metadata
columns below.

Coverage by source (sets which signals get history — see §2.1):
- **`noaa`** — exhaustive US Class-A terrestrial, 2016+ (the US ground truth; tracks EIA exports to a few %).
- **`gfw`** — Global Fishing Watch voyage arcs, EU + global, 2016+ (port visits only; **no coordinates, no anchorage events**).
- **`bbox`** / **`mmsi_filter`** — the live feed before / after the 2026-05-30 cutover.

---

## 2 · The three dimensions every signal carries

These are not signal variants — they are *which information set / fidelity / quality*
each value was computed under. Getting them right is what makes the panel safe to
model on.

### 2.1 · `regime` — the fidelity seam (never blend across it)

The ingestion scheme changed in a hard cutover at **2026-05-30 09:27 UTC**, and the
backfill adds two more data-generating processes. `port_events.regime` is a stored
column tagging each event's fidelity:

| regime | source | missingness |
|---|---|---|
| `noaa` | NOAA backfill | exhaustive (US) |
| `gfw` | GFW backfill | arc-fidelity (EU + global) |
| `bbox` | live, pre-cutover | stochastic (throttle drops vessels at random) |
| `mmsi_filter` | live, post-cutover | systematic (only watchlisted vessels) |

Each `signal_daily` row is tagged by the **item's** regime (fixed at departure/mooring),
plus a synthetic **`all`** row. **Rule: only concatenate series of equal fidelity; a
model must never train across a fidelity change** — segment by `regime`, or carry it
as an indicator. The US side (`noaa` + `mmsi_filter`) is one fidelity and forms a
clean 2016→now line; the EU side keeps a real `gfw → mmsi_filter` step.

> **`all` double-counts US signals — use `regime='noaa'` for US ground truth.** NOAA
> and GFW overlap at US terminals; `reconcile.py` dedups most but a residual GFW
> false-positive tail remains. For `gas_loading_us` and the US-origin leg of
> `gas_in_transit_volume`, read `noaa`, not `all`.

### 2.2 · `basis` — `physical` vs `knowable` (the leakage guard)

Same events, different clock:

- **`physical`** — "what was on the water on day `d`", computed with everything we
  know **today**. Embeds hindsight (final leg classification, observed arrival
  endpoints, NOAA's later republication). **Validation only — never train on it.**
- **`knowable`** — the value the pipeline **would have printed live on `d`** using
  only what was knowable by `d` (an open leg stays open; an arrival is counted only
  once observed; an overdue leg is censored at its voyage window). **The only
  leakage-safe model input.** [`MODELS.md`](MODELS.md) consumes `knowable` only.

Both bases are built every rebuild. For *closed-item* measurements (a completed
voyage's speed, a finished berth's turn-time) the two coincide by construction. They
diverge for stocks with open/un-arrived items — which is exactly where leakage hides.

A `signal_daily_live_vintage` table logs the live-regime values **as printed each
day**, so a later `knowable[d]` recompute can be checked against what was actually
emitted on `d` (the self-validation acceptance test; accrues with the live tail).

### 2.3 · `confidence` — decomposed data-quality metadata

Quality varies enormously across a decade and four feeds. Rather than a single opaque
score (whose blend weights would be unvalidatable), the quality axes are stored
**decomposed**, for the model to consume as observation variance:

| column | meaning | populated for |
|---|---|---|
| `value_dispersion` | MAD of the per-item measurements (robust spread) | distributional signals (turn-time, speed, age, anomaly, round-trip, queue) |
| `open_fraction` | share of value from items with no observed terminating event (open legs/visits) — censoring exposure | all stocks/flows |
| `estimated_fraction` | share resting on an *estimated magnitude* (open-queue eventual wait) | the queue-time signals |

Plus `n_legs` (sample size) and `regime` (fidelity). The one ground-truth-anchored
confidence measure lives outside the table: `data/capture_rate.py` (NOAA vs EIA).
`open_fraction` is also the live-vs-history diagnostic — e.g. `gas_in_transit_volume`
is ~0% open historically (all resolved) but ~64% open in the thin live window (scorer
tier-decay phantoms), which a model must down-weight.

---

## 3 · The signal catalogue (built)

34 keys in `signal_daily`, by family. **Banding** = the `zone_scope` stacking key.

### 3.1 · Headline gas-volume (the dashboard stack)

The desk-facing headline: **volume of gas (m³)** reconstructed per day. At-sea signals
are daily **stocks**; berth signals are amortized daily **flows** (a cargo spread
across its berth hours so it integrates to exactly one cargo).

| `signal_key` | what it is | how built | why it helps modelling |
|---|---|---|---|
| `gas_loading_us` (S) | gas being loaded at US export berths | export `visits`, cargo amortized over berth dwell (open visits use terminal-mean dwell), banded by terminal | the volume form of US export pace — the supply pulse, validatable against EIA |
| `gas_discharging_eu` (D) | laden gas discharged at EU import berths | laden import `visits`, same amortization | EU absorption — the demand pulse |
| `gas_in_transit_volume` (S/A) | laden gas at sea US→EU | laden `legs`, full cargo on every live day `[departed, arrived)`, banded by destination zone (`unknown` when undeclared) | the headline leading indicator: gas mechanically committed to Europe 1–3 weeks out |
| `gas_ballast_to_us` (S) | empty carriers returning to reload | ballast `legs` weighted by the capacity they'll carry, banded by destination zone | forward US loading capacity ~1–2 weeks out |

### 3.2 · Export-side pace — US supply (S)

| `signal_key` | what / how / why |
|---|---|
| `us_loadings_count` (#9/#11) | laden departures per US export terminal per day, from `visits` (count). The most direct "US is exporting X cargoes" measure. **`us_loadings_count_warm`** (#10) excludes `cold_start` events for clean WoW diffs. NOAA-deep. |
| `load_berth_turn_h` (#8) | loading dwell `departed − moored`, median + MAD per terminal. Lengthening = slower throughput. |
| `load_queue_h` (#6) | wait before berthing `moored − anchorage_entry`, median + MAD per terminal; open queues valued at an estimated eventual wait (`estimated_fraction`). Lengthening queues = US can't push gas out → HH softens, spread widens. NOAA-deep. |
| `us_queue_depth` (#7) | vessels currently waiting at US terminals (daily count). |
| `us_queue_formation_wow` (#38) | week-over-week change in `us_queue_depth` — a sudden jump leads an outage before it is confirmed. |
| `days_since_departed` (#36) | days since the most recent departure per US terminal, pooled across feeds. **The outage radar** — the Freeport-2022 outage would show within a day. |

### 3.3 · Import-side absorption — EU demand (D)

EU anchorage events exist only in the live feed (GFW carries none), so the EU queue
signals are **live-only** — a nowcast layer, not a training set. EU queue time is
structurally near-zero in normal markets, so the missing history is a tolerable gap.

| `signal_key` | what / how / why |
|---|---|
| `discharge_berth_turn_h` (#14) | discharge dwell at EU terminals, median + MAD. Lengthening = full downstream storage / regas bottleneck. |
| `discharge_queue_h` (#12) | EU discharge wait (live-only). Long EU queue = local oversupply → TTF soft → spread narrows. |
| `eu_queue_depth` (#13) | vessels queued at EU terminals (live-only). |
| `eu_queue_formation_wow` (#38) | WoW change in EU queue depth (live-only). |
| `queued_rate` (#15) | share of arrivals that anchored before berthing, per terminal — rising = terminals saturating. |
| `meaningful_queue_rate` (#16) | same but only dwell-confirmed `anchored` waits (filters drive-by polygon clips). |
| `days_since_moored` (#37) | days since the most recent mooring per EU terminal — the import-side outage radar. |

### 3.4 · Floating storage & voyage urgency (I / A)

| `signal_key` | what / how / why |
|---|---|
| `laden_voyage_age_d` (#20) | mean age (now − departed) of cargo at sea, over the same in-transit base as `gas_in_transit_volume`, banded by destination zone. **The best floating-storage proxy without satellite AIS** — uses only the two endpoints we observe; rising = slow-steaming / waiting. |
| `voyage_time_anomaly_d` (#21) | actual voyage duration − the lane's median, per O-D lane (median + MAD). Excess time without explanation = slow-steaming / floating. Needs no distance. |
| `voyage_speed_kn` (#22) | implied average speed = great-circle nm / voyage hours, per O-D lane. Distance from event fixes, or **terminal centroids** when endpoints lack coordinates (every GFW leg) — which is what gives it decade depth. Higher = racing to capture a wide spread. |
| `slow_steam_frac` (#24) | share of voyages under 13 kn, per O-D lane. Rising = contango paying for delay. |

### 3.5 · Arbitrage & flow geography (A)

| `signal_key` | what / how / why |
|---|---|
| `od_flow_count` (#5) | closed cross-zone voyages per origin→destination lane per day. Isolates the US→Europe lane vs leakage elsewhere. |
| `declared_eu_share` (#27/#31) | of laden US cargoes currently at sea with a declared destination, the share bound for Europe (vs Asia/other). From the live declaration on open legs (`legs.dest_region`). **Live-only** — declared destination exists only in the live feed (NOAA/GFW carry no master broadcasts). Rising = the arbitrage is already pulling cargoes to Europe → spread compressing in the market's view. |

### 3.6 · Fleet & shocks (S / A)

| `signal_key` | what / how / why |
|---|---|
| `round_trip_d` (#32) | gap between a vessel's consecutive departures, per origin zone (median + MAD). Falling = busy, efficient fleet. |
| `fleet_laden_frac` (#33) | share of active vessels carrying cargo on each day. A whole-fleet utilisation gauge. |
| `active_vessels` (#34) | distinct vessels mid-voyage or in-berth each day. Fleet activity baseline. |
| `newbuild_appearances` (#35) | vessels making their first appearance per day — fleet capacity growth. |
| `cold_start_rate` (#39) | share of arrivals flagged `cold_start` per zone — an AIS-off / dark-fleet proxy. **Read within the live regime**: GFW backfill events are all synthetic cold-starts (regime-confounded). |

### 3.7 · Composites — the model-input features (#40–#44)

Pure functions of the primitives above, the direct feed into the spread model. Each
component is standardised (expanding-window z, leakage-safe) before combining so
counts, hours and m³ are commensurable; emitted on the pooled `regime='all'` series,
both bases. Composites that need an EU-queue component are **live-only** (EU queue has
no history); `net_export_pressure` is US-only and so runs the full decade.

| `signal_key` | what / how / why |
|---|---|
| `net_export_pressure` (#40) | z(US loadings) − z(US load-queue). High = pushing gas out fast and unobstructed → HH soft relative to TTF. **Decade-deep.** |
| `net_absorption_pressure` (#41) | z(EU discharge volume) − z(EU discharge-queue). High = Europe absorbing fast. Live-only. |
| `spread_thrust` (#42) | #40 − #41 — **the headline composite**: supply outrunning demand (positive → spread narrows) vs a bottleneck (negative → spread widens). Live-only (needs #41). |
| `implied_storage_build` (#43) | z(in-transit) + z(voyage anomaly) + z(EU queue depth) − z(EU discharge) — gas in the system not yet consumed. Live-only. |
| `diversion_arbitrage` (#44) | first-difference of `declared_eu_share` — the *change* in where cargoes are heading, leading the realised arbitrage. Live-only. |

---

## 4 · Deferred & infeasible

**Planned (next, see `MODELS.md` build order):**
- **ETA-slip (#30) & re-emergence diversion (#29)** — need at-departure `vessel_state` dest/ETA historization (a temporal join + per-voyage ETA parse). Live-only and thin, so low ROI vs the composites; deferred. (`declared_eu_share` covers the high-value intent already.)
- **Floating-storage volume (#17/#19)** and **speed profiles (#25/#26)** — degraded / need fix-level SOG; lower value (`laden_voyage_age_d` is the robust floating proxy already built).

**Infeasible without satellite AIS** (documented, not built): mid-voyage SOG (#23),
real-time diversion (#28), and any mid-ocean floating-storage volume — terrestrial AIS
is blind beyond ~40–80 nm of coast.

---

## 5 · Reading the panel

```sql
-- US export pace, model-safe, segmented by fidelity (never 'all' for US):
SELECT bucket_date, value, open_fraction, n_legs
FROM signal_daily
WHERE signal_key='gas_loading_us' AND basis='knowable' AND regime='noaa'
ORDER BY bucket_date;
```

Always filter `basis='knowable'` for modelling, pick a single `regime` (or carry it as
an indicator), and weight by the confidence columns. `physical` is for validation only.
