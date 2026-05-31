# Signals derivable from `port_events` + `ais_fixes`

Inventory of market signals that can be extracted from the tanker-flow data, with
notes on which signals survive a **terrestrial-AIS-only** constraint. The
`approach` polygons and anchorages this inventory assumes are now in place
(34/34 terminals), and the voyage-leg foundation under the flow signals is built
(`pipeline/legs.py`). The **modelling** layer — which methods turn these signals
into a tradeable view on the Henry Hub / TTF spread, and why they can extract
signal from only a few months of data — lives in the companion
[`MODELS.md`](MODELS.md).

---

## 0 · Constraint: terrestrial AIS, no satellite feed

AISstream is predominantly fed by land-based receivers. Practical implications:

- Coverage is dense within roughly 40–80 nm of populated coastlines and falls off
  to nothing in mid-ocean. A vessel that has left the US Gulf shelf and not yet
  reached the European shelf is **invisible** to the pipeline.
- We see a vessel's *departure*-side track and *arrival*-side track cleanly.
  We do **not** see the middle.
- `vessel_state.dest` updates emitted mid-ocean are lost; we only catch the
  master's destination broadcast at the start of the voyage (and any update
  once the vessel reappears coastally near a new destination).
- Floating storage off West Africa or mid-Atlantic is invisible until/unless the
  vessel returns within terrestrial range of a known coast.

This shapes the signal list below: each signal is tagged with whether it's
**robust** (works on coastal AIS alone), **degraded** (computable but with loss
of resolution / lead time), or **infeasible** (needs satellite AIS / paid
sources like Spire, Kpler, Vortexa).

The framing for everything that follows: the HH/TTF spread widens when US gas
backs up (slow exports, full storage) or European gas tightens (cold, low
storage, supply shock); it narrows when US LNG reaches European terminals
quickly and in volume. Every signal is either a **supply pulse** (S, US-side
export pace), a **demand pulse** (D, European-side absorption), an **arbitrage
indicator** (A, where the marginal cargo is going), or an **inventory proxy**
(I, gas held on-water or in queue).

---

## 0·5 · Constraint: ingestion regime break (a structural discontinuity)

The terrestrial-AIS limit above is a *spatial* constraint. There is also a
*temporal* one: the ingestion scheme changed in a hard cutover at **2026-05-30
09:27 UTC**, so the real-time position history is two different data-generating
processes spliced together. Anything built as a time series across that seam
inherits an artifactual break.

| | Old regime (bbox + throttle) | New regime (server-side MMSI filter) |
|---|---|---|
| Span | 2026-04-14 → 2026-05-30 09:26 (~6.5 wk, **~93% of `port_events`**) | 2026-05-30 09:27 → present |
| Subscription | every vessel inside fixed geographic boxes | ~150 tier-ranked MMSIs from `priority_watchlist` |
| Missingness | **stochastic** — AISstream throttles by dropping vessels ~at random from the return | **systematic** — vessels the scorer deprioritises are never subscribed |
| Net bias | unbiased *selection*, noisy *capture* | biased *selection*, reliable *capture* |

The regimes are biased in **opposite** directions, so they don't average into one
clean series — they step. Rules for the modelling layer:

- **Never train a model across 2026-05-30.** A spread model fit on data spanning
  the seam will learn the ingestion change as if it were a market move. Segment by
  regime, or start the training corpus at the cutover.
- **The usable real-time history is ~6.5 weeks of a now-defunct regime + the live
  tail** — `MODELS.md` works through the consequences (`N ≈ 45`, `N_eff ≈ 28`).
  The old block is fit for validating *signal logic* (event detection, leg
  geometry, queue/turn-time realism all survive random fix drops), **not** for
  training the spread model. The real training corpus only begins accruing now,
  under the new scheme.
- **Signal *extraction* is safe today; signal *modelling* is not yet.** Computing
  #1/#6/#9 from `port_events` is unaffected by the seam (each event is individually
  correct); fitting any `MODELS.md` spread model on a series that crosses it is not.
- **Regime-specific artifacts:** the old regime inflates "gone-dark" / stale-close
  and manufactures phantom open legs via random drops + box geography; the new
  regime manufactures phantom open legs via scorer tier-decay instead. Both demand
  open-leg **age-censoring** (see #17/#20) regardless of regime.

- **The seam is now explicit in the data.** `port_events.regime` is a STORED
  generated column (`'bbox'` before the 2026-05-30 09:27 UTC cutover,
  `'mmsi_filter'` after, mirroring `config.regime_of`), so every series below can
  be segmented on it directly in SQL — no derivation needed.

Full provenance and per-signal impact: `docs/review-2026-05-31-pre-signal-audit.md`
(§0) and the post-hardening re-audit `docs/review-2026-05-31-post-hardening-audit.md`,
which records the must-fixes (naive pairing, phantom legs, dest_parser, FSRU hosts)
as resolved or quarantined.

---

## 0·6 · Schema notes for the signal builder

A few columns the signal definitions below lean on, to save a schema hunt and
avoid looking for fields where they don't live:

- **`flow_direction` is on `terminals`, not `port_events`.** Signals phrased as
  "`moored` events with `flow_direction='import'`" (#4, #12, #14, #16) require a
  `port_events.terminal_id → terminals.flow_direction` join.
- **`port_events.laden_source`** (`'draught'` | `'flow_direction'`) records how
  `laden_flag` was decided. Outbound (`departed`) legs are now
  flow-direction-determined; the ton-mile (#1) and fleet-laden (#33) weights can
  condition on this.
- **`port_events.regime`** is the STORED generated column from §0.5 — segment
  every time series on it.
- **Registry weights:** `vessel_registry.dwt` is 100% populated (#1 fully
  supported); `gas_capacity_m3` is missing for 4 (#2); `design_draught ≤ 0` for
  75/780 in-scope (those laden flags fall back to flow-direction).

---

## 1 · Flow signals — the headline market signal

| # | Signal | Feasible? | Lead | Type |
|---|---|---|---|---|
| 1 | **Laden ton-miles in transit, US Gulf → NW Europe** — sum over open `departed → next zone_entry` legs of `dwt × laden_flag × great-circle(lat_departed, lat_zone_entry)`. Mid-ocean position is unknown, but the *existence* and *endpoints* of the leg are known, so total ton-miles outstanding is well-defined | **Robust** | 1–3 wk | S |
| 2 | **Gas-capacity-weighted variant** — same, using `gas_capacity_m3` instead of `dwt`. Closer to physical LNG volume; less noisy for fleet mix changes | Robust | 1–3 wk | S |
| 3 | **Open laden voyages by destination zone** — count of legs with `departed` but no terminating `zone_entry` yet, grouped by `vessel_state.dest` at departure | Robust | 1–3 wk | S |
| 4 | **Arrivals-per-week at European terminals** — count of laden `moored` events with `flow_direction='import'` | Robust | 0–1 wk | S |
| 5 | **Origin → destination O-D matrix** — `departed.zone → next zone_entry.zone` flow table. Isolates the US→Europe lane vs US→Asia leakage | Robust | 1–3 wk | A |

> *Mechanism*: These mechanically constrain European supply over the next 7–21
> days. Sustained dip in #1 / #4 should precede TTF strength (spread widens);
> surge should precede TTF weakness (spread narrows).

> **Leg foundation (`pipeline/legs.py`).** Signals #1–#5, #20–#22 and #32 are not
> computed from a naive "`departed` → next `zone_entry`" scan. That pairing was a
> documented failure mode — laden `usgulf→usgulf` legs averaging 31 days, a missed
> European round-trip collapsed into one bogus near-zero-distance "leg". The legs
> module pairs and **classifies** every leg: `closed` (cross-zone real voyage),
> `same_zone` (intra-region hop / berth shift / re-entry — ~zero cross-zone
> ton-miles, so **excluded** from the lane flow), `open_in_transit`, and
> `open_censored` (open beyond `CENSOR_OPEN_DAYS = 30` — quarantined as a phantom;
> see §4). Each leg is regime-tagged and carries `dwt` / `gas_capacity_m3`. The
> signal layer aggregates over this classified base: select `closed` + laden
> `open_in_transit` for the US→Europe lane; never sum `same_zone` or
> `open_censored`.

---

## 2 · Export-side queue / throughput signals — US supply pace

| # | Signal | Feasible? | Lead | Type |
|---|---|---|---|---|
| 6 | **Loading queue time** — `moored - anchorage_entry` for ballast arrivals at US Gulf export terminals | Robust | 1–2 wk | S |
| 7 | **Queue depth** — vessels currently between `anchorage_entry` and `moored` at US Gulf | Robust | 1 wk | S |
| 8 | **Berth turn time** (loading duration) — `departed - moored` for laden departures from export terminals | Robust | 1 wk | S |
| 9 | **Loadings-per-week per export terminal** — count of laden `departed` events. The most direct "US is exporting X mt/wk" measure | Robust | 1–2 wk | S |
| 10 | **Cold-start-corrected throughput** — #9 excluding `cold_start=TRUE` synthetic events. For clean WoW diffs | Robust | 1–2 wk | S |
| 11 | **Per-terminal Sabine / Freeport / Plaquemines time series** — disaggregated #9. The Freeport-2022 outage would have been visible in the per-terminal trace within hours | Robust | 0 | S |

> *Mechanism*: Loading queues lengthening → US can't push gas out → HH softens,
> TTF firms → spread widens. Queues shrinking + loadings rising → spread narrows.

---

## 3 · Import-side queue / discharge signals — European absorption

| # | Signal | Feasible? | Lead | Type |
|---|---|---|---|---|
| 12 | **Discharge queue time** per import terminal — `moored - anchorage_entry` at flow='import' | Robust | 0–1 wk | D |
| 13 | **Discharge queue depth** — vessels currently queued at European terminals | Robust | 0 | D |
| 14 | **Discharge berth turn time** — `departed - moored` at import terminals. Lengthening = full downstream storage / regas bottleneck | Robust | 0–1 wk | D |
| 15 | **Did-this-vessel-queue rate** — share of arrivals where `anchorage_entry` precedes `moored`. Rising = European terminals saturating | Robust | 0–2 wk | D |
| 16 | **Meaningful-queue rate** — share requiring dwell-confirmed `anchored` (not just `anchorage_entry`). Filters drive-by crossings | Robust | 0–2 wk | D |

> *Mechanism*: Long European queue = cargoes stacking up = local oversupply, TTF
> softens, spread narrows. Short queue = terminals accepting immediately =
> tight demand, TTF firms, spread widens.

---

## 4 · Floating-storage signals — gas-on-water inventory

This is the most under-measured part of the LNG market and one of the most
spread-relevant — but it's also where the terrestrial-AIS constraint hurts most.

| # | Signal | Feasible? | Lead | Type |
|---|---|---|---|---|
| 17 | **Laden idle vessels** — `departed.laden_flag=TRUE`, no subsequent `zone_entry` for >14 days. With terrestrial AIS we *cannot* confirm idleness — we can only confirm "didn't arrive yet". A vessel transiting slowly looks identical to a vessel parked | **Degraded** | 2–8 wk | I |
| 18 | **Drifting / circling detection** — laden vessel with track turning >180° in a coastal box, no nearby destination polygon. Only works for vessels loitering *within terrestrial AIS range* (e.g., off Singapore, Skagen, Gibraltar, Suez approaches) | **Degraded** | 2–8 wk | I |
| 19 | **Aggregate floating storage volume** — sum `gas_capacity_m3` over #17. Severely biased without satellite — undercounts mid-Atlantic storage entirely | **Degraded** | 2–8 wk | I |
| 20 | **Mean laden-voyage age** — mean (now − `departed`) across legs with no terminating `zone_entry`. Rising = vessels slow-steaming / waiting. **The best floating-storage proxy available without satellite AIS** — it survives the constraint because it uses only the two endpoints we observe | **Robust** | 1–4 wk | I |
| 21 | **Voyage-time anomaly per O-D pair** — actual leg duration − typical great-circle duration. Excess time on a US→Europe leg without an explanation = slow-steaming or floating storage | **Robust** | 1–4 wk | I |

> *Mechanism*: Floating storage builds when European spot trades below forward —
> vessels are paid to wait. Rising floating storage / voyage-time anomaly =
> market expects tighter Europe later = TTF curve steepening = forward spread
> widens.

> *Without satellite AIS, #20 and #21 are the workable floating-storage proxies*:
> they use only the departure and arrival timestamps, not any mid-ocean fix.
> #17–19 should be reported with the caveat "subset visible from coast".

> **Phantom-leg bias — a second, distinct bias on top of mid-ocean blindness.**
> Beyond the spatial blind spot above, a large share of open legs are not floating
> storage at all: they are arrivals we never recorded because the vessel went dark
> (old-regime throttle drops / box geography, or new-regime scorer tier-decay).
> The pre-signal audit found 47 open laden-US legs with *zero* post-departure
> fixes (oldest 42.9 d). These *phantoms* are indistinguishable from genuine
> idleness and inflate #1, #17, #19 and #20 without bound. Two mitigations are
> live: (a) `pipeline/legs.py` **censors** open legs older than 30 days
> (`open_censored`), so they never enter the in-transit base; (b) `scoring.py`
> **pins** any vessel with a recent open laden leg into a persistent subscription
> slot (`priority_watchlist.is_pinned`) so the new scheme re-acquires it on the
> European approach. The censor is a single global cap — the signal layer should
> still apply a tighter per-O-D window (US→EU ~18 d) on top, and the pin only
> helps the *new* regime, not the historical phantoms.

---

## 5 · Speed / transit-urgency signals

| # | Signal | Feasible? | Lead | Type |
|---|---|---|---|---|
| 22 | **Implied voyage speed on US → Europe leg** — `great_circle(departed, zone_entry) / (t_zone_entry - t_departed)`. Average speed only, not the mid-voyage profile, but that's the spread-relevant statistic anyway | **Robust** | 1–2 wk | A |
| 23 | **Mid-voyage instantaneous SOG distribution** — rolling sog histogram for open legs | **Infeasible** (needs satellite AIS) | 1–3 wk | A |
| 24 | **Slow-steaming fraction** — share of completed legs with implied speed < 13 kn. Rising = vessels deliberately delaying = contango forming. Reconstructed from #22, not direct observation | **Robust** | 2–6 wk | I |
| 25 | **Departure-side acceleration profile** — sog growth rate in the first 12h after `departed`. A noisy proxy for "is the master in a hurry" | **Robust** | 1–3 wk | A |
| 26 | **Arrival-side deceleration profile** — sog and course-change rate in the 24h before `zone_entry`. Quick approach = empty queue; meandering = expects to wait | **Robust** | 0–1 wk | D |

> *Mechanism*: Higher implied voyage speed = vessels racing to capture a wide
> spot spread before it closes. Slow-steaming = contango paying for delay.
> #22 and #24 are the only speed-based signals that survive without satellite
> AIS; they sacrifice intra-voyage resolution but retain the spread-relevant
> mean.

---

## 6 · Destination / intent signals (from `vessel_state`)

| # | Signal | Feasible? | Lead | Type |
|---|---|---|---|---|
| 27 | **Declared-destination flow at departure** — count of laden vessels with `vessel_state.dest` matching European port codes, broadcast at or near `departed` | **Robust** | 1–3 wk | S |
| 28 | **Mid-voyage diversion (real-time)** — `vessel_state.dest` change while vessel is at sea | **Infeasible** without satellite AIS | 1–4 wk | A |
| 29 | **Re-emergence diversion** — vessel arrives in a different zone from the one its last-broadcast `dest` named. Detected at `zone_entry`, so a confirmed-after-the-fact diversion, not a leading one | **Degraded** | 0 | A |
| 30 | **ETA-slip rate** — actual `zone_entry` minus originally declared `eta`. Slip lengthening when downstream is full | **Robust** | 0–2 wk | D |
| 31 | **US→Asia vs US→Europe destination ratio at departure** — among laden vessels just departed from US Gulf, the split by declared destination | **Robust** | 1–4 wk | A |

> *Mechanism*: If declared US→Europe share rises, the spread is already
> compressing in the market's view. The real-time diversion signal (#28) is the
> biggest casualty of no satellite AIS — but the departure-time intent (#27,
> #31) is intact, and most of the spread-relevant decision happens at the point
> of charter / departure, not mid-voyage.

---

## 7 · Round-trip / fleet utilisation signals

| # | Signal | Feasible? | Lead | Type |
|---|---|---|---|---|
| 32 | **Round-trip time per vessel** — `departed_n+1 - departed_n`. Falling = market busy, fleet efficient | Robust | 2–6 wk | S |
| 33 | **Fleet-laden fraction** — share of active in-scope MMSIs with `laden_flag=TRUE` between `departed` and next `zone_entry` | Robust | 0–4 wk | A |
| 34 | **Active vessel count** — distinct MMSIs with ≥1 event in trailing N days | Robust | months | S |
| 35 | **Newbuild appearance** — first observation of MMSI that VF tags as LNG Tanker. Tracks fleet capacity growth | Robust | months | S |

---

## 8 · Per-terminal anomaly / outage signals

Discrete shocks dominate the realised spread; smooth flows are mostly priced in
by the curve. Outage detection is high-leverage.

| # | Signal | Feasible? | Lead | Type |
|---|---|---|---|---|
| 36 | **Days since last `departed` per export terminal** — > 7 days = potential outage | Robust | 0 | S |
| 37 | **Days since last `moored` per import terminal** — > 7 days at an active terminal = mechanical / planned outage | Robust | 0 | D |
| 38 | **Sudden queue formation rate** — week-over-week change in queue depth (#7, #13). Leading indicator of an outage before it's officially confirmed | Robust | 0–1 wk | S/D |
| 39 | **Cold-start anomaly rate per MMSI** — spike in `cold_start=TRUE` for an MMSI = AIS-off behaviour (sanctions / dark fleet). Russia / Yamal LNG-relevant | Robust | months | A |

---

## 9 · Composite / model-input signals

| # | Composite | Definition |
|---|---|---|
| 40 | **Net US-export pressure** | (loadings/wk #9) − z-score(loading queue time #6) |
| 41 | **Net European-absorption pressure** | (arrivals/wk #4) − z-score(discharge queue time #12) |
| 42 | **Spread thrust** | #40 − #41. Positive = supply outrunning demand → spread narrows; negative = bottleneck → spread widens |
| 43 | **Implied storage build** | laden ton-miles in transit (#1) + voyage-time anomaly (#21) + European queue depth (#13) − arrivals/wk (#4) × forward window. Proxy for gas-in-the-system-not-yet-consumed |
| 44 | **Diversion arbitrage indicator** | first-difference of US→Europe vs US→Asia destination ratio (#31) — leading the realised arbitrage |

---

## Modelling → see [`MODELS.md`](MODELS.md)

The modelling layer — which method turns these signals into a spread forecast,
why each can extract signal from only a few months of data, and how to control
for the non-tanker drivers of the spread — has moved to its dedicated companion
**[`MODELS.md`](MODELS.md)**. The headline points:

- **Physical nowcasts work today** (kinematic ETA propagation, Poisson/NB arrival
  counts, Cox/Weibull survival models for queue & berth time). They are high-SNR,
  event-level, and validatable against EIA on the existing window.
- **The spread model does not yet.** With `N ≈ 45` daily rows (`N_eff ≈ 28`) and
  the 2026-05-30 regime seam, training a spread model is premature; the right
  tools are shrinkage + Bayesian priors + honest uncertainty (regularised
  regression, Bayesian structural time series, regime detection), not a point
  forecast. **Never train across the seam** (§0.5). The usable training corpus
  only begins accruing now, under the MMSI-filter regime.
- **The edge is the dataset, not the method:** a clean, low-latency, per-vessel
  AIS feed (≈1 s ingest lag vs vendors' hours-to-days) and 24–48 h-early
  outage / change-point detection (#36–#38) — not smooth-model R².

See `MODELS.md` for the full treatment, the small-sample maths, the non-tanker
control set, and the recommended build order.
