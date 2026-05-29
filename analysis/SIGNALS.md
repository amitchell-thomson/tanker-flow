# Signals derivable from `port_events` + `ais_fixes`

Inventory of market signals that can be extracted from the tanker-flow data once
`approach` polygons and missing anchorages are in place, with notes on which
signals survive a **terrestrial-AIS-only** constraint, and which ML methods most
plausibly turn these signals into a tradeable view on the Henry Hub / TTF spread.

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

## ML methods — which approach for which signal

Targets to consider in priority order: (a) HH/TTF spread first-difference at
1-week and 4-week horizons, (b) realised spread volatility, (c) per-terminal
arrival/loading rates as intermediate model outputs.

The training set is small — daily resolution over ~3–5 years gives ~1000–1800
rows. This is the central constraint: deep models will overfit; the methods
below are picked for sample-efficiency and interpretability.

### A. Baseline / shallow models — necessary, low edge

1. **Lagged linear regression** — spread first-difference on 10–15 hand-picked
   lagged AIS features (`#1`, `#4`, `#7`, `#13`, `#20`, `#42`) + EIA storage +
   degree-days. Sanity-check baseline. Anything fancier must beat this on
   walk-forward CV.
2. **VAR / Bayesian state-space (dynamic linear model)** — treats spread as a
   slowly-varying latent state observed through both the AIS signals and the
   spread itself. Gives uncertainty intervals natively, which is what a hedger
   actually wants. Recommended over pure VAR because of the small sample.

### B. Gradient boosting — workhorse production model

3. **LightGBM / XGBoost** on the full feature set, target = spread Δ at horizon
   `h`. Handles 30–40 features, captures interactions, monotonic constraints
   available for sign-known features (e.g., constrain "loading queue time" to
   have non-negative effect on spread). SHAP for interpretation.
   *Edge: low — every commodity desk has this. But the right baseline for the
   production nowcast.*

### C. Per-vessel micro-models — where the genuine edge sits

This is the part very few people are doing in LNG, and the part where having a
custom AIS pipeline rather than a Kpler / Vortexa subscription pays off. The
pipeline already produces per-vessel events; bulk-data vendors usually don't
expose them at this granularity.

4. **Survival models for `time-at-queue` and `time-at-berth`** — Cox
   proportional hazards (or DeepSurv for non-linearities) per terminal,
   covariates = vessel `dwt`, recent terminal congestion, season, day-of-week,
   weather (degree-day proxy). Yields per-vessel arrival-rate predictions that
   aggregate to a much better forecast of weekly arrivals than naive counts.
   *Edge: HIGH. Underexploited because most LNG modelling stops at aggregate
   counts.*
5. **Multivariate Hawkes process over terminals** — models arrivals at one
   terminal as self-exciting + cross-exciting from neighbours (tide windows,
   weather systems, fleet rotation). Better arrival nowcast than independent
   Poisson rates.
   *Edge: HIGH for short-horizon arrivals; moderate for spread directly.*
6. **Per-vessel "intent" classifier** — given a vessel's last 7 days of fixes
   plus its `vessel_state.dest`, predict P(next `zone_entry` is European). A
   simple boosted tree on engineered features (current zone, sog, declared
   dest, time-since-departed). Aggregated over the fleet gives a probabilistic
   Europe-arrival forecast that beats counting declared destinations naively.
   *Edge: medium-high.*

### D. Event / regime detection — high leverage for outages

7. **Bayesian online change-point detection (BOCPD)** on arrivals-per-week and
   loadings-per-week per terminal. Fires alerts within 24–48h of a regime
   break — the Freeport-2022 archetype. Cheap, well-understood, and outages
   dominate realised spread moves.
   *Edge: HIGH on a per-event basis; sparse signal (few events per year) so
   hard to backtest, but the asymmetric payoff makes it worth running.*
8. **HMM / regime-switching regression** for the spread itself, where the
   regime is driven by composite features (#42 spread thrust, #36/#37 outage
   indicators). Lets the model learn that the linear relationship between
   queue depth and spread differs in "normal" vs "outage" vs "winter freeze"
   regimes.

### E. Causal / event-study layer — for understanding, not nowcasting

9. **Difference-in-differences across outage events** — was the Freeport
   spread move *caused* by the outage, or were both driven by underlying
   weather? Useful for sizing positions when a similar outage recurs.
10. **Synthetic control on natural experiments** — when a single terminal goes
    offline, compare the cumulative spread move against a synthetic
    counterfactual constructed from terminals that remained online. Quantifies
    each terminal's marginal contribution.

### F. Methods to avoid given the data

- **Deep sequence models (Transformer / LSTM / TFT)** — with ~1500 daily
  observations and 30+ features, these overfit badly. The literature claiming
  Transformer wins on financial time series almost always has a leakage bug.
  Don't.
- **GNNs on the O-D matrix** — clean conceptual fit, but the graph is small
  (~33 in-scope terminals) and the data is too short. A boosted tree on
  hand-crafted O-D features beats it.
- **End-to-end RL for trading the spread** — sample-inefficient,
  hard-to-debug. Premature for this dataset size.

---

## Where the edge actually sits

Plain ML technique is *not* the edge. Three things are:

1. **Owning a clean, low-latency AIS-derived dataset.** Median ingest lag is
   currently ~1 s; Kpler / Vortexa publish position-derived reports with
   hours-to-days of lag. The nowcast on **#36–38** (outage / queue formation)
   is the highest-leverage application of the speed advantage.

2. **Per-vessel micro-features that bulk-data vendors flatten away.** The
   `port_events` table is already at per-vessel-per-event granularity. The
   survival / Hawkes / intent classifiers in section C use this directly.
   Vendor data forces everyone into aggregate counts; this pipeline doesn't.

3. **Outage / change-point detection.** Discrete supply shocks (Freeport,
   Yamal sanctions, Norwegian field maintenance) drive a disproportionate
   share of realised spread variance. Being 24–48 h ahead on identifying an
   outage from `port_events` is worth more than any smooth-model R².

The terrestrial-AIS constraint **eliminates the mid-voyage diversion edge**
(signal #28) and **degrades the global floating-storage measurement** (#17–19).
What remains intact is the entire port-side stack — queues, throughput,
turn times, outages, declared-destination-at-departure — which is also where
most of the spread-relevant information actually concentrates. The pipeline is
well-suited to that subset.

Recommended build order:
1. Lagged linear baseline (#1, #4, #7, #13, #20, #42 + storage + degree-days).
2. LightGBM nowcast on the full feature set, walk-forward CV.
3. Per-terminal BOCPD on arrivals/loadings — wire to alerts.
4. Cox survival model on queue / berth times — feed predictions into the GBM.
5. Hawkes model for arrivals — only if (4) shows the survival features have
   signal.
6. State-space / HMM regime layer — only after a stable GBM baseline.

Skip the deep-learning layer until/unless the dataset extends to >5 years of
daily data or moves to hourly resolution.
