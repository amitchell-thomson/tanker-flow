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
| Span | 2026-04-14 → 2026-05-30 09:26 (~6.5 wk, **~77% of *live-collected* `port_events`** as of 2026-06-12, falling as the live tail grows) | 2026-05-30 09:27 → present |
| Subscription | every vessel inside fixed geographic boxes | ~150 tier-ranked MMSIs from `priority_watchlist` |
| Missingness | **stochastic** — AISstream throttles by dropping vessels ~at random from the return | **systematic** — vessels the scorer deprioritises are never subscribed |
| Net bias | unbiased *selection*, noisy *capture* | biased *selection*, reliable *capture* |

The regimes are biased in **opposite** directions, so they don't average into one
clean series — they step. Rules for the modelling layer:

- **Never train a model across 2026-05-30.** A spread model fit on data spanning
  the seam will learn the ingestion change as if it were a market move. Segment by
  regime, or start the training corpus at the cutover.
- **The usable *real-time* history is ~6.5 weeks of a now-defunct regime + the
  live tail** — this is the **live-only corpus**: `N ≈ 60` daily rows worth only
  `N_eff ≈ 7` independent points (ρ=0.8) as of 2026-06-12; `MODELS.md` §0 works
  through the consequences. The old `bbox` block is fit for validating *signal
  logic* (event detection, leg geometry, queue/turn-time realism all survive random
  fix drops), **not** for training the spread model. **The historical backfill
  (`ingestion/historical/PLAN.md`) is the escape from this constraint** — it lifts
  the modelling corpus to `N ≈ 3,000` (`N_eff ≈ 330`) for every signal with a
  historical source. The two percentages above (`bbox` ~77% of *live-collected*
  events) describe only the live stream; once the backfill lands, both live regimes
  together are a thin recent slice of a `port_events` table dominated by 2016+ NOAA
  and 2017+ GFW events.
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

- **Backfill generalises the seam into a multi-fidelity rule.** The historical
  backfill (`ingestion/historical/PLAN.md`) adds two more regimes — `noaa`
  (exhaustive Class A, US, 2015+) and `gfw` (voyage-arc fidelity, EU+global,
  2017+) — so `regime` becomes source-aware (§3.4 of the plan), and the rule above
  generalises from "never cross the 2026-05-30 seam" to **"only concatenate series
  of equal fidelity; render every fidelity change as a visible discontinuity."**
  The crucial consequence for signal-building: on the **US side**, `noaa` and
  `mmsi_filter` are the *same* fidelity (NOAA exhaustive; the MMSI filter
  subscribes to every LNG MMSI), and NOAA retroactively overwrites the throttled
  `bbox` window — so the US export series (#6–11, `gas_loading_us`) becomes a
  clean, seam-free **2015→now** line and the small-sample obstacle below
  *disappears* there. The **EU side** keeps one real fidelity step at the
  `gfw → mmsi_filter` boundary (arc-fidelity → full-fidelity) that must enter any
  EU model as a regime indicator, never a blend.

The pre-signal-layer audit and its post-hardening re-audit recorded the must-fixes
(naive pairing, phantom legs, dest_parser, FSRU hosts) as resolved or quarantined;
those dated review docs were removed once their fixes landed (commit `5787b87`),
and the surviving conclusions are folded into this section and the leg-foundation
note in §1.

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

### 0·6·1 · Signal fidelity by source (which signals get historical depth)

Once the backfill lands, not every signal gets the same history. This matrix
(mirrored in `ingestion/historical/PLAN.md` §4.1) says which signals can be
trained/validated on a deep panel *now* and which begin only at the live cutover:

| Signal family | `noaa` (US) | `gfw` (EU) | `mmsi_filter` (live) |
|---|---|---|---|
| US loadings #9–11, `gas_loading_us` | ✅ full, 2015+ | — | ✅ |
| US queue/berth #6–8 | ✅ full, 2015+ | — | ✅ |
| EU arrivals #4, `gas_discharging_eu` (count) | — | ✅ count, 2017+ | ✅ |
| EU berth-amortized `gas_discharging_eu` | — | ⚠️ no real dwell → terminal-mean estimate | ✅ |
| **EU queue/berth #12–16** | — | ❌ **no history** (no `anchorage_entry`) | ✅ |
| In-transit #1/#2, `gas_in_transit_volume` | ⚠️ departure only | ⚠️ arrival only | ✅ |
| **#1 reconstructed: NOAA dep ⋈ GFW arr** | **✅ full leg, 2017+** | | ✅ |
| Voyage age #20 | ✅ dep obs | arr obs | ✅ |
| laden source | ✅ draught | ❌ flow_direction | ✅ draught |

The headline in-transit volume (#1/#2/`gas_in_transit_volume`) gets full history
*only via the NOAA-departure ⋈ GFW-arrival reconciliation* (`PLAN.md` §3.7) — the
two sources are complementary halves of one leg, and without the dedup the leg is
paired twice and the volume doubles. **EU queue/berth signals #12–16 get no
historical training set** (GFW voyage arcs carry no `anchorage_entry`); they begin
only at the live `mmsi_filter` cutover. §3.5 of the plan argues EU queue is
structurally near-zero in normal conditions, so this is a tolerable gap, not a
hole in the spread thesis.

---

## 0·7 · Two bases: `physical` vs `knowable` (point-in-time)

Every series in `signal_daily` carries a `basis` dimension. It is **not** a variant
of the signal — it is *which information set* was allowed to compute each day's
value. Same vessels, same events; different clock. This is as foundational as the
regime seam (§0·5): get it wrong and the whole spread thesis is invalidated by
lookahead.

| | `basis='physical'` (built) | `basis='knowable'` (deferred — this section) |
|---|---|---|
| Definition | "what was actually on the water on day `d`", computed with **everything we know today** | the value the pipeline **would have printed live on day `d`**, using only what was knowable by `d` |
| Mechanism | one `compute_legs(now=today)` | an as-of replay clocked to the *live* pipeline's knowability |
| Uses | physical validation — capture-rate (#13), "what was out there", the Part II nowcasts that validate in weeks | **the only leakage-safe input for the spread model** (Part III / `MODELS.md`) |
| Safe to train on? | **No** — embeds the future | Yes |

**Why `physical` leaks.** It silently uses hindsight in four ways, each a future
fact relative to day `d`: (1) **leg classification by final outcome** — a leg is
`closed` / `open_censored` (phantom, dropped) / `open_floating` *today*, but on `d`
you didn't know which; (2) **the arrival endpoint** — `gas_in_transit_volume` runs
over `[departed, arrived)` and `arrived` is in `d`'s future; (3) **NOAA-overwrites-live**
(`RESEARCH_PLAN.md` §3.2) — NOAA republishes the same dates 1–3 months later with
fuller data, so a "historical" value improves with information `d` never had; (4)
**late destination declarations** re-band a leg after departure. Backtest the spread
model on `physical` and you get the textbook leak (`MODELS.md` Part IV) — a beautiful
backtest that dies live.

> **The clock rule (the one thing to get right).** `knowable` is point-in-time
> with respect to the **live ingestion pipeline's** knowability, **not** the
> backfill source's publication schedule. In production the live signal is read
> from **aisstream in near-real-time** (US *and* EU watchlist vessels, minutes of
> latency, + the vf_rescue backstop). NOAA (1–3 mo) and GFW (days) are *historical
> proxies* for that real-time feed. So:
> - **Do NOT bake in the NOAA/GFW publication lag.** Live you won't have it;
>   adding it would model a slower world than you deploy into (train/serve skew).
> - **Do replicate the signal's *intrinsic* confirmation delays**, which are present
>   both live and in history: the 30-min `moored` dwell + back-dated event
>   timestamps, "a leg is not `closed` until its arrival is observed" (no future
>   endpoint), the open-leg voyage-window censoring (#17/#20), open-visit dwell
>   estimation, and **no future fixes** in the `d` value.
> - **The publication lag is a leakage *guard*, not a feature delay** — its only job
>   is to keep NOAA from crossing into the live hold-out window (§3.2) so a
>   "historical" value can't improve with data the live system didn't have.

### 0·7·1 · What `knowable` requires to implement

1. **As-of contribution intervals.** `signal.py` already takes `--as-of` and
   `compute_legs(now=as_of)` already classifies *relative to* `as_of` (an open leg
   at `as_of` stays `open_in_transit`, not retro-`closed`). Build `knowable` by
   computing each item's contribution with as-of semantics across the panel —
   arrival endpoint capped at `min(observed_arrival, as_of)`, legs never
   retro-closed, phantoms never retro-excluded — and writing parallel
   `basis='knowable'` rows (the schema dimension already exists). The naïve form is
   a daily sweep of `as_of` (O(days × items)); the efficient form advances `as_of`
   incrementally, since an item only changes state on a handful of days.
2. **Vintage filtering by *availability*, not event time.** As-of on the event
   timestamp isn't enough — the tables today hold fixes that became *available*
   after `d` (NOAA/GFW backfill, late vf_rescue). Filter each source by when we'd
   have had it:
   - **live (`mmsi_filter`):** `ais_fixes.server_ts` / `vessel_state.server_ts` is
     the receipt time → keep `server_ts <= as_of`.
   - **backfill (`noaa`/`gfw`):** no true ingestion stamp (loaded now). For
     *pre-live* history they're legitimately knowable (long since published by any
     later vantage); the only hard rule is **NOAA/GFW barred from the live
     forward-test window** (§3.2). The residual is a modeled publication lag — the
     one unavoidable approximation, and it only bites near the live seam.
3. **Leakage guard at the seam.** Enforce, in the loader, that no `noaa`/`gfw` row
   contributes to a `bucket_date` inside the live hold-out window, regardless of
   when it was loaded.
4. **Free self-validation.** The live tail (since the `mmsi_filter` cutover,
   2026-05-30) is exactly what the system emitted in real time. `knowable[d]`
   recomputed for those days **must match** the value actually printed live on `d`;
   a mismatch means the as-of/vintage logic still leaks. This is the acceptance test.

### 0·7·2 · The residual that timing cannot fix (read before modelling)

Even a perfect point-in-time replay leaves **sensor skew**: the historical proxies
and the live feed are *different instruments* — NOAA terrestrial (~77% US capture),
GFW fused terr+sat (EU), live aisstream (watchlist-limited by the 3-conn/scan cap);
and laden inference differs (NOAA real **draught** vs GFW **flow_direction** vs live
draught — §0·6·1). This is domain shift, not a timing bug, and it is where
train→live transfer is actually won or lost. The mitigation is already doctrine:
build the deployable model only from **arc-derivable features present in both
corpora** (`RESEARCH_PLAN.md` §3.3), keep EU-queue #12–16 as a live-only enrichment
layer, and fit measurement-error-aware across the rich-US/coarse-EU asymmetry.

### 0·7·3 · Convention for every signal below (and for new ones)

To stay model-ready, each signal in §1–§9 should declare two things in its row:
its **physical contribution interval** (already implicit in the definitions) and its
**knowable contribution interval** (when each day's value becomes computable under
the clock rule above). For stocks this is the live interval `[start, min(end,
as_of))`; for the amortized berth flows it is the cumulative-deposit-to-`as_of`
(never the final total until departure is observed). New signals are not "done"
until both are specified — `physical` for validation, `knowable` for training.
`MODELS.md` consumes **`knowable` only**; `physical` never crosses into a model fit.

---

## 0·8 · Signal confidence — decomposed data-quality metadata (built)

Data quality varies enormously across the panel — a 2017 value reconstructed from
sparse early NOAA is not the same instrument as a 2024 value, and a live in-transit
stock built mostly from un-arrived (open) legs is not the same as one built from
resolved voyages. Rather than collapse that into a single opaque `confidence ∈
[0,1]` (un-interpretable, and its blend weights would be hand-tuned against no
ground truth — a fudge), `signal_daily` carries the quality axes **decomposed**, so
the modelling layer combines them as observation variance (the measurement-error
fit `MODELS.md` already calls for). Three nullable columns, each one clear thing,
populated only where meaningful:

| Column | Meaning | Populated for |
|---|---|---|
| `value_dispersion` | MAD of the per-item measurements behind the cell (robust spread; a phantom tail can't inflate it like stdev) | distributional signals: turn-time, speed, voyage-time anomaly, round-trip |
| `open_fraction` | share of `value` from items with **no observed terminating event** (open legs / open visits) — the censoring-exposure axis | every stock/flow over legs/visits; 0 when all contributors closed |
| `estimated_fraction` | share of `value` resting on an **estimated magnitude** (not just an un-observed endpoint) — reserved for the Phase-2 queue-time estimates where it is distinct from `open_fraction` | (reserved) |

Two existing dimensions complete the picture and are *not* duplicated into a score:
`regime` (the fidelity tier) and `n_legs` (sample size). The one **ground-truth-
anchored** confidence measure lives outside this table — `data/capture_rate.py`'s
NOAA-vs-EIA capture rate, the only "what fraction of reality do we see" number
measured against an external truth (US, for now).

`open_fraction` is also the live-vs-historical diagnostic: the `physical`
`gas_in_transit_volume` is ~100% closed historically (`open_fraction≈0` — every past
leg has resolved) but ~64% open in the live `mmsi_filter` window — the scorer
tier-decay phantom-leg fingerprint (§0·5), not a market move. A model must
down-weight a high-`open_fraction` cell; the column makes that mechanical.

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

### 1·1 · Headline display (v2): gas-volume stacked stocks

The dashboard headline was refactored (after an industry conversation) from
ton-miles to **volume of gas (m³)**, the unit a desk actually reasons in. Every
headline signal is a **daily stock reconstructed over a live interval and
stacked into bands** (stacked-area charts). They live in `signal_daily` and are
built by `pipeline/signal.py` from the leg foundation + a new port-visit
foundation (`pipeline/visits.py`, `moored → departed` berth occupancy).

| `signal_key` | Stock | Band (`zone_scope`) | Underlies |
|---|---|---|---|
| `gas_loading_us` | gas in US export berths now (vessel `gas_capacity_m3` while alongside) | terminal | S — supersedes the #9 loadings *flow* with a *stock* |
| `gas_discharging_eu` | laden gas in EU import berths now | terminal | D — volume analog of #4 arrivals |
| `gas_in_transit_volume` | laden gas at sea US→EU (closed `[departed,arrived)` + open to now) | destination zone, undeclared → `unknown` | S/A — the #1/#2 lane as *volume* + the #3/#5 destination split |
| `gas_ballast_to_us` | empty carriers returning to reload, weighted by the capacity they'll carry | destination zone, undeclared → `unknown` | S (forward) — incoming US loading capacity ~1–2 wk out |

Decisions baked in:

- **Unit is `gas_capacity_m3`, no distance.** The ton-mile keys (#1/#2) and the
  count/age/O-D keys (#4/#9/#20/#5) are no longer the headline display.
- **Undeclared open legs are surfaced, not hidden.** ~90% of open legs never
  broadcast a destination; in `gas_in_transit_volume` / `gas_ballast_to_us` they
  occupy their own `unknown` band rather than being assumed NW-Europe. (The leg
  classifier in `legs.py` still applies its NW-Europe fallback *window* for
  phantom-censoring — that is decoupled from the display banding here.)
- **Banding is direction-aware.** A declared destination is only trusted for the
  band when it agrees with the leg's direction — a laden in-transit leg accepts
  an *import*-zone destination, a ballast return an *export*-zone one. This
  defends against the common case where a master sets the declared destination to
  the *next load port* (a US terminal) while a laden voyage is still completing,
  which would otherwise mis-band a Europe-bound cargo as `usgulf`. Mismatched
  declarations fall to `unknown`.
- **Berth visits are phantom-censored.** An open visit (a `moored` with no
  observed `departed`) is capped at `OPEN_VISIT_CEILING_DAYS = 5` so a
  missed-departure (AIS dropout while alongside) — the berth analog of the
  open-leg phantom in §4 — can't pin a terminal's band forever.
- **`gas_ballast_to_us` is approximate on destination.** Undeclared ballast
  returns inherit the import-region voyage window in `legs.py` (not yet
  direction-aware for US-bound legs); they're banded `unknown`. A US-side voyage
  window is a follow-up.

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

> **No historical depth (live-cutover-only).** #12–16 need `anchorage_entry`,
> which only the live state machine emits — GFW voyage arcs can't produce it (§0·6·1).
> So these series begin at the 2026-05-30 `mmsi_filter` cutover with no backfill
> training set, unlike the US queue signals (#6–8), which NOAA reconstructs to 2015.
> Per §3.5 of `PLAN.md`, EU queue time is structurally near-zero in normal markets
> (laden boil-off forces pre-coordinated berthing), so the missing history is a
> minor gap rather than a hole in the thesis.

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
> live: (a) `pipeline/legs.py` applies a **per-O-D voyage window** (US→EU ~18 d,
> `OD_WINDOW_DAYS`) beyond which an open leg is reclassified by last-fix evidence,
> with a 30-day global cap (`CENSOR_OPEN_DAYS`) only as the last resort when no
> destination — declared *or* assumed — is available. A leg with no declared
> destination inherits the same `FALLBACK_DEST_REGION` (NW Europe) the signal layer
> uses to estimate its distance, so it can never be distanced as NW-Europe-bound
> yet kept alive on the looser global cap; (b) `scoring.py` **pins** any vessel with
> a recent open laden leg into a persistent subscription slot
> (`priority_watchlist.is_pinned`) so the new scheme re-acquires it on the European
> approach. The pin only helps the *new* regime, not the historical phantoms.

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
  event-level, and validatable against EIA — on the live window now, and on the
  **NOAA-backfilled US decade** (2016+) once it lands.
- **The spread model: premature on live data, trainable with the backfill.** On
  the **live-only corpus** (`N ≈ 60` daily rows worth `N_eff ≈ 7`, plus the
  2026-05-30 seam) training a spread model is premature — the right tools are
  shrinkage + Bayesian priors + honest uncertainty (regularised regression,
  Bayesian structural time series, regime detection), not a point forecast. The
  **historical backfill** lifts this to `N ≈ 3,000` (`N_eff ≈ 330`, 2017→now), at
  which point the spread model becomes a real fit — still uncertainty-first (the
  spread stays low-SNR), with the `gfw → mmsi_filter` EU fidelity step carried as a
  regime indicator. **Never train across a fidelity boundary** (§0.5 / §0·6·1).
- **The edge is the dataset, not the method:** a clean, low-latency, per-vessel
  AIS feed (≈1 s ingest lag vs vendors' hours-to-days) and 24–48 h-early
  outage / change-point detection (#36–#38) — not smooth-model R².

See `MODELS.md` for the full treatment, the small-sample maths, the non-tanker
control set, and the recommended build order.
