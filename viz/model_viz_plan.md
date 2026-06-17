# Model-observation viz — design intent (deferred)

> **Status: not built.** This is the forward plan for *observing the models working* on
> the viz, captured so it isn't lost. It lands as the Part A/B models come online
> (`analysis/MODELS.md`). Nothing here is implemented yet. The signal-viewing surface
> (all 34 signals by family) and the collapsible map panels are the built pieces; this
> doc is the third act.

The pitch this surface makes is the whole project's headline: *"trained on a decade of
vessel movements, then forward-tested on a live AIS feed the model had never seen."*
(`analysis/RESEARCH_PLAN.md` §0.) The viz is where that sentence becomes something you
can watch happen.

## What it rides on (already here)
Everything below reuses infrastructure the signals view already established — so this is
additive, not a rebuild:
- Chart.js + the `chartjs-plugin-annotation` (the `regime change` seam line is the
  template for change-point / forecast-boundary markers).
- The card + contributor-drawer pattern (click a value → trace what produced it).
- The 60 s stale-while-revalidate poll and the cached signal-context loader in `app.py`.
- The dual-basis (`physical`/`knowable`) + `regime` panel API.
- A new nav view slots in beside Map / Signals exactly as `js/signals.js` did; the
  CLAUDE.md note about `js/signal.js` slotting in "without further plumbing" applies.

## The surfaces

### 1 · Nowcasts view (Part A — "models working, live, now")
A third top-bar view. One card per physical nowcast, each showing **predicted vs
realized** with the prediction-interval band:
- **A2 arrivals (Poisson/NB)** — predicted weekly arrivals/loadings per terminal vs the
  count that actually landed; validated against EIA on the decade.
- **A3 queue / berth survival (Cox/Weibull)** — predicted wait/turn-time distribution vs
  observed, right-censoring the in-progress visits (the `open_fraction` cells).
- **A4 Kalman flow-rate** — "current US export rate ± band" as a live readout; the band
  comes straight from the confidence columns as observation noise.
- **A1 kinematic ETA** — the no-training physics baseline, per open laden leg, convolved
  into the weekly arrival-count distribution.

Because these are event-level (hundreds of events, not dozens of days) they validate in
**weeks** — this view is the defensible "it works" artifact long before the spread model
has the power to be judged.

### 2 · Model overlays on the existing signal charts
Don't build separate charts where an overlay tells the story on the signal itself:
- Extend `gas_in_transit_volume` forward with the **kinematic-ETA arrival forecast** — a
  dashed forward continuation of the at-sea stock as cargoes are predicted to land.
- Drop **BOCPD change-point markers** (A5) as vertical annotations on the outage signals
  (`days_since_departed`, `us_queue_formation_wow`) — reuse the seam-annotation
  mechanism. Being 24–48 h early on a Freeport-style outage is the asymmetric payoff.

### 3 · Spread view (Part B)
The destination. HH−TTF actual vs model forecast with the **predictive interval** (the
hedger's distribution, not a point):
- **Forecast track** — BSTS / Elastic Net forecast at 1- and 4-week horizons over the
  actual spread.
- **FWL partial-effect panel** — each tanker signal's contribution *net of* the
  weather/storage controls (`analysis/MODELS.md` §2), so the chart shows edge, not the
  rediscovered weather.
- **Posterior inclusion probabilities** — the BSTS spike-and-slab PIP per signal as a
  bar: "does this signal matter?", visually, instead of a fragile p-value.

### 4 · Forward-test scoreboard
The single strongest portfolio number: live out-of-sample error accruing in real time
(`analysis/RESEARCH_PLAN.md` §6). Physics confirmed in weeks, spread accumulating over
months — report widening/narrowing confidence as it grows, never a premature verdict.
A small "trained-through / live-since" provenance chip makes the hold-out boundary
legible (and guards the eye against reading the NOAA-overwrites-live lookahead trap, §3.2).

## Plumbing when the time comes
- New endpoints: `/api/nowcasts`, `/api/spread`, `/api/forward-test` — thin readers over
  whatever tables the model fits write (e.g. a `model_forecast` / `nowcast_eval` table).
- New `js/models.js` + a `#view-models` section; wire into the nav the same way
  `initSignals()` is.
- Keep the leakage discipline visible: everything model-facing reads `basis='knowable'`
  and a single `regime`; the viz should label which basis/regime a forecast was made on
  so a viewer can't mistake a hindsight fit for a live one.

## Sequencing
This waits on `analysis/MODELS.md` build order: Track 1 (control set + spread target) and
Track 2 (Part A nowcasts) must produce model output before any of the above has data to
show. Surface 1 (Nowcasts) comes first — it has data the soonest and validates fastest;
Surface 3 (Spread) is last, gated on the spread model and accruing its verdict slowly.
