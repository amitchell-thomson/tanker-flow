# Modelling: tanker-flow → HH/TTF spread

Companion to [`SIGNALS.md`](SIGNALS.md). That doc is the catalogue of *what* is
measured (34 signals in `signal_daily`); this is *how* to turn it into a forecast of
the **Henry Hub / TTF spread**, with the maths per model and the small-sample
discipline the data demands.

---

## 0 · The corpus

The historical backfill changed the picture decisively, and **asymmetrically**:

| Side | Depth | `N` (daily) | `N_eff` (ρ=0.8) | Consequence |
|---|---|---|---|---|
| **US supply** (loadings, queues, in-transit, `net_export_pressure`) | NOAA 2016+ | ~3,000 | ~330 | a real fit — train + walk-forward CV |
| **EU demand** (arrivals/discharge) | GFW 2016+ | ~3,000 | ~330 | deep, but one fidelity step (`gfw→mmsi_filter`) |
| **EU queue + intent** (`*_queue_*`, `spread_thrust`, `declared_eu_share`) | live only | ~60 | ~7 | nowcast/enrichment only — *not* trainable yet |

`N_eff = N·(1−ρ)/(1+ρ) ≈ N/9` at ρ=0.8. The decade lifts the supply/demand side from
`N_eff≈7` to `≈330` (~50×) — the whole point of the backfill. The live-only tier
survives only because EU queue is structurally near-zero in normal markets.

Three facts follow and shape everything below:
1. **Parameters must stay scarce relative to `N_eff`** — even 330 effective points
   won't carry 34 collinear features unregularised.
2. **Buy information off the calendar grid** — event-level likelihoods (survival,
   point processes), cross-terminal pooling, and economic priors.
3. **Predict high-SNR things first** — the physical nowcasts (Part A) are
   near-deterministic and validate against EIA *now*; the spread (Part B) is low-SNR
   and confounded.

### 0·1 · Validated data caveats (signal-integrity + EIA cross-checks, 2026-06-17)

The sweep is green (`make validate-signals`: 71 PASS / 0 FAIL; Freeport-2022 and
seasonality reproduce). Two findings nonetheless change *how* the panel must be
modelled — both are **level/seasonality effects the `knowable`/confidence columns do
not capture**, so neither is caught by down-weighting `open_fraction`:

- **US capture gradient — early years systematically under-count.** `noaa` US loadings
  ÷ EIA-implied cargoes is **not flat across the decade**: ~95% (2016) → **~46% (2020)**
  → **~103–105% (2024–25)**, monthly within ±7% recently. The cause is NOAA terrestrial-
  AIS receiver density growing over time, so **pre-2022 US-supply *levels* are attenuated
  by up to ~2×**. This is an absolute bias, not censoring → it survives `open_fraction`.
  **It is recoverable, not a reason to discard pre-2022 — see §0·2 for the recipe.** The
  correction applies to *level/count/volume* signals only; ratio/timing signals (queue h,
  speed, turn-time, round-trip) read *observed* events where a representative sample
  suffices, so they are barely affected even in the sparse years. (See also the `'all'`
  regime double-count: read `regime='noaa'` for US, per §1.)
- **Tanker seasonality ≈ weather seasonality — a confounder, flagged.** The signals
  carry a strong, *correct* seasonal cycle: EU discharge winter/summer **1.20×**, gas-in-
  transit **1.48×** (in-transit leads *and* amplifies discharge — as a leading indicator
  should). But that cycle overlaps the spread's own weather seasonality, so a naive fit
  would just re-discover winter. **This makes the degree-day controls + FWL partialling
  of §2 load-bearing, not optional** — the claim is tanker edge *net of* weather, and the
  in-transit lead over discharge is the project's shot at beating a pure degree-day model.

### 0·2 · Recovering the early years — the best historical US-supply signal (NOAA × GFW × EIA)

The pre-2022 US under-count (§0·1) is recoverable because **three sources triangulate**,
and NOAA and GFW have **opposite, complementary coverage gradients** (US loadings as % of
EIA-implied; verified 2026-06-17):

| | 2016 | 2018 | **2019** | **2020** | 2021 | 2023 | 2025 |
|---|---|---|---|---|---|---|---|
| **NOAA** | 95 | 74 | **59** | **46** | 74 | 93 | 105 |
| **GFW**  | 65 | 69 | **79** | **93** | 59 | 33 | 14 |

NOAA (terrestrial) is sparse early, exhaustive now; GFW (satellite-fed) is strong early,
fades recently — crossover ~2021. **GFW captures the 2019–2020 trough that NOAA misses.**
EIA is the **exhaustive monthly ground truth** over the whole span. The recipe:

1. **Fuse, don't pick — a deduped NOAA∪GFW union.** NOAA carries 2022+, GFW carries the
   early trough. ⚠ The current `regime='all'` is **not** this: it over-counts (119–160%
   every year) because `reconcile.py`'s mmsi+time match is too tight and misses true
   NOAA↔GFW pairs (a loading shows at slightly different timestamps in each feed), so the
   union ≈ the *sum*. **Step 0 is a looser cross-source dedup** (wider time window; match
   on mmsi + terminal + day) so the union approaches the true ~100%, not the sum.
2. **Anchor the residual to EIA.** A clean union still won't be exactly 100%/month. Either
   (a) rescale the level signal by `1/capture_t`, or — preferred for the Part A count
   models — (b) carry `log(capture_t)` as a **Poisson/NB exposure offset** (`log λ = xβ +
   log(capture)`), so under-coverage lands on the offset, not the coefficients, and
   low-coverage months down-weight themselves with the right uncertainty.
3. **Vintage discipline.** Historical (`physical`/training) fit may use full EIA (hindsight).
   The live `knowable` series must use **trailing/point-in-time** EIA (it lags ~1–2 mo) to
   stay leakage-safe — but the early-year problem is historical, so this binds only the live tail.
4. **Carry a US fidelity indicator.** Using GFW for early US adds a `gfw→noaa` step to the
   previously single-fidelity US line — keep `regime`/source (or the capture covariate) as
   an indicator rather than silently concatenating (§1's "never blend fidelity").
5. **Scope.** Correct `gas_loading_us` / `us_loadings_count*` / the US in-transit bands only.
   GFW is coarse (port visits, **no coordinates/anchorage**), so it contributes early-year
   *counts/timing* but not fix-level queue/speed (speed already falls back to centroids).
6. **Acceptance test.** The recovered early years should reconcile three ways — our fused
   level ≈ EIA, and GFW's independent early count (≈600 cargoes in 2020) corroborates the
   rescaled NOAA (EIA implies 648). That NOAA × GFW × EIA agreement is the proof the early
   years are *recovered*, not fabricated.

**Net:** the best historical US-supply signal = **deduped NOAA∪GFW union, EIA-calibrated
(exposure offset), regime-indicated** — all six early years kept, not discarded.

### 0·3 · The failure modes "cleared to model" does *not* close

The green validation gate (§ Build order) certifies the **panel** is structurally sound.
It says nothing about the four ways the **modelling** can still manufacture a false edge.
These are researcher-side, not data-side — the `knowable` basis and the confidence
columns do not protect against any of them. Treat this subsection as binding protocol,
not advice.

1. **The real risk is episode memorisation, not `N_eff`.** Even `N_eff≈330` (§0)
   *overstates* independence **for the spread target**: the decade is, in spread-variance
   terms, a handful of macro regimes — 2016–19 glut, 2020 COVID, **2021–22 crisis/Ukraine**,
   2023+ normalisation — and 2021–22 carries most of the realised variance. A model that
   "works" full-sample has most likely just learned to fire on that one spike. **Mandate:**
   score skill *per-regime and out-of-regime*, never only pooled; a signal that survives
   only in 2021–22 is a 2021–22 dummy, not a leading indicator. (This is the empirical case
   for the B4 regime-switching models, not a footnote to them.)

2. **Lead time is a falsification test, not a hyperparameter.** Each pre-registered signal
   implies a *physical* lead — US loadings → EU arrivals at the ~14–18 d voyage time;
   `gas_in_transit_volume` → `gas_discharging_eu` at its convolution lag (§0·1 measured the
   in-transit→discharge amplification this predicts). **Fix each signal's lag from the
   mechanism *before* fitting.** If you instead let CV pick the lag that maximises in-sample
   fit, the lag becomes a hidden free parameter and a forking path; and any "best" lag that
   doesn't match the signal's causal lead time is evidence the relationship is spurious, not
   a result to keep.

3. **Researcher degrees of freedom is the leak `knowable` can't catch.** 34 signals × lags ×
   horizons × transforms × bases is *thousands* of implicit tests against ~a handful of real
   events — you **will** surface a chance-leading signal. Three hard rules:
   - **Pre-registration (C8) is binding, not aspirational.** Commit the hypothesis list —
     signal, expected sign, expected lead — to this file *before* fitting anything in Part B.
   - **One true holdout, looked at once.** Carve the most recent ~12–18 mo now, write the
     boundary down, and do not evaluate on it until the very end. Every peek-and-adjust
     spends it; a holdout you tuned against is just another training set.
   - **Never iterate toward a backtest/PnL number.** Tuning to a metric and re-running is the
     most insidious overfit of all — it routes the entire search through your own choices and
     no point-in-time discipline detects it. Decide the model from the *priors and the CV
     protocol*, not from which variant printed the best Sharpe.

4. **Forecasting the spread *level* is mostly autocorrelation.** A level fit posts a
   flattering R² that means almost nothing (yesterday's spread ≈ today's). The honest target
   is the forward **change** over horizon `h`, scored against the **AR(1)+controls** null
   (§2 / Part B). Decide level-vs-difference deliberately and make sure no undifferenced
   feature leaks the level back in.

> **And not yet:** do **not** compute tradeable PnL / Sharpe in this phase. With no
> transaction costs, spread liquidity, or capacity it will flatter every model and distract
> from the *only* question that is answerable now — **is there an incremental, point-in-time,
> out-of-sample lead over the §2 control set.** Tradability is a later, separate question;
> answering it early just adds degrees of freedom (#3).

---

## 1 · Reading `signal_daily` into a model (the non-negotiables)

The three panel dimensions are modelling inputs, not metadata. Get them wrong and the
backtest lies.

- **`basis='knowable'` for every feature, always.** It is the leakage-free
  point-in-time series (an open leg stays open; arrivals counted only once observed).
  `physical` embeds hindsight — it is for *validation targets and sanity only*, never a
  feature. The `signal_daily_live_vintage` log is the acceptance test: `knowable[d]`
  recomputed must equal what the live pipeline printed on `d`.
- **Never train across a `regime` fidelity seam.** Pick one regime, or carry `regime`
  as an indicator. The US side (`noaa`+`mmsi_filter`) is one fidelity → a clean line;
  the EU side has a real `gfw→mmsi_filter` step to *condition on*, not blend. Use
  `regime='noaa'` for US (the synthetic `'all'` double-counts).
- **Feed the confidence columns as observation variance.** `value_dispersion`
  (within-day spread), `open_fraction` (censoring exposure), `n_legs` (sample size)
  give each cell a noise estimate. This is the operational form of "measurement-error-
  aware": weight observations by `1/σ²`, down-weight high-`open_fraction` cells, and
  let an errors-in-variables / state-space model carry the noise explicitly rather
  than treating every day as equally certain. **This is the single biggest upgrade the
  new panel enables** — a model that ignores it will over-trust the thin, phantom-
  heavy live tail and the sparse early years.

---

## 2 · The target and controls (the gating dependency)

The spread model is meaningless without these — build them **before** any spread fit.

**The FX/unit trap (do first).** HH is USD/MMBtu, TTF is EUR/MWh — a naive difference
leaks EUR/USD as fake signal. Convert to a common basis:

```
TTF[$/MMBtu] = TTF[€/MWh] / 3.412 × (EUR/USD)
spread       = HH[$/MMBtu] − TTF[$/MMBtu]
```

**The control set** — the spread's non-tanker drivers; the claim worth making is "edge
*net of* these". All free/cheap, all with deep history:

| Control | Why | Source |
|---|---|---|
| Heating/Cooling degree-days (US + NW Europe) | dominant demand driver | NOAA / ECMWF |
| US storage | below-norm firms HH | EIA (`data/eia.py`, Phase 2) |
| EU storage | low fill firms TTF | GIE AGSI+ (daily, free) |
| Norwegian + Russian/N-African pipeline flow | marginal EU supply | ENTSOG / Gassco |
| Coal (API2) + EU carbon (EUA) | gas-to-coal switching | ICE/EEX EOD |
| Brent, EUR/USD | oil-indexed LNG, FX | public EOD |
| EU wind / French nuclear availability | gas-for-power burn | ENTSO-E |
| Winter dummy, lagged spread (AR term) | seasonality, persistence | derived |

**Partial out the confounders (Frisch–Waugh–Lovell).** To show a tanker signal `T`
adds edge over controls `Z`: (1) regress spread on `Z` → residual `ỹ`; (2) regress `T`
on `Z` → residual `T̃`; (3) regress `ỹ` on `T̃` — a non-zero coefficient is edge net of
weather/storage. The ML analogue: fit with controls + tanker signals, then SHAP /
permutation importance for the *incremental* lift over a controls-only model.

> **Not optional here (see §0·1).** The tanker panel carries its own strong seasonality
> (gas-in-transit winter/summer ≈ 1.48×) that overlaps the spread's weather cycle, so
> without degree-day partialling a fit re-encodes winter rather than tanker edge. The
> degree-day controls are the highest-priority entry in the table above for that reason.

---

## Part A · Physical nowcasts (high-SNR, validate today) - worth adding destination estimation?

Target: next-week US exports, EU arrivals 1–2 weeks out, queue/berth durations.
Mechanically constrained → high SNR, validatable weekly against EIA, on the decade.

**A1 · Kinematic ETA propagation — the physics baseline (no training).** Each open
laden leg has a known origin and (declared/assumed) destination; forecast arrival from
great-circle distance and observed speed, `t̂ = t_dep + d_gc/v̄`, and convolve per-leg
arrival densities into a weekly **arrival-count distribution** (Poisson-binomial,
closed-form mean/variance). Feeds off `gas_in_transit_volume` + `laden_voyage_age_d` +
`voyage_speed_kn`. No fitted parameter — validate within weeks as legs complete.

**A2 · Count regression — Poisson / Negative Binomial GLM.** Weekly arrivals/loadings
per terminal: `log λ = β₀ + xᵀβ + log(exposure)`; NB adds dispersion `k`
(`Var = μ + μ²/k`) for the clustering. Few parameters, count-matched likelihood,
exposure offset for unequal windows. Targets `us_loadings_count` / `od_flow_count`.

**A3 · Survival models — queue and berth time.** Cox PH `h(t|x)=h₀(t)·exp(xᵀβ)`
(partial likelihood cancels the baseline) or Weibull AFT for a parametric small-data
fit. The unit is the *event* (hundreds, not dozens of days), and right-censoring
handles in-progress visits natively — which is exactly what `open_fraction` /
`estimated_fraction` flag on `load_queue_h` / `*_berth_turn_h`. **Pool across terminals
hierarchically** (Part C) — the highest-leverage move here.

**A4 · Kalman / state-space — latent flow-rate nowcast.** Treat "true current export
rate" as a hidden state observed noisily through daily counts: `x_t=Fx_{t−1}+w`,
`y_t=Hx_t+v`. Two–three parameters, **handles AIS-dropout days for free** (skip the
update), online, calibrated bands. Set the observation noise `R` from the confidence
columns. Ideal "current US export rate ± band" readout.

**A5 · BOCPD outage detection (high asymmetric payoff).** Outages dominate realised
spread variance. Bayesian Online Change-Point Detection tracks the run-length posterior
`P(r_t|y_{1:t})` — almost no training history needed. Feeds off `days_since_departed`,
`us_queue_formation_wow`. Being 24–48 h early on a Freeport-style outage beats any
smooth R².

---

## Part B · Spread models (low-SNR, small-data-hostile)

Target: HH−TTF level / first-difference at 1- and 4-week horizons. Features = the
tanker composites (`spread_thrust`, `net_export_pressure`, `implied_storage_build`,
`diversion_arbitrage`) + the pre-registered primitives + Part 2 controls, all on
`basis='knowable'`. Anything fancy must beat an **AR(1)+controls** baseline on
walk-forward CV.

**B1 · Regularised regression (Ridge / Lasso / Elastic Net).** `β̂ = argmin
(1/2N)‖y−Xβ‖² + λ[α‖β‖₁ + ½(1−α)‖β‖²₂]`. The canonical small-`p/N` tool; λ by
walk-forward CV; **group-lasso** whole signal families in/out together. The honest
default to start.

**B2 · Bayesian structural time series — best fit for this sample.** Spread =
local-level + seasonal + regression + noise; **spike-and-slab** prior on each `β_j`
gives a **posterior inclusion probability** ("does this signal matter?") instead of a
fragile p-value, and the full predictive *distribution* a hedger wants. Priors degrade
gracefully as data thins. This is the recommended T2 model.

**B3 · PLS / PCR — dimension reduction.** Collapse the collinear signals to 2–4
supervised factors before regressing (`Cov(Xw,y)`-maximising), cutting effective `p`
from ~34 to ~3. Good cross-check on B1/B2.

**B4 · Regime-switching (HMM / Markov-switching).** `y_t = x_tᵀβ_{s_t} + ε`; a 2–3
state hidden chain captures the outage/freeze regimes the spread actually lives in.

**B5 · Constrained gradient-boosting (LightGBM) — deferred.** Only viable with
**monotonic constraints** (inject sign priors, e.g. load-queue↑ ⇒ spread↑), stumps,
strong L1/L2, early stopping on walk-forward CV. A post-data-growth model / sanity
check, not the production nowcast. **Never** deep sequence models on a regime-broken,
autocorrelated spread — they overfit and the "Transformer wins" papers usually leak.

---

## Part C · Cross-cutting techniques (buy effective sample size)

1. **Walk-forward / expanding-window CV only**, with **purge + embargo** (drop training
   rows whose target window overlaps the test window). Never k-fold time series.
2. **Hierarchical pooling across terminals** — `β_terminal ~ N(β_global, τ²)`. Each
   terminal shrinks toward the global mean by its data volume; *multiplies* effective
   data. Natural for the A3 survival models.
3. **Confidence-weighting (new).** Weight every observation by `1/σ²` from
   `value_dispersion`/`n_legs`; down-weight high-`open_fraction` cells. Replaces the old
   "discard uncertain rows" with "keep and down-weight".
4. **Escape the daily grid** — event-level likelihoods (A3/A2) beat daily aggregates.
5. **Economic priors as hard constraints** — monotonicity (B5), sign priors,
   spike-and-slab (B2). Each removes hypotheses before the data is seen.
6. **Two-stage** — predict the high-SNR physical target (Part A) first, use it as an
   input to the spread model (Part B); the physical stage is validatable now and
   stabilises the noisy stage.
7. **Honest uncertainty** — Bayesian posteriors + model averaging over B1–B3; report
   intervals. Use Newey–West (HAC) errors for any reported coefficient.
8. **Pre-register the signals you believe** — commit up front to the mechanically-
   motivated few (`gas_in_transit_volume`, `gas_discharging_eu`, `load_queue_h`,
   `laden_voyage_age_d`, `spread_thrust`) so 34 candidates × thin data don't surface
   chance correlations.

---

## Build order

**Done.** ✅ Signals built (34 keys, dual-basis, confidence-instrumented); basis/regime/
confidence machinery in place. ✅ Validation sweep green (`make validate-signals`,
`analysis/VALIDATION.md`) — structural/coverage/range/leakage/confidence pass, Freeport-
2022 + seasonality reproduce. **Gate cleared: cleared to model.**

### Decisions locked
- **TTF source = Barchart Premier, ~$30 one-time** (Build C). Subscribe one month, CSV-
  export the TTF daily futures series (Barchart root `TG` / native `TFM`) 2016→present,
  cancel. Historical EOD download is included; the $155/mo ICE Endex fee is real-time-
  only, so no exchange fee on history. Gives **clean daily TTF across the whole 2016+
  panel** — no monthly-interpolation seam. **License: raw TTF CSV stays out of git**
  (untracked `data/private/`); only derived signals/charts are publishable.
- **Spread cadence = daily**, clean over the entire panel.

The work now runs as **two parallel tracks** — Part A nowcasts need neither TTF nor
controls (they validate against EIA), so TTF resolution never gates modelling progress.

### Track 1 — control set + spread target (new-data integration)
Thin loaders, siblings to `data/eia.py` (pure parse + `merge` + upsert). EIA series →
existing `eia_series`; non-EIA series → a shared `market_series` table (same key shape);
one assembler joins all onto the `signal_daily` daily grid into a new `model_panel`.

1. **EIA Phase 2** (do first, ~30 min, zero-risk) — `--probe storage_l48` / `--probe
   hh_spot` to verify the unverified v2 routes, fix the registry entry on a 404, backfill.
   Yields Henry Hub spot + US Lower-48 storage.
2. **TTF** — one-time Barchart CSV pull into `data/private/`; `data/ttf.py` = pure CSV
   parser → `market_series` upsert. Wire **World Bank Pink Sheet** monthly TTF (CC-BY,
   free) purely as a monthly-resolution cross-check on the daily series.
3. **EUR/USD FX** — FRED `DEXUSEU` (or Yahoo `EURUSD=X`), daily EOD.
4. **Spread target** — `spread = HH[$/MMBtu] − TTF[€/MWh]/3.412 × EUR/USD` (§2). Clean
   daily 2016+. Unblocks Part B.
5. **GIE AGSI+** EU storage (free API, registration key) → `market_series`.
6. **Degree-days** US + NW Europe (NOAA free for US; NW-Europe source TBD) — Track-1 tail,
   a control not the target.

### Track 2 — Part A physical nowcasts (start now, in parallel; validate on the decade)
Walk-forward CV, `basis='knowable'`, confidence columns as observation variance.
7. **A1 kinematic ETA** — no training; physics baseline; validates within weeks.
8. **A2 Poisson/NB arrivals**, **A3 Cox/Weibull survival** on queue/berth with **C2
   hierarchical pooling across terminals** (highest-leverage move), **A4 Kalman flow-rate**.
9. **A5 BOCPD outage nowcast** — cheap, online, asymmetric payoff (Freeport-2022 labelled).

### Track 3 — Part B spread model (after Tracks 1 & 2)
10. **AR(1)+controls baseline** — the null; FWL partial-effect harness.
11. **BSTS (B2)** with spike-and-slab over the pre-registered signals + controls,
    confidence-weighted; cross-check Elastic Net (B1) + PLS (B3). Report posterior
    inclusion probabilities + predictive intervals. Two-stage: feed the Part A nowcast in.
12. **Defer** constrained LightGBM (B5) and cross-exciting Hawkes until the event count is
    in the tens of thousands.

### First moves (decision-free)
1. Buy Barchart Premier + download the TTF CSV (~$30, ~15 min) — the only money/manual step.
2. EIA Phase 2 probe + backfill (~30 min) — HH spot + US storage.
3. Start A1 kinematic ETA in parallel — no dependencies, validates fastest.

### Gate items (tracked, not blocking)
- EIA capture-rate firms when EIA's June-2026 data publishes (~late summer 2026).
- Vintage self-validation accrues with the live tail.

The deliverable arc: **confirm edge** (Part A nowcasts + FWL partial effects, defensible
now on the decade) → **forecast the spread** (Part B, Bayesian and uncertainty-first
because the spread stays low-SNR regardless of `N`). The EU fidelity seam and the Part 2
control-partialling matter *more* on a long panel, not less.
```
