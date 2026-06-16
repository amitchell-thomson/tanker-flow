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

---

## Part A · Physical nowcasts (high-SNR, validate today)

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

0. **Refresh complete (this doc).** Signals built; basis/regime/confidence machinery in
   place.
1. **Validation sweep** — confirm the 34 signals are model-ready (see
   `analysis/VALIDATION.md`): capture-rate vs EIA, the knowable self-validation, range/
   coverage/sign checks. **Gate: do not model until green.**
2. **Target + controls** — FX/unit-consistent spread (§2) + EIA Phase 2 + AGSI +
   degree-days assembled into the daily panel.
3. **AR(1)+controls baseline** — the null; FWL partial-effect harness.
4. **Physical nowcasts (Part A)** — kinematic ETA + Poisson/NB + survival, validated
   against EIA on the decade. The defensible "models working today" deliverable.
5. **BOCPD outage nowcast (A5)** — cheap, online, asymmetric payoff.
6. **Hierarchical pooling (C2)** across terminals for the survival models.
7. **Spread model — BSTS (B2)** with spike-and-slab over the pre-registered signals +
   controls, confidence-weighted; cross-check Elastic Net (B1) and PLS (B3). Report
   posterior inclusion probabilities and predictive intervals.
8. **Defer** constrained LightGBM (B5) and full cross-exciting Hawkes until the event
   count is in the tens of thousands.

The deliverable arc: **confirm edge** (Part A nowcasts + FWL partial effects, defensible
now on the decade) → **forecast the spread** (Part B, Bayesian and uncertainty-first
because the spread stays low-SNR regardless of `N`). The EU fidelity seam and the Part 2
control-partialling matter *more* on a long panel, not less.
```
