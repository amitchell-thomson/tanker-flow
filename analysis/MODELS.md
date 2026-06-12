# Modelling approaches for tanker-flow → HH/TTF spread

Companion to [`SIGNALS.md`](SIGNALS.md). That doc enumerates *what* can be
measured from `port_events` + `ais_fixes`. This doc covers *how* to turn those
measurements into predictions, with the maths behind each model and — the part
that matters here — **why it can extract signal from only a few months of
data**, plus how to control for the many non-tanker drivers of the spread.

---

## 0 · The small-sample problem, stated precisely

> **Two corpora — keep them separate (read this first).** Every sample-size figure
> in this doc is one of two things, and they differ by ~50×. The whole §0 argument
> is about the **live-only** corpus; the historical backfill
> (`ingestion/historical/PLAN.md`) supplies the other.
>
> | Corpus | Span | `N` (daily rows) | `N_eff` (ρ=0.8) | What it's for |
> |---|---|---|---|---|
> | **Live-only** (as of 2026-06-12) | 2026-04-14 → now | **≈ 60** | **≈ 7** | the only data for signals with no historical source (EU queue #12–16) and the live tail in isolation |
> | *reference: one full clean year* | — | 250 | ≈ 28 | the autocorrelation ceiling — even a year is thin |
> | **With historical backfill** | 2017 → now | **≈ 3,000** | **≈ 330** | training/validating every signal that *has* a historical source (all of Part II + the spread model) |
>
> `N_eff = N·(1−ρ)/(1+ρ) ≈ N/9` at ρ=0.8 — so live `N ≈ 60` is worth only
> **≈ 7** independent points, while the backfilled `N ≈ 3,000` is worth **≈ 330**.
> That ~50× jump in effective information is the entire reason for the backfill.
>
> The backfill changes the picture **asymmetrically** — read the rest of the doc
> through this lens:
>
> - **US supply side — the small-sample problem dissolves.** NOAA gives exhaustive
>   Class-A history to 2016 (US LNG exports begin then) and *retroactively
>   overwrites* the throttled `bbox` window, so the US export signals (#6–11,
>   `gas_loading_us`) become a clean, **seam-free decade** at the same fidelity as
>   the live feed. The §1–§5 physical nowcasts (kinematic ETA, Poisson/NB counts,
>   Cox/Weibull survival, Hawkes) can be **trained and walk-forward-validated on
>   years, not weeks** (`N_eff` in the hundreds), and the hierarchical terminal
>   pooling (Part IV.2) finally has the data to bite.
> - **EU demand side — deeper but coarser, with one real seam.** GFW voyage arcs
>   give EU *arrival counts* to 2017 but no queue/berth internals — **#12–16 stay
>   live-only** (`N_eff ≈ 7`; see SIGNALS.md §0·6·1). The `gfw → mmsi_filter`
>   boundary is a genuine **fidelity step** that must enter EU models as a regime
>   indicator — not a seam you train across, but one you *condition on*.
> - **Spread model — moves from "premature" to "trainable."** With NOAA (2016+) +
>   GFW (2017+) + the control set (EIA, GIE AGSI, degree-days, all with deep
>   history), the daily panel goes from live `N ≈ 60` (`N_eff ≈ 7`) to **≈ 3,000
>   rows** (`N_eff ≈ 330`). Part III's framing flips from *"confirm edge only"* to
>   *"fit and validate with walk-forward CV"* — the shrinkage/Bayesian tooling
>   stays the right default (the spread is still low-SNR and confounded), but it is
>   no longer a placeholder. The one caveat that survives in full force: the EU
>   fidelity seam and the Part V control-partialling matter *more*, not less, on a
>   long panel.

Everything below is shaped by one number: the effective sample size. **Unless a
passage is explicitly tagged "with backfill", every `N` / `N_eff` figure in §0 is
the live-only corpus** (≈ 60 rows / `N_eff ≈ 7` as of 2026-06-12) — the regime
that has no historical source, and the worst case the modelling must survive.

For an AR(1) series with autocorrelation `ρ`, the information content of `N`
observations about the mean is only

```
N_eff ≈ N · (1 − ρ) / (1 + ρ)
```

With `ρ = 0.8` (typical for a daily spread level), the live `N ≈ 60` carries the
information of just `N_eff ≈ 7` independent points; even a *full clean year*
(`N = 250`) tops out at `N_eff ≈ 28`. **This is the central fact** for the
live-only regime. It means:

1. **Parameters must be scarce.** OLS variance is `Var(β̂) = σ²(XᵀX)⁻¹`; with
   `p` features and `N_eff` effective rows, the variance of each coefficient
   blows up as `p → N_eff`. With 40 candidate signals and live `N_eff ≈ 7`,
   unregularised regression is pure noise-fitting (the backfilled `N_eff ≈ 330`
   is what lifts this constraint for any signal with a historical source).
2. **You must buy information from somewhere other than calendar time.** Three
   escape hatches recur throughout this doc:
   - **Event-level data** (survival, point processes) — hundreds of *events*
     instead of dozens of *days*.
   - **Cross-sectional pooling** (hierarchical models) — 33 terminals sharing
     statistical strength.
   - **Economic priors** (monotonicity, sign constraints, Bayesian priors) —
     shrink the hypothesis space before the data is even seen.
3. **Predict high-SNR things first.** The physical nowcasts (Part II) have a
   near-deterministic signal; the spread itself (Part III) is low-SNR and
   confounded. Validate the pipeline on the former before betting on the latter.

Two model families follow this split: **Part II** (physical, small-data
friendly) and **Part III** (spread, small-data hostile but approachable).

---

## Part II · Models for physical nowcasts (high SNR, work *now*)

Target family **T1**: next-week US LNG exports, EU arrivals over the next 1–2
weeks, per-terminal queue/berth durations. These are mechanically constrained,
so the signal-to-noise ratio is high and you can validate weekly against EIA.

### 1 · Kinematic ETA propagation — the physics baseline (no training data)

**What.** Not machine learning — deterministic mechanics, and the strongest
small-data tool you have. Each open laden leg (`departed`, no terminating
`zone_entry`) has a known origin and a declared/destination zone. Forecast its
arrival from great-circle distance and observed speed:

```
t̂_arrival(i) = t_departed(i) + d_gc(origin_i, dest_i) / v̄_i
```

where `v̄_i` is the vessel's recent mean SOG (or a fleet prior, e.g. 13–19 kn).
Aggregate over all open legs to a forecast **arrival count distribution** for
each EU terminal and week:

```
N̂_arrivals(zone, week) = Σ_i  1[ t̂_arrival(i) ∈ week ] · P(dest_i = zone)
```

**Maths driving it.** Pure kinematics plus a convolution of per-leg arrival-time
densities. If each leg's arrival time is `t̂_i ± σ_i` (Gaussian from speed
uncertainty), the weekly count is Poisson-binomial over the legs, whose mean and
variance are closed-form.

**Why it works on a few months.** It needs essentially **no training data** —
the relationship is physical law, not a fitted parameter. A vessel that left the
US Gulf laden 10 days ago *will* reach Europe in ~3–5 more days. You can validate
it *within your existing 6-week window* as legs complete. This is your
ground-truth anchor and your first "model working today."

> Requires `departed` events → gated on the approach polygons. The import-side
> count (arrivals already landed) is computable today.

### 2 · Count regression — Poisson / Negative Binomial GLM

**What.** Model weekly event counts (arrivals, loadings) per terminal as a
function of recent congestion, season, and the kinematic forecast from §1.

**Maths.** Poisson GLM with a log link:

```
Y ~ Poisson(λ),   log λ = β₀ + xᵀβ + log(exposure)
```

Counts are usually *overdispersed* (variance > mean) because of clustering, so
use Negative Binomial, which adds one dispersion parameter `k`:

```
Var(Y) = μ + μ²/k        (k → ∞ recovers Poisson)
```

Fit by maximum likelihood (IRLS for Poisson; one extra parameter for NB).

**Why it works on a few months.** Very few parameters (a handful of `β`s + `k`),
a likelihood matched to count data (no Gaussian misspecification on small
integers), interpretable coefficients, and an `exposure`/offset term to handle
unequal observation windows. This is the right "arrivals-per-week" nowcast — far
better calibrated than a naive count.

### 3 · Survival / duration models — queue time and berth time

**What.** Model **time-at-queue** (`moored − anchorage_entry`) and
**time-at-berth** (`departed − moored`) per terminal. The unit of observation is
an *event*, not a day — you already have ~180 `moored` / ~110 `anchorage_entry`
events, which is enough to start.

**Maths — Cox proportional hazards.** The hazard (instantaneous rate of leaving
the queue/berth at time `t`) is

```
h(t | x) = h₀(t) · exp(xᵀβ)
```

`β` is estimated from the **partial likelihood**, which cancels the unknown
baseline `h₀(t)` entirely — this is the key efficiency:

```
L(β) = Π_i  exp(xᵢᵀβ) / Σ_{j ∈ R(tᵢ)} exp(xⱼᵀβ)
```

(`R(tᵢ)` = the risk set still queued at `tᵢ`). For a parametric small-data
alternative, **Weibull AFT** models log-duration directly:

```
log T = xᵀβ + σ·ε,   ε ~ standard extreme-value
```

**Why it works on a few months.** (a) The sample unit is the *event*, so you
have hundreds, not dozens. (b) Cox is *semiparametric* — it spends no parameters
on the baseline hazard shape. (c) **Censoring** is handled natively: a vessel
currently still in the queue is a right-censored observation, not discarded —
so in-progress visits add information. This is the "median loading time at
Sabine, and it's lengthening" signal, with proper statistics.

### 4 · Self-exciting point processes — Hawkes for arrivals

**What.** Model the *stream of arrival timestamps* at terminals as a process
that excites itself and its neighbours (tide windows, weather systems, fleet
rotation cluster arrivals).

**Maths.** Conditional intensity for terminal `i`:

```
λ_i(t) = μ_i + Σ_j Σ_{t_k^j < t}  α_ij · exp(−β (t − t_k^j))
```

`μ_i` = background rate, `α_ij` = excitation of `i` by an event at `j`, `β` =
decay. Fit by maximising the point-process log-likelihood

```
ℓ = Σ_k log λ(t_k) − ∫₀ᵀ λ(s) ds
```

**Why it could work on a few months.** It consumes **every individual event
timestamp** — hundreds to thousands — rather than weekly aggregates, so the
effective sample is far larger than the daily-grid models. **Caveat:** the full
cross-excitation matrix has `K²` parameters (too many for 33 terminals on weeks
of data). Make it viable by sharing one decay `β`, restricting `α_ij` to
geographic neighbours, and L1-penalising the matrix. Start with a single-terminal
self-exciting model and only add cross terms as data grows.

### 5 · State-space / Kalman filter — latent flow-rate nowcast

**What.** Treat the "true current export rate" as a hidden state observed
noisily through daily event counts; filter it in real time.

**Maths.** Linear Gaussian state space:

```
state:        x_t = F x_{t−1} + w_t,   w_t ~ N(0, Q)
observation:  y_t = H x_t   + v_t,     v_t ~ N(0, R)
```

The Kalman recursions give the optimal `x̂_t` and its variance:

```
predict:  x̂_t⁻ = F x̂_{t−1},   P_t⁻ = F P_{t−1} Fᵀ + Q
update:   K_t = P_t⁻ Hᵀ (H P_t⁻ Hᵀ + R)⁻¹
          x̂_t = x̂_t⁻ + K_t (y_t − H x̂_t⁻)
```

**Why it works on a few months.** Two or three parameters; handles **missing
data** for free (an AIS-dropout day = skip the update step) — directly relevant
to your terrestrial-AIS gaps; produces calibrated uncertainty bands; runs
online. Ideal for a live "current US export rate ± band" readout.

---

## Part III · Models for the spread directly (low SNR, small-data hostile)

Target family **T2**: HH/TTF spread, level or first-difference, at 1-week and
4-week horizons. Honest framing **on the live-only corpus** (`N_eff ≈ 7`): you will
not get a reliable point forecast, so the goal is **shrinkage + priors +
uncertainty**, not heroics. **With the backfilled panel** (`N_eff ≈ 330`, see §0)
this stops being a placeholder and becomes a real fit — but the same tooling still
applies, because the spread is low-SNR and confounded regardless of `N`. Anything
fancier must beat the §6 baseline on *walk-forward* CV.

### 6 · Regularised linear regression — Ridge / Lasso / Elastic Net

**What.** Linear spread model with a penalty that prevents overfitting the 40
collinear signals.

**Maths.** Elastic Net objective:

```
β̂ = argmin  (1/2N)·‖y − Xβ‖²  +  λ[ α‖β‖₁ + ½(1−α)‖β‖²₂ ]
```

- `α = 0` → **Ridge**, closed form `β̂ = (XᵀX + λI)⁻¹Xᵀy`. Shrinks all
  coefficients, keeps correlated features together.
- `α = 1` → **Lasso**, drives coefficients to exactly zero (feature selection).
- In between → Elastic Net, the practical default for correlated signals.

The bias–variance identity is the whole point: shrinkage *adds* a little bias to
*remove* a lot of variance, and at small `N` the variance term dominates MSE.

**Why it works on a few months.** Regularisation is *the* canonical small-`p/N`
tool. The penalty `λ` (chosen by walk-forward CV) explicitly controls model
complexity so 40 features don't overwhelm 28 effective rows. Use **group lasso**
to select whole signal families (all queue signals in or out together).

### 7 · Bayesian structural time series / dynamic linear models — *best T2 fit*

**What.** A state-space spread model with a regression component, spike-and-slab
priors for variable selection, and native uncertainty — the most appropriate
spread model for this small a sample.

**Maths.** Decompose the spread into latent components plus regressors:

```
y_t = μ_t (local level) + τ_t (seasonal) + xᵀ_t β + ε_t
μ_t = μ_{t−1} + δ_{t−1} + η_t      (trend evolves)
```

Priors do the regularisation. A **spike-and-slab** prior on each `β_j`,

```
β_j ~ (1 − π_j)·δ₀  +  π_j·N(0, τ²)
```

puts mass at exactly zero (the "spike") so the posterior reports a **posterior
inclusion probability** for each signal — a principled "does this signal
matter?" measure instead of a fragile p-value. Inference by MCMC / Kalman
smoothing.

**Why it works on a few months.** (a) Priors are explicit, tunable
regularisation that degrade gracefully as data shrinks. (b) You get the full
predictive *distribution* — exactly what a hedger wants, and honest about how
little 6 weeks tells you. (c) Spike-and-slab handles many candidate signals
without the multiple-testing trap. (d) The local-level component absorbs slow
drift so the regressors only have to explain deviations.

### 8 · Dimension reduction — PCA / PCR and PLS

**What.** Collapse the 40 correlated signals into 2–4 latent factors, then
regress the spread on the factors.

**Maths.** Factor model `X = ΛF + E`; factors `F` are the leading eigenvectors
of the signal covariance (PCA). **Principal Component Regression** then fits
`y = Fγ + u` using only the top `k` components. **Partial Least Squares** is the
supervised cousin — it chooses components maximising `Cov(Xw, y)` rather than
`Var(Xw)`, so it keeps only variation that predicts the spread.

**Why it works on a few months.** Reduces effective `p` from ~40 to ~3 *before*
the regression, so the parameter count fits `N_eff`. Your signals are highly
collinear (queues, throughput, ton-miles all co-move), so a few factors capture
most of the structure. PLS usually beats PCR here because supervision matters
when the dominant variance direction isn't the spread-relevant one.

### 9 · Gradient-boosted trees — LightGBM, *constrained*

**What.** The commodity-desk workhorse — but only viable at small `N` if heavily
shackled.

**Maths.** Additive stage-wise model:

```
F_m(x) = F_{m−1}(x) + ν · h_m(x)
```

where each weak learner `h_m` is fit to the negative gradient of the loss
(pseudo-residuals `−∂L/∂F`), and `ν` is the learning rate. The regularised
objective penalises tree complexity (`γ·#leaves + ½λ‖w‖²`).

**Why it *might* work on a few months — with discipline.** Out of the box it
overfits 250 rows instantly. The small-data levers that can rescue it:
- **Stumps** (`max_depth = 1–2`) — additive, low-variance.
- **Monotonic constraints** — force the sign of known relationships (e.g.
  loading-queue-time ↑ ⇒ spread ↑). This injects economic priors directly into
  the model and is the single biggest small-data lever.
- Small `ν`, high `min_child_weight`, strong L1/L2, **early stopping on
  walk-forward CV**.

**Honest verdict:** still risky below `N ≈ 250`. Treat as a *post-data-growth*
model; until then it's a sanity check, not the production nowcast.

### 10 · Regime detection — HMM / Markov-switching and BOCPD

**What.** Outages and freezes dominate realised spread variance. Detect regime
breaks rather than predict a smooth level.

**Maths — Markov-switching regression.** A hidden state `s_t ∈ {1..K}` with
transition matrix `A` selects the regression regime:

```
y_t = x_tᵀ β_{s_t} + ε_t,   ε_t ~ N(0, σ²_{s_t})
```

Fit by EM (Baum–Welch); decode states with Viterbi. **Bayesian Online
Change-Point Detection** instead tracks the posterior over the *run length*
`r_t` (time since the last break):

```
P(r_t | y_{1:t}) ∝ Σ_{r_{t−1}} P(r_t | r_{t−1}) · P(y_t | r_{t−1}) · P(r_{t−1} | y_{1:t−1})
```

with a hazard function `H(r)` setting the prior break rate.

**Why it works on a few months.** BOCPD is essentially **online and
prior-driven — it needs almost no training history**, which is exactly the
small-data situation. A 2–3 state HMM has few parameters. The payoff is
asymmetric: being 24–48 h early on a Freeport-style outage from `port_events`
(days-since-last-`departed`, sudden queue formation) is worth more than any
smooth-model R², and you don't need years to stand it up.

---

## Part IV · Cross-cutting techniques that buy you effective sample size

These are *methods*, not models — apply them across the board.

1. **Walk-forward / expanding-window CV only.** Never k-fold on time series — it
   leaks the future. Use **purged & embargoed** splits (drop training rows whose
   target window overlaps the test window) to kill leakage from overlapping
   multi-day horizons.
2. **Hierarchical / partial pooling across terminals.** Instead of one
   data-starved model per terminal, fit one multilevel model:

   ```
   β_terminal ~ N(β_global, τ²)
   ```

   Each terminal's estimate shrinks toward the global mean by an amount set by
   its own data volume (James–Stein / empirical-Bayes shrinkage). This
   **multiplies** effective data — the 33 terminals inform each other. One of the
   highest-leverage small-data moves available, and a natural fit for the
   queue/berth survival models (§3).
3. **Escape the daily grid.** Prefer event-level likelihoods (survival §3, Hawkes
   §4) wherever the question allows — hundreds of events beat dozens of days.
4. **Two-stage / transfer learning.** Predict the high-SNR physical target first
   (Part II), then use *that* as the input to the spread model (Part III). The
   physical stage is validatable now and stabilises the noisy stage.
5. **Economic priors as hard constraints.** Monotonicity (§9), sign priors,
   spike-and-slab (§7). Every constraint removes hypotheses the data would
   otherwise have to rule out.
6. **Honest uncertainty + model averaging.** Bayesian posteriors (§7) and
   Bayesian model averaging across §6–§8 are more robust than committing to one
   fragile point estimate. Report intervals, not just means.
7. **Autocorrelation-robust inference.** When you do report a coefficient's
   significance, use Newey–West (HAC) standard errors, or the relationship will
   look more certain than it is.
8. **Pre-register the signals you believe.** With 40 candidates and weeks of
   data, some *will* correlate by chance. Commit up front to the mechanically-
   motivated few (#1 ton-miles, #4 arrivals, #6 queue time, #20 voyage age, #42
   thrust) and validate those.

---

## Part V · Controlling for non-tanker drivers of the spread

Tanker flow is **one of several** drivers of HH/TTF. To claim the tanker signal
carries edge, you must show it predicts the spread **net of the obvious
confounders** — otherwise you've just rediscovered the weather. All of the
following are free or cheap and should enter every spread model as controls.

### V.1 · The easy-to-get control set

| Control | Why it moves the spread | Source (free) |
|---|---|---|
| **Heating/Cooling Degree Days** (US + NW Europe) | The dominant driver of gas demand — cold snaps swamp flow effects | NOAA (US), ECMWF/national met, or a degree-day API |
| **US storage** | Below-5yr-norm storage firms HH | EIA weekly (your `data/eia.py`) |
| **EU storage** | Low EU fill firms TTF, widens spread | GIE **AGSI+** API — daily, by country, free |
| **Norwegian pipeline flow** | The marginal EU supply alongside LNG | Gassco / ENTSOG transparency platform |
| **EU pipeline imports (Russia/TurkStream/N. Africa)** | Substitute for LNG into Europe | ENTSOG |
| **Coal (API2) + EU carbon (EUA front)** | Gas-to-coal switching sets a TTF floor/ceiling in power | ICE/EEX EOD; some public proxies |
| **Brent crude** | Oil-indexed LNG contracts; macro energy beta | Public EOD |
| **EU wind generation / nuclear (France) availability** | Low wind / nuclear outages → gas-for-power burn | ENTSO-E transparency |
| **EUR/USD** | *Mechanical* — see V.3 | Public EOD |
| **Calendar** | Winter dummy, month, day-of-week seasonality | derived |
| **Lagged spread (AR term)** | The spread is persistent; AR(1) is the null to beat | derived |

### V.2 · How to include them — partialling out the confounders

The rigorous way to show the tanker signal adds something is the
**Frisch–Waugh–Lovell** theorem. To get the *pure* partial effect of a tanker
signal `T` on the spread `y`, controlling for the matrix of confounders `Z`:

```
1. Regress y on Z      → residuals  ỹ   (spread, weather/storage/FX removed)
2. Regress T on Z      → residuals  T̃   (tanker signal, same removed)
3. Regress ỹ on T̃     → coefficient = T's marginal contribution beyond Z
```

A non-zero step-3 coefficient is "edge net of the obvious drivers" — exactly the
claim worth making. In an ML pipeline the analogue is: fit with controls +
tanker signals, then use **SHAP / permutation importance** to show the tanker
signals carry incremental predictive power *above* the control-only model.

For a causal-flavoured version, **Double/Debiased ML** (Chernozhukov et al.)
estimates the tanker effect with ML-fitted nuisance functions for `E[y|Z]` and
`E[T|Z]` plus cross-fitting — robust to overfitting the controls. Flag as
advanced; the FWL/linear version is the place to start.

### V.3 · The unit/FX trap (do this before any modelling)

HH is quoted in **USD/MMBtu**; TTF in **EUR/MWh**. A naive "spread" mixes
currencies and energy units, so EUR/USD moves leak in as fake signal. Convert to
a common basis first:

```
TTF[$/MMBtu] = TTF[€/MWh] / 3.412 × (EUR/USD)        # 1 MWh ≈ 3.412 MMBtu
spread       = HH[$/MMBtu] − TTF[$/MMBtu]            # (or the sign you prefer)
```

Define the spread consistently in one currency/unit, and keep EUR/USD as a
control (V.1) to absorb any residual FX sensitivity.

---

## Recommended build order (small-data first)

1. **Define the target correctly** — currency/unit-consistent spread (V.3),
   plus EIA + AGSI + degree-day controls assembled into a daily panel.
2. **AR(1) + controls linear baseline** — the null every tanker signal must beat
   (FWL partial effect, V.2).
3. **Physical nowcasts** — kinematic ETA (§1) + Poisson/NB arrivals (§2) +
   survival queue/berth (§3), validated against EIA *now*. This is the
   defensible "models working today" deliverable.
4. **BOCPD outage nowcast** (§10) — cheap, online, asymmetric payoff.
5. **Hierarchical pooling** (Part IV.2) across terminals for the §3 models.
6. **Spread model: Bayesian structural time series** (§7) with spike-and-slab
   over the pre-registered signals + controls; report posterior inclusion
   probabilities and predictive intervals. Cross-check with Elastic Net (§6) and
   PLS (§8).
7. **Defer** LightGBM (§9, constrained only) and full cross-exciting Hawkes (§4)
   until the panel passes ~1 year / the event count is large.
8. **Never** deep sequence models (Transformer / LSTM / TFT) on this sample
   size — with weeks of data and 30+ features they overfit badly; the "Transformer
   wins on financial time series" literature almost always has a leakage bug.

The honest summary (live-only corpus): on a few months you **confirm edge**
(Parts II + V.2) rather than **forecast the spread** (Part III). The physical
nowcasts and the partial-effect controls are what make the work defensible now;
the spread model is a Bayesian, uncertainty-first placeholder that strengthens as
the panel grows.

**Post-backfill, the build order shifts** (see the §0 reframe): steps 1–3 (target,
AR(1)+controls baseline, physical nowcasts) run on a **decade of US data** rather
than weeks, so they graduate from "confirm edge" to genuinely fitted, walk-forward-
validated models with terminal pooling (step 5) live from the start. Step 6 (the
spread model) becomes a real fit on the ~3,000-row 2017→now panel — still
Bayesian/regularised and uncertainty-first because the spread is low-SNR, but no
longer a placeholder — with the `gfw → mmsi_filter` EU fidelity seam carried as a
regime indicator and the Part V controls partialled out on the long panel. Step 7's
deferrals (constrained LightGBM, full cross-exciting Hawkes) come *forward* once the
event count is in the tens of thousands; step 8's prohibitions (deep sequence
models) still hold — a long panel of an autocorrelated, regime-broken spread is not
the i.i.d. corpus those models need.
```
