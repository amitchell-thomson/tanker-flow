# Modelling: tanker-flow → HH/TTF spread

Companion to [`SIGNALS.md`](SIGNALS.md). That doc is the catalogue of *what* is
measured (34 signals in `signal_daily`); this is *how* to turn it into a forecast of
the **Henry Hub / TTF spread**, with the maths per model and the small-sample
discipline the data demands.

Every design decision, protocol commitment and finding is logged append-only in
[`DECISIONS.md`](DECISIONS.md) — the pre-registration instrument §0·3 #3 requires,
and the source of record the final research note is assembled from. A spec below is
*locked* once its D-entry lands; changes go through a superseding entry, never a
silent edit.

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

## Part A · Physical nowcasts (high-SNR, validate today)

Target: next-week US exports, EU arrivals 1–2 weeks out, queue/berth durations.
Mechanically constrained → high SNR, validatable weekly against EIA, on the decade.

**A1 · Arrival-count baseline — at-sea duration climatology (no fitted parameters;
spec locked 2026-08-10, D-001–D-003).** The physics question: *of the laden cargo at
sea now, how much lands in Europe next week, and the week after?* Target = the
**conditional weekly EU arrival count** — laden legs (origin ∈ {usgulf, usatlantic},
`departed ≤ as_of`) whose first EU `zone_entry` falls in `W₁ = [as_of, +7d)` /
`W₂ = [+7d, +14d)`, pooled across EU zones. *Not* the unconditional count (the
not-yet-departed tail is a departure-process model — A2's job, composed later) and
*not* per-zone (the live stock is ~75–83 % unknown-destination, and the historical
banded `knowable` is hindsight-banded — D-004b). The original "great-circle ÷ speed"
framing is this in disguise: `voyage_speed_kn` is *defined* as gc-nm ÷ duration, so
`d_gc/v̄` cancels to the lane's typical duration — A1 is duration climatology, named
honestly.

Per open laden leg `i` at age `a`, window `(a+u₀, a+u₁]` (form per D-007, which
superseded D-002's parametric posterior):

    p_i(W) = π(a) · [F_eu(a+u₁) − F_eu(a+u₀)] / (1 − F_eu(a))
           = N_eu(duration ∈ window) / N(open at a)        ← telescopes (D-009)

Two measured factors, cleanly separating *will it be EU?* from *when?* — and because
both are estimated from **one** population, the middle terms cancel exactly, so the
decomposition is a factorisation of a single count ratio rather than two
approximations multiplied. That is what removes the ratio (hence D-002's `p99` tail
rule) and guarantees the factors can never be drawn from different windows or regimes:

- **`π(a)`** — the **empirical age-conditional EU-arrival rate**: among matured legs
  still open at age `a`, the share that produced an observed EU arrival. Estimated on
  a **365 d rolling** window of departures ≥ 90 d old (`MAX_LEG_PAIR_DAYS` — the age
  at which `pair_legs` can no longer change the outcome, so maturity is structural
  rather than tuned). Measured shape: flat ≈ 0.41 to day 10, then collapsing through
  the 12–18 d voyage window (.393 → .317 → .236 → .172 at 12/14/16/18 d, .052 by 60 d)
  as EU-bound legs close and the residual pool becomes non-EU / missed-arrival. This
  curve *is* the destination model; no survival assumption is made.
- **`F_eu`** — duration CDF over **EU-arriving legs only**, same population. The
  timing model, and A3's starting point (A3 replaces this ECDF with a fitted,
  censoring-aware distribution).

The forecast population is **every open laden US-export-origin leg, `open_censored`
included** — the estimation denominator makes no status distinction either, so
filtering phantoms from one side only would apply the correction twice.

`π` is deliberately **capture-inclusive** — a leg whose real EU arrival we never saw
counts in the denominator — so A1 forecasts *observed* arrivals, exactly what the
D-003 truth series measures. Rolling (not expanding) because `π` drifts ~4× across
the decade; fallback ladder widens window → origin → regime, tagging every curve with
its rung and marking unsupported ones. Zero fitted parameters — two trailing empirical
objects, both point-in-time.

Weekly count = **Poisson-binomial** over the open legs: mean `Σpᵢ`, variance
`Σpᵢ(1−pᵢ)`, and the **exact PMF** by convolution DP (D-010) — CRPS and PIT read
masses, not moments. Zero-probability legs drop out exactly, which is what keeps it
cheap (1,348 open legs but ~129 with `p>0` at a typical date). *Independence across
legs is an assumption*: an outage or freeze would correlate arrivals and true variance
would exceed `Σpᵢ(1−pᵢ)` — that is A5's subject, not A1's. Implementation:
`analysis/a1.py`.

Feeds off **`compute_legs()` legs directly**, not the aggregated `signal_daily` keys
(a convolution needs per-leg state — D-005); requires the as-of-true replay loader
(D-004a). Validation is **wholly historical as-of replay** (weekly Monday grid from
2018-01 — 2016–17 is burn-in, too thin to support `π`, D-008; no live tail accrues
since the 2026-08-10 ingest stop). **Truth** = `physical` closed-leg arrivals in
`(as_of+u₀, as_of+u₁]`, *conditional on having departed by `as_of`* — the same
`is_eu_arrival` event the forecast predicts, so both sides are capture-limited
identically. The conditioning is material, not pedantic: it moves 156 of 417 W₂
weeks. **Nulls** = persistence (the last *fully-elapsed* week's count — D-003's
literal wording leaked the future for W₂; amended in D-011) + the trailing 4-week
mean. Scored MAE/RMSE + CRPS + PIT/interval coverage, by departure regime and year,
2021–22 vs rest. **Pre-registered acceptance (D-003): beat both nulls on W₁ MAE over
the NOAA-departure decade *and* 80 % interval coverage in [70, 90] %** — miss the
second and A1 stands as a point baseline whose calibration gap is A3's opening. A3
(below) upgrades `F_eu` to a fitted, censoring-aware per-O-D arrival-time
distribution; A1 stays the no-training baseline it must beat.

> ### A1 · RESULT — does not meet the bar (2026-08-10, `make a1-replay`, D-013)
>
> 448 weekly as-ofs, 2018-01-01 → 2026-07-27, `regime='noaa'`, horizon W₁:
>
> | | A1 | persistence | climatology |
> |---|---|---|---|
> | **W₁ MAE** | **2.314** | 2.217 | **1.814** |
>
> **(a) beat both nulls — FAIL.** **(b) 80 % coverage in [70,90] % — PASS (73.7 %).**
> Per D-003 a failure on (a) means A1 does not even stand as a point baseline.
>
> **Not an artefact.** It loses to climatology in 2022, 2023 *and* 2024 — clean,
> high-volume years — and in both halves of the §0·3 crisis split. Excluding the one
> known boundary artefact (30 post-NOAA-tail weeks) makes it marginally *worse*:
> 2.369 vs 2.366 / 1.871.
>
> **Why: staleness, and the bias column proves it.** Forecast − truth by year: 2022
> **−3.48**, 2024 **+2.76**, 2025 **−2.27**, against ≈0 in flat years. Classic lag —
> under-shoot on the way up, over-shoot on the way down. `π` is estimated from
> departures ≥ 90 d old (`MATURITY_DAYS`, structural per D-007) inside a 365 d window,
> so it trails the market by ~3–15 months. On a market that moved 4× in a decade, a
> 4-week moving average of actual arrivals simply carries fresher information than
> mechanism does.
>
> **Calibration passes but is not clean.** The PIT histogram is strongly U-shaped (72
> / 97 in the extreme deciles vs ~45 uniform) — the forecast is **under-dispersed**.
> (b) survives partly because discreteness makes intervals conservatively wide
> (D-010). Report the PIT, not just the coverage figure.
>
> **Where it does win:** the `gfw` regime (W₁ MAE 0.950 vs 1.199 / 1.006) and 2018 —
> i.e. where volume is low and the market is flat. Not where it matters, and **not
> independent corroboration**: up to 65 % of GFW's US departures are the same
> voyages NOAA already has (D-014).
>
> **Read the regime label carefully (D-014).** `regime='noaa'` means *NOAA saw it
> leave, GFW saw it arrive* — there are **zero** NOAA arrivals in Europe (NOAA is
> US-only terrestrial AIS), so the target is 100 % GFW-observed in every scorecard.
> This does not affect the verdict: A1 and both nulls read the same truth series, so
> any capture limitation deflates all three identically.
>
> **Kept as-is.** No parameter retuned, no slice dropped: the bar existed before the
> number so this could be reported rather than engineered away (§0·3 #3).
>
> **Consequence for A3 (pre-registered here, before A3 is built):** A1's ceiling is
> staleness, and A3's censoring-aware fit runs on **open** legs, so it needs no
> maturity gate and can use current data. That is the specific, mechanism-level
> reason A3 should beat A1 — and the thing to check first when it is built. Any
> Part-B use of an A1-style nowcast must treat it as a *lagging* indicator.

**A2 · Count regression — Negative Binomial GLM (spec locked 2026-08-12, D-016).**
The direct answer to A1's failure. A1 was mechanism with no knobs, running on inputs
3–18 months stale; A2 is the first *fitted* model, with features observable **today**
and coefficients refit weekly.

`log λ = β₀ + Σβⱼxⱼ`, count ~ **Negative Binomial** (`Var = μ + μ²/k`). NB is the
headline and Poisson a nested weekly cross-check — no data-dependent switching:
NB nests Poisson (`k→∞`) so it cannot be worse in-sample, and A1's diagnosed failure
was **under-dispersion**, exactly what Poisson's forced `Var = μ` would repeat.

**Target: weekly US laden loadings** (`departed`, laden, from `usgulf`/`usatlantic`),
W₁ = (0,7] d and W₂ = (7,14] d — A1's window convention, so the two are comparable.
Loadings rather than arrivals because it is the one line that is **NOAA-native at
both ends** (every US→EU leg is NOAA-out/GFW-in, D-014) and it is the supply pulse
Part B wants. EU arrivals are scored secondary, against A1's W₁ MAE of 2.314.

**Five features, signs pre-registered:** `lag1` (+), `trail4` (+), `in_berth` (+,
near-deterministic for W₁), `ballast_arrivals_1w` (+, the feedstock — a ship must
arrive empty before it can leave full), `queue_depth` (+, ⚠ the one ambiguous sign;
a robustly negative fit **falsifies** the mechanism rather than being reinterpreted).
Feature 4 is deliberately the **US-side ballast arrival**, not the EU-side ballast
departure, which is GFW-only and would put a capture-drifting covariate in a
NOAA-target model. Features are built by reusing `visits.pair_visits` /
`queues.pair_queues` with the existing open-item ceilings, not reimplemented.

**Protocol:** expanding window (≥104 wk) — unlike A1's rolling one, because βs are
*ratios* (capture-robust per §0·1) where `π` was a drifting *level*; purge by target
closure; standardisation on training statistics only; MLE via `scipy.optimize`; refit
weekly from 2018-01. Nulls are persistence + 4-week climatology — which *are*
features 1 and 2, so the scorecard's real question is **whether the three physical
features add anything over the autoregression**. **Acceptance: (a) beat both nulls on
W₁ MAE, (b) 80 % coverage in [70,90] %; (c) signs hold — a mechanism test binding on
the narrative, not on pass/fail.** Full pre-registration in D-016.

> ### A2 · RESULT — does not meet the bar (2026-08-12, `make a2-replay`, D-018)
>
> 345 scored weeks, `regime='noaa'`, W₁, NB:
>
> | | A2 (NB) | persistence | climatology |
> |---|---|---|---|
> | **W₁ MAE** | **3.961** | 2.551 | **2.152** |
>
> **(a) FAIL** — 84 % worse than climatology. **(b) PASS** (75.7 %).
> **(c) `ballast_arrivals_1w` FALSIFIED**: fits **−0.049**, negative in 99 % of weeks
> and stable across all three eras, against a pre-registered **+**. Reported as a
> falsification per D-016, not reinterpreted. The other three signs hold.
>
> **Why — over-extrapolation, the mirror image of A1.** Bias is systematically
> positive and scales with the level: −5 % of truth in 2020, +11 % in 2024, **+27 %
> in 2025** (forecast 37.9 vs truth 29.8). The log link on a trending series fitted
> over an *expanding* window puts current features several sds above the training
> mean, and `exp()` converts that distance into multiplicative overshoot. **A1 was
> too stale and under-reacted; A2 over-reacts. Both lose to a 4-week mean, from
> opposite directions** — a more informative pair of results than either alone.
> Robust to the one artefact (31 all-zero 2026 weeks, NOAA ends 2025-12-31):
> excluding them gives 3.828 vs 2.752 / 2.145, unchanged verdict.
>
> **The NB prior did not pay off.** Fitted `k` runs to its bound (median 1e6) — the
> data is not over-dispersed once conditioned on the features, so NB collapses to
> Poisson (3.961 vs 3.980). The D-016 reasoning was sound and the choice was free,
> but it did not help.
>
> **Consequence — A4 is promoted ahead of A3 (predicted here, before either).** Two
> independent failures now say the same thing: this series is dominated by a trend
> that a 4-week mean tracks well and that neither stale mechanism nor log-linear
> extrapolation handles. A **local-level state-space model (A4)** is designed for
> precisely that — track the level, don't extrapolate it multiplicatively — so it is
> the better-motivated next model. Any future count model here must be pre-registered
> with a rolling window or an explicit level/trend decomposition.

**A3 · Survival models — queue, berth, and voyage time.** Cox PH `h(t|x)=h₀(t)·exp(xᵀβ)`
(partial likelihood cancels the baseline) or Weibull AFT for a parametric small-data
fit. The unit is the *event* (hundreds, not dozens of days), and right-censoring
handles in-progress visits natively — which is exactly what `open_fraction` /
`estimated_fraction` flag on `load_queue_h` / `*_berth_turn_h`. **The same machinery
fits voyage time-to-arrival per O-D**, turning A1's point ETA into a fitted arrival-time
*distribution*: the right-censored unit is the open laden leg, with `legs.py`'s
`open_floating` / `open_arrival_gap` / `open_censored` classes supplying the censoring
(an open leg past its window is *censored, not missing*), and the per-leg posterior
densities convolve into A1's Poisson-binomial arrival-count distribution — so the
censoring the kinematic baseline ignores becomes the likelihood. **Pool across
terminals / O-D pairs hierarchically** (Part C) — the highest-leverage move here.

**A4 · Kalman / state-space — latent flow-rate nowcast (spec D-022; RESULT below).**
`x_t = x_{t−1} + w` (latent weekly export rate), `y_t = x_t + v` (observed loadings);
two parameters `(q, r)` by exact Kalman MLE, walk-forward. Same target, grid and nulls
as A2 so the two are directly comparable. Pre-registered secondary: local **linear
trend**, which nests local level as `q_slope→0`.

> ### A4 · RESULT — misses by 0.8 %, and closes the question (2026-08-12, `make a4-replay`, D-023)
>
> 314 scored weeks, `regime='noaa'`, W₁:
>
> | | A4 level | A4 trend | persistence | climatology |
> |---|---|---|---|---|
> | **W₁ MAE** | **2.104** | **2.101** | 2.694 | **2.088** |
>
> **(a) FAIL** — beats persistence comfortably, misses climatology by **0.016 MAE
> (0.8 %)** on n=314: a statistical tie. **(b) PASS** (72.6 %).
>
> **The fitted parameter is the result.** Free to choose any smoothing by MLE, the
> filter selected **equivalent EWMA α = 0.251 — a ~7-week effective window** (median
> `q/r` = 0.082). Given complete freedom it smooths *more* than the 4-week SMA it was
> competing with, and lands 0.8 % away. D-022 pre-registered a tie as informative:
> **the naive moving average is near-optimal for this series, now demonstrated rather
> than assumed.**
>
> **The D-018 prediction held.** Against A2 on comparable spans (A2 ex-artefact
> 3.828), A4 cuts error ~45 % and eliminates the bias that killed its predecessors —
> yearly bias range A1 −3.5…+2.8, A2 −0.31…+8.11, **A4 −1.28…+0.08**. It beats
> climatology outright in **2022 and 2023**, and in the §0·3 #1 split wins the
> **2021-22 crisis block** (2.221 vs 2.231) while losing narrowly in calm years — the
> one regime where the level genuinely moved is the one where tracking it paid.
> Calibration is the best of the four (PIT 41/45 extreme deciles vs A1's 72/97).

**A5 · BOCPD outage detection (spec D-019/D-020; RESULT below).** Outages dominate
realised spread variance, and a moving average is structurally worst at breaks — so
A5 was deliberately a different question on a different scoring axis from A1/A2.
Per-terminal BOCPD on daily laden-departure counts, Poisson-Gamma conjugate,
run-length posterior `P(r_t|y_{1:t})`, all constants fixed a priori. Labels are
**rate-relative** (a gap ≥ max(14 d, 5× the terminal's own baseline)) — essential,
since Elba averages ~26 d between loadings while Sabine averages 1.4 d. The rule was
validated against externally-known events before locking: it recovers **Freeport
2022-06-08 (252 d, 127×, the explosion)**, the **2020 COVID cargo-cancellation wave**,
and **Cove Point's annual September turnaround**.

> ### A5 · RESULT — does not meet the bar; the naive rule does the job (2026-08-12, `make a5-replay`, D-021)
>
> 16 labelled outages, 8 terminals, 53.7 terminal-years, `regime='noaa'`:
>
> | detector | recall | median delay | false alarms / terminal-yr |
> |---|---|---|---|
> | **BOCPD** | 12 % | **17 d** | 0.13 |
> | N1 absolute (14 d) | 88 % | 14 d | 2.33 |
> | **N2 rate-relative** | 38 % | **12 d** | **0.19** |
>
> **FAIL** — BOCPD is slower than both nulls and detects almost nothing; its low
> false-alarm rate reflects near-silence, not precision.
>
> **Why: binning destroyed the signal.** At a 2-day cadence ~half of all days are
> zero-count during *normal* operation, so each zero carries almost no evidence and
> the filter needs 2–3 weeks to react. The nulls read **time since last departure** —
> the sufficient statistic for a point process, using the exact timing that daily
> binning discards. The lesson is representational, not Bayesian: the right BOCPD
> here is on **inter-arrival times** (Gamma-exponential), logged as a possible A5b but
> **not built**, since specifying it after seeing this scorecard is a forking path.
>
> **N1's 88 % recall is near-tautological** (the label requires a ≥14 d gap, so N1
> fires at day 14 by construction); its 2.33 false alarms/terminal-yr is the real
> story. **N2's recall is capped by the pre-registered 21-day detection window** — it
> fires at 5× baseline, which exceeds 21 d for slow terminals, explaining every Cove
> Point miss.
>
> **The usable artefact is N2, the baseline.** 12-day median detection at **0.19 false
> alarms per terminal-year**, catching Freeport 2022 at **10 d**, Freeport 2024 at
> **8 d**, Sabine 2017/18/19 at 12/10/13 d, Corpus 2019 at 13 d. Every miss is a
> slow-baseline terminal. A deployable outage monitor came out of this — as the null,
> not the model.

**A6 · Destination / routing nowcast — call EU-bound from the Gulf exit (validated 2026-06-17).**
The live in-transit stock is **81 % `unknown`-destination** and `declared_eu_share` is
effectively dead (6 days), so the highest-value family — *where the marginal cargo goes* —
is blind exactly where it matters most (live, pre-arrival). It is recoverable from
geometry: a US-Gulf cargo bound for Europe must exit through the **Straits of Florida** and
turn NE up the Atlantic, while Asia-bound cargoes route south (Yucatán → Panama). **Decade
validation** (5,955 laden NOAA Gulf departures): a vessel seen **NE-bound in the Straits**
arrives at an EU terminal **64–68 %** of the time vs **9 %** for those not seen crossing
(**≈18× odds**); the crossing is observed **~2.5 d after departure**, giving **~13.5 d of
lead** on the EU arrival, at **59 % recall**. The 64–68 % is a *floor* — the misses are
mostly un-captured EU arrivals + Latin-America/Caribbean Atlantic cargoes, not Asia. **The
heading gate is essential, not cosmetic:** the box is ~50/50 NE-outbound vs SW-returning-to-
reload, so without COG (≈100 % populated) half the "crossings" are ballast returners.

*Build.* **Chokepoint geofences** (Straits-of-Florida NE = EU/Atlantic; the Yucatán/Panama
corridor is *out of terrestrial coverage* → the detector is **one-sided**: a NE crossing is
strong positive EU evidence, absence is weak). Emit a **probabilistic destination** per open
laden leg — prior = EIA exports-by-destination base rate, likelihood = chokepoint + heading
— and use it to (a) split the `gas_in_transit_volume` `unknown` band and (b) revive the
intent family (`declared_eu_share` → an `intent_eu_share` fusing declared + inferred;
`diversion_arbitrage`). Leakage-safe by construction (inference at `d` uses only fixes ≤ `d`;
validate the `knowable` call against the `physical` arrival). It is a clean supervised
problem — decade arrival labels + EIA destination ground truth — so report accuracy and lead
*per chokepoint*. Improves *lead time* on the headline at-sea signal, which §0·3 makes a
first-class scoring axis.

> ⚠ **Live coverage is the prerequisite, not the signal.** Historically NOAA was exhaustive
> (59 % caught); on the **live MMSI-filter feed only ~5 % of departures are caught at the
> Straits (5 / 104)** — the watchlist demotes a vessel as it leaves the terminal, so it is
> unsubscribed before Florida. The fix is an ingestion change, not a model change; the
> Straits are inside terrestrial AISstream range, so coverage is *free* once the vessel is
> kept in a slot:
>
> **Outbound-transit scoring pin (spec).** In `pipeline/scoring.py`, beside the existing
> open-leg *approach* pin (`is_pinned`):
> - **Trigger** — a vessel whose latest event is a laden `departed` from a US export
>   terminal (`zone IN (usgulf, usatlantic)`), within `OUTBOUND_TRANSIT_DAYS` (≈5 d) of that
>   departure and not yet past the Straits (still south/west of ~27 °N off Florida). Pin →
>   tier 1 / persistent slot so AISstream keeps delivering its fixes. Exclude FSRUs.
> - **Release** — a fix clearing the Straits (east of Florida, above ~26.5 °N into the
>   Atlantic) **or** the window expires **or** an arrival / `zone_entry` fires.
> - **Budget** — ~2.5 US-Gulf laden departures/day × 5 d ≈ **~13 concurrent pins**; the
>   persistent block runs under-full (slot overhaul), so it fits — cap at `MAX_OUTBOUND_PINS`
>   as a safeguard.
> - **Backstop** — a `vf_rescue` "dest-resolution" class polls a laden-departed vessel that
>   still goes silent approaching the Straits (credit-budgeted, surplus-only, same machinery
>   as the existing rescue classes).
> - **Acceptance** — live Florida-NE recall rises from ~5 % toward the ~59 % historical,
>   measured by re-running this validation on the live tail as it accrues.

**A7 · Live vintage de-bias — nowcast the firmed value, not the first print.** Every live
`knowable` rebuild snapshots `signal_daily_live_vintage`; the eventual `physical` series is
what that print firms to as late fixes land and capture fills in. The pair *(as-printed,
as-firmed)* is a supervised revision dataset — fit `E[physical | knowable_t, x]` per
`(signal_key, regime)` with a band (a multiplicative capture-style correction, or a Kalman
level state à la A4 with `R` from the confidence columns) and apply it to today's print.
This is the **live analogue of the §0·2 historical recovery**: where §0·2 de-attenuates
pre-2022 *levels* offline via NOAA×GFW×EIA triangulation, A7 de-attenuates the *live tail*
online — `capture_rate.py`'s observed ÷ EIA-implied becomes a live `1/capture` scale-up
rather than a passive validator. The payoff is **negative-latency lead**: print the firmed
level now instead of waiting out weeks of revision, and it is robust to dropped fixes *by
construction* — it models the observation process instead of imputing positions (the reason
the per-vessel position filter was not worth building). Leakage-safe: the revision model at
day `d` is fit only on legs/visits whose target windows close before `d` (purge + embargo,
Part C #1), and the corrected output is still `knowable`. Validate by replaying the vintage
log against `physical` and reporting the revision-RMSE reduction over the raw print; it
extends Part C #3 from confidence-*weighting* to confidence-*correcting*.

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
- ~~**TTF source = Barchart Premier, ~$30 one-time**~~ — **superseded 2026-09-05 by
  D-026.** TTF now comes from **Yahoo `TTF=F`** (free, keyless, 2017-10-23 → present,
  251-254 rows/yr, 3 nulls in 2,234 rows). The Barchart purchase existed to buy the
  2016-17 head, but **D-008 starts scoring at 2018-01** — that head is burn-in and is
  never scored, so the paid depth was unusable. **License: Yahoo is personal-use, so
  the raw series is not committed** — `market_series` in the DB is the only copy;
  derived signals/charts are publishable.
- **Spread cadence = daily**, clean over the entire panel.

The work ran as **two parallel tracks** — Part A nowcasts needed neither TTF nor
controls (they validate against EIA), so TTF resolution never gated modelling progress.
**Both tracks are now resolved:** Part A is complete at h = 1-2 weeks (D-024/D-025) and
Track 1's target + control set is built and validated (D-026). Track 3 (Part B) is next.

### Track 1 — control set + spread target (new-data integration)
Thin loaders, siblings to `data/eia.py` (pure parse + `merge` + upsert). EIA series →
existing `eia_series`; non-EIA series → a shared `market_series` table (same key shape);
one assembler joins all onto the `signal_daily` daily grid into a new `model_panel`.

1. ~~**EIA Phase 2**~~ ✅ **DONE 2026-08-10.** Both v2 routes probed live and confirmed
   correct as-registered (no 404, no registry fix needed), then backfilled: `RNGWHHD`
   Henry Hub daily spot, 7,431 rows 1997-01-07 → 2026-08-03; `NW2_EPG0_SWO_R48_BCF`
   Lower-48 weekly storage, 866 rows 2010-01-01 → 2026-07-31. `ACTIVE_PHASE` bumped 1→2
   so a plain `make eia` now keeps all three series current on their revision windows.
   Note: HH spot prints on business days only — 68.9% of panel days — so the `model_panel`
   assembler needs a business-day forward-fill, not an inner join.
2. ✅ **TTF** — **DONE 2026-09-05** (D-026). Yahoo `TTF=F` → `market_series.ttf_front_month`,
   2,234 rows 2017-10-23 → 2026-09-04. Barchart dropped. The **World Bank Pink Sheet**
   monthly cross-check is ✅ **DONE** (`ttf_eu_monthly`, CC-BY): slope 0.9990,
   R² 0.9999, mean relative difference +0.007 % over 106 months — **the roll is
   benign and the unit conversion is right, so spread levels are safe to model**
   (D-027). `make check-ttf-roll`.
3. ✅ **EUR/USD FX** — **DONE 2026-09-05.** FRED `DEXUSEU` → `market_series.eurusd`,
   2,781 rows from 2016-01-01. Brent (`DCOILBRENTEU`) loaded alongside it.
4. ✅ **Spread target** — **DONE 2026-09-05.** `spread = HH[$/MMBtu] − TTF[€/MWh]/3.412 ×
   EUR/USD`, business-day forward-filled; **104 complete months from 2018-01**, and it
   reproduces the record including the 2020-05 HH>TTF inversion (D-026). **Part B unblocked.**
5. ✅ **GIE AGSI+** EU storage — **DONE 2026-09-05.** `continent=eu` aggregate (note:
   `country=eu` returns empty), 3,899 rows from 2016-01-01, as both fill-% and TWh.
6. ✅ **Degree-days** US + NW Europe — **DONE 2026-09-05.** Open-Meteo ERA5 archive
   (free, keyless, daily) at weighted demand centres, base 18.3 °C, both regions from
   one loader — this closes the "NW-Europe source TBD". Eurostat was rejected as
   monthly-only (D-026).

### Track 2 — Part A physical nowcasts (start now, in parallel; validate on the decade)
Walk-forward CV, `basis='knowable'`, confidence columns as observation variance.
7. ✅ **A1 arrival-count baseline** — **DONE 2026-08-10, negative result** (fails the
   pre-registered bar: beaten on W₁ MAE by both nulls; diagnosis = staleness of the
   maturity-gated `π`; full result block under Part A / D-013, regime caveat D-014).
   `make a1-replay` reproduces. A3 inherits the specific brief: censoring-aware fit
   on *open* legs, no maturity gate, current data.
8. ✅ **A2 NB count GLM** — **DONE 2026-08-12, negative result** (W₁ MAE 3.961 vs
   climatology 2.152; over-extrapolation, mirror image of A1's staleness; one
   pre-registered sign falsified — D-018). `make a2-replay`. **A4 Kalman is now
   promoted ahead of A3** on the strength of the A1+A2 diagnosis (D-018). Then
   **A3 Cox/Weibull survival** on queue/berth — *then extend
   the same A3 fit to per-O-D voyage time-to-arrival* (open-leg `legs.py` censoring as the
   right-censored unit, posteriors convolved into A1's arrival-count distribution) — with
   **C2 hierarchical pooling across terminals/O-D pairs** (highest-leverage move), **A4
   Kalman flow-rate**.
9. ✅ **A5 BOCPD outage nowcast** — **DONE 2026-08-12, negative result** (BOCPD 17 d
   median delay / 12 % recall vs the rate-relative null's 12 d / 0.19 FAR; cause =
   binning a point process — D-021). `make a5-replay`. **Usable artefact: the N2
   rate-relative silence rule** (12 d median, 0.19 false alarms/terminal-yr).
9b. ✅ **A4 Kalman local level** — **DONE 2026-08-12** (W₁ MAE 2.104 vs climatology
   2.088 — a 0.8 % tie; fitted EWMA α=0.251 ⇒ ~7-week optimal window). **Part A is
   closed**: four models converge to the naive mean from above without crossing it
   (D-023/D-024). **A3 and A5b deliberately not built**, with reasons logged.
10. **A7 live vintage de-bias** — fit `E[physical | knowable_t]` per `(signal_key, regime)`
    from the `signal_daily_live_vintage` ↔ `physical` pairing (+ `capture_rate.py` as the
    live `1/capture` scale-up); replay the vintage log against `physical` for the
    revision-RMSE win. Needs vintage history to have accrued, so it trails A1–A5.

### Track 3 — Part B spread model — **Tracks 1 & 2 are done; this is the live front**
10b. ✅ **`model_panel` assembled** — **DONE 2026-09-05** (D-027). 20 features,
   77,500 rows, 2016-01-01 → 2026-08-10; **2,764 fully-complete days from 2018-01**.
   Forward-filled onto a daily grid (never inner-joined — HH prints on 68.9 % of
   days) with each source's **publication lag** applied, so a row is what a model
   standing on that date could see. `make model-panel`; `load_wide()` returns the
   DataFrame. **Feature-set amendment (D-027): `spread_thrust`,
   `implied_storage_build`, `diversion_arbitrage` and `declared_eu_share` are
   live-only** (9-46 daily observations — they need EU anchorage events GFW does
   not carry) **and are therefore not Part B features.** The decade-deep
   primitives plus `net_export_pressure` (1,770 rows) carry the same information,
   with the weights fitted rather than imposed.
11. ✅ **AR(1)+controls baseline + FWL harness** — **DONE 2026-09-05, negative result** (spec D-028, result D-029). `analysis/b0.py` (OLS+Newey-West HAC, purged walk-forward, the M0/M1/M2 ladder) + `analysis/fwl.py` (partial effects, graded against pre-registered signs). `make b0-replay`. **M2 loses to no-change** (−10.4% h=1, −19.2% h=4) ⇒ **M0 is the operative null**; **no tanker signal is significant** at either horizon (max |HAC t| 1.72 vs 1.96, max partial R² 0.009). Closes the linear, contemporaneous claim; non-linearity, regimes, joint fits and signal lags remain untested.
12. **BSTS (B2)** with spike-and-slab over the pre-registered signals + controls,
    confidence-weighted; cross-check Elastic Net (B1) + PLS (B3). Report posterior
    inclusion probabilities + predictive intervals. Two-stage: feed the Part A nowcast in.
13. **Defer** constrained LightGBM (B5) and cross-exciting Hawkes until the event count is
    in the tens of thousands.

### First moves (decision-free)
1. ~~Buy Barchart Premier + download the TTF CSV~~ — **obsolete (D-026): TTF is free via Yahoo `TTF=F`; the control set now costs £0 and has no manual step.**
2. EIA Phase 2 probe + backfill (~30 min) — HH spot + US storage.
3. Start A1 (arrival-count baseline) in parallel — no dependencies, validates fastest.

### Gate items (tracked, not blocking)
- EIA capture-rate firms when EIA's June-2026 data publishes (~late summer 2026).
- Vintage self-validation accrues with the live tail.

The deliverable arc: **confirm edge** (Part A nowcasts + FWL partial effects, defensible
now on the decade) → **forecast the spread** (Part B, Bayesian and uncertainty-first
because the spread stays low-SNR regardless of `N`). The EU fidelity seam and the Part 2
control-partialling matter *more* on a long panel, not less.
```
