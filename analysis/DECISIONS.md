# Modelling decision log

Append-only, dated record of every modelling decision, protocol commitment and
finding. This file is the **pre-registration instrument** `MODELS.md` §0·3 #3
requires: a hypothesis, lag, target or acceptance criterion is *committed* the day
its entry lands here — before any result is seen — so a later change is a visible,
dated superseding entry, not a silent forking path.

The final research note is assembled from four sources, each with one job:

| doc | role |
|---|---|
| `DECISIONS.md` (this) | **why** — decisions, alternatives rejected, findings, dated |
| `MODELS.md` | **what** — the current spec per model (kept consistent with the log) |
| `SIGNALS.md` + `VALIDATION.md` | **data** — panel definitions + the panel gate |
| results sections (appended to `MODELS.md` per model as they land) | **outcome** |

**Rules.**
- Append-only. A decision is changed by a *superseding entry* that names the old
  one and says what result prompted the change — never by editing history.
- A decision is **locked** once its entry lands and `MODELS.md` reflects it.
- Findings (bugs, leaks, data surprises) get entries too — they are decisions about
  what the data can be trusted for.
- Every entry: **Context → Decision → Rejected alternatives → Consequences.**

---

## D-000 · 2026-08-10 · Adopt this log

**Context.** §0·3 makes pre-registration binding but no artefact implemented it;
the September research note needs a source of record for *why* each choice was made.

**Decision.** This file, with the rules above. Entries D-001–D-005 lock the A1
design as the first use.

---

## D-001 · 2026-08-10 · A1 target: conditional weekly EU arrival count, pooled EU

**Context.** A1 must forecast something (a) physically constrained by the at-sea
stock, (b) observable as ground truth on the decade, (c) not contaminated by the
destination blindness (live in-transit stock is 75–83 % unknown-destination,
measured 2026-07-15 / 2026-08-10 on `mmsi_filter` knowable).

**Decision.** The target is the **conditional weekly EU arrival count**: of laden
legs with origin ∈ {usgulf, usatlantic} and `departed_ts ≤ as_of`, the number whose
first EU `zone_entry` falls in `W₁ = [as_of, as_of+7d)` and `W₂ = [as_of+7d,
as_of+14d)`. Pooled across all EU zones. Truth from `physical`-basis closed legs.

**Rejected alternatives.**
- *Unconditional weekly arrivals* — needs a not-yet-departed term, i.e. a departure-
  process model. That is A2's job (Poisson/NB on loadings); folding a climatological
  departure top-up into A1 blurs the A1/A2 boundary for a tail that matters only in
  W₂. A1 stays pure conditional-on-at-sea; A2 will later compose with it for the
  unconditional forecast.
- *Per-zone / per-terminal arrivals* — you would be guessing the answer for ~3 in 4
  cargoes (unknown-destination share), and the historical banded `knowable` panel
  cannot even validate it honestly (D-004b). Blocked on A6, which is itself blocked
  on live Straits coverage that no longer exists.

**Consequences.** The A1↔A3 comparison is clean (A3 also operates on open legs).
The unconditional, market-facing arrival number arrives only when A2 lands.

---

## D-002 · 2026-08-10 · A1 probability model: survival-conditioned duration climatology with a destination posterior

**Context.** The doc's original `t̂ = t_dep + d_gc/v̄` is duration climatology in
disguise: `voyage_speed_kn` is *defined* as great-circle nm ÷ duration, so distance
cancels and `d_gc/v̄` reduces to the lane's typical duration. Great-circle is also
physically wrong on this lane (Sabine→Rotterdam gc = 4,295 nm runs overland; routed
via the Florida Straits ≈ 4,843 nm, ~13 % longer — why observed "speeds" read
10–12 kn against real service speeds of 16–17 kn). Rather than fake the physics,
name the baseline honestly and make the probability model exact.

**Decision.** Per open laden leg `i` at `as_of`, age `a = as_of − departed_ts`,
window offset `[u₀, u₁)` days ahead:

    p_i(W) = π · [S(a + u₀) − S(a + u₁)] / (π · S(a) + 1 − π)

- **`S`** — survival function of the **pooled origin-zone→EU closed-leg duration
  ECDF**, expanding window over arrivals observed ≤ `as_of`. Pooling across EU
  destinations sidesteps needing a per-leg destination: the EU mix is implicit in
  the empirics. Historically no declarations exist (NOAA/GFW carry none), so this
  is not a degraded path — it is the only honest one.
- **`π`** — trailing **matured EU-arrival base rate** per origin zone: of laden
  departures ≥ 90 d old at `as_of` (90 = `MAX_LEG_PAIR_DAYS`, so maturity is
  unambiguous — beyond it a leg can never close by construction), the share that
  closed at an EU terminal.
- **Denominator** — the posterior that a still-open leg is EU-bound at all: non-EU
  legs (Asia, Latin America, uncaptured) essentially never close at EU on these
  horizons, so their survival ≈ 1 and `P(EU | open at a) = πS(a)/(πS(a)+1−π)`.
  This correctly decays an aging leg's arrival probability instead of conditioning
  as if it were certainly EU-bound.
- **Weekly count** — Poisson-binomial over open legs: mean `Σpᵢ`, variance
  `Σpᵢ(1−pᵢ)`, exact PMF by direct convolution (n ≈ 50, trivial).
- **Tail rule** — `a` beyond the ECDF p99 ⇒ `p_i = 0` (timing unknowable; the
  parametric tail is A3's upgrade). Logged when it fires.
- **Floors** — ECDF used at ≥ 50 closed legs for the origin zone, else pooled over
  both origins; `π` at ≥ 100 matured departures, else pooled. **Burn-in:** 2016
  seeds the empirics; scoring starts 2017-01.

Zero *fitted* parameters — two trailing empirical objects, both point-in-time.

**Rejected alternatives.**
- *Fixed-dispersion "truly no-training" kernel* (e.g. Normal(median, fixed σ)) —
  demoted to a sensitivity check. The lane durations are strongly right-skewed
  (usgulf→nweurope: median 14.75 d, p90 24.7 d, n = 1,413 NOAA), so a symmetric
  kernel is wrong exactly in the tail that matters.
- *Vessel-position kinematics* (remaining routed distance ÷ current SOG) — needs
  mid-ocean fixes terrestrial AIS doesn't have; historically NOAA/GFW carry no
  usable mid-voyage state. Not buildable on the corpus.
- *Constant-π conditioning* (no denominator) — overstates old legs; the posterior
  form costs nothing and is correct.

**Consequences.** A1 is renamed to what it is (duration-climatology arrival
baseline). A3's job description sharpens: replace `S` with a fitted, censoring-aware
per-O-D distribution and beat this.

---

## D-003 · 2026-08-10 · A1 replay & scoring protocol (pre-registered)

**Context.** No live tail will accrue (ingest stopped 2026-08-10), so validation is
wholly historical as-of replay. §0·3 requires horizons and scoring fixed before any
fit or replay is run.

**Decision — committed before first replay:**
- **Grid.** Weekly, Mondays 00:00 UTC, first `as_of` 2017-01-02, last = latest date
  with full W₂ truth available; the live `mmsi_filter` tail (≈ 8–10 weeks,
  2026-06→08) is scored separately and reported as anecdote, not evidence.
- **Truth.** Realised conditional count per D-001, from `physical` closed legs.
  Caveat carried in the write-up: historical EU arrivals are GFW-observed, so truth
  is capture-limited on the arrival side (one fidelity across the decade — a level
  caveat, not a seam).
- **Nulls.** For horizon `h`: (1) *persistence* — the realised value of the same
  conditional statistic at `as_of − 7d`, same horizon; (2) *climatology-mean* — the
  trailing 4-week mean of that statistic. Both computable point-in-time.
- **Metrics.** MAE and RMSE on the count; CRPS from the exact PMF; PIT histogram +
  central-interval coverage (50 %, 80 %).
- **Breakouts.** By departure regime (`noaa` primary; `gfw` secondary; `mmsi_filter`
  anecdote), by calendar year, and 2021–22 vs rest (§0·3 #1). Never pooled-only.
- **Horizons.** W₁ and W₂ only. No horizon sweep, no lag search.
- **Acceptance (the pre-registered bar).** A1 "works" iff on the NOAA-departure
  decade: (a) beats **both** nulls on W₁ MAE, and (b) 80 % interval coverage lands
  in **[70 %, 90 %]**. Failing (b) alone ⇒ A1 stands as a point baseline whose
  calibration gap is A3's explicit opening — reported as such, not massaged.

**Rejected alternatives.** Scoring on `signal_daily` aggregates (the panel reduces
away per-leg state — see D-005); k-fold CV (never for time series, Part C #1);
letting the replay end date float per-experiment (fixed by truth availability once,
recorded in the harness output).

---

## D-004 · 2026-08-10 · Findings: two point-in-time leaks (one to fix, one to fence)

**(a) `compute_legs` enrichment is as-of-unsafe — fix scheduled (A1 build step 1).**
`LAST_FIX_SQL` returns the latest-*ever* fix and `DEST_REGION_SQL` reads the
*current* watchlist. Replayed at a historical `now`, `_classify_overdue` compares a
2026 fix against `now − 4d` and returns `open_floating` for essentially every
historical open leg. Never bit the panel build because `signal.py` calls
`compute_legs` once at true-now and reconstructs `knowable` analytically
(`knowable_leg_interval`); A1's replay is the first true point-in-time consumer.
**Fix:** parameterise the event stream and both enrichment queries by `as_of`
(`event_time ≤ as_of`, latest fix ≤ `as_of`, declarations ≤ `as_of`), with a
regression test that `as_of = now` reproduces current behaviour exactly. Benefits
A3 (same machinery) — not A1-local scaffolding.

**(b) The banded `knowable` in-transit series is hindsight-banded — fenced, fix
deferred.** `dest_band()` assigns closed legs' `zone_scope` from the *observed
arrival zone* for **both** bases; only interval trimming differs between bases. DB
confirms: `noaa`/`knowable` `gas_in_transit_volume` has `open_fraction ≈ 1.000` on
the `unknown` band and ≈ 0 on every named band — named bands *are* the closed legs,
banded by an arrival a live observer had not yet seen. NOAA/GFW carry no
declarations, so historically the arrival is the *only* band source.
**Rule (binding):** `zone_scope` on the in-transit/ballast stocks is never a
point-in-time feature and never a validation target for destination-aware models.
`validate_signals.py`'s Tier-4 checks don't test the band — the green gate is not
evidence against this. **Fix deferred** until a consumer actually needs a
point-in-time band (A6 territory); a warning lands in SIGNALS.md §2.2 with the A1
write-up.

---

## D-007 · 2026-08-10 · **Supersedes D-002's kernel form** — `pi` becomes an empirical age-conditional curve `pi(a)`

**Context.** A1 build step 2. D-002 specified a parametric posterior

    p_i(W) = pi * [S(a+u0) - S(a+u1)] / (pi*S(a) + 1 - pi)

whose denominator rests on "non-EU legs essentially never close, so their survival
≈ 1". Measuring the population before implementing it falsified that premise.

**Findings that forced the change** (decade, `point_in_time=True` at 2026-08-11;
9,614 matured laden US-export-origin legs):

- **`same_zone` is 46.4 % of the matured population (4,464 legs) and is *not* a
  berth-shift artefact.** Median duration **825 h ≈ 34 d** (p75 61 d, p90 77 d;
  45.6 % of the 9,855-leg unmatured population). These are departures whose
  EU arrival was never observed, pairing instead with the vessel's *return to the
  Gulf to reload*. They leave the open pool steadily right through the ages A1
  forecasts over — so survival-≈-1 is wrong exactly where it is load-bearing.
  (A ~850-leg sub-population *does* close within 6 h — genuine berth shifts — but
  it is the minority and flushes out before the first forecast age.)
- **The quantity the assumption existed to produce is directly measurable.**
  `pi | still open at age a`, whole matured sample:

  | a (d) | 0 | 1 | 5 | 10 | 12 | 14 | 16 | 18 | 21 | 25 | 30 | 60 |
  |---|---|---|---|---|---|---|---|---|---|---|---|---|
  | pi | .351 | .404 | .412 | .412 | .393 | .317 | .236 | .172 | .122 | .090 | .076 | .052 |

  Flat to ~day 10, then collapsing through the 12–18 d voyage window as EU-bound
  legs close and the residual open pool becomes non-EU / missed-arrival.

**Decision.** Replace the parametric posterior with the **directly-estimated step
function `pi(a)`** — among matured legs still open at age `a`, the share that
eventually produced an observed EU arrival. Step 3's kernel becomes

    p_i(W) = pi(a) * [F_eu(a+u1) - F_eu(a+u0)] / (1 - F_eu(a))

with `F_eu` the duration CDF over **EU-arriving legs only**. Both factors are now
measured, and the two questions are cleanly separated: *will it be EU?* (`pi(a)`)
and *when?* (`F_eu`). Still zero fitted parameters. Implemented in `analysis/a1.py`.

**Rolling, not expanding, estimation window — 365 d.** D-002 said expanding. `pi`
drifts ~4× across the decade (pi(10 d) by as-of date: .098 in 2017 → .161 in 2018 →
.381 in 2020 → .303 in 2022-03 → .526 in 2023 → .418 in 2025 → .456 in 2026), from
both the 2022 trade-flow regime change and the §0·1 capture gradient. An expanding
window is a badly lagging estimator of that. 365 d holds ≥ ~350 legs even in the thin
years and ~1,500 recently, comfortably above the `PI_MIN_LEGS = 100` floor. **Chosen
on sample-size grounds before any scoring and not to be retuned against results**
(§0·3 #3).

**Maturity gate stays at 90 d = `MAX_LEG_PAIR_DAYS`.** Structural, not tuned: beyond
it `pair_legs` cannot pair an arrival at all, so the outcome is determined *by
construction*. Cost, accepted and documented: `pi` is estimated from data ≥ 3 months
stale, so at 2022-03-07 it still reads the pre-Ukraine 2021 mix (.303). A shorter
gate would be fresher but would misclassify still-open legs as non-EU (~3.6 % of the
population at a 30 d gate, ≈ 8 % relative downward bias on `pi`) — and picking the
gate by which scored better is precisely the forking path §0·3 #3 forbids.

**`pi` is capture-inclusive, by design.** A leg whose real EU arrival we never saw
sits in the denominator, not the numerator. A1 therefore forecasts *observed*
arrivals — the same thing the D-003 truth series measures. Neither side is
capture-corrected here; that is A7's job. Stated so the write-up does not later
claim A1 estimates physical arrivals.

**Fallback ladder** (widen one axis at a time, never straight to a pooled estimate
across a fidelity seam): `(zone, regime, 365d)` → `(zone, regime, 730d)` →
`(pooled-origin, regime, 730d)` → `(pooled-origin, pooled-regime, 730d)`. Every
curve carries the rung that produced it and its population size; an estimate that
falls off the end is returned tagged `UNSUPPORTED(...)` rather than silently
indistinguishable from a supported one (no silent caps).

**Verified (2026-08-10).** Curves built at 8 as-of dates on the live DB behave as
designed: burn-in marked `UNSUPPORTED` (2016-07 n=8, 2017-01 n=46), first supported
date 2018-01-01 (n=124, `usgulf/noaa/365d`), the collapse shape present at every
date, and the drift tracked rather than smeared. The estimator is a suffix-count
over sorted close-ages (bisect per grid point, not a rescan per age — the replay
calls it ~520 times); refactoring to that form left all 8 curves identical to the
digit. 341 tests pass, ruff clean; 16 new tests in `tests/test_a1.py`.

**Rejected alternatives.**
- *Keep the parametric form* — its premise is measurably false, and it is strictly
  more assumption-laden than the thing it approximates.
- *Exclude `same_zone` from the denominator* — tempting when it looked like a berth
  shift; wrong once measured. Those legs represent real departures we failed to
  observe arriving, and dropping them would inflate `pi` and break the
  capture-inclusive property that makes forecast and truth commensurable.
- *Expanding window* (as D-002 said) — lags a 4× drift.
- *Per-age ladder rungs* — more responsive but multiplies provenance bookkeeping;
  a suffix truncation at `PI_MIN_AT_AGE = 20` with carry-forward is simpler and the
  thin region is recorded on the curve.

---

## D-026 · 2026-09-05 · Track 1 control set built — TTF sourced free, Barchart dropped

**Context.** Part B was blocked on the §2 target + control set. Only EIA Phase 2
(HH spot, US L48 storage) existed. The locked plan bought TTF from **Barchart
Premier** (~$30, one month, cancel); that route failed at the vendor.

**Decision 1 — TTF comes from Yahoo `TTF=F`, not a paid vendor. Supersedes the
MODELS.md "Decisions locked" Barchart entry.** Verified live 2026-09-05 against
the chart endpoint: 2,234 daily rows, **2017-10-23 → 2026-09-04**, 251-254 rows
per calendar year, largest gap 4 days (holiday weekends), 3 nulls total.

The reason this is not a compromise: **D-008 already moved the first scored
`as_of` to 2018-01-01** because the matured leg population is below
`PI_MIN_LEGS` before then. 2016-17 is burn-in that is never scored, so the
21-month "gap" the Barchart purchase existed to close sits **entirely outside the
scored window**, and `TTF=F` still supplies a ~10-week run-up for AR/lag terms.
The paid backfill was buying panel depth that the panel cannot use.

**Rejected alternatives (all re-checked 2026-09-05, not carried over from the
June research):** *Databento* — ICE Endex history still starts 2018-12-23, later
than Yahoo, so it cannot supply what Barchart was for. *ICE Report Center* —
end-of-day CSV packages are subscription-only. *Stooq* — now behind a JavaScript
proof-of-work challenge, not scriptable. *Investing.com* — plausible daily depth
to ~2010 but the CSV export is gated behind InvestingPro; not pursued once the
scored-window argument made 2016-17 depth unnecessary.

**Decision 2 — NW-Europe degree days come from Open-Meteo ERA5, closing the
MODELS.md "source TBD".** Eurostat's degree-day product (`nrg_chddr2_m`) is
monthly NUTS-3 only, which would reintroduce exactly the monthly-interpolation
seam the daily-TTF decision existed to avoid. Open-Meteo's archive is daily, free,
keyless, ERA5-backed from 1940, and serves **both** regions — so US and NW Europe
share one loader and one definition (`HDD = max(0, 18.3 - T̄)`) rather than
NOAA-for-US plus something else for Europe, which would have made the two legs
non-comparable. Demand-centre weights are judgement, documented in-module, and
flagged as such: the model standardises these series, so weight stability matters
more than weight precision.

**Decision 3 — one `data/market.py`, not four sibling modules.** MODELS.md
Track 1 named `data/ttf.py`; four providers landing in one table with one key
shape means the upsert, merge model, CLI and incremental logic are shared and
only the *parse* differs. Each provider contributes one pure parser; a new
control is a registry entry, matching `data/eia.py`'s "new series is one entry,
not new code" property.

**Consequences.**
- New table `market_series` (migration `b7c2e9f1a3d4`), same `(series_id, period)`
  key as `eia_series` so one assembler joins both onto the `signal_daily` grid.
- **9 series backfilled, 31,200 rows**: `ttf_front_month`, `eurusd`, `brent`,
  `eu_storage_full`, `eu_storage_twh`, `hdd_us`, `cdd_us`, `hdd_nwe`, `cdd_nwe`.
  All span 2016-01-01 → 2026-09 except TTF (2017-10-23, above).
- `make market` / `make market-full`; incremental re-pulls a per-provider trailing
  revision window and is verified idempotent (31,200 rows before and after).
- **Cost of the control set is now £0 and it carries no cancellation deadline** —
  materially better than the locked plan, which required remembering to cancel.

**Verified (2026-09-05, live DB).** The assembled spread reproduces the known
record on a business-day forward-filled grid, 104 complete months from 2018-01:

| month | HH $ | TTF €/MWh | TTF $/MMBtu | spread |
|---|---|---|---|---|
| 2018-01 | 3.90 | 18.7 | 6.67 | −2.77 |
| 2020-05 | 1.73 | 4.9 | 1.58 | **+0.15** |
| 2021-10 | 5.48 | 89.4 | 30.39 | −24.91 |
| 2022-08 | 8.83 | 238.7 | 70.71 | −61.87 |
| 2023-06 | 2.12 | 32.1 | 10.21 | −8.09 |
| 2026-08 | 2.80 | 62.0 | 21.08 | −18.28 |

The **2020-05 sign flip is the load-bearing check**: it is the one month in the
decade when HH exceeded TTF (the COVID demand collapse drove TTF to €4.9), and it
is not a value a mis-scaled or mis-joined series would produce by accident.
Degree days and EU storage validate on seasonality independently — HDD peaks in
January and is zero in July, US CDD (7.1) far exceeds NW-Europe CDD (1.5) in
August, and EU storage troughs at 38.6 % in March and peaks at 89.9 % in October.

509 tests pass, ruff clean; 20 new tests (provider parsers, degree-day maths,
weight renormalisation, incremental start dates, merge idempotency).

**Finding, unrelated but blocking on arrival.** Adding `FRED_API_KEY` /
`GIE_AGSI_API_KEY` to `.env` **broke every entry point** until they were declared
on `Settings` — `pydantic-settings` forbids undeclared env keys, so `alembic`,
the loaders and the viz all raised `extra_forbidden` at import. Recorded because
the failure names the *key* rather than the cause and reads like a credential
problem rather than a schema one.

**Still open in Track 1 (neither blocking Part B).** The World Bank Pink Sheet
monthly TTF cross-check (CC BY 4.0, the only redistributable price series, and
the intended check on `TTF=F`'s continuous-roll discontinuities) is **not built**
— its download URL rolls monthly and needs a landing-page scrape. ENTSOG /
Gassco pipeline flow and ENTSO-E wind/nuclear (§2, tier-2 controls) remain
unbuilt by choice: four more collinear controls on a low-SNR target is more
likely to hurt than help before the FWL residuals ask for them.

---

## D-025 · 2026-08-12 · Scoping amendment to D-024, and one deferred pre-specified test

**Why this entry exists.** D-024's heading — "PART A CLOSED" — claims more than the
evidence supports, and the overclaim was caught by challenge rather than by the
protocol. What was actually tested is **four models at one horizon band (h = 1–2
weeks) on two targets**, plus one outage detector. "These four, there, do not beat the
mean" is not "no nowcast can". D-024's *body* is correctly scoped; its framing is not.
This amendment fixes the framing and records what remains open.

### What is genuinely closed — and it is theory, not exhaustion
**Weekly US loadings at h = 1.** A4 fitted a local-level model by MLE and chose
α = 0.251. For a series that truly *is* local level (random walk + observation noise),
the EWMA is the **provably optimal linear predictor** — so a fitted α landing 0.8 %
from SMA(4) is the optimum being found, not four methods coincidentally tying. A fifth
method on that target/horizon would be wasted effort. This closure is narrow and
solid; nothing beyond it is closed on the same grounds.

### Deferred pre-specified test — **H1: the horizon hypothesis**
Recorded **now, before any longer-horizon data is examined**, so that running it later
is a genuine test rather than target-shopping.

**The gap.** Everything was scored at h = 1–2 weeks — precisely where autocorrelation
is strongest and the physical lead is shortest. The two decay in *opposite*
directions: a moving average gets **worse** as `h` grows on a trending series (it never
extrapolates), while the pipeline signal does not decay — a US→EU voyage takes 14–18 d
(measured: usgulf→nweurope median 14.75 d, p10 12.35, p90 24.7), so cargo at sea today
mechanically determines EU arrivals 2–3 weeks out.

**Pre-specified predictions** (the falsifiable part — an edge appearing anywhere else
is evidence of a spurious fit, not a result to keep):
1. **EU arrivals:** skill over the naive nulls should be **increasing in `h` across
   h = 1…4 weeks and peak at h ≈ 2–3 weeks**, matching transit time, then decay beyond
   as the at-sea stock empties.
2. **US loadings:** a weaker version peaking at **h ≈ 1–2 weeks** via
   `gas_ballast_to_us` / `ballast_arrivals` (the ballast-return lead). A4 already ties
   at h = 1–2, so the expected gain here is small — stated so a null result is not
   read as surprising.
3. The nulls (persistence, 4-week climatology) must **degrade monotonically in `h`**.
   If they do not, the premise is wrong and H1 should be abandoned rather than pursued.

**Bar:** unchanged in shape — beat both nulls on MAE at the peak horizon, 80 %
coverage in [70, 90] %, scored per-regime and per-year with the §0·3 #1 split.
**Cost:** ~1 day; all four harnesses already parameterise `u0`/`u1`.
**Honest prior:** ~40–50 % that it beats the null at the predicted horizon.

### Also open, ranked, with the Part-B relevance filter applied
The filter that matters: **Part B consumes signals as *features*, not forecasts.** A
better nowcast of `load_queue_h` does not help a spread model that already reads
`load_queue_h` directly.

| # | avenue | beats a null? | helps Part B? |
|---|---|---|---|
| 1 | **H1 longer horizons (EU arrivals)** | plausible, ~40-50 % | **yes** — forward EU supply information exists in no current feature |
| 2 | Per-terminal + hierarchical pooling (Part C #2, never used) | plausible | weakly |
| 3 | Deviation-from-trend targets | unclear | weakly |
| 4 | The 29 signals no model has used | lower (A2 failed on extrapolation, not feature poverty) | no |
| 5 | Duration targets (A3) | likely — durations are not trend-dominated | **no** — B reads the queue signal directly |

Only #1 survives the filter, which is why it is the one written up as a pre-specified
test and the rest are listed as inventory.

### Correction to D-024's "what Part A delivered" — it under-sold two things
Part A hands Part B **two ready-made features**, both already built:
- **A4's filtered level** — a denoised export-rate state with calibrated bands;
  strictly a better regressor than the raw noisy weekly count.
- **A5's N2 outage flags** — event indicators at 12-day median detection. MODELS.md
  notes outages dominate realised spread variance, and this is an asymmetric,
  event-shaped feature no smooth signal supplies.

### Sequencing decision
**Part B first, then revisit H1 informed by B** — not because Part A is exhausted, but
because of what each test costs and pays. Part B is entirely untested and is the
thesis; H1 is ~1 day with existing machinery and does not expire. Decisively: **Part B
tells you whether H1 is worth running at all.** If the spread model finds nothing in
the tanker signals, a better 3-week arrival forecast will not rescue it; if it does,
you will know which signal and which horizon matter, and H1 becomes targeted rather
than speculative.

---

## D-024 · 2026-08-12 · **Part A complete at h = 1–2 weeks** — synthesis of A1, A2, A4, A5

> ⚠ **Scope (amended by D-025).** This entry originally read "PART A CLOSED", which
> overclaims. What follows holds for **h = 1–2 week horizons on weekly US loadings and
> weekly EU arrivals**. Longer horizons are untested and carry a pre-specified
> hypothesis (D-025 · H1). Read the conclusion below with that bound.

Part A is complete at the tested horizons. Four pre-registered models, four honest
scorecards, and a single coherent conclusion.

### The arc
| model | target | W1 MAE | climatology | gap |
|---|---|---|---|---|
| **A2** NB count GLM | weekly US loadings | 3.961 | 2.152 | **+84 %** |
| **A1** duration climatology | weekly EU arrivals | 2.314 | 1.814 | **+27.6 %** |
| **A4** Kalman local level | weekly US loadings | 2.104 | 2.088 | **+0.8 %** |
| **A4** local linear trend | weekly US loadings | 2.101 | 2.088 | **+0.6 %** |

(A5 sits on a different axis: 17 d median detection vs the naive rule's 12 d.)

**Read the gap column downward — that is the finding.** Each model attacked the
previous one's diagnosed weakness, and the gap to the naive baseline closed
monotonically: 84 % → 27.6 % → 0.8 %. The diagnoses were right and the fixes worked.
And the sequence **converges to the moving average from above without ever crossing
it**. Four independent methods — mechanism, regression, state-space, change-point —
all land at or above a 4-week mean.

### The conclusion, stated plainly
**On weekly US LNG loadings at a 1-2 week horizon, the naive moving average is
near-optimal, and this is now demonstrated rather than assumed.** The strongest single piece of evidence is A4's own
fitted parameter: given free rein to choose any smoothing constant by maximum
likelihood, the filter selected an **effective window of ~7 weeks** — i.e. it
independently concluded that heavy smoothing is right, and landed a statistically
indistinguishable 0.8 % from the fixed 4-week window it was competing against.

This is a *positive* result about the data-generating process, not four failures:
weekly loadings are a smooth, trend-dominated, terminal-buildout-driven series whose
best short-horizon predictor is its own recent average. There is no exploitable
week-ahead structure left for a physical model to find.

### What Part A delivered
1. **A closed question**, with the mechanism named (above).
2. **A deployable artefact** — A5's N2 rate-relative silence rule: 12-day median
   outage detection at 0.19 false alarms/terminal-year, catching Freeport 2022 at
   10 days and Freeport 2024 at 8 days (D-021).
3. **Partial mechanism validation** — A2's `in_berth` and `queue_depth` fitted
   positive in 100 % of weeks across all eras; `ballast_arrivals_1w` was falsified
   (D-018). The physical story is real but does not translate into week-ahead edge.
4. **Four reusable harnesses** and a point-in-time replay discipline that caught two
   leakage-class bugs (D-004a, D-017) and two specification bugs (D-017, D-020)
   before any of them reached a headline.

### What this means for Part B — the load-bearing distinction
Part A tested **"do tanker signals predict tanker outcomes better than naive
baselines?"** Answer: no, because those outcomes are near-deterministic and smooth.

Part B tests **"do tanker signals carry information about the HH/TTF spread net of
weather and storage?"** That is a *different claim*, and nothing here bears on it:
Part B consumes the **signals as features**, not the Part A forecasts. The one thing
genuinely weakened is Part C #6's two-stage plan (feed a Part A nowcast into Part B) —
which should now be dropped rather than attempted.

**Carry-forward for Part B:**
- The naive baselines on this panel are strong. Pre-register an **AR(1)+controls null**
  and expect it to be hard to beat; §0·3 #4's "predict the change, not the level" is
  now empirically supported, not just protocol.
- A4's ~7-week effective window is the natural smoothing for any loadings-derived
  feature entering the spread model.
- Every Part-A replay lesson (per-regime spans, purge, hindsight-label/point-in-time-
  feature separation) transfers directly.

### Deliberately not built
**A3 (survival on queue/berth durations)** — its A1-facing purpose died with A1, and
Part B consumes `load_queue_h` as a feature directly, so a better queue *forecast* has
no consumer. **A5b (BOCPD on inter-arrival times)** — the correct fix to D-021's
representational error, but specifying it after seeing A5's scorecard is a forking
path. Both are closed with reasons, not silently dropped.

---

## D-023 · 2026-08-12 · **RESULT — A4 misses the bar by 0.8 %, and closes the question**

**The scorecard.** 314 scored weeks, 2019-12-23 → 2025-12-22, `regime='noaa'`, W1.

| | A4 level | A4 trend | persistence | climatology |
|---|---|---|---|---|
| **W1 MAE** | **2.104** | **2.101** | 2.694 | **2.088** |

- **(a) Beat both nulls — FAIL.** Beats persistence comfortably; misses climatology by
  **0.016 MAE (0.8 %)** on n=314. That is a statistical tie, not a defeat.
- **(b) 80 % coverage in [70, 90] % — PASS at 72.6 %.**

**Verdict: A4 does not meet the bar** — but D-022 pre-registered precisely this
outcome as informative: *"a tie is a perfectly good outcome: it would establish that
the naive moving average is near-optimal for this series, which closes the
count-forecasting question."* It did.

**The interpretable output is the result.** Fitted by maximum likelihood with no
constraint, the filter chose **equivalent EWMA α = 0.251**, a **~7-week effective
window** (median q/r = 0.082). Given complete freedom, the optimal linear filter for
this series smooths *more* heavily than the 4-week SMA it was competing with — and
lands 0.8 % away from it. That is a much stronger statement than "A4 lost".

**The D-018 prediction was correct.** A4 was promoted ahead of A3 on the theory that
tracking the level beats extrapolating it. Against A2 on comparable spans (A2
excluding its 2026 artefact weeks: 3.828), **A4 cuts the error by ~45 %** and
essentially eliminates the bias that killed both predecessors:

| | A1 | A2 | **A4** |
|---|---|---|---|
| bias range across years | −3.5 … +2.8 | −0.31 … +8.11 | **−1.28 … +0.08** |

**It beats climatology exactly where it matters most.** A4 wins outright in **2022
(2.257 vs 2.308) and 2023 (2.201 vs 2.284)** — the crisis and post-crisis years — and
in the pre-registered §0·3 #1 split it beats both nulls across **2021-22 (2.221 vs
2.231)**, losing narrowly only in the calm years (2.046 vs 2.018). The one regime
where the level genuinely moved is the one where tracking it paid.

**Calibration is the best of the four.** PIT is mildly U-shaped (41/45 in the extreme
deciles against ~31 uniform) — still slightly under-dispersed, but far tamer than
A1's 72/97.

**The local linear trend variant does not change the story** (2.101 vs 2.104,
coverage 74.5 %). Reported as the pre-registered secondary; it is *not* swapped into
the headline, and its near-identical result is itself evidence that trend
extrapolation adds nothing here.

**Comparability caveat, stated.** A4's span (314 rows, ends 2025-12-29 per D-017) is
not identical to A2's (345 rows, polluted by 31 all-zero 2026 weeks), so the
climatology nulls differ slightly (2.088 vs 2.152). The A2-vs-A4 comparison above uses
A2's artefact-excluded 3.828. **Kept as-is** — no retuning, no span alignment after
the fact.

---

## D-022 · 2026-08-12 · **A4 design lock — pre-registration (binding, before any run)**

**Context.** A4 closes Part A and tests the prediction made in **D-018**, before A4
existed: *"a local-level state-space model is designed for exactly this — track the
level, don't extrapolate it multiplicatively."* Testing it is the point; leaving a
pre-registered prediction unrun would be an omission.

**Honest prior, stated up front.** A steady-state local-level Kalman filter *is*
an exponentially-weighted moving average. So A4 is essentially asking: **is an
optimally-tuned EWMA better than a fixed 4-week SMA on this series?** A fixed SMA(4)
lags a trend by ~1.5 weeks structurally and an EWMA lags less, so there is a genuine
edge available — but the realistic prize is modest. A tie is a perfectly good
outcome: it would establish that *the naive moving average is near-optimal for this
series*, which closes the count-forecasting question rather than leaving it open.

### Target — identical to A2, so the comparison is direct
Weekly **US laden loadings** (`departed`, laden, `usgulf`/`usatlantic`), `regime='noaa'`,
horizons W1 = (0,7] d and W2 = (7,14] d, same weekly Monday grid from 2018-01.
The observation series is `y_t` = loadings in `(t-7d, t]`; W1 forecasts `y_{t+1}`,
W2 forecasts `y_{t+2}`. Note `y_t` **is** A2's `lag1` and the persistence null, and
the 4-week mean **is** A2's `trail4` and the climatology null — so A2, A4 and both
nulls are all read off one series with one definition.

### Model
**Headline: local level** (faithful to what D-018 actually predicted — the model is
*not* upgraded after the fact):

    x_t = x_{t-1} + w_t,   w ~ N(0, q)      (latent export rate)
    y_t = x_t     + v_t,   v ~ N(0, r)      (observed weekly count)

`h`-step forecast: mean `x_t`, variance `P_t + h·q + r`. **Two parameters** (`q`, `r`),
fitted by exact Kalman MLE via the prediction-error decomposition, walk-forward with
the same expanding window (≥104 wk) and purge as A2. Diffuse init (`x_0` = first
observation, `P_0` large).

**Pre-registered secondary: local linear trend** (Holt), which adds a slope state and
a third variance and *does* extrapolate the trend linearly. It nests local level as
`q_slope → 0`, exactly as NB nested Poisson in A2. Reported alongside; **if it wins it
is reported as a secondary finding, not swapped into the headline.**

**Gaussian observation model, stated as an approximation.** Weekly counts of ~5–30 are
close to Gaussian (Poisson at mean 20 nearly is), and MODELS.md specifies a linear
Gaussian `y_t = Hx_t + v`. Consequence: the predictive is continuous, so CRPS uses the
closed-form Gaussian expression and PIT/coverage use Gaussian quantiles rather than
A1/A2's discrete PMF. MAE, coverage and CRPS remain in observable units and stay
comparable; noted so the difference is not discovered later.

### Nulls and acceptance
Nulls identical to A2/D-011: **persistence** = `y_t`; **climatology** = mean of the
last 4 observed weeks.

**Acceptance: A4 works iff (a) W1 MAE beats both nulls, and (b) 80 % coverage in
[70, 90] %.** Reported alongside but not gating: W2, the local-linear-trend variant,
per-year and 2021-22-vs-rest splits, the fitted signal-to-noise ratio `q/r` (the
implied EWMA smoothing constant — the interpretable output), and **A2's W1 MAE of
3.961** for the direct model-vs-model comparison.

**Span (D-017, applied):** the scored grid ends at the last week fully inside the
NOAA regime's own departure history (NOAA ends 2025-12-31), not at the panel data
max — so the all-zero 2026 weeks that polluted A2 cannot recur.

---

## D-021 · 2026-08-12 · **RESULT — A5 (BOCPD) does not meet the bar; the naive rule does the job**

**The scorecard.** 16 labelled outages, 8 terminals, 53.7 terminal-years, `noaa`.

| detector | recall | median delay | false alarms / terminal-yr |
|---|---|---|---|
| **BOCPD** | **12 % (2/16)** | **17 d** | **0.13** |
| N1 absolute (14 d) | 88 % (14/16) | 14 d | 2.33 |
| **N2 rate-relative** | 38 % (6/16) | **12 d** | 0.19 |

**Verdict: A5 does NOT meet the bar.** BOCPD is *slower* than both nulls (17 d vs
14 / 12) and detects almost nothing. Its low false-alarm rate is not a virtue — it
barely alarms at all. Excluding the Cove Point cluster changes nothing material
(BOCPD 18 %, N2 55 %).

**Why BOCPD fails — binning destroys the signal.** At a terminal loading every ~2
days, roughly half of all days are zero-count *during normal operation*, so a single
zero carries almost no evidence. The filter needs ~2–3 weeks of consecutive zeros to
move the run-length-averaged rate below half. Meanwhile the naive detectors read
**time since the last departure**, which is the sufficient statistic for a point
process and uses the exact event timing that daily binning throws away. That is the
clean statistical lesson: **A5 modelled a point process as binned counts, and the
binning cost more than the Bayesian machinery bought.** The right BOCPD formulation
here is on *inter-arrival times* (Gamma-exponential), not Poisson daily counts —
recorded as a possible pre-registered A5b, **not** built now, because building it
after seeing this scorecard would be exactly the forking path §0·3 #3 forbids.

**Structural interaction, reported not fixed.** N2's recall is capped by the
pre-registered 21-day detection window: it fires at `5 × baseline` days, so for a
slow terminal (Cove Point, baseline ~5 d ⇒ 28 d) it cannot fire inside the window at
all. That explains every Cove Point miss. The window and the label rule were both
pre-registered; the interaction is a finding, not something to retune.

**N1's 88 % recall is near-tautological** and should not be read as skill: the label
rule requires a gap ≥ 14 d, so every labelled outage *necessarily* contains 14 days
of silence and N1 fires at day 14 by construction. Its 2.33 false alarms per
terminal-year — 12× N2's — is the real story: it cries wolf constantly at slow
terminals.

**The genuinely positive result: N2 is a deployable outage detector.** 12-day median
detection, **0.19 false alarms per terminal-year** (≈ one per five terminal-years),
and it works on exactly the terminals that matter for the spread signal — it caught
**Freeport 2022 at 10 days, Freeport 2024 at 8 days, Sabine 2017/2018/2019 at 12/10/13
days, Corpus 2019 at 13 days**. Every miss is a slow-baseline terminal. This came out
of A5 as the *baseline*, not the model, which is the honest way to report it: the
simple rate-relative silence rule is the usable artefact here.

**Three models, three failures, three distinct diagnosed causes** — A1 stale
(under-reacts), A2 over-extrapolating (over-reacts), A5 mis-specified representation
(binning a point process). None is a vague "it didn't work", and in each case a naive
baseline won for an understandable reason. That is a coherent research finding about
this panel: **the simple baselines are strong, and beating them requires attacking a
specific diagnosed weakness rather than adding sophistication.**

**Kept as-is.** No parameter retuned, no window widened, no detector re-specified
after seeing the numbers.

---

## D-020 · 2026-08-12 · Amendment to D-019 — the alarm rule asked the wrong question

**Found before any scoring, by a unit test.** D-019 pre-registered the alarm as
`P(r_t <= R_SHORT) > P_ALARM` **and** a rate-drop gate. The synthetic
"steady-then-dead-stop" test raised no alarm at all.

**Diagnosis.** The filter was working perfectly — on a clean stop at day 200 it
identifies the break and the rate ratio collapses to **0.10** by day 218, far below
the 0.5 gate. But `P(r <= 3)` peaks at **0.01** and never approaches 0.5, because
BOCPD detects a gradual change *retrospectively*: by the time evidence has
accumulated it concludes "the change happened ~18 days ago", so the probability that
the change is ≤3 days old is correctly near zero. **The gate can essentially never
fire for any change that takes time to detect** — it asked whether the break was
*recent*, when the question is whether a break *happened*.

**Amendment.** Alarm on the rate statistic alone: the **run-length-averaged**
posterior mean rate falling below `DROP_FRAC ×` the no-change rate. `R_SHORT` /
`P_ALARM` are demoted to a reported diagnostic. This is a **specification bug caught
by a test before any real data was scored**, in the same class as D-017's
target-slicing bug — not a response to a result. `DROP_FRAC`, the hazard and the
prior are unchanged.

---

## D-019 · 2026-08-12 · **A5 design lock — pre-registration (binding, before any run)**

**Context.** A1 and A2 both lost to a 4-week mean on a trend-dominated level series
(D-013, D-018). A5 is deliberately a **different question on a different scoring
axis**: not "what is next week's count" but "**has this terminal stopped loading, and
how fast can we tell**". A moving average is structurally *worst* at breaks, which is
where BOCPD is designed to win — so this is not a rerun of the same contest.

### Detector
Per-terminal **independent** Bayesian Online Change-Point Detection (Adams & MacKay
2007) on the **daily laden-departure count**, Poisson-Gamma conjugate:

    y_t ~ Poisson(theta_r),  theta ~ Gamma(a0, b0)
    posterior predictive for a run of length n with sum s: NegBinom(a0+s, b0+n)
    run-length recursion with constant hazard H = 1/lambda

**All parameters fixed a priori — none fitted, none tuned** (this is BOCPD's whole
selling point: it needs almost no training history):
`lambda = 365 d` (≈1 regime change per terminal-year), `a0 = 1, b0 = 1` (weak prior,
mean rate 1/day), `R_SHORT = 3 d`, `P_ALARM = 0.5`, `DROP_FRAC = 0.5`.

**Alarm rule** — a changepoint *downward*, since a rate increase is not an outage:
alarm at `t` iff `P(r_t <= R_SHORT) > P_ALARM` **and** the post-change posterior mean
rate is below `DROP_FRAC ×` the pre-change rate.

### Labels (hindsight, rate-relative, pre-registered)
An outage starts at departure `i` iff `gap_i >= max(14 d, 5 × base_i)`, where `base_i`
is the mean of that terminal's previous 10 inter-departure gaps (needs ≥10 priors).
**Rate-relative is essential, not cosmetic:** Elba Island averages ~26 d between
loadings, so an absolute 21-day rule would label its normal operation as an outage,
while at Sabine (1.4 d) a 21-day gap is a 15× anomaly.

**The rule was validated against externally-known events before being locked** — it
recovers **Freeport 2022-06-08 (gap 252 d, ratio 127×, the explosion, exact date)**,
the **COVID cargo-cancellation wave** (Freeport 2020-05, Corpus 2020-06, Cameron
2020-08), and **Cove Point's annual September turnaround** (2021-25). That external
correspondence is what makes the labels evidence rather than circular self-detection:
the label uses hindsight, the detector sees only data ≤ t — the same physical/knowable
split used throughout.

**n = 15 labelled outages**, `regime='noaa'`, US export terminals. ⚠ **5 of the 15 are
the Cove Point September cluster** — one terminal's recurring, partly *predictable*
maintenance. Concentration risk per §0·3 #1, so results are reported both **overall
(headline)** and **excluding the Cove Point cluster (pre-registered secondary)**, so a
detector cannot be credited for learning one terminal's calendar.

### Nulls
- **N1 absolute:** alarm when `days_since_departed >= 14`.
- **N2 rate-relative (the strong null):** alarm when `days_since_departed >= 5 × base`
  — deliberately *the labelling rule run online*. This is the hardest honest baseline:
  if the Bayesian machinery cannot beat the obvious rule that defines the target, it
  has no value. Note N2 is not trivially perfect — it fires at `5 × base` days, before
  the 14-day floor the label also requires, so it generates genuine false alarms.

### Metrics and acceptance
Per detector, at its pre-registered defaults:
- **recall** — labelled outages alarmed within 21 d of start;
- **median detection delay** (days from outage start to first alarm, detected only);
- **false alarms per terminal-year** — alarms outside any labelled outage window.

**Acceptance: A5 works iff BOCPD achieves a lower median detection delay than *both*
nulls at a false-alarm rate no worse than the better null's.** Detecting faster while
alarming more often is **inconclusive, not a win**, and will be reported as such.

### Span
Per terminal, from its first departure + 90 d burn-in to **its own last departure**
(NOAA ends 2025-12-31) — applying D-017's thrice-stated rule at last. A terminal's
final open gap yields no label by construction (the rule needs a closing departure),
so it cannot be scored and is excluded automatically.

---

## D-018 · 2026-08-12 · **RESULT — A2 does not meet the pre-registered bar**

**The scorecard.** 345 scored weeks, 2019-12-30 → 2026-08-03, `regime='noaa'`,
horizon W1, NB. Everything fixed in D-016 before this ran.

| | A2 (NB) | persistence | climatology |
|---|---|---|---|
| **W1 MAE** | **3.961** | 2.551 | **2.152** |

- **(a) Beat both nulls — FAIL**, and by a wide margin (84 % worse than climatology).
- **(b) 80 % coverage in [70,90] % — PASS at 75.7 %.**
- **(c) Mechanism — `ballast_arrivals_1w` FALSIFIED** (below). The other three hold:
  AR block +0.406, `in_berth` +0.143, `queue_depth` +0.091.

**Verdict: A2 does not meet the bar.**

**Robust to the one known artefact.** NOAA's last departure is 2025-12-31, so the 31
weeks of 2026 have truth ≡ 0 while the model still forecasts ~5.3. Excluding them:
A2 **3.828** vs persistence 2.752, climatology 2.145 — still a clear failure. (See
the process note in D-017 about why those weeks were scored at all.)

**Why it fails — over-extrapolation, the mirror image of A1.** A2's error is
systematically **positive**: mean bias **+2.60**, and it scales with the level.

| year | truth | forecast | bias | bias / truth |
|---|---|---|---|---|
| 2020 | 5.67 | 5.36 | −0.31 | −5 % |
| 2022 | 17.60 | 18.94 | +1.34 | +8 % |
| 2024 | 23.32 | 26.00 | +2.68 | +11 % |
| **2025** | **29.81** | **37.92** | **+8.11** | **+27 %** |

The mechanism is the log link on a strongly trending series fitted over an
**expanding** window. Standardisation uses the training mean, so a 2025 feature value
sits several sds above it, and `exp()` turns that distance into a multiplicative
overshoot. A1 was too **stale** and under-reacted; A2 **over-reacts**. Both lose to a
4-week mean, from opposite directions — which is the more interesting finding than
either failure alone. The left-skewed PIT (85 in the lowest decile vs 11 in the
highest) is the same fact seen through the predictive distribution.

**The falsified mechanism, reported as falsified.** `ballast_arrivals_1w` fits
**−0.049**, negative in **99 %** of weeks, stable across all three eras. D-016
committed to **+** on the physical argument that a ship must arrive empty before it
can leave full, and explicitly forbade post-hoc reinterpretation. So: **the mechanism
as pre-registered is falsified.** Candidate explanations exist (conditional on recent
pace and berth occupancy, extra ballast arrivals may indicate ships *waiting* rather
than loading; or collinearity with `in_berth`/`queue_depth`) — these are **untested
hypotheses for a future pre-registration**, not an explanation of this result.

**The NB prior did not pay off, and cost nothing.** Fitted `k` runs to the `K_MAX`
bound (median 1e6) — i.e. the data is *not* over-dispersed once the features are
conditioned on, so NB collapses to Poisson (MAE 3.961 vs 3.980). D-016 chose NB on
the prior that A1's under-dispersion would recur; that prior was wrong here. Worth
recording: the reasoning was sound and the choice was free, but it did not help.

**Same low-volume-only pattern as A1.** A2 beats both nulls in 2020 (truth 5.67/wk)
and 2019 only, and on the sparse `gfw` slice it beats one null. Both models win only
where the market is small and flat.

**Kept as-is.** No feature dropped, no window changed, no slice excluded from the
headline. The bar existed before the number (§0·3 #3).

**What it sets up.** Two independent failures now point the same way: **the weekly
US-loadings series is dominated by a trend that neither a stale mechanism nor a
log-linear extrapolation handles, and a 4-week mean is a genuinely strong baseline on
it.** Concrete, pre-registered implications for A3/A4:
1. **A4 (Kalman/local-level) is now the better-motivated next model than A3** — a
   local-level state-space model is *designed* for exactly this: track the level,
   don't extrapolate it multiplicatively. That reordering is a prediction made now.
2. Any future count model on this target should be pre-registered with either a
   **rolling** window or an explicit **level/trend decomposition**, and should be
   checked for multiplicative overshoot at high feature values before scoring.
3. The persistence/climatology nulls are strong here, not weak. Beating them is the
   real bar for Part A, and nothing has yet.

---

## D-017 · 2026-08-12 · Bug found and fixed — the target window was being sliced away

**What happened.** The first A2 replay produced an obviously-impossible scorecard:
every target 0.00, every coefficient exactly 0.000, MAE 0.000, coverage 100 %, while
the nulls read 16.9. It was reported as a bug, not a result, and not written up.

**Cause.** `build_row` took a single `events` list. Its docstring said the target is
read from the *full hindsight* stream — but `build_all_rows` passed the stream
already sliced to `<= as_of`. The target window `(as_of+u0, as_of+u1]` therefore
contained no events by construction, so every label was 0. The features were fine,
which is why persistence (read from `feature('lag1')`) still looked sensible — the
inconsistency between a 16.9 baseline and a 0.00 truth is what exposed it.

**Fix, and why it is structural rather than a patch.** `build_row` now takes
`feature_events` and `target_events` as **separate, non-interchangeable parameters**,
with the asymmetry documented at the signature: features must be sliced, the target
must not be. Two regression tests pin it — one asserting the sliced-stream case
yields the wrong answer (`target == 0`), one asserting feature windows still cannot
see the future even when handed the full stream. The unit tests had passed
throughout, because they legitimately passed the full list; only a harness-level test
catches a caller violating the contract.

**Process note, owned.** D-012 recorded that the A1 harness's "score a week only if
the population is non-empty" rule was insufficient, and that **"score only while the
regime still has departures" should be pre-registered next time**. D-016 is that next
time, and I did not carry the rule across. The consequence is the 31 all-zero 2026
weeks in D-018. Per D-012's own reasoning the filter was **not** applied after the
fact; the headline stands and the sensitivity is reported alongside (the verdict is
unchanged either way). **Carry-forward, now stated for a third time so it is not
missed again: any Part-A replay must pre-register the end of each regime's scored
span from that regime's last departure, not from the panel's data max.**

---

## D-016 · 2026-08-12 · **A2 design lock — pre-registration (binding, before any fit)**

**Context.** A2 build step 0. A1 failed because a no-knob mechanism ran on inputs
3-15 months stale (D-013). A2 is the first *fitted* model: a small count GLM whose
features are all observable **today**, refit weekly. This entry is the §0·3 #3
pre-registration and is binding — every quantity below is fixed before a single
skill number exists.

### Target
Weekly **US laden loadings** — `departed` events with `laden_flag IS TRUE` from
`zone IN (usgulf, usatlantic)` — counted in `(as_of+u0, as_of+u1]`, horizons
**W1 = (0,7] d** and **W2 = (7,14] d**. Window convention matches A1 (D-009) so the
two models are directly comparable.

*Why loadings, not arrivals, as primary:* it is the single cleanest line in the
panel — NOAA-native at **both** ends (unlike every US→EU leg, which is NOAA-out /
GFW-in, D-014), decade-deep, and it is the supply pulse Part B ultimately wants.
The EU-arrivals target (A1's) is scored **secondary**, for a like-for-like A1
comparison.

### Model
`log λ = β0 + Σ βj xj`, count ~ **Negative Binomial** (`Var = μ + μ²/k`).

**NB is the headline; Poisson is fitted every week as a nested cross-check.** No
data-dependent switching rule. Reasons, both prior not empirical: NB nests Poisson
(`k → ∞`), so it cannot be worse in-sample and is safer out-of-sample; and A1's
diagnosed calibration failure was **under-dispersion** (U-shaped PIT, D-013), which
is exactly what Poisson's forced `Var = μ` would repeat. Deliberately **not**
decided by peeking at the target's dispersion.

No exposure offset: all windows are exactly 7 days. The §0·2 `log(capture)` offset is
**deferred** — it needs point-in-time EIA and is its own piece of work; the
walk-forward refit plus the AR features absorb slow level drift. Recorded as a known
omission, not an oversight.

### Features — pre-registered with expected signs
All NOAA-native, all computable from events ≤ `as_of`. Built by reusing the existing
pure pairers (`visits.pair_visits`, `queues.pair_queues`) with `signal.py`'s
`OPEN_VISIT_CEILING_DAYS` / `QUEUE_OPEN_CEILING_DAYS` — **not** reimplemented in SQL.
(A naive "anchorage_entry with no later moored" probe returned queue_depth=30 against
in_berth=1 at 2023-01-02: stale entries accumulating, precisely the phantom the
ceilings exist to kill.)

| # | feature | mechanism | sign |
|---|---|---|---|
| 1 | `lag1` — loadings in `(as_of-7d, as_of]` | recent pace | **+** |
| 2 | `trail4` — mean of the 4 elapsed weeks | monthly pace | **+** |
| 3 | `in_berth` — vessels moored at a US export terminal at `as_of`, visit still open | a ship at berth departs within days — near-deterministic for W1 | **+** |
| 4 | `ballast_arrivals_1w` — ballast `zone_entry` at US export zones in the last 7 d | the feedstock: a ship must arrive empty before it can leave full; ~1-2 wk lead, so stronger for W2 | **+** |
| 5 | `queue_depth` — open anchorage queues at US export terminals at `as_of` | ships waiting are the next berths filled | **+** ⚠ |

**Feature 4 is deliberately US-side.** The natural "ballast inbound" feature is
ballast departures from Europe — but those are **GFW-only** (10,271 events; NOAA has
exactly 0, since NOAA cannot see a ship leave Rotterdam). Using it would put a
GFW-capture-drifting covariate in a NOAA-target model, straddling a fidelity seam
(SIGNALS.md §2.1) with the covariate's scale drifting independently of the target's.
The US-side ballast **arrival** captures the same mechanism one step later and is
NOAA-native: 6,447 ballast vs 2,295 laden `zone_entry` at US export zones, only 272
unknown (3 %).

⚠ **Feature 5's sign is the one genuine ambiguity** and is called out rather than
hedged. Queue-as-feedstock says **+**; queue-as-congestion (SIGNALS.md's reading, an
outage backing gas up) says **−**. Committed to **+**, on the grounds that at a 1-2
week horizon the anchorage is mechanically the input to the berth, and the outage
channel is a different regime (A5's subject). **A robustly negative fitted
coefficient falsifies the mechanism as pre-registered and must be reported as a
falsification — not reinterpreted post hoc as "the congestion channel dominating".**

**Sign test, collinearity-aware.** `lag1` and `trail4` are strongly collinear by
construction, and collinearity can flip individual signs without harming prediction.
So the AR block is tested **jointly** (`β1 + β2 > 0`) and the three physical features
**individually** (`β3, β4, β5 > 0`).

### Training protocol
- **Expanding window, minimum 104 weeks.** Not rolling — and the contrast with A1 is
  the point: A1's `π` is a *rate* whose level drifted 4×, so it had to roll; A2's βs
  are *ratios*, which §0·1 documents as barely affected by the coverage gradient, and
  level drift is absorbed by `lag1`/`trail4`. Expanding maximises N for a 6-parameter
  fit.
- **Purge (Part C #1).** At fit date `T`, a training row at `as_of` is admitted only
  if its target window has closed: `as_of + u1 <= T`. No further embargo is needed —
  features are point-in-time by construction, so nothing else overlaps.
- **Standardisation on training statistics only** (means/sds from the purged training
  rows, never the test point) for optimiser conditioning. Sign-preserving, so the
  sign tests are unaffected; coefficients reported in standardised units.
- Fit by direct MLE with `scipy.optimize` (scipy is already a dependency).
- Refit **every week**. First scored `as_of` = **2018-01-01**, matching A1 (D-008) —
  104 weeks of training then starts from 2016-01.

### Nulls
Identical to A1's (D-011): **persistence** = last fully-elapsed week's count;
**climatology** = trailing 4-week mean. Note these *are* features 1 and 2 — by
design. A2 therefore effectively contains both nulls, so the real question the
scorecard asks is **whether the three physical features add anything on top of the
autoregression**. A2 losing to persistence would indicate something badly wrong.

### Acceptance bar (pre-registered)
Primary: US loadings, `regime='noaa'`, horizon **W1**, NB model.
- **(a)** A2 beats **both** nulls on MAE.
- **(b)** 80 % interval coverage in **[70, 90] %**.
- **(c)** Pre-registered signs hold: `β1+β2 > 0`, `β3 > 0`, `β4 > 0`, `β5 > 0`.

**Verdict: A2 works iff (a) and (b).** (c) is a **mechanism** test, reported
separately and binding on the *narrative*, not on pass/fail — a model may predict
well through a mechanism other than the one claimed, and that distinction is the
finding, not something to paper over.

Secondary, reported but not gating: W2; the Poisson cross-check; coefficient paths
and per-era stability (§0·3 #1 — a sign that flips between eras is episode
memorisation in coefficient form); and the **EU-arrivals target**, where the extra
question is whether A2 beats **A1's W1 MAE of 2.314** on the same grid and regime.

**Stated in advance about `gfw`:** for the *loadings* target the GFW series is
expected to be near-meaningless, because GFW's US-departure capture collapses from
93 % (2020) to 14 % (2025) (§0·2 / D-014) — a non-stationary observation process, not
a market signal. It is reported for completeness; no conclusion will be drawn from it.

---

## D-015 · 2026-08-11 · A1 closed — write-up filed, obligations discharged

**Status.** A1 is complete: built (steps 1–4), validated (step 5), scored (step 6,
D-013), and written up. **No further A1 work** — the negative result is kept as the
fixed reference A3 must beat, and tuning it post-scorecard would be the §0·3 #3
forking path (any change from here would be chosen *because* it improves a number
already seen).

**Write-up artefacts, and where each lives:**
- **Result block** — MODELS.md Part A, under the A1 spec (headline table, bias
  mechanism, PIT caveat, D-014 regime caveat). The research note's A1 section reads
  from there + D-001…D-014 for the why.
- **SIGNALS.md §2.2 warning** — the hindsight-`zone_scope` fence (D-004b's promised
  deliverable). A Tier-4 validator check was considered and **rejected**: "no
  consumer uses the band as a PIT feature" is a property of consumers, not of the
  data — a data-integrity sweep cannot assert it. The binding rule lives in the
  warning + D-004b.
- **Build order** — Track 2 item 7 marked done-negative; CLAUDE.md updated
  (`make a1-replay` + modelling-status line).
- **Code** — `analysis/a1.py` (model, pure), `analysis/a1_replay.py` (harness, no
  tunables), 66 tests in `tests/test_a1.py`; probes kept: `check_pit_legs`,
  `check_pi_conditional`, `check_a1_pi`, `check_a1_forecast`, `check_a1_predictive`,
  `check_a1_truth`, `check_a1_enrich_invariance`.

**What carries forward:**
1. **A3's brief (pre-registered in D-013):** A1's ceiling is staleness from the 90 d
   maturity gate; A3's censored fit uses *open* legs, needs no gate, and should win
   for exactly that reason. Check that mechanism first when A3 lands.
2. **Part-B constraint:** any A1-style mechanism nowcast is a *lagging* indicator.
3. **Harness lesson (D-012):** pre-register "regime still has departures" as the
   scoring criterion next time, not just "population non-empty".
4. **Regime framing (D-014):** all US→EU models are NOAA-out/GFW-in; say
   "NOAA-departure legs", state arrival-side fidelity explicitly.
5. **Reusable machinery for A2/A3:** the hoisted-events replay pattern, the
   `Predictive` scoring stack (CRPS/PIT/intervals), the truth/null functions, and
   the D-011 conditioning discipline all transfer as-is.

---

## D-014 · 2026-08-11 · Finding — `regime='noaa'` legs are NOAA-out, GFW-in

**Context.** Reviewing how the two backfill feeds were treated in the A1 replay.

**Finding.** Cross-tabulating departure feed against arrival feed for laden US→EU
legs:

| departure | arrival | n |
|---|---|---|
| **noaa** | **gfw** | **2,647** |
| gfw | gfw | 485 |
| noaa | noaa | **0** |

**Every NOAA leg has a GFW arrival; there are no NOAA arrivals in Europe at all** —
NOAA is US terrestrial AIS and physically cannot see Rotterdam. So `regime='noaa'` is
not "a NOAA-fidelity voyage"; it is *NOAA saw it leave, GFW saw it arrive*, tagged by
departure per `config.regime_of`'s pipeline-wide convention.

The two regimes also **partially overlap**: the share of GFW US laden departures that
NOAA also recorded within ±3 d runs 20 % (2018) → 6.7 % (2020) → 26 % (2022) → 42 %
(2024) → **65 % (2025)**, tracking the §0·2 crossover (NOAA sparse-then-exhaustive,
GFW strong-then-faded). Those voyages appear as a leg in *both* regimes and pair to
the same GFW arrival event.

**Treatment was correct; two claims were overstated.**
- *Correct:* regimes are segmented and never pooled (SIGNALS.md §2.1), so the known
  `regime='all'` double-count never enters. Within each scorecard the forecast and
  the truth are drawn from the same leg population, so A1 predicts exactly the event
  it is scored on. And `pi` is capture-inclusive, so a GFW-missed EU arrival deflates
  forecast and truth by the same amount.
- *Overstated #1:* calling D-013's headline "the NOAA decade" — the **target** is
  100 % GFW-observed. Say "NOAA-departure legs".
- *Overstated #2:* D-013 presented the `gfw` win as A1 succeeding in a second
  setting. With up to 65 % voyage overlap it is a **partially-redundant, sparser view
  of the same voyages**, not independent corroboration. The claim should be dropped
  to "A1 wins only at low volume", which the 2018 slice already shows.

**Does not change the verdict.** A1 and both nulls are computed from the *same* truth
series, so any GFW capture limitation deflates all three identically — a measurement
artefact in truth cannot explain A1 losing to a moving average *of that truth*.
Independent support: truth reads 14.4 arrivals/wk in 2025, consistent with US
export volumes; GFW's *US departure* capture collapsed to ~14 % by 2025 but its *EU
arrival* capture evidently did not, or the truth series would fall rather than rise.

**Carry into A2/A3.** The same asymmetry applies to any US→EU model: the supply side
can be NOAA-fidelity, the demand side is GFW-only. Per-regime scoring must keep
tagging by departure (for consistency with the panel) while the write-up states the
arrival-side fidelity explicitly.

---

## D-013 · 2026-08-10 · **RESULT — A1 does not meet the pre-registered bar**

**The scorecard.** 448 weekly as-ofs, 2018-01-01 → 2026-07-27, primary regime
`noaa`, horizon W1. Every choice fixed before this ran (D-001/003/007/008/009/011).

| | A1 | persistence | climatology |
|---|---|---|---|
| **W1 MAE** | **2.314** | 2.217 | **1.814** |

- **(a) Beat both nulls on W1 MAE — FAIL.** A1 loses to persistence *and* to a
  trailing 4-week mean.
- **(b) 80 % coverage in [70, 90] % — PASS at 73.7 %.**

**Verdict: A1 does not meet the bar.** It is not a usable arrival nowcast, and per
D-003 it does not even stand as a point baseline, because the point forecast is the
half that failed.

**The failure is real, not an artefact.** A1 loses to climatology in **2022, 2023 and
2024** — clean, high-volume years with no data-boundary effects. And it is not a
crisis-episode story (§0·3 #1): it loses in both the 2021-22 split (2.864 vs 2.171)
and all other years (2.148 vs 1.706). One boundary artefact does exist — after the
NOAA backfill's last departure (2026-01-28) truth is structurally 0 while lingering
phantom open legs still generate forecasts (30 weeks, bias +1.55). Removing those
weeks makes the result *slightly worse*, not better: A1 2.369, persistence 2.366,
climatology 1.871. The verdict is robust to it.

**Why it fails — the staleness is decisive, and the bias column proves it.**

| year | truth | forecast | bias |
|---|---|---|---|
| 2021 | 3.60 | 3.75 | +0.15 |
| **2022** | **9.48** | **6.00** | **−3.48** |
| 2023 | 10.73 | 11.40 | +0.67 |
| **2024** | **8.60** | **11.36** | **+2.76** |
| **2025** | **13.44** | **11.17** | **−2.27** |

Textbook lag: A1 under-shoots on the way up (2022's Europe surge), over-shoots on the
way down (2024), under-shoots again on the recovery (2025). Pooled bias is only −0.24
because the errors cancel — which is why MAE, not bias, is the headline. This is
exactly the cost D-007 recorded when fixing `MATURITY_DAYS` at 90 d on structural
grounds: `pi` is estimated from departures at least 3 months old within a 365 d
window, so it trails the market by roughly 3-15 months. A 4-week moving average of
actual arrivals simply has fresher information, and on a market that moved 4× in a
decade that beats mechanism.

**Calibration passes but should not be claimed.** (b) passes at 73.7 %, yet the PIT
histogram is strongly U-shaped (72 and 97 in the extreme deciles against ~45 uniform)
— the forecast is **under-dispersed**, too confident. The interval test survives
partly because discreteness makes intervals conservatively wide (D-010). Both
statements are true; the write-up must give the PIT, not just the coverage number.

**What did work, reported without inflation.** On the `gfw` regime A1 beats both
nulls at both horizons (W1 MAE 0.950 vs 1.199 / 1.006) — but `gfw` is the secondary
regime with mean truth 1.55/wk, where near-zero forecasts are easy. It also beats
both in 2018 (mean truth 0.60). The honest summary is that A1 wins where volume is
low and the market is flat, and loses everywhere that matters.

**This result is kept.** No parameter is changed, no window retuned, no slice
dropped. The bar was set in D-003 before any skill number existed precisely so that
this outcome could be reported rather than engineered away (§0·3 #3). A negative
result on a pre-registered baseline is a finding, not a failure of the work.

**What it sets up.** The diagnosis is specific and actionable, and it is A3's brief:
A1's ceiling is staleness, and A3's censoring-aware survival fit is built on
**open** legs (right-censored), so it does **not** need the 90 d maturity gate and
can use current data. That is now a concrete, mechanism-level prediction about why
A3 should beat A1 — pre-registered here, before A3 is built. The other live thread
is that any Part-B use of A1's output must treat it as a lagging indicator.

---

## D-012 · 2026-08-10 · Step 6 — the replay harness

**Context.** Mechanical assembly: walk the grid, forecast, score. The module contains
no tunable and must never acquire one.

**Decisions.**
- **Skip leg enrichment.** `compute_legs(enrich=...)` only selects among `open_*`
  sub-statuses; A1 reads open-vs-closed and `is_eu_arrival`, both enrichment-
  independent. Verified: forecasts identical to 1e-12 at four as-of dates spanning
  2019-2025. Drops the per-as-of LATERAL and declaration query (~7 s → ~0.03 s).
- **Hoist the events.** `pair_legs` is pure, so the decade's 43,589 leg events are
  fetched once, sorted, and sliced by bisect per as-of. Three DB queries total, not
  448 × 3. Full replay runs in ~2 min.
- **Score a week only if the regime's forecast population is non-empty.** Structural
  ("was A1 asked a question?"), decided when building the harness and *before* any
  score existed — not a performance filter. ⚠ It proved **insufficient**: it does not
  catch weeks where legs exist but the feed has ended, which is the 2026 artefact in
  D-013. A pre-registered "score only while the regime still has departures" would
  have been better. **Not applied retroactively** — that would be choosing a filter
  after seeing it move the number. Recorded for the next model.
- **CRPS comparability.** The CRPS of a deterministic forecast equals its absolute
  error, so the nulls' MAE columns compare directly with A1's CRPS — the harder
  comparison for A1, which gets no credit for expressing uncertainty.
- **PIT seed fixed at 20260810** and recorded, so the histogram reproduces.

**Verified.** 391 tests pass, ruff clean; 8 new harness tests (grid construction,
MAE/RMSE/null columns, bias, coverage counting with inclusive bounds, empty-slice
safety, PIT binning).

---

## D-011 · 2026-08-10 · Step 5 — truth series; and the persistence null in D-003 leaked

**Context.** A1 build step 5: the series A1 is scored against, and the two nulls it
must beat.

**Truth definition (implements D-001).** Computed on a **hindsight** leg load — that
is what the `physical` basis is for — as: laden legs with origin ∈ {usgulf,
usatlantic}, `departed_ts <= as_of`, whose `arrived_ts` falls in `(as_of+u0,
as_of+u1]` at an EU import zone. Two properties keep the comparison honest:
1. It counts the **same event** the forecast predicts — one `is_eu_arrival`
   definition, shared by both sides, so both are capture-limited identically.
2. It is **conditional on having departed by `as_of`**, matching the forecast
   population. An arrival from a vessel that departed *after* `as_of` was never
   visible to A1; counting it would score A1 against a target it was structurally
   denied. **This is material, not pedantic:** 1,200 of 3,200 real US→EU legs arrive
   within 14 d, and the conditioning changes 156 of 417 W2 weeks (sum 3,099 vs
   3,321 unconditioned).

**Amendment to D-003 — the persistence null was leaky for W2.** D-003 worded it as
"the realised value of the same conditional statistic at `as_of − 7d`, same horizon".
For W1 that is fine. For **W2** the statistic at `as_of − 7d` spans `(as_of,
as_of+7d]` — *the future at `as_of`*, and in fact exactly the W1 truth, which is a
strong predictor of W2. Taken literally, D-003 would have scored A1 against a
baseline that cannot exist.

**Decision.** Persistence = the **last fully-elapsed 7-day count**, `(as_of−7d,
as_of]`, for *both* horizons. Identical to D-003's literal reading for W1; the only
point-in-time reading available for W2.

> **Direction, stated plainly:** this makes the null **weaker**, and therefore makes
> A1's bar easier to clear. That is not the reason for the change — a baseline
> computed from data the forecaster does not have is not a baseline — but the
> direction is exactly the kind a reader should be suspicious of, so it is recorded
> rather than buried. The climatology null was unaffected (all four of its windows
> already lie at or before `as_of`).

Conditioning each null week on departures known at *its own* start keeps it
like-for-like with the target. That conditioning is a formality here: **no US→EU
laden leg in the decade arrives within 7 days of departing** (observed minimum
**7.04 d**, 0 of 3,200 under 7 d), so a leg cannot depart and arrive inside the same
persistence week.

**Verified (2026-08-10, live DB; 418 weekly as-ofs, 2018-01-01 → 2026-01-01).**
- **Exactly-once holds exactly.** Summing the W1 truth across the grid gives
  **3,321**, equal to a direct count of qualifying legs arriving in the span. No
  arrival dropped, none double-counted — guaranteed by the ≥7.04 d minimum, since an
  arrival in `(as_of, as_of+7d]` always has its departure at or before `as_of`.
- **Independent corroboration.** `od_flow_count` over US→EU lanes for the same span
  reads **3,316** against truth **3,321** — a 0.15 % gap, and it is a differently
  built signal (dated by *departure*, ballast included), so the agreement is
  meaningful rather than circular.
- Series shape is sane and matches known US LNG history: 1.11 arrivals/wk (2018) →
  5.42 (2021) → **12.63 (2022, the Europe surge)** → 14.40 (2025).
- 383 tests pass, ruff clean; 11 new tests, including that a post-`as_of` departure
  is excluded from truth but counted from a later `as_of`, and that neither null can
  see a future arrival.

---

## D-010 · 2026-08-10 · Step 4 — exact Poisson-binomial predictive distribution

**Context.** A1 build step 4. D-003 scores CRPS and interval coverage, both of which
read probability *masses*, not just a mean and variance — so the forecast has to be a
distribution, not two moments.

**Decision.** `poisson_binomial_pmf` — straight convolution DP, `new[k] =
old[k]*(1-p) + old[k-1]*p`, folding one leg in at a time. Exact, and numerically
stable here because every term is positive (no cancellation). Wrapped in a
`Predictive` dataclass exposing `mean` / `variance` / `cdf` / `quantile` /
`interval` / `crps` / `pit`.

- **Zero-probability legs are dropped first** — exact (a Bernoulli(0) leaves the
  convolution unchanged), and it is what makes the DP cheap. Measured: at 2025-01-06
  the forecast carries **1,348 open legs but only 129 with p > 0** (the rest are
  long-overdue legs whose window contains no EU arrivals at all), so an O(1348²) fold
  becomes O(129²).
- **`crps` is the Ranked Probability Score**, `sum_k (F(k) - 1{y<=k})²`, summed past
  the support so an observation beyond it is penalised correctly.
- **`pit` is the randomised discrete PIT**, `F(y-1) + u·P(Y=y)`, with `u` passed in
  rather than drawn — keeps the function pure and lets the replay control the seed.
- **`interval` is conservative by construction.** Discreteness means realised
  coverage ≥ nominal, so D-003's "80 % interval covering 70–90 %" is read against a
  slightly wide interval. Recorded so over-coverage is not later mistaken for
  miscalibration.

**Independence is A1's assumption, stated plainly.** `Var = sum p(1-p)` holds only if
legs arrive independently. A terminal outage or a freeze correlates them and true
variance would exceed this. That correlation is A5's subject; A1 does not model it.

**Verified (2026-08-10, live DB).** PMF integrity exact: mass sums to
`1.000000000000`, `mean` matches `sum(p)` to 1.8e-15, `variance` matches
`sum(p(1-p))` to 0. Unit-tested against brute-force enumeration over all 2ⁿ outcomes,
against the binomial closed form for equal `p`, and for the zero-dropping identity.
372 tests pass, ruff clean; 18 new tests.

**Honest finding — the exact PMF does not beat the normal approximation on
intervals.** At all five probe dates the exact and normal 80 % intervals are
*identical* after rounding to counts (`[0,2]`, `[2,6]`, `[3,9]`, `[8,16]`, `[8,15]`),
including the low-count 2018 date (mean 0.83) where normality should be worst. The
exact PMF is still the right thing to carry — CRPS and PIT need the masses, and it
costs nothing at these sizes — but it should not be claimed as a source of accuracy
over a Gaussian. Recorded now so the write-up does not overclaim it later.

> ⚠ **Caution flag, not a result.** On the five probe dates, 3 fall outside the 80 %
> interval (2022-03-07: 5.8 vs 16 realised; 2023-01-02: 12.1 vs 7; 2025-01-06: 11.5
> vs 18), in both directions. Five hand-picked non-random dates prove nothing about
> calibration — and one was chosen precisely *because* it was already known to be a
> large miss (the `MATURITY_DAYS` staleness, D-009). It is logged as a flag to carry
> into step 6, and **nothing here may be used to adjust the model** (§0·3 #3): the
> scorecard is the whole replay and the bar is D-003, fixed in advance.

---

## D-009 · 2026-08-10 · The two factors telescope — one population, exact estimator

**Context.** A1 build step 3: add the timing factor `F_eu` and assemble `p_i(W)`.

**Finding (algebraic, then pinned by test).** An EU leg "open at age `a`" is exactly
one whose duration exceeds `a`, so `N_eu(open at a) == N_eu(dur > a)` and D-007's
kernel collapses:

    pi(a) * [F_eu(a+u1)-F_eu(a+u0)] / (1-F_eu(a))
      = [N_eu(open a)/N(open a)] * [N_eu(dur in window)/N_eu(dur > a)]
      =  N_eu(dur in window) / N(open at a)

The decomposition is an **exact factorisation of a single count ratio**, not two
independent approximations multiplied together — *provided both factors come from
one population*.

**Decision.** Build **one `ArrivalCurve` per (origin_zone, regime) from one matured
population**, carrying two sorted arrays (`closes`, `eu_closes`). Evaluate the
telescoped form directly; expose `pi_at` and `f_eu` as views for interpretation and
as A3's starting point. Consequences:
- **The tail rule is gone.** D-002's "a beyond ECDF p99 ⇒ p_i = 0" existed to dodge
  the `0/0` in `[..]/(1-F_eu(a))`. The telescoped form has no such ratio: an age with
  no EU legs ahead of it scores 0 on its own. One fewer knob.
- Separate ladders for `pi` and `F_eu` are impossible by construction, so the two can
  never be silently drawn from different windows/regimes.
- All estimates are `bisect` suffix/range counts — exact at arbitrary ages, O(log n).

**Window convention: `(a+u0, a+u1]`, left-open right-closed.** Not the panel's
half-open calendar-day convention, and deliberately so: this is a *duration* window
measured from a leg's own age, "still open at `a`" already means `duration > a`
strictly, and `n_open`/`n_eu_open` are `bisect_right` suffix counts. Matching the
window to them makes the windows partition `(a, a+u1]` exactly and the telescoping
identity hold *exactly* rather than up to a boundary. Found by the identity test
failing (`0.235` vs `0.441` at `a=10`) — a genuine convention bug, not a bad test.
Residual disagreement with the calendar-week truth series is measure-zero: an arrival
exactly at `a` is impossible (that leg is closed, so not in the forecast population)
and one exactly at `a+u1` requires a timestamp collision.

**Forecast population = every open laden US-export-origin leg, `open_censored`
included.** Not an oversight. The curve's denominator is "matured legs still open at
age `a`", which likewise makes no status distinction — many of those resolved to
phantoms, and `pi(a)`'s collapse past day ~12 *is* the model learning that.
Filtering phantoms from the forecast population while leaving them in the estimation
population would apply the correction twice and bias the forecast up.

**Verified (2026-08-10, live DB).** Assembled forecast runs end to end at 9 as-of
dates 2018→2025. Magnitude check: at 2025-01-06 it predicts **11.5 EU arrivals/week**
(sd 3.0) against actual 2025 US exports of ~1,150 cargoes/yr ≈ 22/week with roughly
half Europe-bound — the right order, tracking the decade's growth (0.8/wk in 2018 →
13.6/wk in 2024). Each leg is scored on its own `(origin, regime)` curve. 354 tests
pass, ruff clean; 13 new tests including the telescoping identity across ages ×
windows.

> **Not evidence of skill.** Those nine dates are a smoke test on hindsight truth,
> not the scored replay. Single-week deviations are large (W1 sd ≈ 3) and some are
> stark — 2022-03-07 predicts 5.8 against 16 realised, which is exactly the
> `MATURITY_DAYS` staleness D-007 documented, arriving in the wild: in March 2022 the
> curve still reads the pre-Ukraine 2021 mix. **Nothing here may be used to adjust
> the model** (§0·3 #3); the scorecard is step 6 and the bar is D-003.

---

## D-008 · 2026-08-10 · Scoring start moves 2017-01 → 2018-01 (amends D-003)

**Context.** D-003 pre-registered the replay grid as starting 2017-01-02, on the
assumption that 2016 alone was enough burn-in. Building the estimator showed it is
not: at 2017-01-02 the matured 365 d population is **n = 46**, below the
`PI_MIN_LEGS = 100` floor, and every ladder rung falls through — the curve returns
`UNSUPPORTED`. First supported date is 2018-01-01 (n = 124).

**Decision.** First scored `as_of` = **2018-01-01**. 2016–17 becomes burn-in that
seeds the empirics without being scored.

This is an amendment made **before any scoring**, on the pre-committed sample-size
criterion (`PI_MIN_LEGS`), not on observed skill — logged rather than silently
applied so the change is auditable. Replays may still be *run* from 2017 for
diagnostics; those dates are reported as `UNSUPPORTED` and excluded from the
headline scorecard.

**Consequence.** Scored span 2018-01 → the last date with full W₂ truth; ~8.5 years
of weekly as-ofs rather than ~9.5. The NOAA-departure decade claim in D-003's
acceptance bar reads on that span.

---

## D-006 · 2026-08-10 · D-004a implemented — `compute_legs(point_in_time=True)`

**Context.** A1 build step 1. Closes the as-of leak logged in D-004a.

**Decision.** Additive `point_in_time: bool = False` flag on `compute_legs`; default
path byte-for-byte unchanged (live pipeline / viz / vf_rescue untouched). When True,
all three sources are bounded by `now`:
- events `event_time <= now`;
- last fix — per-MMSI `LATERAL … fix_ts <= now`, replacing the global `DISTINCT ON`;
- declaration — **source substituted**: `priority_watchlist` is a current-snapshot
  table with no history and is structurally un-replayable, so the point-in-time path
  reads raw `vessel_state.dest` at `state_ts <= now` (90 d window, mirroring
  `scoring.py`'s `latest_state`) and re-parses it via `dest_parser` in a new pure
  `resolve_dest_regions()`.

**Two-pass last-fix narrowing.** Last-fix evidence only affects *open* legs, and
openness is decided by departure time alone — independent of the evidence. So pair
once with no evidence, probe only the vessels holding an open leg, re-pair.
Provably equivalent to a single full-fleet pass; probes ~50–100 MMSIs, not ~830.

**Verified (2026-08-10, live DB).**
- *The leak was real and large.* At `as_of = 2020-06-01`, the default loader returns
  20,351 legs of which **13,130 are closed by an arrival after `as_of`**, and 2,101
  carry a declared `dest_region` — in a year when no declarations existed at all
  (declarations arrive only with the 2026 live feed). Bounded: 4,713 legs, **0**
  future closures, **0** declarations. The `open_floating` count that D-004a
  predicted would be inflated: 95 → 9.
- *Reduction property holds.* At `as_of = 2026-08-11` (past the data max,
  `port_events` 2026-08-10 15:12 UTC / `ais_fixes` 15:51 UTC) the two paths are
  **identical on every status count and on the declaration count (2101 = 2101)** —
  so the `vessel_state` re-parse reproduces the stored `priority_watchlist` parse
  exactly, and the substitution is sound. At a *midnight* `as_of` on the final day
  they differ slightly (10 legs floating→censored, 13 declarations), correctly:
  midnight excludes that day's fixes.
- *Cost.* 44.4 s → 6.5 s per as-of date (6.8×); the narrowing alone took 22.9 s →
  7.2 s. ~520 weekly replays ≈ 1 h, and step 6 can hoist the event fetch out of the
  loop. `last_fix` was 20.3 s of the original 22.9 s (828 LATERAL probes returning
  147 rows).
- 325 tests pass, ruff clean. 8 new tests: bounded-SQL selection + `as_of` binding,
  default-path regression, the leak end-to-end, post-`as_of` arrival hidden, probe
  narrowing, `last_fix_*` preserved on in-window open legs, and 4 pure
  `resolve_dest_regions` cases. `scripts/check_pit_legs.py` re-runs the DB parity
  check (kept — there is no DB test harness to hold this invariant).

**Rejected alternatives.**
- *Making the bounds unconditional* — would silently change `signal.py --as-of`
  semantics for the whole panel. Out of step-1 scope; see the open item below.
- *Making the `vessel_state` declaration path the only path* — more correct in
  principle, but changes the live signal for no A1 benefit.
- *Probing only `open_censored` vessels* (a slightly cheaper narrowing) — would
  blank `last_fix_*` on in-window open legs, an invisible API asymmetry, for a
  negligible saving.

**Opened, not closed.** `signal.py --as-of` still calls the default loader, so a
historical panel rebuild retains the same leak (a future arrival closes a leg; the
`knowable` basis then trims by `arrived_ts`). This does **not** affect A1, which
consumes `compute_legs` directly, and it does not affect the shipped panel, which is
built at true-now. Whether the panel's `--as-of` should adopt `point_in_time=True`
is a separate decision with its own blast radius — deferred, with its own entry when
a consumer needs it.

---

## D-005 · 2026-08-10 · MODELS.md corrections folded into the A1 spec

**Context.** Three claims in the previous A1 paragraph were wrong or dead.

**Decision.** Corrected in the rewritten spec:
1. A1 cannot "feed off `gas_in_transit_volume` + `laden_voyage_age_d` +
   `voyage_speed_kn`" — those are daily reductions; a convolution needs per-leg
   state. A1 consumes `compute_legs()` directly.
2. "Validate within weeks as legs complete" is dead — ingest stopped 2026-08-10;
   validation is historical replay (D-003).
3. "No fitted parameter" sharpened to "no *fitted* parameter, two trailing
   empirical objects" — and the kinematic framing renamed to duration climatology
   (D-002), since the distance algebraically cancels.
