# Research plan — train on history, forward-test on live

Execution companion to [`SIGNALS.md`](SIGNALS.md) (*what* can be measured),
[`MODELS.md`](MODELS.md) (*how* to model it), and
[`ingestion/historical/PLAN.md`](../ingestion/historical/PLAN.md) (*how* the
historical corpus is built). This doc covers the **experimental design**: given a
deep historical corpus *and* a thin-but-growing live feed, how do we actually do
the research without fooling ourselves.

Read it after the other three — it depends on the fidelity/regime vocabulary they
establish.

---

## 0 · The premise — two corpora, two jobs

The backfill (`PLAN.md`) inverts the workflow. We do **not** wait months for a live
modelling corpus and then begin. Instead:

| Corpus | Role | Size |
|---|---|---|
| **Historical** (NOAA 2016+, GFW 2017+, controls) | **training + validation** (walk-forward CV *within* history) | `N ≈ 3,000` daily rows, `N_eff ≈ 330` |
| **Live** (mmsi_filter, 2026-05-30→) | **true out-of-sample forward test** — data the model never saw when fit | `N ≈ 60`, `N_eff ≈ 7` (as of 2026-06-12, growing ~1/day) |

This is the standard quant discipline stated plainly: in-sample fit is worthless;
the only honest validation is performance on data that **did not exist when the
model was built**. The live feed is exactly that — a forward test that accrues in
real time, the strongest evidence a flow→spread relationship can produce.

> The portfolio framing writes itself: *"trained on a decade of vessel movements,
> then forward-tested on a live AIS feed the model had never seen."* That sentence
> is worth more than any in-sample R².

---

## 1 · What we can do now vs. what waits for live

Most signal-finding and model-training happens **now**, on history. The genuine
"wait for live" items are few and specific. This is the fidelity split from
`SIGNALS.md` §0·6·1, read as a research-readiness table:

| Signal class | Trainable on history now? | Notes |
|---|---|---|
| US supply (#6–11, `gas_loading_us`, US queue/berth) | ✅ yes — full fidelity, 2016+ | the clean case; live US data is same-fidelity out-of-sample |
| In-transit volume (#1/#2, `gas_in_transit_volume`) | ✅ yes — NOAA dep ⋈ GFW arr (§3.7) | the headline at-sea signal, fully reconstructable |
| EU arrivals / volume (#4, `gas_discharging_eu` count) | ✅ yes — GFW arcs, 2017+ | counts only; carry the `gfw→mmsi_filter` regime indicator |
| Outage / change-point (#36–#38) | ✅ yes — visible in NOAA gaps | Freeport-2022 is in the corpus as a labelled event |
| Voyage age / floating-storage proxies (#20–#21) | ✅ yes — endpoint-only | survive the terrestrial constraint |
| **EU queue/berth (#12–16)** | ❌ **no — no historical source** | GFW arcs carry no `anchorage_entry`; **build forward, on live** |
| Intra-voyage SOG (#23) | ❌ no — needs satellite AIS | out of scope regardless of corpus |

**So "wait for the live data to fill in" applies mainly to the EU queue family and
to lengthening the spread forward-test — not to the bulk of the research.**

---

## 2 · Two reasons history ≠ live (keep them separate)

"Apply a historical model to live data and see what happens" only means something
if you know *why* the two might diverge. There are two distinct causes, and
conflating them is the classic backtest error.

### 2.1 · Data-fidelity transfer (can the model even run on both?)

Decided by §1. The US signals are **same-fidelity** across `noaa` and
`mmsi_filter`, so a US model transfers cleanly — the live US series is genuine
out-of-sample from the *same data-generating process*. The EU signals step in
fidelity at the `gfw→mmsi_filter` boundary, so EU features must enter the
deployable model in a fidelity-robust form (arc-derivable only; see §3.3).

### 2.2 · Market non-stationarity (the interesting one)

Even with perfect fidelity, 2017–2025 coefficients need not predict 2026+, because
the **market itself changed**:

- the US export fleet grew ~10× over 2016–2026 — the flow→spread map is
  non-stationary by construction;
- **2022 (Ukraine) was a structural break** — the HH/TTF spread reached levels
  with no historical precedent; a model trained including 2022 learns shock
  dynamics, one trained without it cannot;
- Freeport-2022 and similar outages are idiosyncratic.

> **This is why the forward test is a *result*, not a rubber stamp.** Small
> historical-vs-live divergence ⇒ the relationship is stable and there is edge.
> Large divergence ⇒ it is regime-dependent — itself a finding that says *use the
> regime-detection models* (`MODELS.md` §10), not a static fit. Either outcome is
> publishable; neither is a failure.

---

## 3 · The train/test protocol

### 3.1 · Freeze splits by calendar date (the cardinal rule)

All CV is **walk-forward / expanding-window**, never k-fold (`MODELS.md` Part
IV.1), with **purged & embargoed** boundaries so an overlapping multi-day target
window can't leak across the split. The train/test boundary is a *date*, fixed
once, and never moved to chase a result.

### 3.2 · The NOAA-overwrites-live lookahead trap

NOAA lags only ~1–3 months, so it will eventually publish the **same dates** now
being collected live. Training on NOAA rows that fall inside the live forward-test
window **leaks the future**. Mitigation: the forward-test period is defined by
calendar date and **NOAA data is barred from crossing into it**, no matter when it
is downloaded. The live test window stays a pure hold-out.

### 3.3 · Keep EU queue features out of the transferable model

Build the deployable spread model only from features present in **both** corpora
(arc-derivable). The EU queue signals (#12–16) are a **live-only enrichment layer**
bolted on later — if they enter the base model it cannot run on history at all.

### 3.4 · Physics validates fast; the spread validates slow

Sequence the forward test by statistical power:

- **Physical nowcasts (Part II) validate in weeks.** They are event-level —
  every loading, arrival, queue duration checked against EIA / observed outcomes.
  Hundreds of events, not dozens of days. This is the "models working, live, now"
  deliverable.
- **The spread model (Part III) validates over months.** The live forward test
  yields ~1 spread row/day (`N_eff ≈ 7` over weeks) — **too little power to
  confirm or reject the spread model on a few weeks of live data.** Let it
  accumulate; report widening/narrowing confidence as it does, never a premature
  verdict.

---

## 4 · The built-in validation loop (a rare free advantage)

Because NOAA backfills the *same dates* collected live (US side, ~1–3 month lag),
the project gets a **continuous self-audit for free**: live `mmsi_filter` capture
vs NOAA exhaustive capture for identical days directly measures the live pipeline's
capture rate and any selection bias — `data/capture_rate.py` extended from a
one-shot EIA check into a rolling live-vs-ground-truth monitor. Almost no portfolio
project can check its own live feed against an independent exhaustive source.
Treat a divergence here as a *data* alarm (the live scorer is missing vessels), to
be ruled out before any *model* divergence (§2.2) is interpreted as a market
finding.

---

## 5 · Build order

1. **Assemble the historical panel.** Execute `PLAN.md` Phases 1–4 (NOAA US,
   GFW voyages+events, ALSI/ENTSO-G), plus the control set (EIA, GIE AGSI,
   degree-days) on a common daily grid; define the spread target unit-consistently
   (`MODELS.md` V.3).
2. **Validate signal logic on history.** Confirm event detection, leg geometry,
   queue/berth realism reproduce known episodes (Freeport-2022, the 2022 EU surge)
   before trusting any aggregate.
3. **Train Part II physical nowcasts on the decade**, walk-forward CV, hierarchical
   pooling across terminals (`MODELS.md` Part IV.2). These graduate from "confirm
   edge" to genuinely fitted models.
4. **Fit the Part III spread model on the ~3,000-row panel** — regularised /
   Bayesian, uncertainty-first, FWL control-partialling (V.2), EU fidelity step as
   a regime indicator.
5. **Freeze every model**, then **begin the live forward test** (§3): physics
   confirmed in weeks, spread accumulating over months.
6. **Run the capture-rate self-audit (§4) continuously** to separate data drift
   from market findings.
7. **Interpret the live divergence (§2.2)** — stability ⇒ edge; regime-dependence
   ⇒ escalate to change-point models.

---

## 6 · What each test actually proves

| Test | Corpus | Proves | Power |
|---|---|---|---|
| Walk-forward CV | historical | the model isn't over-fit *to history* | high (`N_eff ≈ 330`) |
| Physics forward test | live | the pipeline produces correct events/counts in real time | high (event-level, weeks) |
| Spread forward test | live | the flow→spread relationship survives out-of-sample | low early, grows monthly |
| Capture-rate audit | live ⋈ NOAA | the live feed isn't silently missing vessels | high (exhaustive ground truth) |

The honest summary: **the historical corpus lets us do the research now — find the
signals, fit and cross-validate the models — and the live feed is the slow,
honest, out-of-sample referee.** The US side transfers cleanly; the spread model's
live divergence is a finding rather than a failure; and the EU queue family is the
one genuine wait-for-live item. That is a stronger experimental design than either
corpus could give alone.
