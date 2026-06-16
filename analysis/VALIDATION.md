# Validation sweep — are the 34 signals model-ready?

The gate between signal *extraction* (done) and *modelling* (`MODELS.md`). A single
harness — proposed `analysis/validate_signals.py`, run by `make validate-signals` —
runs the tiers below over `signal_daily` (+ cross-refs to `port_events`, `eia_series`,
`signal_daily_live_vintage`) and emits a per-signal pass/fail report.

**Gate rule:** Tiers 0–5 are **blocking** — no spread fit until green. Tier 6 is
**confirmatory** (does each signal carry the economic meaning we built it for). Tier 3
capture-rate and Tier 4 vintage are **time-gated** (firm once EIA publishes June 2026 /
the live vintage accrues) — run-when-ready, tracked, not blockers for the physical
nowcasts.

Each check is keyed by `signal_key` so the report reads "pass / warn / fail per
signal", not one global verdict.

---

## Tier 0 · Structural integrity (blocking, cheap SQL)
- All **34 expected keys present**; no unexpected keys.
- Each key present in **both bases** (`physical`, `knowable`).
- Each key present in its **expected regimes** (US-deep keys carry `noaa`; EU-live keys
  carry only `bbox`/`mmsi_filter`; fleet/composite keys carry `all`). A regime where it
  shouldn't exist is a failure.
- `value` is **non-NULL and finite** (no NaN/Inf); `n_legs ≥ 0` where present.
- The `UNIQUE(signal_key, bucket_date, zone_scope, regime, basis)` invariant holds.

## Tier 1 · Coverage & continuity (blocking)
- **Span per (key, regime)** matches the design: US-side keys 2016→now; EU-queue +
  intent keys **live-only** (assert **no pre-2026 rows** — this is the regression test
  for the cross-ocean queue bug we just fixed).
- **Holes:** in the mature window (2022–2025) the continuous stocks
  (`gas_in_transit_volume`, `gas_loading_us`, `gas_discharging_eu`) have longest-gap
  ≤ 1 day. Flag any unexpected multi-day gap; expected sparsity (early years, EU)
  is whitelisted, not silently passed.
- **`'all'` ≥ component regimes** in row count (the synthetic row is a superset).

## Tier 2 · Range / plausibility (blocking — physical bounds)
| check | bound |
|---|---|
| `voyage_speed_kn` | 3 ≤ v ≤ 25 kn |
| `load_queue_h` / `discharge_queue_h` | 0 < h ≤ 14 d (the open-queue ceiling) |
| `*_berth_turn_h` | 0 < h < ~30 d |
| `laden_voyage_age_d`, `days_since_*` | ≥ 0 |
| fractions (`slow_steam_frac`, `queued_rate`, `meaningful_queue_rate`, `cold_start_rate`, `declared_eu_share`) | ∈ [0, 1] |
| `open_fraction`, `estimated_fraction` | ∈ [0, 1] |
| gas-volume stocks | ≥ 0, and `gas_in_transit_volume` total < global fleet capacity |
| `value_dispersion` | ≥ 0 |

Report the worst N outliers per check, not just a count — an out-of-range value is a
pointer to a construction bug.

## Tier 3 · Cross-source / ground-truth consistency (blocking where data exists)
- **Capture rate vs EIA** (`data/capture_rate.py`): NOAA `us_loadings_count` annual
  cargoes vs EIA implied cargoes — recent years within tolerance (~±15%). *Time-gated*
  on EIA June-2026 firming.
- **NOAA/GFW residual:** `gas_loading_us` `'all'` ÷ `noaa` by year — must sit at the
  documented ~1.15–1.25× recent (not regrown toward 2× — the reconcile regression test).
- **Live-vs-historical level continuity:** per signal, `mmsi_filter` mean vs
  `noaa`/`gfw` mean — flag > 2× divergence **except** `gas_in_transit_volume` (the known
  open-fraction phantom, which Tier 5 explains via `open_fraction≈0.64`).

## Tier 4 · Leakage / basis integrity (blocking)
- **Closed-event signals identical across bases:** for `*_berth_turn_h`,
  `voyage_speed_kn`, `slow_steam_frac`, `voyage_time_anomaly_d`, `round_trip_d`,
  `us_loadings_count*`, `od_flow_count` — assert `knowable == physical` per cell.
- **Stocks differ in the right direction:** `knowable` counts overdue legs over their
  pre-recognition window; verify `knowable` ≥ `physical` where the design says so, and
  that no `knowable` value uses a future endpoint.
- **Vintage self-validation:** `knowable[d]` recomputed == `signal_daily_live_vintage`
  value for post-cutover `d`, within tolerance. *Time-gated* on the vintage accruing a
  few weeks. A mismatch = the point-in-time logic leaks → hard fail.
- **Composite z is causal:** `_expanding_z` leaves the first ≥2 days undefined and uses
  only past — structural assertion (no full-panel std anywhere in the composite path).

## Tier 5 · Confidence-column correctness (blocking)
- `value_dispersion` populated **only** on the distributional signals; NULL elsewhere.
- `open_fraction` populated on all stocks/flows; in [0,1]; **spot-recompute** a sample
  (open-item value ÷ total) and match.
- `estimated_fraction` populated **only** on the queue-time signals.
- `open_fraction` sanity: `gas_in_transit_volume` ≈ 0 for `noaa`/`gfw` (all resolved),
  high for `mmsi_filter` — the documented censoring fingerprint.

## Tier 6 · Economic-sign / event validation (confirmatory — "does it mean anything")
- **Known episode — Freeport outage (2022-06):** `days_since_departed` at Freeport
  spikes; `us_loadings_count` there drops to ~0 for the outage span; recovery visible.
  The single best end-to-end "the signal sees reality" test.
- **Seasonality:** `gas_in_transit_volume` / arrivals show the winter-demand pull;
  `slow_steam_frac` rises in contango episodes.
- **Sign correlations (FWL-lite):** `us_loadings_count` ↔ EIA exports positive;
  `load_queue_h` ↔ loadings; `spread_thrust` components co-move as designed. Wrong-sign
  correlations are a design red flag.
- **Capture improves over the decade:** NOAA capture early-years < recent — matches the
  early-sparse `days_since` and the EIA crossover.

## Tier 7 · Regime-seam artifact check (confirmatory)
- **Within a single regime, no artifactual level step** — a `noaa` series should be
  smooth across 2016→2025 with no jump at the (unrelated) `bbox`/`gfw` calendar
  boundaries. The only legitimate step is the **EU `gfw→mmsi_filter`** seam, which must
  appear (it's the fidelity change models condition on).

---

## Output & cadence
- **Report:** one row per signal × tier → ✔ / ⚠ (advisory/whitelisted) / ✗ (blocking),
  with the offending values inlined. A non-empty ✗ column blocks modelling.
- **Pure where possible:** Tiers 0–2, 4–5, 7 are deterministic over the current panel —
  runnable now, in CI on every `make signals`. Tier 3 (EIA) and Tier 4 (vintage) are
  time-gated and tracked until their data firms.
- **Regression tests:** Tier 1 (EU live-only) and Tier 3 (NOAA/GFW residual) double as
  guards against re-introducing the two bugs already fixed (cross-ocean queue pairing,
  terminal-attribution double-count).

The sweep is green ⇒ the substrate is model-ready and `MODELS.md` build-order step 2
(target + controls) begins.
```
