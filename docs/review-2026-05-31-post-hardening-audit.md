# tanker-flow post-hardening re-audit — 2026-05-31

Re-run of the pre-signal-layer audit (`docs/review-2026-05-31-pre-signal-audit.md`)
after the `pre-signal-hardening` branch landed (9 commits on `master`). Same
methodology: read-only queries against the live TimescaleDB + the pure-logic
modules, with two **independent** verification streams — one reviewing the code
changes for correctness/regressions (20k/50k randomized property tests, live
adjacency scans), one re-checking the live health of the unchanged subsystems.
No tables were mutated except the idempotent `port_events` rebuild.

> One subagent claim was wrong and is corrected here: the live-health stream
> reported `design_draught` NULLs "improved to 0". A direct check shows the count
> is **still 75/780** — those rows have `design_draught = 0` (the agent counted
> `IS NULL` only). Finding 3b therefore **stands**.

---

## TL;DR — re-verdict

**The port_events foundation is now trustworthy enough to build the signal
*extraction* layer.** Every must-fix from the first audit is resolved or
correctly quarantined, with no regressions:

- `departed` events **171 → 178** (the skip is fixed) and `moored→zone_exit`
  skips **7 → 0**; the DFA holds across 20k randomized sequences.
- The 10 NW-Europe "laden departure" mislabels are **gone** (all outbound legs
  now decided by flow_direction).
- The naive `departed→next zone_entry` pairing that polluted the first audit is
  replaced by a tested **legs module** that classifies and **censors**: 29
  closed / 31 same-zone / 90 open-in-transit / **28 open-censored** (phantoms),
  so the headline signal sums over a clean base (14 closed laden transatlantic
  legs at ~4,250 nm + 58 laden in-transit, with phantoms quarantined).
- Rebuilds are now **deterministic** (`--as-of`; identical digest verified), the
  ingestion **regime** is tagged on every row, and an **open-leg pin** prevents
  new-scheme phantoms going forward.

What is **not** fixed — and is structural, not a code defect — gates only the
*modelling* layer: the 2026-05-30 regime break, the ~6.5-week usable history, the
75/780 missing `design_draught`, and the ~81% US-Gulf stale-close (a coverage
artifact). See §0 of the first audit and the conclusion below.

The re-audit itself surfaced **two low-severity latent risks** (dest leading-token
false-positive surface; pinned/scan double-pick) — both already **fixed** in the
final commit, neither ever active on live data.

---

## §0 · Regime context (unchanged)

The hard 2026-05-30 09:27 UTC ingestion cutover still splits the history into two
opposite-biased regimes (old bbox+throttle = 93% of port_events; new MMSI-filter).
This is now **explicit** in the data via the generated `port_events.regime` column
(verified: 0 disagreements vs the cutover, 0 NULLs) and documented in
`analysis/SIGNALS.md` §0.5. The rule is unchanged: extract signals now, but never
train a model across the seam.

---

## 10-invariant re-assessment (audit-1 → audit-2)

| # | Invariant | Audit-1 | Audit-2 | Evidence |
|---|---|---|---|---|
| 1 | State-machine / DFA | OK | **OK** | +departed-recovery verified: 20k randomized walks → 0 DFA failures / 0 double-departed; live adjacency scan → 0 illegal transitions |
| 2a | Transatlantic legs | OK | **OK** | legs module: 14 closed laden `usgulf↔nweurope` @ ~15d, 4,216–4,354 nm |
| 2b | Leg mis-pairing tail | **BUG** | **RESOLVED** | legs module splits the naive 60-closed into 29 cross-zone closed + 31 `same_zone` (the old `usgulf→usgulf` 31-day mis-pairs now correctly `same_zone`, excluded) |
| 2c | Phantom open legs | **BUG** | **MITIGATED** | 28 `open_censored` quarantine phantoms (not counted as in-transit); open-leg pin prevents new-scheme ones. Phantoms still *exist* in raw data (unrecoverable old-scheme) but are no longer summed |
| 3 | laden_flag integrity | OK | **OK** | 100% of `departed` laden-labeled; 124 NULLs confined to anchorage/FSRU (unchanged) |
| 3b | design_draught gaps | RISK | **UNCHANGED** | **75/780** still `design_draught ≤ 0` → flow-direction fallback (corrected from a subagent's erroneous "0") |
| 3c | Import-departure laden mislabel | RISK | **RESOLVED** | 0 NW-Europe laden departures (was 10); 100% of `departed` now `laden_source='flow_direction'` |
| 4 | cold_start | OK | **OK** | 193, unchanged; only first-fix-in-polygon + FSRU synthetics |
| 5 | FSRU short-circuit | **BUG** | **RESOLVED** | Lubmin (t19) confirmed `in_signal_scope=FALSE` (decommissioned); Mukran (t40, host 257356000) is the in-scope replacement |
| 6 | Zone/terminal attribution | OK | **OK** | clean O-D; all 34 in-scope terminals have berths |
| 7 | Coverage bias (scoring) | RISK | **ADDRESSED (fwd)** | open-leg pin (`is_pinned`, 25 bounded); dormant today (all pins tier 1-3, MMSI scheme too young), activates as new-scheme legs decay |
| 8 | dest_parser | **BUG** | **RESOLVED** | Plaquemines `USPLQ` (live 213, was unresolved); resolve rate ↑; FP surface anchored |
| 9a | Idempotency (fixed now) | OK | **OK** | deterministic given inputs |
| 9b | Wall-clock determinism | RISK | **RESOLVED** | `--as-of` pins `now`; identical content digest across two rebuilds |
| 9c | Timestamps / TZ | OK | **OK** | regime boundary semantics identical in Python (`regime_of`) and SQL generated column |
| 9d | departed-skip | RISK | **RESOLVED** | `MOORED→open-ocean` now emits `departed`; non-cold `moored→zone_exit` 7 → 0 |
| 9e | Stale-close prevalence | OK(doc) | **UNCHANGED** | usgulf 81% / nweurope 3% — a coverage artifact (not targeted); zone_exit timing is a lower bound, documented |
| 10 | Ingestion liveness | OK | **OK** | 3 sources ~1s lag, 0 errors/24h, reconnects all planned |
| — | **NEW (re-audit)** | — | **FIXED** | 2 low-sev latent risks (dest leading-token FP; scan/pin double-pick) found by independent review and fixed in the final commit |

---

## Audit-1 → Audit-2 comparison

| Metric | Audit-1 (pre) | Audit-2 (post) | Change |
|---|---|---|---|
| `departed` events | 171 | **178** | +7 — skip recovered |
| non-cold `moored→zone_exit` (departed-skip) | 7 | **0** | fixed |
| NW-Europe laden-departure mislabels | 10 | **0** | fixed (flip) |
| `laden_source` = flow_direction / draught | 291 / 1079 | **515 / 867** | outbound flip |
| dest tier-2 vessels (declared inbound) | 22 | **30** | +8 (Plaquemines + robustness) |
| dest resolve rate | low | ~40% of non-null dest (by obs) | improved |
| Regime tagging | none | **generated column** (1483 bbox / 23 mmsi) | new |
| Rebuild determinism | wall-clock dependent | **`--as-of`, identical digest** | fixed |
| Open-leg handling | naive (54 closed / 117 open, ~46% same-zone noise) | **classified**: 29 closed / 31 same_zone / 90 in_transit / 28 censored | new |
| Open-leg pin | none | **is_pinned** (25, bounded) | new |
| usgulf stale-close | 82% | 81% | unchanged (coverage artifact) |
| design_draught gaps (in-scope) | 75/780 | 75/780 | unchanged |
| Load-bearing tests | stale-close & departed-skip untested | **64 pass**, new tests non-vacuous | coverage added |
| Live ingestion | healthy | healthy | unchanged |

Note: a side-effect worth recording — the laden flip makes outbound laden purely
flow-direction-determined, so all 97 usgulf departures are now `laden=TRUE` (audit-1
had 6 draught-ballast). This is the symmetric cost of the fix: a genuine
off-pattern departure (ballast-from-export, or a reload/re-export from an import
terminal) is no longer distinguished. Rare; a deliberate robustness trade.

---

## Live-data snapshot (post-hardening baseline)

Captured 2026-05-31; `port_events` rebuilt (deterministic) — 1,506 rows, 177 MMSIs,
2026-04-14 → 2026-05-31.

```
event_type:  zone_entry 360 | zone_exit 346 | moored 193 | departed 178
             anchorage_entry 173 | anchorage_exit 170 | anchored 86
regime:      bbox 1483 / mmsi_filter 23   (departed 173 / 5)
laden:       draught 867 | flow_direction 515 | NULL 124
departed laden: nweurope 78 ballast | usgulf 97 laden | (all flow_direction)
stale-close: usgulf 108/133 (81%) | nweurope 6/209 (3%)

legs (compute_legs):  178 total
  closed 29  (usgulf→nweurope 14 @15.1d, nweurope→usgulf 15 @15.4d)
  same_zone 31 | open_in_transit 90 | open_censored 28
  signal #1 base: 14 closed laden cross-zone (4216–4354 nm) + 58 laden in-transit
                  28 phantoms censored

registry:    in-scope 780 | design_draught≤0 75 | dwt 0 | gas 4 | FSRU hosts 10/11 (Lubmin out of scope)
watchlist:   tiers 1:32 2:30 3:74 4:417 5:227 | is_pinned 25 | 100 persistent + 50 scan filled
ingestion:   3 sources ~1s lag | 0 errors/24h
tests:       64 pass | ruff clean
```

---

## Standing limitations (carry into the signal layer — not blockers for extraction)

1. **Regime break + thin history** — ~6.5 wk of a now-defunct regime + the live
   tail. Extraction is safe; **modelling cannot train across 2026-05-30** and has
   far too little data for SIGNALS.md §A–F yet. (Structural; unchanged.)
2. **design_draught missing for 75/780** — those vessels' laden flag comes from
   flow_direction (reliable at known-flow terminals). Optionally backfill.
3. **usgulf stale-close ~81%** — vessels departing into mid-ocean go dark; their
   `zone_exit` timestamp is a lower bound. Largely an old-regime throttle artifact;
   should ease under the new scheme. Signals keyed on departure use `departed`
   (well-captured), not `zone_exit`.
4. **Outbound laden = flow-direction** — see the comparison note; rare off-pattern
   departures mislabeled.
5. **Open-leg censor is a single 30-day cap** — quarantines 28 of the ~47 phantoms;
   the legs module deliberately delegates a tighter per-O-D window (US→EU ~18d) to
   the signal layer.
6. **Deferred by plan** — market aggregation, FSRU multi-host/Eemshaven
   substitution, dest chained-LHS/DEWVN/`is_for_orders`, old-regime backfill.

---

## Conclusion — state of the project & signal-readiness

**The pipeline is in a reliable, safe state and ready for signal *extraction*.**

The state machine is sound (DFA verified under randomized stress and on live data),
every leg-affecting correctness bug from the first audit is fixed, and the data the
signals consume is now either correct or explicitly classified/censored/tagged so
the signal layer can select a clean base. Concretely, the headline lane is clean:
14 closed laden `usgulf→nweurope` legs at the right distance/duration, 58 laden
legs genuinely in transit, and 28 phantoms quarantined rather than silently
inflating #1/#20. Rebuilds are reproducible, the regime seam is explicit, and the
live ingester is healthy. Independent review found no correctness regressions; the
two minor risks it raised are fixed.

**What this unblocks now:** building `pipeline/signal.py` to aggregate the
`legs`/`port_events` foundation into the extraction-side signals — laden ton-miles
in transit (#1), loadings/arrivals per week (#9/#4), queue and berth-turn times
(#6/#8/#12/#14), the O-D matrix (#5) — segmented by regime, censoring open legs,
excluding same-zone. These are well-supported by the current data.

**What remains gated (and not by code):** the *modelling* layer (SIGNALS.md §A–F).
It needs a consistent, sufficiently long single-regime training corpus, which only
begins accruing now under the MMSI scheme. Train on new-regime data as it
accumulates; use the old-regime block for logic validation only.

Recommended next step: implement the extraction signals on top of `pipeline/legs.py`
(regime-segmented, censored, same-zone-excluded), and start accumulating the
new-regime corpus — do not begin model training until the post-cutover history is
long enough to be meaningful.
