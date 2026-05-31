# tanker-flow pre-signal-layer audit — 2026-05-31

Full correctness audit of the pipeline logic **before** building the signal layer
in `analysis/SIGNALS.md`. Branch `master` @ `3a3aca9`. Every data claim below is
backed by a read-only query against the live TimescaleDB (PostgreSQL 16); no
tables were mutated and no `make` targets were run.

Method: three independent subsystems (dest parsing, ingestion/scoring, enrichment/
registry) were audited in parallel against live data; the state-machine →
`port_events` → leg-integrity core was audited directly. Findings cross-checked
where they overlap (e.g. the phantom-open-leg failure was reached independently
from both the scoring angle and the leg-pairing angle, and the numbers agree).

> **READ THIS FIRST — regime context (added after initial draft).** The real-time
> position history is **not one homogeneous dataset**. There was a hard cutover in
> the ingestion scheme at **2026-05-30 09:27 UTC**, and `port_events` is computed
> across it. See "§0 · Regime break" immediately below — it re-attributes the
> cause of finding M1 and re-frames the stale-close numbers. The *logic* findings
> (M2/M3/M4, DFA, laden, determinism) are regime-independent and unaffected.

---

## §0 · Regime break — the data is two different processes

`ais_fixes` real-time history splits cleanly into two regimes (the pre-2026-04-14
`min(fix_ts)` of 2025-06-12 is `vesselfinder` reconciliation, 624 rows, not
position history):

| Regime | Span | Source label | Volume | Selection | Per-fix capture |
|---|---|---|---|---|---|
| **Old (bbox + throttle)** | 2026-04-14 → 2026-05-30 09:26 | `aisstream` | 21.93M fixes (**93% of `port_events`**) | Unbiased — *everything* in the geographic boxes | **Stochastic drops** — AISstream throttles by dropping vessels ~randomly from the return |
| **New (MMSI filter)** | 2026-05-30 09:27 → now | `aisstream-mmsi-{1,2,3}` | ~10.7k fixes (~1.3 days) | **Biased** — only ~150 tier-gated MMSIs from `priority_watchlist` | Reliable for subscribed vessels |

Clean switch, no overlap. Segmentation of `port_events` by the 09:27 cutover:
`departed` 166 old / 5 new; open legs **112 old / 5 new**; stale-closes 111 old / 3
new; total events 1386 old / 112 new.

**The two regimes have opposite bias structures** — old = unbiased selection +
random missingness; new = biased selection + reliable capture. Consequences:

- **Every count/rate/time-series signal (#1,#4,#7,#9,#13,#20…) has an artifactual
  discontinuity at 2026-05-30** that an ML model would learn as a market move.
  Do not train across the break.
- **Data volume reality vs SIGNALS.md's ML premise:** the usable real-time history
  is **~6.5 weeks of a now-defunct regime + ~1.3 days of the live one**, not the
  "1000–1800 daily rows over 3–5 years" assumed in the ML section. **Signal
  *extraction* (#1/#6/#9 from `port_events`) can proceed; the *modeling* layer
  (§B–F of SIGNALS.md) cannot be trained yet** and must never span the break. The
  old block is good for validating *logic*, not for training the spread model.
- **What this does NOT change:** all logic/config/parsing findings below (M2, M3,
  M4, DFA soundness, laden inference, wall-clock determinism, test gaps). It also
  doesn't change the *physical realism* of the logic-validation numbers (15.1-day
  transatlantic legs, ~31h loading durations are robust to random fix drops) —
  only the *distribution* of those numbers is regime-biased.

---

## TL;DR — is `port_events` trustworthy enough to build signals on?

**CONDITIONAL — yes for the export-side / port-side stack, no for the headline
in-transit signals without two fixes first.**

The big structural blocker from the 2026-05-28 review is **resolved**: approach
polygons now cover all 34 terminals (was 0), and `departed` events exist (171,
was 0). The clean transatlantic legs are *physically correct* — `usgulf→nweurope`
averages **15.1 days** (min 12.0d) over 14 legs, exactly right for a laden
Atlantic crossing. The export-throughput stack (loading queue #6 ≈ 28.5h, loading
duration #8 ≈ 31.6h) is realistic and ready. The live ingester is healthy (3
sources, ~1s lag, clean reconnects).

But two things make the **headline "laden ton-miles in transit" (#1) and "mean
laden-voyage age" (#20) untrustworthy as specified today**:

1. **Phantom open legs.** 117 of 171 `departed` events have no terminating
   `zone_entry`. A large share are not "in transit" — they are vessels whose
   arrival we never saw because `scoring.py` rotated them out of the AIS
   subscription as their tier decayed. **47 open laden US legs have zero position
   fixes after departure** (oldest 42.9 days). These are indistinguishable from
   genuine floating storage, and they bias #20 upward without bound.

2. **Naive leg pairing.** "`departed` → next `zone_entry`" mis-pairs whenever an
   intermediate arrival was missed: e.g. laden `usgulf→usgulf` legs averaging
   **31 days** (a missed European round-trip collapsed into one bogus leg), plus
   same-terminal/sub-24h re-entry noise. ~46% of *closed* legs are same-zone
   pairings that signal #1 must exclude.

Neither is a crash or a schema defect — the state machine itself is sound (DFA
clean, build passes `validate_sequence`). They are **semantic** gaps that the
signal layer (and a small scoring change) must handle before #1/#3/#17/#20 mean
what SIGNALS.md says they mean.

---

## Findings table

| # | Invariant | Verdict | Evidence | Affected signals | Severity |
|---|---|---|---|---|---|
| 0 | **Ingestion regime break** | **BUG (data)** | Hard cutover 2026-05-30 09:27; `port_events` 93% old-bbox+throttle / 7% new-MMSI; opposite bias structures (§0). Structural discontinuity in every rate/count series. | #1,#4,#7,#9,#13,#20 + all ML | **must-address** |
| 1 | State-machine soundness / DFA | **OK** | Live table: every adjacent `prev→curr` per-MMSI transition is DFA-legal (query P); build completed → `validate_sequence` passed for all walked vessels. | all | — |
| 2a | Leg pairing — transatlantic legs | **OK** | `usgulf→nweurope` 14 legs avg 15.1d (min 12.0d); `nweurope→usgulf` 15 legs avg 15.3d (query L). Physically correct. | #1,#5,#22,#32 | — |
| 2b | Leg pairing — mis-pairing tail | **BUG (signal-design)** | `usgulf→usgulf` *laden* legs avg 31d (max 38.8d) = missed Europe arrival collapsed into one leg; 10 same-terminal, 9 sub-24h legs; 46% of closed legs are same-zone (queries K/L/O). | #1,#5,#20,#21,#22,#32 | **must-fix** |
| 2c | Open legs — phantom vs in-transit | **BUG** | 117/171 departeds open; 47 open laden-US legs have **zero** post-departure fixes (oldest 42.9d). Indistinguishable from floating storage. **Cause is regime-dependent: 112/117 are old-bbox+throttle, not scoring (§0/M1).** | #1,#3,#17,#19,#20,#21 | **must-fix** |
| 3 | `laden_flag` integrity | **OK** | 100% of `departed` (171/171) and 100% of non-FSRU `moored` (183/183) are laden-labeled (queries I/U). 705/780 in-scope have `design_draught`; ranges sane (7.3–15.0m). NULLs (124) concentrated in anchorage/open-envelope events, not legs. | #1,#4,#33 | known-limitation |
| 3b | `design_draught` gaps | **RISK** | 75/780 in-scope missing `design_draught` → fall back to flow-direction inference (`laden.py:150,168`). Reliable at known-flow terminals; weaker mid-ocean. | #1,#2,#17,#33 | known-limitation |
| 3c | Import-departure laden mislabel | **RISK** | 10 `nweurope` departures labeled laden=TRUE via draught (`laden._draught_after` can pick a stale pre-discharge reading inside the +6h window, overriding the reliable flow-direction=ballast). | #1 | known-limitation |
| 4 | `cold_start` semantics | **OK** | 193 cold-start events; only on first-fix-in-polygon + FSRU synthetics (query C). Not set on mid-stream events. | #10,#39 | — |
| 5 | FSRU short-circuit | **BUG** | Lubmin II (terminal 19) has no `fsru_host_mmsi` → 0 events. ENERGOS IGLOO is a relief FSRU at Eemshaven (already hosted) → silently dropped. 35 other `is_fsru` vessels are out-of-zone (cosmetic). 10 FSRU mooreds emitted (query J). | #4,#19 | **must-fix** (Lubmin II + substitute) |
| 6 | Zone / terminal attribution | **OK** | All 34 terminals have berth+approach polygons; 29 have anchorage (query G). Correct transatlantic O-D durations confirm zone attribution. No in-scope terminal lacks a berth (query H). | #5,#9,#11,#36,#37 | — |
| 7 | Coverage / observability bias (scoring) | **RISK (forward-looking)** | `scoring.py` tier-decay (3d→tier1 `:242`, 7d→tier5 `:258`) + 10 tier-5 slots can drop a departed vessel before its arrival — but this is the **new-scheme** failure mode (≤5 legs since cutover); it is *not* the cause of the historical phantoms (§0/M1). | #1,#3,#17,#20 | fix for new scheme |
| 8 | `dest_parser` | **BUG (several)** | Plaquemines seeded `USPMS` but vessels broadcast `USPLQ`; chained-LHS dropped (`USSAB>KRPTK`→None); `DEWVN` duplicate collapses WHV1; FOR-ORDERS/suffix variants miss. Resolve rate 5.2% of in-scope (mostly correct — foreign ports). | #27,#29,#31 + tier-2 | **must-fix** (Plaquemines, chained-LHS) |
| 9a | Idempotency (fixed `now`) | **OK** | TRUNCATE+rebuild; spatial join `ORDER BY (mmsi,fix_ts)` is total (unique idx); array_agg ordering total. Same `now`+same `ais_fixes` → identical output. | reproducibility | — |
| 9b | Wall-clock determinism | **RISK** | `now` captured once (`port_events.py:143`) drives end-of-stream stale-close. Rebuilding later retroactively closes currently-open envelopes → open-leg-age signals are rebuild-time-dependent. | #17,#20 | known-limitation |
| 9c | Timestamps / TZ | **OK** | Go-format nanosecond timestamp parsed correctly (truncates to µs); all TIMESTAMPTZ; verified by ingestion stream. | weekly buckets | — |
| 9d | `departed`-skip | **RISK** | 7 `moored→zone_exit` (non-cold-start): vessel's first post-undock fix already outside approach polygon → `_step_open_ocean` emits `zone_exit` with no `departed` (query R). Undercounts loadings. | #8,#9 | known-limitation |
| 9e | Stale-close prevalence | **OK (document)** | 114/344 exits (33%) stale-closed; 82% at usgulf — but 74/114 follow a `departed` (benign lower-bound) and **111/114 are old-scheme: the rate is substantially a throttle artifact and should fall under the new scheme** (§0). 24 from `zone_entry` lost their moored. | #6,#8,#12,#14 timing | known-limitation |
| 10 | Ingestion liveness | **OK** | 3 sources flowing at ~1s lag; mean_lag ~1s, p95 ~1.3s, `max_raw_q`=0; 0 errors in last 24h; watchlist 100/100 persistent + 50/50 scan slots filled (ingestion stream). | foundational | — |

---

## Must-fix before the signal layer is trustworthy

### M1 — Phantom open legs · invariants 2c + 7
The single biggest threat to the headline signals: 117 of 171 `departed` events
have no terminating `zone_entry`, and a large share are not "in transit" but
arrivals we simply never recorded. **47 of 68 open laden-US legs have zero
position fixes after departure** (oldest 42.9d); 25/30 open US legs older than
20 days have gone dark >7d. These are **indistinguishable** from genuine floating
storage / slow-steaming, so "laden ton-miles in transit" (#1) is inflated and
"mean laden-voyage age" (#20) is biased upward without bound.

**Cause — corrected for the regime break (see §0).** The initial draft blamed
`scoring.py` tier-decay (3d→tier1 `scoring.py:242`, 7d→tier5 `:258`, 10 tier-5
slots `aisstream.py:60`) rotating vessels out of the subscription. That mechanism
is real **but it has only existed since the 2026-05-30 cutover** — and **112 of
117 open legs (96%) departed *before* the cutover**, so the new scheme cannot have
caused them. The causation is reversed for the history: those vessels went dark
under the **old bbox+throttle** scheme (random fix drops / box geography / genuine
US→Asia leakage), so they have no recent fix and are *now* classified tier-5. The
tier is a *symptom* of the missing arrival, not its cause. Scoring tier-decay is
the *forward-looking* (new-scheme) version of the same failure and accounts for at
most the 5 post-cutover open legs (which are ≤1.3d old — likely still crossing).

Mitigations:
- **Censoring (mandatory, regime-independent):** in the signal, treat open legs
  older than a per-O-D cap (US→EU ~18d, US→Asia ~32d) as *censored*, not
  *in-transit*. This is the load-bearing fix and is the *only* thing that helps the
  112 historical phantoms.
- **Open-leg pin (forward-looking only):** any MMSI with an open laden leg gets a
  guaranteed slot regardless of tier, so the *new* scheme re-acquires it on the
  European approach. Does nothing for the old-scheme history.
- **Backfill** from the weekly `vesselfinder` source to close legs the live feed
  missed.

### M2 — Leg pairing must not be naive "next zone_entry" · invariant 2b
`signal.py` (planned) cannot literally sum over "`departed` → next `zone_entry`".
Live closed legs include laden `usgulf→usgulf` pairs averaging **31 days** — a
vessel that loaded at Sabine, crossed to Europe (arrival missed), discharged, and
returned to load again, collapsed into one bogus near-zero-distance "leg". Plus
10 same-terminal and 9 sub-24h re-entries (berth shifts / approach-polygon
drift-outs). **46% of closed legs are same-zone.**

The signal must: (a) only count cross-zone legs for the US→Europe lane; (b)
discard same-terminal and sub-day legs; (c) sanity-bound leg duration per O-D pair
and route over-long same-zone legs to "missed arrival", not a real voyage.

### M3 — `dest_parser` data/logic bugs · invariant 8
Needed before destination-intent signals (#27/#29/#31) and to keep tier-2 scoring
honest:
- **Plaquemines**: `terminals.unlocode` is `USPMS` but live LNG carriers broadcast
  `USPLQ` (confirmed: AL FATH, CELSIUS GANDHINAGAR). Plaquemines is a major,
  growing US export terminal → currently invisible to dest resolution. Fix the
  seed and re-verify all 33 LOCODEs against real broadcasts.
- **Chained-LHS dropped**: `USSAB>KRPTK` (Sabine→Korea) returns None because the
  parser takes only the RHS (`dest_parser.py:131-137`). For an export signal the
  LHS *origin* terminal is the higher-value datum. Fall back to LHS when RHS is
  unresolved. 9 in-scope vessels affected now.

### M4 — FSRU host coverage · invariant 5
- **Lubmin II (terminal 19)** has no `fsru_host_mmsi` → contributes zero events to
  any import signal. Declare its host MMSI.
- **Eemshaven substitution**: ENERGOS IGLOO appears at Eemshaven but Eemshaven
  already has a declared host, so the relief vessel is silently dropped. The
  one-host-per-terminal model needs a story for FSRU swaps.
(The 35 out-of-zone `is_fsru` vessels are cosmetic — they never enter our zones —
but they do consume scoring/ingest slots.)

---

## Known limitations to document (not blockers)

- **Terrestrial-AIS mid-ocean blindness** — already in SIGNALS.md §0. The
  phantom-leg bias (M1) is a *second, distinct* bias on top of this and is **not**
  yet documented in SIGNALS.md.
- **`design_draught` missing for 75/780 in-scope** → laden via flow-direction
  fallback (reliable at known-flow terminals). `dwt` is 100% populated (#1
  ton-mile weighting is fully supported); `gas_capacity_m3` missing for 4.
- **Stale-close timestamps are lower bounds.** 33% of exits overall, 82% at
  usgulf, are synthetic. Mostly benign (post-`departed` ocean exits) but any
  signal using `zone_exit` *timing* at usgulf should treat it as a lower bound.
- **Wall-clock non-determinism (9b).** Rebuilding `port_events` at a later time
  retroactively closes open envelopes via the end-of-stream stale check. For
  reproducible backtests, pin `now` or snapshot `port_events` per as-of date.
- **`departed`-skip (9d).** ~7 loadings lose their `departed` when the first
  post-undock fix is already outside the approach polygon → slight undercount of
  #9. Consider emitting `departed` on the `MOORED → open-ocean` transition.
- **Import-departure laden mislabel (3c).** 10 NW-Europe departures labeled laden
  via a stale draught reading. Prefer flow-direction over a draught reading that
  pre-dates `event_time` for `side='post'`.
- **Thin coverage outside usgulf/nweurope.** baltic (2), emed (1), iberian (6),
  wmed (13) have almost only FSRU-placeholder mooreds — those zones' signals will
  be statistically thin for a while.
- **dest_parser minor**: `DEWVN` shared by Wilhelmshaven 1&2 collapses to one
  (WHV1 unreachable via dest); FOR-ORDERS whitespace/prefix variants and
  suffix-decorated LOCODEs (`ESCAR<D9 HRS`) miss. `is_for_orders` is computed but
  never consumed.
- **`reattribute_overlaps` is dead code** in production (`port_events.py` doesn't
  import it; the inline `_can_reattribute_envelope_to` path does the work). The
  test asserts it's a no-op. Harmless, but delete or wire it to avoid confusion.

## Load-bearing logic with no test coverage
- **Stale-envelope close** (the `now`-driven path, commit dde479c) — produces 33%
  of all `zone_exit` events live, **zero unit tests**. `test_cold_end_no_synthetic_departed`
  only covers the `now=None` (leave-open) branch.
- **`departed`-skip** (`MOORED → open-ocean`) — current behavior untested.
- **dest_parser tests use idealized inputs** — none of the live bugs (M3) would be
  caught; add the real unresolved-but-valid strings as regression cases.

---

## Live-data snapshot (baseline to diff against)

Captured 2026-05-31 ~12:47 UTC. `port_events` last rebuilt 09:33 UTC (≈3h stale
vs `ais_fixes`, which is live to 12:46).

```
ais_fixes        21,937,967 rows   2025-06-12 → 2026-05-31 12:46   (3 live sources ~1s lag)
vessel_state      3,445,587 rows   2025-06-12 → 2026-05-31 12:46
port_events           1,494 rows   2026-04-14 → 2026-05-31 09:29   176 distinct MMSIs
in-scope fleet          780 vessels (734 LNG + 46 FSRU)

event_type:  zone_entry 359 | zone_exit 344 | moored 193 | anchorage_entry 172
             departed 171 | anchorage_exit 170 | anchored 85
cold_start:  1301 false / 193 true
laden:       draught 1079 (T 530 / F 549) | flow_direction 291 (T 143 / F 148) | NULL 124

legs:        171 departed → 54 closed / 117 open
             closed O-D: usgulf→nweurope 14 (15.1d) | nweurope→usgulf 15 (15.3d)
                         nweurope→nweurope 16 | usgulf→usgulf 9 (mis-pairs, up to 38.8d)
             open >20d: usgulf 30 (25 gone-dark) | nweurope 18 (9 gone-dark)
             phantom (open laden-US, zero post-dep fix): 47

stale-close: 114/344 exits (33%) synthetic; usgulf 108/132 (82%), nweurope 6/209 (3%)
             prior state: departed 74 (benign) | zone_entry 24 | anchorage_exit 13 | moored 3

polygons:    berth 55/34 terminals | anchorage 53/29 | approach 34/34
registry:    design_draught missing 75/780 | dwt missing 0 | gas missing 4
             FSRU hosts: 10 declared, Lubmin II missing
signal probes (usgulf): loading queue #6 avg 28.5h (34 obs) | loading dur #8 avg 31.6h (100 obs)
```

Re-run `make port-events` then diff event_type counts, closed/open leg split, and
the phantom count to track whether M1/M2 mitigations are landing.
