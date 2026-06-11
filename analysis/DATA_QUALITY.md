# Data-quality & vessel-coverage issues

Investigation summary, **2026-06-10**. Triggered by repeated MarineTraffic spot-checks
finding vessels physically at our terminals that the pipeline wasn't tracking
(Energy Atlantic ✓ but K. Mugungwha, Maran Gas Vergina, Barzan, Celsius Georgetown,
Orion Hugo, Ignacy Lukasiewicz, Zoe Knutsen ✗). All figures below are live-DB
snapshots taken on 2026-06-10 and will drift; treat them as orders of magnitude.

Companion docs: [`SIGNALS.md`](SIGNALS.md) (what the signals are), [`MODELS.md`](MODELS.md)
(how they become a view). This doc is about **whether the underlying observations
are complete enough to trust those signals**.

---

## 0 · TL;DR

The pipeline runs a **closed-loop watcher**: AISstream server-side MMSI filtering
means we only ever receive positions for vessels we explicitly subscribe to. We
have **~150 subscription slots for ~781 in-scope carriers**, so at any moment we
actively hear only a small fraction of the fleet. Coverage therefore depends
entirely on *predicting* which vessels to subscribe to before they arrive — and
that prediction fails for an important class of vessels (US-export arrivals that
sail "FOR ORDERS" and go AIS-dark mid-crossing).

Missing a vessel's arrival is not cosmetic: it drops or mis-times a berth visit,
which under-counts the day's loading/discharge in `signal_daily`. The residual
miss-rate is currently **unmeasured**, which is what undermines confidence in the
signal.

Nothing here is a code *bug* — the state machine, zone geometry, and scoring all
work correctly for vessels we hear. The gaps are **coverage** (who we subscribe
to) and **completeness** (who is in the fleet list at all).

---

## 1 · The core constraint

AISstream is subscribed per-MMSI across 3 WebSocket connections (3 is the hard
per-source-IP cap — a second key/account on the same IP does not add connections):
- **100 persistent slots** — top tiers 1–3 by score (`ingestion/aisstream.py`
  `load_persistent_mmsis`).
- **50 scan-rotation slots** — overflow + tier-4 + tier-5 discovery, rotating
  least-recently-scanned (`load_scan_mmsis`).

Fleet coverage snapshot (in-scope = `is_lng_carrier OR is_fsru AND NOT excluded`):

| bucket | count | of 781 |
|---|---|---|
| heard live (AISstream fix < 2d) | 105 | 13% |
| AIS-stale (2–7d) | 73 | 9% |
| only a stale/VF snapshot (> 7d) | **603** | **77%** |
| never seen | 0 | — |

Watchlist tier distribution (`priority_watchlist`): t1=35, t2=23, t3=76, t4=72,
**t5=575**. The tier-5 pool is ~575 deep and the scan sweeps it ~10/cycle, so a
vessel that has dropped to tier 5 waits ~2 days for a subscription attempt.

---

## 2 · Why prediction fails: destination resolution

The main path that pulls an *approaching* vessel into a persistent slot is **tier-2
"declared inbound"** (`pipeline/scoring.py`): the vessel's `vessel_state.dest`
must resolve to a known terminal via `pipeline/dest_parser.py`.

Of **641** carriers that declared a destination in the last 21 days, only **6%
(41) resolve to a known terminal**; 94% do not. Breakdown of the unresolved:

- **"FOR ORDERS" and variants — ~181 declarations** (`FOR ORDERS`, `FOR ORDER`,
  `OPEN SEA FOR ORDERS`). US FOB cargoes sail before the discharge port is fixed,
  so the destination is **unpredictable by construction**. This is the single
  largest bucket and the hardest to fix.
- **Unmapped UN/LOCODEs** — most are legitimately *foreign* terminals (Ras Laffan
  `QARLF`, Bonny `NGBON`, Gladstone `AUGLT`, Pyeongtaek `KRPTK`, …) and correctly
  irrelevant. **But a few are ours**, declared under codes we haven't mapped:
  - `USNSS` / `US NSS` → Sabine Pass (we only map `USSAB`) — 7 instances.
  - `US LCH` → Lake Charles area (Cameron / Calcasieu) — seen on Celsius Georgetown.
  - `USG`, `USG FOR ORDERS` → generic US Gulf.

A vessel whose dest doesn't resolve never earns a persistent slot, drops to tier
5, and — combined with going AIS-dark on the ocean crossing — arrives at the
terminal invisible.

---

## 3 · Failure taxonomy

Every missed vessel this session falls into one of four classes:

| Class | Description | Examples (2026-06-10) | Fix domain |
|---|---|---|---|
| **A — not in the fleet list** | MMSI/IMO absent from `vessel_registry` *and* the IGU CSV; cannot subscribe to an unknown vessel | Amerjack LNG, Barzan, Celsius Georgetown (now added) | discovery / fleet source |
| **B — known but unsubscribed** | In the fleet, but tier-5 dark + unresolvable dest; crosses the ocean unsubscribed and arrives before the slow scan finds it | K. Mugungwha, Maran Gas Vergina, Orion Hugo, Ignacy Lukasiewicz | dest aliases, approach-sweep, scan priority, more slots |
| **C — subscribed but not heard** | Holds a slot (prediction worked) but went AIS-dark mid-crossing; no re-acquisition fired | Zoe Knutsen (tier-2, persistent slot, dest `USFPO` resolved, yet 10d silent) | VF approach-sweep, MMSI-integrity check |
| **D — subscribed, AIS-gap at berth, rescue starved** | In a slot, dropped AIS near the terminal; the vf_rescue class that should bridge it is budget-gated and starved | Vladimir Rusanov (`import_berth` = priority-1, starved while P0 ate the glide cap) | rescue priority / budget |

Roughly half of the concrete misses were **A** (not in the fleet) and the other
half **B/C/D** (in the fleet, coverage failed) — so a slot increase alone (which
only addresses B) would not have caught most of them.

---

## 4 · The "appears in berth, arrival events missed" pattern

This is the most signal-corrupting symptom and was investigated directly.

**Finding.** Of **258** `moored` events, **236 (91%)** have a proper arrival
sequence (`zone_entry`/`anchorage_entry`/`anchored` precede them); **22 (8.5%)**
are **cold-start** "appear-in-berth"; **0** are reacquisition-orphans.

**Mechanism.** When the *first fix we ever observe* for a vessel is already inside
a berth polygon, `pipeline/state_machine.py` cold-starts it: it emits a synthetic
`zone_entry` **and** `moored` at the **same timestamp** (the first-fix time), both
flagged `cold_start=TRUE`. There is no real arrival timeline because **we had no
position data during the approach** — we weren't subscribed (tier-5 dark) or the
vessel is an FSRU (short-circuited to one `moored` at its host).

Triggers of the 22 cold-starts: **15 via aisstream** (first caught already
berthed by persistent/scan), **7 via vesselfinder** (re-acquired by a VF
rescue/import once alongside — e.g. the Celsius Georgetown manual add at 11:32).
By zone: usgulf 9, nweurope 6, wmed 4, baltic 2, emed 1.

**Why it matters for the signal.** A cold-start `moored` is timestamped at *first
sighting*, not true mooring. If the vessel had been alongside for hours/days
before we saw it, the berth-occupancy interval starts too late, so the amortized
flow (`gas_loading_us` / `gas_discharging_eu` in `pipeline/signal.py`) mis-times
and under-counts that cargo. The real arrival/queue timeline (anchorage dwell) is
lost entirely, so the planned queue-time signals (#6/#12) cannot be computed for
these visits. The 22 cold-starts are the **downstream symptom of the same upstream
cause** as the outright misses: no coverage during approach.

---

## 5 · Signal-quality impact

- **Under-count of loadings/discharges.** Each fully-missed arrival (Class A–D) is
  a berth visit absent from `signal_daily` → that day's loading/discharge volume
  at that terminal is understated.
- **Timing bias.** Each cold-start/reacquired visit is back-dated to first
  sighting, biasing berth-occupancy start late and smearing the amortized cargo.
- **Terminal-level blind spots.** Golden Pass had **0 `port_events` in 14 days**
  (last 2026-05-09) — its lifters (new QatarEnergy/charter tonnage) aren't in
  slots or the fleet list, so the terminal is effectively dark to us.
- **Unmeasured residual.** There is currently **no metric** for what fraction of
  real activity we capture, which is the root of the confidence problem.

---

## 6 · Remediation plan

Phased by impact-per-effort. Cheap items first; the structural ceiling last.

**Phase 0 — recover current misses + deploy committed fix**
- Restart `make ingest` to deploy the FSRU watchlist demotion (frees ~13
  persistent slots wasted on stationary FSRUs).
- Manual `vf-rescue --mmsi` the known-MMSI dark vessels; add Class-A vessels by IMO
  (Celsius Georgetown done; Amerjack/Barzan pending IMOs).

**Phase 1 — cheap, high-leverage code** (targets B/C/D, no new infra)
1. **Dest aliases** — `USNSS`/`US NSS`→Sabine, `US LCH`→Cameron/Calcasieu; audit
   the unresolved list for any code hitting our 14 terminals (`pipeline/dest_parser.py`).
2. **Geographic approach-sweep in `vf_rescue`** — a low-priority class that VF-polls
   tier-4/5 carriers last seen heading into a zone box and gone silent. VF bills
   per returned row so a miss is **free** (a 404/empty body costs 0 credits);
   this re-acquires dark approachers before they berth with no extra slots. The
   single highest-leverage cheap fix for Classes B and C.
3. **Scan closing-ness** — rank the tier-5 scan by heading-toward-our-zones (as
   tier-3 already is) so approachers are swept first, not by luck.
4. **Rescue P0 promotion** — make laden `import_berth` in final approach
   budget-exempt so discharge captures aren't starved (Class D).

**Phase 2 — fleet-list completeness** (Class A)
- A `make add-vessel IMO=…` one-shot (generalize the Celsius Georgetown add); a
  maintained supplementary newbuild CSV the discovery worker reads alongside the
  IGU CSV; the IGU-2026 refresh when published.

**Phase 3 — lift the ceiling** (structural)
- A **2nd source IP** → +3 connections → ~300 slots, sequenced *after* Phase 1–2
  so the extra capacity lands on a fleet we can resolve. Covers all of tiers 1–4
  and sweeps tier-5 ~8× faster.

**Phase 4 — measure it so it can be trusted**
- **EIA capture-rate metric** (Phase 1b, already scaffolded in `data/eia.py`): our
  counted US loadings vs EIA monthly LNG-export ground truth → a hard
  "captured X% of actual exports" number, trended. Quantifies *and* enables
  bias-correction of the signal.
- A **coverage panel** surfacing the §1 buckets (live / stale / blind) over time.

**Recommended sequence:** 0 → 1.1+1.2+1.4 → 4 (capture-rate) → 2 → 3.
The approach-sweep + dest aliases + capture-rate are the trio that most directly
*reduce* misses and *quantify* the residual.

---

## 7 · Open decisions (need owner input)

1. **2nd IP** — infra cost vs the coverage ceiling it lifts.
2. **Fleet source for Class A** — maintain a supplementary newbuild CSV vs buy a
   commercial fleet list. (The IGU list misses newbuilds and some established
   tonnage, e.g. Barzan.)
3. **Glide-cap posture** — the rescue reserve is ~4,800cr to mid-2027 but P0
   demand already saturates the ~14/day glide, so P1 captures (`import_berth`,
   `eta_arrival`) are chronically starved. Raise the cap (deplete faster) or
   keep starving P1?
