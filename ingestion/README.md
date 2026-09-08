# ingestion

Subscribes to AISstream's WebSocket feed and writes raw fixes to `ais_fixes`,
voyage state to `vessel_state`, and identity to `vessel_registry`. Most of the
non-obvious design comes from AISstream's throttling — which we work around
by filtering server-side to the specific LNG carriers and FSRUs we care about
rather than pulling broad geographic data and discarding 95% of it.

## Why MMSI filtering (history)

Under the old bbox-based subscription, AISstream's per-account throttle rotated
the admitted-MMSI set randomly minute-to-minute. In a 20-min window of ~1500
fixes/min, any given LNG carrier was visible ~23% of the time, so state
transitions in `port_events` landed 2-12 minutes late. The fix was to subscribe
*only* to LNG-carrier MMSIs via AISstream's `FiltersShipMMSI` (50-MMSI cap
per subscription, 3 connections per API key = 150 slots) — keeping each
subscription's "ask" well below the throttle threshold.

## Server-side MMSI filtering (current design)

`aisstream.py` runs **three parallel WebSockets**, each subscribed to a
disjoint chunk of up to 50 MMSIs via `FiltersShipMMSI`:

- **Chunks 0 + 1 (100 slots)** — the persistent block. Filled in order by
  pinned vessels, then the tier-1-3 band by `(tier ASC, score DESC)`. The band
  is routinely *smaller* than 100 (there are rarely 100 near-terminal vessels),
  so any remainder is **backfilled** with the highest-value scan candidates —
  tier 4 before tier 5, and within a tier the recently-transmitting ones first,
  so a continuous slot goes to a vessel we can actually hear rather than a dark
  mid-ocean hull. Backfilled MMSIs are written back as `slot_kind='persistent'`
  so the scan loader drops them next cycle and nothing is double-subscribed.
- **Chunk 2 (50 slots)** — the scan rotation, drawn from four priority-ordered
  pools with roll-over so the chunk is always full when candidates exist:

  | Pool | Slots | Ordering |
  |---|---|---|
  | Persistent-band overflow (tier ≤ 3 that missed a slot) | `SCAN_OVERFLOW_SLOTS` = 15 | `score DESC` — closest/most-closing first |
  | Tier 4 | `SCAN_TIER4_SLOTS` = 22 | least-recently-scanned |
  | Tier 5 (discovery quota) | `SCAN_TIER5_SLOTS` = 10 | recent parsed ETA first, then least-recently-scanned |
  | FSRU host-watch | `SCAN_FSRU_SLOTS` = 3 | least-recently-scanned |

  The **overflow** pool exists because tier-3 vessels crowded out of the
  persistent block used to be excluded from a tier ≥ 4-only scan and went fully
  dark. It is ordered by score rather than pure rotation because those vessels
  carry a real recent position, so their score already encodes closing-ness
  (proximity + heading). The **tier-5 carve-out** prevents a starvation loop: a
  tier-5 vessel that is never subscribed can never accrue a fix to promote out
  of tier 5. The **FSRU quota** is deliberately tiny — a deployed FSRU sits
  moored at its host for months and its own fixes never drive the signal, so it
  only needs an occasional relocation check.

Each scan pick advances `last_scan_window_at = now()` in the same transaction,
so the next reconnect — planned 1h *or* the 5-min silence watchdog — picks a
different batch. `load_persistent_mmsis` / `load_scan_mmsis` also write back
`in_slot` / `slot_kind` so the viz Health tab can render who is subscribed.

Source labels in `ais_fixes` / `vessel_state` / `ingestion_stats_minute` /
`ingestion_events`: `aisstream-mmsi-1` / `aisstream-mmsi-2` / `aisstream-mmsi-3`.
Health queries aggregate across them with `source LIKE 'aisstream%'`.

### Multi-worker sharding

`WORKER_COUNT` / `WORKER_ID` (in `config.py`) shard the fleet across egress IPs,
since the 3-connection cap is **per source IP**, not per API key. At the default
`WORKER_COUNT=1` every partition clause collapses to `TRUE` and the code behaves
exactly as single-worker. A second egress (Oracle VM over Tailscale) runs
`WORKER_COUNT=2, WORKER_ID=1` and holds a disjoint hashed half of the fleet, for
300 slots total. The partition hashes the MMSI rather than taking it modulo,
because LNG-carrier MMSIs cluster heavily by flag state.

## Volume (what to expect)

The theoretical ceiling (1 fix/vessel/min × 150 vessels) is not what you see,
because:

- Most tier-1 vessels are *anchored or moored* (sog < 1 kn). AIS class A
  broadcasts position every 3-10 minutes when stationary, not every 10 seconds
  like a vessel underway.
- AISstream covers terrestrial AIS receivers only. A vessel mid-ocean (often a
  5-15 day transit gap) contributes nothing during the crossing, even while
  subscribed.
- Many subscribed vessels are out of any coastal AIS coverage during their scan
  or persistent window.

**Observed steady-state: roughly 200-1000 fixes/hour across all three
connections** (≈ 4-20 fixes/min), highly variable with time of day and fleet
position. Volume is *not* the right health metric — what matters is whether the
in-zone vessels deliver sub-minute state transitions to `port_events`. The Health
tab's per-source liveness and scan-rotation panels are the useful indicators.

### Discovered AISstream constraints (empirical)

- **Concurrent-connection cap = 3, per source IP.** The 4th simultaneous
  WebSocket from the same IP returns HTTP 429 at the handshake. A second API key
  or account does not help; a second egress IP does.
- **Account-level throttle scales with subscription size**, not connection count.
  Under the old bbox design, two connections covering 7 bboxes between them
  decayed identically to one covering all 7. MMSI filtering keeps each
  subscription's "ask" well below any threshold, so the throttle never engages.
- **`FiltersShipMMSI` is exclusive**: a subscription with the filter set returns
  zero off-target messages (verified empirically; upstream issues #108 / #197
  reported the filter broken in the past, but it works on our key today).
- The MMSI filter is a *whitelist*, not a hint — subscribed MMSIs out of range of
  any terrestrial receiver simply do not report.
- Subscribe-OK-but-silent outages happen (see the June 2026 episode): the socket
  connects and the subscription is acknowledged, but no messages arrive. Diagnose
  with a standalone global-bbox probe from a quiet IP before suspecting our code.

## Watchlist selection (picking 150 of ~830 vessels)

`vessel_registry` holds the full global LNG/FSRU fleet, bulk-imported from the
IGU World LNG Report (`db/seed/lng_fleet_igu_2025.csv` via
`scripts/import_igu_fleet.py`). The 150-slot cap means we cannot subscribe to all
of them, so slots are allocated by a tier scoring layer that runs every hour
inside the ingester (`pipeline/scoring.py`, also runnable via `make scoring`):

| Tier | Rule | Typical count |
|---|---|---|
| 1 | Fix inside any `terminal_zones` polygon in last 3d (plausibly *currently* in zone) | 30-50 |
| 2 | A parsed ETA within `ETA_IMMINENT_HOURS` ahead (or `ETA_PAST_GRACE_HOURS` just past), **or** `vessel_state.dest` parsing to a known terminal with `state_ts` < 14d | 15-30 |
| 3 | Fix inside any `config.ZONES` rectangle in last 14d (not 1/2), ordered within-tier by closing-ness | 50-80 |
| 4 | Any fix in last 7d (not 1-3) | 400-500 |
| 5 | Fix in 7-90d, never seen, **or any FSRU** | 150-250 |

Tier 1 was originally a 14-day window, tightened to 3 days because the longer one
admitted vessels that had visited a week earlier and were since mid-Atlantic,
wasting persistent slots on ghost MMSIs. Tier 2 is deliberately **not** gated on a
resolved destination: a "FOR ORDERS" carrier with an imminent ETA still promotes,
which is what rescues long-voyage vessels whose declaration is stale or absent but
whose arrival is imminent. **FSRUs short-circuit the whole ladder** and are forced
to tier 5 — a deployed FSRU would otherwise score tier 1 forever and hold a
persistent slot for no signal benefit.

`is_pinned` force-holds a persistent slot for two cases, both bounded by recency
and a cap well below the 100 slots:

- an **open leg in its expected approach window** — laden → import arrival *and*
  ballast → export-terminal loading (the ballast return was the dominant
  appear-in-berth miss), ranked by closeness to expected arrival;
- a vessel **currently open in a port visit** (last event is not a departure),
  which protects long berth queues from decaying out of coverage.

**Promotion:** a scan vessel that delivers an in-zone fix (or a parseable inbound
`dest`) is re-tiered on the next scoring run and lands in the persistent block on
the very next reconnect. Promotions into the persistent band are logged to
`tier_promotions` (`via='scoring'` or `via='inline'`).

The 1h reconnect cycle (`RECONNECT_INTERVAL_SECONDS = 3600`):

1. `scoring_loop` recomputes `priority_watchlist`
2. each `connection_loop` closes and reopens its WebSocket
3. persistent chunks get the freshest top-100; the scan chunk swaps in the next 50

The 5-min `SILENCE_THRESHOLD_SECONDS` watchdog also reconnects mid-cycle when a
connection goes quiet. For the scan connection this happens often (most of the
tier-4/5 pool is offshore or laid up) and is *expected* — it accelerates rotation
rather than hurting signal. Health views distinguish "silent" (socket alive, no
fixes) from "dead" (no events for 10+ min) so the scan connection is not
mis-flagged.

### Discovery (new LNG carriers)

The old bbox subscription doubled as passive discovery. Under MMSI filtering,
unknown MMSIs never flow through us — that path is dead. Newbuilds arrive two
ways:

- **`make discover`** (daily, budgeted) sweeps IGU orderbook hulls with
  `delivery_year ≤ current year` that are not yet in the registry and resolves the
  ones VesselFinder can now map to a live MMSI. VF bills per *returned* record, so
  an undelivered hull costs 0 credits and a full sweep of unresolved hulls is
  free; only a genuine catch costs the 3-credit master + AIS lookup. The budget is
  a brake, not a throttle. `make discover-dry` previews without spending.
- **`make refresh-fleet`** (annual) re-parses the latest IGU PDF and imports newly
  listed IMOs — this is what widens the candidate pool, turning "Hull NNN"
  orderbook rows into named, resolvable vessels.

A one-off newbuild can also be added by hand: insert the MMSI + IMO into
`vessel_registry`, run `make enrich`, confirm `is_lng_carrier` or `is_fsru` is now
TRUE, and wait up to 1h for the next planned reconnect.

## Rescue: the live-position backstop

`vf_rescue.py` runs inside the ingester every 30 min (and via `make vf-rescue`).
The signal is built from leg-defining port *events*, so when AISstream drops a
vessel at or approaching a terminal, that event is at risk. The worker picks
coastal vessels gone AIS-silent inside an actionable band, buys their current
position from VesselFinder's `/vessels` feed (terrestrial, 1 credit), sanity-checks
it against freshness and teleport gates, and injects it as a normal `ais_fixes`
row — the existing pipeline then re-acquires the vessel for free.

Credits are a fixed reserve that **expires unused**, so the policy is to deplete
it to ~zero *exactly at expiry*, not to minimise it. The daily cap is derived each
run from the latest `vf_account_status` snapshot, and the budget is split by
priority: leg-defining classes are exempt from the glide cap (bounded only by a
disaster brake), while lower-value classes spend only the surplus above the glide
line, so overspend on the important ones automatically starves the rest until the
line recovers. Candidates the budget cannot serve are logged as
`result='skipped_budget'` so unmet demand stays measurable. `vf_rescue_log` is both
the audit trail and the restart-safe credit ledger and per-vessel cooldown.

`make vf-rescue-dry` previews candidates and cost without spending; `make vf-status`
snapshots the balance for free.

## What this means for the signal

For subscribed vessels actively broadcasting inside terrestrial coverage,
visibility approaches ~100% — state-transition timestamps land within seconds of
the actual transition, not the 2-12 minutes the throttled bbox design produced.
The state machine's back-dating logic still exists for the rare cases where
AISstream momentarily does not deliver a vessel.

**The right health metric is *coverage* of vessels currently in zone**, not raw fix
volume. Practical checks:

- Run `make port-events` and verify recent `zone_entry` / `moored` / `departed`
  events look complete against reality at a known port.
- The Health tab's connection panel should show all three sources live, or two
  active and the scan connection silent between windows. Either is healthy.
- Per-source fix rate in `ingestion_stats_minute` is highly variable: persistent
  connections typically deliver 50-300 fixes/hour each, scan can be near zero
  between productive windows. Trends over hours beat minute-by-minute spot checks.

**Failure modes worth flagging:**

- One of chunks 0/1 silent for >10 min while still emitting lifecycle events →
  AISstream may have dropped the subscription; the next planned or watchdog
  reconnect should recover it.
- All three sources silent for >10 min → check the process is alive and the API
  key still works, then run a standalone global-bbox probe before assuming it is
  our bug.
- Tier 1 dropping to single digits → either a real lull at our terminals, or the
  scoring query is broken.

**Chunk 2 (scan) reports differently** from chunks 0+1: its 50 vessels swap on
every reconnect, planned *and* watchdog. Its distinct-MMSI count over a multi-hour
window is far higher than its per-minute fix rate suggests, and the Health tab's
scan-rotation panel exposes the in-flight window age and time to next rotation.
