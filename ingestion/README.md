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
disjoint chunk of up to 50 MMSIs via `FiltersShipMMSI`. Slot allocation:

- **Chunks 0 + 1 (100 slots)** — persistent block. Highest-tier vessels from
  `priority_watchlist` (tiers 1-3: in or approaching our zones).
- **Chunk 2 (50 slots)** — scan rotation, split between a **40-slot tier-4
  quota** and a **10-slot tier-5 discovery quota** (each ordered by
  `last_scan_window_at ASC NULLS FIRST`, with roll-over if either pool is
  short). Each scan window writes back `last_scan_window_at = now()` so the
  next reconnect (planned 1h *or* the 5-min silence watchdog) picks the next
  stalest batch. The quota split exists because under a single
  `(tier ASC, ...)` ordering tier 4 (hundreds of candidates) consumed every
  slot and tier-5 vessels never got subscribed — they could then never accrue
  a fix to promote out of tier 5 (starvation loop). With 10 reserved slots,
  every tier-5 vessel cycles within ~10-22h.

Source labels in `ais_fixes` / `vessel_state` / `ingestion_stats_minute` /
`ingestion_events`: `aisstream-mmsi-1` / `aisstream-mmsi-2` / `aisstream-mmsi-3`.
The TUI aggregates across them with `source LIKE 'aisstream%'`.

## Volume (what to expect)

The README's earlier "~150 fixes/min" figure was the theoretical ceiling
(1 fix/vessel/min × 150 vessels). Reality is significantly lower because:

- Most tier-1 vessels are *anchored or moored* (sog < 1 kn). AIS class A
  broadcasts position only every 3-10 minutes when stationary, not every
  10 seconds like underway vessels.
- AISstream covers terrestrial AIS receivers only. Vessels mid-ocean
  (often a 5-15 day transit gap) contribute nothing to ais_fixes during
  the crossing, even though they're in our subscription.
- Many subscribed vessels are out of any coastal AIS coverage during their
  scan or persistent window.

**Observed steady-state: roughly 200-1000 fixes/hour total across all three
connections** (≈ 4-20 fixes/min), highly variable with time of day and fleet
position. Volume is *not* the right health metric — what matters is whether
the in-zone vessels we care about deliver sub-minute state transitions to
`port_events`. The TUI's per-source liveness pane and scan rotation panel
are the useful indicators.

### Discovered AISstream constraints (empirical)

- **Concurrent-connection cap = 3 per API key.** The 4th simultaneous
  WebSocket from the same key returns HTTP 429 at the handshake.
- **Account-level throttle scales with subscription size**, not connection
  count. MMSI filtering keeps each subscription small enough that the
  throttle simply doesn't engage on our account.
- **`FiltersShipMMSI` is exclusive**: zero off-target messages. Verified
  empirically.
- The MMSI filter is a *whitelist*, not a hint — subscribed MMSIs that
  aren't in range of any terrestrial AIS receiver simply don't report.

### Discovered constraints (empirical, from investigation)

- **Concurrent-connection cap = 3 per API key.** The 4th simultaneous WebSocket
  from the same key returns HTTP 429 at the handshake.
- **Account-level throttle scales with subscription size**, not connection
  count. Under the old bbox design, two connections covering 7 bboxes between
  them decayed identically to one covering all 7. MMSI filtering keeps each
  subscription's "ask" well below any threshold, so the throttle simply doesn't
  engage.
- **`FiltersShipMMSI` is exclusive**: a subscription with the filter set returns
  zero off-target messages (verified empirically; GitHub issues #108 / #197
  reported the filter broken in the past but it works on our key today).
- The MMSI filter is a *whitelist*, not a hint — subscribed MMSIs that aren't
  in range of any terrestrial AIS receiver simply don't report.

## Watchlist selection (picking 150 of ~780 vessels)

`vessel_registry` holds the full global LNG/FSRU fleet (~780 vessels),
bulk-imported from the IGU 2025 World LNG Report (`db/seed/lng_fleet_igu_2025.csv`
via `scripts/import_igu_fleet.py`). The 150-slot cap means we can't subscribe
to all of them at once — instead, slots are allocated by a tier scoring layer
that runs every hour inside the ingester (`pipeline/scoring.py`, also runnable
manually via `make scoring`):

| Tier | Rule | Typical count |
|---|---|---|
| 1 | Fix inside any `terminal_zones` polygon in last 3d (vessel plausibly *currently* in zone) | 30-50 |
| 2 | `vessel_state.dest` parses to a `terminals.unlocode` AND `state_ts > now() - 14d` | 15-30 |
| 3 | Fix inside any `config.ZONES` rectangle in last 14d (not 1/2) | 50-80 |
| 4 | Any fix in last 7d (not 1-3) | 400-500 |
| 5 | Fix in 7-90d OR never seen | 150-250 |

The numbers shift across the day as vessels arrive, depart, and declare new
destinations. Tier 1 was originally a 14-day window — tightened to 3 days
because the longer window admitted vessels that had visited a week earlier
and were since mid-Atlantic, wasting persistent slots on ghost MMSIs.

- **Persistent block (chunks 0 + 1, 100 slots)** — top 100 by
  `(tier ASC, score DESC)` where tier in 1-3. If tier 1-3 totals exceed 100,
  the tail is culled by oldest `last_fix`.
- **Scan rotation (chunk 2, 50 slots)** — 40 from tier 4 + 10 from tier 5,
  each pool ordered by `last_scan_window_at ASC NULLS FIRST`. Shortfall in
  one pool rolls over to the other so the chunk is always fully filled when
  the watchlist has ≥50 scan-eligible vessels. Each pick advances
  `last_scan_window_at = now()` in the same transaction so the next reconnect
  picks a *different* batch, even when the 5-min silence watchdog fires
  repeatedly. The tier-5 carve-out exists to prevent the previous starvation
  state where a tier-5 vessel could never be subscribed (and so could never
  accrue a fix to promote out of tier 5).
- **Promotion** — a scan vessel that delivers an in-zone fix (or a parseable
  inbound `dest`) gets re-tiered to 1-3 on the next scoring run, and lands
  in the persistent block on the very next 1h reconnect.

`load_persistent_mmsis` and `load_scan_mmsis` in `aisstream.py` read from
`priority_watchlist` and write back `in_slot` / `slot_kind` so the TUI can
render who's currently subscribed. The 1h reconnect cycle
(`RECONNECT_INTERVAL_SECONDS = 3600`):

1. `scoring_loop` recomputes `priority_watchlist`
2. Each `connection_loop` close+reopens its WebSocket
3. Persistent chunks get the freshest top-100; scan chunk swaps in the next 50

The 5-min `SILENCE_THRESHOLD_SECONDS` watchdog also reconnects mid-cycle
when a connection goes silent. For the scan connection this happens
frequently (most of the tier-4/5 pool is offshore or laid up), and is
*expected* — it accelerates scan-pool rotation rather than hurting signal.
The TUI distinguishes "silent" (socket alive, no fixes) from "dead"
(no events for 10+ min) to avoid mis-flagging the scan connection.

### Discovery (new LNG carriers)

The old bbox subscription doubled as a passive discovery mechanism. Under MMSI
filtering, unknown MMSIs never flow through us — that path is dead. Newbuilds
arrive via the IGU report refresh instead:

- IGU publishes annually; expect ~50-80 newbuilds/year added
- Download the latest report PDF, save to `db/seed/igu-world-lng-report-latest.pdf`
- Run `make refresh-fleet` — re-parses the appendix and incrementally imports
  any new IMOs via the VF VESSELS endpoint (~3 credits each, one-time)

Ad-hoc newbuilds before the next IGU refresh can be added manually:

1. Insert a row into `vessel_registry` with the new MMSI + IMO
2. `make enrich` — runs VesselFinder against the new entry to classify it
3. Verify `is_lng_carrier` or `is_fsru` is now TRUE
4. Wait up to 1h for the next planned reconnect

## What this means for the signal

For vessels in our subscribed set that are actively broadcasting in
terrestrial AIS coverage, visibility approaches ~100% — state-transition
timestamps in `port_events` land within seconds of the actual transition,
not the 2-12 minutes the throttled bbox design produced. The state machine's
back-dating logic still exists for the rare cases when AISstream momentarily
doesn't deliver a vessel (receiver outage, brief subscription gap).

**The right health metric is *coverage* of vessels currently in zone**, not
raw fix volume. Practical checks:

- Run `make port-events` and verify recent `zone_entry` / `moored` /
  `departed` events look complete vs reality (cross-check against a known
  port like Sabine or Rotterdam if needed).
- The TUI's connection-liveness pane should show **`live 3/3`** (active) or
  **`alive 3/3 (active: 2, silent: 1)`** (cyan; scan connection between
  windows). Either is healthy.
- Per-source fix rate in `ingestion_stats_minute` is highly variable —
  persistent connections typically each deliver 50-300 fixes/hour;
  scan can be near zero between productive windows. Trends over hours
  matter more than minute-by-minute spot checks.

**Failure modes worth flagging:**

- One of chunks 0/1 going silent for >10 min while still emitting
  lifecycle events → AISstream may have dropped the subscription;
  the next planned or watchdog reconnect should recover.
- All three sources silent for >10 min → check the process is still
  running and the API key still works.
- Tier 1 totals dropping to single digits → either a real lull in
  fleet activity at our terminals, or the scoring query is broken.

**Chunk 2 (scan) reports differently** from chunks 0+1: its 50 vessels
swap on every reconnect (planned 1h *and* watchdog 5-min). The distinct-MMSI
count over a multi-hour window is much higher than its per-minute fix rate
suggests, and the TUI's "Scan rotation" panel exposes the in-flight window
age and time to next planned rotation.
