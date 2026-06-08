# Design: second-IP ingester (scaling AIS coverage past the 3-connection cap)

**2026-06-08.** A design + cost/benefit decision for running a second
`ingestion.aisstream` worker behind a second egress IP, to roughly double watched
vessels (150 → 300 slots). Written after the CLEAN VITALITY miss (a FOR-ORDERS
LNG carrier that went dark in tier 5 and arrived in the US Gulf unwatched).

**Status: DEFERRED.** Not worth building during the park. The cheap, targeted
fixes already shipped (see "What we already did") are the right-sized response.
This doc records the analysis and a ready-to-execute plan so it's a known
~2-session move when a trigger fires.

---

## TL;DR — the decision

The hard cap is **3 concurrent AISstream connections per source IP** (proven
empirically 2026-06-02 and re-confirmed 2026-06-08 on two distinct IPs — home WAN
`81.102.34.222` and a laptop exit-node IP `192.76.8.201`, each giving a clean 3
OK / 4th+ → 429; see `scripts/aisstream_conn_test.py`). It is per-IP, **not**
per-key/account. So the only way past 150 slots is a second egress IP.

**Verdict: defer.** A second IP is a broad coverage move with steep diminishing
returns, real ongoing complexity, and a corpus-cleanliness risk that cuts against
the whole reason the project is parked. It would also probably **not** have caught
the miss that motivated it. Keep the cheap wins; revisit on a concrete trigger
(below).

---

## What we already did (the right-sized response)

The CLEAN VITALITY miss had three causes; the first two are now fixed cheaply:

1. **ETA-format bug** (`scripts/import_igu_fleet.py` wrote `{"raw": ...}`, which
   `scoring._parse_eta` silently rejects) — fixed; routes ETA through the
   canonical `vf_rescue.vf_eta_to_ais_dict`; 608 existing rows backfilled. This
   had structurally disabled tier-2 imminent-ETA promotion for ~500
   fleet-imported vessels.
2. **No targeted backstop for the pattern** — added the `eta_arrival` rescue
   class (#7): a silent carrier with an imminent parsed ETA gets one ~1-credit
   discovery poll even with no near-terminal fix. This directly targets the
   missed-arrival pattern; surplus-only, capped per run.
3. **Thin tier-5 discovery** — the structural coverage limit this doc is about.
   Monitored, not fixed: park-checkup item 12 (imminent-ETA & tier ≥ 4, read by
   silence) surfaces the dark-but-arriving set early.

The honest gut-punch: a second IP would **probably not have caught CLEAN
VITALITY**. It was tier 5 (20 days dark, FOR-ORDERS); even with 300 slots, tier 5
(~553 vessels) mostly doesn't earn a persistent slot — the extra 150 land on
tier 3–4. The thing that fixes that pattern is `eta_arrival` + the ETA fix, both
shipped. The expensive option doesn't reliably solve the problem that motivated it.

## Cost / benefit

**Cost**
- *Money:* trivial — ~€5/mo Mullvad endpoint (or $5/mo VPS). ~€60/yr.
- *Engineering:* 1–2 sessions (sharding + `slot_worker` migration + singleton
  gating + observability).
- *The real cost — ongoing complexity in a parked system:* a second always-on
  process, a VPN dependency (new failure mode: tunnel drop → half the fleet goes
  dark intermittently), a schema migration, doubled per-source monitoring, and
  the partition/clobber bugs of two writers. The park exists to accrue a **clean
  single-regime corpus**; a flaky second egress risks injecting the very
  coverage gaps/jitter the park protects against.

**Benefit**
- 150 → 300 watched vessels, but **steeply diminishing**: tiers are 41/27/77/83/553
  (t1–t5, 2026-06-08). The first 150 slots already cover the highest-relevance
  vessels (in-zone, declared-inbound, in-bbox). The extra 150 go disproportionately
  to tier 4–5, much of which is **non-Atlantic-basin** trade (Qatar→Japan,
  Australia→Korea) that never touches the 7 zones.
- Second-order win: more free AIS coverage → fewer VF-rescue candidates → eases
  the chronically-negative glide surplus. AIS (fixed cost) substitutes for VF
  credits (depleting reserve). Real but modest.

**Timing.** Nothing downstream consumes this coverage for decisions yet — the
spread model is unbuilt (premature until the corpus accrues). Maximal-coverage
value materializes when the live signal is built, not during the park. And the
headline lane is already healthy: last check-up had EU-laden arrivals tight
(worst 2d2h, one new >12h case) and entry-side `cold_start = 0` (no fully-missed
arrivals in the new regime). CLEAN VITALITY was a tier-5 edge case, not a
systemic hole.

## Triggers to revisit

Build it when **any** of these fires:
1. Park-checkup item 12 shows **sustained misses in the relevant Atlantic cohort**
   that `eta_arrival` can't catch (a real hole, not edge cases).
2. You start building the **live signal** and need maximal corpus completeness.
3. VF glide surplus stays **chronically negative with genuine priority-0 unmet
   demand** — then trading €5/mo of AIS coverage for credit relief is clearly
   net-positive.

---

## Implementation plan (when triggered)

Grounded in the current ingester: `connection_loop(source_name, chunk_index)`
spawned `for i in range(NUM_CONNECTIONS)`; `load_persistent_mmsis` /
`load_scan_mmsis`; `update_in_slot`; the singleton `scoring_loop` /
`port_events_loop` / `vf_rescue_loop` in `main()`.

**Invariant:** two workers behind two distinct egress IPs, each holding ≤3
connections (→ 6 conns / 300 slots), covering **disjoint** vessels (no overlap,
no gaps), both writing the same TimescaleDB. The union = top 300 of
`priority_watchlist`, partitioned deterministically so the workers never
coordinate at runtime.

### 1. Topology — the second IP (single machine, no VPS)

Recommended: a **WireGuard/VPN sidecar container + the second ingester sharing
its network namespace** (Docker is already the runtime for TimescaleDB):
- a VPN container (`gluetun` or plain `wireguard`) → a stable second public IP;
- `ingester-1` with `network_mode: "service:vpn"` so its traffic exits the tunnel;
- **exclude the DB subnet from the tunnel** (gluetun `FIREWALL_OUTBOUND_SUBNETS`
  or a WG `AllowedIPs` carve-out) so `ingester-1 → TimescaleDB` stays local and
  only AISstream traffic is tunneled.

`ingester-0` stays on the host default route (home IP). Durable, survives reboots.
Alternatives: a second always-on host on a separate uplink (cleanest, needs
hardware elsewhere); Tailscale Mullvad exit node (same cost, less control). The
laptop exit node used for testing is **not** a 24/7 option.

### 2. Code changes (mostly `ingestion/aisstream.py`)

**(a) Worker identity.** Add `WORKER_ID` / `WORKER_COUNT` to `config.py`
(default `0` / `1` → today's exact behavior; zero change when unscaled).

**(b) Deterministic slot partition.** Both workers read the same
`priority_watchlist` snapshot and partition by rank — disjoint by construction, no
locks:
- `load_persistent_mmsis`: select top `PERSISTENT_SLOTS × WORKER_COUNT` (200), keep
  rows where `row_number() OVER (ORDER BY tier, score) % WORKER_COUNT = WORKER_ID`,
  then `chunk_persistent` that share across the worker's 2 persistent connections.
- `load_scan_mmsis`: same `% WORKER_COUNT = WORKER_ID` filter on the scan pools;
  the `last_scan_window_at` write-back already self-rotates per worker.
- Connection labels → `aisstream-w{WORKER_ID}-{i+1}` (stats/TUI key on the label).

**(c) Fix the `update_in_slot` clobber (correctness).** It currently does a global
`UPDATE priority_watchlist SET in_slot=FALSE` then sets its own rows — two workers
would **clobber each other's `in_slot`**. Add a `slot_worker SMALLINT` column;
each worker only clears/sets rows it owns. This is the one schema change
(migration + `schema.sql`). Worth doing even before scaling — it's latent today.

**(d) Singleton background tasks.** `scoring_loop`, `port_events_loop`,
`vf_rescue_loop` are global recomputes / shared-budget spenders — they must run on
exactly one worker (else double-spent VF credits, redundant rebuilds). Gate behind
`RUN_SCORING` / `RUN_PORT_EVENTS` / `RUN_VF_RESCUE` flags, true only for worker 0.
Add a startup assertion that exactly one worker has `RUN_VF_RESCUE`. (Cleaner v2:
lift scoring/port_events into a dedicated coordinator unit; flag-gating is the
low-risk v1.)

### 3. Observability

- Generalize park-checkup item 1 (per-source liveness) and the TUI health row
  from `aisstream-mmsi-{1,2,3}` to the worker-suffixed labels; assert **all 6**
  fresh (the B5 aggregate-HUD blind spot doubles with two workers).
- Add a per-worker heartbeat so "worker 1's VPN dropped → its half went dark" is
  visible; today that would be silent behind the green aggregate HUD.

### 4. Failure modes (decide explicitly)

- **VPN drop** → worker 1's IP vanishes, its 3 conns fail; the watchdog retries,
  coverage degraded until the tunnel heals. Acceptable for v1 with monitoring.
- **Worker-1 death** → its partition goes dark (not redistributed). Monitor +
  alert; dynamic re-partition is a v2 nicety, not v1.
- **Both run a singleton** (misconfig) → VF double-spend. The `vf_rescue_log`
  ledger bounds it, but the flag-gate + startup assertion must be correct.

### 5. Phased rollout

1. **Sharding code at `WORKER_COUNT=1`** — land partition + `slot_worker` +
   singleton flags with the default = today's behavior. Tests + a live run prove
   zero regression. Ships the clobber-bug fix even with no second IP.
2. **VPN sidecar up, validate the IP** — `scripts/aisstream_conn_test.py --ramp 3`
   from inside the container's netns shows the VPN IP, 3 OK.
3. **`WORKER_COUNT=2`, worker 1 on a tiny slice first** — verify disjoint
   partition (no MMSI in both workers' `in_slot`), both workers' DB writes, all 6
   sources fresh, VF/scoring on worker 0 only.
4. **Full 50/50**, then re-run the missed-arrival monitor — tier-5 dark set
   should roughly halve.

### 6. Effort

~1–2 focused sessions for steps 1–3 (the partition + `slot_worker` migration +
singleton gating + observability are the bulk; the compose/VPN is config). Step 4
is a config flip + a checkup.

### Open decisions before building

1. VPN-sidecar topology vs. an always-on second box.
2. Scoring/port_events flag-gated in the ingester vs. lifted to a coordinator unit
   (lean flag-gate for v1).
