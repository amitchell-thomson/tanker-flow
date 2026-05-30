# ingestion

Subscribes to AISstream's WebSocket feed and writes raw fixes to `ais_fixes`,
voyage state to `vessel_state`, and identity to `vessel_registry`. Most of the
non-obvious design comes from AISstream's throttling — which we work around
by filtering server-side to the specific LNG carriers and FSRUs we care about
rather than pulling broad geographic data and discarding 95% of it.

## What AISstream actually delivers

A subscribed vessel, while admitted to your subscription, reports roughly once
per minute (per-vessel cadence is constant at ~1.05 fixes/MMSI/minute across
all observed throughput regimes). The throttle does not change *how often* an
admitted vessel reports — it changes *which* vessels are admitted in any given
minute.

Under the old bbox-based subscription, the set of admitted vessels rotated
randomly minute-to-minute. Two minutes that delivered very different total fix
counts shared only a fraction of their MMSIs:

```
                   distinct MMSI    in both    spike-only    settled-only
spike   13:32 UTC      1298            250        1048             —
settled 13:35 UTC       715            250          —             465
```

Drops had no characteristic bias — spike-only, settled-only, and common groups
all showed the same speed-band breakdown. Vessels we specifically care about
(LNG carriers, FSRUs) were not preferentially retained. In a 20-minute window
of 1500 fixes/min, an LNG carrier in our bboxes was visible an average of 4.9
minutes (~23% of the window). State transitions therefore landed in
`port_events` 2-12 minutes after the actual moment of the transition, which
became the dominant latency in the signal.

## Server-side MMSI filtering (current design)

AISstream exposes a `FiltersShipMMSI` subscription field that delivers only
messages from a fixed list of MMSIs (cap: 50 per subscription). With the LNG
fleet around the size we cover, this is enough to subscribe to *every* LNG
carrier and FSRU we care about and skip everything else.

The DB has 142 active LNG carriers + FSRUs (`vessel_registry` rows where
`is_lng_carrier OR is_fsru` and at least one fix in 30d). Across the
documented 3-conn-per-API-key cap that's 3 × 50 = 150 slots, with headroom.

`aisstream.py` runs **three parallel WebSockets**, each subscribed to a disjoint
~50-MMSI chunk of the priority watchlist. Per-vessel visibility climbs to
~100% — every fix from every priority vessel reaches `ais_fixes`. Total volume
collapses to ~150 fixes/min (one per priority vessel per minute) instead of
the old ~1100/min throttled sampling.

Source labels in `ingestion_stats_minute` and `ingestion_events`:

- `aisstream-mmsi-1` / `aisstream-mmsi-2` / `aisstream-mmsi-3` — one per parallel
  connection. The TUI aggregates across them with `source LIKE 'aisstream%'`.

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

## Watchlist selection (which 150 vessels to subscribe to)

`vessel_registry` now holds the full global LNG/FSRU fleet (~800 vessels),
bulk-imported from the IGU 2025 World LNG Report (`db/seed/lng_fleet_igu_2025.csv`
via `scripts/import_igu_fleet.py`). The 150 slots cap means we can't subscribe
to all of them at once — instead, the slots are allocated by a tier scoring
layer that runs every hour:

- **`pipeline/scoring.py`** ranks every LNG/FSRU vessel into one of 5 tiers
  using current AIS history + parsed `vessel_state.dest`:

  | Tier | Rule |
  |---|---|
  | 1 | Fix inside any `terminal_zones` polygon in last 3d (vessel plausibly *currently* in zone, not "was there last week") |
  | 2 | `vessel_state.dest` parses to a `terminals.unlocode` AND `state_ts > now() - 14d` |
  | 3 | Fix inside any `config.ZONES` rectangle in last 14d (not 1/2) |
  | 4 | Any fix in last 7d (not 1-3) |
  | 5 | Fix in 7-90d OR never seen |

- **Persistent block (chunks 0 + 1, 100 slots)** — top 100 by `(tier ASC, score DESC)`
  where tier in 1-3. Vessels currently in or unambiguously heading to our zones.
- **Scan rotation (chunk 2, 50 slots)** — next 50 by `(tier ASC, score ASC)` where
  tier in 4-5. Stalest first, so every vessel in the registry cycles through over
  ~13h.
- **Promotion** — a scan-window fix that lands inside a zone polygon (or a parsed
  inbound `dest`) bumps the vessel's tier to 1-3 on the next scoring run. The
  vessel gets a persistent slot on the very next 1h reconnect.

`load_persistent_mmsis` and `load_scan_mmsis` in `aisstream.py` read from
`priority_watchlist` and write back `in_slot` / `slot_kind` so the TUI can
render who's currently subscribed. The 1h reconnect cycle (`RECONNECT_INTERVAL_SECONDS`)
runs:

1. `scoring_loop` recomputes `priority_watchlist`
2. Each `connection_loop` close+reopens its WebSocket
3. Persistent chunks get the freshest top-100; scan chunk swaps in the next 50

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

Per-LNG-vessel visibility is now ~100%, so state-transition timestamps in
`port_events` are minute-accurate at worst. Median back-dating drops from ~2
minutes to ~ 1 second; p95 from ~ 12 minutes to ~ 60 seconds. The state machine's
back-dating logic still exists for the rare cases when AISstream momentarily
doesn't deliver a vessel (terrestrial receiver outage, etc.).

The single biggest lever on signal latency is keeping all three connections
healthy. `ingestion_stats_minute.fix_count` summed across the three
`aisstream-mmsi-N` sources should be ~150/min and stable; if it drops to ~100
or below for sustained periods, one of the connections is silently dropping or
the watchlist drifted.

Note that **chunk 2 (the scan-rotation connection) reports differently** from
chunks 0+1: its 50 vessels swap every hour, so the rate per *vessel* is lower
than persistent connections but the distinct-MMSI count over a multi-hour
window is much higher. The TUI's "Scan rotation" panel reports the in-flight
window age and time to next rotation.
