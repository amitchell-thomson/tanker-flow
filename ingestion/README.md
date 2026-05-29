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

## Discovery (new LNG carriers)

The old bbox subscription doubled as a passive discovery mechanism:
`dynamic_enrichment.maybe_queue` watched every fix for an unknown MMSI inside
a `terminal_zones` polygon and triggered a VesselFinder lookup. With pure
MMSI filtering, unknown MMSIs never flow through us — that path is dead.

Operational implication: when a new LNG carrier enters service (industry-wide
~3-5 newbuilds/month, all announced months in advance) we need to add its
MMSI to `vessel_registry` ourselves. The next planned reconnect (every 6h)
picks up the new MMSI on its connection's chunk.

Manual addition flow for now:

1. Insert a row into `vessel_registry` with the new MMSI + IMO
2. `make enrich` — runs VesselFinder against the new entry to classify it
3. Verify `is_lng_carrier` or `is_fsru` is now TRUE
4. Wait up to 6h for the ingester's next planned reconnect, or restart ingest

**Follow-up planned**: a daily script that queries VesselFinder for `LNG
Tanker` vessel type globally, reconciles against `vessel_registry`, and
auto-adds new MMSIs. Until then, manual discovery is the discipline.

## What this means for the signal

Per-LNG-vessel visibility is now ~100%, so state-transition timestamps in
`port_events` are minute-accurate at worst. Median back-dating drops from ~2
minutes to ~1 second; p95 from ~12 minutes to ~60 seconds. The state machine's
back-dating logic still exists for the rare cases when AISstream momentarily
doesn't deliver a vessel (terrestrial receiver outage, etc.).

The single biggest lever on signal latency is keeping all three connections
healthy. `ingestion_stats_minute.fix_count` summed across the three
`aisstream-mmsi-N` sources should be ~150/min and stable; if it drops to ~100
or below for sustained periods, one of the connections is silently dropping or
the watchlist drifted.
