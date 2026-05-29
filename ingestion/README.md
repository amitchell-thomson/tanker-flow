# ingestion

Subscribes to AISstream's WebSocket feed and writes raw fixes to `ais_fixes`,
voyage state to `vessel_state`, and identity to `vessel_registry`. The non-obvious
work is dealing with AISstream's throttling behaviour, which shapes much of the
design here and downstream in the state machine.

## What AISstream actually delivers

A subscribed vessel, while admitted to your subscription, reports roughly once
per minute (per-vessel cadence is constant at ~1.05 fixes/MMSI/minute across
all observed throughput regimes). The throttle does not change *how often* an
admitted vessel reports — it changes *which* vessels are admitted in any given
minute.

The set of admitted vessels rotates, and rotates randomly. Two minutes that
deliver very different total fix counts will share only a fraction of their
MMSIs:

```
                   distinct MMSI    in both    spike-only    settled-only
spike   13:32 UTC      1298            250        1048             —
settled 13:35 UTC       715            250          —             465
```

There is no characteristic bias in the drops — spike-only, settled-only, and
common groups all show essentially the same speed-band breakdown (~55%
stationary, ~30% underway, ~15% slow). Vessels we specifically care about
(LNG carriers, FSRUs) are not preferentially retained either.

Concretely, for the LNG carriers in our `vessel_registry` that were within our
bounding boxes at all during a 20-minute window of 1500 fixes/min:

- 9 LNG carriers/FSRUs visible at any point in the window
- 0 visible in 18+ of the 21 minute buckets
- Average per-vessel visibility: 4.9 minutes (≈23% of the window)

That ratio scales roughly linearly with throughput: at the proven-sustainable
3300/min, per-vessel visibility is ~50%; at the original 7-bbox 600/min floor,
it was ~10%.

## Why the connection setup looks the way it does

Two undocumented behaviours discovered empirically:

- **Concurrent-connection cap = 3 per API key.** The 4th simultaneous WebSocket
  from the same key gets HTTP 429 at the handshake. Fanout beyond 3 is not
  available.
- **Throttle budget scales with total bboxes across all active connections.**
  Two connections covering 3 bboxes between them deliver the same total
  throughput as one connection covering the same 3. Two connections covering 7
  bboxes between them decay identically to one connection covering all 7.

Reconnecting a single WebSocket gives a fresh spike for a few minutes before
decaying back. So does *changing the subscription on an open WebSocket* — and
crucially, swapping the subscription resets the throttle bucket on the new
bbox set without consuming a fresh connection.

This is why `aisstream.py` runs one WebSocket and rotates its subscription on a
6-minute cycle: 5 minutes on `MAIN_ZONES` (nweurope, usgulf, wmed — the three
high-volume zones) and 1 minute on `SECONDARY_ZONES` (the four lower-volume
ones). Effective sustained rate is ~3160 fixes/min covering all 7 zones, vs
~600/min eventual floor with a static 7-bbox subscription.

Source labels in `ingestion_stats_minute`:

- `aisstream-main` — minutes when the main subscription was active
- `aisstream-secondary` — minutes when the secondary subscription was active

Lifecycle events (`ingestion_events`) record each subscription change with the
bbox set and window length in its `detail` JSONB.

## Account-level throttle: cycle decay under cumulative load

Rotation isn't a free lunch. Beyond the per-subscription throttle that the
swap resets, AISstream appears to track an account-level credit pool with
two distinct dynamics:

- a per-subscription **burst capacity** — the spike at the start of each
  rotation window
- a per-account **refill rate** — the floor that cycles converge to under
  sustained usage

A pristine session (no recent heavy usage on the key) holds the burst
capacity flat: ~3700/min on main, every cycle, for hours. A session run
after the account has pulled tens of thousands of fixes/hour for many
hours shows clear cycle-over-cycle decay until it stabilises near the
refill rate. Observed once-per-cycle main throughput over a 65-minute
run that started after ~14 hours of heavy testing:

```
cycle 1 first minute   3137/min      ← burst capacity, briefly available
cycle 2                2183
cycle 3                1677
...
cycle 7+               ~1050/min     ← steady-state refill rate
```

Within each cycle, main also decays ~15% over its 5-minute window —
again, the burst-bucket draining faster than the refill replenishes it.

The asymmetry between main and secondary is the diagnostic that fingers
the throttle as the cause. Main pulls from one of the highest-volume
geographic windows AISstream serves (nweurope alone is thousands of
candidate vessels); it always saturates the per-subscription budget.
Secondary's 4 zones combined deliver ~330 messages/min at source, well
under any throttle threshold — those windows stay flat at ~325/min
across all cycles regardless of how depleted the account is.

So if you see `aisstream-main` drifting low for sustained periods, that
is the account budget speaking, not the rotation breaking. The single
lever that helps is **idle time**: extended periods without subscription
activity refill the bucket. Frequent process restarts and short test
runs both burn through it faster than they refill.

## What this means for the signal

State transitions get back-dated. When a vessel berths, anchors, or departs,
that event only enters `port_events` when AISstream actually delivers the fix
on the right side of the polygon. With per-vessel visibility around 25-50%,
this introduces latency:

- median back-dating: ~2 minutes
- 95th percentile: ~12 minutes

`pipeline/state_machine.py` already handles this for the dwell-confirmed
events: `anchored`, `moored`, and `departed` are timestamped at the *first
qualifying fix* (the moment of transition), not at the moment dwell is
confirmed. Raw polygon-crossing events — `anchorage_entry`, `anchorage_exit`,
`zone_entry`, `zone_exit` — fire at the actual observed fix and inherit the
full latency floor.

Practical implications:

- Higher ingest rate raises the per-vessel hit rate, which lowers transition
  latency. It does not produce denser tracks of the same vessels.
- Coverage of any specific moment in time is a sampling problem. Backtests
  should expect more reliable transition timing on busy days (more LNG
  carriers in our bboxes → more get sampled in any given minute) than on
  quiet ones.
- The single biggest lever on signal latency is keeping the rotation healthy.
  `ingestion_stats_minute.fix_count` for `aisstream-main` is the leading
  indicator — if it drifts below ~2000/min for sustained periods, transition
  back-dating gets noticeably worse.
