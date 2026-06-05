# Park check-up log

Append-only log written by `/park-checkup` (see `.claude/commands/park-checkup.md`).
Baseline entry below is transcribed from `docs/review-2026-06-02-park-audit.md`.

## 2026-06-02 — GREEN (baseline, from park audit)
- liveness: all 3 sources live | fixes/day: ~9k (new regime) | errors: n/a | scoring: live | backup: daily cron installed + validated | VF: cap 14/day
- phantom (mmsi_filter): 0/14 open legs censored | stale-close usgulf: 0/8 | events: 1595 (port_events total)
- notes: corpus ~4 days old at audit; phantom/stale-close metrics not yet meaningful (no leg has had time to age out). B1 (flush re-queue) + B4 (backups) fixed same day.

## 2026-06-04 — AMBER (volume slide; everything else green)
- liveness: 3/3 live (≤24s) | fixes/day: 8.7k → 5.8k → ~5.9k pace (slide on mmsi-1/2; mmsi-3 steady) | errors: 0 (129 planned / 95 watchdog reconnects, watchdog almost all mmsi-3 scan churn — structural) | scoring: 2m50s old, 150 in_slot | backup: daily 04:00 firing, 705M ×3 | VF: 19/14/14 per day (06-02's 19 predates the 16:30 cap change 20→14), balance 4898 ÷ 363 d ≈ 13.5/day — on glide
- phantom (mmsi_filter): 0/26 open legs censored — still degenerate (oldest new-regime leg ~5.5 d < 14–18 d OD windows) | stale-close usgulf: 2/9 trip the >72h-silence proxy, but both are benign laden departed→zone_exit→dark-mid-ocean (real exit fix, not a synthetic close) | events: 1344 total (bbox 1199, −303 vs audit — teleport-filter rebuild 9f95619, not data loss; mmsi_filter 145, +52), max event_time fresh, signal_daily → 06-04
- notes: WATCH fixes/day next check-up — ~−40% vs the 9k baseline over two days, concentrated in the persistent block; connections healthy + 0 errors so likely fleet-geometry/terrestrial-coverage variation, but a continued slide would mean real coverage loss. port_events audit baseline (1595) is no longer comparable post-9f95619; new comparison baseline = 1344.
- addendum (same-day follow-up): **VF cap saturated + front-loaded** — exhausted by 10:16 (06-03) / 08:51 (06-04), next-day first bill 00:14–01:25 → demand > 14/day, ~14h/day with zero rescue capacity; export_arrival stricter bar working (credits 2/6/1/0 by day). **Late pickups**: 15/33 new-regime zone entries re-acquired after >48h dark (13 usgulf + 1 usatlantic ballast returns, 1 nweurope); EU laden side tight (worst 2d2h); 0 cold starts; scan rotation recovered the worst case (34d dark, mmsi-3) and scoring pinned it within 5 min; 14 inline promotions since cutover. **Volume shape**: 10.1k (05-31) → 9.2k → 8.6k → 5.8k → ~5.9k pace; flat last ~36h, slight uptick in latest buckets; MMSIs/3h stable (30–42) → per-vessel rate drop, not vessel loss; 26 open in-transit legs (vs 14 at audit) + only 47/100 pinned+persistent slots seen in 24h → mid-ocean cohort is the likely mechanism; expect recovery as the 05-31→06-02 departure wave reaches EU coastal range (~06-10 onward).
