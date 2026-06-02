# tanker-flow park-readiness audit — 2026-06-02

A full-state audit taken **before parking the project** to let the AIS ingestion
accrue a clean single-regime corpus unattended for weeks. Three axes:
**architecture quality**, **ingestion robustness**, and (deepest) **data/signal
quality**. Same read-only methodology as the two prior audits
(`docs/review-2026-05-31-pre-signal-audit.md`,
`docs/review-2026-05-31-post-hardening-audit.md`) — live TimescaleDB queries +
the pure-logic modules — extended with three things those two didn't do: an
**external EIA validation**, a **magic-constant sensitivity sweep**, and a
**park-survival** assessment.

**This is an audit, not a remediation.** No production code was changed. Every
number traces to a query or to the throwaway `analysis/scratch_audit.py` (see the
reproducibility appendix). The signal tables were *not* rebuilt by this audit; the
live ingester rebuilds `port_events` every ~2 min, so the snapshot is current
(`port_events` max event 16:37, captured ~16:46 UTC).

---

## TL;DR — verdict

**The project is in a state to be proud of, and safe to park — with two ingestion
fixes worth making first (B1, B4) that are about *protecting the accruing data*,
not the signal logic.**

- **Signal/data quality (primary): GREEN.** Every prior-audit invariant still
  holds at +2 days; the DFA has **0 genuine violations**; the regime seam is
  perfectly captured (0 nulls / 0 disagreements); phantom-leg censoring is
  **load-bearing and working** (removing it would inflate the headline in-transit
  signal +69%); and an **EIA cross-check passes the order-of-magnitude sanity
  test** (implied ~8 Bcf/d vs EIA ~17–18.5 Bcf/d → ~45–50% capture, as expected
  for terrestrial coverage). The single most influential judgment-call constant is
  `OD_WINDOW_DAYS['nweurope']` (Phase-2 replacement already planned); the others
  are robust.
- **The new regime is materially cleaner than the old one** — the exact reason
  parking is the right call. Under the MMSI-filter scheme: **0% phantom rate** (vs
  44% old), **0/8 synthetic stale-close exits at usgulf** (vs 83% old). The
  artifacts that dog the historical block are old-regime-only and gone going
  forward. The catch: the new corpus is only **~4 days / ~9k fixes/day** so far —
  hence parking.
- **Ingestion robustness: GREEN for survival, with two data-protection gaps.**
  tmux + your dashboard-watching neutralise the obvious risks. The background
  loops are well-isolated and WS drops self-heal. But: **(B1)** a transient DB
  error silently drops a flush batch (buffers cleared before the write, no
  re-buffer), and **(B4)** there is **no backup** — weeks of parked data live only
  on the `/srv/data` volume, one `make reset` (`rm -rf`) or disk failure from gone.
- **Architecture quality: GREEN.** 150 tests pass (0.19s), production ruff clean,
  Alembic at a single clean head (`current == head`). Gaps are the known ones
  (no integration/ingestion tests, no CI) — low urgency for parked code, but worth
  a safety net for cold-return-in-months.

Nothing here blocks parking. B1 + B4 are the only items I'd act on *before*
stepping away, because they protect the data the whole exercise is meant to
collect.

---

## Severity-rated findings

| # | Area | Finding | Severity | Action posture |
|---|------|---------|----------|----------------|
| B1 | Ingestion | Flush path clears buffers **before** the DB write; on insert exception the batch is logged and **dropped, not re-buffered** (`aisstream.py:419–487`). Silent data loss on any transient DB hiccup. Inserts are `ON CONFLICT DO NOTHING` (idempotent) so write-before-clear is safe. | **High** | **Resolved 2026-06-02** (re-queue on failure) |
| B4 | Ingestion/DB | **No backup automation.** Data only on `/srv/data/tanker_db`; `make reset` is `sudo rm -rf` of that path. The parked corpus is the entire point of the wait. | **High** | **Resolved 2026-06-02** (`make backup` + daily cron) |
| A4-1 | Signal | `OD_WINDOW_DAYS['nweurope']` is the most influential constant: in-transit current stock swings 6.25M→9.4M m³ across 12→30 d (±3 d ≈ ±7%). Phase-2 rolling-median replacement already planned. | Medium | Monitor; Phase-2 |
| A7 | Signal | Signal captures only **~45–50%** of EIA US LNG export volume (level undercount). Fine for a *relative/leading* signal; not calibrated for absolute volume. | Medium | Document; validate on accrual |
| A5 | Signal | **60%** of in-transit legs (39/65) fall in the `unknown` destination band (terrestrial AIS loses the mid-ocean dest broadcast). Honest by design, but the per-zone in-transit breakdown is dominated by `unknown`. | Medium | Known limitation |
| B5 | Ingestion | Web HUD shows a **single aggregate** liveness (`/api/ingest-status` maxes over all sources); a partial outage (1 of 3 connections dead) stays green. Per-source view is TUI-only. | Medium | Recommend HUD tweak |
| B2 | Ingestion | Process-level death (OOM / bug outside the per-loop guards) leaves the tmux pane idle with no restart. Inner loops + WS reconnect are already well-guarded. | Low/Med | Recommend restart wrapper |
| C1 | Architecture | No integration test (port_events orchestration), no ingestion-layer tests, no CI. | Low | Recommend safety net |
| A6 | Signal | usgulf stale-close still 78% (old-regime artifact); benign (72% follow a `departed`, a lower-bound timestamp). **0% under the new regime.** | Low (doc) | Known; self-resolving |
| 3b | Signal | `design_draught` missing for **75/780** in-scope (unchanged) → flow-direction laden fallback. | Low | Optional backfill |
| C3 | Architecture | Large modules (`tui.py` 1559, `vf_rescue.py` 1078, `app.py` 1029). | Low | Future refactor, no action |

---

## Track A — Data & signal quality (primary)

### A1 · Re-baseline vs the post-hardening audit (no regressions, healthy growth)

`port_events` 1595 rows (was 1506), `departed` **194** (was 178), 207 distinct
MMSIs. Regime split **bbox 1502 (174 MMSI) / mmsi_filter 93 (32 MMSI)** — the new
regime has grown 23 → 93 events. `laden_source`: draught 915 / flow_direction 561
/ NULL 119. Departures: usgulf 107 laden, usatlantic 2 laden, nweurope 83 ballast,
wmed 2 ballast (109 laden / 85 ballast). All consistent with the post-hardening
baseline scaled up by ~2 days — no metric moved in a way that signals a regression.

### A2 · Regime-seam integrity — GREEN

- Generated `port_events.regime` column: **0 NULLs, 0 disagreements** vs the
  2026-05-30 09:27 UTC cutover.
- Clean straddle: last bbox event 09:25:50, first mmsi_filter event 09:54:31.
- `signal_daily` carries `all` (416) / `bbox` (394) / `mmsi_filter` (36) rows, so
  a regime-segmented *and* pooled series are both queryable — nothing can silently
  aggregate across the seam.
- **The volume cliff at the seam is large:** old regime ~480k–1.1M fixes/day; new
  regime **~9k fixes/day** (a ~50–100× drop, the expected consequence of MMSI
  filtering). The clean single-regime corpus is **~4 days old** today. This is the
  quantitative justification for parking — the model needs months of this.

### A3 · Phantom-leg quantification — censoring is load-bearing and the new regime is clean

Leg status (baseline): closed 38, same_zone 34, open_in_transit 70,
**open_censored 39**, open_arrival_gap 9, open_floating 4 (194 total).

| Regime | open legs | censored | arrival_gap | floating | in_transit | **phantom %** |
|--------|----------:|---------:|------------:|---------:|-----------:|--------------:|
| bbox | 108 | 39 | 9 | 4 | 56 | **44%** |
| mmsi_filter | 14 | 0 | 0 | 0 | 14 | **0%** |

**Censoring inflation test (the guard against the headline-signal bias):**
current-day in-transit stock is **7.50M m³ (44 legs)** with censoring on, vs
**12.70M m³ (75 legs)** if every open leg were kept — **+69% inflation** if the
censor were removed. The phantom-leg fix is doing real work.

The new-regime 0% phantom rate is excellent — but note the new regime is only 4
days old, so no leg has yet had *time* to age past its window into a phantom; the
open-leg pin (`scoring.py`) is what should keep it near zero as the corpus
accrues. **This is the #1 thing to re-check on return.**

### A4 · Magic-constant sensitivity (new) — one fragile constant, the rest robust

Run in-memory against the live `port_events` via the pure functions (no DB writes;
`port_events`/`signal_daily` row counts unchanged before/after).

- **`OD_WINDOW_DAYS['nweurope']` — the one to watch.** In-transit current stock:
  6.25M (12d) → 6.96M (15d) → **7.50M (18d, current)** → 7.69M (21d) → 8.51M (25d)
  → 9.41M m³ (30d). ±3 d around the chosen 18 d ≈ ±7%. Moderately sensitive; this
  is exactly the constant Phase-2 plans to replace with per-O-D rolling medians.
- **`CENSOR_OPEN_DAYS` — nearly inert.** in_transit 67→72 across 18–60 d. The
  `nweurope` fallback window governs undeclared legs, so this global cap rarely
  binds. (Implication: the real lever is `OD_WINDOW_DAYS` + `FALLBACK_DEST_REGION`,
  not `CENSOR_OPEN_DAYS`.)
- **`RECENT_FIX_DAYS` — fully insensitive** in current data (floating=4,
  censored=39 across 2–14 d). The floating/phantom split isn't on this boundary
  today.
- **`OPEN_VISIT_CEILING_DAYS=5` — robust.** Loading-stock plateaus at 693k m³ for
  any ceiling 2–10 d; only dropping to 1 d would cut it (to 347k). The chosen 5 d
  sits comfortably on the plateau.
- **Dwell windows (30/30/15 min, SOG 1.0)** can't be swept in-memory (they live
  inside the event walk); A1/A6 show no instability, so deferred to a dedicated
  re-walk only if a future baseline drifts.

### A5 · Null-rate & coverage — clean now, one structural limit

- **`gas_capacity_m3` NULL in the signal base: 0** (108 in-transit/ballast legs,
  205 visits — none skipped). The silent-skip surface in `signal._gas` is empty
  today (the registry has 4 gas-missing vessels, but none are currently in a
  signal base). Worth monitoring, not currently a problem.
- **`unknown` destination band: 39/65 in-transit legs (60%)** — terrestrial AIS
  loses the dest broadcast for most open legs. Surfaced honestly rather than
  assumed NW-Europe. The dominant in-transit band is therefore `unknown`.
- `design_draught` missing **75/780**; `dwt` 0 missing; dest resolved for **50/780**
  watchlist vessels (≈6%, mostly because most carriers don't declare a known-LNG
  terminal dest — consistent with prior audits).

### A6 · Invariants & event realism — DFA holds, stale-close benign and self-resolving

- **DFA: 0 genuine violations.** A naive `event_time`-ordered re-scan flags 38
  apparent failures, but all decompose into **10 FSRU bare-`moored` synthetics**
  (which bypass the walk by design and are never validated at build time) + **28
  tied-timestamp artifacts** (event-time order ≠ emission order). The authoritative
  check is `validate_sequence` at `port_events.py:346`, run in emission order on
  every walked vessel and raising on failure; the ingester rebuilds the table every
  ~2 min and it is fresh — so the DFA is passing in production.
- **Stale-close:** usgulf 111/142 `zone_exit` synthetic (78%), nweurope 6/213 (3%)
  — unchanged. Benign: **80/111 (72%) follow a `departed`** (vessel left laden, went
  dark mid-ocean → the timestamp is a lower bound, not a fabricated event).
  Forward-looking win: **new regime 0/8 synthetic exits at usgulf vs 83% old** —
  the artifact is old-regime-only and effectively gone under the MMSI scheme.
- `cold_start` 199/1595, all on first-fix-in-polygon + FSRU synthetics.

### A7 · EIA external validation (new — first ground-truth cross-check)

*Method & limits stated up front:* terrestrial AIS undercounts, the m³-LNG→Bcf
conversion carries ~±15%, and — critically — there is **no clean overlapping
single-regime window** (EIA monthly lags to ~March 2026; our clean new-regime data
is 4 days). So this is an **order-of-magnitude sanity check**, not a backtest.

- **Implied throughput from the data:** 109 laden US-export departures over
  2026-04-14 → 06-02 (49 d), summing **18.45 M m³ LNG** ≈ **376k m³/day** ≈
  **8.0–8.8 Bcf/d** (at ~22.8–23.4 MMBtu/m³).
- **EIA actual (series N9133US2, fetched 2026-06-02):** US LNG exports **539,203
  MMcf Jan 2026 (~17.4 Bcf/d)**, **573,479 MMcf Mar 2026 (~18.5 Bcf/d)**; Apr not
  yet released (next update 6/30).
- **Result: ~45–50% capture.** Right order of magnitude → **sanity check passes**.
  The undercount is expected (old-regime random drops, departed-skip, partial
  terminal/destination coverage, conversion uncertainty). For a *relative/leading*
  signal this is fine **provided the capture ratio is stable** — which it is **not**
  across the seam (old random-drop vs new MMSI-filter), reinforcing "never train
  across 2026-05-30."

**Recommendation:** promote this into the planned `data/eia.py` as a standing
harness. Its value compounds during the park: once the new-regime corpus spans
months, compute the **new-regime capture ratio and its stability vs EIA monthly**
— that is the real signal validation, and parking is what makes it possible.

---

## Track B — Ingestion robustness (tmux + dashboard context applied)

Context accepted: runs under tmux on a home server (survives client disconnect),
and you watch the viz dashboard frequently (total outages get noticed). So the
findings below are the things tmux + a human watcher **don't** cover.

### B1 · Silent data loss on flush failure — **High**, fix before parking
`flush_buffers` (`aisstream.py:417`) swaps the buffers into locals and clears
`ingest_state.{fix,registry,state}_buf` at lines 419–428, **then** writes at
430–487. Each `executemany` is wrapped in a `try/except` that logs a warning and
**moves on** (444/462/479/487) — the batch is already detached and is lost. A
transient DB blip (restart, lock timeout, brief disk pressure, pool exhaustion)
therefore drops up to a flush-interval of fixes *silently*, even while the process
is healthy. Likelihood is low per event but the exposure is unbounded over a weeks-
long unattended window, and under the sparse new regime a single dropped flush can
take out a leg-defining terminal-approach fix.
*Fix (recommended, not applied):* write-before-clear, or re-append the batch on
exception. The inserts are `ON CONFLICT DO NOTHING`, so a retry/replay is safe.

### B2 · Crash survival — **Low/Med**, recommend a restart wrapper
The three in-process loops (`scoring_loop` :642, `port_events_loop` :658,
`vf_rescue_loop` :673) each wrap their body in a per-iteration `try/except` that
logs and continues, and `connection_loop` (:682) self-heals WS drops with 30/60s
backoff — so a failure in one rebuild does **not** kill ingestion. The uncovered
case is *process-level* death (OOM, an exception outside these guards, a
permanently dead pool): tmux keeps the pane but won't relaunch python.
*Recommendation:* wrap the pane in `while true; do uv run python -m
ingestion.aisstream >> logs/ingestion.log 2>&1; sleep 5; done`, or a `systemd
--user` unit with `Restart=on-failure`. Near-zero effort, removes the one survival
gap tmux leaves.

### B3 · Disk growth — **non-issue** (dissolved on inspection)
`/srv/data` is a dedicated 458 GB NVMe partition, **430 GB free (2% used)**. DB is
**4.7 GB** total (ais_fixes 3.81 GB / vessel_state 887 MB) — almost entirely the
old-regime backlog at ~181 bytes/row. New-regime growth is ~9k fixes/day ≈
**~2–3 MB/day**, so even a year of parking adds ~1 GB. No compression policy and no
chunk retention exist (0/85 chunks compressed) — and **none is needed** at this
horizon. No action.

### B4 · Backup — **High**, fix before parking
No backup automation exists (no `pg_dump`/`pg_basebackup`/cron in the repo or
crontab). The only copy of the data is the `/srv/data/tanker_db` volume, and
`make reset` is `sudo rm -rf /srv/data/tanker_db`. The accruing corpus is the
entire reason for parking; a disk failure or one stray command erases weeks of
unattended collection.
*Recommendation:* a weekly (or daily) `pg_dump` to a separate disk/location, e.g.
a cron running `docker exec tanker_db pg_dump -U tanker_user -Fc tanker_flow`.
Even weekly is a large risk reduction.

### B5 · HUD can't see a partial outage — **Medium**, recommend a tweak
`/api/ingest-status` (`app.py:190`) returns a single aggregate `last_bucket_age_s`
= `MAX(bucket)` over all `aisstream%` sources, and `hud.js` shows green if that age
< 180 s. If one of the three connections dies but the others flow, the aggregate
stays fresh and the dashboard stays green — the partial outage is invisible on the
surface you actually watch (per-source liveness is TUI-only). Since the scan
rotation (chunk 2) is a distinct connection, losing it would silently stop
discovery/rotation while the HUD reads "live."
*Recommendation:* have `/api/ingest-status` return per-source ages and let the HUD
flag the worst one (e.g. "2/3 live · mmsi-3 stale 12m").

---

## Track C — Architecture quality (lightest)

- **C1 · Tests & CI.** **150 tests pass in 0.19s**; pure-logic layers
  (state_machine, legs, visits, signal, scoring, laden, dest_parser, vf_rescue) are
  well covered. **Untested:** `aisstream.py`, `metrics.py`, `vesselfinder.py`, and
  `port_events.py` *orchestration* (the pure pieces it calls are tested). **No CI**
  (`.github/workflows` absent). For a project you'll return to cold in months,
  recommend (not build): one end-to-end smoke test
  (`port_events`→`legs`→`visits`→`signal` on a tiny fixture DB) + a minimal GitHub
  Actions workflow running `ruff` + `pytest`. Safety net for future-you.
- **C2 · Schema/migration integrity — clean.** Alembic at a **single head**
  (`f1a8c3d5e7b9`) and `alembic current == head` (DB stamped correctly); 25
  migration files; `db/init/schema.sql` is the documented `make reset` source of
  truth. *Limitation:* I did not run a full schema.sql-vs-migrations structural
  diff (it would need a throwaway DB); the single-clean-head + green tests +
  healthy live ingester are strong indirect evidence of consistency.
- **C3 · Hygiene.** Production **ruff clean**. Largest modules `tui.py` (1559),
  `vf_rescue.py` (1078), `app.py` (1029) — naturally complex UI/credit-budget code;
  note as future-refactor candidates, no action. CLAUDE.md is broadly accurate
  (the lazysql→sqlit note is already tracked).

---

## Park-readiness checklist

Before stepping away:

- [x] **B1 — DONE (2026-06-02).** `flush_buffers` now re-queues a failed batch
  ahead of newly-arrived data instead of dropping it (`aisstream.py`, all three
  data buffers; idempotent inserts make replay safe). ruff clean, 155 tests pass.
- [x] **B4 — DONE (2026-06-02).** `scripts/backup_db.sh` + `make backup` dump the
  DB (custom format) to `/home/alec/backups/tanker-flow` — on the `/` device, off
  the `nvme1n1` DB volume — keeping the newest 14. Installed as a **daily 04:00
  cron** (verified under a cron-like minimal env). First backups taken & validated
  with `pg_restore -l` (1276 TOC entries, all core tables present).
- [ ] **B2** (optional, ~2 min) — wrap the ingest tmux pane in a restart loop.
- [ ] **B5** (optional) — surface per-source liveness on the HUD so a partial
  outage is visible on the dashboard you watch.

Safe to leave as-is: signal logic, architecture, disk, the background loops, the
DB container (`restart: unless-stopped`).

## What to validate as data accrues (the return-to-project list)

1. **Phantom rate under the new regime.** It is 0% today only because no leg has
   aged out yet. Re-run A3 after a few weeks: the open-leg pin should keep it low.
   A rising new-regime `open_censored` share signals tier-decay dropping vessels
   pre-arrival.
2. **`OD_WINDOW_DAYS` → rolling medians (Phase 2).** Once enough closed legs exist
   per O-D, replace the constant (the one sensitive knob, A4) with observed median
   durations from `legs.py`.
3. **EIA capture ratio + stability (build `data/eia.py`).** As the new-regime
   corpus spans months, track the capture ratio vs EIA monthly — the real external
   validation, and the leakage-free `basis='knowable'` series this enables.
4. **Stale-close trend** — should stay ~0% under the new regime; a rise means
   coverage is slipping.

---

## Reproducibility appendix

All numbers above come from read-only queries against the live DB (`docker exec
tanker_db psql -U tanker_user -d tanker_flow`) and from `analysis/scratch_audit.py`
(THROWAWAY, marked as such — loads leg/visit/signal inputs once and re-runs the
pure `pair_legs`/`pair_visits`/`accumulate_daily` functions in-memory under varied
constants; writes nothing). Run it with `PYTHONPATH=. uv run python
analysis/scratch_audit.py`.

- A1/A2: `port_events` grouped by `event_type`/`regime`; generated-column vs
  cutover check; `ais_fixes` per-day-by-source; `signal_daily` regime coverage.
- A3/A4/A5: `analysis/scratch_audit.py` (status distribution, censoring on/off
  stock, OD/censor/recent/visit-ceiling sweeps, null-gas + unknown-band counts).
- A6: `scratch_audit.py` DFA section (FSRU + tie classification); SQL `lag()` scan
  of the event before each usgulf synthetic `zone_exit`; regime split of synthetic
  exits.
- A7: SQL throughput (laden US-export departures × `gas_capacity_m3`); EIA series
  N9133US2 via `eia.gov/dnav/ng/hist/n9133us2m.htm`.
- B1/B2/B5: `ingestion/aisstream.py:417–487`, `:641–680`, `:682–738`;
  `viz/app.py:190`; `viz/static/js/hud.js`.
- B3/B4: `df -h /srv/data`; `pg_database_size`/`hypertable_size`;
  `timescaledb_information.chunks`; repo + crontab grep for backup; `docker-compose.yml`.
- C1/C2/C3: `uv run pytest -q`; `uv run ruff check .`; `uv run alembic heads`/
  `current`; module `wc -l`.

Snapshot captured 2026-06-02 ~16:46 UTC. `port_events` 1595 rows
(2026-04-14 → 06-02 16:37); `ais_fixes` 21,958,922 rows (live to 16:42);
`signal_daily` 846 rows (→ 06-02); in-scope fleet 780.
