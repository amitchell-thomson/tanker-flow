# Runbook: bring up the second-egress ingester (Stage 3b/3c)

**2026-06-11.** How to take the Stage-3a sharding code (already landed,
zero-regression at `WORKER_COUNT=1`) live as a real second worker behind a second
egress IP, lifting the AISstream cap from 3 → 6 connections (≈150 → ≈300 slots).

Topology decided (see the plan + `project_second_ip_tailscale` memory): **Oracle
Cloud Always Free ARM VM + Tailscale**, £0 recurring. Worker-1's AISstream traffic
egresses Oracle's public IP; its DB writes go to the home TimescaleDB over the
tailnet. This **supersedes** the WireGuard-sidecar / Mullvad options in
`design-2026-06-08-second-ip-ingester.md`.

The code is done; everything below is the **manual provisioning** that code can't
do, plus the validation gates.

---

## 0. Prerequisites
- The Stage-3a code is merged and the `slot_worker` migration applied (`alembic
  upgrade head`).
- Home worker-0 is running normally (`make ingest`, `WORKER_COUNT=1`).

## 1. Oracle Cloud Always Free VM
1. Create an Oracle Cloud account; in the **same region as the home WAN** (London
   for the UK home IP — keeps the tailnet RTT low). Enable Pay-As-You-Go (no
   charge for Always-Free resources; it just unlocks ARM Ampere capacity).
2. Launch an **Ampere A1 (ARM)** VM, Ubuntu LTS, with a **reserved public IP**
   (Always-Free includes one — this is the stable second egress).
3. Security list / firewall: allow outbound 443 (AISstream + Tailscale control).
   You do **not** need any inbound public ports — Tailscale dials out.

## 2. Tailscale mesh (the connection)
Install on **both** boxes, same tailnet:
```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up            # authenticate both to the same account
tailscale ip -4              # note each node's 100.x.y.z address
```
- Home node → e.g. `100.64.0.1`; Oracle node → e.g. `100.64.0.2`.
- **No home-router port-forward is needed** — Tailscale does NAT traversal, so
  neither side needs a public inbound port or a static IP.
- **Do NOT** make worker-1 an exit node or `--accept-routes` a default route.
  Tailscale then routes *only* `100.64.0.0/10` through the tunnel, so worker-1's
  AISstream traffic keeps using Oracle's public IP (the new egress) while only DB
  writes traverse the tailnet — the split is automatic, no custom routing.

## 3. Lock down the home DB (three independent locks)
The DB must be reachable from the Oracle node over the tailnet **only**:
1. **Bind** the published port to the home Tailscale IP, not `0.0.0.0`. In the
   home `docker-compose.yml`, set the DB ports mapping to
   `100.64.0.1:5432:5432` (not `5432:5432`). Recreate the container.
2. **Tailscale ACL** (admin console → Access Controls): allow only the Oracle
   node to reach the home node's `:5432`; deny everything else.
3. **pg_hba**: add a line allowing `100.64.0.0/10` with `scram-sha-256`, so the
   existing `DB_PASSWORD` still authenticates every tailnet connection.

Verify from the Oracle VM:
```bash
psql "host=100.64.0.1 port=5432 dbname=tanker_flow user=tanker_user" -c "select 1"
```

## 4. Confirm the second egress lifts the cap (Stage-3b gate)
From the **Oracle VM**:
```bash
uv run python scripts/aisstream_conn_test.py --ramp 5
```
Expect conns 1–3 OK / 4th → 429 — an *independent* bucket from the home IP's 3.
(That independence is the whole point of the second IP.)

## 5. Throttle probe — the Stage-3c gate
Still on the Oracle VM (it has its own connection budget; this can't run from home
where the 3 slots are full):
```bash
uv run python scripts/aisstream_bbox_probe.py --minutes 20
```
Read the gap stats:
- **Steady cadence, low `gap >300s` %** ⇒ the small terminal-box catch-all stays
  under the throttle → **deploy 3c** (add one bbox-only connection on worker-1;
  source-labelled `aisstream-bbox`, additive, dedup by `(mmsi, fix_ts)`).
- **Sparse / high gap %** ⇒ the throttle still drops vessels → **abandon 3c** and
  give all 3 of worker-1's conns to pure-MMSI sharding (still doubles slots).

Report the number either way — do not deploy 3c on a hunch.

## 6. Deploy worker-1
On the Oracle VM:
```bash
git clone <repo> ~/tanker-flow && cd ~/tanker-flow
uv sync
cp deploy/worker.env.example .env       # then fill in DB_PASSWORD / keys /
                                        # DB_HOST=<home tailscale IP>
sudo cp deploy/tanker-ingester-worker.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now tanker-ingester-worker
journalctl -u tanker-ingester-worker -f   # expect "worker 1/2 · run_scoring=False …"
```
Worker-1 runs **pure ingestion** — no scoring/port_events/VF (it would refuse to
start with `RUN_VF_RESCUE=true`).

## 7. Flip the home worker to WORKER_COUNT=2
On the home box, set `WORKER_COUNT=2` in `.env` (keep `WORKER_ID=0`, `RUN_*`
default true), then restart `make ingest`. Now:
- worker-0 holds the **even-MMSI** half (3 conns), runs all singletons.
- worker-1 holds the **odd-MMSI** half (3 conns), pure ingestion.
- Union ≈ 300 slots, disjoint by construction.

## 8. Verify
- **All 6 sources fresh** — per-source liveness should show
  `aisstream-w0-{1,2,3}` and `aisstream-w1-{1,2,3}` all reporting. (The TUI's
  per-source strip still assumes the 3 `aisstream-mmsi-*` labels; generalising it
  to the worker-suffixed labels is the one remaining observability task — until
  then, check liveness via `ingestion_stats_minute`.)
- **Disjoint partition** — no MMSI in both workers' slots:
  ```sql
  SELECT count(*) FROM priority_watchlist
  WHERE in_slot AND slot_worker IS NULL;          -- expect 0 once both have cycled
  SELECT slot_worker, count(*) FROM priority_watchlist
  WHERE in_slot GROUP BY 1;                        -- ~even split across 0 and 1
  ```
- **Singletons on worker-0 only** — `vf_account_status` / scoring logs come from
  home only; `vf_rescue_log` shows no double-spend.
- Re-run `make coverage`: the blind fraction should fall and the tier-5 dark set
  roughly halve.

## 9. Rollback
Set `WORKER_COUNT=1` on the home box + restart `make ingest`; stop
`tanker-ingester-worker` on the VM. The home worker reverts to the exact
single-worker behaviour (the partition becomes a no-op). No schema rollback
needed — `slot_worker` is harmless when unused.

## Failure modes (accept for v1, monitor)
- **Tailscale drop** → worker-1's DB writes stall, its half goes dark until the
  tunnel heals; the watchdog/systemd restart retries. Caught by the "all 6 fresh"
  check.
- **Worker-1 death** → its odd-MMSI partition goes dark (not redistributed).
  Monitor + alert; dynamic re-partition is a v2 nicety.
- **Oracle reclaims an idle ARM VM** → ours is always-on (not idle); the systemd
  unit also keeps it busy.
