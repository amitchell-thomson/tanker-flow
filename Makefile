.PHONY: up down db-ui psql logs reset seed-terminals seed-zones seed-unlocodes viz ingest enrich port-events backfill-noaa scoring signals vf-rescue vf-rescue-dry vf-status refresh-fleet discover discover-dry discover-berths discover-berths-dry backup eia eia-full capture-rate coverage

up:
	docker compose up -d

down:
	docker compose down

db-ui:
	PYTHON_KEYRING_BACKEND=keyrings.alt.file.PlaintextKeyring sqlit -c tanker-flow

psql:
	docker exec -it tanker_db psql -U tanker_user -d tanker_flow

logs:
	docker compose logs -f timescaledb

# Dump the DB (custom format) to a directory on a different disk than /srv/data,
# keeping the newest 14. Run daily via cron for unattended protection. Restore:
# pg_restore -U tanker_user -d tanker_flow --clean <dump>
backup:
	bash scripts/backup_db.sh

reset:
	@echo "WARNING: This will delete all data. Ctrl+C to cancel."
	@sleep 5
	docker compose down
	sudo rm -rf /srv/data/tanker_db
	docker compose up -d

seed-terminals:
	docker exec -i tanker_db psql -U tanker_user -d tanker_flow < db/seed/terminals.sql

seed-zones:
	PYTHONPATH=. uv run python db/seed/import_terminal_zones.py

ingest:
	@mkdir -p logs
	@uv run python -m ingestion.aisstream >> logs/ingestion.log 2>&1 & \
	INGEST_PID=$$!; \
	uv run python -m viz.tui; \
	kill $$INGEST_PID 2>/dev/null || true

enrich:
	uv run python -m ingestion.vesselfinder --terminal-only

viz:
	uv run uvicorn viz.app:app --host 127.0.0.1 --port 8000 --reload

port-events:
	uv run python -m pipeline.port_events

# Historical NOAA US AIS backfill (two-tier: tanker Parquet archive + LNG-in-buffer
# into ais_fixes). Bounded-concurrent download, one-at-a-time on disk. RUN NATIVELY
# (not via the agent sandbox, which throttles + breaks parallel TLS).
# Usage: make backfill-noaa START=2022-01-01 END=2022-12-31 [N=6]
backfill-noaa:
	uv run python -m ingestion.historical.noaa_ais --start $(START) --end $(END) --concurrency $(or $(N),6)

seed-unlocodes:
	docker exec -i tanker_db psql -U tanker_user -d tanker_flow < db/seed/terminal_unlocodes.sql

scoring:
	uv run python -m pipeline.scoring

# Rebuild the signal_daily panel (laden ton-miles in transit + flow signals)
# from voyage legs + port_events. Idempotent: TRUNCATEs then rebuilds.
signals:
	uv run python -m pipeline.signal

# VesselFinder rescue: fetch live positions for high-value AIS-silent vessels.
# Credit-budgeted. Use vf-rescue-dry first for a no-spend candidate/cost preview.
vf-rescue:
	uv run python -m ingestion.vf_rescue

vf-rescue-dry:
	uv run python -m ingestion.vf_rescue --dry-run

# Fetch + store the VF account balance (free /status call).
vf-status:
	uv run python -m ingestion.vf_rescue --status

# EIA ground-truth + fundamentals loader (data/eia.py). Idempotent upsert into
# eia_series. `eia` is the incremental refresh of the active series set (monthly
# US LNG exports — the capture-rate ground truth); `eia-full` backfills history
# on a fresh DB. Verify v2 routes first with `python -m data.eia --probe lng_exports`.
eia:
	uv run python -m data.eia

eia-full:
	uv run python -m data.eia --full

# Read-only capture-rate report: captured US LNG-export departures vs the
# EIA-implied cargo count per month (needs `make eia` first). Lands dark until
# the first wholly-post-cutover month (June 2026) is published + revised.
capture-rate:
	uv run python -m data.capture_rate

# Read-only coverage panel: in-scope fleet bucketed live/stale/blind, watchlist
# tiers vs subscribed slots, cold-start mooring rate, unmet rescue demand. The
# interim residual-miss measurement (analysis/DATA_QUALITY.md §1) until the EIA
# capture-rate metric firms.
coverage:
	uv run python -m data.coverage

# Periodic refresh of the global LNG/FSRU fleet from the IGU report.
# Step 1 (manual): download the latest "IGU World LNG Report" PDF from
# https://www.igu.org/igu-reports and save it to
# db/seed/igu-world-lng-report-latest.pdf.
# Step 2: run this target. It re-parses the appendix, regenerates the
# canonical CSV, and incrementally imports any new IMOs via VF.
refresh-fleet:
	uv run --with pypdf python scripts/parse_igu_fleet.py \
	  --pdf db/seed/igu-world-lng-report-latest.pdf \
	  --out db/seed/lng_fleet_igu_2025.csv
	uv run python scripts/import_igu_fleet.py

# Budgeted daily newbuild discovery: resolve orderbook hulls that have since been
# delivered (IMO->MMSI via VF) into vessel_registry. Misses are free; catches
# spend only glide-surplus credits, subordinate to vf-rescue (see the script).
discover:
	uv run python scripts/discover_newbuilds.py

discover-dry:
	uv run python scripts/discover_newbuilds.py --dry-run

# Phase-2 berth auto-add: VF-enrich unknown tankers the bbox catch-all caught
# sitting in an LNG berth, and register the ones VF confirms are LNG carriers.
# Each candidate is checked at most once (negative-cached). discover-berths-dry
# previews candidates + cost with no VF spend.
discover-berths:
	uv run python scripts/discover_berth_tankers.py

discover-berths-dry:
	uv run python scripts/discover_berth_tankers.py --dry-run
