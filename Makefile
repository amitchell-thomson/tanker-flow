.PHONY: up down db-ui psql logs reset seed-terminals seed-zones seed-unlocodes viz ingest enrich port-events scoring vf-rescue vf-rescue-dry vf-status refresh-fleet

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

seed-unlocodes:
	docker exec -i tanker_db psql -U tanker_user -d tanker_flow < db/seed/terminal_unlocodes.sql

scoring:
	uv run python -m pipeline.scoring

# VesselFinder rescue: fetch live positions for high-value AIS-silent vessels.
# Credit-budgeted. Use vf-rescue-dry first for a no-spend candidate/cost preview.
vf-rescue:
	uv run python -m ingestion.vf_rescue

vf-rescue-dry:
	uv run python -m ingestion.vf_rescue --dry-run

# Fetch + store the VF account balance (free /status call).
vf-status:
	uv run python -m ingestion.vf_rescue --status

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
