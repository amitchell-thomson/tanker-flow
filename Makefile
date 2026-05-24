.PHONY: up down psql logs reset viz

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

viz:
	uv run uvicorn viz.app:app --host 127.0.0.1 --port 8000 --reload
