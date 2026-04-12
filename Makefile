.PHONY: up down psql logs reset

up:
	docker compose up -d

down:
	docker compose down

ui:
	lazysql "postgres://tanker_user:$(shell grep DB_PASSWORD .env | cut -d= -f2)@localhost:5432/tanker_flow?sslmode=disable"

psql:
	docker exec -it tanker_db psql -U tanker_user -d tanker_flow

logs:
	docker compose logs -f timescaledb

reset:
	@echo "WARNING: This will delete all data. Ctrl+C to cancel."
	@sleep 5
	docker compose down -v && docker compose up -d
