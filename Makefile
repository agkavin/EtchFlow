.PHONY: run stop clean build kill-test logs

## run — build and start EtchFlow + Postgres in Docker (detached)
run:
	docker compose up --build -d
	@echo "EtchFlow running at http://localhost:8080"
	@echo "Check logs: make logs"

## stop — stop containers (preserves Postgres data volume)
stop:
	docker compose down

## clean — stop containers AND wipe Postgres data volume (re-runs migrations on next `make run`)
clean:
	docker compose down -v
	@echo "Postgres data volume wiped. Migrations will re-run on next 'make run'."

## build — compile Go binary locally (requires Go 1.22+)
build:
	go build -o ./bin/etchflow ./cmd/server

## logs — tail EtchFlow service logs
logs:
	docker compose logs -f etchflow

## kill-test — run the full Kill Test scenario automatically
## Requires: `make run` first, and Python deps installed.
kill-test:
	@./scripts/kill-test.sh
