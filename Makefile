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
## Requires: `make run` first, and Python deps installed in python_adapter/
kill-test:
	@echo ""
	@echo "=== EtchFlow Kill Test ==="
	@echo ""
	@echo ">>> Starting 8-node graph (nodes sleep 5s each)..."
	@cd python_adapter && python example_graph.py & echo $$! > /tmp/etchflow_pid
	@echo ">>> Graph running (PID=$$(cat /tmp/etchflow_pid))"
	@echo ">>> Waiting 18s (nodes 1-3 complete, node 4 mid-execution)..."
	@sleep 18
	@echo ""
	@echo ">>> Killing Python process..."
	@kill -9 $$(cat /tmp/etchflow_pid) 2>/dev/null || true
	@sleep 1
	@echo ""
	@echo ">>> Verifying EtchFlow is still alive..."
	@curl -sf http://localhost:8080/health | python3 -m json.tool && echo "✅ EtchFlow alive"
	@echo ""
	@echo ">>> Last committed checkpoint:"
	@curl -sf http://localhost:8080/runs/$$(cat /tmp/etchflow_run_id)/state | python3 -m json.tool
	@echo ""
	@echo ">>> Resuming run from last checkpoint..."
	@cd python_adapter && python example_graph.py --resume $$(cat /tmp/etchflow_run_id)
	@echo ""
	@echo "=== Kill Test Complete ==="
