.PHONY: run stop clean build test test-resume test-kill worker submit logs ps

## run — start EtchFlow + Postgres
run:
	docker compose up --build -d
	@echo "EtchFlow: http://localhost:8080"

## stop — stop containers
stop:
	docker compose down

## clean — stop and wipe data
clean:
	docker compose down -v

## build — compile Go
build:
	go build -o ./bin/etchflow ./cmd/server

## test — run demo (fresh execution)
test:
	@python examples/demo.py

## test-resume — resume from last run
test-resume:
	@python examples/demo.py --resume

## test-kill — run kill test (crash recovery)
test-kill:
	@chmod +x scripts/kill-test.sh && ./scripts/kill-test.sh

## worker — start worker (batch processing)
worker:
	@python examples/worker_demo.py

## submit — submit jobs to queue
submit:
	@python examples/submit_job.py --count 3

## logs — tail EtchFlow logs
logs:
	docker compose logs -f etchflow

## ps — show containers
ps:
	docker compose ps