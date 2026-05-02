# EtchFlow — Phase MVP Implementation Plan

> **Goal:** Prove the Kill Test end-to-end. Kill a Python LangGraph process mid-execution. Restart it. It resumes exactly where it stopped.

> **No worker pool. No reaper. No retry. No heartbeat.** Just atomic checkpoint + state recovery.

---

## ⚠️ Critical Finding: LangGraph API Mismatch

The PRD's Python adapter code uses an **outdated** LangGraph checkpoint API. The real `BaseCheckpointSaver` interface (2025) requires:

| PRD says | Actual API |
|---|---|
| `get(config)` → `Checkpoint` | `get_tuple(config)` → `CheckpointTuple` |
| `put(config, checkpoint, metadata)` | `put(config, checkpoint, metadata, new_versions)` → `RunnableConfig` |
| _(missing)_ | `put_writes(config, writes, task_id)` → `None` |
| `list(config)` | `list(config, *, filter, before, limit)` → `Iterator[CheckpointTuple]` |

`CheckpointTuple` is a named tuple: `(config, checkpoint, metadata, parent_config, pending_writes)`

**Impact on MVP:** The `EtchFlowCheckpointSaver` must implement the real interface. `put_writes` can be a no-op for MVP (we don't need pending writes for crash recovery). `get_tuple` must return a proper `CheckpointTuple`, not a raw dict.

---

## MVP Scope — What's IN and What's OUT

### ✅ IN (MVP)

- 3 SQL migrations (runs, checkpoints, agent_logs)
- Go service with 4 endpoints: `POST /runs`, `PUT /runs/{id}/checkpoint`, `GET /runs/{id}/state`, `GET /health`
- State machine: `PENDING → RUNNING → SUCCESS | FAILED` only
- Atomic checkpoint: `INSERT ... ON CONFLICT DO NOTHING` + `UPDATE current_state`
- Python adapter: `EtchFlowClient`, `EtchFlowCheckpointSaver` (real API), `graph_serializer`
- 8-node demo graph with `--resume` flag
- Docker Compose: EtchFlow Go service + PostgreSQL 16
- Makefile with `make run`, `make kill-test`
- **No fake data, no hardcoded state, no mock LLM calls** — real sleep-based nodes simulating LLM latency

### ❌ OUT (Phase 1.5+)

- Worker pool / Run Activators
- Reaper goroutine
- Heartbeat system
- Retry engine / exponential backoff
- `POST /runs/{id}/fail`, `POST /runs/{id}/cancel`
- `GET /runs/{id}`, `GET /runs/{id}/checkpoints`, `GET /runs/{id}/logs`, `GET /ready`
- RETRYING, DEAD, CANCELLED, TIMEOUT states

---

## MVP Simplification: No Activator = Python Sets RUNNING

Since MVP has **no worker pool**, Python must transition the run from `PENDING → RUNNING` itself. The flow becomes:

```
Python: POST /runs                → EtchFlow inserts PENDING, returns run_id
Python: POST /runs/{id}/start     → EtchFlow sets RUNNING (NEW MVP-ONLY ENDPOINT)
        OR
Python: PUT /runs/{id}/checkpoint → first checkpoint auto-transitions PENDING→RUNNING
```

**Decision: Auto-transition on first checkpoint.** When EtchFlow receives the first `PUT /checkpoint` for a run that is still `PENDING`, it atomically sets `status = RUNNING` in the same transaction. No extra endpoint needed. Clean and honest.

---

## File Structure (MVP Only)

```
etchflow/
│
├── cmd/
│   └── server/
│       └── main.go                  # Entry point. Wires config → DB pool → stores → router → server.
│
├── internal/
│   ├── api/
│   │   ├── handler/
│   │   │   ├── runs.go              # POST /runs
│   │   │   ├── checkpoint.go        # PUT /runs/{id}/checkpoint
│   │   │   ├── state.go             # GET /runs/{id}/state
│   │   │   └── health.go            # GET /health
│   │   ├── middleware/
│   │   │   ├── logging.go           # Structured request logging (zap)
│   │   │   └── recovery.go          # Panic recovery → 500 JSON
│   │   └── router.go                # chi router, all routes registered here
│   │
│   ├── store/
│   │   ├── run_store.go             # CreateRun, GetRun, UpdateStatus
│   │   ├── checkpoint_store.go      # SaveCheckpoint (atomic INSERT + UPDATE)
│   │   └── log_store.go             # AppendLog (append-only writes)
│   │
│   ├── statemachine/
│   │   └── transitions.go           # MVP transitions only: PENDING→RUNNING→SUCCESS|FAILED
│   │
│   ├── models/
│   │   ├── run.go                   # Run struct + status constants
│   │   ├── checkpoint.go            # Checkpoint struct
│   │   └── log.go                   # AgentLog struct + event type constants
│   │
│   └── config/
│       └── config.go                # Viper loader. DATABASE_URL required, HTTP_PORT, LOG_LEVEL.
│
├── migrations/
│   ├── 001_create_runs.up.sql
│   ├── 001_create_runs.down.sql
│   ├── 002_create_checkpoints.up.sql
│   ├── 002_create_checkpoints.down.sql
│   ├── 003_create_agent_logs.up.sql
│   └── 003_create_agent_logs.down.sql
│
├── python_adapter/
│   ├── etchflow_client.py           # httpx wrapper: submit_run, get_state, save_checkpoint
│   ├── etchflow_checkpoint_saver.py # Real BaseCheckpointSaver: get_tuple, put, put_writes, list
│   ├── graph_serializer.py          # Extracts DAG metadata from StateGraph
│   └── example_graph.py             # 8-node graph with --resume flag
│
├── docker/
│   ├── Dockerfile                   # Multi-stage: golang:1.22-alpine → distroless/static
│   └── docker-compose.yml           # EtchFlow + PostgreSQL 16
│
├── .env.example
├── go.mod
├── go.sum
├── Makefile
└── README.md
```

---

## Database Migrations (MVP)

### Migration 001 — `runs` table (MVP-slim)

MVP strips out all Phase 1.5 columns (worker_id, heartbeat, retry fields). Only what crash recovery needs.

```sql
-- 001_create_runs.up.sql
CREATE TABLE IF NOT EXISTS runs (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    graph_definition    JSONB       NOT NULL,
    input_data          JSONB       NOT NULL,
    current_state       JSONB,
    status              TEXT        NOT NULL DEFAULT 'PENDING',
    last_node_completed TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_runs_status ON runs (status);
```

```sql
-- 001_create_runs.down.sql
DROP TABLE IF EXISTS runs CASCADE;
```

**Why no worker_id, heartbeat, retry columns?** MVP has no activators, no reaper, no retry. Adding unused columns creates confusion. Phase 1.5 migration will `ALTER TABLE` to add them.

---

### Migration 002 — `checkpoints` table

```sql
-- 002_create_checkpoints.up.sql
CREATE TABLE IF NOT EXISTS checkpoints (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id      UUID        NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    node_name   TEXT        NOT NULL,
    state_json  JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_checkpoint_run_node UNIQUE (run_id, node_name)
);

CREATE INDEX idx_checkpoints_run_id ON checkpoints (run_id, created_at ASC);
```

```sql
-- 002_create_checkpoints.down.sql
DROP TABLE IF EXISTS checkpoints CASCADE;
```

---

### Migration 003 — `agent_logs` table

```sql
-- 003_create_agent_logs.up.sql
CREATE TABLE IF NOT EXISTS agent_logs (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id      UUID        NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    node_name   TEXT,
    event_type  TEXT        NOT NULL,
    message     TEXT,
    metadata    JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_agent_logs_run_id ON agent_logs (run_id, created_at ASC);
```

```sql
-- 003_create_agent_logs.down.sql
DROP TABLE IF EXISTS agent_logs CASCADE;
```

---

## Go Dependencies (go.mod)

```
module github.com/marcus/etchflow

go 1.22

require (
    github.com/go-chi/chi/v5      v5.1.0
    github.com/jackc/pgx/v5        v5.7.1
    github.com/spf13/viper         v1.19.0
    go.uber.org/zap                v1.27.0
    github.com/google/uuid         v1.6.0
)
```

No `golang-migrate` library for MVP — migrations run via `docker-entrypoint-initdb.d` volume mount (Postgres auto-executes `.sql` files on first boot). Simple and zero dependencies.

---

## Environment Config (MVP)

```bash
# .env.example — MVP only needs these
DATABASE_URL=postgres://etchflow:etchflow@localhost:5432/etchflow?sslmode=disable
DB_POOL_MAX_CONNS=10
HTTP_PORT=8080
LOG_LEVEL=info
LOG_FORMAT=console
```

---

## Part 2: Go Service Architecture

### Entry Point — `cmd/server/main.go`

Wiring order:
1. Load config via viper (fail fast if `DATABASE_URL` missing)
2. Initialize zap logger
3. Create `pgxpool.Pool` with `DB_POOL_MAX_CONNS`
4. Run migrations check (verify tables exist via a simple SELECT — not auto-migrate)
5. Create store instances (`RunStore`, `CheckpointStore`, `LogStore`)
6. Create handler instances (inject stores + logger)
7. Build chi router via `router.go`
8. Start HTTP server on `HTTP_PORT`
9. Graceful shutdown: listen for SIGINT/SIGTERM, close pool, drain connections

---

### State Machine — `internal/statemachine/transitions.go`

MVP-only valid transitions:

```
PENDING  → RUNNING     (first checkpoint auto-transitions)
RUNNING  → SUCCESS     (final node checkpointed)
RUNNING  → FAILED      (explicit failure — future use)
```

Implementation: a `map[string][]string` of valid `from → []to` states. A single function:

```go
func IsValidTransition(from, to string) bool
```

Any attempt to transition outside this map is rejected with an error. This is the safety net — no code path can put a run into an invalid state.

---

### Store Layer — `internal/store/`

#### `run_store.go`

| Method | SQL | Notes |
|---|---|---|
| `CreateRun(ctx, graphDef, inputData) → Run` | `INSERT INTO runs (graph_definition, input_data) VALUES ($1, $2) RETURNING *` | Status defaults to PENDING |
| `GetRun(ctx, id) → Run, error` | `SELECT * FROM runs WHERE id = $1` | Returns `ErrNotFound` if missing |
| `UpdateStatus(ctx, id, newStatus) → error` | `UPDATE runs SET status = $1, updated_at = NOW() WHERE id = $2` | Validates transition via state machine before executing |
| `SetRunning(ctx, id) → error` | `UPDATE runs SET status = 'RUNNING', started_at = NOW(), updated_at = NOW() WHERE id = $1 AND status = 'PENDING'` | Only transitions from PENDING. Returns error if already RUNNING (idempotent guard). |
| `SetSuccess(ctx, id) → error` | `UPDATE runs SET status = 'SUCCESS', completed_at = NOW(), updated_at = NOW() WHERE id = $1 AND status = 'RUNNING'` | Called when finish_point node is checkpointed |
| `UpdateCurrentState(ctx, id, stateJSON, lastNode) → error` | `UPDATE runs SET current_state = $1, last_node_completed = $2, updated_at = NOW() WHERE id = $3` | Called atomically inside checkpoint transaction |

#### `checkpoint_store.go`

| Method | SQL | Notes |
|---|---|---|
| `SaveCheckpoint(ctx, tx, runID, nodeName, stateJSON) → (created bool, err)` | `INSERT INTO checkpoints (run_id, node_name, state_json) VALUES ($1, $2, $3) ON CONFLICT ON CONSTRAINT uq_checkpoint_run_node DO NOTHING RETURNING id` | Returns `created=true` if inserted, `false` if conflict (idempotent). Uses the transaction passed in. |

**The critical atomicity:** `SaveCheckpoint` is called inside a transaction that also calls `UpdateCurrentState`. Both succeed or both fail. This is the core durability guarantee.

```go
// Pseudocode for the atomic checkpoint flow
func (s *Store) AtomicCheckpoint(ctx context.Context, runID uuid.UUID, nodeName string, stateJSON []byte, isFinishNode bool) error {
    tx, _ := s.pool.Begin(ctx)
    defer tx.Rollback(ctx)

    // 1. Insert checkpoint (idempotent)
    created, _ := s.checkpoint.SaveCheckpoint(ctx, tx, runID, nodeName, stateJSON)

    if created {
        // 2. Update run's current_state and last_node_completed
        tx.Exec(ctx, `UPDATE runs SET current_state = $1, last_node_completed = $2, updated_at = NOW() WHERE id = $3`,
            stateJSON, nodeName, runID)

        // 3. If run is still PENDING, auto-transition to RUNNING
        tx.Exec(ctx, `UPDATE runs SET status = 'RUNNING', started_at = COALESCE(started_at, NOW()), updated_at = NOW()
                       WHERE id = $1 AND status = 'PENDING'`, runID)

        // 4. If this is the finish node, set SUCCESS
        if isFinishNode {
            tx.Exec(ctx, `UPDATE runs SET status = 'SUCCESS', completed_at = NOW(), updated_at = NOW()
                           WHERE id = $1 AND status = 'RUNNING'`, runID)
        }
    }

    return tx.Commit(ctx)
}
```

#### `log_store.go`

| Method | SQL | Notes |
|---|---|---|
| `Append(ctx, runID, nodeName, eventType, message, metadata) → error` | `INSERT INTO agent_logs (run_id, node_name, event_type, message, metadata) VALUES (...)` | Append-only. Never updates. Fire-and-forget (log failure should not block checkpoint). |

---

### API Handlers — `internal/api/handler/`

#### `POST /runs` — `handler/runs.go`

```
Request:
{
  "graph_definition": { "nodes": [...], "edges": [...], "entry_point": "...", "finish_point": "..." },
  "input_data": { ... }
}

Logic:
1. Validate: graph_definition must have nodes, edges, entry_point, finish_point
2. Validate: input_data must not be empty
3. store.CreateRun(ctx, graphDef, inputData)
4. logStore.Append(ctx, run.ID, "", "SUBMITTED", "run created", nil)
5. Return 201: { "run_id": "...", "status": "PENDING", "created_at": "..." }

Errors:
- 400: missing/invalid fields
- 500: DB error
```

#### `PUT /runs/{id}/checkpoint` — `handler/checkpoint.go`

This is the **most important endpoint**. Everything revolves around this.

```
Request:
{
  "node_name": "analyse",
  "state": { ... }
}

Logic:
1. Parse run_id from URL path (UUID validation)
2. Parse node_name and state from body
3. Validate: node_name not empty, state not null
4. Fetch run: store.GetRun(ctx, runID)
5. Guard: run status must be PENDING or RUNNING (reject if SUCCESS/FAILED)
6. Determine isFinishNode: compare node_name == run.GraphDefinition.FinishPoint
7. Call store.AtomicCheckpoint(ctx, runID, nodeName, stateJSON, isFinishNode)
8. logStore.Append(ctx, runID, nodeName, "NODE_COMPLETED", "node: "+nodeName, nil)
9. If isFinishNode → logStore.Append(ctx, runID, "", "SUCCESS", "run completed", nil)
10. Return 200:
    - If run continues: { "continue": true, "halt_reason": null }
    - If run finished:  { "continue": false, "halt_reason": null }

Errors:
- 400: invalid UUID, missing fields
- 404: run not found
- 409: run is in terminal state (SUCCESS/FAILED)
- 500: DB error
```

**Idempotency:** If the same `(run_id, node_name)` checkpoint is sent twice, `ON CONFLICT DO NOTHING` silently ignores it. The response is still `{ continue: true }`. No double-counting, no corruption.

#### `GET /runs/{id}/state` — `handler/state.go`

```
Logic:
1. Parse run_id from URL path
2. Fetch run: store.GetRun(ctx, runID)
3. If run.CurrentState is NULL → return 404 (no checkpoint yet)
4. Return 200: {
     "run_id": "...",
     "last_node_completed": "...",
     "state": { ... },
     "checkpointed_at": "..."   (run.updated_at)
   }

Errors:
- 400: invalid UUID
- 404: run not found OR no checkpoint exists
```

#### `GET /health` — `handler/health.go`

```
Logic: Return 200: { "status": "ok", "version": "0.1.0-mvp" }
```

No database check — that's `/ready` (Phase 1.5).

---

### Router — `internal/api/router.go`

```go
func NewRouter(handlers *Handlers, logger *zap.Logger) chi.Router {
    r := chi.NewRouter()

    // Middleware
    r.Use(middleware.RequestLogging(logger))
    r.Use(middleware.Recovery(logger))
    r.Use(chiMiddleware.Timeout(30 * time.Second))

    // Routes
    r.Post("/runs", handlers.CreateRun)
    r.Put("/runs/{id}/checkpoint", handlers.SaveCheckpoint)
    r.Get("/runs/{id}/state", handlers.GetState)
    r.Get("/health", handlers.Health)

    return r
}
```

---

### Middleware

#### `logging.go`
Wraps each request with structured zap logging: method, path, status code, duration, request_id (generated UUID per request).

#### `recovery.go`
Catches panics in handlers, logs the stack trace, returns:
```json
{ "type": "internal-error", "title": "Internal Server Error", "status": 500, "detail": "An unexpected error occurred" }
```

---

### Config — `internal/config/config.go`

Uses viper. Loads from `.env` file + environment variables. Fails fast on startup if `DATABASE_URL` is missing.

```go
type Config struct {
    DatabaseURL    string // required
    DBPoolMaxConns int    // default: 10
    HTTPPort       int    // default: 8080
    LogLevel       string // default: "info"
    LogFormat      string // default: "console"
}
```

---

## Part 3: Python Adapter (SDK)

### `python_adapter/etchflow_client.py`

Thin HTTP wrapper around EtchFlow's REST API. Uses `httpx` (sync).

```python
class EtchFlowClient:
    def __init__(self, base_url: str = "http://localhost:8080", timeout: float = 30.0)
    def submit_run(self, graph, input_data: dict) -> str           # POST /runs → returns run_id
    def get_state(self, run_id: str) -> dict | None                # GET /runs/{id}/state → dict or None (404)
    def save_checkpoint(self, run_id: str, node_name: str, state: dict) -> dict  # PUT /runs/{id}/checkpoint
```

`submit_run` internally calls `graph_serializer.serialize_graph()` to extract DAG metadata before POSTing.

---

### `python_adapter/graph_serializer.py`

```python
def serialize_graph(graph: StateGraph) -> dict:
    """Extract DAG topology (nodes, edges, entry_point, finish_point) from a LangGraph StateGraph."""
```

Returns JSON-serializable dict. This is metadata only — no execution logic.

---

### `python_adapter/etchflow_checkpoint_saver.py` — The Real API

This is the critical integration point. Must implement the **actual** 2025 LangGraph `BaseCheckpointSaver` interface.

```python
from langgraph.checkpoint.base import BaseCheckpointSaver, CheckpointTuple
from langchain_core.runnables import RunnableConfig

class EtchFlowCheckpointSaver(BaseCheckpointSaver):
    """
    Routes LangGraph's checkpoint calls to EtchFlow's REST API.
    LangGraph calls these methods automatically — no manual invocation needed.
    """

    def __init__(self, client: EtchFlowClient, run_id: str):
        super().__init__()
        self.client = client
        self.run_id = run_id   # Bound to a specific run

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """
        Called by LangGraph on graph.invoke() start.
        Fetches last committed checkpoint from EtchFlow.
        If exists → LangGraph resumes from that point.
        If None  → LangGraph starts from entry_point.
        """
        data = self.client.get_state(self.run_id)
        if not data:
            return None

        # Build a CheckpointTuple from EtchFlow's response
        checkpoint = data["state"]  # The full graph state dict
        config_with_id = {
            "configurable": {
                "thread_id": self.run_id,
                "checkpoint_id": data.get("last_node_completed", ""),
            }
        }
        return CheckpointTuple(
            config=config_with_id,
            checkpoint=checkpoint,
            metadata={"source": "etchflow", "node": data.get("last_node_completed")},
            parent_config=None,
            pending_writes=[],
        )

    def put(self, config: RunnableConfig, checkpoint: dict,
            metadata: dict, new_versions: dict) -> RunnableConfig:
        """
        Called by LangGraph after every node completes.
        Sends state to EtchFlow for atomic persistence.
        """
        node_name = metadata.get("source", "unknown")

        # Send checkpoint to EtchFlow
        response = self.client.save_checkpoint(self.run_id, node_name, checkpoint)

        # If EtchFlow says halt (run cancelled, finished, etc.), raise to stop Python
        if not response.get("continue", True):
            halt = response.get("halt_reason", "unknown")
            raise RuntimeError(f"EtchFlow halted execution: {halt}")

        # Return config for LangGraph's internal tracking
        return {
            "configurable": {
                "thread_id": self.run_id,
                "checkpoint_id": node_name,
            }
        }

    def put_writes(self, config: RunnableConfig,
                   writes: list, task_id: str) -> None:
        """
        Stores intermediate pending writes. No-op for MVP.
        EtchFlow only persists committed node state, not intermediate writes.
        """
        pass

    def list(self, config: RunnableConfig, *,
             filter=None, before=None, limit=None):
        """
        Lists checkpoint history. Stub for MVP — yields nothing.
        Full implementation in Phase 1.5 via GET /runs/{id}/checkpoints.
        """
        return iter([])
```

**Why `run_id` is in the constructor, not extracted from `config["thread_id"]`:**
In MVP, the Python script already knows the run_id (it either just submitted it or received it via `--resume`). Binding it at construction avoids fragile config parsing and makes the flow explicit.

---

### `python_adapter/example_graph.py` — The 8-Node Demo

8 nodes, each sleeping 5 seconds to simulate LLM latency. Total ~40s for uninterrupted run.

```python
# Node names in execution order:
NODES = ["extract", "classify", "analyse", "summarise", "draft", "review", "format", "publish"]
```

Each node function:
1. Prints `[Node X/8: {name}] executing...`
2. Sleeps 5 seconds (simulating LLM call)
3. Adds its output to state
4. LangGraph auto-calls `saver.put()` → EtchFlow persists
5. Prints `[Node X/8: {name}] ✓ checkpointed`

**The `--resume` flag:**

```python
if "--resume" in sys.argv:
    run_id = sys.argv[sys.argv.index("--resume") + 1]
    print(f"Resuming run: {run_id}")
else:
    run_id = client.submit_run(graph=builder, input_data={"input": "Analyse Q3 report"})
    print(f"Run submitted: {run_id}")

saver = EtchFlowCheckpointSaver(client=client, run_id=run_id)
graph = builder.compile(checkpointer=saver)
result = graph.invoke({"input": "Analyse Q3 report"}, config={"configurable": {"thread_id": run_id}})
```

On resume:
- `saver.get_tuple()` returns last checkpoint from EtchFlow
- LangGraph skips already-completed nodes
- Execution resumes from the next uncompleted node

**No fake skip logic.** LangGraph handles this natively via its checkpoint system. We just provide the state — LangGraph does the rest.

---

## Part 4: Docker Setup

### `docker/Dockerfile`

Multi-stage build. Pure Go binary (CGO_ENABLED=0) → distroless/static.

```dockerfile
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o etchflow ./cmd/server

FROM gcr.io/distroless/static-debian12
COPY --from=builder /app/etchflow /etchflow
COPY --from=builder /app/migrations /migrations
EXPOSE 8080
ENTRYPOINT ["/etchflow"]
```

### `docker/docker-compose.yml`

```yaml
services:
  etchflow:
    build:
      context: ..
      dockerfile: docker/Dockerfile
    ports:
      - "8080:8080"
    environment:
      DATABASE_URL: postgres://etchflow:etchflow@postgres:5432/etchflow?sslmode=disable
      DB_POOL_MAX_CONNS: "10"
      HTTP_PORT: "8080"
      LOG_LEVEL: info
      LOG_FORMAT: console
    depends_on:
      postgres:
        condition: service_healthy
    restart: unless-stopped

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: etchflow
      POSTGRES_PASSWORD: etchflow
      POSTGRES_DB: etchflow
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ../migrations:/docker-entrypoint-initdb.d
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U etchflow"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  postgres_data:
```

**Migration strategy:** SQL files in `migrations/` are mounted into Postgres's `docker-entrypoint-initdb.d`. Postgres auto-executes them on first boot (sorted alphabetically, so `001_` runs before `002_`). No migration library needed for MVP.

**Important:** `docker-entrypoint-initdb.d` only runs on **first boot** (when the data volume is empty). To re-run migrations: `docker compose down -v` to wipe the volume.

---

## Part 5: Kill Test Procedure

### Makefile

```makefile
.PHONY: run stop kill-test clean

run:
	docker compose -f docker/docker-compose.yml up --build -d

stop:
	docker compose -f docker/docker-compose.yml down

clean:
	docker compose -f docker/docker-compose.yml down -v

kill-test:
	@echo "=== EtchFlow Kill Test ==="
	@echo ">>> Starting 8-node graph..."
	@python python_adapter/example_graph.py & echo $$! > /tmp/etchflow_pid
	@sleep 18
	@echo ">>> Killing Python at ~node 4..."
	@kill -9 $$(cat /tmp/etchflow_pid) 2>/dev/null || true
	@sleep 1
	@echo ">>> EtchFlow still alive?"
	@curl -sf http://localhost:8080/health && echo " ✅"
	@echo ">>> Last checkpoint:"
	@curl -sf http://localhost:8080/runs/$$(cat /tmp/etchflow_run_id)/state | python -m json.tool
	@echo ">>> Resuming from checkpoint..."
	@python python_adapter/example_graph.py --resume $$(cat /tmp/etchflow_run_id)
	@echo "=== Kill Test Complete ==="
```

### Kill Test — Step by Step

```
Step 1: docker compose up (EtchFlow + Postgres running)
Step 2: python example_graph.py
        - Submits run → gets run_id
        - Writes run_id to /tmp/etchflow_run_id (for Makefile)
        - Executes nodes 1-3 (extract, classify, analyse) → each checkpointed to EtchFlow
        - Node 4 (summarise) starts executing...

Step 3: kill -9 <python_pid>
        - Python dies instantly
        - EtchFlow is unaffected (separate Docker container)
        - Node 4 was NOT checkpointed (it was mid-execution)
        - Nodes 1-3 are safely persisted in Postgres

Step 4: curl /health → {"status":"ok"} ✅
Step 5: curl /runs/{id}/state → {"last_node_completed":"analyse", "state":{...}} ✅

Step 6: python example_graph.py --resume <run_id>
        - Calls saver.get_tuple() → loads checkpoint from EtchFlow
        - LangGraph sees state includes nodes 1-3 completed
        - Skips extract, classify, analyse ✅
        - Resumes at summarise (node 4)
        - Executes nodes 4-8 → each checkpointed
        - Final node (publish) → EtchFlow sets status=SUCCESS
        - Run complete ✅

Expected output:
  Resuming run: <run_id>
  Last checkpoint: analyse (node 3/8)
  [Node 4/8: summarise]  executing... ✓ checkpointed
  [Node 5/8: draft]      executing... ✓ checkpointed
  [Node 6/8: review]     executing... ✓ checkpointed
  [Node 7/8: format]     executing... ✓ checkpointed
  [Node 8/8: publish]    executing... ✓ checkpointed
  Run complete ✅
```

### Success Criteria

```
✅ Python process killed between nodes — state not lost
✅ EtchFlow service still running after Python dies
✅ GET /runs/{id}/state returns correct last checkpoint
✅ Python restart loads checkpoint, skips completed nodes
✅ Final output identical to an uninterrupted run
✅ Nodes 1-3 never re-executed (zero duplicate work)
```

---

## Part 6: Build Order (Exact Sequence)

Build in this order. Each step is testable before moving to the next.

### Step 1 — Skeleton + Config + Docker

```
[ ] go mod init
[ ] Create file structure (all empty files)
[ ] config.go — viper loader
[ ] .env.example
[ ] docker/Dockerfile
[ ] docker/docker-compose.yml
[ ] Migration SQL files (001, 002, 003)
[ ] docker compose up → verify Postgres boots, tables created
```

**Test:** `docker compose up`, connect to Postgres, verify 3 tables exist.

### Step 2 — Models + State Machine

```
[ ] models/run.go — Run struct, status constants (PENDING, RUNNING, SUCCESS, FAILED)
[ ] models/checkpoint.go — Checkpoint struct
[ ] models/log.go — AgentLog struct, event type constants
[ ] statemachine/transitions.go — transition map, IsValidTransition()
```

**Test:** Unit test IsValidTransition for all valid/invalid combos.

### Step 3 — Store Layer

```
[ ] store/run_store.go — CreateRun, GetRun, SetRunning, SetSuccess, UpdateCurrentState
[ ] store/checkpoint_store.go — SaveCheckpoint (ON CONFLICT DO NOTHING)
[ ] store/log_store.go — Append
[ ] store/store.go — Store struct that holds pool + all sub-stores + AtomicCheckpoint method
```

**Test:** Integration test against real Postgres in Docker. Insert run, save checkpoint, verify idempotency.

### Step 4 — Handlers + Router + Middleware

```
[ ] handler/health.go
[ ] handler/runs.go — POST /runs
[ ] handler/checkpoint.go — PUT /runs/{id}/checkpoint
[ ] handler/state.go — GET /runs/{id}/state
[ ] middleware/logging.go
[ ] middleware/recovery.go
[ ] router.go
```

**Test:** `curl POST /runs`, `curl PUT /checkpoint`, `curl GET /state`. Verify correct JSON responses.

### Step 5 — main.go (Wire Everything)

```
[ ] cmd/server/main.go — load config, create pool, create stores, create handlers, start server
[ ] Graceful shutdown
```

**Test:** `docker compose up --build`, `curl /health` → 200 OK.

### Step 6 — Python Adapter

```
[ ] etchflow_client.py
[ ] graph_serializer.py
[ ] etchflow_checkpoint_saver.py (real BaseCheckpointSaver API)
[ ] example_graph.py — 8-node graph, --resume flag
[ ] requirements.txt (langgraph, langchain-core, httpx)
```

**Test:** Run `example_graph.py` end-to-end (no crash). Verify all 8 nodes checkpoint to EtchFlow. Verify `GET /state` shows last node. Verify status = SUCCESS.

### Step 7 — Kill Test

```
[ ] Makefile (make run, make stop, make clean, make kill-test)
[ ] README.md — Kill Test walkthrough as first section
[ ] Run the full kill test manually
[ ] Run `make kill-test` — verify it passes
```

**Test:** The Kill Test itself IS the test. If it passes, MVP is done.

---

## Part 7: What NOT To Do

> These are the traps. Avoid them.

| Trap | Why it's wrong |
|---|---|
| Hardcode `last_node_completed` skip logic in Python | LangGraph handles resume natively via checkpoint. Don't reinvent it. |
| Mock LLM calls with instant returns | 5-second sleep is needed so the kill test has a window to kill Python mid-node. |
| Add worker pool "just in case" | MVP scope is durability only. Worker pool is Phase 1.5. |
| Use SQLite instead of Postgres | The entire value prop is external Postgres. SQLite is in-process. |
| Run migrations from Go code | `docker-entrypoint-initdb.d` handles this. No migration library for MVP. |
| Skip the agent_logs table | Audit trail is cheap to add now and critical for debugging. |
| Use `ON CONFLICT DO UPDATE` instead of `DO NOTHING` | `DO NOTHING` is the idempotency guarantee. `DO UPDATE` would overwrite. |
| Put Python adapter logic in a separate repo | It ships with EtchFlow. Same repo, same docker-compose. |

