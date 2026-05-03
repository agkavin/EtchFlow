# EtchFlow

A durable execution engine for LangGraph that provides crash recovery, automatic retry, and worker-based batch processing.

---

## The Problem

Building durable, long-running agentic workflows with LangGraph requires external state management. When pods crash, timeout, or are preempted, you lose state midway and have to restart expensive LLM generations from scratch.

## The Solution

EtchFlow is a **Bring-Your-Own-Compute (BYOC) Durable Execution Engine** for LangGraph. It acts as an external checkpointing backend - you write standard Python LangGraph code, and EtchFlow persists state after each node. If your process crashes, simply restart and resume from the exact node where it died.

---

## Two Ways to Use EtchFlow

### 1. Direct Mode (demo.py)

For single workflow executions where you control execution directly.

```python
from etchflow import EtchFlow, EtchFlowClient

# Build your graph
builder = StateGraph(BlogState)
builder.add_node("researcher", researcher)
builder.add_node("writer", writer)
builder.set_entry_point("researcher").add_edge("researcher", END)

# Wrap with EtchFlow
app = EtchFlow("http://localhost:8080").compile(builder)

# Execute - EtchFlow auto-checkpoints each node
result = app.invoke(
    {"topic": "The History of Coffee"},
    config={"configurable": {"thread_id": "my-run-123"}}
)

# If crashed, resume with same thread_id
# app.invoke({"topic": "..."}, config={"configurable": {"thread_id": "my-run-123"}})
```

**Run:**
```bash
python examples/demo.py              # Fresh run
python examples/demo.py --resume     # Resume from last checkpoint
```

### 2. Worker Mode (worker_demo.py + submit_job.py)

For production batch processing with multiple workers that poll for jobs.

```python
# Worker - polls and executes runs
from etchflow import EtchFlowClient, EtchFlowWorker

worker = EtchFlowWorker(
    client=EtchFlowClient("http://localhost:8080"),
    graph=compiled_graph,
    concurrency=4,        # Process 4 runs at a time
    poll_interval=5.0,   # Poll every 5 seconds
    heartbeat_interval=30.0
)
worker.start()
```

```python
# Submitter - adds jobs to the queue
from etchflow import EtchFlowClient

client = EtchFlowClient("http://localhost:8080")
client.submit_run(
    graph=builder,
    input_data={"task_id": "task-1"},
    run_id="job-task-1"
)
```

**Run:**
```bash
# Terminal 1: Start worker
python examples/worker_demo.py

# Terminal 2: Submit jobs
python examples/submit_job.py --count 5
```

---

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Python    │────▶│    Go       │────▶│  PostgreSQL │
│  (LangGraph)│     │  EtchFlow   │     │             │
└─────────────┘     └─────────────┘     └─────────────┘
       │                   │                   │
       │                   │                   │
  - execute nodes    - coordinate           - store runs
  - checkpoint      - lease runs           - store checkpoints  
  - heartbeat       - reap stale runs      - store logs
                    - retry backoff
```

### Go Backend Services

| Service | Purpose |
|---------|---------|
| **Reaper** | Scans RUNNING runs with stale heartbeats (>60s), resets to PENDING so another worker can pick up |
| **Retry Scanner** | Wakes RETRYING runs when backoff expires, flips to PENDING for retry |

### Python SDK Components

| Component | Purpose |
|-----------|---------|
| `client.py` | HTTP client - submit_run, get_state, heartbeat, complete, fail |
| `saver.py` | LangGraph CheckpointSaver - saves state after each node |
| `worker.py` | Managed worker - polls, executes, heartbeats, handles failures |
| `graph.py` | Wrapper - auto-calls complete on invoke return |

---

## Execution Flow

### Direct Mode

```
1. User → client.submit_run() → POST /runs → PENDING
2. User → app.invoke(input, config={"thread_id": run_id})
3. LangGraph executes each node
4. After each node: saver.put() → PUT /checkpoint → PostgreSQL
5. On completion: client.complete_run() → status = SUCCESS
```

### Worker Mode

```
1. User → client.submit_run() → POST /runs → PENDING
2. Worker → POST /runs/claim → atomically claims run (SKIP LOCKED)
3. Worker → executes graph, sends heartbeats every 30s
4. On completion: POST /runs/{id}/complete → SUCCESS
5. If worker dies → Reaper detects stale heartbeat (>60s)
6. Reaper → resets to PENDING → new worker picks up
7. If failed → POST /runs/{id}/fail → retry with backoff
```

### Resume After Crash

```
1. Same run_id → invoke() 
2. saver.get_tuple() → GET /runs/{id}/state
3. LangGraph sees last checkpoint
4. Skips already-completed nodes
5. Resumes from last node
```

---

## Run States

```
PENDING → RUNNING → SUCCESS
              ↓
           FAILED (fatal error)
              ↓
           RETRYING (exponential backoff)
              ↓
           PENDING (retry) OR DEAD (max retries exceeded)
              ↓
           CANCELLED (user requested)
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/runs` | Create new run |
| GET | `/runs/{id}` | Get run status |
| GET | `/runs/{id}/state` | Get current state + last node |
| GET | `/runs/{id}/checkpoints` | List checkpoint history |
| GET | `/runs/{id}/logs` | List audit trail |
| PUT | `/runs/{id}/checkpoint` | Save node checkpoint |
| PUT | `/runs/{id}/heartbeat` | Worker keeps lease alive |
| POST | `/runs/{id}/complete` | Mark run SUCCESS |
| POST | `/runs/{id}/fail` | Report failure (triggers retry) |
| POST | `/runs/{id}/cancel` | Cancel run |
| POST | `/runs/claim` | Worker claims PENDING run |
| GET | `/ready` | Readiness check (DB ping) |
| GET | `/health` | Health check |

---

## Project Structure

```
etchflow/
├── cmd/server/main.go       # Go entry point, wires services
├── internal/
│   ├── api/                 # HTTP handlers + router
│   ├── store/               # Database operations
│   ├── worker/              # Reaper + Retry Scanner
│   └── models/              # Data types
├── migrations/              # SQL migrations (001-004)
├── etchflow-py-sdk/         # Python SDK
│   └── etchflow/
│       ├── client.py        # HTTP client
│       ├── saver.py         # LangGraph checkpointer
│       ├── worker.py        # Managed worker
│       └── graph.py         # EtchFlow wrapper
├── examples/
│   ├── demo.py             # Direct mode example
│   ├── worker_demo.py      # Worker mode example
│   ├── submit_job.py       # Job submitter
│   └── README.md           # Examples documentation
├── docs/
│   ├── plan.md             # MVP plan
│   ├── plan_1.5.md         # Phase 1.5 plan
│   ├── issues.md           # Implementation issues
│   └── summary.md          # This summary
├── Makefile                # Build and test commands
└── docker/                 # Docker configuration
```

---

## How to Run

### Start EtchFlow

```bash
make run
```

### Direct Mode

```bash
# Fresh execution
python examples/demo.py

# Resume from crash
python examples/demo.py --resume
```

### Worker Mode

```bash
# Terminal 1: Start worker (polls for jobs)
python examples/worker_demo.py

# Terminal 2: Submit jobs
python examples/submit_job.py --count 3
```

### Tests

```bash
make test          # Run demo
make test-resume   # Resume demo
make test-kill     # Kill test (crash recovery)
make worker        # Start worker
make submit        # Submit jobs
```

---

## Key Features

- **Atomic Checkpointing** - State saved after each node, idempotent
- **Crash Recovery** - Resume from exact node after kill/cancel
- **Worker Pool** - Multiple workers process runs in parallel
- **Heartbeat** - Workers ping every 30s, Go tracks liveness
- **Auto-Reaper** - Resets stale runs (>60s) to PENDING
- **Retry Engine** - Exponential backoff on failure
- **Graceful Shutdown** - Workers finish current node before exit
- **Audit Logs** - Full event trail in agent_logs table