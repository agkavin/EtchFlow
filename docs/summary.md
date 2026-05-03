# EtchFlow - Durable Execution for LangGraph

## Overview

EtchFlow is a durable execution engine for LangGraph that provides crash recovery, automatic retry, and worker-based batch processing. It coordinates execution state between Go (backend) and Python (LangGraph).

**Current Phase:** 1.5 - Worker Pool, Reaper, Heartbeat, and Retry

---

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Python    │────▶│    Go       │────▶│  PostgreSQL │
│  (LangGraph)│     │  EtchFlow   │     │             │
└─────────────┘     └─────────────┘     └─────────────┘
       │                   │                   │
       │                   │                   │
  - execute nodes    - coordinate       - store runs
  - checkpoint      - lease runs       - store checkpoints
  - heartbeat       - reap stale       - store logs
                    - retry backoff
```

---

## Data Flow

### 1. Submit Run (Direct Mode)
```
User → client.submit_run(graph, input_data)
              ↓
       POST /runs → creates PENDING run
              ↓
       returns run_id
```

### 2. Execute with Checkpointing
```
graph.invoke(input, config={"configurable": {"thread_id": run_id}})
              ↓
   LangGraph executes each node
              ↓
   After each node: saver.put() → PUT /runs/{id}/checkpoint
              ↓
   Go stores checkpoint + updates current_state atomically
              ↓
   On completion: client.complete_run() → POST /runs/{id}/complete
              ↓
   status = SUCCESS
```

### 3. Resume after Crash
```
Same run_id → invoke() → saver.get_tuple()
              ↓
   GET /runs/{id}/state → returns last checkpoint
              ↓
   LangGraph skips already-completed nodes
              ↓
   Resumes from last checkpoint
```

---

## Go Backend (EtchFlow)

### Database Tables

| Table | Purpose |
|-------|---------|
| `runs` | Workflow executions - status, state, worker, retry config |
| `checkpoints` | Node-level state snapshots for crash recovery |
| `agent_logs` | Audit trail of events |

### Background Services

| Service | Role |
|---------|------|
| **Reaper** | Scans RUNNING runs with stale heartbeats (>60s), resets to PENDING |
| **Retry Scanner** | Wakes RETRYING runs when backoff expires, flips to PENDING |

### API Endpoints

| Method | Endpoint | Purpose |
|--------|----------|---------|
| POST | `/runs` | Create new run |
| GET | `/runs/{id}` | Get run status |
| GET | `/runs/{id}/state` | Get current state + last node |
| GET | `/runs/{id}/checkpoints` | List checkpoint history |
| GET | `/runs/{id}/logs` | List audit logs |
| PUT | `/runs/{id}/checkpoint` | Save node checkpoint |
| PUT | `/runs/{id}/heartbeat` | Worker keeps lease alive |
| POST | `/runs/{id}/complete` | Mark run as SUCCESS |
| POST | `/runs/{id}/fail` | Report failure (triggers retry) |
| POST | `/runs/{id}/cancel` | Cancel run |
| POST | `/runs/claim` | Worker claims PENDING run |
| GET | `/ready` | Readiness check (DB ping) |
| GET | `/health` | Health check |

### Run States

```
PENDING → RUNNING → SUCCESS
              ↓
           FAILED (fatal error)
              ↓
           RETRYING (with backoff)
              ↓
           PENDING (retry) OR DEAD (max retries)
              ↓
           CANCELLED (user cancelled)
```

---

## Python SDK

### Components

| File | Purpose |
|------|---------|
| `client.py` | HTTP client - submit_run, get_state, heartbeat, complete, fail |
| `saver.py` | LangGraph CheckpointSaver - get_tuple, put, list |
| `worker.py` | Managed worker - polls, executes, heartbeats, handles failures |
| `graph.py` | EtchFlow wrapper - auto-calls complete on invoke return |

### EtchFlowWorker

```python
worker = EtchFlowWorker(
    client=client,
    graph=compiled_graph,
    concurrency=2,       # Process 2 runs at a time
    poll_interval=5.0,   # Poll every 5s
    heartbeat_interval=30.0  # Send heartbeat every 30s
)
worker.start()
```

**What it does:**
1. Polls `/runs/claim` every 5s (SKIP LOCKED - atomic)
2. Executes graph in thread pool (concurrency limit)
3. Sends heartbeats every 30s while running
4. Auto-calls `/complete` on success
5. Auto-calls `/fail` on exception (triggers retry)
6. Handles graceful shutdown (SIGTERM/SIGINT)

---

## Examples

### Direct Mode (demo.py)
```bash
python examples/demo.py              # Fresh run
python examples/demo.py --resume      # Resume from .run_id
```

### Worker Mode
```bash
# Terminal 1: Start worker
python examples/worker_demo.py

# Terminal 2: Submit jobs
python examples/submit_job.py --count 3
```

---

## Key Features

### ✅ Implemented

- **Atomic Checkpointing** - State saved after each node, no data loss
- **Crash Recovery** - Resume from last checkpoint after kill/Ctrl+C
- **Worker Pool** - Multiple workers poll and execute runs in parallel
- **Heartbeat** - Workers send heartbeats, Go tracks liveness
- **Reaper** - Auto-resets stale runs (>60s no heartbeat) to PENDING
- **Retry Engine** - Exponential backoff on failure, configurable max retries
- **API Endpoints** - Full CRUD on runs, checkpoints, logs
- **Graceful Shutdown** - Workers finish current node before exit

### 📋 Phase 1.5 Complete

All features from `docs/plan_1.5.md` are implemented:
- Migration 004 (worker_id, heartbeat, retry columns)
- Go Reaper and Retry Scanner
- API: checkpoints, logs, ready, heartbeat, fail, cancel, claim
- Python EtchFlowWorker class
- Auto-complete on invoke return
- Examples: demo, worker_demo, submit_job

---

## Configuration

### Go (cmd/server/main.go)
```go
reaper := worker.NewReaper(s, logger, 10*time.Second, 60*time.Second)
retryScanner := worker.NewRetryScanner(s, logger, 5*time.Second)
```

### Python (examples/worker_demo.py)
```python
worker = EtchFlowWorker(
    client=client,
    graph=compiled,
    concurrency=2,
    poll_interval=5.0,
    heartbeat_interval=30.0
)
```

---

## Testing

```bash
# Start services
make run

# Direct mode test
make test

# Resume test
make test-resume

# Worker test
make worker    # Terminal 1
make submit    # Terminal 2

# Kill test (crash recovery)
make test-kill
```

---

## File Structure

```
etchflow/
├── cmd/server/main.go       # Entry point, wires Go services
├── internal/
│   ├── api/                 # HTTP handlers + router
│   ├── store/               # Database operations
│   ├── worker/              # Reaper + Retry Scanner
│   └── models/              # Data types
├── migrations/              # SQL migrations (001-004)
├── etchflow-py-sdk/         # Python SDK
│   └── etchflow/
│       ├── client.py
│       ├── saver.py
│       ├── worker.py
│       └── graph.py
├── examples/                # demo.py, worker_demo.py, submit_job.py
├── docs/                    # plan.md, plan_1.5.md, issues.md, summary.md
└── Makefile                 # Build and test commands
```

---

## Summary

EtchFlow provides durable execution for LangGraph workflows:

1. **Submit** → Run created as PENDING
2. **Execute** → LangGraph runs nodes, each checkpointed to PostgreSQL
3. **Crash** → Resume with same run_id, LangGraph auto-skips completed nodes
4. **Worker Mode** → Multiple workers poll, execute, heartbeat, auto-retry on failure
5. **Recovery** → Go's Reaper detects stale runs, resets to PENDING, new worker picks up

The system is self-healing: crashes, failures, and worker deaths are all handled automatically without manual intervention.