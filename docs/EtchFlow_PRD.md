
> **One-Line Pitch**
>
> *"EtchFlow is a Postgres-backed durable execution engine for DAG-based AI workflows, with idempotent checkpoints and crash recovery — so your LLM pipeline never starts over just because a process died."*

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Root Cause Analysis](#2-root-cause-analysis)
3. [What EtchFlow Actually Is](#3-what-etchflow-actually-is)
4. [Execution Model — The Critical Design Decision](#4-execution-model--the-critical-design-decision)
5. [Why Go + Why External Service](#5-why-go--why-external-service)
6. [Tech Stack](#6-tech-stack)
7. [Project File Structure](#7-project-file-structure)
8. [Environment Configuration](#8-environment-configuration)
9. [Data Model](#9-data-model)
10. [How a LangGraph DAG is Submitted and Parsed](#10-how-a-langgraph-dag-is-submitted-and-parsed)
11. [What a Worker (Run Activator) Is and Does](#11-what-a-worker-run-activator-is-and-does)
12. [System Architecture](#12-system-architecture)
13. [API Design](#13-api-design)
14. [User Flow — Detailed](#14-user-flow--detailed)
15. [The Kill Test — Primary Demo](#15-the-kill-test--primary-demo)
16. [Build Phases](#16-build-phases)
17. [Docker Packaging](#17-docker-packaging)
18. [Future Phases](#18-future-phases)
19. [What EtchFlow Is and Is Not](#19-what-etchflow-is-and-is-not)

---

## 1. Problem Statement

LangGraph is one of the most powerful frameworks for building stateful, multi-step AI agent workflows as Directed Acyclic Graphs (DAGs). Engineers use it to define complex reasoning pipelines where each node is an LLM call, a tool invocation, or a branching decision.

**LangGraph is built for logic definition — not for production reliability.**

When LangGraph workflows run in real infrastructure, they break in predictable ways:

---

### ❌ Problem 1 — No Crash Recovery

A 12-step financial analysis pipeline crashes at Step 9 due to an OOM kill or network failure.

```
DAG:  [1]→[2]→[3]→[4]→[5]→[6]→[7]→[8]→[9]→[10]→[11]→[12]
                                         ☠️ crash here

Without EtchFlow: restart from [1]. Lose $4–12 in LLM API spend.
With EtchFlow:    restart from [9]. Resume exactly.
```

---

### ❌ Problem 2 — In-Process State is Fragile

LangGraph's built-in `PostgresSaver` runs **inside the Python process**. When the process dies, the checkpointer dies with it. Any inflight write is lost. There is no external service that guarantees a write committed before the crash.

---

### ❌ Problem 3 — No Concurrent Worker Safety

Two Python workers pulling from the same workflow queue may attempt to read and write the same graph state simultaneously. LangGraph has no locking or atomic coordination for multi-worker scenarios.

**Result:** Race conditions, duplicate step execution, corrupted state.

---

### ❌ Problem 4 — No Execution Orchestration

LangGraph defines the graph. It does not manage a task queue, enforce concurrency limits, control retry policy, or provide structured audit logs. These are infrastructure concerns the framework ignores entirely.

---

### The Gap in One Line

> LangGraph tells agents **what to do**.  
> Nothing guarantees they actually **do it reliably**.

---

## 2. Root Cause Analysis

The root cause is architectural — not a bug in LangGraph, but a category mismatch.

```
LangGraph is an AI framework.       → optimized for reasoning logic
Production systems need a runtime.  → optimized for reliability guarantees
```

The AI/ML ecosystem builds excellent frameworks for the intelligence layer but consistently under-invests in the infrastructure layer beneath it. EtchFlow fills that gap.

---

## 3. What EtchFlow Actually Is

> **EtchFlow is a distributed state machine backed by Postgres.**

Everything in the system revolves around three concerns:

1. **State transitions** — every run moves through a defined set of valid states. Invalid transitions are rejected.
2. **Idempotency** — the same checkpoint can be written twice without corrupting state or double-counting.
3. **Recovery** — if anything dies at any point, the system can resume from the last committed state.

Python handles the intelligence. EtchFlow handles whether that intelligence completes reliably.

```
┌─────────────────────────────────────────────────────┐
│              DATA PLANE (Python)                    │
│   LangGraph defines nodes, edges, LLM calls         │
│   Python drives its own execution                   │
│   Python reports state to EtchFlow after each node      │
└─────────────────────┬───────────────────────────────┘
                      │  HTTP REST
                      │  (checkpoint writes, state reads)
┌─────────────────────▼───────────────────────────────┐
│              CONTROL PLANE (Go — EtchFlow)              │
│   Owns state, checkpoints, transitions, audit log   │
│   Guarantees: idempotency, recoverability, safety   │
└─────────────────────┬───────────────────────────────┘
                      │  pgx (atomic transactions)
┌─────────────────────▼───────────────────────────────┐
│              PERSISTENCE (PostgreSQL)               │
│   JSONB graph state · node checkpoints · audit logs │
│   Postgres queue (SKIP LOCKED) · state machine      │
└─────────────────────────────────────────────────────┘
```

---

## 4. Execution Model — The Critical Design Decision

This is the most important design question in the system. Getting it wrong creates confusion throughout the codebase.

### The Question

> When a run is submitted to EtchFlow — who actually starts executing the LangGraph nodes?

There are two possible answers. EtchFlow uses **Option A**.

---

### Option A — Python Drives Execution (EtchFlow = Durability Layer) ✅ CHOSEN

```
┌──────────────────────────────────────────────────────────┐
│                    Python Process                        │
│                                                          │
│  1. Submits run:    POST /runs ──────────────────▶ EtchFlow  │
│  2. Starts graph:   graph.invoke(input, thread_id)       │
│  3. Executes nodes: LLM calls happen inside Python       │
│  4. After each node: PUT /checkpoint ───────────▶ EtchFlow   │
│  5. On resume:      GET /state ────────────────▶ EtchFlow    │
│                     Skips completed nodes                │
└──────────────────────────────────────────────────────────┘

EtchFlow's role: receive state, persist it durably, respond with
             { continue: true/false }.
             EtchFlow never calls Python. Python always calls EtchFlow.
```

**EtchFlow does not trigger Python. Python triggers itself.**

This means:
- Python submits a run and immediately begins executing it
- EtchFlow is a pure state store and durability guarantee
- If Python dies: EtchFlow holds the last committed state, Reaper requeues the run, Python restarts and loads state from EtchFlow
- The "queue" (runs table) is how Python finds runs that need to be resumed after crash, not how EtchFlow dispatches work to Python

**Why Option A for Phase 1:**

| Reason | Detail |
|:---|:---|
| Simple | No IPC, no HTTP callbacks from EtchFlow to Python |
| Correct | Matches what `EtchFlowCheckpointSaver` actually does |
| Testable | Kill Test works with a single Python script |
| Honest | EtchFlow's value is durability, not orchestration |

---

### Option B — EtchFlow Triggers Python (True Runtime) ❌ NOT Phase 1

```
EtchFlow worker claims run → calls Python worker via HTTP/gRPC
→ Python executes → checkpoints back to EtchFlow
```

This is a much harder model requiring service discovery, worker registration, and bidirectional HTTP. It is the right long-term direction but out of scope for Phase 1.

---

### What EtchFlow's "Worker Pool" Actually Does in Option A

In Option A, a EtchFlow "worker" is not an execution engine. It is a **run activator** — a goroutine that:

1. Polls the Postgres queue for `PENDING` runs that have been **submitted but not yet picked up by any Python process** (e.g. runs that were reaped after a crash)
2. Marks them `RUNNING` so Python knows they are ready to execute
3. Fires a heartbeat to prevent the Reaper from wrongly evicting the run
4. Monitors for timeout

The worker does not execute nodes. It does not call Python. It coordinates state so Python can safely proceed. This distinction is critical.

> **Worker = Run Activator, not Executor.**

---

## 5. Why Go + Why External Service

### Why Go

| Go Property | Why It Matters for EtchFlow |
|:---|:---|
| Goroutines + Channels | Run heartbeat, reaper, and worker pool concurrently with zero overhead |
| `context.Context` | Timeout and cancellation propagation through every DB call and goroutine |
| Static binary | Single deployable artifact. No runtime. Trivial Docker image. |
| `pgx/v5` | Native Postgres driver. JSONB, prepared statements, `pgxpool`, atomic transactions. |
| `sync` primitives | Mutex-protected state transitions prevent races in multi-worker scenarios |
| Standard library HTTP | `net/http` + `chi`. No heavy framework. Clean and fast. |

### Why External Service

```
In-Process (LangGraph's built-in PostgresSaver):

  Python Process
  ┌─────────────────────────────────┐
  │  LangGraph DAG execution        │
  │  + PostgresSaver (in-process)   │ ← dies when process dies
  └─────────────────────────────────┘

External Service (EtchFlow):

  Python Process              EtchFlow Go Service
  ┌──────────────────┐        ┌──────────────────────┐
  │  LangGraph DAG   │─HTTP──▶│  State machine       │──▶ PostgreSQL
  │  execution only  │        │  Idempotent writes   │
  └──────────────────┘        │  Reaper goroutine    │
      ☠️ dies here            │  Heartbeat tracker   │
                              │  still running ✅    │
                              └──────────────────────┘
```

When Python dies, EtchFlow is still alive. It holds the last committed checkpoint. The Reaper detects the dead worker and requeues the run. Python restarts, loads state from EtchFlow, and resumes.

**This is not possible with any in-process checkpointer.**

---

## 6. Tech Stack

### Control Plane — Go Service

| Component | Technology | Reason |
|:---|:---|:---|
| Language | Go 1.22+ | Concurrency, single binary, context propagation |
| HTTP Router | `chi` | Lightweight, idiomatic, middleware support |
| Database Driver | `pgx/v5` | Native Postgres, JSONB, `pgxpool`, atomic txns |
| Migrations | `golang-migrate` | Versioned SQL, CI-friendly |
| Logging | `go.uber.org/zap` | Structured JSON logs, `run_id` on every line |
| Config | `viper` | ENV + YAML, 12-factor app ready |
| Testing | `testify` + `testcontainers-go` | Real Postgres in tests. No mocks. |

### Data Plane — Python Adapter

| Component | Technology | Reason |
|:---|:---|:---|
| Language | Python 3.11+ | LangGraph ecosystem |
| Agent Framework | LangGraph | Primary integration target |
| HTTP Client | `httpx` | Sync + async, clean timeout API |
| Checkpoint Adapter | Custom `EtchFlowCheckpointSaver` | Implements `BaseCheckpointSaver` |

### Persistence

| Component | Technology | Reason |
|:---|:---|:---|
| Database | PostgreSQL 16 | JSONB state, ACID, `SKIP LOCKED` queue, state machine |
| Queue Mechanism | `SELECT FOR UPDATE SKIP LOCKED` | No extra infra. Production-grade. |

---

## 7. Project File Structure

```
etchflow/
│
├── cmd/
│   └── server/
│       └── main.go                  # Entry point. Wires all components. Starts HTTP server,
│                                    # worker pool, and reaper goroutines.
│
├── internal/
│   │
│   ├── api/
│   │   ├── handler/
│   │   │   ├── runs.go              # POST /runs · GET /runs/{id}
│   │   │   ├── checkpoint.go        # PUT /runs/{id}/checkpoint
│   │   │   ├── state.go             # GET /runs/{id}/state
│   │   │   ├── fail.go              # POST /runs/{id}/fail  (called by Python on node error)
│   │   │   ├── cancel.go            # POST /runs/{id}/cancel
│   │   │   ├── logs.go              # GET /runs/{id}/logs
│   │   │   ├── checkpoints.go       # GET /runs/{id}/checkpoints
│   │   │   └── health.go            # GET /health · GET /ready
│   │   ├── middleware/
│   │   │   ├── logging.go           # Structured request logging
│   │   │   └── recovery.go          # Panic recovery → 500
│   │   └── router.go                # All routes registered here
│   │
│   ├── worker/
│   │   ├── pool.go                  # Spawns N activator goroutines. Graceful shutdown.
│   │   ├── activator.go             # Single run activator: poll → claim → heartbeat → monitor
│   │   └── heartbeat.go             # Pings last_heartbeat_at every 30s while run is RUNNING
│   │
│   ├── queue/
│   │   └── postgres_queue.go        # ClaimNext(): SELECT FOR UPDATE SKIP LOCKED + UPDATE
│   │
│   ├── reaper/
│   │   └── reaper.go                # Detects stale RUNNING runs → resets to PENDING
│   │
│   ├── store/
│   │   ├── run_store.go             # CRUD + state transitions for runs table
│   │   ├── checkpoint_store.go      # Atomic checkpoint save with ON CONFLICT DO NOTHING
│   │   └── log_store.go             # Append-only agent_logs writes and queries
│   │
│   ├── statemachine/
│   │   └── transitions.go           # Valid transition map. Rejects invalid moves at DB layer.
│   │
│   ├── retry/
│   │   └── policy.go                # Exponential backoff + jitter. Calculates next_retry_at.
│   │
│   ├── models/
│   │   ├── run.go                   # Run struct + status constants (PENDING, RUNNING, etc.)
│   │   ├── checkpoint.go            # Checkpoint struct
│   │   └── log.go                   # AgentLog struct + event type constants
│   │
│   └── config/
│       └── config.go                # Viper loader. Validates required env vars on startup.
│                                    # Fails fast if DATABASE_URL is missing.
│
├── migrations/
│   ├── 001_create_runs.sql          # runs table with all columns
│   ├── 002_create_checkpoints.sql   # checkpoints table + UNIQUE(run_id, node_name)
│   ├── 003_create_agent_logs.sql    # agent_logs append-only table
│   └── 004_add_indexes.sql          # Partial indexes for queue poll + reaper scan
│
├── python_adapter/
│   ├── etchflow_client.py               # httpx wrapper: submit_run, get_state, save_checkpoint, fail_run
│   ├── etchflow_checkpoint_saver.py     # EtchFlowCheckpointSaver: get(), put(), list()
│   ├── graph_serializer.py          # Extracts DAG metadata (nodes, edges) from StateGraph
│   └── example_graph.py            # 8-node demo graph. Supports --resume <run_id>.
│
├── docker/
│   ├── Dockerfile                   # Multi-stage: golang:1.22-alpine → distroless/static
│   └── docker-compose.yml           # EtchFlow + PostgreSQL with healthchecks
│
├── .env.example                     # All env vars documented with tuning guidance
├── go.mod
├── go.sum
├── Makefile                         # make run · make test · make migrate · make kill-test
└── README.md                        # Kill Test walkthrough is the first section
```

---

## 8. Environment Configuration

```bash
# ─────────────────────────────────────────────────────────────────────
# .env.example — EtchFlow Configuration
# Copy to .env before running locally.
# All vars can be overridden via docker-compose environment block.
# ─────────────────────────────────────────────────────────────────────

# ── Database ─────────────────────────────────────────────────────────
# Required. Full Postgres DSN.
DATABASE_URL=postgres://etchflow:etchflow@localhost:5432/etchflow?sslmode=disable

# Max open connections in pgxpool.
# Rule of thumb: MAX_WORKERS * 3
# (each activator needs: 1 claim conn + 1 heartbeat conn + 1 for API calls)
DB_POOL_MAX_CONNS=30

# ── Run Activator Pool ────────────────────────────────────────────────
# Number of concurrent run activator goroutines.
# These claim PENDING runs and monitor them — they do NOT execute nodes.
# Start low. Each activator is cheap (goroutine + 2 DB connections).
MAX_WORKERS=10

# How often (seconds) each activator polls for PENDING runs when idle.
# Lower = faster pickup. Higher = less DB load. 2s is a good balance.
WORKER_POLL_INTERVAL_SECONDS=2

# How often (seconds) a run activator sends a heartbeat to the DB.
# Must be less than STALE_THRESHOLD_SECONDS / 3.
WORKER_HEARTBEAT_INTERVAL_SECONDS=30

# ── Reaper ────────────────────────────────────────────────────────────
# How often (seconds) the Reaper scans for stale RUNNING runs.
REAPER_INTERVAL_SECONDS=60

# A RUNNING run is stale if last_heartbeat_at is older than this many
# seconds. Must be > WORKER_HEARTBEAT_INTERVAL_SECONDS * 3 to avoid
# wrongly evicting runs with slow but alive LLM calls.
# Default: 120s (2 minutes).
STALE_THRESHOLD_SECONDS=120

# ── Timeouts ──────────────────────────────────────────────────────────
# Maximum time (seconds) a run may stay RUNNING before EtchFlow marks it
# TIMEOUT. 0 = no limit. Recommended: 3600 (1 hour) for LLM workloads.
DEFAULT_RUN_TIMEOUT_SECONDS=3600

# ── Retry Policy ──────────────────────────────────────────────────────
# Applied when Python calls POST /runs/{id}/fail.
# Per-run values submitted at POST /runs override these defaults.
DEFAULT_MAX_RETRIES=3
DEFAULT_BASE_DELAY_MS=1000      # Initial wait before first retry
DEFAULT_MAX_DELAY_MS=30000      # Caps exponential growth at 30 seconds

# ── Server ────────────────────────────────────────────────────────────
HTTP_PORT=8080

# ── Logging ───────────────────────────────────────────────────────────
# debug | info | warn | error
LOG_LEVEL=info
# json | console  (use json in production for log aggregators)
LOG_FORMAT=console
```

---

## 9. Data Model

### Table: `runs`

One row per workflow execution. This table serves three purposes simultaneously:

- **Task queue** — `WHERE status = 'PENDING' ORDER BY created_at` is the queue
- **State machine** — `status` column drives the distributed state machine
- **Coordination** — `worker_id` and `last_heartbeat_at` coordinate Reaper vs active activators

```sql
CREATE TABLE runs (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Metadata (stored, not interpreted by EtchFlow at execution time)
    -- EtchFlow uses this for validation and display only.
    -- It does NOT use graph_definition to drive execution.
    graph_definition    JSONB       NOT NULL,

    -- Initial input passed to the graph on first run or resume
    input_data          JSONB       NOT NULL,

    -- Latest graph state, updated atomically per checkpoint.
    -- This is what Python loads on resume.
    current_state       JSONB,

    -- Lifecycle
    status              TEXT        NOT NULL DEFAULT 'PENDING',
    -- Valid values: PENDING | RUNNING | SUCCESS | FAILED
    --               RETRYING | DEAD | CANCELLED | TIMEOUT
    worker_id           TEXT,                    -- Which activator goroutine claimed this run
    last_node_completed TEXT,                    -- Name of last successfully checkpointed node

    -- Retry policy (per-run values override defaults from env)
    attempt_count       INT         NOT NULL DEFAULT 0,
    max_retries         INT         NOT NULL DEFAULT 3,
    base_delay_ms       INT         NOT NULL DEFAULT 1000,
    max_delay_ms        INT         NOT NULL DEFAULT 30000,
    next_retry_at       TIMESTAMPTZ,

    -- Error tracking
    last_error          TEXT,
    last_error_at       TIMESTAMPTZ,

    -- Timestamps
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Heartbeat: updated every 30s by the activator goroutine.
    -- Reaper uses this to detect dead workers, NOT updated_at.
    last_heartbeat_at   TIMESTAMPTZ
);

-- Fast queue poll: only scans PENDING rows, ordered by arrival time
CREATE INDEX idx_runs_pending_queue
    ON runs (created_at ASC)
    WHERE status = 'PENDING';

-- Fast reaper scan: only scans RUNNING rows
CREATE INDEX idx_runs_running_heartbeat
    ON runs (last_heartbeat_at ASC)
    WHERE status = 'RUNNING';

-- General status index for GET /runs/{id} and status filters
CREATE INDEX idx_runs_status ON runs (status);
```

---

### Table: `checkpoints`

One row per completed node per run. The source of truth for crash recovery.

```sql
CREATE TABLE checkpoints (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id      UUID        NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    node_name   TEXT        NOT NULL,
    state_json  JSONB       NOT NULL,    -- Full LangGraph state after this node
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Idempotency guarantee: a node can only checkpoint once per run.
    -- ON CONFLICT DO NOTHING on this constraint prevents double-writes
    -- from retries or network blips from corrupting state.
    CONSTRAINT uq_checkpoint_run_node UNIQUE (run_id, node_name)
);

CREATE INDEX idx_checkpoints_run_id ON checkpoints (run_id, created_at ASC);
```

---

### Table: `agent_logs`

Append-only audit trail. Written on every state transition, checkpoint, reap, and failure. Never updated. Never deleted.

```sql
CREATE TABLE agent_logs (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id      UUID        NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    node_name   TEXT,                    -- NULL for run-level events
    event_type  TEXT        NOT NULL,
    -- Values: SUBMITTED | CLAIMED | NODE_COMPLETED | NODE_FAILED
    --         RETRYING | DEAD | REAPED | CANCELLED | TIMEOUT | SUCCESS
    message     TEXT,
    metadata    JSONB,                   -- e.g. { "attempt": 2, "error": "rate limit" }
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_agent_logs_run_id ON agent_logs (run_id, created_at ASC);
```

---

## 10. How a LangGraph DAG is Submitted and Parsed

### What "parsing" means in EtchFlow

EtchFlow **does not execute LangGraph nodes**. Python does. What EtchFlow receives and stores is the DAG **topology as metadata** — node names, edges, entry point. EtchFlow uses this metadata for:

- Validating that checkpoint `node_name` values match known nodes
- Displaying the run's structure in `GET /runs/{id}`
- Future: detecting the finish node to auto-transition to `SUCCESS`

The graph definition is **not execution logic**. It is **descriptive metadata**. Be clear on this.

---

### The Role of the Python Adapter (SDK)

You need a thin Python adapter (SDK) to use EtchFlow. But keep it in perspective:

**❌ You are NOT modifying LangGraph.**  
**✅ You are plugging into its checkpoint system.**

LangGraph by default runs the graph and optionally uses its own in-process checkpointing. It does **not know about EtchFlow** — it doesn't know where to store state externally or how to resume from it.

Your thin adapter does exactly three things:
1. **Submit run:** Registers the run with EtchFlow to get a `run_id`.
2. **Hook into checkpoint system:** Implements LangGraph's `BaseCheckpointSaver`. LangGraph *already calls checkpoint automatically* after every node. You just replace the default saver with the `EtchFlowCheckpointSaver` (implementing `get()` to fetch the last state and `put()` to send the checkpoint).
3. **Handle resume:** On restart, LangGraph calls `get()`, your adapter returns the last saved state, and LangGraph resumes automatically.

**The adapter lets LangGraph store and load its state from EtchFlow instead of memory.** It does not rewrite LangGraph, fork it, or add custom execution loops.

---

### Step 1 — Python defines a normal LangGraph graph

```python
# python_adapter/example_graph.py
from langgraph.graph import StateGraph
from typing import TypedDict

class AgentState(TypedDict):
    input:        str
    analysis:     str
    summary:      str
    final_report: str

def analyse_node(state):
    # real LLM call here
    return {"analysis": f"Analysis of: {state['input']}"}

def summarise_node(state):
    return {"summary": f"Summary: {state['analysis']}"}

def report_node(state):
    return {"final_report": f"Report: {state['summary']}"}

builder = StateGraph(AgentState)
builder.add_node("analyse",   analyse_node)
builder.add_node("summarise", summarise_node)
builder.add_node("report",    report_node)
builder.set_entry_point("analyse")
builder.add_edge("analyse",   "summarise")
builder.add_edge("summarise", "report")
builder.set_finish_point("report")
```

---

### Step 2 — Serializer extracts metadata (not logic)

```python
# python_adapter/graph_serializer.py

def serialize_graph(graph: StateGraph) -> dict:
    """
    Extracts the DAG topology from a LangGraph StateGraph.
    Returns JSON-serializable metadata stored in runs.graph_definition.
    EtchFlow stores this but does NOT use it to drive execution.
    Python drives its own execution. EtchFlow records what happened.
    """
    compiled = graph.compile()
    nodes = list(compiled.nodes.keys())
    edges = [
        {"from": src, "to": dst}
        for src, destinations in compiled.edges.items()
        for dst in destinations
    ]
    return {
        "nodes":        nodes,
        "edges":        edges,
        "entry_point":  compiled.entry_point,
        "finish_point": compiled.finish_point,
    }
```

**Output stored in `runs.graph_definition`:**

```json
{
  "nodes":       ["__start__", "analyse", "summarise", "report", "__end__"],
  "edges":       [
    { "from": "__start__",  "to": "analyse"   },
    { "from": "analyse",    "to": "summarise" },
    { "from": "summarise",  "to": "report"    },
    { "from": "report",     "to": "__end__"   }
  ],
  "entry_point":  "analyse",
  "finish_point": "report"
}
```

---

### Step 3 — Submit the run and start executing immediately

```python
# python_adapter/etchflow_client.py
import httpx
from .graph_serializer import serialize_graph

class EtchFlowClient:
    def __init__(self, base_url: str):
        self.http = httpx.Client(base_url=base_url, timeout=30.0)

    def submit_run(self, graph, input_data: dict, max_retries: int = 3) -> str:
        """Register the run with EtchFlow. Returns run_id. Does not start execution."""
        resp = self.http.post("/runs", json={
            "graph_definition": serialize_graph(graph),
            "input_data":       input_data,
            "max_retries":      max_retries,
        })
        resp.raise_for_status()
        return resp.json()["run_id"]

    def get_state(self, run_id: str) -> dict | None:
        """Fetch the last committed checkpoint. Returns None if no checkpoint exists."""
        resp = self.http.get(f"/runs/{run_id}/state")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def save_checkpoint(self, run_id: str, node_name: str, state: dict) -> dict:
        """Atomically persist node state. Returns { continue: bool }."""
        resp = self.http.put(f"/runs/{run_id}/checkpoint", json={
            "node_name": node_name,
            "state":     state,
        })
        resp.raise_for_status()
        return resp.json()

    def fail_run(self, run_id: str, error: str) -> dict:
        """Called by Python when a node raises an exception. Triggers retry logic."""
        resp = self.http.post(f"/runs/{run_id}/fail", json={"error": error})
        resp.raise_for_status()
        return resp.json()  # { status: "RETRYING"|"DEAD", retry_in_ms: 2000 }
```

---

### Step 4 — EtchFlowCheckpointSaver hooks into LangGraph's execution

```python
# python_adapter/etchflow_checkpoint_saver.py
import json
from langgraph.checkpoint.base import BaseCheckpointSaver, Checkpoint, CheckpointMetadata
from langchain_core.runnables import RunnableConfig

class EtchFlowCheckpointSaver(BaseCheckpointSaver):
    """
    Replaces LangGraph's in-process PostgresSaver.
    LangGraph calls put() after every node automatically.
    LangGraph calls get() on graph.invoke() to check for an existing checkpoint.
    """

    def __init__(self, client):
        self.client = client

    def get(self, config: RunnableConfig) -> Checkpoint | None:
        """
        Called when graph.invoke() starts.
        If a checkpoint exists → LangGraph resumes from that node.
        If not → LangGraph starts from entry_point.
        This is the entire crash recovery mechanism on the Python side.
        """
        run_id = config["configurable"]["thread_id"]
        data = self.client.get_state(run_id)
        if not data or not data.get("state"):
            return None
        return json.loads(data["state"])

    def put(self, config: RunnableConfig, checkpoint: Checkpoint, metadata: CheckpointMetadata):
        """
        Called by LangGraph after every node completes.
        Sends state to EtchFlow for atomic persistence.
        If EtchFlow returns continue=false, raises to halt Python execution.
        """
        run_id    = config["configurable"]["thread_id"]
        node_name = metadata.get("source", "unknown")
        state     = json.dumps(checkpoint)

        response  = self.client.save_checkpoint(run_id, node_name, json.loads(state))

        if not response.get("continue", True):
            raise RuntimeError(f"EtchFlow halted: {response.get('halt_reason')}")

    def list(self, config: RunnableConfig, *, filter=None, before=None, limit=None):
        """
        Returns all node checkpoints in execution order.
        Used by LangGraph for history and time-travel features.
        Backed by GET /runs/{id}/checkpoints.
        """
        run_id = config["configurable"]["thread_id"]
        resp   = self.client.http.get(f"/runs/{run_id}/checkpoints")
        for row in resp.json():
            yield json.loads(row["state"])
```

---

### Step 5 — Wire it together and run

```python
# python_adapter/example_graph.py (continued)
import sys
from etchflow_client import EtchFlowClient
from etchflow_checkpoint_saver import EtchFlowCheckpointSaver

client = EtchFlowClient(base_url="http://localhost:8080")
saver  = EtchFlowCheckpointSaver(client=client)

if "--resume" in sys.argv:
    # Crash recovery path: run_id was stored before the crash
    run_id = sys.argv[sys.argv.index("--resume") + 1]
    print(f"Resuming run: {run_id}")
else:
    # Happy path: submit a new run
    run_id = client.submit_run(
        graph=builder,
        input_data={"input": "Analyse Q3 financial report"},
        max_retries=3,
    )
    print(f"Run submitted: {run_id}")

# Python drives its own execution.
# LangGraph calls saver.get() on invoke → loads last checkpoint if one exists.
# LangGraph calls saver.put() after each node → persists to EtchFlow.
graph  = builder.compile(checkpointer=saver)
result = graph.invoke(
    {"input": "Analyse Q3 financial report"},
    config={"configurable": {"thread_id": run_id}},
)
print(f"Run complete: {result}")
```

---

## 11. What a Worker (Run Activator) Is and Does

### Naming clarity

The component previously called "Worker" is renamed **Run Activator** throughout this PRD. This name more accurately describes what it does: it activates runs in the queue so Python can pick them up safely, without competing with other Python processes.

A Run Activator does **not**:
- Execute LangGraph nodes
- Call any LLM
- Run Python code
- Block waiting for Python to finish

A Run Activator **does**:
- Poll the Postgres queue for `PENDING` runs
- Atomically claim a run with `SKIP LOCKED`
- Set `status = RUNNING` and record `worker_id`
- Fire a heartbeat goroutine every 30s
- Monitor the run for timeout
- Handle the run reaching `SUCCESS`, `FAILED`, or `DEAD`

---

### The Activator is non-blocking

This is critical. Once an activator claims a run and starts the heartbeat, **it immediately returns to polling**. It does not sit and wait for Python. The HTTP handler for `PUT /runs/{id}/checkpoint` handles all state advancement. The activator and the HTTP layer coordinate exclusively through the database — no in-process channels, no shared memory.

```
Activator goroutine              HTTP Handler (PUT /checkpoint)
       │                                    │
       │  Claims run, sets RUNNING          │
       │  Starts heartbeat goroutine        │
       │  Returns to poll loop ──────────── │ ← activator is done, back to polling
                                            │
                                            │  Python sends PUT /checkpoint
                                            │  Handler saves checkpoint atomically
                                            │  Handler updates current_state
                                            │  Handler writes NODE_COMPLETED to logs
                                            │  Handler returns { continue: true }
                                            │
                                            │  ... more checkpoints ...
                                            │
                                            │  Python sends final node checkpoint
                                            │  Handler sets status = SUCCESS
                                            │  Handler cancels heartbeat (via run_id lookup)
                                            │  Handler writes SUCCESS to logs
```

---

### Run Activator Lifecycle

```
┌──────────────────────────────────────────────────────────────────────┐
│                       RUN ACTIVATOR GOROUTINE                        │
│                                                                      │
│  1. POLL     Every 2s: SELECT 1 PENDING run FOR UPDATE SKIP LOCKED   │
│              If nothing: sleep 2s, repeat                            │
│              If found: proceed to CLAIM                              │
│                                                                      │
│  2. CLAIM    UPDATE runs SET status=RUNNING, worker_id=self.id,      │
│              started_at=NOW(), last_heartbeat_at=NOW()               │
│              INSERT agent_logs (CLAIMED)                             │
│                                                                      │
│  3. HEARTBEAT  Launch heartbeat goroutine (shares context with run)  │
│               Goroutine pings last_heartbeat_at every 30s            │
│               Goroutine exits when context is cancelled              │
│                                                                      │
│  4. TIMEOUT  If DEFAULT_RUN_TIMEOUT_SECONDS > 0:                     │
│              Launch timeout goroutine                                │
│              After N seconds: UPDATE status=TIMEOUT                  │
│              Cancel heartbeat context                                │
│                                                                      │
│  5. RETURN   Activator returns to step 1. Ready for next run.        │
│              (State advancement is handled by HTTP handlers)         │
│                                                                      │
│  ── On node failure (Python calls POST /runs/{id}/fail) ──           │
│     HTTP handler increments attempt_count                            │
│     If attempt_count < max_retries:                                  │
│       SET status=RETRYING, next_retry_at = backoff formula           │
│       INSERT agent_logs (RETRYING)                                   │
│       Background goroutine resets to PENDING when next_retry_at passes│
│       Activator will re-claim it on next poll                        │
│     If attempt_count >= max_retries:                                 │
│       SET status=DEAD                                                │
│       INSERT agent_logs (DEAD)                                       │
└──────────────────────────────────────────────────────────────────────┘
```

---

### Activator Pool Code

```go
// internal/worker/pool.go
type ActivatorPool struct {
    size   int
    queue  *queue.PostgresQueue
    store  *store.Store
    logger *zap.Logger
}

func (ap *ActivatorPool) Start(ctx context.Context) {
    var wg sync.WaitGroup
    for i := 0; i < ap.size; i++ {
        wg.Add(1)
        id := fmt.Sprintf("activator-%d", i)
        go func() {
            defer wg.Done()
            a := &Activator{id: id, queue: ap.queue, store: ap.store,
                logger: ap.logger.With(zap.String("activator_id", id))}
            a.Run(ctx) // blocks until ctx cancelled
        }()
    }
    wg.Wait()
}

// internal/worker/activator.go
func (a *Activator) Run(ctx context.Context) {
    ticker := time.NewTicker(cfg.PollIntervalSeconds * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            run, err := a.queue.ClaimNext(ctx, a.id)
            if err != nil {
                a.logger.Error("poll error", zap.Error(err))
                continue
            }
            if run == nil {
                continue // nothing in queue
            }
            // Non-blocking: launch heartbeat and timeout, return immediately
            runCtx, cancel := context.WithCancel(ctx)
            a.store.RegisterCancelFn(run.ID, cancel) // so HTTP handler can stop heartbeat
            go a.heartbeat(runCtx, run.ID)
            if run.TimeoutSeconds > 0 {
                go a.enforceTimeout(runCtx, run.ID, run.TimeoutSeconds)
            }
            a.store.Log(ctx, run.ID, "", "CLAIMED",
                fmt.Sprintf("activator %s claimed run", a.id), nil)
        }
    }
}
```

---

### Postgres Queue Claim

```go
// internal/queue/postgres_queue.go
func (q *PostgresQueue) ClaimNext(ctx context.Context, activatorID string) (*models.Run, error) {
    tx, err := q.pool.Begin(ctx)
    if err != nil {
        return nil, err
    }
    defer tx.Rollback(ctx)

    var run models.Run
    err = tx.QueryRow(ctx, `
        SELECT id, graph_definition, input_data,
               max_retries, base_delay_ms, max_delay_ms
        FROM   runs
        WHERE  status = 'PENDING'
        ORDER  BY created_at ASC
        LIMIT  1
        FOR UPDATE SKIP LOCKED
    `).Scan(&run.ID, &run.GraphDef, &run.InputData,
            &run.MaxRetries, &run.BaseDelayMs, &run.MaxDelayMs)

    if err == pgx.ErrNoRows {
        return nil, nil // queue is empty
    }
    if err != nil {
        return nil, err
    }

    _, err = tx.Exec(ctx, `
        UPDATE runs
        SET    status            = 'RUNNING',
               worker_id         = $1,
               started_at        = NOW(),
               last_heartbeat_at = NOW(),
               updated_at        = NOW()
        WHERE  id = $2
    `, activatorID, run.ID)
    if err != nil {
        return nil, err
    }

    return &run, tx.Commit(ctx)
}
```

---

## 12. System Architecture

### Component Map

```
                    ┌──────────────────────────────────────┐
                    │           Python Process             │
                    │                                      │
                    │  LangGraph DAG                       │
                    │  EtchFlowCheckpointSaver                 │
                    │  EtchFlowClient (httpx)                  │
                    │                                      │
                    │  Python drives its own execution.    │
                    │  EtchFlow never calls Python.            │
                    └──────────────┬───────────────────────┘
                                   │
                     POST /runs (submit)
                     PUT  /runs/{id}/checkpoint (after each node)
                     GET  /runs/{id}/state (on resume)
                     POST /runs/{id}/fail (on node error)
                                   │
                                   ▼ HTTP REST :8080
┌─────────────────────────────────────────────────────────────────────┐
│                         EtchFlow Go Service                             │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                     REST API Layer (chi)                    │   │
│  │                                                             │   │
│  │  POST /runs              → insert PENDING run               │   │
│  │  PUT  /runs/{id}/checkpoint → atomic checkpoint save        │   │
│  │  GET  /runs/{id}/state   → return last checkpoint           │   │
│  │  GET  /runs/{id}/checkpoints → checkpoint history           │   │
│  │  GET  /runs/{id}/logs    → audit trail                      │   │
│  │  POST /runs/{id}/fail    → trigger retry or DEAD            │   │
│  │  POST /runs/{id}/cancel  → cancel run                       │   │
│  │  GET  /health            → liveness                         │   │
│  │  GET  /ready             → readiness (Postgres ping)        │   │
│  └──────────────────────────┬──────────────────────────────────┘   │
│                             │                                       │
│          ┌──────────────────▼──────────────────┐                   │
│          │            Store Layer              │                   │
│          │                                     │                   │
│          │  RunStore         — CRUD + FSM       │                   │
│          │  CheckpointStore  — atomic saves     │                   │
│          │  LogStore         — audit appends    │                   │
│          │  StateMachine     — valid transitions│                   │
│          └──────────────────┬──────────────────┘                   │
│                             │                                       │
│  ┌──────────────────────────▼──────────────────────────────────┐   │
│  │                  Run Activator Pool                         │   │
│  │                                                             │   │
│  │  ┌─────────────────┐  ┌─────────────────┐                  │   │
│  │  │  Activator 1    │  │  Activator N    │  (goroutines)    │   │
│  │  │  ┌───────────┐  │  │  ┌───────────┐  │                  │   │
│  │  │  │ Heartbeat │  │  │  │ Heartbeat │  │                  │   │
│  │  │  └───────────┘  │  │  └───────────┘  │                  │   │
│  │  └─────────────────┘  └─────────────────┘                  │   │
│  │                                                             │   │
│  │  Poll: SELECT FOR UPDATE SKIP LOCKED every 2s              │   │
│  │  Non-blocking: claim → start heartbeat → return to poll    │   │
│  └──────────────────────────────────────────────────────────── ┘   │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                        Reaper                               │   │
│  │                                                             │   │
│  │  Runs on startup (catches pre-restart crashes) + every 60s │   │
│  │  Finds: status=RUNNING AND last_heartbeat_at < NOW()-120s  │   │
│  │  Action: UPDATE status=PENDING                              │   │
│  │  Logs:   INSERT agent_logs (REAPED) for every reaped run   │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                     Retry Engine                            │   │
│  │                                                             │   │
│  │  Triggered by POST /runs/{id}/fail                         │   │
│  │  Formula: wait = base_ms * 2^attempt + jitter              │   │
│  │  On RETRYING: background goroutine resets to PENDING       │   │
│  │               when next_retry_at passes                     │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                             │
                             ▼ pgx/v5 (atomic transactions)
                   ┌─────────────────────┐
                   │     PostgreSQL 16   │
                   │                     │
                   │  runs               │  ← queue + state machine + coordination
                   │  checkpoints        │  ← crash recovery source of truth
                   │  agent_logs         │  ← append-only audit trail
                   └─────────────────────┘
```

---

### State Machine

```
                    ┌─────────────┐
   POST /runs       │             │
  ────────────────▶ │   PENDING   │ ◀── Reaper resets stale RUNNING runs here
                    │             │ ◀── Retry engine resets RETRYING runs here
                    └──────┬──────┘
                           │ Activator claims (SKIP LOCKED)
                           ▼
                    ┌─────────────┐
                    │   RUNNING   │ ── heartbeat every 30s → last_heartbeat_at
                    └──────┬──────┘
          ┌────────────────┼──────────────┬────────────────┐
          ▼                ▼              ▼                ▼
    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌────────────┐
    │ SUCCESS  │    │  FAILED  │    │ TIMEOUT  │    │ CANCELLED  │
    └──────────┘    └────┬─────┘    └──────────┘    └────────────┘
                         │
              ┌──────────┴───────────┐
       attempt < max          attempt >= max
              ▼                      ▼
        ┌──────────┐           ┌──────────┐
        │ RETRYING │           │   DEAD   │
        └────┬─────┘           └──────────┘
             │ next_retry_at passed
             ▼
          PENDING (re-queued, activator picks up)

Any RUNNING state → CANCELLED via POST /runs/{id}/cancel
```

---

## 13. API Design

All responses use a consistent JSON structure. Errors follow RFC 7807 Problem Details format.

---

### `POST /runs`

Register a new LangGraph DAG for execution. Python submits this then immediately starts graph.invoke().

**Request:**
```json
{
  "graph_definition": {
    "nodes":       ["__start__", "analyse", "summarise", "report", "__end__"],
    "edges":       [
      { "from": "__start__",  "to": "analyse"   },
      { "from": "analyse",    "to": "summarise" },
      { "from": "summarise",  "to": "report"    },
      { "from": "report",     "to": "__end__"   }
    ],
    "entry_point":  "analyse",
    "finish_point": "report"
  },
  "input_data":   { "input": "Analyse Q3 financial report" },
  "max_retries":  3,
  "base_delay_ms": 1000,
  "max_delay_ms":  30000
}
```

**Response `201 Created`:**
```json
{
  "run_id":     "a3f7c821-1234-...",
  "status":     "PENDING",
  "created_at": "2024-11-15T09:00:00Z"
}
```

---

### `PUT /runs/{id}/checkpoint`

Called by Python's `EtchFlowCheckpointSaver.put()` after every node completes. The most important endpoint. Atomically: inserts checkpoint, updates `current_state`, advances `last_node_completed`. Idempotent — safe to retry.

**Request:**
```json
{
  "node_name": "analyse",
  "state":     { "input": "...", "analysis": "..." }
}
```

**Response `200 OK` — continue execution:**
```json
{
  "continue":    true,
  "halt_reason": null
}
```

**Response `200 OK` — halt (e.g. run was cancelled):**
```json
{
  "continue":    false,
  "halt_reason": "CANCELLED"
}
```

---

### `GET /runs/{id}/state`

Called by Python on startup to check for an existing checkpoint. Drives crash recovery. Returns `404` if no checkpoints exist (fresh start).

**Response `200 OK`:**
```json
{
  "run_id":             "a3f7c821-...",
  "last_node_completed": "analyse",
  "state":              { "input": "...", "analysis": "..." },
  "checkpointed_at":    "2024-11-15T09:01:22Z"
}
```

**Response `404 Not Found`:**
```json
{
  "type":   "https://etchflow.dev/errors/no-checkpoint",
  "title":  "No Checkpoint Found",
  "status": 404,
  "detail": "Run a3f7c821 has no committed checkpoints. Start from the beginning."
}
```

---

### `POST /runs/{id}/fail`

Called by Python when a node raises an exception. Triggers the retry engine. Python should call this before exiting or retrying locally.

**Request:**
```json
{
  "error": "OpenAI rate limit 429: Too Many Requests"
}
```

**Response `200 OK` — will retry:**
```json
{
  "status":       "RETRYING",
  "attempt_count": 1,
  "max_retries":   3,
  "retry_in_ms":  2000,
  "next_retry_at": "2024-11-15T09:05:02Z"
}
```

**Response `200 OK` — max retries exceeded:**
```json
{
  "status":       "DEAD",
  "attempt_count": 3,
  "max_retries":   3,
  "retry_in_ms":  null,
  "error":        "Max retries exceeded. Last error: OpenAI rate limit 429"
}
```

---

### `GET /runs/{id}`

Full run record including current status, last node, timestamps.

**Response `200 OK`:**
```json
{
  "run_id":             "a3f7c821-...",
  "status":             "RUNNING",
  "last_node_completed":"analyse",
  "attempt_count":      1,
  "max_retries":        3,
  "worker_id":          "activator-3",
  "started_at":         "2024-11-15T09:00:05Z",
  "updated_at":         "2024-11-15T09:01:22Z",
  "last_heartbeat_at":  "2024-11-15T09:01:20Z",
  "graph_definition":   { "nodes": [...], "edges": [...] }
}
```

---

### `GET /runs/{id}/checkpoints`

All node checkpoints in execution order. Used by `EtchFlowCheckpointSaver.list()`.

**Response `200 OK`:**
```json
[
  {
    "node_name":  "__start__",
    "state":      { "input": "..." },
    "created_at": "2024-11-15T09:00:05Z"
  },
  {
    "node_name":  "analyse",
    "state":      { "input": "...", "analysis": "..." },
    "created_at": "2024-11-15T09:01:22Z"
  }
]
```

---

### `GET /runs/{id}/logs`

Full audit trail in chronological order.

**Response `200 OK`:**
```json
[
  { "event_type": "SUBMITTED",      "message": "run created by python client",      "created_at": "..." },
  { "event_type": "CLAIMED",        "message": "activator-3 claimed run",           "created_at": "..." },
  { "event_type": "NODE_COMPLETED", "message": "node: analyse",                    "created_at": "..." },
  { "event_type": "NODE_COMPLETED", "message": "node: summarise",                  "created_at": "..." },
  { "event_type": "REAPED",         "message": "reset to PENDING — heartbeat stale","created_at": "..." },
  { "event_type": "CLAIMED",        "message": "activator-1 claimed run",           "created_at": "..." },
  { "event_type": "NODE_COMPLETED", "message": "node: report",                     "created_at": "..." },
  { "event_type": "SUCCESS",        "message": "run completed",                    "created_at": "..." }
]
```

---

### `POST /runs/{id}/cancel`

Immediately cancels a RUNNING or PENDING run.

**Response `200 OK`:**
```json
{ "run_id": "a3f7c821-...", "status": "CANCELLED" }
```

---

### `GET /health` / `GET /ready`

```json
{ "status": "ok",    "version": "1.0.0" }            // /health
{ "status": "ready", "postgres": "ok" }              // /ready
```

---

## 14. User Flow — Detailed

### Flow A — Happy Path (No Crash)

```
PYTHON PROCESS                    EtchFlow GO SERVICE                   POSTGRESQL
     │                                   │                               │
     │  1. build graph                   │                               │
     │  2. serialize topology            │                               │
     │  POST /runs ────────────────────▶ │                               │
     │                                   │ INSERT runs (PENDING) ───────▶│
     │ ◀── { run_id: "abc-123" } ──────── │ ◀── OK ───────────────────────│
     │                                   │                               │
     │  3. graph.invoke() starts         │                               │
     │     saver.get() called            │                               │
     │  GET /runs/abc-123/state ───────▶ │                               │
     │ ◀── 404 (no checkpoint) ─────────  │                               │
     │     LangGraph starts fresh        │                               │
     │                                   │                               │
     │                      Activator polls → claims run                 │
     │                                   │ UPDATE status=RUNNING ───────▶│
     │                                   │ Heartbeat goroutine starts    │
     │                                   │ INSERT agent_logs (CLAIMED) ──▶│
     │                                   │                               │
     │  [analyse node executes: LLM call]│                               │
     │  saver.put() called by LangGraph  │                               │
     │  PUT /checkpoint { node: analyse }│                               │
     │ ───────────────────────────────▶  │                               │
     │                                   │ BEGIN TRANSACTION             │
     │                                   │ INSERT checkpoints ──────────▶│
     │                                   │   ON CONFLICT DO NOTHING      │
     │                                   │ UPDATE current_state ────────▶│
     │                                   │ UPDATE last_node_completed ──▶│
     │                                   │ COMMIT ──────────────────────▶│
     │                                   │ INSERT agent_logs (NODE_DONE)─▶│
     │ ◀── { continue: true } ────────── │ ◀── OK ───────────────────────│
     │                                   │                               │
     │  [summarise node executes]        │                               │
     │  PUT /checkpoint { node: summarise│                               │
     │ ───────────────────────────────▶  │  (same atomic flow) ─────────▶│
     │ ◀── { continue: true } ────────── │                               │
     │                                   │                               │
     │  [report node executes]           │                               │
     │  PUT /checkpoint { node: report } │                               │
     │ ───────────────────────────────▶  │                               │
     │                                   │ UPDATE status=SUCCESS ───────▶│
     │                                   │ SET completed_at=NOW() ──────▶│
     │                                   │ INSERT agent_logs (SUCCESS) ──▶│
     │                                   │ Cancel heartbeat context      │
     │ ◀── { continue: false,            │                               │
     │       halt_reason: null } ─────── │                               │
     │                                   │                               │
     │  graph.invoke() returns ✅        │                               │
```

---

### Flow B — Crash Recovery (The Kill Test)

```
PYTHON PROCESS                    EtchFlow GO SERVICE                   POSTGRESQL
     │                                   │                               │
     │  [analyse, summarise checkpointed]│                               │
     │  [report node executing...]       │                               │
     │                                   │                               │
  ☠️  kill -9 <python_pid>              │                               │
     │                                   │                               │
     │              EtchFlow still running ✅│                               │
     │              Heartbeat goroutine exits (its Python context died)  │
     │              last_heartbeat_at stops being updated               │
     │                                   │                               │
     │              [~2 minutes pass]    │                               │
     │                                   │                               │
     │              Reaper fires (every 60s)                            │
     │                                   │ SELECT RUNNING runs           │
     │                                   │ WHERE last_heartbeat_at       │
     │                                   │   < NOW() - 120s ───────────▶│
     │                                   │ ◀── { id: "abc-123" } ────────│
     │                                   │ UPDATE status=PENDING ───────▶│
     │                                   │ INSERT agent_logs (REAPED) ──▶│
     │                                   │                               │
     │              Activator polls → claims abc-123                     │
     │                                   │ UPDATE status=RUNNING ───────▶│
     │                                   │ New heartbeat goroutine starts│
     │                                   │                               │
  Python restarts                        │                               │
     │                                   │                               │
     │  python example_graph.py --resume abc-123                        │
     │  graph.invoke() called            │                               │
     │  saver.get() called               │                               │
     │  GET /runs/abc-123/state ───────▶ │                               │
     │                                   │ SELECT last checkpoint ──────▶│
     │                                   │ ◀── { node: summarise, ... } ─│
     │ ◀── { last_node: "summarise",     │                               │
     │       state: {...} } ─────────── │                               │
     │                                   │                               │
     │  LangGraph loads checkpoint       │                               │
     │  Skips analyse ✅                 │                               │
     │  Skips summarise ✅               │                               │
     │                                   │                               │
     │  [report node executes]           │                               │
     │  PUT /checkpoint { node: report } │                               │
     │ ───────────────────────────────▶  │ UPDATE status=SUCCESS ───────▶│
     │ ◀── { continue: false } ──────── │ INSERT agent_logs (SUCCESS) ──▶│
     │                                   │                               │
     │  Completed ✅                     │                               │
     │  Zero LLM re-calls ✅             │                               │
     │  Audit log shows full history ✅  │                               │
```

---

### Flow C — Node Failure and Retry

```
     │  [summarise node raises exception in Python]                      │
     │                                                                   │
     │  Python catches exception                                         │
     │  POST /runs/abc-123/fail ───────▶ │                               │
     │  { "error": "rate limit 429" }    │                               │
     │                                   │ UPDATE attempt_count=1 ──────▶│
     │                                   │ UPDATE status=RETRYING ──────▶│
     │                                   │ SET next_retry_at=NOW()+2s ──▶│
     │                                   │ INSERT agent_logs (RETRYING)─▶│
     │ ◀── { status: RETRYING,           │                               │
     │       retry_in_ms: 2000 } ─────── │                               │
     │                                   │                               │
     │  Python sleeps 2s or exits        │                               │
     │                                   │                               │
     │              [2 seconds pass]     │                               │
     │              Retry goroutine fires│                               │
     │                                   │ UPDATE status=PENDING ───────▶│
     │                                   │                               │
     │              Activator claims run │                               │
     │  Python restarts or retries       │                               │
     │  GET /runs/abc-123/state ───────▶ │                               │
     │  ◀── { last_node: analyse }       │                               │
     │  Resumes from analyse ✅          │                               │
     │  (summarise had not checkpointed) │                               │
```

---

### Flow D — Concurrent Workers, No Collision

```
PYTHON PROCESS 1            PYTHON PROCESS 2         POSTGRESQL
     │                             │                       │
     │  POST /runs  (run A)        │                       │
     │ ──────────────────────────────────────────────────▶ │
     │                             │  POST /runs  (run B)  │
     │                             │ ────────────────────▶ │
     │                             │                       │
     │           ACTIVATOR 1             ACTIVATOR 2       │
     │                │                       │            │
     │ SELECT PENDING FOR UPDATE SKIP LOCKED  │            │
     │                │ ──────────────────────────────────▶│
     │                │ ◀── run A                          │
     │                │ UPDATE run A: RUNNING              │
     │                │                       │            │
     │                │  SELECT PENDING FOR UPDATE SKIP LOCKED
     │                │                       │ ─────────▶ │
     │                │                       │ ◀── run B  │
     │                │                       │ UPDATE B: RUNNING
     │                │                       │            │
     │  [run A executes on Python 1]           │            │
     │  [run B executes on Python 2]           │            │
     │                                         │            │
     │  No collision ✅ SKIP LOCKED guarantees │            │
     │  each run is claimed by exactly one activator        │
```

---

## 15. The Kill Test — Primary Demo

The Kill Test is the project's proof of concept. It proves the core value in a way no explanation can match.

### Setup

```bash
docker compose up --build          # EtchFlow + Postgres in one command
python python_adapter/example_graph.py   # Start an 8-node graph
```

Each node sleeps 5 seconds to simulate an LLM call. The whole graph takes ~40 seconds.

### The Test

```bash
# Terminal 1 — run the graph
python python_adapter/example_graph.py
# Run submitted: abc-123  (save this)
# [Node 1/8: extract]       ✓ checkpointed (5s)
# [Node 2/8: classify]      ✓ checkpointed (5s)
# [Node 3/8: analyse]       ✓ checkpointed (5s)
# [Node 4/8: summarise]     executing...

# Terminal 2 — kill Python mid-node
kill -9 $(pgrep -f example_graph.py)

# Terminal 3 — verify EtchFlow is still alive
curl http://localhost:8080/health
# { "status": "ok" }

# Verify last checkpoint is safe
curl http://localhost:8080/runs/abc-123/state
# { "last_node_completed": "analyse", "state": {...} }

# Verify audit trail shows the crash
curl http://localhost:8080/runs/abc-123/logs
# [ SUBMITTED, CLAIMED, NODE_COMPLETED×3, ... waiting for REAPED ]

# Wait ~2 minutes for Reaper, then resume
python python_adapter/example_graph.py --resume abc-123
# Resuming run: abc-123
# Last checkpoint: analyse (node 3/8)
# [Node 4/8: summarise]     executing...   ← resumes here
# [Node 5/8: draft]         ✓ checkpointed
# [Node 6/8: review]        ✓ checkpointed
# [Node 7/8: format]        ✓ checkpointed
# [Node 8/8: publish]       ✓ checkpointed
# Run complete ✅
```

### Success Criteria

```
✅ Python process killed between nodes — state not lost
✅ EtchFlow service still running after Python dies
✅ GET /runs/{id}/state returns correct last checkpoint
✅ Python restart loads checkpoint, skips completed nodes
✅ Final output identical to an uninterrupted run
✅ Audit log: SUBMITTED → CLAIMED → NODE_COMPLETED×3 → REAPED → CLAIMED → NODE_COMPLETED×5 → SUCCESS
✅ Nodes 1–3 never re-executed (no duplicate LLM spend)
```

---

## 16. Build Phases

### Phase MVP — Crash Recovery, No Queue

> **Goal:** Prove the Kill Test end-to-end. This is the entire project's reason for existing.

No worker pool. No reaper. No retry. Just the atomic checkpoint and state recovery mechanism.

```
DATABASE
[ ] Migration 001: runs table (id, graph_definition, input_data, current_state,
                   status, last_node_completed, created_at, updated_at)
[ ] Migration 002: checkpoints table + UNIQUE(run_id, node_name)
[ ] Migration 003: agent_logs table

GO SERVICE
[ ] config.go: viper loader, fail fast on missing DATABASE_URL
[ ] models: Run, Checkpoint, AgentLog structs
[ ] statemachine/transitions.go: PENDING→RUNNING→SUCCESS/FAILED only for MVP
[ ] store/run_store.go
[ ] store/checkpoint_store.go: INSERT ... ON CONFLICT DO NOTHING + conditional UPDATE
[ ] store/log_store.go

API ENDPOINTS (4 only)
[ ] POST /runs          → insert PENDING run, return run_id
[ ] PUT  /runs/{id}/checkpoint  → atomic save (the core endpoint)
[ ] GET  /runs/{id}/state       → return last checkpoint or 404
[ ] GET  /health                → liveness

PYTHON ADAPTER
[ ] etchflow_client.py: submit_run, get_state, save_checkpoint
[ ] etchflow_checkpoint_saver.py: get(), put() (list() can be a stub)
[ ] graph_serializer.py: serialize_graph()
[ ] example_graph.py: 8-node graph with --resume flag

DOCKER
[ ] Dockerfile: multi-stage build
[ ] docker-compose.yml: etchflow + postgres

KILL TEST
[ ] Makefile: make kill-test runs the full scenario
[ ] README: Kill Test is the first section, with expected output
```

**Definition of Done:** Kill a Python LangGraph process mid-execution. Restart it. It resumes exactly where it stopped. First-time demo works every time.

---

### Phase 1.5 — Worker Pool, Reaper, Heartbeat, Retry

> **Goal:** Make the system self-healing. No manual intervention needed after a crash.

Build on top of MVP. Do not change the checkpoint or recovery logic.

```
DATABASE
[ ] Migration 004: add worker_id, started_at, last_heartbeat_at,
                   attempt_count, max_retries, base_delay_ms,
                   max_delay_ms, next_retry_at, last_error columns

GO SERVICE
[ ] queue/postgres_queue.go: ClaimNext() with SKIP LOCKED
[ ] worker/pool.go: ActivatorPool, spawns N goroutines
[ ] worker/activator.go: poll loop, non-blocking claim, context management
[ ] worker/heartbeat.go: pings last_heartbeat_at every 30s
[ ] reaper/reaper.go: runs on startup + every 60s, resets stale RUNNING→PENDING
[ ] retry/policy.go: backoff + jitter formula, calculates next_retry_at
[ ] Background goroutine: resets RETRYING→PENDING when next_retry_at passes
[ ] statemachine: add RETRYING, DEAD, CANCELLED, TIMEOUT transitions

API ENDPOINTS (additions)
[ ] GET  /runs/{id}             → full run record
[ ] GET  /runs/{id}/checkpoints → checkpoint history (for saver.list())
[ ] GET  /runs/{id}/logs        → audit trail
[ ] POST /runs/{id}/fail        → trigger retry or DEAD
[ ] POST /runs/{id}/cancel      → cancel run
[ ] GET  /ready                 → readiness (Postgres ping)

TESTING
[ ] Integration test: happy path (all nodes complete)
[ ] Integration test: crash recovery (insert RUNNING, trigger reaper, verify PENDING)
[ ] Integration test: SKIP LOCKED (two activators, one run — only one claims)
[ ] Integration test: idempotent checkpoint (same node twice, verify one row)
[ ] Integration test: retry (fail 3 times, verify DEAD)
```

---

### Phase 2 — Token Budget & Cost Governance

> Deferred from Phase 1. Keeping cost tracking out of the critical path until crash recovery is solid.

```
[ ] token_budget field on runs table (0 = unlimited)
[ ] tokens_used accumulator updated atomically per checkpoint
[ ] Budget check in PUT /checkpoint handler: if tokens_used > budget → halt
[ ] Checkpoint response: { continue: false, halt_reason: "BUDGET_EXCEEDED" }
[ ] Python adapter: halts graph cleanly on continue: false
[ ] Last clean checkpoint preserved on budget halt
[ ] status: BUDGET_EXCEEDED added to state machine
[ ] GET /runs/{id}: expose tokens_used, token_budget, tokens_remaining
```

---

### Phase 3 — Kafka Queue

> Replace `SKIP LOCKED` with Kafka for horizontal scale across multiple EtchFlow instances.

**Important note before implementing:** `confluent-kafka-go` uses CGO (wraps `librdkafka`). A CGO binary cannot run in the `distroless/static` image used in Phase MVP. Switch to `github.com/twmb/franz-go` (pure Go, no CGO) to keep the distroless image — or change the Docker base to `distroless/cc-debian12`.

```
[ ] Switch to franz-go (pure Go Kafka client)
[ ] Topic: etchflow.runs.submitted
[ ] Topic: etchflow.runs.events
[ ] Consumer group: prevents duplicate execution across EtchFlow instances
[ ] Update docker-compose.yml: add Kafka + Zookeeper
[ ] Load test: 50 concurrent runs, zero state corruption
```

---

### Phase 4 — Observability

```
[ ] Prometheus metrics endpoint: GET /metrics
    - etchflow_runs_total{status}
    - etchflow_checkpoint_duration_seconds histogram
    - etchflow_retry_total{reason}
    - etchflow_activator_pool_utilization
[ ] OpenTelemetry tracing: one trace per run, one span per node
[ ] Grafana dashboards shipped with the project
[ ] GET /runs/{id}/stream SSE endpoint: live node events
```

---

### Phase 5 — gRPC + Performance

```
[ ] Proto: RunService (CreateRun, SaveCheckpoint, GetState, CancelRun)
[ ] gRPC server alongside REST
[ ] Python gRPC adapter
[ ] Benchmark: REST vs gRPC at 100 runs/second
```

---

### Phase 6 — Kubernetes

EtchFlow pods are fully stateless (all state in Postgres + Kafka). Horizontal scaling requires no changes to the codebase.

```
[ ] Deployment manifest with replicas: 3
[ ] HorizontalPodAutoscaler via KEDA (Kafka lag metric)
[ ] ConfigMap + Secret for env vars
[ ] Liveness probe: GET /health
[ ] Readiness probe: GET /ready
[ ] CloudNativePG or AWS RDS for managed Postgres
```

---

## 17. Docker Packaging

### Dockerfile

Phase MVP uses `distroless/static` because the binary is pure Go (CGO disabled). This is safe until Phase 3 adds Kafka. If `confluent-kafka-go` is chosen at Phase 3, switch to `distroless/cc-debian12`. If `franz-go` is chosen, `distroless/static` remains valid.

```dockerfile
# ── Stage 1: Build ────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-w -s" \
    -o etchflow ./cmd/server

# ── Stage 2: Minimal Runtime ──────────────────────────────────────────
# Safe for Phase MVP + 1.5 (pure Go, CGO disabled).
# Change to distroless/cc-debian12 only if you add a CGO dependency.
FROM gcr.io/distroless/static-debian12

COPY --from=builder /app/etchflow /etchflow

EXPOSE 8080
ENTRYPOINT ["/etchflow"]
```

### docker-compose.yml

```yaml
version: "3.9"

services:

  etchflow:
    build: .
    ports:
      - "8080:8080"
    env_file: .env
    environment:
      - DATABASE_URL=postgres://etchflow:etchflow@postgres:5432/etchflow?sslmode=disable
    depends_on:
      postgres:
        condition: service_healthy
    restart: unless-stopped

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER:     etchflow
      POSTGRES_PASSWORD: etchflow
      POSTGRES_DB:       etchflow
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./migrations:/docker-entrypoint-initdb.d
    ports:
      - "5432:5432"
    healthcheck:
      test:     ["CMD-SHELL", "pg_isready -U etchflow"]
      interval: 5s
      timeout:  5s
      retries:  5

volumes:
  postgres_data:
```

### Makefile

```makefile
.PHONY: run stop test migrate kill-test

run:
	docker compose up --build -d

stop:
	docker compose down

migrate:
	docker compose exec etchflow ./etchflow migrate

test:
	go test ./... -v -count=1

# Full Kill Test — runs automatically, prints pass/fail
kill-test:
	@echo "=== EtchFlow Kill Test ==="
	@python python_adapter/example_graph.py & echo $$! > /tmp/etchflow_pid
	@sleep 18
	@echo ">>> Killing Python at node 4..."
	@kill -9 $$(cat /tmp/etchflow_pid) 2>/dev/null || true
	@echo ">>> Verifying EtchFlow is alive..."
	@curl -sf http://localhost:8080/health | grep '"status":"ok"'
	@echo ">>> Checking last checkpoint..."
	@curl -sf http://localhost:8080/runs/$$(cat /tmp/etchflow_run_id)/state | python -m json.tool
	@echo ">>> Waiting for Reaper (130s)..."
	@sleep 130
	@echo ">>> Resuming..."
	@python python_adapter/example_graph.py --resume $$(cat /tmp/etchflow_run_id)
	@echo "=== Kill Test Complete ==="
```

---

## 18. Future Phases

Summarised. Full specs written when the prior phase is complete and stable.

| Phase | Goal | Key Addition |
|:---|:---|:---|
| Phase 2 | Cost governance | Token budget enforcement, BUDGET_EXCEEDED status |
| Phase 3 | Scale queue | Kafka (`franz-go`), consumer groups, multi-instance EtchFlow |
| Phase 4 | Observability | Prometheus metrics, OpenTelemetry traces, Grafana |
| Phase 5 | Performance | gRPC checkpoint endpoint, connection pool tuning |
| Phase 6 | Kubernetes | HPA via KEDA, managed Postgres, stateless pods |
| Phase 7 | Python SDK | `pip install etchflow-client`, publish to PyPI |

---

## 19. What EtchFlow Is and Is Not

### ✅ What EtchFlow Is

- A **distributed state machine backed by Postgres**
- A **durable execution engine** for LangGraph DAG workflows
- An **external service** that survives Python process death
- A **reliability guarantee** for long-running, expensive LLM pipelines
- A **crash recovery system** that resumes from the last committed node

### ❌ What EtchFlow Is Not

- Not a replacement for LangGraph or LangChain
- Not an agent framework or reasoning engine
- Not an orchestrator that calls Python (Python calls EtchFlow)
- Not a visual workflow builder
- Not a prompt engineering tool
- Not a message broker (Phase 1 uses Postgres as queue)

---

## Interview One-Liner

**"What does EtchFlow solve that LangGraph doesn't already solve with its built-in Postgres checkpointer?"**

> LangGraph's `PostgresSaver` saves state to Postgres, but it runs **inside the Python process**. When the process dies, the checkpointer dies with it. Any inflight write is lost. EtchFlow is an **external service** — a completely separate process. It survives Python's death. It persists checkpoints atomically with `ON CONFLICT DO NOTHING` so retries never double-count. It runs a Reaper that detects dead workers via heartbeat staleness and requeues their runs automatically. It uses `SELECT FOR UPDATE SKIP LOCKED` so multiple Python processes can run concurrently against the same queue without collision. And critically — Python always calls EtchFlow, EtchFlow never calls Python. The checkpoint saver is a library. EtchFlow is infrastructure.

---

*EtchFlow — Because your LLM pipeline shouldn't start over just because a pod restarted.*
