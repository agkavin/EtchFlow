# Phase 1.5: Worker Pool, Reaper, Heartbeat, and Retry

## Goal
Make the system **self-healing**. After a crash or node failure, EtchFlow should automatically recover without requiring manual intervention, process restarts, or manual retry triggers.

This phase builds on the MVP by adding autonomous coordination mechanisms. The core execution of LangGraph nodes **remains in user-controlled Python processes**; EtchFlow coordinates, leases, monitors, and retries those executions.

---

## Table of Contents
1. [Overview of Changes](#overview-of-changes)
2. [Database Migration (004)](#database-migration-migration-004)
3. [Architecture & Components](#architecture--components)
4. [Python Side Integration](#python-side-your-compute)
5. [Deployment Patterns](#typical-deployment-patterns)
6. [Worker Implementation & Deployment](#worker-implementation--deployment)
7. [Deep Dive: Implementation Details & FAQ](#deep-dive-implementation-details--faq)
8. [Checklist: Key Concepts to Implement](#key-concepts--terms-to-implement)

---

## Overview of Changes

| Feature | MVP (Current) | Phase 1.5 (Added) |
| :--- | :--- | :--- |
| **Crash Recovery** | Manual: Restart process with same `run_id`. | **Automatic**: Worker pool detects and resumes `PENDING` runs. |
| **Leasing** | None. | **Leasing & Heartbeats**: Activators claim runs via `SKIP LOCKED`. |
| **Retries** | None. | **Retry Engine**: Exponential backoff on `POST /fail`. |
| **Liveness** | Stalled workers go undetected. | **Reaper**: Scans for stale heartbeats and resets runs. |
| **Visibility** | Limited. | **New Endpoints**: `/runs/{id}`, `/checkpoints`, `/logs`, `/ready`. |
| **Control** | Manual only. | **Lifecycle API**: `/fail` and `/cancel` endpoints for user code. |

**Result**: A robust, self-recovering system where crashes and failures are handled autonomously.

---

## Database Migration (Migration 004)

Adds columns to the `runs` table to support autonomous coordination.

| Column | Type | Purpose |
| :--- | :--- | :--- |
| `worker_id` | `TEXT` | ID of the activator currently leasing the run (NULL if unclaimed). |
| `started_at` | `TIMESTAMPTZ` | Timestamp when the run first entered `RUNNING`. |
| `last_heartbeat_at`| `TIMESTAMPTZ` | Updated every ~30s; used by reaper to detect staleness. |
| `attempt_count` | `INTEGER` | Number of attempts made (for retry logic). |
| `max_retries` | `INTEGER` | Maximum allowed retry attempts. |
| `base_delay_ms` | `INTEGER` | Initial delay for exponential backoff. |
| `max_delay_ms` | `INTEGER` | Upper bound for backoff delay. |
| `next_retry_at` | `TIMESTAMPTZ` | When the run should flip back to `PENDING` for retry. |
| `last_error` | `TEXT` | Error message from the most recent failure. |

---

## Architecture & Components

EtchFlow coordinates execution via several internal background processes (goroutines).

### 1. Activator Pool (Worker Pool)
- Runs as Go goroutines inside the EtchFlow service.
- Polls the database every `WORKER_POLL_INTERVAL_SECONDS` (default: 2s).
- **Lease Logic**:
  ```sql
  SELECT * FROM runs
  WHERE status = 'PENDING'
  ORDER BY created_at ASC
  LIMIT 1
  FOR UPDATE SKIP LOCKED
  ```
- **On Claim**: Atomically sets status to `RUNNING`, records `worker_id`, sets `started_at`, and initializes the heartbeat.

### 2. Heartbeat
- A dedicated goroutine for each active run.
- Updates `last_heartbeat_at` every `WORKER_HEARTBEAT_INTERVAL_SECONDS` (default: 30s).
- Ensures the system knows the activator (and by extension, the lease) is still healthy.

### 3. Reaper
- Runs periodically (default: 60s).
- Scans for runs where `status = 'RUNNING'` and `last_heartbeat_at` is older than the `STALE_THRESHOLD_SECONDS` (default: 120s).
- **Recovery**: Resets stalled runs to `PENDING` and clears lease metadata.

### 4. Retry Engine
- Triggered by `POST /runs/{id}/fail`.
- **Logic**:
  1. If `fatal: true` is provided in the request body:
     - Immediately marks run as `DEAD`.
     - Records `last_error` and skips any retry attempts.
  2. Else:
     - Increments `attempt_count`.
     - If `attempt_count < max_retries`:
       - Computes exponential backoff: `now() + (base_delay_ms * 2^attempt) + jitter`.
       - Sets status to `RETRYING` and updates `next_retry_at`.
     - Else: Marks run as `DEAD`.
- A background scanner flips `RETRYING` runs back to `PENDING` once the backoff expires.

---

## Python Side (Your Compute)

Your execution code remains largely the same, but benefits from the new orchestration.

1. **Submit**: `client.submit_run(graph, input_data) -> run_id`.
2. **Setup**: Initialize `EtchFlowCheckpointSaver(client, run_id)`.
3. **Execute**: `graph.invoke(..., config={"thread_id": run_id})`.
4. **Resumption**: The saver automatically fetches the latest state via `GET /state`.
5. **Failure Handling**: Wrap execution in a try/except. Call `client.fail_run(run_id, error)` for retriable errors, or `client.fail_run(run_id, error, fatal=True)` to permanently kill a run with a logic error.

> [!IMPORTANT]
> **EtchFlow does not start your Python code.** You are responsible for running the process that calls `graph.invoke()`. EtchFlow only manages the *status* and *lease* of the run.

---

## Managed SDK Worker (Pull Model)

EtchFlow provides a robust, "enterprise-grade" `Worker` class within the Python SDK. This completely hides the complexity of polling, backoff, concurrency, and signal handling from the user. 

Instead of writing manual `while True` loops or managing webhooks and queues, users start the worker with a single line of code.

### 1. The Single-Line Implementation
To run an EtchFlow worker, the user simply instantiates the `Worker` class with their compiled LangGraph graph and starts it.

```python
from etchflow import EtchFlowClient, Worker
from my_agent import graph # Your compiled LangGraph

client = EtchFlowClient(base_url="http://localhost:8080")
worker = Worker(client, builder)
# One line to start a fully managed, self-healing worker pool
worker.start(concurrency=5)
```

**What the SDK Worker handles under the hood:**
1. **Polling:** Safely polls the EtchFlow API for runs that have transitioned from `PENDING` to `RUNNING`.
2. **Concurrency:** Uses a ThreadPoolExecutor (or Asyncio) to process multiple runs simultaneously, respecting the `concurrency` limit to provide natural backpressure.
3. **Signal Handling:** Listens for `SIGINT` and `SIGTERM` to perform graceful shutdowns, ensuring no state corruption if the container scales down.
4. **Resumption & Auto-Restart:** If the Python worker dies, the EtchFlow backend (Reaper) notices the missed heartbeats and re-queues the run. A new Worker instance automatically picks it up and resumes exactly where it left off using the checkpoint history.
5. **Failure Reporting:** Unhandled exceptions in the user's graph are automatically caught and sent to `POST /runs/{id}/fail` to trigger EtchFlow's exponential backoff engine.

### 2. Deployment Patterns

Because the SDK provides a robust, long-lived process, deploying EtchFlow workers follows standard infrastructure orchestration. The worker is meant to run continuously in the background.

| Deployment Target | Recommended Approach |
| :--- | :--- |
| **Docker / Compose** | Run a dedicated container executing `python my_worker.py` with `restart: unless-stopped`. |
| **Kubernetes** | Create a `Deployment` with your desired number of replicas. Scaling out compute is native via `kubectl scale deployment`. |
| **Systemd** | Run the worker script as a service configured with `Restart=always` on a standard VM. |
| **Sidecar (K8s)** | Add the worker as a secondary container inside your main API pod if they share the same codebase and dependencies. |

### 3. Why this is the "Professional" Way
- **Zero Infrastructure Boilerplate:** Users don't manage queues (like RabbitMQ or Redis), loops, or backoff logic.
- **Built-in Backpressure:** The worker only fetches work when it has capacity, preventing the user's compute (and LLM API limits) from being overwhelmed.
- **Secure & Simple:** Because the worker *pulls* from EtchFlow, there is no need to expose public webhook endpoints, manage firewall rules/NAT routing, or implement request signature verification.

---

## Life of a Run: The Self-Healing Flow

To illustrate how Phase 1.5 works in practice, here is the lifecycle of a 5-node graph (`Node 1` → `Node 2` → `Node 3` → `Node 4` → `Node 5`) that suffers a catastrophic crash at **Node 4**.

### 1. The Claim (Go Side)
A user submits a run. The **Go Activator** sees the `PENDING` run in Postgres and uses `SKIP LOCKED` to claim it. 
- **State:** `status = RUNNING`, `worker_id` is set.
- **Heartbeat:** A goroutine in the Go engine starts tracking the liveness of this specific lease.

### 2. The Pickup (Python Side)
A healthy **Managed SDK Worker** polls EtchFlow, sees the `RUNNING` task, and fetches it. 
- **Worker Action:** It initializes the graph and calls `app.invoke()`.
- **Internal Heartbeat:** The SDK Worker begins sending periodic liveness pings to the Go engine.

### 3. Successful Progress (Nodes 1-3)
The worker executes the first three nodes successfully. After each node completes, the SDK calls `PUT /checkpoint`.
- **Persistence:** The DB records the latest successful state as "Node 3".

### 4. The Crash (Node 4)
The worker starts Node 4. Mid-execution, the Python process dies (e.g., OOM, Pod preemption, or hardware failure).
- **The Gap:** The checkpoint for Node 4 is **never called**. 
- **Silence:** The Python worker stops sending liveness pings to the Go engine.

### 5. The Detection (Go Reaper)
After the `STALE_THRESHOLD` (e.g., 120s), the Go **Reaper** detects the missing heartbeats.
- **Action:** The Reaper "recycles" the run by resetting it to `PENDING` and clearing the stale worker lease.

### 6. The Resurrection (Auto-Restart)
Moments later, the **Activator** claims the run again, and a healthy **Python Worker** pulls the task.
1.  The worker calls `app.invoke()`.
2.  The SDK fetches the last state from EtchFlow (**Node 3**).
3.  **LangGraph Fast-Forward:** LangGraph sees the checkpoint for Node 3 and automatically skips the already-finished nodes, resuming execution directly at **Node 4**.
4.  The graph completes successfully.

---

## Deep Dive: Implementation Details & FAQ

### Why Polling?
We use `SELECT ... FOR UPDATE SKIP LOCKED` because:
- **Simplicity**: Atomic operation handled entirely by Postgres.
- **Reliability**: No need for complex websocket or message bus infrastructure.
- **Back-pressure**: Parallelism is naturally capped by the `MAX_WORKERS` setting.

### Does this limit parallelism?
Yes, by design. The number of concurrent runs is bounded by the activator pool size. This provides built-in back-pressure. You can scale horizontally by running more EtchFlow replicas; the database-level locks ensure coordination across all instances.

### Why not launch user code directly?
EtchFlow is a **coordination service**, not a job orchestrator like Airflow or Celery. Spawning Python processes would require managing venvs, dependencies, and environment configurations—multiplying complexity. By following a "Bring Your Own Compute" (BYOC) model, EtchFlow remains lightweight and language-agnostic.

### Does recovery always depend on the Reaper timeout?
It depends on the failure type:
1.  **Hard Crashes** (e.g., OOM, Pod kill, Segfault): **Yes.** Since the Python process is gone, EtchFlow must wait for the heartbeat to expire before the Reaper re-queues the run.
2.  **Soft Failures** (e.g., LLM API error, Python Exception): **No.** The **Managed Worker** catches the exception and immediately calls `POST /fail`. This triggers the **Retry Engine** to schedule a rerun (either instantly or with backoff) without waiting for the Reaper.

### Comparison: Component Responsibilities

| Component | Responsibility | Environment |
| :--- | :--- | :--- |
| **Activator Pool** | Claims `PENDING` runs, marks `RUNNING`, starts heartbeat. | Go (EtchFlow) |
| **Heartbeat** | Keeps lease alive by updating `last_heartbeat_at`. | Go (EtchFlow) |
| **Reaper** | Rescues stalled runs (stale heartbeats) back to `PENDING`. | Go (EtchFlow) |
| **Retry Engine** | Manages backoff and retry cycles after failures. | Go (EtchFlow) |
| **Python Worker** | **Executes the LangGraph nodes**, LLM calls, and logic. | **Your Environment** |

### Design Alternatives Considered
- **Postgres LISTEN/NOTIFY**: Could replace polling to eliminate periodic wake-ups while preserving the `SKIP LOCKED` logic.
- **Webhook / Callback**: EtchFlow could POST to a URL when a run is ready. This would move "pull" responsibility to the user but add complexity (retries, security).
- **Hybrid**: Keep the activator for leasing but expose a lightweight `/pending` endpoint for long-polling or websocket clients.

---

## Phase 1.5 Implementation Checklist

### 1. Database (Migration 004)
- [x] **Schema Update**: Implement Migration 004 to add `worker_id`, `started_at`, `last_heartbeat_at`, `attempt_count`, `max_retries`, `base_delay_ms`, `max_delay_ms`, `next_retry_at`, `last_error` to the `runs` table.

### 2. Go Backend (The Orchestrator)
- [x] **Leasing Logic**: Implement `ClaimNextRun()` in `run_store.go` using `SELECT ... FOR UPDATE SKIP LOCKED`.
- [x] **Activator Pool**: Internal goroutines that poll for `PENDING` runs and transition them to `RUNNING`.
- [x] **Reaper Service**: Scans for `RUNNING` runs with stale `last_heartbeat_at` and resets them to `PENDING`.
- [x] **Retry Engine**: Calculates exponential backoff + jitter and manages the `RETRYING` state.
- [x] **Retry Scanner**: Background goroutine that flips `RETRYING` → `PENDING` when `next_retry_at` is reached.
### 3. API Endpoints (New)
- [x] `GET /runs/{id}`: Fetch full run record.
- [ ] `GET /runs/{id}/checkpoints`: Historical list for `saver.list()`.
- [ ] `GET /runs/{id}/logs`: Audit trail of events (CLAIMED, REAPED, etc.).
- [x] `PUT /runs/{id}/heartbeat`: **[CRITICAL]** Updates `last_heartbeat_at`.
- [x] `POST /runs/{id}/fail`: Supports `{ "error": "string", "fatal": bool }`.
- [x] `POST /runs/{id}/cancel`: Signal run cancellation.
- [ ] `GET /ready`: Readiness check (Postgres ping).

### 4. Python SDK (The Worker)
- [x] **`etchflow.Worker` Class**: Managed execution loop with `ThreadPoolExecutor` concurrency.
- [x] **Heartbeat Thread**: Background task that pings `PUT /heartbeat` every 30s while a graph is running.
- [x] **Graceful Shutdown**: `SIGTERM/SIGINT` handling to allow the current node to finish before exiting.
- [x] **Automatic Failure Reporting**: Try/Except wrapper that calls `POST /fail` on unhandled exceptions.

### 5. Verification & Testing
- [ ] **Integration: "The Kill Test"**: Verify that a killed Python process is re-queued by the Reaper and resumed by a different worker.
- [ ] **Integration: "The Fatal Test"**: Verify that a logic error with `fatal=True` skips retries and goes straight to `DEAD`.
- [ ] **Integration: "The Backpressure Test"**: Verify that workers only claim runs up to their `concurrency` limit.