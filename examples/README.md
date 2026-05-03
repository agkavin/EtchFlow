# EtchFlow Examples

## Quick Start

```bash
# Start EtchFlow
make run

# Run demo
make test

# Resume last run
make test-resume

# Kill test (crash recovery)
make test-kill
```

---

## Demo Mode (`demo.py`)

Direct invocation - user controls execution:

```bash
python examples/demo.py                    # Fresh run
python examples/demo.py --resume           # Resume from saved run_id
python examples/demo.py --resume <id>     # Resume specific run
```

**Flow:**
```
User → EtchFlow.compile() → graph.invoke()
                ↓
         LangGraph executes nodes
                ↓
         After each node: saver.put() → PUT /checkpoint
                ↓
         On completion: client.complete_run() → status=SUCCESS
```

**Use case:** Single workflows, development, ad-hoc runs

---

## Worker Mode (`worker_demo.py` + `submit_job.py`)

Batch processing - workers poll for work:

```bash
# Terminal 1: Start worker (polls for PENDING runs)
make worker

# Terminal 2: Submit jobs
make submit
```

**Flow:**
```
User → client.submit_run() → run created as PENDING
                                    ↓
Worker → POST /runs/claim → atomic claim (SKIP LOCKED)
                                    ↓
Worker → execute graph, send heartbeats
                                    ↓
If worker dies → Go Reaper detects stale heartbeat
                 → reset to PENDING
                 → another worker picks it up
```

**Use case:** Production batch processing, multiple workers

---

## Files

| File | Mode | Description |
|------|------|-------------|
| `demo.py` | Direct | Zero-boilerplate single-run demo |
| `worker_demo.py` | Worker | Polling worker for batch jobs |
| `submit_job.py` | Worker | Submit jobs to worker queue |

---

## Key Concepts

1. **thread_id** = run_id - the same string links checkpoint to run
2. **Checkpoint** - saved after each node, enables crash recovery
3. **Worker** - polls PENDING runs, handles concurrency + heartbeats
4. **Reaper** - detects stale runs (no heartbeat), resets to PENDING
5. **Retry Engine** - on failure, exponential backoff before retry