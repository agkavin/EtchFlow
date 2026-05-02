# EtchFlow

### The Problem
Building durable, long-running agentic workflows (like with LangGraph) usually requires buying into an entire ecosystem like Temporal. This forces you to completely rewrite your Python logic into complex workflow primitives, activities, and signals. While LangGraph provides in-memory state tracking natively, deploying it reliably to production where pods can crash, timeout, or be preempted means you lose state midway and have to start expensive LLM generations from scratch.

### The Solution
EtchFlow is a **Bring-Your-Own-Compute (BYOC) Durable Execution Engine** specifically designed for LangGraph. It acts as an external state machine and checkpointing backend. You write standard Python LangGraph code without workflow primitives, and EtchFlow natively plugs into LangGraph's `BaseCheckpointSaver`. If your Python process crashes, you simply restart it, and EtchFlow fast-forwards your state to the exact node where it died.

### Features (Phase MVP)
- **Zero-Rewrite Integration**: Plugs directly into LangGraph via `BaseCheckpointSaver`.
- **Atomic Checkpointing**: Fully idempotent state saves using Postgres `ON CONFLICT DO NOTHING`.
- **Crash Recovery**: Transparently resumes graphs from the exact node of failure.
- **BYOC Architecture**: The Go engine just handles state; Python handles the execution.

---

## Architecture Flow

```text
+-------------------+           1. submit_run()            +----------------------+
|                   | -----------------------------------> |                      |
|  Python LangGraph |                                      |  EtchFlow Go Engine  |
|  (App Compute)    | <----------------------------------- |  (State & Durability)|
|                   |           2. Returns run_id          |                      |
+-------------------+                                      +----------------------+
        |                                                              |
        |  3. graph.invoke()                                           |
        v                                                              v
+-------------------+           4. Fetch last state        +----------------------+
| LangGraph Routing | -----------------------------------> | Postgres DB          |
| (Skip finished)   | <----------------------------------- | (runs, checkpoints)  |
+-------------------+                                      +----------------------+
        |                                                              |
        |  5. Execute Node (e.g. LLM call)                             |
        v                                                              |
+-------------------+           6. saver.put()             +----------------------+
| Node Completion   | -----------------------------------> | Atomic TX            |
| (State Updated)   |                                      | - Save Checkpoint    |
+-------------------+                                      | - Update Run State   |
        |                                                  +----------------------+
        |  7. Loop until Finish Node
        v
+-------------------+
|  Graph Success    |
+-------------------+
```

---

## How to Run

### 1. Start the EtchFlow Engine
EtchFlow is packaged as a Docker Compose stack containing the Go server and PostgreSQL.
```bash
# Start the database and backend API
make run
```

### 2. Install Python Dependencies
```bash
cd python_adapter
pip install -r requirements.txt
```

### 3. Run the "Kill Test" (Crash Recovery Demo)
The included kill test spins up an 8-node mock LangGraph execution, deliberately force-kills the Python process midway, and then seamlessly resumes it from the exact node it died on.
```bash
make kill-test
```
