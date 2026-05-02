# EtchFlow

### The Problem
Building durable, long-running agentic workflows (like with LangGraph) usually requires buying into an entire ecosystem like Temporal. This forces you to completely rewrite your Python logic into complex workflow primitives, activities, and signals. While LangGraph provides in-memory state tracking natively, deploying it reliably to production where pods can crash, timeout, or be preempted means you lose state midway and have to start expensive LLM generations from scratch.

### The Solution
EtchFlow is a **Bring-Your-Own-Compute (BYOC) Durable Execution Engine** specifically designed for LangGraph. It acts as an external state machine and checkpointing backend. You write standard Python LangGraph code without workflow primitives, and EtchFlow natively plugs into LangGraph's `BaseCheckpointSaver`. If your Python process crashes, you simply restart it, and EtchFlow fast-forwards your state to the exact node where it died.

### Features (Phase MVP)
- **Zero-Boilerplate SDK**: Wraps LangGraph directly. Just use `etchflow.compile(builder)`.
- **Atomic Checkpointing**: Fully idempotent state saves using Postgres `ON CONFLICT DO NOTHING`.
- **Crash Recovery**: Transparently resumes graphs from the exact node of failure using your own string `thread_id`.
- **Professional SDK**: Clean `etchflow` package with standard Python distribution structure.

---

## Architecture Flow

```text
+-------------------+           1. app.invoke({"topic"...})    +----------------------+
|                   | -----------------------------------> |                      |
|  Python LangGraph |           (Uses standard thread_id)  |  EtchFlow Go Engine  |
|  (App Compute)    | <----------------------------------- |  (State & Durability)|
|                   |           2. Graph initialized       |                      |
+-------------------+                                      +----------------------+
        |                                                              |
        |  3. Fetch last state (auto-resume if crashed)                |
        v                                                              v
+-------------------+                                      +----------------------+
| LangGraph Routing | -----------------------------------> | Postgres DB          |
| (Skip finished)   | <----------------------------------- | (runs, checkpoints)  |
+-------------------+                                      +----------------------+
        |                                                              |
        |  4. Execute Node (e.g. LLM call)                             |
        v                                                              |
+-------------------+           5. saver.put()             +----------------------+
| Node Completion   | -----------------------------------> | Atomic TX            |
| (State Updated)   |                                      | - Save Checkpoint    |
+-------------------+                                      | - Update Run State   |
        |                                                  +----------------------+
        |  6. Loop until Finish Node
        v
+-------------------+
|  Graph Success    |
+-------------------+
```

---

## Project Structure

- `cmd/server/`: Go service entry point.
- `internal/`: Core Go logic (models, state machine, database store).
- `etchflow-py-sdk/`: The official Python SDK source code.
- `examples/`: Ready-to-run demo workflows.
- `scripts/`: Utility scripts like `kill-test.sh`.
- `migrations/`: Database schema definitions.

---

## How to Run

### 1. Start the EtchFlow Engine
EtchFlow is packaged as a Docker Compose stack containing the Go server and PostgreSQL.
```bash
# Start the database and backend API
make run
```

### 2. Install Python dependencies
Ensure you have the required libraries installed in your environment:
```bash
pip install langgraph langchain-core httpx
```

### 3. Run the "Kill Test" (Crash Recovery Demo)
The included kill test spins up a 5-node LangGraph execution, deliberately force-kills the Python process midway, and then seamlessly resumes it from the exact node it died on.
```bash
make kill-test
```
