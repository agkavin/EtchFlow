# EtchFlow

EtchFlow is a Postgres-backed durable execution engine for LangGraph AI workflows. It ensures that your complex, long-running agentic workflows can recover from crashes and resume exactly where they stopped, with zero re-execution of completed nodes.

## 🚀 Quick Start

### 1. Requirements
- Docker & Docker Compose
- Python 3.11+
- Go 1.22+ (only for local development outside Docker)

### 2. Start EtchFlow
```bash
make run
```
This starts the EtchFlow Go service and a PostgreSQL 16 database. Migrations are automatically applied on startup.

### 3. Setup Python Environment
```bash
cd python_adapter
pip install -r requirements.txt
```

### 4. Run the Demo
```bash
python python_adapter/example_graph.py
```

## 🧪 The Kill Test (Crash Recovery Demo)

The "Kill Test" is the ultimate proof of EtchFlow's durability. It simulates a process crash mid-execution and demonstrates how the workflow resumes.

### Automated Kill Test
```bash
make kill-test
```

### Manual Kill Test Walkthrough

1. **Start a run:**
   ```bash
   python python_adapter/example_graph.py
   ```
   The 8-node demo graph will start. Each node takes 5 seconds.

2. **Kill the process:**
   Wait for node 2 or 3 to finish, then press `Ctrl+C` or run `kill -9 <pid>` to terminate the Python process.

3. **Verify EtchFlow state:**
   ```bash
   curl http://localhost:8080/runs/<run_id>/state
   ```
   You will see the last successfully checkpointed node and the full graph state.

4. **Resume the run:**
   ```bash
   python python_adapter/example_graph.py --resume <run_id>
   ```
   LangGraph will load the state from EtchFlow, skip the already completed nodes, and resume execution from the exact point of the crash.

## 📁 Project Structure

- `cmd/server/`: Go service entry point.
- `internal/`: Core logic (API handlers, database stores, state machine).
- `migrations/`: SQL migration files for PostgreSQL.
- `python_adapter/`: The EtchFlow Python SDK and LangGraph `BaseCheckpointSaver` implementation.
- `Dockerfile` & `docker-compose.yml`: Container orchestration.

## 🛠️ Makefile Commands

- `make run`: Build and start services.
- `make stop`: Stop services.
- `make clean`: Stop services and wipe database volumes.
- `make logs`: Tail EtchFlow service logs.
- `make kill-test`: Run the automated crash recovery test.
