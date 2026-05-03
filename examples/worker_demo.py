#!/usr/bin/env python3
"""
EtchFlow Worker Example - Batch Processing Mode

This demonstrates the worker pattern where:
1. User submits runs (tasks)
2. Workers poll for PENDING runs
3. Workers execute them with heartbeats
4. If worker dies, Go's Reaper re-queues the run
5. Another worker picks it up

Usage:
    # Terminal 1: Start workers (will poll for work)
    python examples/worker_demo.py
    
    # Terminal 2: Submit jobs (they'll be picked up by workers)
    python examples/submit_job.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'etchflow-py-sdk'))

from typing import TypedDict
import time
import logging
from langgraph.graph import StateGraph, END
from etchflow import EtchFlowClient, EtchFlowWorker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# === Define the workflow ===
class TaskState(TypedDict):
    task_id: str
    result: str
    status: str


def processor(state: TaskState):
    """Simulate work (e.g., LLM call, API request)"""
    logger.info(f"Processing task: {state['task_id']}")
    time.sleep(3)  # Simulate work
    
    # Example: process the task
    result = f"processed-{state['task_id']}"
    return {"result": result, "status": "completed"}


# Build graph
builder = StateGraph(TaskState)
builder.add_node("process", processor)
builder.set_entry_point("process")
builder.add_edge("process", END)

# The worker expects a compiled graph (it's already compiled in this script)
compiled = builder.compile()


def main():
    print("="*60)
    print("EtchFlow Worker - Batch Processing Demo")
    print("="*60)
    print("")
    print("This worker will:")
    print("  1. Poll /runs/claim every 5 seconds")
    print("  2. Execute any PENDING runs")
    print("  3. Send heartbeats every 30s")
    print("  4. If it dies, Reaper will re-queue the run")
    print("")
    print("In another terminal, run:")
    print("  python examples/submit_job.py")
    print("")
    print("Starting worker...")
    print("="*60)
    
    client = EtchFlowClient("http://localhost:8080")
    
    worker = EtchFlowWorker(
        client=client,
        graph=compiled,
        concurrency=2,      # Process 2 runs at a time
        poll_interval=5.0,  # Poll every 5 seconds
        heartbeat_interval=30.0
    )
    
    try:
        worker.start()
    except KeyboardInterrupt:
        print("\nWorker stopped.")


if __name__ == "__main__":
    main()