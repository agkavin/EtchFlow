#!/usr/bin/env python3
"""
Job Submitter - Submits tasks to the EtchFlow queue

Usage:
    python examples/submit_job.py              # Submit 1 job
    python examples/submit_job.py --count 5    # Submit 5 jobs
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'etchflow-py-sdk'))

from typing import TypedDict
import time
import argparse
from langgraph.graph import StateGraph, END
from etchflow import EtchFlowClient


# === Same workflow as worker_demo.py ===
class TaskState(TypedDict):
    task_id: str
    result: str
    status: str


def processor(state: TaskState):
    time.sleep(3)
    return {"result": f"processed-{state['task_id']}", "status": "completed"}


builder = StateGraph(TaskState)
builder.add_node("process", processor)
builder.set_entry_point("process")
builder.add_edge("process", END)


def submit_job(client, task_id):
    """Submit a job - returns immediately, worker picks it up"""
    run_id = f"job-{task_id}-{int(time.time())}"
    
    # Submit to EtchFlow - creates PENDING run
    # Workers poll for PENDING runs, execute them
    client.submit_run(
        graph=builder,
        input_data={"task_id": task_id, "result": "", "status": "queued"},
        run_id=run_id
    )
    
    print(f"✅ Submitted: {run_id}")
    return run_id


def main():
    parser = argparse.ArgumentParser(description="Submit jobs to EtchFlow")
    parser.add_argument("--count", type=int, default=1, help="Number of jobs to submit")
    args = parser.parse_args()
    
    client = EtchFlowClient("http://localhost:8080")
    
    print(f"Submitting {args.count} job(s)...")
    print("")
    
    for i in range(args.count):
        submit_job(client, f"task-{i+1}")
        time.sleep(0.5)  # Small delay between submissions
    
    print("")
    print("Jobs submitted! Workers should pick them up.")
    print("Check status: curl http://localhost:8080/runs/<job-id>")


if __name__ == "__main__":
    main()