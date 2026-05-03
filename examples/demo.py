#!/usr/bin/env python3
"""
EtchFlow Demo - Zero-boilerplate LangGraph + Durable Execution

Usage:
    python demo.py                    # Fresh run (saves run_id to .run_id)
    python demo.py --resume           # Resume from saved run_id
    python demo.py --resume <id>      # Resume from specific run_id
"""

import os
import sys

# Add SDK to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'etchflow-py-sdk'))

from typing import TypedDict
import time
from langgraph.graph import StateGraph, END
from etchflow import EtchFlow, EtchFlowClient


class BlogState(TypedDict):
    topic: str
    research: str
    outline: str
    content: str
    final: str


def researcher(state: BlogState):
    print("[Research] Gathering info...", flush=True)
    time.sleep(2)
    return {"research": f"Data about: {state['topic']}"}


def planner(state: BlogState):
    print("[Planner] Creating outline...", flush=True)
    time.sleep(2)
    return {"outline": f"Plan based on: {state['research']}"}


def writer(state: BlogState):
    print("[Writer] Writing content...", flush=True)
    time.sleep(2)
    return {"content": f"Content from: {state['outline']}"}


def editor(state: BlogState):
    print("[Editor] Editing...", flush=True)
    time.sleep(2)
    return {"content": state["content"] + "\n\n[Edited by AI]"}


def formatter(state: BlogState):
    print("[Formatter] Finalizing...", flush=True)
    time.sleep(2)
    return {"final": f"# {state['topic']}\n\n{state['content']}\n\n---Done---"}


# Build graph (5 nodes)
builder = StateGraph(BlogState)
builder.add_node("researcher", researcher)
builder.add_node("planner", planner)
builder.add_node("writer", writer)
builder.add_node("editor", editor)
builder.add_node("formatter", formatter)
builder.set_entry_point("researcher")
builder.add_edge("researcher", "planner")
builder.add_edge("planner", "writer")
builder.add_edge("writer", "editor")
builder.add_edge("editor", "formatter")
builder.add_edge("formatter", END)

# Wrap with EtchFlow
app = EtchFlow("http://localhost:8080").compile(builder)

# Handle run_id
RUN_ID_FILE = ".run_id"

# Get run_id from args or saved file or generate new
def get_run_id():
    if len(sys.argv) > 2 and sys.argv[1] == "--resume":
        return sys.argv[2]
    if len(sys.argv) > 1 and sys.argv[1] == "--resume" and len(sys.argv) == 3:
        return sys.argv[2]
    if os.path.exists(RUN_ID_FILE):
        return open(RUN_ID_FILE).read().strip()
    return f"demo-{int(time.time())}"

run_id = get_run_id()

# Save run_id immediately (before execution - for crash recovery)
with open(RUN_ID_FILE, "w") as f:
    f.write(run_id)

print(f"Run ID: {run_id}")
print("="*50)

# Invoke
result = app.invoke(
    {"topic": "The History of Coffee"},
    config={"configurable": {"thread_id": run_id}}
)

print("="*50)
print(f"Final: {result.get('final', 'N/A')}")

# Show status
client = EtchFlowClient("http://localhost:8080")
status = client.http.get(f"/runs/{run_id}").json()["status"]
print(f"Status: {status}")
print(f"Run {run_id} complete ✅")