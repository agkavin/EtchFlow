"""
example_graph.py

8-node LangGraph demo that integrates with EtchFlow for crash recovery.

Each node sleeps 5 seconds to simulate an LLM call.
Total runtime: ~40 seconds for an uninterrupted run.
This gives a wide window for the kill test.

Usage:
    # Start a new run:
    python example_graph.py

    # Resume after a crash:
    python example_graph.py --resume <run_id>

The run_id is written to /tmp/etchflow_run_id on start for use by `make kill-test`.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import TypedDict

from langgraph.graph import StateGraph

from etchflow_client import EtchFlowClient
from etchflow_checkpoint_saver import EtchFlowCheckpointSaver


# ── Graph state definition ────────────────────────────────────────────────────

class AnalysisState(TypedDict):
    """State passed between all 8 nodes."""
    input: str
    extracted: str
    classified: str
    analysed: str
    summarised: str
    drafted: str
    reviewed: str
    formatted: str
    published: str


# ── Node functions ────────────────────────────────────────────────────────────
# Each node simulates an LLM call with time.sleep(5).
# LangGraph automatically calls saver.put() after each one completes.

def node_extract(state: AnalysisState) -> dict:
    print("[Node 1/8: extract]   executing...", flush=True)
    time.sleep(5)
    result = {"extracted": f"Extracted data from: {state['input']}"}
    print("[Node 1/8: extract]   ✓ checkpointed", flush=True)
    return result


def node_classify(state: AnalysisState) -> dict:
    print("[Node 2/8: classify]  executing...", flush=True)
    time.sleep(5)
    result = {"classified": f"Classified: {state['extracted']}"}
    print("[Node 2/8: classify]  ✓ checkpointed", flush=True)
    return result


def node_analyse(state: AnalysisState) -> dict:
    print("[Node 3/8: analyse]   executing...", flush=True)
    time.sleep(5)
    result = {"analysed": f"Analysis of: {state['classified']}"}
    print("[Node 3/8: analyse]   ✓ checkpointed", flush=True)
    return result


def node_summarise(state: AnalysisState) -> dict:
    print("[Node 4/8: summarise] executing...", flush=True)
    time.sleep(5)
    result = {"summarised": f"Summary: {state['analysed']}"}
    print("[Node 4/8: summarise] ✓ checkpointed", flush=True)
    return result


def node_draft(state: AnalysisState) -> dict:
    print("[Node 5/8: draft]     executing...", flush=True)
    time.sleep(5)
    result = {"drafted": f"Draft: {state['summarised']}"}
    print("[Node 5/8: draft]     ✓ checkpointed", flush=True)
    return result


def node_review(state: AnalysisState) -> dict:
    print("[Node 6/8: review]    executing...", flush=True)
    time.sleep(5)
    result = {"reviewed": f"Reviewed: {state['drafted']}"}
    print("[Node 6/8: review]    ✓ checkpointed", flush=True)
    return result


def node_format(state: AnalysisState) -> dict:
    print("[Node 7/8: format]    executing...", flush=True)
    time.sleep(5)
    result = {"formatted": f"Formatted: {state['reviewed']}"}
    print("[Node 7/8: format]    ✓ checkpointed", flush=True)
    return result


def node_publish(state: AnalysisState) -> dict:
    print("[Node 8/8: publish]   executing...", flush=True)
    time.sleep(5)
    result = {"published": f"Published: {state['formatted']}"}
    print("[Node 8/8: publish]   ✓ checkpointed", flush=True)
    return result


# ── Build the graph ───────────────────────────────────────────────────────────

def build_graph() -> StateGraph:
    builder = StateGraph(AnalysisState)

    builder.add_node("extract",   node_extract)
    builder.add_node("classify",  node_classify)
    builder.add_node("analyse",   node_analyse)
    builder.add_node("summarise", node_summarise)
    builder.add_node("draft",     node_draft)
    builder.add_node("review",    node_review)
    builder.add_node("format",    node_format)
    builder.add_node("publish",   node_publish)

    builder.set_entry_point("extract")
    builder.add_edge("extract",   "classify")
    builder.add_edge("classify",  "analyse")
    builder.add_edge("analyse",   "summarise")
    builder.add_edge("summarise", "draft")
    builder.add_edge("draft",     "review")
    builder.add_edge("review",    "format")
    builder.add_edge("format",    "publish")
    builder.set_finish_point("publish")

    return builder


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    client = EtchFlowClient(base_url="http://localhost:8080")
    builder = build_graph()

    if "--resume" in sys.argv:
        # Crash recovery path
        idx = sys.argv.index("--resume")
        run_id = sys.argv[idx + 1]
        print(f"\nResuming run: {run_id}", flush=True)
        data = client.get_state(run_id)
        if data:
            print(f"Last checkpoint: {data['last_node_completed']}", flush=True)
        else:
            print("No checkpoint found — starting fresh", flush=True)
    else:
        # Happy path: submit a new run
        run_id = client.submit_run(
            graph=builder,
            input_data={"input": "Analyse Q3 financial report"},
        )
        print(f"\nRun submitted: {run_id}", flush=True)

        # Write run_id to temp file for make kill-test
        Path("/tmp/etchflow_run_id").write_text(run_id)

    print(f"Starting graph execution...\n", flush=True)

    # Bind the saver to this specific run
    saver = EtchFlowCheckpointSaver(client=client, run_id=run_id)

    # Compile the graph with our checkpoint saver
    graph = builder.compile(checkpointer=saver)

    try:
        result = graph.invoke(
            {"input": "Analyse Q3 financial report"},
            config={"configurable": {"thread_id": run_id}},
        )
        print(f"\nRun complete ✅")
        print(f"Final state: {result.get('published', 'N/A')}")
    except StopIteration as e:
        # EtchFlow signalled halt (run finished successfully)
        print(f"\nRun complete ✅ ({e})")
    except Exception as e:
        print(f"\nRun failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
