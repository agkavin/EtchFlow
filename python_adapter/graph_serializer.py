"""
graph_serializer.py

Extracts DAG topology (nodes, edges, entry_point, finish_point)
from a compiled LangGraph StateGraph as JSON-serializable metadata.

IMPORTANT: This extracts metadata only — not execution logic.
EtchFlow stores this for display and validation, not to drive execution.
Python drives its own execution. EtchFlow records what happened.
"""

from __future__ import annotations

from typing import Any


def serialize_graph(graph: Any) -> dict:
    """
    Extract DAG topology from a LangGraph StateGraph (uncompiled or compiled).

    Returns a dict with:
        nodes:       list of node name strings
        edges:       list of {"from": str, "to": str} dicts
        entry_point: str — name of the first node to execute
        finish_point: str — name of the last node (triggers SUCCESS on checkpoint)

    Args:
        graph: A LangGraph StateGraph (uncompiled) or CompiledGraph.

    Returns:
        JSON-serializable dict representing the DAG topology.

    Raises:
        ValueError: If the graph has no nodes or entry_point cannot be determined.
    """
    # Compile if needed
    compiled = graph if hasattr(graph, "nodes") else graph.compile()

    # Extract node names (filter out LangGraph internals like __start__, __end__)
    all_nodes = list(compiled.nodes.keys())

    # Build edge list
    edges = []
    raw_edges = getattr(compiled, "edges", [])

    if isinstance(raw_edges, dict):
        for src, destinations in raw_edges.items():
            if isinstance(destinations, (list, set)):
                for dst in destinations:
                    edges.append({"from": src, "to": dst})
            elif isinstance(destinations, str):
                edges.append({"from": src, "to": destinations})
    elif isinstance(raw_edges, (list, set)):
        for edge in raw_edges:
            # Handle Edge objects (namedtuples with source/target)
            if hasattr(edge, "source") and hasattr(edge, "target"):
                edges.append({"from": edge.source, "to": edge.target})
            # Handle tuples (source, target)
            elif isinstance(edge, (list, tuple)) and len(edge) >= 2:
                edges.append({"from": edge[0], "to": edge[1]})

    # Determine entry_point and finish_point
    entry_point = getattr(compiled, "entry_point", None)
    finish_point = getattr(compiled, "finish_point", None)

    # Fallback: if attributes missing, infer from nodes
    if not entry_point:
        # The node after __start__ is the entry point
        for edge in edges:
            if edge["from"] == "__start__":
                entry_point = edge["to"]
                break

    if not finish_point:
        # The node before __end__ is the finish point
        for edge in edges:
            if edge["to"] == "__end__":
                finish_point = edge["from"]
                break

    if not entry_point:
        raise ValueError("Could not determine entry_point from graph. "
                         "Ensure you called builder.set_entry_point().")
    if not finish_point:
        raise ValueError("Could not determine finish_point from graph. "
                         "Ensure you called builder.set_finish_point().")

    return {
        "nodes": all_nodes,
        "edges": edges,
        "entry_point": entry_point,
        "finish_point": finish_point,
    }
