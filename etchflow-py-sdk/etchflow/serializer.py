"""
graph_serializer.py

Extracts DAG topology (nodes, edges, entry_point, finish_point)
from a compiled LangGraph StateGraph as JSON-serializable metadata.
"""

from __future__ import annotations

from typing import Any


def serialize_graph(graph: Any) -> dict:
    """
    Extract DAG topology from a LangGraph StateGraph (uncompiled or compiled).
    """
    # Compile if needed
    compiled = graph if hasattr(graph, "nodes") else graph.compile()

    # Extract node names
    all_nodes = list(compiled.nodes.keys())

    # Build edge list
    edges = []
    # In LangGraph 0.2+, graph.edges is a set of Edge objects or tuples.
    # We use getattr to be safe across versions.
    raw_edges = getattr(compiled, "edges", [])
    
    # Defensive handling of edges structure
    if hasattr(raw_edges, "items"):  # It's a dict
        for src, destinations in raw_edges.items():
            if isinstance(destinations, (list, set)):
                for dst in destinations:
                    edges.append({"from": src, "to": dst})
            elif isinstance(destinations, str):
                edges.append({"from": src, "to": destinations})
    elif isinstance(raw_edges, (list, set)):  # It's a list or set
        for edge in raw_edges:
            # Handle Edge objects (namedtuples with source/target)
            if hasattr(edge, "source") and hasattr(edge, "target"):
                edges.append({"from": edge.source, "to": edge.target})
            # Handle tuples (source, target)
            elif isinstance(edge, (list, tuple)) and len(edge) >= 2:
                edges.append({"from": edge[0], "to": edge[1]})
            # Handle Edge namedtuple (some versions use 'src' and 'dst')
            elif hasattr(edge, "src") and hasattr(edge, "dst"):
                edges.append({"from": edge.src, "to": edge.dst})

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
    
    # If still not found, check if __start__ is in nodes (unlikely)
    if not entry_point and all_nodes:
        # Most linear graphs entry point is the first node added
        entry_point = all_nodes[0]

    if not finish_point:
        # The node before __end__ is the finish point
        for edge in edges:
            if edge["to"] == "__end__":
                finish_point = edge["from"]
                break
                
    # If still not found, last node is usually the finish point
    if not finish_point and all_nodes:
        finish_point = all_nodes[-1]

    return {
        "nodes": all_nodes,
        "edges": edges,
        "entry_point": entry_point,
        "finish_point": finish_point,
    }
