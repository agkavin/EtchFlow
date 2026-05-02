"""
etchflow_client.py

Thin HTTP wrapper around the EtchFlow REST API.
Provides submit_run, get_state, and save_checkpoint.

All methods are synchronous (httpx sync client).
"""

from __future__ import annotations

from typing import Any

import httpx

from etchflow.serializer import serialize_graph


class EtchFlowClient:
    """
    HTTP client for the EtchFlow Go service.

    Usage:
        client = EtchFlowClient(base_url="http://localhost:8080")
        run_id = client.submit_run(graph=builder, input_data={"input": "..."})
    """

    def __init__(self, base_url: str = "http://localhost:8080", timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.http = httpx.Client(base_url=self.base_url, timeout=timeout)

    def submit_run(self, graph: Any, input_data: dict, run_id: str | None = None) -> str:
        """
        Register a new LangGraph run with EtchFlow.

        Serialises the graph topology, POSTs to /runs, and returns the run_id.
        Python MUST call graph.invoke() immediately after — EtchFlow does NOT
        trigger Python. Python triggers itself.

        Args:
            graph:      A LangGraph StateGraph (uncompiled). Topology is extracted
                        as metadata only — no execution logic is sent.
            input_data: The initial input dict for the graph.
            run_id:     Optional user-defined thread ID. If omitted, EtchFlow 
                        auto-generates one.

        Returns:
            run_id (str): The created run ID.

        Raises:
            httpx.HTTPStatusError: If EtchFlow returns a non-2xx response.
        """
        graph_definition = serialize_graph(graph)

        payload = {
            "graph_definition": graph_definition,
            "input_data": input_data,
        }
        if run_id is not None:
            payload["id"] = run_id

        resp = self.http.post("/runs", json=payload)
        resp.raise_for_status()
        return resp.json()["run_id"]

    def get_state(self, run_id: str) -> dict | None:
        """
        Fetch the last committed checkpoint for a run.

        Called by EtchFlowCheckpointSaver.get_tuple() on graph.invoke() start.
        Returns None if no checkpoint exists yet (fresh start).

        Args:
            run_id: UUID of the run.

        Returns:
            dict with keys: run_id, last_node_completed, state, checkpointed_at
            None if no checkpoint exists (404 response).

        Raises:
            httpx.HTTPStatusError: On any non-404 error response.
        """
        resp = self.http.get(f"/runs/{run_id}/state")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def save_checkpoint(self, run_id: str, node_name: str, state: dict) -> dict:
        """
        Atomically persist node state to EtchFlow.

        Called by EtchFlowCheckpointSaver.put() after every node completes.
        Idempotent: sending the same (run_id, node_name) checkpoint twice
        is safe — EtchFlow uses ON CONFLICT DO NOTHING.

        Args:
            run_id:    UUID of the run.
            node_name: Name of the completed node.
            state:     Full LangGraph state dict after this node.

        Returns:
            dict with keys: continue (bool), halt_reason (str | None)

        Raises:
            httpx.HTTPStatusError: On error response.
        """
        resp = self.http.put(f"/runs/{run_id}/checkpoint", json={
            "node_name": node_name,
            "state": state,
        })
        resp.raise_for_status()
        return resp.json()

    def close(self):
        """Close the underlying HTTP client."""
        self.http.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
