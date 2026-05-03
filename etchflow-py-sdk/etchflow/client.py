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

    def claim_next_run(self, worker_id: str) -> dict | None:
        """
        Poll EtchFlow for the next PENDING run.
        Returns the run record if claimed, or None if no runs are pending.
        """
        resp = self.http.post("/runs/claim", json={"worker_id": worker_id})
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def heartbeat(self, run_id: str) -> None:
        """
        Update the liveness timestamp for a running graph.
        """
        resp = self.http.put(f"/runs/{run_id}/heartbeat")
        resp.raise_for_status()

    def fail_run(self, run_id: str, error: str, fatal: bool = False) -> None:
        """
        Report an unhandled exception to EtchFlow to trigger the retry engine.
        If fatal=True, skip retries and mark DEAD immediately.
        """
        resp = self.http.post(f"/runs/{run_id}/fail", json={
            "error": error,
            "fatal": fatal,
        })
        resp.raise_for_status()

    def complete_run(self, run_id: str) -> None:
        """
        Mark a run as SUCCESS. Called by the worker after the graph completes naturally.
        """
        resp = self.http.post(f"/runs/{run_id}/complete")
        resp.raise_for_status()

    def get_checkpoints(self, run_id: str) -> list[dict]:
        """
        Fetch full checkpoint history for a run.
        """
        resp = self.http.get(f"/runs/{run_id}/checkpoints")
        resp.raise_for_status()
        return resp.json().get("checkpoints", [])

    def get_logs(self, run_id: str) -> list[dict]:
        """
        Fetch audit logs for a run.
        """
        resp = self.http.get(f"/runs/{run_id}/logs")
        resp.raise_for_status()
        return resp.json().get("logs", [])




    def cancel_run(self, run_id: str) -> None:
        """
        Cancel a running or pending graph.
        """
        resp = self.http.post(f"/runs/{run_id}/cancel")
        resp.raise_for_status()

    def get_checkpoints(self, run_id: str) -> list[dict]:
        """
        Fetch the checkpoint history for a run.
        Returns list of checkpoints with node_name, state, and created_at.
        """
        resp = self.http.get(f"/runs/{run_id}/checkpoints")
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        data = resp.json()
        return data.get("checkpoints", [])

    def get_logs(self, run_id: str) -> list[dict]:
        """
        Fetch the audit log for a run.
        """
        resp = self.http.get(f"/runs/{run_id}/logs")
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        data = resp.json()
        return data.get("logs", [])

    def close(self):
        """Close the underlying HTTP client."""
        self.http.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

