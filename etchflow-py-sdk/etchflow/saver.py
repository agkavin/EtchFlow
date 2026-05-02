"""
etchflow_checkpoint_saver.py

Custom LangGraph checkpoint saver that routes state to EtchFlow instead of memory.

Implements the real 2025 BaseCheckpointSaver interface:
    get_tuple(config)                          → CheckpointTuple | None
    put(config, checkpoint, metadata, versions) → RunnableConfig
    put_writes(config, writes, task_id)         → None  (no-op for MVP)
    list(config, *, filter, before, limit)      → Iterator[CheckpointTuple]  (stub for MVP)

IMPORTANT: LangGraph calls these methods automatically.
You do NOT call them directly. Just pass the saver to builder.compile(checkpointer=saver).
"""

from __future__ import annotations

from typing import Any, Iterator, Optional, Sequence, Tuple

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
)

from etchflow.client import EtchFlowClient


class EtchFlowCheckpointSaver(BaseCheckpointSaver):
    """
    Routes LangGraph's checkpoint calls to the EtchFlow REST API.

    This is the integration point between LangGraph and EtchFlow.
    LangGraph already calls checkpointing automatically after every node.
    This class just redirects those calls to EtchFlow instead of an in-process store.

    The run_id is bound at construction because Python always knows the run_id:
    either it just submitted the run (new) or it has it from --resume (crash recovery).

    Usage:
        client = EtchFlowClient()
        run_id = client.submit_run(graph=builder, input_data={...})
        saver = EtchFlowCheckpointSaver(client=client, run_id=run_id)
        graph = builder.compile(checkpointer=saver)
        graph.invoke({...}, config={"configurable": {"thread_id": run_id}})
    """

    def __init__(self, client: EtchFlowClient, run_id: str):
        super().__init__()
        self.client = client
        self.run_id = run_id

    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        """
        Called by LangGraph at the start of graph.invoke().

        Fetches the last committed checkpoint from EtchFlow.
        - If a checkpoint exists → LangGraph resumes from that node (skipping earlier nodes).
        - If None → LangGraph starts fresh from the entry_point.

        This is the entire crash recovery mechanism on the Python side.
        No manual logic needed — just return the checkpoint and LangGraph handles the rest.
        """
        data = self.client.get_state(self.run_id)
        if not data:
            return None  # Fresh start

        # The state stored in EtchFlow is the full LangGraph checkpoint dict
        checkpoint: Checkpoint = data["state"]
        last_node = data.get("last_node_completed", "")

        config_with_id: RunnableConfig = {
            "configurable": {
                "thread_id": self.run_id,
                "checkpoint_id": checkpoint.get("id"),
                "checkpoint_ns": "",
            }
        }

        return CheckpointTuple(
            config=config_with_id,
            checkpoint=checkpoint,
            metadata={
                "source": "loop",
                "step": -1,
                "parents": {},
            },
            parent_config=None,
            pending_writes=[],
        )

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: dict,
    ) -> RunnableConfig:
        """
        Called by LangGraph after every node completes.

        Sends the full graph state to EtchFlow for atomic persistence.
        If EtchFlow returns continue=false (run finished), raises RuntimeError
        to signal LangGraph that execution should stop.

        Returns the updated RunnableConfig for LangGraph's internal tracking.
        """
        # Extract node name from metadata
        # In LangGraph 0.2+, metadata["source"] is usually just "loop".
        # The actual node name is typically the key in metadata["writes"].
        node_name = metadata.get("source", "unknown")
        writes = metadata.get("writes", {})
        if isinstance(writes, dict) and writes:
            # Get the first key from writes (e.g., "extract", "publish")
            node_name = list(writes.keys())[0]
        elif "step" in metadata:
            # Fallback to step number if no writes
            node_name = f"step_{metadata['step']}"

        # Send to EtchFlow
        response = self.client.save_checkpoint(
            run_id=self.run_id,
            node_name=node_name,
            state=checkpoint,
        )

        # If EtchFlow says halt (run finished or cancelled), stop Python execution
        if not response.get("continue", True):
            halt = response.get("halt_reason") or "run completed"
            raise StopIteration(f"EtchFlow: {halt}")

        # Return updated config for LangGraph's internal use
        return {
            "configurable": {
                "thread_id": self.run_id,
                "checkpoint_id": node_name,
                "checkpoint_ns": "",
            }
        }

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[Tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """
        Stores intermediate pending writes (not yet committed to a full checkpoint).
        No-op for MVP — EtchFlow only persists committed node state.

        Phase 1.5 may implement this to support finer-grained recovery.
        """
        pass

    def list(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[dict] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:
        """
        Lists checkpoint history for a run.
        Stub for MVP — yields nothing.

        Phase 1.5 full implementation: GET /runs/{id}/checkpoints.
        """
        return iter([])
