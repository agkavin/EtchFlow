"""
etchflow_graph.py

High-level wrapper to provide zero-boilerplate LangGraph integration.
"""

from typing import Any
from langgraph.graph import StateGraph

from .client import EtchFlowClient
from .saver import EtchFlowCheckpointSaver

class EtchFlowGraph:
    """
    Wraps a LangGraph StateGraph to provide seamless EtchFlow integration.
    """
    
    def __init__(self, client: EtchFlowClient, builder: StateGraph):
        self.client = client
        self.builder = builder
        
    def invoke(self, inputs: dict | None = None, config: dict | None = None, **kwargs) -> Any:
        config = config or {}
        configurable = config.setdefault("configurable", {})
        thread_id = configurable.get("thread_id")
        
        if not thread_id:
            raise ValueError("A `thread_id` is required in config to use EtchFlow (standard LangGraph behavior).")
            
        run_id = str(thread_id)
            
        # 1. Handle Run Initialization
        state = self.client.get_state(run_id)
        if state is None:
            if inputs is None:
                raise ValueError(f"Run {run_id} not found. Inputs required to start a new run.")
            self.client.submit_run(self.builder, inputs, run_id=run_id)
        else:
            # Run already exists. LangGraph requires inputs to be None when resuming 
            # without human-in-the-loop state changes, otherwise it restarts the graph!
            inputs = None

        # 2. Transparently compile and execute
        saver = EtchFlowCheckpointSaver(self.client, run_id)
        graph = self.builder.compile(checkpointer=saver)
        return graph.invoke(inputs, config, **kwargs)


class EtchFlow:
    """
    High-level orchestrator for EtchFlow workflows.
    """
    
    def __init__(self, base_url: str = "http://localhost:8080", timeout: float = 30.0):
        self.client = EtchFlowClient(base_url, timeout=timeout)
        
    def compile(self, builder: StateGraph) -> EtchFlowGraph:
        """
        Wraps the StateGraph builder in an EtchFlowGraph.
        """
        return EtchFlowGraph(self.client, builder)
