"""
etchflow.worker

A professional, managed SDK Worker class that handles the lifecycle
of fetching runs, executing them with LangGraph, keeping the run alive
via heartbeats, and handling errors/retries.
"""

import concurrent.futures
import logging
import signal
import threading
import time
import uuid
from typing import Any

from etchflow.client import EtchFlowClient
from etchflow.saver import EtchFlowCheckpointSaver

logger = logging.getLogger(__name__)


class EtchFlowWorker:
    def __init__(
        self,
        client: EtchFlowClient,
        graph: Any,
        concurrency: int = 4,
        poll_interval: float = 5.0,
        heartbeat_interval: float = 30.0,
    ):
        self.client = client
        self.graph = graph
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self.worker_id = str(uuid.uuid4())
        
        self.is_running = False
        self._executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._active_runs: set[str] = set()
        self._lock = threading.Lock()

    def start(self):
        """Starts the worker polling loop."""
        self.is_running = True
        self._setup_signals()
        
        logger.info(f"Worker {self.worker_id} starting with concurrency {self.concurrency}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            self._executor = executor
            
            while self.is_running:
                # Don't poll if we're at max capacity
                with self._lock:
                    active_count = len(self._active_runs)
                
                if active_count >= self.concurrency:
                    time.sleep(self.poll_interval)
                    continue

                try:
                    run = self.client.claim_next_run(self.worker_id)
                except Exception as e:
                    logger.error(f"Error polling for runs: {e}")
                    time.sleep(self.poll_interval)
                    continue
                    
                if not run:
                    # No runs available
                    time.sleep(self.poll_interval)
                    continue
                    
                run_id = run["run_id"]
                logger.info(f"Claimed run {run_id}")
                
                with self._lock:
                    self._active_runs.add(run_id)
                    
                # Submit to thread pool
                executor.submit(self._execute_run, run)

        logger.info("Worker stopped.")

    def stop(self):
        """Gracefully stops the worker."""
        logger.info("Graceful shutdown requested...")
        self.is_running = False
        if self._executor:
            self._executor.shutdown(wait=True)

    def _setup_signals(self):
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        self.stop()

    def _execute_run(self, run: dict):
        run_id = run["run_id"]
        
        stop_heartbeat = threading.Event()
        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(run_id, stop_heartbeat),
            daemon=True
        )
        heartbeat_thread.start()
        
        saver = EtchFlowCheckpointSaver(self.client, run_id)
        
        try:
            if hasattr(self.graph, 'compile'):
                compiled_graph = self.graph.compile(checkpointer=saver)
            else:
                if hasattr(self.graph, 'checkpointer') and self.graph.checkpointer is not None:
                    if isinstance(self.graph.checkpointer, EtchFlowCheckpointSaver):
                        compiled_graph = self.graph
                    else:
                        compiled_graph = self._recompile_with_checkpointer(self.graph, saver)
                else:
                    compiled_graph = self._recompile_with_checkpointer(self.graph, saver)
        except Exception as e:
            logger.error(f"Failed to compile graph for run {run_id}: {e}")
            stop_heartbeat.set()
            return

        config = {"configurable": {"thread_id": run_id}}
        
        try:
            state = self.client.get_state(run_id)
            input_data = run.get("input_data", {})
            if state and state.get("state"):
                logger.info(f"Run {run_id} has existing state. Resuming...")
                input_data = None

            compiled_graph.invoke(input_data, config=config)
            
            self.client.complete_run(run_id)
            logger.info(f"Run {run_id} completed successfully")
        except Exception as e:
            logger.exception(f"Run {run_id} failed with error: {e}")
            try:
                self.client.fail_run(run_id, str(e), fatal=False)
            except Exception as fail_err:
                logger.error(f"Failed to report failure for run {run_id}: {fail_err}")

        finally:
            # Stop heartbeat and cleanup
            stop_heartbeat.set()
            heartbeat_thread.join()
            with self._lock:
                self._active_runs.remove(run_id)

    def _recompile_with_checkpointer(self, compiled_graph, saver):
        """Try to extract original builder and recompile with our saver."""
        try:
            if hasattr(compiled_graph, 'graph'):
                original_graph = compiled_graph.graph
                if hasattr(original_graph, 'compile'):
                    return original_graph.compile(checkpointer=saver)
        except Exception:
            pass
        return compiled_graph

    def _heartbeat_loop(self, run_id: str, stop_event: threading.Event):
        while not stop_event.is_set():
            try:
                self.client.heartbeat(run_id)
            except Exception as e:
                logger.error(f"Heartbeat failed for run {run_id}: {e}")
            
            # Wait for heartbeat_interval, returns True if stop_event is set
            stop_event.wait(self.heartbeat_interval)
